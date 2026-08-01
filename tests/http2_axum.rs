//! The axum `IncomingUpgrade` extractor over HTTP/2 (RFC 8441).
//!
//! `axum::serve` does not expose `enable_connect_protocol()`, so the router is served
//! through hyper's HTTP/2 builder directly. Everything runs over plaintext with HTTP/2
//! prior knowledge, so no certificates are needed.

#![cfg(all(feature = "http2", feature = "axum"))]

use std::net::SocketAddr;

use axum::{response::IntoResponse, routing::any, Router};
use futures::{SinkExt, StreamExt};
use hyper::server::conn::http2;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use tokio::net::TcpListener;
use yawc::{frame::OpCode, Frame, HttpVersion, HttpWebSocket, IncomingUpgrade, Options, WebSocket};

/// Echoes data frames back to the client.
async fn ws_handler(ws: IncomingUpgrade) -> impl IntoResponse {
    let (response, fut) = ws
        .upgrade(Options::default().with_balanced_compression())
        .unwrap();

    tokio::spawn(async move {
        let mut ws = fut.await.unwrap();
        while let Some(frame) = ws.next().await {
            if matches!(frame.opcode(), OpCode::Text | OpCode::Binary)
                && ws.send(frame).await.is_err()
            {
                break;
            }
        }
    });

    response
}

/// Serves an axum router over HTTP/2 with extended CONNECT enabled.
async fn spawn_axum_server() -> SocketAddr {
    // `any` rather than `get`: an RFC 8441 handshake arrives as CONNECT, so a
    // GET-only route would never match it.
    let app = Router::new().route("/chat", any(ws_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let service = TowerToHyperService::new(app.clone());

            tokio::spawn(async move {
                let _ = http2::Builder::new(TokioExecutor::new())
                    .enable_connect_protocol()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    addr
}

async fn connect(addr: SocketAddr) -> HttpWebSocket {
    WebSocket::connect(format!("ws://{addr}/chat").parse().unwrap())
        .http_version(HttpVersion::Http2)
        .with_options(Options::default().with_balanced_compression())
        .await
        .unwrap()
}

#[tokio::test]
async fn extractor_accepts_extended_connect() {
    let addr = spawn_axum_server().await;
    let mut ws = connect(addr).await;

    ws.send(Frame::text("hello axum")).await.unwrap();

    let frame = ws.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Text);
    assert_eq!(frame.payload().as_ref(), b"hello axum");
}

#[tokio::test]
async fn extractor_negotiates_compression_over_http2() {
    let addr = spawn_axum_server().await;
    let mut ws = connect(addr).await;

    let payload = "axum deflate ".repeat(2048);
    ws.send(Frame::text(payload.clone())).await.unwrap();

    let frame = ws.next().await.unwrap();
    assert_eq!(frame.payload().as_ref(), payload.as_bytes());
}

#[tokio::test]
async fn extractor_rejects_a_plain_request_on_the_same_route() {
    // The route accepts any method, so an ordinary HTTP/2 GET reaches the extractor. It
    // is not an extended CONNECT and carries no Sec-WebSocket-Key, so it must be turned
    // away rather than upgraded.
    let addr = spawn_axum_server().await;

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();

    tokio::spawn(conn);

    let request = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri(format!("http://{addr}/chat"))
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();

    let response = sender.send_request(request).await.unwrap();

    assert_eq!(response.status(), hyper::StatusCode::BAD_REQUEST);
}
