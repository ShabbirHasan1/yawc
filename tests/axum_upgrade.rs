//! The axum `IncomingUpgrade` extractor over HTTP/1.1.
//!
//! Adding RFC 8441 support made the extractor's `Sec-WebSocket-Accept` optional, since an
//! extended CONNECT has no key to echo. These tests pin the HTTP/1.1 response so that
//! change cannot alter the RFC 6455 handshake, and they deliberately do not require the
//! `http2` feature: without it the extended-CONNECT branch is compiled out, which is the
//! configuration the autobahn suite runs.

#![cfg(all(feature = "axum", not(target_arch = "wasm32")))]

use std::net::SocketAddr;

use axum::{response::IntoResponse, routing::get, Router};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::Empty;
use hyper::{header, server::conn::http1, StatusCode};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use tokio::net::{TcpListener, TcpStream};
use yawc::{frame::OpCode, Frame, IncomingUpgrade, Options, WebSocket};

/// The example key and its expected accept value from RFC 6455 section 1.3.
const SAMPLE_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
const SAMPLE_ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

async fn ws_handler(ws: IncomingUpgrade) -> impl IntoResponse {
    let (response, fut) = ws.upgrade(Options::default()).unwrap();

    tokio::spawn(async move {
        let Ok(mut ws) = fut.await else { return };
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

async fn spawn_server() -> SocketAddr {
    let app = Router::new().route("/chat", get(ws_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let service = TowerToHyperService::new(app.clone());
            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });

    addr
}

#[tokio::test]
async fn answers_101_with_the_rfc6455_accept_value() {
    let addr = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn.with_upgrades());

    let request = hyper::Request::builder()
        .method("GET")
        .uri("/chat")
        .header(header::HOST, "localhost")
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "upgrade")
        .header(header::SEC_WEBSOCKET_KEY, SAMPLE_KEY)
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = sender.send_request(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response
            .headers()
            .get(header::SEC_WEBSOCKET_ACCEPT)
            .unwrap(),
        SAMPLE_ACCEPT,
    );
    assert_eq!(
        response.headers().get(header::UPGRADE).unwrap(),
        "websocket"
    );
    assert_eq!(
        response.headers().get(header::CONNECTION).unwrap(),
        "upgrade"
    );
}

#[tokio::test]
async fn echoes_over_the_http1_handshake() {
    let addr = spawn_server().await;

    let mut ws = WebSocket::connect(format!("ws://{addr}/chat").parse().unwrap())
        .await
        .unwrap();

    ws.send(Frame::text("still rfc 6455")).await.unwrap();

    let frame = ws.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Text);
    assert_eq!(frame.payload().as_ref(), b"still rfc 6455");
}

#[tokio::test]
async fn rejects_a_request_with_no_key() {
    // The key is only optional for extended CONNECT. Over HTTP/1.1 a missing key must
    // still be turned away rather than producing a keyless 200.
    let addr = spawn_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);

    let request = hyper::Request::builder()
        .method("GET")
        .uri("/chat")
        .header(header::HOST, "localhost")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = sender.send_request(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
