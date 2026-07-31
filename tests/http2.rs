//! End-to-end tests for WebSockets over HTTP/2 (RFC 8441).
//!
//! The server is hyper's HTTP/2 server with `enable_connect_protocol()`, the client is
//! yawc's HTTP/2 handshake. Everything runs over plaintext TCP using HTTP/2 prior
//! knowledge so the tests do not need certificates.

#![cfg(feature = "http2")]

use std::{convert::Infallible, net::SocketAddr};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::Empty;
use hyper::{body::Incoming, server::conn::http2, service::service_fn, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use yawc::{
    close::CloseCode, frame::OpCode, Frame, HttpVersion, HttpWebSocket, Options, WebSocket,
};

/// Starts an HTTP/2 echo server and returns the address it is listening on.
///
/// `enable_connect_protocol` is what makes the server advertise
/// `SETTINGS_ENABLE_CONNECT_PROTOCOL`; without it hyper rejects extended CONNECT before
/// the handler ever sees the request.
async fn spawn_echo_server(options: Options) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let options = options.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| handle(req, options.clone()));
                let _ = http2::Builder::new(TokioExecutor::new())
                    .enable_connect_protocol()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    addr
}

/// Upgrades an extended CONNECT request and echoes every data frame back.
async fn handle(
    mut req: Request<Incoming>,
    options: Options,
) -> Result<Response<Empty<Bytes>>, Infallible> {
    let (response, fut) = WebSocket::upgrade_with_options(&mut req, options).unwrap();

    tokio::spawn(async move {
        let mut ws = fut.await.unwrap();
        // Close is acknowledged automatically, but the reply is only written on the next
        // poll, so the loop runs until the stream ends rather than breaking on Close.
        while let Some(frame) = ws.next().await {
            if matches!(frame.opcode(), OpCode::Text | OpCode::Binary)
                && ws.send(frame).await.is_err()
            {
                break;
            }
        }
    });

    Ok(response)
}

/// Connects to `addr` over HTTP/2 prior knowledge.
async fn connect(addr: SocketAddr, options: Options) -> yawc::Result<HttpWebSocket> {
    WebSocket::connect(format!("ws://{addr}/chat").parse()?)
        .http_version(HttpVersion::Http2)
        .with_options(options)
        .await
}

#[tokio::test]
async fn echoes_text_and_binary() {
    let addr = spawn_echo_server(Options::default()).await;
    let mut ws = connect(addr, Options::default()).await.unwrap();

    ws.send(Frame::text("hello over h2")).await.unwrap();
    let frame = ws.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Text);
    assert_eq!(frame.payload().as_ref(), b"hello over h2");

    ws.send(Frame::binary(vec![1, 2, 3, 4])).await.unwrap();
    let frame = ws.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Binary);
    assert_eq!(frame.payload().as_ref(), &[1, 2, 3, 4]);
}

#[tokio::test]
async fn echoes_payload_larger_than_one_h2_frame() {
    // Comfortably past the 16 KiB default HTTP/2 frame size, so the payload is split
    // across DATA frames and reassembled by the stream adapter.
    let payload = vec![0xa5_u8; 256 * 1024];
    let options = Options::default().with_limits(1024 * 1024, 4 * 1024 * 1024);

    let addr = spawn_echo_server(options.clone()).await;
    let mut ws = connect(addr, options).await.unwrap();

    ws.send(Frame::binary(payload.clone())).await.unwrap();
    let frame = ws.next().await.unwrap();

    assert_eq!(frame.opcode(), OpCode::Binary);
    assert_eq!(frame.payload().len(), payload.len());
    assert_eq!(frame.payload().as_ref(), payload.as_slice());
}

#[tokio::test]
async fn echoes_with_permessage_deflate() {
    let options = Options::default().with_balanced_compression();

    let addr = spawn_echo_server(options.clone()).await;
    let mut ws = connect(addr, options).await.unwrap();

    // Highly compressible, so this exercises the deflate path rather than the
    // passthrough one.
    let payload = "yawc ".repeat(4096);
    ws.send(Frame::text(payload.clone())).await.unwrap();

    let frame = ws.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Text);
    assert_eq!(frame.payload().as_ref(), payload.as_bytes());
}

#[tokio::test]
async fn many_messages_in_sequence() {
    let addr = spawn_echo_server(Options::default()).await;
    let mut ws = connect(addr, Options::default()).await.unwrap();

    for i in 0..100 {
        let msg = format!("message {i}");
        ws.send(Frame::text(msg.clone())).await.unwrap();

        let frame = ws.next().await.unwrap();
        assert_eq!(frame.payload().as_ref(), msg.as_bytes());
    }
}

#[tokio::test]
async fn completes_the_close_handshake() {
    let addr = spawn_echo_server(Options::default()).await;
    let mut ws = connect(addr, Options::default()).await.unwrap();

    ws.send(Frame::close(CloseCode::Normal, b"bye"))
        .await
        .unwrap();

    let frame = ws.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Close);
    assert!(ws.next().await.is_none());
}

#[tokio::test]
async fn one_connection_carries_concurrent_websockets() {
    // Each connect() opens its own HTTP/2 connection, but they share one server task and
    // must not interfere.
    let addr = spawn_echo_server(Options::default()).await;

    let mut sockets = Vec::new();
    for _ in 0..4 {
        sockets.push(connect(addr, Options::default()).await.unwrap());
    }

    for (i, ws) in sockets.iter_mut().enumerate() {
        ws.send(Frame::text(format!("socket {i}"))).await.unwrap();
    }

    for (i, ws) in sockets.iter_mut().enumerate() {
        let frame = ws.next().await.unwrap();
        assert_eq!(frame.payload().as_ref(), format!("socket {i}").as_bytes());
    }
}

#[tokio::test]
async fn server_without_connect_protocol_is_reported_clearly() {
    // Same server, but never advertising SETTINGS_ENABLE_CONNECT_PROTOCOL.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let service = service_fn(|req| handle(req, Options::default()));
                let _ = http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    let err = match connect(addr, Options::default()).await {
        Ok(_) => panic!("handshake unexpectedly succeeded without extended CONNECT support"),
        Err(err) => err,
    };

    assert!(
        err.is_handshake_error(),
        "expected a handshake error, got {err:?}"
    );
}

#[tokio::test]
async fn fragmented_message_is_reassembled() {
    let addr = spawn_echo_server(Options::default()).await;
    let ws = connect(addr, Options::default()).await.unwrap();
    let mut streaming = ws.into_streaming();

    streaming
        .send(Frame::text("first ").with_fin(false))
        .await
        .unwrap();
    streaming
        .send(Frame::continuation("second").with_fin(true))
        .await
        .unwrap();

    // The server reassembles, so what comes back is one unfragmented message.
    let mut received = Vec::new();
    while let Some(frame) = streaming.next().await {
        received.extend_from_slice(frame.payload());
        if frame.is_fin() {
            break;
        }
    }

    assert_eq!(received, b"first second");
}
