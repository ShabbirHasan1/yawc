//! WebSocket echo server over HTTP/2 (RFC 8441).
//!
//! Run with:
//!
//! ```sh
//! cargo run --features http2 --example http2_server
//! ```
//!
//! Then point the matching client at it:
//!
//! ```sh
//! cargo run --features http2 --example http2_client
//! ```
//!
//! The one thing that makes RFC 8441 work is `enable_connect_protocol()` below. It is
//! what advertises `SETTINGS_ENABLE_CONNECT_PROTOCOL`; without it hyper rejects the
//! extended CONNECT before the handler ever runs.
//!
//! This listens over plaintext HTTP/2, which requires clients to use prior knowledge. A
//! real deployment would terminate TLS and negotiate `h2` over ALPN.

use std::convert::Infallible;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::Empty;
use hyper::{body::Incoming, server::conn::http2, service::service_fn, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use yawc::{frame::OpCode, Options, WebSocket};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9002").await?;
    println!("listening on ws://127.0.0.1:9002 (http/2)");

    loop {
        let (stream, peer) = listener.accept().await?;

        tokio::spawn(async move {
            let result = http2::Builder::new(TokioExecutor::new())
                .enable_connect_protocol()
                .serve_connection(TokioIo::new(stream), service_fn(handle))
                .await;

            if let Err(err) = result {
                eprintln!("connection from {peer} failed: {err}");
            }
        });
    }
}

/// Upgrades the request and echoes messages back.
///
/// `upgrade_with_options` picks the handshake from the request, so this same handler also
/// serves HTTP/1.1 clients if the listener is served with `http1::Builder` instead.
async fn handle(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, Infallible> {
    let options = Options::default().with_balanced_compression();

    let (response, fut) = match WebSocket::upgrade_with_options(&mut req, options) {
        Ok(upgrade) => upgrade,
        Err(err) => {
            eprintln!("rejecting handshake: {err}");
            let mut response = Response::new(Empty::new());
            *response.status_mut() = hyper::StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    tokio::spawn(async move {
        let mut ws = match fut.await {
            Ok(ws) => ws,
            Err(err) => {
                eprintln!("upgrade failed: {err}");
                return;
            }
        };

        while let Some(frame) = ws.next().await {
            if matches!(frame.opcode(), OpCode::Text | OpCode::Binary) {
                if let Err(err) = ws.send(frame).await {
                    eprintln!("send failed: {err}");
                    break;
                }
            }
        }
    });

    Ok(response)
}
