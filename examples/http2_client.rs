//! WebSocket client over HTTP/2 (RFC 8441).
//!
//! Start the server first:
//!
//! ```sh
//! cargo run --features http2 --example http2_server
//! ```
//!
//! then run:
//!
//! ```sh
//! cargo run --features http2 --example http2_client
//! ```
//!
//! Pass a `wss://` URL as the first argument to talk to a real endpoint, in which case
//! [`HttpVersion::Auto`] negotiates HTTP/2 over ALPN and quietly falls back to HTTP/1.1
//! when the server does not support RFC 8441.

use futures::{SinkExt, StreamExt};
use yawc::{frame::OpCode, Frame, HttpVersion, Options, WebSocket};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:9002/chat".to_string());

    // Plaintext has no ALPN to negotiate with, so it needs HTTP/2 prior knowledge.
    // Anything over TLS can let ALPN decide.
    let version = if url.starts_with("wss://") {
        HttpVersion::Auto
    } else {
        HttpVersion::Http2
    };

    let mut ws = WebSocket::connect(url.parse()?)
        .http_version(version)
        .with_options(Options::default().with_balanced_compression())
        .await?;

    println!("connected");

    for i in 0..5 {
        let message = format!("hello {i}");
        ws.send(Frame::text(message.clone())).await?;
        println!("sent:  {message}");

        match ws.next().await {
            Some(frame) if frame.opcode() == OpCode::Text => {
                println!("recv:  {}", String::from_utf8_lossy(frame.payload()));
            }
            Some(frame) => println!("recv:  {:?} frame", frame.opcode()),
            None => {
                println!("connection closed by peer");
                return Ok(());
            }
        }
    }

    ws.send(Frame::close(yawc::close::CloseCode::Normal, b"done"))
        .await?;

    Ok(())
}
