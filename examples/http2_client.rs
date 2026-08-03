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
//! Pass a `wss://` URL as the first argument to talk to a real endpoint. Note that this
//! fails against a server that does not implement RFC 8441, which is most of them: the
//! HTTP/2 handshake is opt-in and does not fall back. Drop the `http_version` call to use
//! the default HTTP/1.1 handshake instead.

use futures::{SinkExt, StreamExt};
use yawc::{frame::OpCode, Frame, HttpVersion, Options, WebSocket};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:9002/chat".to_string());

    // Over plaintext this is HTTP/2 prior knowledge; over TLS it offers only the h2 ALPN
    // protocol. Either way the peer has to implement RFC 8441 or the connection fails.
    let mut ws = WebSocket::connect(url.parse()?)
        .http_version(HttpVersion::Http2)
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
