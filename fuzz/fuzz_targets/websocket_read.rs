#![no_main]

//! Feeds arbitrary bytes to a `WebSocket` as if they came from a peer, with
//! permessage-deflate negotiated.
//!
//! This covers the whole read path: frame decoding, fragment assembly, decompression
//! and UTF-8 validation. Reading must always terminate, either with frames or with an
//! error. Issue #40 was a hang in this path, where a deflate stream ending in a final
//! block spun the inflate loop forever on the trailing bytes.

use libfuzzer_sys::fuzz_target;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yawc::{Options, WebSocket};

/// Response headers that get the client past the handshake with compression enabled.
/// `Sec-WebSocket-Accept` is not validated, so a fixed value is fine here.
const HANDSHAKE: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\n\
    upgrade: websocket\r\n\
    connection: upgrade\r\n\
    sec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
    sec-websocket-extensions: permessage-deflate\r\n\
    \r\n";

fuzz_target!(|data: &[u8]| {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async move {
        let (client_io, mut peer) = tokio::io::duplex(64 * 1024);

        // The peer reads the request, writes the handshake, then the fuzz input as the
        // frame stream, then hangs up. Reading first matters: hyper rejects a response
        // that arrives before it has sent its request, which would fail every run
        // before the input is ever parsed. Writing concurrently keeps a large input
        // from filling the duplex.
        let data = data.to_vec();
        tokio::spawn(async move {
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                match peer.read(&mut byte).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => request.push(byte[0]),
                }
            }

            if peer.write_all(HANDSHAKE).await.is_err() {
                return;
            }
            let _ = peer.write_all(&data).await;
            let _ = peer.shutdown().await;
        });

        let url = "ws://localhost/".parse().expect("url");
        let options = Options::default().with_compression_level(Default::default());

        let Ok(mut ws) = WebSocket::handshake(url, client_io, options).await else {
            return;
        };

        // Read until the connection errors out or the peer's bytes run out.
        while ws.next_frame().await.is_ok() {}
    });
});
