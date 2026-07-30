#![no_main]

//! Feeds arbitrary bytes to the frame decoder.
//!
//! The decoder must either produce frames or return an error. It must never panic,
//! and it must never stop making progress: libFuzzer reports a non-terminating input
//! as a hang.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder as _;
use yawc::codec::Decoder;
use yawc::Role;

fuzz_target!(|data: &[u8]| {
    // Both roles: a server rejects unmasked frames, a client rejects masked ones,
    // so they take different paths through the decoder.
    for role in [Role::Server, Role::Client] {
        let mut decoder = Decoder::new(role, 1 << 20);
        let mut buf = BytesMut::from(data);

        // Drain until the decoder errors or asks for more bytes.
        while let Ok(Some(_frame)) = decoder.decode(&mut buf) {
            if buf.is_empty() {
                break;
            }
        }
    }
});
