# Fuzzing

Coverage-guided fuzz targets, run with [cargo-fuzz]. They need a nightly toolchain,
so they are not part of `cargo make ci`.

```sh
cargo install cargo-fuzz
cargo make fuzz             # both targets, 5 minutes each
cargo make fuzz-frame       # or one at a time
cargo make fuzz-websocket
```

To run one directly, with your own limits:

```sh
cargo +nightly fuzz run websocket_read --fuzz-dir fuzz -- -max_total_time=600 -timeout=15
```

## Targets

- `frame_decode`: arbitrary bytes into the frame decoder, as both roles.
- `websocket_read`: arbitrary bytes into a `WebSocket` with permessage-deflate
  negotiated. Covers frame decoding, fragment assembly, decompression and UTF-8
  validation.

## What counts as a failure

Panics, but also hangs. `-timeout` makes libFuzzer report an input that stops making
progress, which is how [issue #40] would have been caught: a deflate stream ending in
a final block spun the inflate loop forever instead of returning.

Keep `-timeout` well above a normal run (a few milliseconds) so only real hangs trip
it. Reproduce a saved crash or hang with:

```sh
cargo +nightly fuzz run websocket_read --fuzz-dir fuzz fuzz/artifacts/websocket_read/<file>
```

The corpus under `fuzz/corpus/` is generated, not checked in.

There are also seeded, deterministic fuzz tests in `src/compression.rs`
(`test_fuzz_*`) that run as part of the normal suite. They cover the same properties
on every CI run, without nightly. This directory is for the deeper, longer runs.

[cargo-fuzz]: https://rust-fuzz.github.io/book/cargo-fuzz.html
[issue #40]: https://github.com/infinitefield/yawc/issues/40
