# rbus-wire

Codecs for the wire protocol of RDK's RBUS broker, `rtrouted`: message framing, msgpack payloads, value records, events and the broker's JSON control messages.

It does no I/O and needs no async runtime. It turns bytes into typed frames and back, and every C wire quirk lives here. Decoders take configurable `Limits`, so a hostile peer can't exhaust memory or the stack.

Most users want [`rbus`](https://crates.io/crates/rbus), the async client and provider built on it.

Part of [rbus-rs](https://github.com/PrintPractical/rbus-rs). Licensed under Apache-2.0.
