//! Codecs for the RBUS wire protocol spoken by `rtrouted`.
//!
//! This crate does no I/O and has no async runtime: it turns bytes into typed
//! frames and back. Every C wire quirk (NUL-terminated strings, raw native-memory
//! values, legacy CCSP codes) is confined here.
#![forbid(unsafe_code)]
