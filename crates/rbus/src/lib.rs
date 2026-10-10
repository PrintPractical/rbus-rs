//! Async Rust client and provider for RDK's RBUS.
//!
//! `rbus` talks to the `rtrouted` broker over its unix socket using the native
//! wire protocol: no C library, no FFI, and no `unsafe` in this crate.
#![forbid(unsafe_code)]
