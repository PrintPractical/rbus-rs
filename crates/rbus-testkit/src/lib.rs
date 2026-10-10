//! Test-only tooling for rbus-rs. Not published.
//!
//! - [`frame`]: splits a byte stream into rtmessage frames and reads the header
//!   fields. It is deliberately minimal and independent of `rbus-wire`, so the
//!   recorder can capture traffic before (and as a cross-check of) the real codec.
//! - [`record`]: the recording proxy used by the `rbus-record` binary to capture
//!   C ↔ rtrouted traffic as fixtures.
//! - [`broker`]: helpers for interop tests that need a running `rtrouted`.
#![forbid(unsafe_code)]

pub mod broker;
pub mod frame;
pub mod record;
