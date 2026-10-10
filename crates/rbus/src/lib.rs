//! Async Rust client and provider for RDK's RBUS.
//!
//! `rbus` talks to the `rtrouted` broker over its unix socket using the native
//! wire protocol: no C library, no FFI, and no `unsafe` in this crate.
//!
//! ```no_run
//! use rbus::Rbus;
//!
//! # async fn demo() -> rbus::Result<()> {
//! let bus = Rbus::connect("com.example.app").await?;
//!
//! let uptime: u32 = bus.get("Device.DeviceInfo.UpTime").await?;
//! bus.set("Device.WiFi.Radio.1.Enable", true).await?;
//!
//! #[derive(serde::Serialize)]
//! struct Args { delay: u32 }
//! let () = bus.invoke("Device.Reboot()", Args { delay: 5 }).await?;
//!
//! bus.close().await?;
//! # Ok(()) }
//! ```
//!
//! - [`Rbus`] is the connection; it is `Clone` and shared between tasks.
//! - Names are typed by kind ([`PropertyName`], [`MethodName`], ...); plain
//!   strings work too and are checked at the call.
//! - Scalars convert through [`RbusType`]; structured data through serde
//!   ([`to_object`], [`from_object`]).
//! - Every failure is an [`Error`].
//! - To serve a data model, derive [`Model`] (or write a
//!   [`provider::Provider`] another way) and register it with
//!   [`Rbus::provide`].
//!
//! Each operation runs inside a `tracing` span (`rbus.get`, `rbus.set`, ...).
#![forbid(unsafe_code)]

// The macros name `::rbus::...`, which this makes work inside the crate too.
extern crate self as rbus;

mod bus;
mod conn;
mod consumer;
mod convert;
mod discover;
mod error;
pub mod name;
pub mod provider;
mod queue;
mod retry;
mod subscribe;
mod table;
mod tree;
mod types;

#[cfg(test)]
mod engine_tests;

// The README's examples compile as doctests.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
struct ReadmeDoctests;

pub use bus::{DEFAULT_SOCKET, Rbus, RbusBuilder, Timeouts};
pub use conn::ConnectionState;
pub use consumer::{SetMany, Values};
pub use convert::{from_object, from_value, to_object, to_value};
pub use discover::{Advisories, Advisory, Discover};
pub use error::{Error, Result, Status, TypeError};
pub use name::{EventName, IntoName, MethodName, Name, NameError, PropertyName, TablePath};
pub use queue::Overflow;
pub use rbus_wire::Limits;
pub use rbus_wire::value::{DateTime, Object, ObjectKind, Property, Value};
pub use retry::Retry;
pub use subscribe::{EventError, RowEvent, RowId, SubscribeBuilder, Subscription, ValueChange};
pub use table::{AddRow, RowRef, Table};
pub use tree::{Tree, TreeFailure};
pub use types::{IntoValue, RbusType};

/// Typed names checked while compiling; derives and the impl-block
/// attribute for providers (design.md 4.5, 4.8, 4.9).
#[cfg(feature = "macros")]
pub use rbus_macros::{Model, Row, name, provider};

/// The wire codecs this crate is built on.
pub use rbus_wire as wire;
