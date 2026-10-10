//! Serving a data model to the bus.
//!
//! There are three ways to write a provider, and all of them are a
//! [`Provider`] in the end (decisions.md D3):
//!
//! - `#[derive(rbus::Model)]` on a struct of [`Prop`]s, [`Table`]s and
//!   [`Event`]s: rbus serves the stored state and publishes its changes.
//! - `#[rbus::provider(path = "..")]` on an impl block, for values computed
//!   on demand and for methods.
//! - [`DataModel`], a builder of closures for trees known only at run time.
//!
//! ```no_run
//! use rbus::provider::{Prop, Table};
//!
//! #[derive(rbus::Model, Default)]
//! #[rbus(path = "Device.Hosts.")]
//! struct Hosts {
//!     #[rbus(writable)]
//!     enable: Prop<bool>,                 // Device.Hosts.Enable
//!     #[rbus(name = "Host")]
//!     hosts: Table<Host>,                 // Device.Hosts.Host.{i}.
//! }
//!
//! #[derive(rbus::Row, Clone, PartialEq, Default)]
//! struct Host {
//!     name: String,                       // Device.Hosts.Host.{i}.Name
//! }
//!
//! # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
//! let hosts = bus.provide(Hosts::default()).await?;
//! hosts.enable.set(true);                 // published to subscribers
//! hosts.hosts.insert(Host { name: "printer".into() })?;
//! # Ok(()) }
//! ```
//!
//! Or implement [`Provider`] by hand and hand it to [`Rbus::provide`](crate::Rbus::provide).
//! The engine registers the [`Schema`]'s names with the broker and answers
//! every request for them: gets, sets (as one [`Transaction`]), method calls,
//! table rows and element listings. It also does what C leaves to provider
//! authors:
//!
//! - **Names.** Partial paths (`Device.WiFi.`), wildcards (`Device.Host.*.Name`)
//!   and aliases (`Host.[lan].Name`) are resolved against the schema and the
//!   table rows, so [`Provider::get`] only ever sees a single property with
//!   instance numbers (PT8).
//! - **Rows.** The engine keeps each table's rows. Consumers add and remove
//!   rows through [`Provider::add_row`] and [`Provider::remove_row`]; the
//!   provider adds its own with [`Provided::add_row`] (PT2, PT3).
//! - **Checks.** Unknown names, sets of read-only properties and calls to
//!   missing rows are answered from the schema, before the provider runs.
//! - **Concurrency.** Every request runs as its own task (P2), unless the
//!   provider is registered [`serialized`](Provide::serialized).
//!
//! ```no_run
//! use rbus::provider::{Access, Context, Fault, Provider, Schema};
//! use rbus::{PropertyName, Rbus, Value};
//! use std::sync::atomic::{AtomicU32, Ordering};
//!
//! struct Counter(AtomicU32);
//!
//! impl Provider for Counter {
//!     fn schema(&self) -> Schema {
//!         Schema::new().property("Device.Counter.Value", Access::ReadOnly)
//!     }
//!
//!     async fn get(&self, _cx: &Context, _name: &PropertyName) -> Result<Value, Fault> {
//!         Ok(Value::U32(self.0.fetch_add(1, Ordering::Relaxed)))
//!     }
//! }
//!
//! # async fn demo() -> rbus::Result<()> {
//! let bus = Rbus::connect("com.example.counter").await?;
//! let counter = bus.provide(Counter(AtomicU32::new(0))).await?;   // Provided<Counter>
//! // ... dropping `counter` unregisters it (P3).
//! # Ok(()) }
//! ```

mod context;
mod data_model;
mod derive;
mod engine;
mod events;
mod fault;
mod handle;
mod model;
mod provided;
mod schema;
mod state;

use std::future::Future;

use rbus_wire::value::{Object, Value};

pub use context::{Change, Context, Transaction};
pub use data_model::DataModel;
pub use derive::{BoxFuture, Model, Row, relative, value_as};
pub use events::{NoStore, SubscriberInfo, SubscriberStore, SubscriptionEvent, SubscriptionEvents};
pub use fault::{Fault, SetFault};
pub use handle::Handle;
pub use provided::{Provide, Provided};
#[cfg(feature = "macros")]
pub use rbus_macros::{Model, Row};
pub use rbus_wire::event::{Filter, RelationOp};
pub use schema::{Access, Element, ElementKind, Rows, Schema};
pub use state::{Event, Prop, Table};

pub(crate) use engine::{Registry, serve};
pub(crate) use handle::element_error;

use crate::name::{MethodName, PropertyName, TablePath};
use crate::subscribe::RowId;

/// A data model served on the bus (decisions.md D3). The derive and
/// attribute macros generate implementations of this trait; you can also
/// write one by hand.
///
/// Implement the methods with `async fn`. Only [`Provider::schema`] and
/// [`Provider::get`] are required; the others fail by default, as a C
/// provider without the matching handler does.
///
/// Every name a method receives is canonical: it exists in the schema, its
/// rows exist, and aliases are replaced by instance numbers
/// ([`Context::instances`] has them as numbers).
pub trait Provider: Send + Sync + 'static {
    /// The elements to register. Called once, by [`Rbus::provide`](crate::Rbus::provide).
    fn schema(&self) -> Schema;

    /// Reads a property. Partial-path and wildcard gets call this once per
    /// property; a property that fails there is left out of the result, as
    /// with C providers.
    fn get(
        &self,
        cx: &Context,
        name: &PropertyName,
    ) -> impl Future<Output = Result<Value, Fault>> + Send;

    /// Applies one consumer's batch of sets, all or nothing. Every name is a
    /// writable property of the schema.
    fn set(
        &self,
        cx: &Context,
        tx: Transaction,
    ) -> impl Future<Output = Result<(), SetFault>> + Send {
        let _ = (cx, tx);
        async { Err(SetFault::not_writable()) }
    }

    /// Calls a method. The input arguments are an [`Object`]; use
    /// [`from_object`](crate::from_object) and [`to_object`](crate::to_object)
    /// for typed arguments. A [`Fault`] reaches C consumers as `error_code`
    /// and `error_string` in the output.
    fn invoke(
        &self,
        cx: &Context,
        method: &MethodName,
        args: Object,
    ) -> impl Future<Output = Result<Object, Fault>> + Send {
        let _ = (cx, method, args);
        async { Err(Fault::invalid_method()) }
    }

    /// A consumer adds a row to a [`Rows::Dynamic`] table (PT2). Return the
    /// new row's instance number; the engine records the row with its alias.
    /// An alias already in use is refused before this is called.
    fn add_row(
        &self,
        cx: &Context,
        table: &TablePath,
        alias: Option<&str>,
    ) -> impl Future<Output = Result<u32, Fault>> + Send {
        let _ = (cx, table, alias);
        async { Err(Fault::not_allowed()) }
    }

    /// A consumer removes a row of a [`Rows::Dynamic`] table. On success the
    /// engine forgets the row and every row of the tables inside it.
    fn remove_row(
        &self,
        cx: &Context,
        table: &TablePath,
        row: &RowId,
    ) -> impl Future<Output = Result<(), Fault>> + Send {
        let _ = (cx, table, row);
        async { Err(Fault::not_allowed()) }
    }

    /// A consumer subscribes to one of the provider's elements. Return an
    /// error to refuse; the consumer gets its status, as C's
    /// `eventSubHandler` allows. Accepts by default. Unsubscribes and
    /// disconnects aren't asked; watch them with
    /// [`Provided::subscription_events`].
    fn on_subscribe(
        &self,
        cx: &Context,
        subscriber: &SubscriberInfo,
    ) -> impl Future<Output = Result<(), Fault>> + Send {
        let _ = (cx, subscriber);
        async { Ok(()) }
    }

    /// Called once, when the provider is registered and before the broker
    /// routes any request to it, with the handle it publishes events and
    /// adds rows through. Keep a clone to publish from handlers or other
    /// tasks. Does nothing by default; `#[derive(Model)]` uses it to connect
    /// its [`Prop`]s, [`Table`]s and [`Event`]s.
    fn attach(&self, handle: Handle) {
        let _ = handle;
    }
}
