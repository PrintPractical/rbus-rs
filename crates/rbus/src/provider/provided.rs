//! Registering a provider, and the guard that keeps it registered.

use std::fmt;
use std::future::{Future, IntoFuture};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use tracing::{Instrument, debug, debug_span};

use crate::bus::Rbus;
use crate::error::{Error, Result, Status};
use crate::name::{IntoName, TablePath};
use crate::provider::Provider;
use crate::provider::engine::DynProvider;
use crate::provider::events::{NoStore, SubscriberInfo, SubscriberStore, SubscriptionEvents};
use crate::provider::handle::{Handle, element_error};
use crate::provider::model::{MountId, SchemaError};
use crate::subscribe::RowId;
use crate::types::IntoValue;

impl Rbus {
    /// Registers `provider` and serves its data model until the returned
    /// [`Provided`] is dropped (P1, P3).
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus, model: impl rbus::provider::Provider) -> rbus::Result<()> {
    /// let provided = bus.provide(model).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Several providers can share one connection. Registration fails with
    /// [`Status::ElementNameDuplicate`] if a name is taken, by this
    /// connection or another component, and with [`Error::Name`] if a schema
    /// name is malformed.
    pub fn provide<P: Provider>(&self, provider: P) -> Provide<P> {
        Provide {
            bus: self.clone(),
            provider,
            serialized: false,
            store: Arc::new(NoStore),
        }
    }
}

/// A provider being registered; await it. Made by [`Rbus::provide`].
#[must_use = "a provider is registered only when awaited"]
pub struct Provide<P> {
    bus: Rbus,
    provider: P,
    serialized: bool,
    store: Arc<dyn SubscriberStore>,
}

impl<P: Provider> Provide<P> {
    /// Serves this provider's requests one at a time, in arrival order, for
    /// code written for C's single callback thread. By default each request
    /// runs as its own task.
    ///
    /// Each request is served whole before the next starts: a set's checks,
    /// handler and events, or every property of a partial-path get. The
    /// engine's own reads (value-change polling, `notify_changed`) don't wait
    /// their turn, so a handler may call them. Like C's callback thread, a
    /// handler must not wait on a request to its own serialized provider:
    /// it would queue behind itself until it timed out.
    pub fn serialized(mut self) -> Self {
        self.serialized = true;
        self
    }

    /// Keeps the provider's subscribers in `store`, so they survive a
    /// restart (D11). Subscribers it held are restored at registration,
    /// unless their process is gone. The default keeps nothing.
    pub fn subscriber_store(mut self, store: impl SubscriberStore) -> Self {
        self.store = Arc::new(store);
        self
    }
}

fn schema_error(e: SchemaError) -> Error {
    match e {
        SchemaError::Name(e) => Error::Name(e),
        SchemaError::Duplicate(name) => status(&name, Status::ElementNameDuplicate),
        SchemaError::Conflict(name) => status(&name, Status::InvalidNamespace),
    }
}

fn status(name: &str, status: Status) -> Error {
    element_error(name, status)
}

impl<P: Provider> IntoFuture for Provide<P> {
    type Output = Result<Provided<P>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<Provided<P>>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let Provide {
                bus,
                provider,
                serialized,
                store,
            } = self;
            let schema = provider.schema();
            let serial = schema.is_serialized();
            let span = debug_span!("rbus.provide", elements = schema.elements().len());
            async move {
                let provider = Arc::new(provider);
                let conn = &bus.inner.conn;
                let names: Vec<String> = schema
                    .elements()
                    .iter()
                    .map(|e| e.name().to_owned())
                    .collect();
                let dyn_provider: Arc<dyn DynProvider> = provider.clone();
                let id = conn
                    .registry
                    .mount_with(schema, dyn_provider, serialized || serial, store)
                    .map_err(schema_error)?;
                let handle = Handle::new(conn, id);
                let provided = Provided {
                    provider,
                    bus: bus.clone(),
                    id,
                    handle: handle.clone(),
                    names: Vec::with_capacity(names.len()),
                };
                // Before the broker routes anything here, so models can
                // record the rows they start with.
                provided.provider.attach(handle);
                let provided = provided.register(names).await?;
                if conn.registry.restore(id, &***conn) {
                    crate::provider::ensure_poller(conn);
                }
                Ok(provided)
            }
            .instrument(span)
            .await
        })
    }
}

/// A registered provider (P3). Derefs to the provider. Dropping it removes
/// its names from the broker and its rows from the engine.
///
/// It holds a clone of the [`Rbus`] it was registered on, so the connection
/// stays open while it lives.
pub struct Provided<P> {
    provider: Arc<P>,
    bus: Rbus,
    id: MountId,
    handle: Handle,
    /// The element names claimed for this provider.
    names: Vec<String>,
}

impl<P> Provided<P> {
    /// Registers every name as an alias of the component, all requests in
    /// flight at once (`rbus_addElement`). On any failure, the aliases that
    /// did register are removed again, as `rbus_regDataElements` does.
    async fn register(mut self, names: Vec<String>) -> Result<Self> {
        // Held before anything is awaited: if this future is dropped, the
        // `Provided` releases every claimed name.
        self.names = names.clone();
        let results = self
            .bus
            .inner
            .conn
            .claim_aliases(names)
            .await
            .map_err(|_| Error::Disconnected)?;
        match results.into_iter().find_map(|(_, r)| r.err()) {
            // Dropping `self` releases the names, including those that did
            // register.
            Some(e) => Err(e),
            None => {
                debug!(names = self.names.len(), "provider registered");
                Ok(self)
            }
        }
    }

    /// The handle the provider got in [`Provider::attach`]: publishing and
    /// rows, without holding on to the `Provided`.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// The connection this provider is registered on.
    pub fn bus(&self) -> &Rbus {
        &self.bus
    }

    /// Adds a row the provider owns to one of its tables; see
    /// [`Handle::add_row`].
    pub fn add_row(&self, table: impl IntoName<TablePath>, row: impl Into<RowId>) -> Result<()> {
        self.handle.add_row(table, row)
    }

    /// Removes a row; see [`Handle::remove_row`].
    pub fn remove_row(&self, table: impl IntoName<TablePath>, inst: u32) -> Result<()> {
        self.handle.remove_row(table, inst)
    }

    /// The rows of a table, in the order they were added.
    pub fn rows(&self, table: impl IntoName<TablePath>) -> Result<Vec<RowId>> {
        self.handle.rows(table)
    }

    /// Publishes a general event to its subscribers (P4); see
    /// [`Handle::publish`].
    ///
    /// ```no_run
    /// # fn demo(provided: rbus::provider::Provided<impl rbus::provider::Provider>) -> rbus::Result<()> {
    /// #[derive(serde::Serialize)]
    /// struct Alarm { level: u32 }
    /// provided.publish("Device.WiFi.Alarm!", &Alarm { level: 3 })?;
    /// # Ok(()) }
    /// ```
    pub fn publish<T: serde::Serialize + ?Sized>(&self, event: &str, data: &T) -> Result<()> {
        self.handle.publish(event, data)
    }

    /// Publishes a change of a property's value that the provider made
    /// itself (P9); see [`Handle::publish_value_change`].
    pub fn publish_value_change(
        &self,
        property: &str,
        old: impl IntoValue,
        new: impl IntoValue,
    ) -> Result<()> {
        self.handle.publish_value_change(property, old, new)
    }

    /// Reads a property again and publishes the change, if any; see
    /// [`Handle::notify_changed`].
    pub async fn notify_changed(&self, property: &str) -> Result<()> {
        self.handle.notify_changed(property).await
    }

    /// The current subscribers to this provider's elements (P8); see
    /// [`Handle::subscribers`].
    pub fn subscribers(&self, name: Option<&str>) -> Vec<SubscriberInfo> {
        self.handle.subscribers(name)
    }

    /// A stream of consumers subscribing and unsubscribing (P8). It ends
    /// when this `Provided` is dropped.
    pub fn subscription_events(&self) -> SubscriptionEvents {
        self.handle.subscription_events()
    }
}

impl<P> Deref for Provided<P> {
    type Target = P;
    fn deref(&self) -> &P {
        &self.provider
    }
}

impl<P> Drop for Provided<P> {
    /// Removes the aliases (fire and forget, as `rbus_unregDataElements`
    /// does) and the provider's rows.
    fn drop(&mut self) {
        let conn = &self.bus.inner.conn;
        conn.release_aliases(&self.names);
        conn.registry.unmount(self.id);
    }
}

impl<P> fmt::Debug for Provided<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Provided")
            .field("provider", &std::any::type_name::<P>())
            .field("component", &self.bus.component())
            .field("names", &self.names.len())
            .finish()
    }
}
