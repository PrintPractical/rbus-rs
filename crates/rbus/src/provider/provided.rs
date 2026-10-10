//! Registering a provider, and the guard that keeps it registered.

use std::fmt;
use std::future::{Future, IntoFuture};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use rbus_wire::control::{self, ResultReply, Route};
use tracing::{Instrument, debug, debug_span};

use crate::bus::Rbus;
use crate::error::{Error, Result, Status};
use crate::name::{IntoName, Name, TablePath};
use crate::provider::Provider;
use crate::provider::engine::DynProvider;
use crate::provider::model::{MountId, SchemaError};
use crate::subscribe::RowId;

/// The broker's answer to an alias that is already registered
/// (`RT_ERROR_DUPLICATE_ENTRY`).
const ALIAS_DUPLICATE: i32 = 1001;
/// The broker's answer to an alias that clashes with a table or property
/// (`RT_ERROR_PROTOCOL_ERROR`), which rbus maps to `INVALID_NAMESPACE`.
const ALIAS_CONFLICT: i32 = 1003;

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
        }
    }
}

/// A provider being registered; await it. Made by [`Rbus::provide`].
#[must_use = "a provider is registered only when awaited"]
pub struct Provide<P> {
    bus: Rbus,
    provider: P,
    serialized: bool,
}

impl<P: Provider> Provide<P> {
    /// Serves this provider's requests one at a time, in arrival order, for
    /// code written for C's single callback thread. By default each request
    /// runs as its own task.
    pub fn serialized(mut self) -> Self {
        self.serialized = true;
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
    match Name::parse(name.to_owned()) {
        Ok(name) => Error::Status { name, status },
        Err(e) => Error::Name(e),
    }
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
            } = self;
            let schema = provider.schema();
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
                    .mount(schema, dyn_provider, serialized)
                    .map_err(schema_error)?;
                let provided = Provided {
                    provider,
                    bus: bus.clone(),
                    id,
                    names: Vec::with_capacity(names.len()),
                };
                provided.register(names).await
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
    /// The aliases registered with the broker.
    names: Vec<String>,
}

impl<P> Provided<P> {
    /// Registers every name as an alias of the component, all requests in
    /// flight at once (`rbus_addElement`). On any failure, the aliases that
    /// did register are removed again, as `rbus_regDataElements` does.
    async fn register(mut self, names: Vec<String>) -> Result<Self> {
        let conn = &self.bus.inner.conn;
        let route = conn.component_route;
        let mut pending = Vec::with_capacity(names.len());
        for name in names {
            let request = conn.broker_request(control::TOPIC_SUBSCRIBE, &Route::add(&*name, route));
            pending.push((name, request));
        }
        let mut failure = None;
        for (name, request) in pending {
            let reply = match request {
                Ok(p) => p.wait(self.bus.inner.timeouts.broker, "register").await,
                Err(e) => Err(e),
            };
            let result =
                reply.and_then(|frame| Ok(control::decode::<ResultReply>(&frame.payload)?.result));
            match result {
                Ok(0) => self.names.push(name),
                Ok(code) => {
                    let s = match code {
                        ALIAS_DUPLICATE => Status::ElementNameDuplicate,
                        ALIAS_CONFLICT => Status::InvalidNamespace,
                        _ => Status::BusError,
                    };
                    failure.get_or_insert_with(|| status(&name, s));
                }
                Err(e) => {
                    failure.get_or_insert(e);
                }
            }
        }
        match failure {
            // Dropping `self` unregisters what did register.
            Some(e) => Err(e),
            None => {
                debug!(names = self.names.len(), "provider registered");
                Ok(self)
            }
        }
    }

    /// Adds a row the provider owns to one of its tables (PT3). `table` is
    /// the table's path with instance numbers for any outer rows:
    /// `Device.WiFi.Radio.` or `Device.Bridge.2.Port.`.
    ///
    /// Fails with [`Status::ElementDoesNotExist`] if the table (or an outer
    /// row) doesn't exist, [`Status::AccessNotAllowed`] if another provider
    /// owns it, and [`Status::ElementNameDuplicate`] if the instance number
    /// or alias is taken.
    pub fn add_row(&self, table: impl IntoName<TablePath>, row: impl Into<RowId>) -> Result<()> {
        let table = table.into_name()?;
        self.bus
            .inner
            .conn
            .registry
            .add_local_row(self.id, &table, row.into())
            .map_err(|status| Error::Status {
                name: table.into(),
                status,
            })
    }

    /// Removes a row, and every row of the tables inside it.
    pub fn remove_row(&self, table: impl IntoName<TablePath>, inst: u32) -> Result<()> {
        let table = table.into_name()?;
        self.bus
            .inner
            .conn
            .registry
            .remove_local_row(self.id, &table, inst)
            .map_err(|status| Error::Status {
                name: table.into(),
                status,
            })
    }

    /// The rows of a table, in the order they were added.
    pub fn rows(&self, table: impl IntoName<TablePath>) -> Result<Vec<RowId>> {
        let table = table.into_name()?;
        self.bus
            .inner
            .conn
            .registry
            .rows(&table)
            .map_err(|status| Error::Status {
                name: table.into(),
                status,
            })
    }

    /// The connection this provider is registered on.
    pub fn bus(&self) -> &Rbus {
        &self.bus
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
        if !conn.is_closed() {
            for name in &self.names {
                let _ = conn.send_control(&Route::remove(&**name, conn.component_route));
            }
        }
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
