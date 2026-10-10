//! A provider's link to the engine once it is registered.

use std::fmt;
use std::sync::{Arc, Weak};

use crate::conn::Core;
use crate::error::{Error, Result, Status};
use crate::name::{IntoName, Name, TablePath};
use crate::provider::events::{SubscriberInfo, SubscriptionEvents};
use crate::provider::model::MountId;
use crate::subscribe::RowId;
use crate::types::IntoValue;

/// What a registered provider uses to publish events and manage its rows
/// from anywhere: its own handlers, the app, or other tasks (P4, P9, PT3).
///
/// [`Provider::attach`](crate::provider::Provider::attach) hands one to the
/// provider when it is registered; [`Provided::handle`](crate::provider::Provided::handle)
/// returns another. It is cheap to clone. It doesn't keep the connection or
/// the provider alive: once the provider is dropped, every call fails with
/// [`Status::ElementDoesNotExist`] (or [`Error::Disconnected`] once the
/// connection is gone), and `publish` calls do nothing.
#[derive(Clone)]
pub struct Handle {
    core: Weak<Core>,
    id: MountId,
}

impl Handle {
    pub(crate) fn new(core: &Arc<Core>, id: MountId) -> Self {
        Handle {
            core: Arc::downgrade(core),
            id,
        }
    }

    fn core(&self) -> Result<Arc<Core>> {
        self.core
            .upgrade()
            .filter(|c| !c.is_closed())
            .ok_or(Error::Disconnected)
    }

    /// Publishes a general event to its subscribers (P4). `data` is any
    /// serializable value that converts to an object, such as a struct or
    /// an [`Object`](crate::Object). With no subscribers it does nothing
    /// (D10).
    ///
    /// Fails with [`Status::ElementDoesNotExist`] unless `event` is an event
    /// of this provider's schema (rows included), and with
    /// [`Error::Type`] if `data` isn't an object.
    pub fn publish<T: serde::Serialize + ?Sized>(&self, event: &str, data: &T) -> Result<()> {
        let data = crate::convert::to_object(data)?;
        let core = self.core()?;
        core.registry
            .publish_event(self.id, &*core, event, data)
            .map_err(|s| element_error(event, s))
    }

    /// Publishes a change of a property's value that the provider made
    /// itself (P9, P11): subscribers get `{value, oldValue, by}`, with `by`
    /// our component, and filters apply. Nothing is sent if the values are
    /// equal or nobody is subscribed.
    pub fn publish_value_change(
        &self,
        property: &str,
        old: impl IntoValue,
        new: impl IntoValue,
    ) -> Result<()> {
        let core = self.core()?;
        core.registry
            .publish_change(
                self.id,
                &*core,
                property,
                old.into_value(),
                new.into_value(),
            )
            .map_err(|s| element_error(property, s))
    }

    /// Reads a property through [`Provider::get`](crate::provider::Provider::get)
    /// and, if its value differs from the last one its subscribers saw,
    /// publishes the change. Use it when the provider knows a value changed
    /// but not the old value.
    pub async fn notify_changed(&self, property: &str) -> Result<()> {
        let core = self.core()?;
        core.registry
            .notify_changed(self.id, &*core, property)
            .await
            .map_err(|s| element_error(property, s))
    }

    /// Adds a row the provider owns to one of its tables (PT3) and publishes
    /// `OBJECT_CREATED` (PT9). `table` is the table's path with instance
    /// numbers for any outer rows: `Device.WiFi.Radio.` or
    /// `Device.Bridge.2.Port.`.
    ///
    /// Fails with [`Status::ElementDoesNotExist`] if the table (or an outer
    /// row) doesn't exist, [`Status::AccessNotAllowed`] if another provider
    /// owns it, and [`Status::ElementNameDuplicate`] if the instance number
    /// or alias is taken.
    pub fn add_row(&self, table: impl IntoName<TablePath>, row: impl Into<RowId>) -> Result<()> {
        let table = table.into_name()?;
        let core = self.core()?;
        core.registry
            .add_local_row(self.id, &table, row.into(), &*core)
            .map_err(|status| Error::Status {
                name: table.into(),
                status,
            })
    }

    /// Removes a row, and every row of the tables inside it, and publishes
    /// `OBJECT_DELETED`.
    pub fn remove_row(&self, table: impl IntoName<TablePath>, inst: u32) -> Result<()> {
        let table = table.into_name()?;
        let core = self.core()?;
        core.registry
            .remove_local_row(self.id, &table, inst, &*core)
            .map_err(|status| Error::Status {
                name: table.into(),
                status,
            })
    }

    /// The rows of a table, in the order they were added.
    pub fn rows(&self, table: impl IntoName<TablePath>) -> Result<Vec<RowId>> {
        let table = table.into_name()?;
        let core = self.core()?;
        core.registry.rows(&table).map_err(|status| Error::Status {
            name: table.into(),
            status,
        })
    }

    /// The current subscribers to this provider's elements (P8). With a
    /// name, only the subscriptions that cover it: an exact subscription,
    /// or a pattern (`*`, `[alias]`) that matches it now. Empty once the
    /// connection is gone.
    pub fn subscribers(&self, name: Option<&str>) -> Vec<SubscriberInfo> {
        match self.core() {
            Ok(core) => core.registry.subscribers(self.id, name),
            Err(_) => Vec::new(),
        }
    }

    /// A stream of consumers subscribing and unsubscribing (P8). It ends
    /// when the provider is dropped; it is empty if it already was.
    pub fn subscription_events(&self) -> SubscriptionEvents {
        match self.core() {
            Ok(core) => core.registry.listen(self.id),
            Err(_) => SubscriptionEvents::ended(),
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("connected", &self.core().is_ok())
            .finish_non_exhaustive()
    }
}

/// An error about one of the provider's elements.
pub(crate) fn element_error(name: &str, status: Status) -> Error {
    match Name::parse(name.to_owned()) {
        Ok(name) => Error::Status { name, status },
        Err(e) => Error::Name(e),
    }
}
