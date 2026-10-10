//! The state a `#[derive(Model)]` provider is made of: properties
//! ([`Prop`]), tables ([`Table`]) and events ([`Event`]). Each one serves
//! consumers from its stored state and publishes its own changes once the
//! model is registered (design.md 4.8).

use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use rbus_wire::value::Value;
use tracing::debug;

use crate::error::{Error, Result, Status};
use crate::provider::Handle;
use crate::provider::derive::Row;
use crate::provider::events::SubscriberInfo;
use crate::provider::fault::Fault;
use crate::provider::handle::element_error;
use crate::table::RowId;
use crate::types::RbusType;

/// Where a member lives once its model is registered.
#[derive(Clone)]
struct Bound {
    handle: Handle,
    name: String,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn read<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|e| e.into_inner())
}

fn write<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|e| e.into_inner())
}

/// A property whose value the model stores (P11). Consumers' gets read it,
/// their sets write it, and changes the app makes with [`Prop::set`] are
/// published to subscribers at once.
///
/// ```
/// use rbus::provider::Prop;
///
/// let enable = Prop::new(false);
/// enable.set(true);              // VALUE_CHANGED to subscribers, once registered
/// enable.update(|v| *v = !*v);
/// assert!(!enable.get());
/// ```
pub struct Prop<T> {
    value: RwLock<T>,
    bound: Mutex<Option<Bound>>,
}

impl<T> Prop<T> {
    /// A property holding `value`.
    pub const fn new(value: T) -> Self {
        Prop {
            value: RwLock::new(value),
            bound: Mutex::new(None),
        }
    }
}

impl<T: RbusType + Clone + PartialEq + Sync> Prop<T> {
    /// The current value.
    pub fn get(&self) -> T {
        read(&self.value).clone()
    }

    /// Calls `f` with the current value, without cloning it.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&read(&self.value))
    }

    /// Sets the value. If it changed and the model is registered, its
    /// subscribers get a value-change event, by our component.
    pub fn set(&self, value: T) {
        self.update(|v| *v = value);
    }

    /// Changes the value in place, then publishes the change as
    /// [`Prop::set`] does. Don't touch this property from `f`.
    pub fn update(&self, f: impl FnOnce(&mut T)) {
        let mut guard = write(&self.value);
        let old = guard.clone();
        f(&mut guard);
        if *guard == old {
            return;
        }
        // Still holding the value, so concurrent changes publish in order.
        if let Some(b) = lock(&self.bound).as_ref() {
            if let Err(e) = b.handle.publish_value_change(&b.name, old, guard.clone()) {
                debug!(name = %b.name, error = %e, "value change not published");
            }
        }
    }

    /// Sets the value without publishing anything. The derive macros store
    /// consumers' sets this way: the engine publishes those changes itself,
    /// by the setter.
    #[doc(hidden)]
    pub fn store(&self, value: T) {
        *write(&self.value) = value;
    }

    /// The value as an rbus [`Value`].
    pub fn value(&self) -> Value {
        self.get().into_value()
    }

    /// Connects the property to its registered name, or disconnects it with
    /// `None`. The derive macros call this from [`Provider::attach`](crate::provider::Provider::attach).
    #[doc(hidden)]
    pub fn bind(&self, name: impl Into<String>, handle: Option<&Handle>) {
        *lock(&self.bound) = handle.map(|h| Bound {
            handle: h.clone(),
            name: name.into(),
        });
    }
}

impl<T: Default> Default for Prop<T> {
    fn default() -> Self {
        Prop::new(T::default())
    }
}

impl<T> From<T> for Prop<T> {
    fn from(value: T) -> Self {
        Prop::new(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for Prop<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = lock(&self.bound).as_ref().map(|b| b.name.clone());
        f.debug_struct("Prop")
            .field("value", &*read(&self.value))
            .field("name", &name)
            .finish()
    }
}

/// An event the model publishes (P4). `T` is the data, any type that
/// serializes to an object.
///
/// Events carry no state: clones publish the same event, and any two are
/// equal, so an `Event` can sit in a [`Row`].
pub struct Event<T> {
    bound: Arc<Mutex<Option<Bound>>>,
    data: PhantomData<fn(T)>,
}

impl<T> Event<T> {
    /// An event, published once its model is registered.
    pub fn new() -> Self {
        Event {
            bound: Arc::default(),
            data: PhantomData,
        }
    }

    /// Connects the event to its registered name, or disconnects it with
    /// `None`. The derive macros call this from [`Provider::attach`](crate::provider::Provider::attach).
    #[doc(hidden)]
    pub fn bind(&self, name: impl Into<String>, handle: Option<&Handle>) {
        *lock(&self.bound) = handle.map(|h| Bound {
            handle: h.clone(),
            name: name.into(),
        });
    }

    fn bound(&self) -> Option<Bound> {
        lock(&self.bound).clone()
    }

    /// True once the event is connected to a registered name.
    #[doc(hidden)]
    pub fn is_bound(&self) -> bool {
        lock(&self.bound).is_some()
    }

    /// The consumers subscribed to this event (P8).
    pub fn subscribers(&self) -> Vec<SubscriberInfo> {
        self.bound()
            .map(|b| b.handle.subscribers(Some(&b.name)))
            .unwrap_or_default()
    }
}

impl<T: serde::Serialize> Event<T> {
    /// Publishes the event to its subscribers. With no subscribers, or
    /// before the model is registered, it does nothing (D10).
    ///
    /// Fails with [`Error::Type`] if `data` doesn't serialize to an object.
    pub fn publish(&self, data: impl Borrow<T>) -> Result<()> {
        match self.bound() {
            Some(b) => b.handle.publish(&b.name, data.borrow()),
            None => crate::convert::to_object(data.borrow())
                .map(drop)
                .map_err(Error::from),
        }
    }
}

impl<T> Default for Event<T> {
    fn default() -> Self {
        Event::new()
    }
}

impl<T> Clone for Event<T> {
    fn clone(&self) -> Self {
        Event {
            bound: self.bound.clone(),
            data: PhantomData,
        }
    }
}

impl<T> PartialEq for Event<T> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T> fmt::Debug for Event<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("name", &self.bound().map(|b| b.name))
            .finish()
    }
}

/// A table whose rows the model stores (PT1, PT3–PT5). App inserts and
/// consumers' `add_row` requests take the same path, so rows get the same
/// instance numbers, aliases and events either way.
///
/// ```
/// use rbus::provider::{Row, Table};
///
/// #[derive(rbus::Row, Clone, PartialEq, Default)]
/// struct Host {
///     name: String,
/// }
///
/// # fn main() -> rbus::Result<()> {
/// let hosts: Table<Host> = Table::new();
/// let id = hosts.insert(Host { name: "laptop".into() })?;      // OBJECT_CREATED, once registered
/// hosts.update(id, |h| h.name = "desktop".into())?;            // VALUE_CHANGED for Name
/// assert_eq!(hosts.get(id).unwrap().name, "desktop");
/// hosts.remove(id)?;                                            // OBJECT_DELETED
/// # Ok(()) }
/// ```
///
/// Instance numbers count up from 1 and are never reused, as in C. A
/// `Table` is a shared handle: clones are the same table, and two tables
/// are equal only if they are the same table. Don't touch a table from
/// inside its own [`Table::update`] or [`Table::with`] closure.
pub struct Table<R> {
    inner: Arc<RwLock<TableState<R>>>,
}

struct TableState<R> {
    rows: Vec<(RowId, R)>,
    next: u32,
    /// The table's path (`Device.WiFi.Radio.`) once registered.
    bound: Option<Bound>,
}

impl<R> Table<R> {
    /// An empty table.
    pub fn new() -> Self {
        Table {
            inner: Arc::new(RwLock::new(TableState {
                rows: Vec::new(),
                next: 1,
                bound: None,
            })),
        }
    }
}

impl<R: Row> TableState<R> {
    fn position(&self, inst: u32) -> Option<usize> {
        self.rows.iter().position(|(id, _)| id.inst == inst)
    }

    fn row_name(&self, inst: u32) -> String {
        match &self.bound {
            Some(b) => format!("{}{inst}.", b.name),
            None => inst.to_string(),
        }
    }

    fn insert(&mut self, alias: Option<String>, row: R) -> Result<u32> {
        if let Some(a) = &alias {
            if self.rows.iter().any(|(id, _)| id.alias.as_ref() == Some(a)) {
                return Err(element_error(a, Status::ElementNameDuplicate));
            }
        }
        // A clone of a row in a table shares its tables and events; this row
        // would take them over.
        if row.is_bound() {
            return Err(element_error(
                &self.row_name(self.next),
                Status::InvalidOperation,
            ));
        }
        let id = RowId {
            inst: self.next,
            alias,
        };
        if let Some(b) = &self.bound {
            match b.handle.add_row(b.name.as_str(), id.clone()) {
                Ok(()) => {}
                // Kept, as `Prop::set` keeps a value: the state is the
                // app's, whatever the connection does.
                Err(Error::Disconnected) => {
                    debug!(table = %b.name, inst = id.inst, "row kept; not connected");
                }
                Err(e) => return Err(e),
            }
            row.bind(&format!("{}{}.", b.name, id.inst), Some(&b.handle));
        }
        self.next += 1;
        let inst = id.inst;
        self.rows.push((id, row));
        Ok(inst)
    }

    fn take(&mut self, inst: u32) -> Option<R> {
        let i = self.position(inst)?;
        let (_, row) = self.rows.remove(i);
        row.bind("", None);
        Some(row)
    }
}

impl<R: Row> Table<R> {
    fn read(&self) -> RwLockReadGuard<'_, TableState<R>> {
        read(&self.inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, TableState<R>> {
        write(&self.inner)
    }

    /// Adds a row and returns its instance number. Once the model is
    /// registered, the row's subscribers get `OBJECT_CREATED` (PT9).
    pub fn insert(&self, row: R) -> Result<u32> {
        self.write().insert(None, row)
    }

    /// Adds a row with an alias, so consumers can name it `[alias]` (PT4).
    /// Fails with [`Status::ElementNameDuplicate`] if the alias is taken.
    pub fn insert_with_alias(&self, alias: impl Into<String>, row: R) -> Result<u32> {
        self.write().insert(Some(alias.into()), row)
    }

    /// Changes a row in place. Once the model is registered, every value
    /// that changed is published to its subscribers, by our component.
    /// Fails with [`Status::ElementDoesNotExist`] if there is no such row.
    pub fn update(&self, inst: u32, f: impl FnOnce(&mut R)) -> Result<()> {
        let mut state = self.write();
        let Some(i) = state.position(inst) else {
            return Err(element_error(
                &state.row_name(inst),
                Status::ElementDoesNotExist,
            ));
        };
        let row = &mut state.rows[i].1;
        let old = row.clone();
        f(row);
        let mut changes = Vec::new();
        row.diff(&old, "", &mut changes);
        if let Some(b) = &state.bound {
            let prefix = format!("{}{inst}.", b.name);
            for (name, old, new) in changes {
                let name = format!("{prefix}{name}");
                if let Err(e) = b.handle.publish_value_change(&name, old, new) {
                    debug!(%name, error = %e, "value change not published");
                }
            }
        }
        Ok(())
    }

    /// Removes a row, and the rows of the tables inside it, and returns it.
    /// Once the model is registered, subscribers get `OBJECT_DELETED`.
    /// Fails with [`Status::ElementDoesNotExist`] if there is no such row.
    pub fn remove(&self, inst: u32) -> Result<R> {
        let mut state = self.write();
        if state.position(inst).is_none() {
            return Err(element_error(
                &state.row_name(inst),
                Status::ElementDoesNotExist,
            ));
        }
        if let Some(b) = &state.bound {
            // The engine may have forgotten it already (its connection is
            // gone); the row still goes.
            if let Err(e) = b.handle.remove_row(b.name.as_str(), inst) {
                debug!(table = %b.name, inst, error = %e, "row removal not published");
            }
        }
        Ok(state.take(inst).expect("the row exists"))
    }

    /// A copy of a row. Tables inside it are shared, not copied.
    pub fn get(&self, inst: u32) -> Option<R> {
        self.with(inst, R::clone)
    }

    /// Calls `f` with a row, without copying it.
    pub fn with<T>(&self, inst: u32, f: impl FnOnce(&R) -> T) -> Option<T> {
        let state = self.read();
        state.position(inst).map(|i| f(&state.rows[i].1))
    }

    /// Copies of the rows, in the order they were added.
    pub fn rows(&self) -> Vec<(RowId, R)> {
        self.read().rows.clone()
    }

    /// The rows' instance numbers and aliases.
    pub fn ids(&self) -> Vec<RowId> {
        self.read().rows.iter().map(|(id, _)| id.clone()).collect()
    }

    /// The instance number of the row with this alias.
    pub fn find(&self, alias: &str) -> Option<u32> {
        self.read()
            .rows
            .iter()
            .find(|(id, _)| id.alias.as_deref() == Some(alias))
            .map(|(id, _)| id.inst)
    }

    /// The number of rows.
    pub fn len(&self) -> usize {
        self.read().rows.len()
    }

    /// True if the table has no rows.
    pub fn is_empty(&self) -> bool {
        self.read().rows.is_empty()
    }

    /// The consumers subscribed to the table's row events (P8).
    pub fn subscribers(&self) -> Vec<SubscriberInfo> {
        match &self.read().bound {
            Some(b) => b.handle.subscribers(Some(&b.name)),
            None => Vec::new(),
        }
    }

    // The rest is for the code `#[derive(Model)]` and `#[derive(Row)]`
    // generate.

    /// True once the table is connected to a registered path.
    #[doc(hidden)]
    pub fn is_bound(&self) -> bool {
        self.read().bound.is_some()
    }

    /// Connects the table to its registered path (`Device.WiFi.Radio.`) and
    /// records the rows it already has, or disconnects it with `None`.
    #[doc(hidden)]
    pub fn bind(&self, path: &str, handle: Option<&Handle>) {
        let mut state = self.write();
        state.bound = handle.map(|h| Bound {
            handle: h.clone(),
            name: path.to_owned(),
        });
        for (id, row) in &state.rows {
            let prefix = format!("{path}{}.", id.inst);
            if let Some(h) = handle {
                if let Err(e) = h.add_row(path, id.clone()) {
                    debug!(table = path, inst = id.inst, error = %e, "row not recorded");
                }
            }
            row.bind(&prefix, handle);
        }
    }

    /// The row `seg` names, as an instance number.
    fn inst(seg: Option<&&str>) -> Result<u32, Fault> {
        seg.and_then(|s| s.parse().ok())
            .ok_or_else(Fault::not_found)
    }

    /// Reads `path` below the table: an instance number, then the row's
    /// member.
    #[doc(hidden)]
    pub fn get_path(&self, path: &[&str]) -> Result<Value, Fault> {
        let inst = Self::inst(path.first())?;
        let state = self.read();
        let i = state.position(inst).ok_or_else(Fault::not_found)?;
        state.rows[i].1.get(&path[1..])
    }

    /// Checks or applies a consumer's set of `path` below the table.
    #[doc(hidden)]
    pub fn set_path(&self, path: &[&str], value: &Value, apply: bool) -> Result<(), Fault> {
        let inst = Self::inst(path.first())?;
        let mut state = self.write();
        let i = state.position(inst).ok_or_else(Fault::not_found)?;
        state.rows[i].1.set(&path[1..], value, apply)
    }

    /// A consumer adds `row` (PT2). The table records it with the engine,
    /// with the rows of the tables inside it.
    #[doc(hidden)]
    pub fn insert_for(&self, alias: Option<&str>, row: R) -> Result<u32, Fault> {
        self.write()
            .insert(alias.map(str::to_owned), row)
            .map_err(|e| match e.status() {
                Some(s) => Fault::new(s),
                None => Fault::internal(e),
            })
    }

    /// A consumer removes a row. The engine forgets it and publishes the
    /// event.
    #[doc(hidden)]
    pub fn remove_for(&self, row: &RowId) -> Result<(), Fault> {
        self.write()
            .take(row.inst)
            .map(drop)
            .ok_or_else(Fault::not_found)
    }

    /// A consumer adds a row to a table inside one of this table's rows:
    /// `path` is the row's instance number, then the path below it.
    #[doc(hidden)]
    pub fn add_row_below<'a>(
        &'a self,
        cx: &'a crate::provider::Context,
        path: &'a [&'a str],
        alias: Option<&'a str>,
    ) -> crate::provider::BoxFuture<'a, Result<u32, Fault>> {
        Box::pin(async move {
            let row = self
                .get(Self::inst(path.first())?)
                .ok_or_else(Fault::not_found)?;
            row.add_row(cx, &path[1..], alias).await
        })
    }

    /// A consumer removes a row of a table inside one of this table's rows.
    #[doc(hidden)]
    pub fn remove_row_below<'a>(
        &'a self,
        cx: &'a crate::provider::Context,
        path: &'a [&'a str],
        row: &'a RowId,
    ) -> crate::provider::BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async move {
            let outer = self
                .get(Self::inst(path.first())?)
                .ok_or_else(Fault::not_found)?;
            outer.remove_row(cx, &path[1..], row).await
        })
    }
}

impl<R> Default for Table<R> {
    fn default() -> Self {
        Table::new()
    }
}

impl<R> Clone for Table<R> {
    fn clone(&self) -> Self {
        Table {
            inner: self.inner.clone(),
        }
    }
}

impl<R> PartialEq for Table<R> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<R: fmt::Debug> fmt::Debug for Table<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = read(&self.inner);
        f.debug_struct("Table")
            .field("path", &state.bound.as_ref().map(|b| &b.name))
            .field("rows", &state.rows)
            .finish()
    }
}

#[cfg(test)]
mod tests;
