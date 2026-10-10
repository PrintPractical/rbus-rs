//! The traits `#[derive(Model)]` and `#[derive(Row)]` implement. The
//! derives only describe a struct's members; the work of serving
//! them is done here, in the provided methods.
//!
//! Members are addressed by paths relative to their struct: `Radio.3.Channel`
//! below `Device.WiFi.` is `["Radio", "3", "Channel"]`.

use std::future::Future;
use std::pin::Pin;

use rbus_wire::value::Value;
use tracing::debug;

use crate::name::{PropertyName, TablePath};
use crate::provider::{Context, Fault, Handle, Schema, SetFault, Transaction};
use crate::table::RowId;
use crate::types::RbusType;

/// A boxed future, as the derive traits' async methods return.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The segments of `name` below `prefix`: `relative("Device.WiFi.",
/// "Device.WiFi.Radio.3.Channel")` is `["Radio", "3", "Channel"]`. A
/// trailing `.` is dropped, so a table path gives the table's segments.
///
/// Fails with [`Fault::not_found`] if `name` isn't below `prefix`.
pub(crate) fn relative<'a>(prefix: &str, name: &'a str) -> Result<Vec<&'a str>, Fault> {
    let rest = name
        .strip_prefix(prefix)
        .filter(|r| !r.is_empty())
        .ok_or_else(Fault::not_found)?;
    Ok(rest.strip_suffix('.').unwrap_or(rest).split('.').collect())
}

/// Converts a consumer's value to a member's type, failing with
/// `InvalidParameterType`.
pub fn value_as<T: RbusType>(value: &Value) -> Result<T, Fault> {
    T::from_value(value.clone()).map_err(|e| Fault::invalid_type(e.to_string()))
}

/// A struct of [`Prop`](crate::provider::Prop)s, [`Table`](crate::provider::Table)s,
/// [`Event`](crate::provider::Event)s and nested models, served from its
/// stored state. Implemented by `#[derive(rbus::Model)]`, which also
/// implements [`Provider`](crate::provider::Provider) when the struct has a
/// `path`.
///
/// The required methods dispatch on a path relative to the model; the
/// provided ones serve a [`Provider`](crate::provider::Provider) with them.
pub trait Model: Send + Sync + 'static {
    /// Adds the model's elements to `schema`, named below `prefix` (which
    /// ends in `.`), in field order.
    fn describe(schema: Schema, prefix: &str) -> Schema
    where
        Self: Sized;

    /// Reads the property at `path`.
    fn get(&self, path: &[&str]) -> Result<Value, Fault>;

    /// Checks a consumer's new value for the property at `path`: its type
    /// and the field's `validate` function. With `apply`, also stores it,
    /// without publishing (the engine publishes consumers' changes).
    fn set(&self, path: &[&str], value: &Value, apply: bool) -> Result<(), Fault>;

    /// A consumer adds a row to the table at `path`.
    fn add_row<'a>(
        &'a self,
        cx: &'a Context,
        path: &'a [&'a str],
        alias: Option<&'a str>,
    ) -> BoxFuture<'a, Result<u32, Fault>>;

    /// A consumer removes a row of the table at `path`.
    fn remove_row<'a>(
        &'a self,
        cx: &'a Context,
        path: &'a [&'a str],
        row: &'a RowId,
    ) -> BoxFuture<'a, Result<(), Fault>>;

    /// Connects every member to its name below `prefix`, or disconnects
    /// them with `None`.
    fn bind(&self, prefix: &str, handle: Option<&Handle>);

    /// [`Provider::get`](crate::provider::Provider::get) for a model at `prefix`.
    fn get_at(&self, prefix: &str, name: &PropertyName) -> Result<Value, Fault> {
        self.get(&relative(prefix, name)?)
    }

    /// Checks every change of a transaction, naming the first that fails.
    fn check_transaction(&self, prefix: &str, tx: &Transaction) -> Result<(), SetFault> {
        for c in tx {
            relative(prefix, c.name())
                .and_then(|path| self.set(&path, c.value(), false))
                .map_err(|f| SetFault::at(c.name(), f))?;
        }
        Ok(())
    }

    /// Stores every change of a checked transaction.
    fn apply_transaction(&self, prefix: &str, tx: &Transaction) {
        for c in tx {
            let applied = relative(prefix, c.name()).and_then(|p| self.set(&p, c.value(), true));
            // Only a row removed since the check can fail here.
            if let Err(fault) = applied {
                debug!(name = %c.name(), %fault, "change not applied");
            }
        }
    }

    /// [`Provider::set`](crate::provider::Provider::set) for a model at
    /// `prefix`: the whole transaction is checked before anything is
    /// stored, so a failure changes nothing.
    fn set_at(&self, prefix: &str, tx: &Transaction) -> Result<(), SetFault> {
        self.check_transaction(prefix, tx)?;
        self.apply_transaction(prefix, tx);
        Ok(())
    }

    /// [`Provider::add_row`](crate::provider::Provider::add_row) for a model at `prefix`.
    fn add_row_at<'a>(
        &'a self,
        prefix: &'a str,
        cx: &'a Context,
        table: &'a TablePath,
        alias: Option<&'a str>,
    ) -> BoxFuture<'a, Result<u32, Fault>> {
        Box::pin(async move {
            let path = relative(prefix, table)?;
            self.add_row(cx, &path, alias).await
        })
    }

    /// [`Provider::remove_row`](crate::provider::Provider::remove_row) for a model at `prefix`.
    fn remove_row_at<'a>(
        &'a self,
        prefix: &'a str,
        cx: &'a Context,
        table: &'a TablePath,
        row: &'a RowId,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async move {
            let path = relative(prefix, table)?;
            self.remove_row(cx, &path, row).await
        })
    }
}

/// A table row: plain values, plus tables, events and sub-objects inside the
/// row. Implemented by `#[derive(rbus::Row)]`; the row is kept in a
/// [`Table`](crate::provider::Table).
///
/// A row type is also how a sub-object of a row is written
/// (`#[rbus(object)]`).
pub trait Row: Clone + PartialEq + Send + Sync + 'static {
    /// Adds the row's elements to `schema`, named below `prefix`
    /// (`Device.WiFi.Radio.{i}.`), in field order.
    fn describe(schema: Schema, prefix: &str) -> Schema
    where
        Self: Sized;

    /// Reads the value at `path`.
    fn get(&self, path: &[&str]) -> Result<Value, Fault>;

    /// Checks, and with `apply` stores, a consumer's value at `path`, as
    /// [`Model::set`] does.
    fn set(&mut self, path: &[&str], value: &Value, apply: bool) -> Result<(), Fault>;

    /// A consumer adds a row to a table inside this row.
    fn add_row<'a>(
        &'a self,
        cx: &'a Context,
        path: &'a [&'a str],
        alias: Option<&'a str>,
    ) -> BoxFuture<'a, Result<u32, Fault>> {
        let _ = (cx, path, alias);
        Box::pin(async { Err(Fault::not_found()) })
    }

    /// A consumer removes a row of a table inside this row.
    fn remove_row<'a>(
        &'a self,
        cx: &'a Context,
        path: &'a [&'a str],
        row: &'a RowId,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        let _ = (cx, path, row);
        Box::pin(async { Err(Fault::not_found()) })
    }

    /// Connects the tables and events inside the row to their names below
    /// `prefix` (`Device.WiFi.Radio.3.`), or disconnects them with `None`.
    fn bind(&self, prefix: &str, handle: Option<&Handle>) {
        let _ = (prefix, handle);
    }

    /// Adds `(path, old, new)` for every value that differs from `old`,
    /// with paths below `prefix`. Tables and events are skipped.
    fn diff(&self, old: &Self, prefix: &str, changes: &mut Vec<(String, Value, Value)>);

    /// True if a table or event inside the row is connected to a registered
    /// row already. Clones share them, so inserting a clone of a row would
    /// take them away from the original; [`Table::insert`] refuses that.
    ///
    /// [`Table::insert`]: crate::provider::Table::insert
    fn is_bound(&self) -> bool {
        false
    }
}
