//! Tables as a consumer: add, remove and list rows (T1–T3).

use std::future::{Future, IntoFuture};
use std::pin::Pin;

use rbus_wire::body::{
    AddRowRequest, AddRowResponse, DeleteRowRequest, NamesMode, NamesRequest, RowNamesResponse,
    StatusResponse,
};
use rbus_wire::message::Method;
use tracing::{Instrument, debug_span};

use crate::bus::Rbus;
use crate::error::{Error, Result, Status};
use crate::name::{IntoName, Name, NameError, TablePath};

/// A table on the bus. Made by [`Rbus::table`].
///
/// ```no_run
/// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
/// let hosts = bus.table("Device.Hosts.Host.");
/// let row = hosts.add_row().alias("guest").await?;     // RowId { inst: 7, alias: Some("guest") }
/// for r in hosts.rows().await? { println!("{} {:?}", r.inst, r.alias); }
/// hosts.remove_row(&row).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct RemoteTable {
    bus: Rbus,
    path: Result<TablePath, NameError>,
}

/// A table row, by instance number and alias.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowId {
    /// Instance number.
    pub inst: u32,
    /// Alias, if the row has one. Removal events don't carry it.
    pub alias: Option<String>,
}

impl From<u32> for RowId {
    /// A row without an alias.
    fn from(inst: u32) -> Self {
        RowId { inst, alias: None }
    }
}

/// Which row to remove: an instance number or an alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowRef {
    /// By instance number: `Table.7.`.
    Inst(u32),
    /// By alias: `Table.[guest].`.
    Alias(String),
}

impl From<&str> for RowRef {
    fn from(alias: &str) -> Self {
        RowRef::Alias(alias.to_owned())
    }
}

impl From<String> for RowRef {
    fn from(alias: String) -> Self {
        RowRef::Alias(alias)
    }
}

impl From<u32> for RowRef {
    fn from(inst: u32) -> Self {
        RowRef::Inst(inst)
    }
}

impl From<&RowId> for RowRef {
    fn from(row: &RowId) -> Self {
        RowRef::Inst(row.inst)
    }
}

impl From<RowId> for RowRef {
    fn from(row: RowId) -> Self {
        RowRef::Inst(row.inst)
    }
}

fn check(status: i32, name: &str) -> Result<()> {
    let status = Status::from_wire(status);
    if status.is_success() {
        Ok(())
    } else {
        Err(Error::Status {
            name: Name::parse(name.to_owned())?,
            status,
        })
    }
}

impl Rbus {
    /// A handle on the table at `path` (ending in `.`). An invalid path is
    /// reported by the first operation.
    pub fn table(&self, path: impl IntoName<TablePath>) -> RemoteTable {
        RemoteTable {
            bus: self.clone(),
            path: path.into_name(),
        }
    }
}

impl RemoteTable {
    fn path(&self) -> Result<&TablePath> {
        self.path.as_ref().map_err(|e| Error::Name(e.clone()))
    }

    /// Adds a row (T1). Await the builder; set an alias with `.alias(..)`.
    pub fn add_row(&self) -> AddRow<'_> {
        AddRow {
            table: self,
            alias: None,
        }
    }

    /// Removes a row by instance number or alias (T2).
    pub async fn remove_row(&self, row: impl Into<RowRef>) -> Result<()> {
        let path = self.path()?;
        let row_name = match row.into() {
            RowRef::Inst(i) => format!("{path}{i}."),
            RowRef::Alias(a) => format!("{path}[{a}]."),
        };
        let body = DeleteRowRequest {
            session_id: 0,
            row: row_name.clone(),
        };
        let resp: StatusResponse = self
            .bus
            .start_call(
                &row_name,
                Method::DeleteTableRow,
                &body,
                self.bus.inner.timeouts.set,
                "remove row",
                Name::parse(row_name.clone())?,
            )?
            .instrument(debug_span!("rbus.remove_row", row = %row_name))
            .await?;
        check(resp.status, &row_name)
    }

    /// The table's rows: instance numbers and aliases (T3).
    pub async fn rows(&self) -> Result<Vec<RowId>> {
        let path = self.path()?;
        let body = NamesRequest {
            name: path.to_string(),
            depth: -1,
            mode: NamesMode::Rows,
        };
        let resp: RowNamesResponse = self
            .bus
            .start_call(
                path,
                Method::GetParameterNames,
                &body,
                self.bus.inner.timeouts.get,
                "list rows",
                path.clone().into(),
            )?
            .instrument(debug_span!("rbus.rows", table = %path))
            .await?;
        check(resp.status, path)?;
        Ok(resp
            .rows
            .into_iter()
            .map(|r| RowId {
                inst: r.instance,
                alias: r.alias,
            })
            .collect())
    }
}

/// A row being added; await it. Made by [`RemoteTable::add_row`].
#[must_use = "a row is added only when awaited"]
#[derive(Debug)]
pub struct AddRow<'a> {
    table: &'a RemoteTable,
    alias: Option<String>,
}

impl AddRow<'_> {
    /// Gives the new row an alias, so it can be named `Table.[alias].`.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
}

impl<'a> IntoFuture for AddRow<'a> {
    type Output = Result<RowId>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<RowId>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let path = self.table.path()?;
            let bus = &self.table.bus;
            let body = AddRowRequest {
                session_id: 0,
                table: path.to_string(),
                alias: self.alias.clone(),
            };
            let resp: AddRowResponse = bus
                .start_call(
                    path,
                    Method::AddTableRow,
                    &body,
                    bus.inner.timeouts.set,
                    "add row",
                    path.clone().into(),
                )?
                .instrument(debug_span!("rbus.add_row", table = %path))
                .await?;
            check(resp.status, path)?;
            let inst = resp.instance.ok_or_else(|| {
                Error::Protocol(format!("{path}: add-row reply without an instance number"))
            })?;
            Ok(RowId {
                inst,
                alias: self.alias,
            })
        })
    }
}
