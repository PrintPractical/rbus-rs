//! The provider engine: the registry of providers on a connection, and the
//! handling of every request the broker routes to them.
//!
//! Each request runs as its own task ([`serve`]), so a slow handler never
//! holds up others, and handlers may call back into the bus. The registry's
//! lock is only held for lookups, never across a handler.
//!
//! Replies follow `rbus.c` (`_callback_handler` and the handlers it calls),
//! so C consumers can't tell a Rust provider from a C one.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use rbus_wire::Limits;
use rbus_wire::body::{
    AddRowRequest, AddRowResponse, DeleteRowRequest, ElementNamesResponse, GetRequest, GetResponse,
    NamesMode, NamesRequest, RowName, RowNamesResponse, RpcRequest, RpcResponse, SetRequest,
    SetResponse, StatusResponse, SubscribeRequest,
};
use rbus_wire::frame::{Flags, Frame, Header};
use rbus_wire::message::{Body, Method, response_payload, split_method_payload};
use rbus_wire::msgpack::Reader;
use rbus_wire::value::{Object, Property, Value};
use tokio::sync::mpsc;
use tracing::{Instrument, debug, debug_span, trace, warn};

use crate::conn::Core;
use crate::error::Status;
use crate::name::{MethodName, PropertyName, TablePath};
use crate::provider::Provider;
use crate::provider::context::{Change, Context, Transaction};
use crate::provider::derive::BoxFuture;
use crate::provider::events::{SubscriberInfo, SubscriberStore, SubscriptionEvent};
use crate::provider::fault::{Fault, SetFault};
use crate::provider::model::{
    Found, Model, MountId, NodeKind, Resolved, SchemaError, Target, Tree,
};
use crate::provider::schema::{Rows, Schema};
use crate::queue::Queue;
use crate::table::RowId;

mod dispatch;
mod handlers;
mod subs;

use dispatch::Request;
pub(crate) use dispatch::{dispatch, ensure_poller};
use handlers::{name_of, wire_status};

pub(crate) use subs::poller;

/// Where the engine sends what it publishes. The connection implements it;
/// tests record what would be sent.
pub(crate) trait Outbox: Send + Sync {
    /// Our component name: events name it as the publisher.
    fn component(&self) -> &str;
    /// Sends an event message to a subscriber's inbox.
    fn send_event(&self, inbox: &str, payload: Vec<u8>);
    /// Makes sure the broker tells us when clients disconnect, so their
    /// subscriptions can be dropped.
    fn watch_disconnects(&self);
}

impl Outbox for Core {
    fn component(&self) -> &str {
        &self.component
    }
    fn send_event(&self, inbox: &str, payload: Vec<u8>) {
        // A failure means the connection is closing.
        let _ = self.send_message(inbox, payload);
    }
    fn watch_disconnects(&self) {
        let _ = self.ensure_advisories();
    }
}

/// `RBUSCORE_ERROR_UNSUPPORTED_METHOD`: what `rbus_sendResponse` answers for
/// a request no handler took (`rbuscore.c:2404`), and what we answer for
/// direct mode.
const UNSUPPORTED_METHOD: i32 = 9;

/// [`Provider`] made object safe, so providers of different types can share
/// the registry.
pub(crate) trait DynProvider: Send + Sync {
    fn get<'a>(
        &'a self,
        cx: &'a Context,
        name: &'a PropertyName,
    ) -> BoxFuture<'a, Result<Value, Fault>>;
    fn set<'a>(&'a self, cx: &'a Context, tx: Transaction) -> BoxFuture<'a, Result<(), SetFault>>;
    fn invoke<'a>(
        &'a self,
        cx: &'a Context,
        method: &'a MethodName,
        args: Object,
    ) -> BoxFuture<'a, Result<Object, Fault>>;
    fn add_row<'a>(
        &'a self,
        cx: &'a Context,
        table: &'a TablePath,
        alias: Option<&'a str>,
    ) -> BoxFuture<'a, Result<u32, Fault>>;
    fn remove_row<'a>(
        &'a self,
        cx: &'a Context,
        table: &'a TablePath,
        row: &'a RowId,
    ) -> BoxFuture<'a, Result<(), Fault>>;
    fn on_subscribe<'a>(
        &'a self,
        cx: &'a Context,
        subscriber: &'a SubscriberInfo,
    ) -> BoxFuture<'a, Result<(), Fault>>;
}

impl<P: Provider> DynProvider for P {
    fn get<'a>(
        &'a self,
        cx: &'a Context,
        name: &'a PropertyName,
    ) -> BoxFuture<'a, Result<Value, Fault>> {
        Box::pin(Provider::get(self, cx, name))
    }
    fn set<'a>(&'a self, cx: &'a Context, tx: Transaction) -> BoxFuture<'a, Result<(), SetFault>> {
        Box::pin(Provider::set(self, cx, tx))
    }
    fn invoke<'a>(
        &'a self,
        cx: &'a Context,
        method: &'a MethodName,
        args: Object,
    ) -> BoxFuture<'a, Result<Object, Fault>> {
        Box::pin(Provider::invoke(self, cx, method, args))
    }
    fn add_row<'a>(
        &'a self,
        cx: &'a Context,
        table: &'a TablePath,
        alias: Option<&'a str>,
    ) -> BoxFuture<'a, Result<u32, Fault>> {
        Box::pin(Provider::add_row(self, cx, table, alias))
    }
    fn remove_row<'a>(
        &'a self,
        cx: &'a Context,
        table: &'a TablePath,
        row: &'a RowId,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(Provider::remove_row(self, cx, table, row))
    }
    fn on_subscribe<'a>(
        &'a self,
        cx: &'a Context,
        subscriber: &'a SubscriberInfo,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(Provider::on_subscribe(self, cx, subscriber))
    }
}

/// A registered provider.
struct Mount {
    schema: Schema,
    handler: Handler,
    /// Streams of subscription changes.
    listeners: Vec<Weak<Queue<SubscriptionEvent>>>,
    store: Arc<dyn SubscriberStore>,
}

/// What serves a mount's requests.
#[derive(Clone)]
struct Handler {
    provider: Arc<dyn DynProvider>,
    /// Requests that reach it run one at a time, in arrival order.
    serialized: bool,
}

/// A set buffered until its legacy session commits.
struct Buffered {
    requested: String,
    owner: MountId,
    change: Change,
}

#[derive(Default)]
struct State {
    mounts: Vec<(MountId, Mount)>,
    model: Model,
    /// Uncommitted sets, by the caller's inbox and session id. Dropped when
    /// the caller disconnects.
    sessions: HashMap<(String, u32), Vec<Buffered>>,
    next_mount: MountId,
    /// Every subscription to every provider.
    subs: Vec<subs::Sub>,
    next_sub_id: u32,
    /// The last value subscribers saw of each watched property.
    values: HashMap<String, Value>,
    /// When each polled property is next due.
    poll_due: HashMap<String, tokio::time::Instant>,
    /// Mounts whose subscriber stores need saving.
    unsaved: Vec<MountId>,
}

impl State {
    fn handler(&self, owner: Option<MountId>) -> Option<Handler> {
        self.mount(owner?).map(|m| m.handler.clone())
    }

    fn rebuild(&self, extra: Option<(MountId, &Schema)>) -> Result<Tree, SchemaError> {
        Tree::build(
            self.mounts
                .iter()
                .map(|(id, m)| (*id, m.schema.elements()))
                .chain(extra.map(|(id, s)| (id, s.elements()))),
        )
    }
}

/// Every provider on one connection, and the rows of their tables.
pub(crate) struct Registry {
    state: Mutex<State>,
    /// Held while subscriber stores are saved.
    saving: Mutex<()>,
    poller_started: AtomicBool,
    /// The most set values one caller may leave uncommitted.
    max_buffered: usize,
    /// Feeds the worker that serves requests to serialized providers.
    serial: OnceLock<mpsc::UnboundedSender<Job>>,
}

/// A request being served.
type Job = BoxFuture<'static, ()>;

/// The locked state. Dropping it releases the lock, then saves the
/// subscriber stores that changed.
struct StateGuard<'a> {
    guard: Option<MutexGuard<'a, State>>,
    registry: &'a Registry,
}

impl std::ops::Deref for StateGuard<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        self.guard.as_ref().expect("held until dropped")
    }
}

impl std::ops::DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.guard.as_mut().expect("held until dropped")
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        let unsaved = self.guard.as_ref().is_some_and(|s| !s.unsaved.is_empty());
        self.guard = None;
        if unsaved {
            self.registry.flush_saves();
        }
    }
}

impl Default for Registry {
    fn default() -> Registry {
        Registry::new(&Limits::DEFAULT)
    }
}

impl Registry {
    /// A registry that holds at most `limits.max_items` uncommitted set
    /// values per caller.
    pub(crate) fn new(limits: &Limits) -> Registry {
        Registry {
            state: Mutex::default(),
            saving: Mutex::new(()),
            poller_started: AtomicBool::new(false),
            max_buffered: limits.max_items,
            serial: OnceLock::new(),
        }
    }
}

impl Registry {
    /// Locks the state. Stores marked for saving meanwhile are saved when
    /// the guard goes.
    fn state(&self) -> StateGuard<'_> {
        StateGuard {
            guard: Some(self.state.lock().unwrap_or_else(|e| e.into_inner())),
            registry: self,
        }
    }

    /// Saves the stores [`State::changed`] marked, outside the state lock,
    /// one save at a time and each from a fresh snapshot, so the last save
    /// is always the latest state. A save that finds another under way
    /// leaves its marks for that one.
    fn flush_saves(&self) {
        while let Ok(saving) = self.saving.try_lock() {
            let unsaved = std::mem::take(&mut self.lock_raw().unsaved);
            for id in unsaved {
                let snapshot = self.lock_raw().snapshot(id);
                if let Some((store, subscribers)) = snapshot {
                    store.save(&subscribers);
                }
            }
            drop(saving);
            if self.lock_raw().unsaved.is_empty() {
                return;
            }
        }
    }

    fn lock_raw(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Adds a provider. Fails if its schema is malformed or overlaps a
    /// provider already registered.
    #[cfg(test)]
    pub(crate) fn mount(
        &self,
        schema: Schema,
        provider: Arc<dyn DynProvider>,
        serialized: bool,
    ) -> Result<MountId, SchemaError> {
        self.mount_with(
            schema,
            provider,
            serialized,
            Arc::new(crate::provider::NoStore),
        )
    }

    /// [`Registry::mount`] with a subscriber store.
    pub(crate) fn mount_with(
        &self,
        schema: Schema,
        provider: Arc<dyn DynProvider>,
        serialized: bool,
        store: Arc<dyn SubscriberStore>,
    ) -> Result<MountId, SchemaError> {
        let mut state = self.state();
        let id = state.next_mount;
        let tree = state.rebuild(Some((id, &schema)))?;
        state.next_mount += 1;
        state.model.set_tree(tree);
        state.mounts.push((
            id,
            Mount {
                schema,
                handler: Handler {
                    provider,
                    serialized,
                },
                listeners: Vec::new(),
                store,
            },
        ));
        Ok(id)
    }

    /// Removes a provider, its rows, its subscribers and its buffered sets.
    /// Its subscription streams end.
    pub(crate) fn unmount(&self, id: MountId) {
        let mut state = self.state();
        state.subs.retain(|s| s.mount != Some(id));
        if let Some(m) = state.mount(id) {
            for q in m.listeners.iter().filter_map(Weak::upgrade) {
                q.close();
            }
        }
        state.mounts.retain(|(m, _)| *m != id);
        // A subset of a valid set of schemas is valid.
        let tree = state.rebuild(None).unwrap_or_default();
        state.model.set_tree(tree);
        for pending in state.sessions.values_mut() {
            pending.retain(|b| b.owner != id);
        }
        state.sessions.retain(|_, p| !p.is_empty());
        state.prune_values();
    }

    /// Adds a row on the provider's behalf and publishes
    /// `OBJECT_CREATED`.
    pub(crate) fn add_local_row(
        &self,
        id: MountId,
        table: &str,
        row: RowId,
        out: &dyn Outbox,
    ) -> Result<(), Status> {
        let mut state = self.state();
        let (t, r) = state.model.resolve_table(table)?;
        if state.model.tree.node(t).owner.is_some_and(|o| o != id) || state.mount(id).is_none() {
            return Err(Status::AccessNotAllowed);
        }
        state.model.add_row(&r.canonical, row.clone())?;
        state.row_event(out, &r.canonical, &row, true);
        Ok(())
    }

    /// Removes a row on the provider's behalf.
    pub(crate) fn remove_local_row(
        &self,
        id: MountId,
        table: &str,
        inst: u32,
        out: &dyn Outbox,
    ) -> Result<(), Status> {
        let mut state = self.state();
        let (t, r) = state.model.resolve_table(table)?;
        if state.model.tree.node(t).owner.is_some_and(|o| o != id) || state.mount(id).is_none() {
            return Err(Status::AccessNotAllowed);
        }
        state
            .remove_row(out, &r.canonical, inst)
            .ok_or(Status::ElementDoesNotExist)?;
        Ok(())
    }

    /// The rows of a table.
    pub(crate) fn rows(&self, table: &str) -> Result<Vec<RowId>, Status> {
        let state = self.state();
        let (_, r) = state.model.resolve_table(table)?;
        Ok(state.model.rows(&r.canonical).to_vec())
    }
}

#[cfg(test)]
mod tests;
