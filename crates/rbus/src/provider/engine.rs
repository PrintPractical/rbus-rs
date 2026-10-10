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
use std::pin::Pin;
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
use crate::provider::events::{SubscriberInfo, SubscriberStore, SubscriptionEvent};
use crate::provider::fault::{Fault, SetFault};
use crate::provider::model::{
    Found, Model, MountId, NodeKind, Resolved, SchemaError, Target, Tree,
};
use crate::provider::schema::{Rows, Schema};
use crate::queue::Queue;
use crate::subscribe::RowId;

mod subs;

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
/// direct mode (P19).
const UNSUPPORTED_METHOD: i32 = 9;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`Provider`] made object safe, so providers of different types can share
/// the registry (decisions.md D3).
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
    /// Streams of subscription changes (P8).
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

/// A set buffered until its legacy session commits (D8).
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
    /// Every subscription to every provider (M5).
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
        let owner = owner?;
        self.mounts
            .iter()
            .find(|(id, _)| *id == owner)
            .map(|(_, m)| m.handler.clone())
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
type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

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

/// One property to read for a get.
struct Read {
    /// The name the reply carries.
    reply: String,
    canonical: String,
    instances: Vec<u32>,
    handler: Handler,
    /// A failure fails the whole get; otherwise the property is left out,
    /// as `rbus.c` does inside partial paths.
    required: bool,
}

/// A request, decoded before any handler runs.
enum Request {
    Get(GetRequest),
    Set(SetRequest),
    Names(NamesRequest),
    AddRow(AddRowRequest),
    RemoveRow(DeleteRowRequest),
    Rpc(RpcRequest),
    /// Subscribe (true) or unsubscribe (false).
    Subscribe(SubscribeRequest, bool),
    /// Answered with a bare status.
    Status(i32),
}

impl Request {
    /// The names a request reaches, to find their providers.
    fn names(&self) -> Vec<String> {
        match self {
            Request::Get(r) => r.names.clone(),
            Request::Set(r) => r.properties.iter().map(|p| p.name.clone()).collect(),
            Request::Names(r) => vec![r.name.clone()],
            Request::AddRow(r) => vec![r.table.clone()],
            Request::RemoveRow(r) => vec![r.row.clone()],
            Request::Rpc(r) => vec![r.method.clone()],
            Request::Subscribe(r, _) => vec![r.event_name.clone()],
            Request::Status(_) => Vec::new(),
        }
    }

    fn decode(method: &Method, r: &mut Reader<'_>) -> rbus_wire::Result<Request> {
        Ok(match method {
            Method::GetParameterValues => Request::Get(GetRequest::decode(r)?),
            Method::SetParameterValues => Request::Set(SetRequest::decode(r)?),
            Method::GetParameterNames => Request::Names(NamesRequest::decode(r)?),
            Method::AddTableRow => Request::AddRow(AddRowRequest::decode(r)?),
            Method::DeleteTableRow => Request::RemoveRow(DeleteRowRequest::decode(r)?),
            Method::Rpc => Request::Rpc(RpcRequest::decode(r)?),
            // rbus providers have nothing to commit or report (`rbus.c:2879-2899`).
            Method::Commit | Method::GetParameterAttributes => Request::Status(0),
            Method::Subscribe => Request::Subscribe(SubscribeRequest::decode(r)?, true),
            Method::Unsubscribe => Request::Subscribe(SubscribeRequest::decode(r)?, false),
            // Direct mode is out of scope (P19, D14).
            _ => Request::Status(UNSUPPORTED_METHOD),
        })
    }
}

/// The status to send for a fault; success is never a fault.
fn wire_status(status: Status) -> i32 {
    if status.is_success() {
        Status::BusError.code()
    } else {
        status.code()
    }
}

fn name_of<N: crate::name::ElementName>(canonical: &str) -> Result<N, Status> {
    N::parse(canonical.to_owned()).map_err(|e| {
        warn!(error = %e, "canonical name rejected");
        Status::BusError
    })
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
        if let Some((_, m)) = state.mounts.iter().find(|(m, _)| *m == id) {
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

    /// Adds a row on the provider's behalf (PT3) and publishes
    /// `OBJECT_CREATED` (PT9).
    pub(crate) fn add_local_row(
        &self,
        id: MountId,
        table: &str,
        row: RowId,
        out: &dyn Outbox,
    ) -> Result<(), Status> {
        let mut state = self.state();
        let (t, r) = state.model.resolve_table(table)?;
        if state.model.tree.node(t).owner.is_some_and(|o| o != id)
            || !state.mounts.iter().any(|(m, _)| *m == id)
        {
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
        if state.model.tree.node(t).owner.is_some_and(|o| o != id)
            || !state.mounts.iter().any(|(m, _)| *m == id)
        {
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

    /// True if any of `names` reaches a provider that asked to be
    /// serialized.
    fn reaches_serialized(&self, names: &[String]) -> bool {
        let state = self.state();
        let serialized = |owner: Option<MountId>| {
            owner.is_some_and(|o| {
                state
                    .mounts
                    .iter()
                    .any(|(id, m)| *id == o && m.handler.serialized)
            })
        };
        if !state.mounts.iter().any(|(_, m)| m.handler.serialized) {
            return false;
        }
        let model = &state.model;
        names.iter().any(|name| {
            let resolved = model.resolve_matching(name);
            let owner = |t: &Target| match *t {
                Target::Node(n) | Target::Table(n) | Target::Row(n, _) => model.tree.node(n).owner,
            };
            let template = model.resolve_template(name).ok();
            serialized(template.as_ref().and_then(owner))
                || resolved.iter().any(|r| {
                    let node = owner(&r.target);
                    let mut found = Vec::new();
                    model.properties_under(r, &mut found);
                    serialized(node)
                        || found
                            .iter()
                            .any(|f| serialized(model.tree.node(f.node).owner))
                })
        })
    }

    /// Queues a request for the serial worker, which serves one at a time,
    /// in the order they arrived. Each runs as its own task, so a panic in a
    /// handler doesn't stop the worker.
    fn run_serially(&self, job: Job) {
        let tx = self.serial.get_or_init(|| {
            let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
            tokio::spawn(async move {
                while let Some(job) = rx.recv().await {
                    let _ = tokio::spawn(job).await;
                }
            });
            tx
        });
        let _ = tx.send(job);
    }

    /// Serves one request and returns the response payload.
    async fn handle(&self, req: Request, cx: Context, out: &dyn Outbox) -> Vec<u8> {
        match req {
            Request::Get(r) => response_payload(&self.get(r, cx).await),
            Request::Set(r) => response_payload(&self.set(r, cx, out).await),
            Request::Names(r) => self.names(r),
            Request::AddRow(r) => response_payload(&self.add_row(r, cx, out).await),
            Request::RemoveRow(r) => response_payload(&StatusResponse {
                status: self.remove_row(r, cx, out).await,
            }),
            Request::Rpc(r) => response_payload(&self.invoke(r, cx).await),
            Request::Subscribe(r, added) => self.subscribe(r, added, cx, out).await,
            Request::Status(status) => response_payload(&StatusResponse { status }),
        }
    }

    /// Works out which properties a get reads (`_get_callback_handler`).
    fn plan_get(&self, names: &[String]) -> Result<Vec<Read>, Status> {
        if names.is_empty() {
            return Err(Status::InvalidInput);
        }
        let state = self.state();
        let model = &state.model;
        let mut reads = Vec::new();
        let mut read = |found: Found, reply: String, required: bool| -> Result<(), Status> {
            let handler = state
                .handler(model.tree.node(found.node).owner)
                .ok_or(Status::ElementDoesNotExist)?;
            reads.push(Read {
                reply,
                canonical: found.name,
                instances: found.instances,
                handler,
                required,
            });
            Ok(())
        };
        for name in names {
            if name.contains('*') || name.ends_with('.') {
                for r in model.resolve_all(name)? {
                    let mut found = Vec::new();
                    model.properties_under(&r, &mut found);
                    // A wildcard naming properties needs every one of them.
                    let required = matches!(r.target, Target::Node(n)
                        if matches!(model.tree.node(n).kind, NodeKind::Property(_)));
                    for f in found {
                        read(f, String::new(), required)?;
                    }
                }
            } else {
                let r = model.resolve(name)?;
                match r.target {
                    Target::Node(n) => match model.tree.node(n).kind {
                        NodeKind::Property(a) if a.readable() => read(
                            Found {
                                name: r.canonical,
                                node: n,
                                instances: r.instances,
                            },
                            name.clone(),
                            true,
                        )?,
                        // Registered, but with no getter (`rbus.c:2001`).
                        _ => return Err(Status::InvalidOperation),
                    },
                    _ => return Err(Status::InvalidOperation),
                }
            }
        }
        Ok(reads)
    }

    async fn get(&self, req: GetRequest, cx: Context) -> GetResponse {
        let fail = |status: Status| GetResponse {
            status: wire_status(status),
            properties: Vec::new(),
        };
        let reads = match self.plan_get(&req.names) {
            Ok(r) => r,
            Err(status) => return fail(status),
        };
        let mut properties = Vec::with_capacity(reads.len());
        for read in reads {
            let result = match name_of::<PropertyName>(&read.canonical) {
                Ok(name) => {
                    let cx = cx.with_instances(read.instances);
                    read.handler.provider.get(&cx, &name).await
                }
                Err(s) => Err(Fault::new(s)),
            };
            match result {
                Ok(value) => {
                    let name = if read.reply.is_empty() {
                        read.canonical
                    } else {
                        read.reply
                    };
                    properties.push(Property::new(name, value));
                }
                Err(fault) if read.required => {
                    debug!(name = %read.canonical, %fault, "get failed");
                    return fail(fault.status());
                }
                Err(fault) => trace!(name = %read.canonical, %fault, "left out of partial get"),
            }
        }
        GetResponse {
            status: 0,
            properties,
        }
    }

    /// Checks each parameter of a set against the schema
    /// (`_set_callback_handler`).
    fn plan_set(&self, props: Vec<Property>) -> Result<Vec<Buffered>, (Status, String)> {
        let state = self.state();
        let mut planned = Vec::with_capacity(props.len());
        for p in props {
            let r = state
                .model
                .resolve(&p.name)
                .map_err(|_| (Status::ElementDoesNotExist, p.name.clone()))?;
            let owner = match r.target {
                Target::Node(n) => match state.model.tree.node(n) {
                    node if matches!(node.kind, NodeKind::Property(a) if a.writable()) => {
                        node.owner
                    }
                    _ => None,
                },
                _ => None,
            };
            let Some(owner) = owner else {
                return Err((Status::NotWritable, p.name));
            };
            let name = name_of(&r.canonical).map_err(|s| (s, p.name.clone()))?;
            planned.push(Buffered {
                requested: p.name,
                owner,
                change: Change {
                    name,
                    value: p.value,
                    instances: r.instances,
                },
            });
        }
        Ok(planned)
    }

    async fn set(&self, req: SetRequest, cx: Context, out: &dyn Outbox) -> SetResponse {
        let fail = |status: Status, name: String| SetResponse {
            status: wire_status(status),
            old_values: Vec::new(),
            failed_element: Some(name),
        };
        if req.properties.is_empty() {
            return fail(Status::InvalidInput, req.component);
        }
        let mut planned = match self.plan_set(req.properties) {
            Ok(p) => p,
            Err((status, name)) => return fail(status, name),
        };
        // By inbox, which the caller can't choose, rather than the
        // component name in the request, which it can.
        let key = (cx.inbox().to_owned(), req.session_id);
        // Group by provider, in the order each first appears.
        let mut groups: Vec<(Handler, Vec<Buffered>)> = Vec::new();
        {
            let mut state = self.state();
            if !req.commit {
                let held: usize = state
                    .sessions
                    .iter()
                    .filter(|((inbox, _), _)| *inbox == key.0)
                    .map(|(_, b)| b.len())
                    .sum();
                if held + planned.len() > self.max_buffered {
                    return fail(Status::OutOfResources, req.component);
                }
                debug!(caller = %key.0, session = key.1, "set buffered until commit");
                state.sessions.entry(key).or_default().extend(planned);
                return SetResponse {
                    status: 0,
                    old_values: Vec::new(),
                    failed_element: None,
                };
            }
            if let Some(mut earlier) = state.sessions.remove(&key) {
                earlier.append(&mut planned);
                planned = earlier;
            }
            for b in planned {
                let Some(handler) = state.handler(Some(b.owner)) else {
                    return fail(Status::ElementDoesNotExist, b.requested);
                };
                match groups
                    .iter_mut()
                    .find(|(h, _)| Arc::ptr_eq(&h.provider, &handler.provider))
                {
                    Some((_, g)) => g.push(b),
                    None => groups.push((handler, vec![b])),
                }
            }
        }
        for (handler, group) in groups {
            let first = group[0].requested.clone();
            let requested: Vec<(PropertyName, String)> = group
                .iter()
                .map(|b| (b.change.name.clone(), b.requested.clone()))
                .collect();
            let tx = Transaction {
                changes: group.into_iter().map(|b| b.change).collect(),
            };
            let changed: Vec<(PropertyName, Vec<u32>)> = tx
                .changes
                .iter()
                .map(|c| (c.name.clone(), c.instances.clone()))
                .collect();
            self.before_set(out, &handler, &tx.changes).await;
            let result = { handler.provider.set(&cx, tx).await };
            if let Err(fault) = result {
                debug!(%fault, "set failed");
                let name = fault
                    .name()
                    .and_then(|n| requested.iter().find(|(c, _)| c == n))
                    .map_or(first, |(_, r)| r.clone());
                return fail(fault.fault().status(), name);
            }
            self.after_set(out, &handler, changed, cx.caller().unwrap_or_default())
                .await;
        }
        SetResponse {
            status: 0,
            old_values: Vec::new(),
            failed_element: None,
        }
    }

    /// `GETPARAMETERNAMES` (`_get_parameter_names_handler`).
    fn names(&self, req: NamesRequest) -> Vec<u8> {
        let state = self.state();
        let model = &state.model;
        let resolved = model.resolve(&req.name);
        match req.mode {
            NamesMode::Rows => {
                let resp = match resolved {
                    Ok(
                        r @ Resolved {
                            target: Target::Table(_),
                            ..
                        },
                    ) => RowNamesResponse {
                        status: 0,
                        rows: model
                            .rows(&r.canonical)
                            .iter()
                            .map(|row| RowName {
                                instance: row.inst,
                                alias: row.alias.clone(),
                            })
                            .collect(),
                    },
                    Ok(_) => RowNamesResponse {
                        status: Status::InvalidInput.code(),
                        rows: Vec::new(),
                    },
                    Err(s) => RowNamesResponse {
                        status: s.code(),
                        rows: Vec::new(),
                    },
                };
                response_payload(&resp)
            }
            NamesMode::Elements => response_payload(&match resolved {
                Ok(r) => ElementNamesResponse {
                    status: 0,
                    elements: model.element_names(&r, req.depth),
                },
                Err(s) => ElementNamesResponse {
                    status: s.code(),
                    elements: Vec::new(),
                },
            }),
        }
    }

    /// `METHOD_ADDTBLROW` (`_table_add_row_callback_handler`).
    async fn add_row(&self, req: AddRowRequest, cx: Context, out: &dyn Outbox) -> AddRowResponse {
        let fail = |status: Status| AddRowResponse {
            status: wire_status(status),
            instance: Some(0),
        };
        let planned = {
            let state = self.state();
            state.model.resolve_table(&req.table).and_then(|(t, r)| {
                let node = state.model.tree.node(t);
                let handler = match node.kind {
                    NodeKind::Table(Rows::Dynamic) => state.handler(node.owner),
                    _ => None,
                }
                .ok_or(Status::InvalidOperation)?;
                if req
                    .alias
                    .as_deref()
                    .is_some_and(|a| state.model.alias_taken(&r.canonical, a))
                {
                    return Err(Status::ElementNameDuplicate);
                }
                Ok((handler, r))
            })
        };
        let (handler, r) = match planned {
            Ok(p) => p,
            Err(status) => return fail(status),
        };
        let table: TablePath = match name_of(&r.canonical) {
            Ok(t) => t,
            Err(s) => return fail(s),
        };
        let cx = cx.with_instances(r.instances);
        let before: Vec<u32> = self
            .state()
            .model
            .rows(&r.canonical)
            .iter()
            .map(|row| row.inst)
            .collect();
        let inst = {
            match handler
                .provider
                .add_row(&cx, &table, req.alias.as_deref())
                .await
            {
                Ok(inst) => inst,
                Err(fault) => {
                    debug!(%table, %fault, "add row failed");
                    return fail(fault.status());
                }
            }
        };
        let row = RowId {
            inst,
            alias: req.alias,
        };
        let mut state = self.state();
        // A provider may record the new row itself, with `Handle::add_row`,
        // as `Table` does so that the rows inside it can be recorded too.
        // The row and its event are already there.
        if !before.contains(&inst)
            && state
                .model
                .rows(&r.canonical)
                .iter()
                .any(|x| x.inst == inst)
        {
            return AddRowResponse {
                status: 0,
                instance: Some(inst),
            };
        }
        match state.model.add_row(&r.canonical, row.clone()) {
            Ok(()) => {
                state.row_event(out, &r.canonical, &row, true);
                AddRowResponse {
                    status: 0,
                    instance: Some(inst),
                }
            }
            Err(status) => {
                warn!(%table, inst, "provider returned an instance number or alias in use");
                fail(status)
            }
        }
    }

    /// `METHOD_DELETETBLROW` (`_table_remove_row_callback_handler`).
    async fn remove_row(&self, req: DeleteRowRequest, cx: Context, out: &dyn Outbox) -> i32 {
        let planned = {
            let state = self.state();
            state.model.resolve_row(&req.row).and_then(|(t, inst, r)| {
                let node = state.model.tree.node(t);
                let handler = match node.kind {
                    NodeKind::Table(Rows::Dynamic) => state.handler(node.owner),
                    _ => None,
                }
                .ok_or(Status::InvalidOperation)?;
                let table = r
                    .canonical
                    .strip_suffix(&format!("{inst}."))
                    .unwrap_or_default()
                    .to_owned();
                let alias = state
                    .model
                    .rows(&table)
                    .iter()
                    .find(|row| row.inst == inst)
                    .and_then(|row| row.alias.clone());
                let mut instances = r.instances;
                instances.pop();
                Ok((handler, table, RowId { inst, alias }, instances))
            })
        };
        let (handler, table, row, instances) = match planned {
            Ok(p) => p,
            Err(status) => return wire_status(status),
        };
        let path: TablePath = match name_of(&table) {
            Ok(t) => t,
            Err(s) => return wire_status(s),
        };
        let cx = cx.with_instances(instances);
        {
            if let Err(fault) = handler.provider.remove_row(&cx, &path, &row).await {
                debug!(table = %path, inst = row.inst, %fault, "remove row failed");
                return wire_status(fault.status());
            }
        }
        self.state().remove_row(out, &table, row.inst);
        0
    }

    /// `METHOD_RPC` (`_method_callback_handler`). A failure still returns
    /// an output object, with `error_code` and `error_string` added when the
    /// provider didn't set them (`rbus.c:2524-2532`).
    async fn invoke(&self, req: RpcRequest, cx: Context) -> RpcResponse {
        let planned = {
            let state = self.state();
            match state.model.resolve(&req.method) {
                Err(_) => Err((
                    Status::ElementDoesNotExist,
                    "RBUS_ERROR_ELEMENT_DOES_NOT_EXIST",
                )),
                Ok(r) => match r.target {
                    Target::Node(n) if state.model.tree.node(n).kind == NodeKind::Method => state
                        .handler(state.model.tree.node(n).owner)
                        .map(|h| (h, r))
                        .ok_or((Status::InvalidOperation, "RBUS_ERROR_INVALID_OPERATION")),
                    _ => Err((Status::InvalidOperation, "RBUS_ERROR_INVALID_OPERATION")),
                },
            }
        };
        let (handler, r) = match planned {
            Ok(p) => p,
            // `rbus.c` spells these out with the enum names.
            Err((status, text)) => {
                return RpcResponse {
                    status: status.code(),
                    output: Some(error_output(Object::new(), status, text.to_owned())),
                };
            }
        };
        let result = match name_of::<MethodName>(&r.canonical) {
            Ok(method) => {
                let cx = cx.with_instances(r.instances);
                handler
                    .provider
                    .invoke(&cx, &method, req.input.unwrap_or_default())
                    .await
            }
            Err(s) => Err(Fault::new(s)),
        };
        match result {
            Ok(output) => RpcResponse {
                status: 0,
                output: Some(output),
            },
            Err(fault) => {
                debug!(method = %r.canonical, %fault, "method failed");
                let (status, message, detail) = fault.into_detail();
                let status = Status::from_wire(wire_status(status));
                let text = message.unwrap_or_else(|| status.c_message().to_owned());
                RpcResponse {
                    status: status.code(),
                    output: Some(error_output(detail.unwrap_or_default(), status, text)),
                }
            }
        }
    }
}

/// Adds `error_code` and `error_string` unless the output has an
/// `error_code` already.
fn error_output(mut output: Object, status: Status, text: String) -> Object {
    if output.get("error_code").is_none() {
        output
            .properties
            .push(Property::new("error_code", Value::I32(status.code())));
        output
            .properties
            .push(Property::new("error_string", Value::String(text)));
    }
    output
}

/// Routes one inbound request, in arrival order: through the connection's
/// serial worker when it reaches a serialized provider, otherwise as a task
/// of its own. The reader calls this for every request.
pub(crate) fn dispatch(core: &Arc<Core>, frame: Frame) {
    let (request, method) = decode_request(&frame, &core.limits);
    let (names, job) = serve(core.clone(), frame, request, method);
    if core.registry.reaches_serialized(&names) {
        core.registry.run_serially(Box::pin(job));
    } else {
        tokio::spawn(job);
    }
}

/// Starts the connection's value-change poller, once.
pub(crate) fn ensure_poller(core: &Arc<Core>) {
    if core.registry.start_poller() {
        tokio::spawn(poller(Arc::downgrade(core)));
    }
}

/// Decodes a request. One that doesn't decode is answered with
/// `INVALID_INPUT`, rather than left to time out.
fn decode_request(frame: &Frame, limits: &Limits) -> (Request, Method) {
    let invalid = || Request::Status(Status::InvalidInput.code());
    match split_method_payload(&frame.payload, limits) {
        Ok((mut body, meta)) => {
            let request = Request::decode(&meta.method, &mut body).unwrap_or_else(|e| {
                warn!(error = %e, topic = %frame.header.topic, "malformed request body");
                invalid()
            });
            (request, meta.method)
        }
        Err(e) => {
            warn!(error = %e, topic = %frame.header.topic, "undecodable request");
            (invalid(), Method::Other("?".into()))
        }
    }
}

/// The request's names, and a future that serves it and sends the response.
fn serve(
    core: Arc<Core>,
    frame: Frame,
    request: Request,
    method: Method,
) -> (Vec<String>, impl Future<Output = ()> + Send + 'static) {
    let names = request.names();
    let job = async move {
        let span = debug_span!("rbus.serve", method = method.as_str(), topic = %frame.header.topic);
        let caller = match &request {
            Request::Get(r) => Some(r.requesting_component.clone()),
            Request::Set(r) => Some(r.component.clone()),
            _ => None,
        };
        let cx = Context::new(caller, frame.header.reply_topic.clone(), span.clone());
        if matches!(request, Request::Subscribe(_, true)) {
            ensure_poller(&core);
        }
        let payload = core
            .registry
            .handle(request, cx, &*core)
            .instrument(span)
            .await;
        let header = Header::new(
            frame.header.seq,
            Flags::RESPONSE | Flags::RAW_BINARY,
            frame.header.reply_topic,
            frame.header.topic,
        );
        // A failure means the connection is closing; the caller will time out.
        let _ = core.queue(&Frame::new(header, payload));
    };
    (names, job)
}

#[cfg(test)]
mod tests;
