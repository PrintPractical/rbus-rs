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
use std::sync::{Arc, Mutex, MutexGuard};

use rbus_wire::body::{
    AddRowRequest, AddRowResponse, DeleteRowRequest, ElementNamesResponse, GetRequest, GetResponse,
    NamesMode, NamesRequest, RowName, RowNamesResponse, RpcRequest, RpcResponse, SetRequest,
    SetResponse, StatusResponse,
};
use rbus_wire::frame::{Flags, Frame, Header};
use rbus_wire::message::{Body, Method, response_payload, split_method_payload};
use rbus_wire::msgpack::Reader;
use rbus_wire::value::{Object, Property, Value};
use tracing::{Instrument, debug, debug_span, trace, warn};

use crate::conn::Core;
use crate::error::Status;
use crate::name::{MethodName, PropertyName, TablePath};
use crate::provider::Provider;
use crate::provider::context::{Change, Context, Transaction};
use crate::provider::fault::{Fault, SetFault};
use crate::provider::model::{
    Found, Model, MountId, NodeKind, Resolved, SchemaError, Target, Tree,
};
use crate::provider::schema::{Rows, Schema};
use crate::subscribe::RowId;

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
}

/// A registered provider.
struct Mount {
    schema: Schema,
    handler: Handler,
}

/// What serves a mount's requests: the provider, and the lock that orders
/// them if it asked to be serialized.
#[derive(Clone)]
struct Handler {
    provider: Arc<dyn DynProvider>,
    serial: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl Handler {
    async fn lock(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        match &self.serial {
            Some(m) => Some(m.lock().await),
            None => None,
        }
    }
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
    /// Uncommitted sets, by caller and session id.
    sessions: HashMap<(String, u32), Vec<Buffered>>,
    next_mount: MountId,
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
#[derive(Default)]
pub(crate) struct Registry {
    state: Mutex<State>,
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
    /// Answered with a bare status.
    Status(i32),
}

impl Request {
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
            // The subscription registry arrives with M5.
            Method::Subscribe | Method::Unsubscribe => {
                Request::Status(Status::InvalidOperation.code())
            }
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
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Adds a provider. Fails if its schema is malformed or overlaps a
    /// provider already registered.
    pub(crate) fn mount(
        &self,
        schema: Schema,
        provider: Arc<dyn DynProvider>,
        serialized: bool,
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
                    serial: serialized.then(|| Arc::new(tokio::sync::Mutex::new(()))),
                },
            },
        ));
        Ok(id)
    }

    /// Removes a provider, its rows and its buffered sets.
    pub(crate) fn unmount(&self, id: MountId) {
        let mut state = self.state();
        state.mounts.retain(|(m, _)| *m != id);
        // A subset of a valid set of schemas is valid.
        let tree = state.rebuild(None).unwrap_or_default();
        state.model.set_tree(tree);
        for pending in state.sessions.values_mut() {
            pending.retain(|b| b.owner != id);
        }
        state.sessions.retain(|_, p| !p.is_empty());
    }

    /// Adds a row on the provider's behalf (PT3).
    pub(crate) fn add_local_row(&self, id: MountId, table: &str, row: RowId) -> Result<(), Status> {
        let mut state = self.state();
        let (t, r) = state.model.resolve_table(table)?;
        if state.model.tree.node(t).owner.is_some_and(|o| o != id)
            || !state.mounts.iter().any(|(m, _)| *m == id)
        {
            return Err(Status::AccessNotAllowed);
        }
        state.model.add_row(&r.canonical, row)
    }

    /// Removes a row on the provider's behalf.
    pub(crate) fn remove_local_row(
        &self,
        id: MountId,
        table: &str,
        inst: u32,
    ) -> Result<(), Status> {
        let mut state = self.state();
        let (t, r) = state.model.resolve_table(table)?;
        if state.model.tree.node(t).owner.is_some_and(|o| o != id) {
            return Err(Status::AccessNotAllowed);
        }
        state
            .model
            .remove_row(&r.canonical, inst)
            .map(drop)
            .ok_or(Status::ElementDoesNotExist)
    }

    /// The rows of a table.
    pub(crate) fn rows(&self, table: &str) -> Result<Vec<RowId>, Status> {
        let state = self.state();
        let (_, r) = state.model.resolve_table(table)?;
        Ok(state.model.rows(&r.canonical).to_vec())
    }

    /// Serves one request and returns the response payload.
    async fn handle(&self, req: Request, cx: Context) -> Vec<u8> {
        match req {
            Request::Get(r) => response_payload(&self.get(r, cx).await),
            Request::Set(r) => response_payload(&self.set(r, cx).await),
            Request::Names(r) => self.names(r),
            Request::AddRow(r) => response_payload(&self.add_row(r, cx).await),
            Request::RemoveRow(r) => response_payload(&StatusResponse {
                status: self.remove_row(r, cx).await,
            }),
            Request::Rpc(r) => response_payload(&self.invoke(r, cx).await),
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
                    let _guard = read.handler.lock().await;
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

    async fn set(&self, req: SetRequest, cx: Context) -> SetResponse {
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
        let key = (req.component, req.session_id);
        // Group by provider, in the order each first appears.
        let mut groups: Vec<(Handler, Vec<Buffered>)> = Vec::new();
        {
            let mut state = self.state();
            if !req.commit {
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
            let _guard = handler.lock().await;
            if let Err(fault) = handler.provider.set(&cx, tx).await {
                debug!(%fault, "set failed");
                let name = fault
                    .name()
                    .and_then(|n| requested.iter().find(|(c, _)| c == n))
                    .map_or(first, |(_, r)| r.clone());
                return fail(fault.fault().status(), name);
            }
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
    async fn add_row(&self, req: AddRowRequest, cx: Context) -> AddRowResponse {
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
        let inst = {
            let _guard = handler.lock().await;
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
        match self.state().model.add_row(&r.canonical, row) {
            Ok(()) => AddRowResponse {
                status: 0,
                instance: Some(inst),
            },
            Err(status) => {
                warn!(%table, inst, "provider returned an instance number or alias in use");
                fail(status)
            }
        }
    }

    /// `METHOD_DELETETBLROW` (`_table_remove_row_callback_handler`).
    async fn remove_row(&self, req: DeleteRowRequest, cx: Context) -> i32 {
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
            let _guard = handler.lock().await;
            if let Err(fault) = handler.provider.remove_row(&cx, &path, &row).await {
                debug!(table = %path, inst = row.inst, %fault, "remove row failed");
                return wire_status(fault.status());
            }
        }
        self.state().model.remove_row(&table, row.inst);
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
                let _guard = handler.lock().await;
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

/// Serves one inbound request and sends the response. Runs as its own task.
pub(crate) async fn serve(core: Arc<Core>, frame: Frame) {
    let decoded = split_method_payload(&frame.payload)
        .map(|(mut body, meta)| (Request::decode(&meta.method, &mut body), meta.method));
    let (request, method) = match decoded {
        Ok(d) => d,
        Err(e) => return warn!(error = %e, topic = %frame.header.topic, "undecodable request"),
    };
    let span = debug_span!("rbus.serve", method = method.as_str(), topic = %frame.header.topic);
    let request = request.unwrap_or_else(|e| {
        warn!(parent: &span, error = %e, "malformed request body");
        Request::Status(Status::InvalidInput.code())
    });
    let caller = match &request {
        Request::Get(r) => Some(r.requesting_component.clone()),
        Request::Set(r) => Some(r.component.clone()),
        _ => None,
    };
    let cx = Context::new(caller, frame.header.reply_topic.clone(), span.clone());
    let payload = core.registry.handle(request, cx).instrument(span).await;
    let header = Header::new(
        frame.header.seq,
        Flags::RESPONSE | Flags::RAW_BINARY,
        frame.header.reply_topic,
        frame.header.topic,
    );
    // A failure means the connection is closing; the caller will time out.
    let _ = core.queue(&Frame::new(header, payload));
}

#[cfg(test)]
mod tests;
