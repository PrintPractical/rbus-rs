//! Answering each kind of request, as `rbus.c`'s callback handlers do.

use super::*;

/// One property to read for a get.
pub(super) struct Read {
    /// The name the reply carries.
    reply: String,
    canonical: String,
    instances: Vec<u32>,
    handler: Handler,
    /// A failure fails the whole get; otherwise the property is left out,
    /// as `rbus.c` does inside partial paths.
    required: bool,
}

/// The status to send for a fault; success is never a fault.
pub(super) fn wire_status(status: Status) -> i32 {
    if status.is_success() {
        Status::BusError.code()
    } else {
        status.code()
    }
}

pub(super) fn name_of<N: crate::name::ElementName>(canonical: &str) -> Result<N, Status> {
    N::parse(canonical.to_owned()).map_err(|e| {
        warn!(error = %e, "canonical name rejected");
        Status::BusError
    })
}

impl Registry {
    /// Serves one request and returns the response payload.
    pub(super) async fn handle(&self, req: Request, cx: Context, out: &dyn Outbox) -> Vec<u8> {
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
    pub(super) fn plan_get(&self, names: &[String]) -> Result<Vec<Read>, Status> {
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

    pub(super) async fn get(&self, req: GetRequest, cx: Context) -> GetResponse {
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
    pub(super) fn plan_set(&self, props: Vec<Property>) -> Result<Vec<Buffered>, (Status, String)> {
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

    pub(super) async fn set(&self, req: SetRequest, cx: Context, out: &dyn Outbox) -> SetResponse {
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
    pub(super) fn names(&self, req: NamesRequest) -> Vec<u8> {
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
    pub(super) async fn add_row(
        &self,
        req: AddRowRequest,
        cx: Context,
        out: &dyn Outbox,
    ) -> AddRowResponse {
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
    pub(super) async fn remove_row(
        &self,
        req: DeleteRowRequest,
        cx: Context,
        out: &dyn Outbox,
    ) -> i32 {
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
    pub(super) async fn invoke(&self, req: RpcRequest, cx: Context) -> RpcResponse {
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
pub(super) fn error_output(mut output: Object, status: Status, text: String) -> Object {
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
