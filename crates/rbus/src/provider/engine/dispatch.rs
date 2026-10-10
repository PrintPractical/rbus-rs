//! Routing inbound requests: decoding them, running those for serialized
//! providers in arrival order, and sending the replies.

use super::*;

/// A request, decoded before any handler runs.
pub(super) enum Request {
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
    pub(super) fn names(&self) -> Vec<String> {
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

    pub(super) fn decode(method: &Method, r: &mut Reader<'_>) -> rbus_wire::Result<Request> {
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

impl Registry {
    /// True if any of `names` reaches a provider that asked to be
    /// serialized.
    pub(super) fn reaches_serialized(&self, names: &[String]) -> bool {
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
    pub(super) fn run_serially(&self, job: Job) {
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
pub(super) fn decode_request(frame: &Frame, limits: &Limits) -> (Request, Method) {
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
pub(super) fn serve(
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
        let cx = Context::traced(caller, frame.header.reply_topic.clone(), span.clone());
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
