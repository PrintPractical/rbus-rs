//! The connection engine: one unix socket to rtrouted, a reader task, a writer
//! task, and the tables that route what arrives.
//!
//! - **Writes** go through an unbounded channel to the connection task, so
//!   sending never blocks a caller and frames leave in the order they were
//!   queued.
//! - **Reads** happen in a reader task per socket. It matches each response to its
//!   pending request by sequence number, routes
//!   each event to its subscription by the echoed component id, fans out
//!   broker advisories, and hands each request for our providers to
//!   `provider::dispatch`: a task of its own, or the serial worker for
//!   serialized providers.
//! - **Cancellation**: a pending request is removed from the table when its
//!   [`Pending`] is dropped, so a cancelled or timed-out call leaves nothing
//!   behind and a late response is discarded.
//! - **Resubscription**: a connect advisory (another client joined the bus)
//!   wakes a debounced task that re-sends every subscription, because a C
//!   provider that restarted has forgotten ours.
//! - **Reconnect**: when the socket is lost, the connection task fails
//!   every pending request and, if the builder asked for it, connects again
//!   with backoff. On the new socket it registers the inbox, the component and
//!   the advisory route before anything else, then re-registers every element
//!   alias and re-sends every subscription. Frames queued for the lost
//!   socket are dropped, never sent on the new one; frames queued while
//!   reconnecting are dropped too, unless the new socket is already up.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use bytes::Bytes;
use rbus_wire::Limits;
use rbus_wire::control::{self, Route};
use rbus_wire::event::Event;
use rbus_wire::frame::{Flags, Frame, FrameDecoder, Header};
use rbus_wire::message::split_event_payload;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use crate::bus::Timeouts;
use crate::discover::Advisory;
use crate::error::{Error, Result, Status};
use crate::queue::Queue;
use crate::retry::Retry;

/// Route id of the inbox (`rtConnection.c:680`).
const INBOX_ROUTE_ID: u32 = 1;
/// Route id C uses for the advisory topic (`rbuscore.c:1797`).
const ADVISORY_ROUTE_ID: u32 = 2;
/// Component route ids come from a process-wide counter starting here, as in C.
static NEXT_COMPONENT_ROUTE: AtomicU32 = AtomicU32::new(10_000);
/// Numbers the connections of this process, to keep inbox names unique.
static NEXT_CONNECTION: AtomicU32 = AtomicU32::new(0);
/// Quiet time after the last connect advisory before resubscribing.
const RESUBSCRIBE_DEBOUNCE: Duration = Duration::from_millis(250);

/// The state of the connection to the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// Connected and registered.
    Connected,
    /// The broker went away and the connection is being made again.
    /// Calls fail with [`crate::Error::Disconnected`] meanwhile; subscriptions
    /// and providers stay in place and are registered again on reconnect.
    Reconnecting,
    /// Closed, by [`crate::Rbus::close`], or because the broker went away
    /// and reconnecting is off or gave up.
    Closed,
}

enum Outgoing {
    /// A frame, and the socket generation it was queued for.
    Frame(Bytes, u64),
    /// Flush, then shut down the write side of the socket.
    Shutdown,
}

/// A live subscription, as the reader task sees it.
pub(crate) struct SubEntry {
    /// The name subscribed to: the provider's routing key, maybe a wildcard.
    pub(crate) event_name: String,
    pub(crate) queue: Arc<Queue<Event>>,
    /// The interval sent with the subscription; 0 for none. Providers key
    /// subscriptions on it, so unsubscribing must send it again.
    pub(crate) interval: u32,
    /// False while the subscribe request is in flight; only ready
    /// subscriptions are re-sent after an advisory.
    pub(crate) ready: bool,
}

/// Everything the connection's tasks share.
pub(crate) struct Core {
    pending: Mutex<HashMap<u32, oneshot::Sender<Frame>>>,
    state: watch::Sender<ConnectionState>,
    tx: mpsc::UnboundedSender<Outgoing>,
    /// Counts lost sockets. A frame queued for an earlier socket is dropped.
    generation: AtomicU64,
    seq: AtomicU32,
    inbox: String,
    pub(crate) timeouts: Timeouts,
    /// What the decoders accept from peers.
    pub(crate) limits: Limits,
    subs: Mutex<HashMap<i32, SubEntry>>,
    next_sub_id: AtomicI32,
    advisory_listeners: Mutex<Vec<Weak<Queue<Advisory>>>>,
    /// Whether `_RTROUTED.ADVISORY` is registered. The lock also orders its
    /// registration against a new socket's (see [`Core::registration`]).
    advisories: Mutex<bool>,
    resubscribe: Arc<Notify>,
    /// The providers served on this connection.
    pub(crate) registry: crate::provider::Registry,
    /// The component's route id; element aliases are registered under it.
    pub(crate) component_route: u32,
    /// The component name.
    pub(crate) component: String,
    /// Every element name this component claims, for releasing and for
    /// reconnects.
    aliases: Mutex<BTreeMap<String, Alias>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Locks are never held across a panic point, but stay usable anyway.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Core {
    fn pending(&self) -> MutexGuard<'_, HashMap<u32, oneshot::Sender<Frame>>> {
        lock(&self.pending)
    }

    fn close(&self) {
        self.state.send_replace(ConnectionState::Closed);
        // Dropping the senders wakes every waiter with `Disconnected`.
        self.pending().clear();
        for (_, sub) in lock(&self.subs).drain() {
            sub.queue.close();
        }
        for listener in lock(&self.advisory_listeners).drain(..) {
            if let Some(q) = listener.upgrade() {
                q.close();
            }
        }
    }

    pub(crate) fn inbox(&self) -> &str {
        &self.inbox
    }

    pub(crate) fn state(&self) -> watch::Receiver<ConnectionState> {
        self.state.subscribe()
    }

    pub(crate) fn is_closed(&self) -> bool {
        *self.state.borrow() == ConnectionState::Closed
    }

    fn is_connected(&self) -> bool {
        *self.state.borrow() == ConnectionState::Connected
    }

    /// The socket was lost and a new one is on its way: fail every pending
    /// request, and make sure nothing queued so far reaches the new socket.
    /// False, and nothing changes, if the connection was closed meanwhile:
    /// `Closed` is final.
    fn lose(&self) -> bool {
        let lost = self.state.send_if_modified(|s| {
            let connected = *s == ConnectionState::Connected;
            if connected {
                *s = ConnectionState::Reconnecting;
            }
            connected
        });
        if lost {
            // In this order, a request either sees the new state or
            // generation, or has its pending entry cleared below.
            self.generation.fetch_add(1, Ordering::AcqRel);
            self.pending().clear();
        }
        lost
    }

    /// The frames that register this connection, written first on every
    /// socket: the inbox, the component and, if anything listens, the
    /// advisory topic. Each route must go out exactly once per socket: a
    /// second add of the same route becomes an alias of itself, and rtrouted
    /// crashes when it later clears the route. So the socket gets a new
    /// generation here, under the lock `ensure_advisories` takes, and
    /// anything queued before is dropped.
    fn registration(&self) -> Result<Vec<Bytes>> {
        let advisories = lock(&self.advisories);
        self.generation.fetch_add(1, Ordering::AcqRel);
        let mut routes = vec![
            Route::add(self.inbox.clone(), INBOX_ROUTE_ID),
            Route::add(self.component.clone(), self.component_route),
        ];
        if *advisories {
            routes.push(Route::add(control::TOPIC_ADVISORY, ADVISORY_ROUTE_ID));
        }
        routes
            .iter()
            .map(|route| {
                let header =
                    Header::new(self.next_seq(), Flags::NONE, control::TOPIC_SUBSCRIBE, "");
                Ok(Frame::new(header, control::encode(route)?).encode()?)
            })
            .collect()
    }

    fn next_seq(&self) -> u32 {
        loop {
            // Zero is never used, as in C.
            let seq = self.seq.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            if seq != 0 {
                return seq;
            }
        }
    }

    /// Queues a frame for the current socket.
    pub(crate) fn queue(&self, frame: &Frame) -> Result<()> {
        self.queue_for(frame, self.generation.load(Ordering::Acquire))
    }

    /// Queues a frame for the socket of `generation`; it is dropped if that
    /// socket is gone by the time the frame's turn comes.
    pub(crate) fn queue_for(&self, frame: &Frame, generation: u64) -> Result<()> {
        let bytes = frame.encode()?;
        self.tx
            .send(Outgoing::Frame(bytes, generation))
            .map_err(|_| Error::Disconnected)
    }

    /// Sends a JSON control message with no flags (no reply expected).
    pub(crate) fn send_control<T: serde::Serialize>(&self, msg: &T) -> Result<()> {
        let header = Header::new(self.next_seq(), Flags::NONE, control::TOPIC_SUBSCRIBE, "");
        self.queue(&Frame::new(header, control::encode(msg)?))
    }

    /// Sends a plain rbus message (an event) to `topic`, with our component
    /// as the reply topic, as `rbus_publishSubscriberEvent` does.
    pub(crate) fn send_message(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        let header = Header::new(self.next_seq(), Flags::RAW_BINARY, topic, &self.component);
        self.queue(&Frame::new(header, payload))
    }

    /// Sends a request and returns a handle to await its response. The request
    /// is registered before it is sent, so the response can't be missed.
    pub(crate) fn request(
        self: &Arc<Self>,
        topic: &str,
        flags: Flags,
        payload: Vec<u8>,
    ) -> Result<Pending> {
        let generation = self.generation.load(Ordering::Acquire);
        if !self.is_connected() {
            return Err(Error::Disconnected);
        }
        let seq = self.next_seq();
        let frame = Frame::new(
            Header::new(seq, Flags::REQUEST | flags, topic, &self.inbox),
            payload,
        );
        let (tx, rx) = oneshot::channel();
        self.pending().insert(seq, tx);
        let pending = Pending {
            rx,
            seq,
            core: self.clone(),
        };
        // The socket was lost after the check above, maybe after `lose`
        // cleared the pending table: this request would wait for nothing.
        if self.generation.load(Ordering::Acquire) != generation {
            return Err(Error::Disconnected);
        }
        self.queue_for(&frame, generation)?;
        trace!(seq, topic, "request sent");
        Ok(pending)
    }

    /// Sends an rbus (msgpack) request.
    pub(crate) fn rbus_request(self: &Arc<Self>, topic: &str, payload: Vec<u8>) -> Result<Pending> {
        self.request(topic, Flags::RAW_BINARY, payload)
    }

    /// Sends a JSON request to a broker topic.
    pub(crate) fn broker_request<T: serde::Serialize>(
        self: &Arc<Self>,
        topic: &str,
        msg: &T,
    ) -> Result<Pending> {
        self.request(topic, Flags::NONE, control::encode(msg)?)
    }

    /// Registers for `_RTROUTED.ADVISORY` the first time anything needs it.
    pub(crate) fn ensure_advisories(&self) -> Result<()> {
        let mut registered = lock(&self.advisories);
        if !*registered {
            self.send_control(&Route::add(control::TOPIC_ADVISORY, ADVISORY_ROUTE_ID))?;
            *registered = true;
        }
        Ok(())
    }

    /// Adds a listener for broker advisories.
    pub(crate) fn listen_advisories(&self, queue: &Arc<Queue<Advisory>>) -> Result<()> {
        self.ensure_advisories()?;
        let mut listeners = lock(&self.advisory_listeners);
        listeners.retain(|w| w.strong_count() > 0);
        listeners.push(Arc::downgrade(queue));
        Ok(())
    }

    /// A fresh subscription id. It travels as the C `componentId`, which C
    /// providers key subscriptions on and echo in every event.
    pub(crate) fn next_sub_id(&self) -> i32 {
        self.next_sub_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn add_sub(&self, id: i32, entry: SubEntry) {
        lock(&self.subs).insert(id, entry);
    }

    /// Forgets a subscription and ends its stream. Closing the queue also
    /// wakes the reader if it is waiting for room in a full `Block` queue,
    /// which would otherwise hold the whole connection.
    pub(crate) fn remove_sub(&self, id: i32) -> Option<SubEntry> {
        let entry = lock(&self.subs).remove(&id);
        if let Some(e) = &entry {
            e.queue.close();
        }
        entry
    }

    pub(crate) fn mark_ready(&self, id: i32) {
        if let Some(sub) = lock(&self.subs).get_mut(&id) {
            sub.ready = true;
        }
    }

    /// `(id, event name, interval)` of every subscription that has been set up.
    pub(crate) fn sub_names(&self) -> Vec<(i32, String, u32)> {
        lock(&self.subs)
            .iter()
            .filter(|(_, s)| s.ready)
            .map(|(id, s)| (*id, s.event_name.clone(), s.interval))
            .collect()
    }

    /// Claims element names for this component and registers them with the
    /// broker, all requests in flight at once (`rbus_addElement`). Returns
    /// each name's result.
    ///
    /// The work runs as a task of its own, so every reply is handled even if
    /// the caller stops waiting: a name released while its request was in
    /// flight is removed again if it did register (see
    /// [`Core::release_aliases`]).
    pub(crate) fn claim_aliases(
        self: &Arc<Self>,
        names: Vec<String>,
    ) -> JoinHandle<Vec<(String, Result<()>)>> {
        // One lock covers the claim and the queueing, so a replay or a
        // release can't interleave with it.
        let requests = {
            let mut aliases = lock(&self.aliases);
            names
                .into_iter()
                .map(|name| {
                    aliases.insert(name.clone(), Alias::Pending);
                    let request = self.alias_request(&name);
                    (name, request)
                })
                .collect()
        };
        tokio::spawn(self.clone().settle_aliases(requests))
    }

    /// Gives names up. Registered ones are removed from the broker (fire and
    /// forget, as `rbus_unregDataElements` does); a pending one is removed
    /// when its reply comes, and only if it registered. A name the broker
    /// refused belongs to someone else and is never removed: rtrouted
    /// removes an alias by name, whoever owns it (`rtrouted.c:842-847`).
    pub(crate) fn release_aliases(&self, names: &[String]) {
        let mut aliases = lock(&self.aliases);
        for name in names {
            if aliases.remove(name) == Some(Alias::Registered) && !self.is_closed() {
                let _ = self.send_control(&Route::remove(&**name, self.component_route));
            }
        }
    }

    fn alias_request(self: &Arc<Self>, name: &str) -> Result<Pending> {
        self.broker_request(
            control::TOPIC_SUBSCRIBE,
            &Route::add(name, self.component_route),
        )
    }

    /// Waits for each registration reply and records what it says.
    async fn settle_aliases(
        self: Arc<Self>,
        requests: Vec<(String, Result<Pending>)>,
    ) -> Vec<(String, Result<()>)> {
        let mut results = Vec::with_capacity(requests.len());
        for (name, request) in requests {
            let result = match request {
                Ok(p) => p
                    .wait(self.timeouts.broker, "register")
                    .await
                    .and_then(|frame| alias_result(&name, &frame)),
                Err(e) => Err(e),
            };
            self.settle_alias(&name, &result);
            results.push((name, result));
        }
        results
    }

    fn settle_alias(&self, name: &str, result: &Result<()>) {
        let mut aliases = lock(&self.aliases);
        match (aliases.contains_key(name), result) {
            // The socket went away; the reconnect registers the name again.
            (_, Err(Error::Disconnected)) => {}
            (true, Ok(())) => {
                aliases.insert(name.to_owned(), Alias::Registered);
            }
            // Released while in flight, and ours: remove it again.
            (false, Ok(())) => {
                let _ = self.send_control(&Route::remove(name, self.component_route));
            }
            // Refused, or no answer: not ours to remove.
            (true, Err(e)) => {
                debug!(name, error = %e, "element not registered");
                aliases.remove(name);
            }
            (false, Err(_)) => {}
        }
    }

    /// Registers every element alias again after a reconnect, then re-sends
    /// every subscription. A name another component took meanwhile is
    /// given up.
    async fn replay(self: Arc<Self>) {
        let requests: Vec<_> = {
            let mut aliases = lock(&self.aliases);
            aliases
                .iter_mut()
                .map(|(name, state)| {
                    *state = Alias::Pending;
                    (name.clone(), self.alias_request(name))
                })
                .collect()
        };
        let count = requests.len();
        for (name, result) in self.clone().settle_aliases(requests).await {
            match result {
                Ok(()) => {}
                Err(Error::Disconnected) => return,
                Err(e) => warn!(name, error = %e, "element not registered again after reconnect"),
            }
        }
        debug!(aliases = count, "registered again after reconnect");
        crate::subscribe::resubscribe_all(&self).await;
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending().len()
    }

    #[cfg(test)]
    pub(crate) fn aliases(&self) -> Vec<String> {
        lock(&self.aliases).keys().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn sub_count(&self) -> usize {
        lock(&self.subs).len()
    }

    async fn dispatch(self: &Arc<Self>, frame: Frame) {
        let flags = frame.header.flags;
        if flags.contains(Flags::RESPONSE) {
            let seq = frame.header.seq;
            match self.pending().remove(&seq) {
                // The waiter may have just given up; that's fine.
                Some(tx) => drop(tx.send(frame)),
                None => trace!(seq, "late or unknown response dropped"),
            }
        } else if frame.header.topic == control::TOPIC_ADVISORY {
            self.on_advisory(&frame).await;
        } else if flags.contains(Flags::RAW_BINARY) && !flags.contains(Flags::REQUEST) {
            self.on_event(&frame).await;
        } else if flags.contains(Flags::RAW_BINARY) {
            crate::provider::dispatch(self, frame);
        } else {
            trace!(topic = %frame.header.topic, ?flags, "unhandled inbound message");
        }
    }

    async fn on_advisory(&self, frame: &Frame) {
        let advisory = match control::decode::<control::Advisory>(&frame.payload) {
            Ok(a) => Advisory::from_wire(a),
            Err(e) => return warn!(error = %e, "bad advisory from rtrouted"),
        };
        trace!(?advisory, "advisory");
        if let Advisory::Disconnected { inbox } = &advisory {
            self.registry.client_gone(inbox);
        }
        if matches!(&advisory, Advisory::Connected { inbox } if *inbox != self.inbox)
            && !lock(&self.subs).is_empty()
        {
            self.resubscribe.notify_one();
        }
        let listeners: Vec<_> = {
            let mut l = lock(&self.advisory_listeners);
            l.retain(|w| w.strong_count() > 0);
            l.iter().filter_map(Weak::upgrade).collect()
        };
        for q in listeners {
            q.push(advisory.clone()).await;
        }
    }

    async fn on_event(&self, frame: &Frame) {
        let decoded = split_event_payload(&frame.payload, &self.limits)
            .and_then(|(mut body, meta)| Ok((Event::decode(&mut body)?, meta)));
        let (event, meta) = match decoded {
            Ok(e) => e,
            Err(e) => return warn!(error = %e, topic = %frame.header.topic, "undecodable event"),
        };
        let queue = {
            let subs = lock(&self.subs);
            // Like C, an event for an id we don't know matches on the name and
            // the interval, so a late sample from a dropped interval
            // subscription doesn't reach a plain one.
            subs.get(&event.component_id)
                .filter(|s| s.event_name == meta.event_name)
                .or_else(|| {
                    subs.values()
                        .find(|s| s.event_name == meta.event_name && s.interval == event.interval)
                })
                .map(|s| s.queue.clone())
        };
        match queue {
            Some(q) => q.push(event).await,
            None => trace!(name = %meta.event_name, "event for no subscription"),
        }
    }
}

/// Where a claimed element name stands with the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alias {
    /// Its registration request is in flight.
    Pending,
    /// The broker routes it to us.
    Registered,
}

/// The broker's answer to an alias that is already registered
/// (`RT_ERROR_DUPLICATE_ENTRY`).
const ALIAS_DUPLICATE: i32 = 1001;
/// The broker's answer to an alias that clashes with a table or property
/// (`RT_ERROR_PROTOCOL_ERROR`), which rbus maps to `INVALID_NAMESPACE`.
const ALIAS_CONFLICT: i32 = 1003;

/// Interprets the broker's reply to an alias registration.
fn alias_result(name: &str, frame: &Frame) -> Result<()> {
    let status = match control::decode::<control::ResultReply>(&frame.payload)?.result {
        0 => return Ok(()),
        ALIAS_DUPLICATE => Status::ElementNameDuplicate,
        ALIAS_CONFLICT => Status::InvalidNamespace,
        _ => Status::BusError,
    };
    Err(crate::provider::element_error(name, status))
}

/// How a connection behaves; set through the builder.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Options {
    pub(crate) timeouts: Timeouts,
    /// Connect again after losing the broker, on this schedule.
    pub(crate) reconnect: Option<Retry>,
    pub(crate) limits: Limits,
}

/// A connection to rtrouted, registered as one component.
pub(crate) struct Conn {
    core: Arc<Core>,
    task: JoinHandle<()>,
    resubscriber: JoinHandle<()>,
}

impl Deref for Conn {
    type Target = Arc<Core>;
    fn deref(&self) -> &Arc<Core> {
        &self.core
    }
}

/// The inbox name: `rbus.<prog>-<n>.INBOX.<pid>`. C providers read the pid
/// from the suffix to prune dead subscribers, so it must stay last.
fn inbox_name() -> String {
    let prog = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "rust".into());
    let prog: String = prog
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let n = NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed);
    format!("rbus.{prog}-{n}.INBOX.{}", std::process::id())
}

/// Connects to the broker's socket. rtrouted listens with a backlog of 4
/// (`rtrouteBase.c:143`); when it is full, a non-blocking connect fails with
/// `EAGAIN` where C's blocking connect would wait. Retry for up to `wait`.
async fn connect_socket(socket: &Path, wait: Duration) -> std::io::Result<UnixStream> {
    let deadline = tokio::time::Instant::now() + wait;
    let mut backoff = Duration::from_millis(1);
    loop {
        match UnixStream::connect(socket).await {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if tokio::time::Instant::now() + backoff > deadline {
                    return Err(e);
                }
                trace!(?backoff, "broker backlog full; retrying connect");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(100));
            }
            other => return other,
        }
    }
}

impl Conn {
    /// Connects to the broker at `socket` and registers the inbox and the
    /// component. Registration is fire-and-forget, as in C: the broker handles
    /// a connection's frames in order, so later requests see the routes.
    /// With `reconnect`, a lost socket is replaced.
    pub(crate) async fn connect(socket: &Path, component: &str, options: Options) -> Result<Conn> {
        let Options {
            timeouts,
            reconnect,
            limits,
        } = options;
        let mut stream = connect_socket(socket, timeouts.broker).await?;
        let (state, _) = watch::channel(ConnectionState::Connected);
        let (tx, rx) = mpsc::unbounded_channel();
        let resubscribe = Arc::new(Notify::new());
        let core = Arc::new(Core {
            pending: Mutex::new(HashMap::new()),
            state,
            tx,
            generation: AtomicU64::new(0),
            seq: AtomicU32::new(0),
            inbox: inbox_name(),
            timeouts,
            limits,
            subs: Mutex::new(HashMap::new()),
            next_sub_id: AtomicI32::new(1),
            advisory_listeners: Mutex::new(Vec::new()),
            advisories: Mutex::new(false),
            resubscribe: resubscribe.clone(),
            registry: crate::provider::Registry::new(&limits),
            component_route: NEXT_COMPONENT_ROUTE.fetch_add(1, Ordering::Relaxed),
            component: component.to_owned(),
            aliases: Mutex::new(BTreeMap::new()),
        });
        let link = Link {
            socket: socket.to_owned(),
            wait: timeouts.broker,
            reconnect,
        };
        // Registered before anything else can queue a frame.
        for frame in core.registration()? {
            stream.write_all(&frame).await?;
        }
        let task = tokio::spawn(connection(Arc::downgrade(&core), rx, stream, link));
        let resubscriber = tokio::spawn(resubscriber(Arc::downgrade(&core), resubscribe));
        let conn = Conn {
            core,
            task,
            resubscriber,
        };
        debug!(inbox = %conn.inbox, component = %conn.component, "connected to rtrouted");
        Ok(conn)
    }

    pub(crate) fn component(&self) -> &str {
        &self.component
    }

    /// Unregisters and closes the socket, then waits (up to `wait`) for the
    /// broker to close its side. Calling it again is a no-op.
    pub(crate) async fn close(&self, wait: Duration) -> Result<()> {
        let mut state = self.state();
        if self.is_closed() {
            return Ok(());
        }
        // Like rbus_close: unsubscribe everything, then unregister. The broker
        // forwards these before it sees the socket close.
        crate::subscribe::unsubscribe_all(&self.core);
        self.unregister();
        // A send error means the writer is already gone; so is the socket.
        let _ = self.tx.send(Outgoing::Shutdown);
        if tokio::time::timeout(wait, state.wait_for(|s| *s == ConnectionState::Closed))
            .await
            .is_err()
        {
            warn!("broker did not close the connection; dropping it");
            self.task.abort();
            self.core.close();
        }
        debug!(component = %self.component, "connection closed");
        Ok(())
    }

    fn unregister(&self) {
        // Best effort: the broker drops every route of a closed socket anyway.
        let _ = self.send_control(&Route::remove(self.component.clone(), self.component_route));
        let _ = self.send_control(&Route::remove(self.inbox.clone(), INBOX_ROUTE_ID));
    }
}

impl Drop for Conn {
    /// Best-effort cleanup: queue the unregistration, then let the
    /// connection task flush it and close the socket.
    fn drop(&mut self) {
        if !self.is_closed() {
            self.unregister();
            let _ = self.tx.send(Outgoing::Shutdown);
        }
        self.resubscriber.abort();
    }
}

/// A request waiting for its response.
pub(crate) struct Pending {
    rx: oneshot::Receiver<Frame>,
    seq: u32,
    core: Arc<Core>,
}

impl Pending {
    /// Waits up to `timeout` for the response.
    pub(crate) async fn wait(mut self, timeout: Duration, op: &'static str) -> Result<Frame> {
        match tokio::time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(Error::Disconnected),
            Err(_) => Err(Error::Timeout { op }),
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.core.pending().remove(&self.seq);
    }
}

/// Where to connect, and whether to come back after losing the socket.
struct Link {
    socket: PathBuf,
    /// How long a full broker backlog is waited out, per attempt.
    wait: Duration,
    reconnect: Option<Retry>,
}

/// Aborts a task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Why a socket's write loop stopped.
enum Stop {
    /// `close()` or `Drop` asked for it.
    Shutdown,
    /// The broker closed the socket, or it failed.
    Lost,
}

/// The connection task: writes queued frames to the socket, and when the
/// socket is lost, either closes the connection or makes a new one.
/// `Conn::connect` registered the first socket already.
async fn connection(
    core: Weak<Core>,
    mut rx: mpsc::UnboundedReceiver<Outgoing>,
    stream: UnixStream,
    link: Link,
) {
    let mut stream = stream;
    let mut registered = true;
    loop {
        let (read, mut write) = stream.into_split();
        let Some(c) = core.upgrade() else { return };
        let mut reader = AbortOnDrop(tokio::spawn(reader(read, c.clone())));
        let registration = if std::mem::take(&mut registered) {
            Ok(())
        } else {
            register(&c, &mut write).await
        };
        let generation = c.generation.load(Ordering::Acquire);
        let stop = match registration {
            Ok(()) => {
                // Only from Reconnecting: close() may have run meanwhile.
                let reconnected = c.state.send_if_modified(|s| {
                    let reconnecting = *s == ConnectionState::Reconnecting;
                    if reconnecting {
                        *s = ConnectionState::Connected;
                    }
                    reconnecting
                });
                if reconnected {
                    debug!(inbox = %c.inbox, "reconnected to rtrouted");
                    tokio::spawn(c.clone().replay());
                }
                drop(c);
                write_frames(&mut rx, &mut write, &mut reader.0, generation).await
            }
            Err(e) => {
                warn!(error = %e, "registering with rtrouted failed");
                drop(c);
                Stop::Lost
            }
        };
        match stop {
            Stop::Shutdown => {
                // close() waits for the broker to close its side; the reader
                // sees that and ends.
                let _ = write.shutdown().await;
                let _ = tokio::time::timeout(link.wait, &mut reader.0).await;
                if let Some(c) = core.upgrade() {
                    c.close();
                }
                return;
            }
            Stop::Lost => {}
        }
        drop((reader, write));
        let Some(c) = core.upgrade() else { return };
        let Some(retry) = link.reconnect else {
            c.close();
            return;
        };
        if !c.lose() {
            return;
        }
        warn!("lost the connection to rtrouted; reconnecting");
        drop(c);
        match reconnect(&mut rx, &link, retry).await {
            Some(s) => stream = s,
            None => {
                if let Some(c) = core.upgrade() {
                    c.close();
                }
                return;
            }
        }
    }
}

/// Writes the registration frames, ahead of anything queued.
async fn register(core: &Core, write: &mut OwnedWriteHalf) -> Result<()> {
    for frame in core.registration()? {
        write.write_all(&frame).await?;
    }
    Ok(())
}

/// Writes queued frames until the socket is lost or the connection ends.
async fn write_frames(
    rx: &mut mpsc::UnboundedReceiver<Outgoing>,
    write: &mut OwnedWriteHalf,
    reader: &mut JoinHandle<()>,
    generation: u64,
) -> Stop {
    loop {
        tokio::select! {
            out = rx.recv() => match out {
                Some(Outgoing::Frame(bytes, g)) if g == generation => {
                    if let Err(e) = write.write_all(&bytes).await {
                        warn!(error = %e, "write to rtrouted failed");
                        return Stop::Lost;
                    }
                }
                Some(Outgoing::Frame(..)) => trace!("frame for a lost socket dropped"),
                // The channel can't close here: the reader holds the core,
                // and with it the sender. Shutting down is right either way.
                Some(Outgoing::Shutdown) | None => return Stop::Shutdown,
            },
            _ = &mut *reader => return Stop::Lost,
        }
    }
}

/// Connects again on `retry`'s schedule, dropping whatever is queued
/// meanwhile. `None` if it gave up, or the connection was closed or dropped
/// (dropping the core closes the channel).
async fn reconnect(
    rx: &mut mpsc::UnboundedReceiver<Outgoing>,
    link: &Link,
    retry: Retry,
) -> Option<UnixStream> {
    let start = tokio::time::Instant::now();
    let mut delay = retry.first_delay();
    loop {
        // Nothing queued so far may reach the new socket.
        loop {
            match rx.try_recv() {
                Ok(Outgoing::Frame(..)) => {}
                Ok(Outgoing::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) => {
                    return None;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
            }
        }
        match connect_socket(&link.socket, link.wait).await {
            Ok(stream) => return Some(stream),
            Err(e) => debug!(error = %e, ?delay, "reconnect failed"),
        }
        if retry.gives_up(start.elapsed(), delay) {
            warn!(elapsed = ?start.elapsed(), "giving up reconnecting to rtrouted");
            return None;
        }
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => break,
                out = rx.recv() => match out {
                    Some(Outgoing::Frame(..)) => {}
                    Some(Outgoing::Shutdown) | None => return None,
                },
            }
        }
        delay = retry.next_delay(delay);
    }
}

async fn reader(mut read: OwnedReadHalf, core: Arc<Core>) {
    let mut decoder = FrameDecoder::with_limits(&core.limits);
    'read: loop {
        match read.read_buf(decoder.buffer_mut()).await {
            Ok(0) => {
                debug!("rtrouted closed the connection");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "read from rtrouted failed");
                break;
            }
        }
        loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => core.dispatch(frame).await,
                Ok(None) => break,
                Err(e) => {
                    // The stream can't be resynchronised after a bad header.
                    warn!(error = %e, "undecodable frame from rtrouted; dropping the socket");
                    break 'read;
                }
            }
        }
    }
}

/// Waits for connect advisories and, once they stop for a moment, re-sends
/// every subscription.
async fn resubscriber(core: Weak<Core>, notify: Arc<Notify>) {
    loop {
        notify.notified().await;
        while tokio::time::timeout(RESUBSCRIBE_DEBOUNCE, notify.notified())
            .await
            .is_ok()
        {}
        let Some(core) = core.upgrade() else { return };
        crate::subscribe::resubscribe_all(&core).await;
    }
}
