//! The connection engine: one unix socket to rtrouted, a reader task, a writer
//! task, and the tables that route what arrives.
//!
//! - **Writes** go through an unbounded channel to the writer task, so sending
//!   never blocks a caller and frames leave in the order they were queued.
//! - **Reads** happen in the reader task. It matches each response to its
//!   pending request by sequence number (wire-protocol.md section 3), routes
//!   each event to its subscription by the echoed component id, fans out
//!   broker advisories, and hands each request for our providers to a task
//!   of its own (`provider::serve`).
//! - **Cancellation**: a pending request is removed from the table when its
//!   [`Pending`] is dropped, so a cancelled or timed-out call leaves nothing
//!   behind and a late response is discarded.
//! - **Resubscription**: a connect advisory (another client joined the bus)
//!   wakes a debounced task that re-sends every subscription, because a C
//!   provider that restarted has forgotten ours (decisions.md D11).

use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use bytes::Bytes;
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
use crate::error::{Error, Result};
use crate::queue::Queue;

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
    /// Closed, by [`crate::Rbus::close`] or because the broker went away.
    Closed,
}

enum Outgoing {
    Frame(Bytes),
    /// Flush, then shut down the write side of the socket.
    Shutdown,
}

/// A live subscription, as the reader task sees it.
pub(crate) struct SubEntry {
    /// The name subscribed to: the provider's routing key, maybe a wildcard.
    pub(crate) event_name: String,
    pub(crate) queue: Arc<Queue<Event>>,
    /// False while the subscribe request is in flight; only ready
    /// subscriptions are re-sent after an advisory.
    pub(crate) ready: bool,
}

/// Everything the connection's tasks share.
pub(crate) struct Core {
    pending: Mutex<HashMap<u32, oneshot::Sender<Frame>>>,
    state: watch::Sender<ConnectionState>,
    tx: mpsc::UnboundedSender<Outgoing>,
    seq: AtomicU32,
    inbox: String,
    pub(crate) timeouts: Timeouts,
    subs: Mutex<HashMap<i32, SubEntry>>,
    next_sub_id: AtomicI32,
    advisory_listeners: Mutex<Vec<Weak<Queue<Advisory>>>>,
    advisories_registered: AtomicBool,
    resubscribe: Arc<Notify>,
    /// The providers served on this connection.
    pub(crate) registry: crate::provider::Registry,
    /// The component's route id; element aliases are registered under it.
    pub(crate) component_route: u32,
    /// The component name.
    pub(crate) component: String,
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

    fn next_seq(&self) -> u32 {
        loop {
            // Zero is never used, as in C.
            let seq = self.seq.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            if seq != 0 {
                return seq;
            }
        }
    }

    /// Queues a frame for the writer task.
    pub(crate) fn queue(&self, frame: &Frame) -> Result<()> {
        let bytes = frame.encode()?;
        self.tx
            .send(Outgoing::Frame(bytes))
            .map_err(|_| Error::Disconnected)
    }

    /// Sends a JSON control message with no flags (no reply expected).
    pub(crate) fn send_control<T: serde::Serialize>(&self, msg: &T) -> Result<()> {
        let header = Header::new(self.next_seq(), Flags::NONE, control::TOPIC_SUBSCRIBE, "");
        self.queue(&Frame::new(header, control::encode(msg)))
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
        if self.is_closed() {
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
        self.queue(&frame)?;
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
        self.request(topic, Flags::NONE, control::encode(msg))
    }

    /// Registers for `_RTROUTED.ADVISORY` the first time anything needs it.
    pub(crate) fn ensure_advisories(&self) -> Result<()> {
        if !self.advisories_registered.swap(true, Ordering::AcqRel) {
            self.send_control(&Route::add(control::TOPIC_ADVISORY, ADVISORY_ROUTE_ID))?;
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

    pub(crate) fn remove_sub(&self, id: i32) -> Option<SubEntry> {
        lock(&self.subs).remove(&id)
    }

    pub(crate) fn mark_ready(&self, id: i32) {
        if let Some(sub) = lock(&self.subs).get_mut(&id) {
            sub.ready = true;
        }
    }

    /// `(id, event name)` of every subscription that has been set up.
    pub(crate) fn sub_names(&self) -> Vec<(i32, String)> {
        lock(&self.subs)
            .iter()
            .filter(|(_, s)| s.ready)
            .map(|(id, s)| (*id, s.event_name.clone()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending().len()
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
            tokio::spawn(crate::provider::serve(self.clone(), frame));
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
        let decoded = split_event_payload(&frame.payload)
            .and_then(|(mut body, meta)| Ok((Event::decode(&mut body)?, meta)));
        let (event, meta) = match decoded {
            Ok(e) => e,
            Err(e) => return warn!(error = %e, topic = %frame.header.topic, "undecodable event"),
        };
        let queue = {
            let subs = lock(&self.subs);
            subs.get(&event.component_id)
                .filter(|s| s.event_name == meta.event_name)
                .or_else(|| subs.values().find(|s| s.event_name == meta.event_name))
                .map(|s| s.queue.clone())
        };
        match queue {
            Some(q) => q.push(event).await,
            None => trace!(name = %meta.event_name, "event for no subscription"),
        }
    }
}

/// A connection to rtrouted, registered as one component.
pub(crate) struct Conn {
    core: Arc<Core>,
    reader: JoinHandle<()>,
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
    pub(crate) async fn connect(
        socket: &Path,
        component: &str,
        timeouts: Timeouts,
    ) -> Result<Conn> {
        let stream = connect_socket(socket, timeouts.broker).await?;
        let (read, write) = stream.into_split();
        let (state, _) = watch::channel(ConnectionState::Connected);
        let (tx, rx) = mpsc::unbounded_channel();
        let resubscribe = Arc::new(Notify::new());
        let core = Arc::new(Core {
            pending: Mutex::new(HashMap::new()),
            state,
            tx,
            seq: AtomicU32::new(0),
            inbox: inbox_name(),
            timeouts,
            subs: Mutex::new(HashMap::new()),
            next_sub_id: AtomicI32::new(1),
            advisory_listeners: Mutex::new(Vec::new()),
            advisories_registered: AtomicBool::new(false),
            resubscribe: resubscribe.clone(),
            registry: crate::provider::Registry::default(),
            component_route: NEXT_COMPONENT_ROUTE.fetch_add(1, Ordering::Relaxed),
            component: component.to_owned(),
        });
        tokio::spawn(writer(write, rx, Arc::downgrade(&core)));
        let reader = tokio::spawn(reader(read, core.clone()));
        let resubscriber = tokio::spawn(resubscriber(Arc::downgrade(&core), resubscribe));
        let conn = Conn {
            core,
            reader,
            resubscriber,
        };
        conn.send_control(&Route::add(conn.inbox.clone(), INBOX_ROUTE_ID))?;
        conn.send_control(&Route::add(conn.component.clone(), conn.component_route))?;
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
            self.reader.abort();
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
    /// Best-effort cleanup (L13): queue the unregistration, then let the writer
    /// flush it and close the socket once the core is gone.
    fn drop(&mut self) {
        if !self.is_closed() {
            self.unregister();
        }
        self.reader.abort();
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

async fn writer(
    mut write: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Outgoing>,
    core: Weak<Core>,
) {
    while let Some(out) = rx.recv().await {
        match out {
            Outgoing::Frame(bytes) => {
                if let Err(e) = write.write_all(&bytes).await {
                    warn!(error = %e, "write to rtrouted failed");
                    if let Some(core) = core.upgrade() {
                        core.close();
                    }
                    return;
                }
            }
            Outgoing::Shutdown => break,
        }
    }
    // Either close() asked for it or every handle is gone.
    let _ = write.shutdown().await;
}

async fn reader(mut read: OwnedReadHalf, core: Arc<Core>) {
    let mut decoder = FrameDecoder::new();
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
                    warn!(error = %e, "undecodable frame from rtrouted; closing");
                    break 'read;
                }
            }
        }
    }
    core.close();
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
