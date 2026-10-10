//! The connection engine: one unix socket to rtrouted, a reader task, a writer
//! task, and the table of requests waiting for a response.
//!
//! - **Writes** go through an unbounded channel to the writer task, so sending
//!   never blocks a caller and frames leave in the order they were queued.
//! - **Reads** happen in the reader task, which matches each response to its
//!   pending request by sequence number (wire-protocol.md section 3).
//! - **Cancellation**: a pending request is removed from the table when its
//!   [`Pending`] is dropped, so a cancelled or timed-out call leaves nothing
//!   behind and a late response is discarded.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rbus_wire::control::{self, Route};
use rbus_wire::frame::{Flags, Frame, FrameDecoder, Header};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use crate::error::{Error, Result};

/// Route id of the inbox (`rtConnection.c:680`).
const INBOX_ROUTE_ID: u32 = 1;
/// Component route ids come from a process-wide counter starting here, as in C.
static NEXT_COMPONENT_ROUTE: AtomicU32 = AtomicU32::new(10_000);
/// Numbers the connections of this process, to keep inbox names unique.
static NEXT_CONNECTION: AtomicU32 = AtomicU32::new(0);

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

/// State shared between the handle and the reader task.
struct Shared {
    pending: Mutex<HashMap<u32, oneshot::Sender<Frame>>>,
    state: watch::Sender<ConnectionState>,
}

impl Shared {
    fn pending(&self) -> std::sync::MutexGuard<'_, HashMap<u32, oneshot::Sender<Frame>>> {
        // The lock is never held across a panic point, but stay usable anyway.
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn close(&self) {
        self.state.send_replace(ConnectionState::Closed);
        // Dropping the senders wakes every waiter with `Disconnected`.
        self.pending().clear();
    }
}

/// A connection to rtrouted, registered as one component.
pub(crate) struct Conn {
    shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Outgoing>,
    seq: AtomicU32,
    inbox: String,
    component: String,
    component_route: u32,
    reader: JoinHandle<()>,
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
    pub(crate) async fn connect(socket: &Path, component: &str, wait: Duration) -> Result<Conn> {
        let stream = connect_socket(socket, wait).await?;
        let (read, write) = stream.into_split();
        let (state, _) = watch::channel(ConnectionState::Connected);
        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
            state,
        });
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(writer(write, rx, shared.clone()));
        let reader = tokio::spawn(reader(read, shared.clone()));
        let conn = Conn {
            shared,
            tx,
            seq: AtomicU32::new(0),
            inbox: inbox_name(),
            component: component.to_owned(),
            component_route: NEXT_COMPONENT_ROUTE.fetch_add(1, Ordering::Relaxed),
            reader,
        };
        conn.send_control(&Route::add(conn.inbox.clone(), INBOX_ROUTE_ID))?;
        conn.send_control(&Route::add(conn.component.clone(), conn.component_route))?;
        debug!(inbox = %conn.inbox, component = %conn.component, "connected to rtrouted");
        Ok(conn)
    }

    pub(crate) fn component(&self) -> &str {
        &self.component
    }

    pub(crate) fn inbox(&self) -> &str {
        &self.inbox
    }

    pub(crate) fn state(&self) -> watch::Receiver<ConnectionState> {
        self.shared.state.subscribe()
    }

    fn is_closed(&self) -> bool {
        *self.shared.state.borrow() == ConnectionState::Closed
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

    fn queue(&self, frame: &Frame) -> Result<()> {
        let bytes = frame.encode()?;
        self.tx
            .send(Outgoing::Frame(bytes))
            .map_err(|_| Error::Disconnected)
    }

    /// Sends a JSON control message with no flags (no reply expected).
    fn send_control<T: serde::Serialize>(&self, msg: &T) -> Result<()> {
        let header = Header::new(self.next_seq(), Flags::NONE, control::TOPIC_SUBSCRIBE, "");
        self.queue(&Frame::new(header, control::encode(msg)))
    }

    /// Sends a request and returns a handle to await its response. The request
    /// is registered before it is sent, so the response can't be missed.
    pub(crate) fn request(&self, topic: &str, flags: Flags, payload: Vec<u8>) -> Result<Pending> {
        if self.is_closed() {
            return Err(Error::Disconnected);
        }
        let seq = self.next_seq();
        let frame = Frame::new(
            Header::new(seq, Flags::REQUEST | flags, topic, &self.inbox),
            payload,
        );
        let (tx, rx) = oneshot::channel();
        self.shared.pending().insert(seq, tx);
        let pending = Pending {
            rx,
            seq,
            shared: self.shared.clone(),
        };
        self.queue(&frame)?;
        trace!(seq, topic, "request sent");
        Ok(pending)
    }

    /// Sends an rbus (msgpack) request.
    pub(crate) fn rbus_request(&self, topic: &str, payload: Vec<u8>) -> Result<Pending> {
        self.request(topic, Flags::RAW_BINARY, payload)
    }

    /// Sends a JSON request to a broker topic.
    pub(crate) fn broker_request<T: serde::Serialize>(
        &self,
        topic: &str,
        msg: &T,
    ) -> Result<Pending> {
        self.request(topic, Flags::NONE, control::encode(msg))
    }

    /// Unregisters and closes the socket, then waits (up to `wait`) for the
    /// broker to close its side. Calling it again is a no-op.
    pub(crate) async fn close(&self, wait: Duration) -> Result<()> {
        let mut state = self.state();
        if self.is_closed() {
            return Ok(());
        }
        self.unregister();
        // A send error means the writer is already gone; so is the socket.
        let _ = self.tx.send(Outgoing::Shutdown);
        if tokio::time::timeout(wait, state.wait_for(|s| *s == ConnectionState::Closed))
            .await
            .is_err()
        {
            warn!("broker did not close the connection; dropping it");
            self.reader.abort();
            self.shared.close();
        }
        debug!(component = %self.component, "connection closed");
        Ok(())
    }

    fn unregister(&self) {
        // Best effort: the broker drops every route of a closed socket anyway.
        let _ = self.send_control(&Route::remove(self.component.clone(), self.component_route));
        let _ = self.send_control(&Route::remove(self.inbox.clone(), INBOX_ROUTE_ID));
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.shared.pending().len()
    }
}

impl Drop for Conn {
    /// Best-effort cleanup (L13): queue the unregistration, then let the writer
    /// flush it and close the socket once this handle's sender is gone.
    fn drop(&mut self) {
        if !self.is_closed() {
            self.unregister();
        }
        self.reader.abort();
    }
}

/// A request waiting for its response.
pub(crate) struct Pending {
    rx: oneshot::Receiver<Frame>,
    seq: u32,
    shared: Arc<Shared>,
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
        self.shared.pending().remove(&self.seq);
    }
}

async fn writer(
    mut write: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Outgoing>,
    shared: Arc<Shared>,
) {
    while let Some(out) = rx.recv().await {
        match out {
            Outgoing::Frame(bytes) => {
                if let Err(e) = write.write_all(&bytes).await {
                    warn!(error = %e, "write to rtrouted failed");
                    shared.close();
                    return;
                }
            }
            Outgoing::Shutdown => break,
        }
    }
    // Either close() asked for it or every handle is gone.
    let _ = write.shutdown().await;
}

async fn reader(mut read: OwnedReadHalf, shared: Arc<Shared>) {
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
                Ok(Some(frame)) => dispatch(&shared, frame),
                Ok(None) => break,
                Err(e) => {
                    // The stream can't be resynchronised after a bad header.
                    warn!(error = %e, "undecodable frame from rtrouted; closing");
                    break 'read;
                }
            }
        }
    }
    shared.close();
}

fn dispatch(shared: &Shared, frame: Frame) {
    let flags = frame.header.flags;
    if flags.contains(Flags::RESPONSE) {
        let seq = frame.header.seq;
        match shared.pending().remove(&seq) {
            // The waiter may have just given up; that's fine.
            Some(tx) => drop(tx.send(frame)),
            None => trace!(seq, "late or unknown response dropped"),
        }
    } else {
        // Requests and events for this component arrive with the provider and
        // subscription engines (M3, M4).
        trace!(topic = %frame.header.topic, ?flags, "unhandled inbound message");
    }
}
