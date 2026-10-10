//! The [`Rbus`] handle and its builder.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::conn::{Conn, ConnectionState};
use crate::error::Result;
use crate::name::check_component;

/// The broker socket C clients use by default (`rbuscore.c:322`).
pub const DEFAULT_SOCKET: &str = "/tmp/rtrouted";

/// Default timeouts per operation (L6). The defaults match the C library's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// `get` and `get_many`; 15 s.
    pub get: Duration,
    /// `set`; 15 s.
    pub set: Duration,
    /// Each provider's part of a `set_many`; 60 s.
    pub set_many: Duration,
    /// `invoke`; 15 s.
    pub invoke: Duration,
    /// Queries to the broker itself, such as owner lookups; 5 s.
    pub broker: Duration,
    /// `subscribe` and `unsubscribe`, per attempt; 15 s.
    pub subscribe: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            get: Duration::from_secs(15),
            set: Duration::from_secs(15),
            set_many: Duration::from_secs(60),
            invoke: Duration::from_secs(15),
            broker: Duration::from_secs(5),
            subscribe: Duration::from_secs(15),
        }
    }
}

impl Timeouts {
    /// The same timeout for every operation.
    pub fn uniform(d: Duration) -> Self {
        Timeouts {
            get: d,
            set: d,
            set_many: d,
            invoke: d,
            broker: d,
            subscribe: d,
        }
    }
}

/// A connection to the bus, registered as one component.
///
/// `Rbus` is cheap to clone, and every clone shares the connection, so hand it
/// to as many tasks as you like. The connection closes on [`Rbus::close`], or
/// when the last clone is dropped.
///
/// ```no_run
/// # async fn demo() -> rbus::Result<()> {
/// use rbus::Rbus;
///
/// let bus = Rbus::connect("com.example.app").await?;
/// let uptime: u32 = bus.get("Device.DeviceInfo.UpTime").await?;
/// bus.close().await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Rbus {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) conn: Conn,
    pub(crate) timeouts: Timeouts,
}

impl Rbus {
    /// Connects to the broker at [`DEFAULT_SOCKET`] as `component`.
    pub async fn connect(component: &str) -> Result<Rbus> {
        Rbus::builder(component).connect().await
    }

    /// Starts configuring a connection for `component`.
    pub fn builder(component: impl Into<String>) -> RbusBuilder {
        RbusBuilder {
            component: component.into(),
            socket: PathBuf::from(DEFAULT_SOCKET),
            timeouts: Timeouts::default(),
        }
    }

    /// This handle's component name.
    pub fn component(&self) -> &str {
        self.inner.conn.component()
    }

    /// The inbox the broker routes replies to: `rbus.<prog>-<n>.INBOX.<pid>`.
    pub fn inbox(&self) -> &str {
        self.inner.conn.inbox()
    }

    /// The default timeouts in effect.
    pub fn timeouts(&self) -> &Timeouts {
        &self.inner.timeouts
    }

    /// Watches the connection state.
    pub fn state(&self) -> watch::Receiver<ConnectionState> {
        self.inner.conn.state()
    }

    /// Unregisters from the broker and closes the connection, for every clone
    /// of this handle (L13). Later calls fail with [`crate::Error::Disconnected`].
    /// Closing twice is fine.
    pub async fn close(&self) -> Result<()> {
        self.inner.conn.close(self.inner.timeouts.broker).await
    }
}

impl fmt::Debug for Rbus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rbus")
            .field("component", &self.component())
            .field("inbox", &self.inbox())
            .field("state", &*self.state().borrow())
            .finish()
    }
}

/// Configures and opens a connection. Made by [`Rbus::builder`].
#[derive(Debug, Clone)]
#[must_use = "a builder does nothing until .connect().await"]
pub struct RbusBuilder {
    component: String,
    socket: PathBuf,
    timeouts: Timeouts,
}

impl RbusBuilder {
    /// The broker's unix socket (L1). Defaults to [`DEFAULT_SOCKET`].
    pub fn socket(mut self, path: impl Into<PathBuf>) -> Self {
        self.socket = path.into();
        self
    }

    /// One timeout for every operation (L6).
    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts = Timeouts::uniform(timeout);
        self
    }

    /// Per-operation timeouts (L6).
    pub fn timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Connects and registers the component.
    pub async fn connect(self) -> Result<Rbus> {
        check_component(&self.component)?;
        let conn = Conn::connect(&self.socket, &self.component, self.timeouts).await?;
        Ok(Rbus {
            inner: Arc::new(Inner {
                conn,
                timeouts: self.timeouts,
            }),
        })
    }
}
