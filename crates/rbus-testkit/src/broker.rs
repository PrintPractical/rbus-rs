//! Helpers for interop tests that run against a real `rtrouted`.
//!
//! Interop tests are marked `#[ignore = "needs rtrouted: run scripts/interop.sh"]`.
//! `scripts/interop.sh` starts the broker inside the interop container and runs
//! the suite with `--include-ignored`. Running them anywhere else fails loudly
//! instead of passing silently.

use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::frame::{Frame, encode_frame, read_frame};

/// Request flag.
pub const FLAG_REQUEST: u32 = 0x01;
/// Response flag.
pub const FLAG_RESPONSE: u32 = 0x02;
/// Set by the broker on a response when no route matched the request.
pub const FLAG_UNDELIVERABLE: u32 = 0x04;

/// Broker control topic for adding and removing routes.
pub const TOPIC_SUBSCRIBE: &str = "_RTROUTED.INBOX.SUBSCRIBE";
/// Broker control topic that resolves a partial path to the routes under it.
pub const TOPIC_QUERY: &str = "_RTROUTED.INBOX.QUERY";
/// Broker control topic that maps exact element names to their owners.
pub const TOPIC_TRACE_ORIGIN: &str = "_trace_origin_object";
/// Broker control topic that lists every registered route.
pub const TOPIC_REGISTERED_COMPONENTS: &str = "_registered_components";

/// The broker socket: `$RBUS_BROKER_SOCKET`, or `/tmp/rtrouted`.
pub fn broker_socket() -> PathBuf {
    std::env::var_os("RBUS_BROKER_SOCKET").map_or_else(|| "/tmp/rtrouted".into(), PathBuf::from)
}

/// Connects to the broker, panicking with instructions if it isn't running.
pub fn require_broker() -> UnixStream {
    let path = broker_socket();
    UnixStream::connect(&path).unwrap_or_else(|e| {
        panic!(
            "cannot connect to rtrouted at {}: {e}. Interop tests run inside the \
             interop container: use scripts/interop.sh",
            path.display()
        )
    })
}

/// A bare-bones client that speaks just enough of the protocol to drive the
/// broker in tests (JSON control messages only). The real client is `rbus`.
pub struct RawClient {
    stream: UnixStream,
    inbox: String,
    seq: u32,
}

impl RawClient {
    /// Connects and registers `inbox` as route 1. The inbox name should end in
    /// `.<pid>`, as C providers expect.
    pub fn connect(inbox: &str) -> io::Result<Self> {
        let mut client = RawClient {
            stream: require_broker(),
            inbox: inbox.to_owned(),
            seq: 0,
        };
        client
            .stream
            .set_read_timeout(Some(Duration::from_secs(5)))?;
        let payload = format!("{{\"add\":1,\"topic\":\"{inbox}\",\"route_id\":1}}");
        client.send(0, TOPIC_SUBSCRIBE, "", payload.as_bytes())?;
        Ok(client)
    }

    /// This client's inbox.
    pub fn inbox(&self) -> &str {
        &self.inbox
    }

    /// Sends one frame with the next sequence number; returns that number.
    pub fn send(
        &mut self,
        flags: u32,
        topic: &str,
        reply_topic: &str,
        payload: &[u8],
    ) -> io::Result<u32> {
        self.seq += 1;
        let frame = encode_frame(self.seq, flags, 0, topic, reply_topic, payload);
        self.stream.write_all(&frame)?;
        Ok(self.seq)
    }

    /// Reads the next frame (5 s timeout).
    pub fn recv(&mut self) -> io::Result<Frame> {
        read_frame(&mut self.stream)?.ok_or_else(|| io::ErrorKind::UnexpectedEof.into())
    }

    /// Sends a request to `topic` and waits for the response with the same seq.
    pub fn request(&mut self, topic: &str, payload: &[u8]) -> io::Result<Frame> {
        let inbox = self.inbox.clone();
        let seq = self.send(FLAG_REQUEST, topic, &inbox, payload)?;
        loop {
            let frame = self.recv()?;
            if frame.header.flags & FLAG_RESPONSE != 0 && frame.header.seq == seq {
                return Ok(frame);
            }
        }
    }

    /// Sends a JSON control request and returns the JSON reply text.
    pub fn control(&mut self, topic: &str, json: &str) -> io::Result<String> {
        let reply = self.request(topic, json.as_bytes())?;
        Ok(String::from_utf8_lossy(reply.payload())
            .trim_end_matches('\0')
            .to_owned())
    }

    /// Routes under a partial path such as `Device.SampleProvider.` (the broker's
    /// `_RTROUTED.INBOX.QUERY`; exact element names resolve to nothing).
    pub fn query(&mut self, partial_path: &str) -> io::Result<String> {
        self.control(
            TOPIC_QUERY,
            &format!("{{\"expression\":\"{partial_path}\"}}"),
        )
    }

    /// The component that owns an exact element name (`_trace_origin_object`).
    pub fn owner_of(&mut self, element: &str) -> io::Result<String> {
        self.control(
            TOPIC_TRACE_ORIGIN,
            &format!("{{\"count\":1,\"items\":[\"{element}\"]}}"),
        )
    }

    /// Every registered route: inboxes and component names (`_registered_components`).
    pub fn registered_components(&mut self) -> io::Result<String> {
        // Never send an empty payload: rtrouted only dispatches a frame once its
        // socket is readable again, so a zero-length payload stalls until the next
        // frame. C clients send this dummy body for the same reason.
        self.control(TOPIC_REGISTERED_COMPONENTS, "{\"dummy\":0}")
    }

    /// Polls [`RawClient::owner_of`] until `owner` owns `element`, for example
    /// after starting a C provider.
    pub fn wait_for_owner(
        &mut self,
        element: &str,
        owner: &str,
        timeout: Duration,
    ) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.owner_of(element)?.contains(&format!("\"{owner}\"")) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{owner} did not register {element} within {timeout:?}"),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A C peer process (sample provider, rbuscli, test consumer) that is killed
/// when dropped.
pub struct Peer {
    child: Child,
}

impl Peer {
    /// Starts `program` (looked up on `PATH`) with `args`, output discarded.
    pub fn spawn(program: &str, args: &[&str]) -> io::Result<Self> {
        let child = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Peer { child })
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
