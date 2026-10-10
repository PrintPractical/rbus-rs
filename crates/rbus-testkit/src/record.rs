//! Recording proxy: sits between clients and `rtrouted` and logs every frame.
//!
//! Clients connect to the proxy's listen socket (normally `/tmp/rtrouted`, where
//! C clients look for the broker). For each client the proxy opens a connection
//! to the real broker (`--upstream`, for example `/tmp/rtrouted.real`) and copies
//! frames in both directions, writing each one as a JSON line:
//!
//! ```json
//! {"n":3,"conn":1,"dir":"to_broker","seq":2,"flags":1,"control_data":0,
//!  "topic":"_RTROUTED.INBOX.SUBSCRIBE","reply_topic":"rbus.x.INBOX.12",
//!  "header_len":76,"fixed_header":32,"payload_len":45,"frame":"aaaa0002..."}
//! ```
//!
//! `frame` is the complete frame (header and payload) in lower-case hex.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::frame::{Frame, read_frame};

/// Which way a frame travelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Client to broker.
    ToBroker,
    /// Broker to client.
    FromBroker,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Direction::ToBroker => "to_broker",
            Direction::FromBroker => "from_broker",
        }
    }
}

/// Shared, ordered JSON-lines log of frames.
pub struct FrameLog {
    out: Mutex<Box<dyn Write + Send>>,
    next: AtomicU64,
}

impl FrameLog {
    /// Logs to any writer.
    pub fn new(out: impl Write + Send + 'static) -> Self {
        FrameLog {
            out: Mutex::new(Box::new(out)),
            next: AtomicU64::new(1),
        }
    }

    /// Logs to a newly created file.
    pub fn create(path: &Path) -> io::Result<Self> {
        Ok(Self::new(BufWriter::new(File::create(path)?)))
    }

    /// Appends one frame and flushes, so a killed recorder loses nothing.
    pub fn log(&self, conn: u64, dir: Direction, frame: &Frame) -> io::Result<()> {
        let mut out = self.out.lock().unwrap_or_else(|e| e.into_inner());
        let n = self.next.fetch_add(1, Ordering::SeqCst);
        writeln!(out, "{}", json_line(n, conn, dir, frame))?;
        out.flush()
    }
}

/// Formats one log line. Kept separate so it can be unit-tested.
pub fn json_line(n: u64, conn: u64, dir: Direction, frame: &Frame) -> String {
    let h = &frame.header;
    format!(
        concat!(
            "{{\"n\":{},\"conn\":{},\"dir\":\"{}\",\"seq\":{},\"flags\":{},\"control_data\":{},",
            "\"topic\":{},\"reply_topic\":{},\"header_len\":{},\"fixed_header\":{},",
            "\"payload_len\":{},\"frame\":\"{}\"}}"
        ),
        n,
        conn,
        dir.as_str(),
        h.seq,
        h.flags,
        h.control_data,
        json_string(&h.topic),
        json_string(&h.reply_topic),
        h.header_len,
        h.fixed_len(),
        h.payload_len,
        hex(&frame.bytes),
    )
}

/// Lower-case hex encoding.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from(DIGITS[usize::from(b >> 4)]));
        s.push(char::from(DIGITS[usize::from(b & 0x0f)]));
    }
    s
}

/// A JSON string literal, with escaping.
pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Copies frames from `from` to `to`, logging each one. When `from` closes or
/// fails, both sockets are shut down completely: rtrouted connections never
/// half-close, and this also unblocks the pump running the other way.
pub fn pump(
    conn: u64,
    dir: Direction,
    mut from: UnixStream,
    mut to: UnixStream,
    log: &FrameLog,
) -> io::Result<()> {
    let result = (|| {
        while let Some(frame) = read_frame(&mut from)? {
            log.log(conn, dir, &frame)?;
            to.write_all(&frame.bytes)?;
        }
        Ok(())
    })();
    let _ = to.shutdown(Shutdown::Both);
    let _ = from.shutdown(Shutdown::Both);
    result
}

/// Accepts clients on `listen` forever, proxying each to `upstream`.
pub fn run(listen: &Path, upstream: &Path, log: Arc<FrameLog>) -> io::Result<()> {
    let _ = std::fs::remove_file(listen);
    let listener = UnixListener::bind(listen)?;
    let mut conn = 0u64;
    for client in listener.incoming() {
        let client = client?;
        conn += 1;
        let broker = match UnixStream::connect(upstream) {
            Ok(b) => b,
            Err(e) => {
                eprintln!(
                    "rbus-record: conn {conn}: cannot reach upstream {}: {e}",
                    upstream.display()
                );
                continue;
            }
        };
        for (dir, from, to) in [
            (
                Direction::ToBroker,
                client.try_clone()?,
                broker.try_clone()?,
            ),
            (Direction::FromBroker, broker, client),
        ] {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                if let Err(e) = pump(conn, dir, from, to, &log) {
                    eprintln!("rbus-record: conn {conn} {}: {e}", dir.as_str());
                }
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{encode_frame, parse_header};

    #[test]
    fn hex_and_json_escaping() {
        assert_eq!(hex(&[0x00, 0xab, 0x1f]), "00ab1f");
        assert_eq!(json_string("a\"b\\c\n\u{1}"), "\"a\\\"b\\\\c\\n\\u0001\"");
    }

    #[test]
    fn formats_a_log_line() {
        let bytes = encode_frame(5, 1, 0, "_RTROUTED.INBOX.SUBSCRIBE", "", b"{}");
        let frame = Frame {
            header: parse_header(&bytes).unwrap(),
            bytes,
        };
        let line = json_line(9, 2, Direction::ToBroker, &frame);
        assert!(
            line.starts_with("{\"n\":9,\"conn\":2,\"dir\":\"to_broker\",\"seq\":5,\"flags\":1,")
        );
        assert!(line.contains("\"topic\":\"_RTROUTED.INBOX.SUBSCRIBE\""));
        assert!(line.contains("\"fixed_header\":32,\"payload_len\":2,"));
        assert!(line.ends_with(&format!("\"frame\":\"{}\"}}", hex(&frame.bytes))));
    }

    #[test]
    fn proxies_and_logs_both_directions() {
        let dir = std::env::temp_dir().join(format!("rbus-record-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (listen, upstream) = (dir.join("listen.sock"), dir.join("upstream.sock"));
        let _ = std::fs::remove_file(&upstream);
        let fake_broker = UnixListener::bind(&upstream).unwrap();

        let lines = Arc::new(Mutex::new(Vec::<u8>::new()));
        struct Shared(Arc<Mutex<Vec<u8>>>);
        impl Write for Shared {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let log = Arc::new(FrameLog::new(Shared(Arc::clone(&lines))));
        let (l, u) = (listen.clone(), upstream.clone());
        thread::spawn(move || run(&l, &u, log));

        let mut client = loop {
            match UnixStream::connect(&listen) {
                Ok(c) => break c,
                Err(_) => thread::sleep(std::time::Duration::from_millis(10)),
            }
        };
        let (mut broker_side, _) = fake_broker.accept().unwrap();
        let timeout = Some(std::time::Duration::from_secs(5));
        broker_side.set_read_timeout(timeout).unwrap();
        client.set_read_timeout(timeout).unwrap();

        let request = encode_frame(1, 1, 0, "Device.X", "rbus.t.INBOX.1", b"req");
        client.write_all(&request).unwrap();
        let got = read_frame(&mut broker_side).unwrap().unwrap();
        assert_eq!(got.bytes, request);

        let reply = encode_frame(1, 2, 1, "rbus.t.INBOX.1", "Device.X", b"rep");
        broker_side.write_all(&reply).unwrap();
        let got = read_frame(&mut client).unwrap().unwrap();
        assert_eq!(got.bytes, reply);

        drop(client);
        assert!(
            read_frame(&mut broker_side).unwrap().is_none(),
            "close is forwarded"
        );

        let text = String::from_utf8(lines.lock().unwrap().clone()).unwrap();
        let logged: Vec<&str> = text.lines().collect();
        assert_eq!(logged.len(), 2);
        assert!(logged[0].contains("\"dir\":\"to_broker\""));
        assert!(logged[1].contains("\"dir\":\"from_broker\""));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
