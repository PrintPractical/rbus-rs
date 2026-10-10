//! Shared fixtures for the interop tests. Every test using them is
//! `#[ignore = "needs rtrouted: run scripts/interop.sh"]` and runs on a
//! multi-threaded runtime, because the fixtures block while C peers start.
#![allow(dead_code)]

pub mod model;

use std::sync::Arc;
use std::time::Duration;

use rbus::Rbus;
use rbus_testkit::broker::{Peer, RawClient, broker_socket, shared_peer, test_inbox};

/// `rbusSampleProvider`, which also opens `Second-Provider-for-Sample` for
/// `Device.SampleProvider.AllTypes.*`.
pub fn sample_provider() -> Arc<Peer> {
    shared_peer(
        "rbusSampleProvider",
        &["3600"],
        "Device.SampleProvider.AllTypes.BytesData",
        "Second-Provider-for-Sample",
    )
}

/// `rbusTestProvider` (`Device.TestProvider.*`), rbus's own conformance provider.
pub fn test_provider() -> Arc<Peer> {
    shared_peer(
        "rbusTestProvider",
        &["-t", "3600"],
        "Device.TestProvider.BigBytes",
        "TestProvider",
    )
}

/// A connection with a component name unique to this test.
pub async fn connect(label: &str) -> Rbus {
    Rbus::builder(format!("rbus-rs.test.{label}"))
        .socket(broker_socket())
        .connect()
        .await
        .expect("connect to rtrouted")
}

/// A raw client for checking what the broker sees.
pub fn raw_client() -> RawClient {
    RawClient::connect(&test_inbox("check")).expect("connect to rtrouted")
}

/// Polls `_registered_components` until `pred` holds for its JSON reply.
pub fn wait_for_routes(pred: impl Fn(&str) -> bool) -> String {
    let mut client = raw_client();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let routes = client.registered_components().unwrap();
        if pred(&routes) || std::time::Instant::now() > deadline {
            return routes;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// True if the `_registered_components` reply lists `route` exactly.
pub fn lists(routes: &str, route: &str) -> bool {
    routes.contains(&format!("\"{route}\""))
}

/// Waits up to `secs` seconds for the next item of a stream.
pub async fn within<F: std::future::Future>(secs: u64, f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(secs), f)
        .await
        .expect("timed out waiting for the bus")
}

/// Runs `rbuscli` with `args` and returns what it printed. `rbuscli` exits
/// with 0 whatever happens, so tests check its output.
pub async fn rbuscli(args: &[&str]) -> String {
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new("rbuscli")
            .args(&args)
            .output()
            .expect("run rbuscli")
    })
    .await
    .unwrap();
    String::from_utf8_lossy(&out.stdout).replace('\r', "")
}

/// The `Name` / `Value` pairs `rbuscli get` and method calls print.
pub fn rbuscli_values(out: &str) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut name = None;
    for line in out.lines() {
        let line = line.trim();
        if let Some(n) = line.strip_prefix("Name  : ") {
            name = Some(n.to_owned());
        } else if let Some(v) = line.strip_prefix("Value : ") {
            if let Some(n) = name.take() {
                found.push((n, v.to_owned()));
            }
        }
    }
    found
}

/// An interactive `rbuscli -i` session: commands go to its stdin, and its
/// output is collected until [`Rbuscli::finish`].
pub struct Rbuscli {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    out: std::thread::JoinHandle<String>,
}

impl Rbuscli {
    pub fn start() -> Rbuscli {
        use std::io::Read;
        use std::process::{Command, Stdio};
        let mut child = Command::new("rbuscli")
            .arg("-i")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("run rbuscli -i");
        let stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let out = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = stdout.read_to_string(&mut s);
            s.replace('\r', "")
        });
        Rbuscli { child, stdin, out }
    }

    /// Sends one command line.
    pub fn send(&mut self, line: &str) {
        use std::io::Write;
        writeln!(self.stdin, "{line}").unwrap();
        self.stdin.flush().unwrap();
    }

    /// Quits and returns everything it printed.
    pub async fn finish(mut self) -> String {
        self.send("quit");
        tokio::task::spawn_blocking(move || {
            drop(self.stdin);
            let _ = self.child.wait();
            self.out.join().unwrap()
        })
        .await
        .unwrap()
    }
}

/// The events `rbuscli` printed, one block per event: the "Event received"
/// line and the data lines after it, up to a blank line.
pub fn rbuscli_events(out: &str) -> Vec<String> {
    let mut events = Vec::new();
    let mut current: Option<Vec<&str>> = None;
    for line in out.lines() {
        let line = line.trim_start_matches("rbuscli> ");
        if line.starts_with("Event received") {
            if let Some(ev) = current.take() {
                events.push(ev.join("\n"));
            }
            current = Some(vec![line]);
        } else if let Some(ev) = &mut current {
            if line.trim().is_empty() && ev.len() > 1 {
                events.push(current.take().unwrap().join("\n"));
            } else if !line.trim().is_empty() {
                ev.push(line);
            }
        }
    }
    events.extend(current.map(|ev| ev.join("\n")));
    events
}
