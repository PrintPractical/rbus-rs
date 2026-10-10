//! `rbus-record --listen <socket> --upstream <socket> --out <file.jsonl>`
//!
//! Recording proxy between RBUS clients and `rtrouted`. See
//! `rbus_testkit::record` for the log format. Runs until killed.
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use rbus_testkit::record::{FrameLog, run};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (mut listen, mut upstream, mut out) = (None, None, None);
    while let Some(flag) = args.next() {
        let slot = match flag.as_str() {
            "--listen" => &mut listen,
            "--upstream" => &mut upstream,
            "--out" => &mut out,
            _ => return usage(&format!("unknown argument {flag}")),
        };
        match args.next() {
            Some(v) => *slot = Some(PathBuf::from(v)),
            None => return usage(&format!("{flag} needs a value")),
        }
    }
    let (Some(listen), Some(upstream), Some(out)) = (listen, upstream, out) else {
        return usage("missing argument");
    };
    let log = match FrameLog::create(&out) {
        Ok(log) => Arc::new(log),
        Err(e) => {
            eprintln!("rbus-record: cannot create {}: {e}", out.display());
            return ExitCode::FAILURE;
        }
    };
    match run(&listen, &upstream, log) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rbus-record: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage(problem: &str) -> ExitCode {
    eprintln!("rbus-record: {problem}");
    eprintln!("usage: rbus-record --listen <socket> --upstream <socket> --out <file.jsonl>");
    ExitCode::from(2)
}
