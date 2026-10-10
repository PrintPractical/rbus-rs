//! Shared fixtures for the interop tests. Every test using them is
//! `#[ignore = "needs rtrouted: run scripts/interop.sh"]` and runs on a
//! multi-threaded runtime, because the fixtures block while C peers start.
#![allow(dead_code)]

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
