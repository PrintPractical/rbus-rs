//! M0 smoke tests: the interop harness reaches a real rtrouted and C peers.
//! Run with `scripts/interop.sh`.

use std::time::Duration;

use rbus_testkit::broker::{FLAG_RESPONSE, FLAG_UNDELIVERABLE, Peer, RawClient};

fn inbox(name: &str) -> String {
    format!("rbus.{name}.INBOX.{}", std::process::id())
}

#[test]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
fn smoke_registers_inbox() {
    let mut client = RawClient::connect(&inbox("smoke-self")).unwrap();
    let me = client.inbox().to_owned();
    let routes = client.registered_components().unwrap();
    assert!(
        routes.contains(&format!("\"{me}\"")),
        "broker does not list our inbox: {routes}"
    );
}

#[test]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
fn smoke_discovers_a_c_provider() {
    let _provider = Peer::spawn("rbusSampleProvider", &["30"]).unwrap();
    let mut client = RawClient::connect(&inbox("smoke-disc")).unwrap();
    client
        .wait_for_owner(
            "Device.SampleProvider.SampleData.IntData",
            "rbusSampleProvider",
            Duration::from_secs(10),
        )
        .unwrap();
    let routes = client.query("Device.SampleProvider.").unwrap();
    assert!(
        routes.contains("\"rbusSampleProvider\""),
        "partial-path query does not list the provider: {routes}"
    );
}

#[test]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
fn smoke_request_to_unknown_element_is_undeliverable() {
    let mut client = RawClient::connect(&inbox("smoke-undeliverable")).unwrap();
    // Non-empty payload on purpose: see RawClient::registered_components.
    let reply = client.request("Device.Does.Not.Exist", b"{}").unwrap();
    assert_eq!(
        reply.header.flags & (FLAG_RESPONSE | FLAG_UNDELIVERABLE),
        FLAG_RESPONSE | FLAG_UNDELIVERABLE
    );
    assert_eq!(reply.header.payload_len, 0);
}
