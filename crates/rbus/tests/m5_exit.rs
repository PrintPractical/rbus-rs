//! M5 exit test: `rbuscli sub`, plain and filtered, behaves exactly as it
//! does against a C provider. The same `rbuscli -i` session runs against
//! `rbusTestProvider` and against the Rust conformance provider, which serve
//! the same `Device.TestProvider.VCParamBy`, and must print the same events.
//! The other half of the exit test, the `rbusTestConsumer` groups, is
//! `test_consumer.rs`. Run with `scripts/interop.sh`.

mod common;
mod conformance;

use std::time::Duration;

use common::{Rbuscli, connect, raw_client, rbuscli_events, test_provider};
use conformance::Conformance;
use rbus::Rbus;
use rbus_testkit::broker::broker_socket;

const BY: &str = "Device.TestProvider.VCParamBy";

/// Subscribes `rbuscli` to `VCParamBy` twice, with an initial value and with
/// the filter `> 1500`, sets it to 1000, 2000 and 1000, and returns the
/// events `rbuscli` printed, sorted. C polls the value every 2 s, so each
/// set gets 3 s.
async fn session(label: &str) -> Vec<String> {
    let setter = connect(label).await;
    let mut cli = Rbuscli::start();
    cli.send(&format!("sub {BY} true"));
    cli.send(&format!("sub {BY} > 1500"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    for v in [1000, 2000, 1000] {
        setter.set(BY, v).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let mut events = rbuscli_events(&cli.finish().await);
    // The setter's name differs between the runs; the rest must not.
    for e in &mut events {
        *e = e.replace(setter.component(), "<setter>");
    }
    events.sort();
    events
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m5_exit_rbuscli_sub_sees_the_same_events_from_c_and_rust() {
    let c = {
        let _provider = test_provider();
        session("m5-exit-c").await
    };
    raw_client()
        .wait_for_no_owner(BY, "TestProvider", Duration::from_secs(15))
        .unwrap();
    let bus = Rbus::builder("TestProvider")
        .socket(broker_socket())
        .connect()
        .await
        .unwrap();
    let running = Conformance::start(&bus).await.unwrap();
    let rust = session("m5-exit-rust").await;
    drop(running);

    let kinds: Vec<&str> = c
        .iter()
        .map(|e| e.lines().next().unwrap().rsplit(' ').next().unwrap())
        .collect();
    assert_eq!(
        kinds
            .iter()
            .filter(|k| **k == "RBUS_EVENT_VALUE_CHANGED")
            .count(),
        5,
        "three changes, two of them flip the filter:\n{c:#?}"
    );
    assert!(kinds.contains(&"RBUS_EVENT_INITIAL_VALUE"), "{c:#?}");
    assert!(c.iter().any(|e| e.contains("value:true")), "{c:#?}");
    assert_eq!(
        rust, c,
        "the Rust provider's events differ from the C provider's"
    );
}
