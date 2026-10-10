//! Surviving a broker restart (L5). Run with `scripts/interop.sh`.
//!
//! Each test restarts `rtrouted`, which drops every client, so `run.sh` runs
//! these alone, after the rest of the suite: every test name contains
//! `broker_restart`.

mod common;

use std::time::Duration;

use common::{rbuscli, rbuscli_values, within};
use rbus::provider::{Event, Prop};
use rbus::{ConnectionState, Error, Object, Rbus, Retry};
use rbus_testkit::broker::{Peer, RawClient, broker_socket, restart_broker, test_inbox};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

static TURN: Mutex<()> = Mutex::const_new(());

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tick {
    n: u32,
}

#[derive(rbus::Model, Default)]
#[rbus(path = "Device.L5.")]
struct Counter {
    #[rbus(writable)]
    value: Prop<u32>,
    tick: Event<Tick>,
}

fn retry() -> Retry {
    Retry::exponential()
        .initial(Duration::from_millis(200))
        .max_delay(Duration::from_secs(1))
        .max_elapsed(Duration::from_secs(30))
}

async fn connect(label: &str) -> Rbus {
    Rbus::builder(format!("rbus-rs.test.{label}"))
        .socket(broker_socket())
        .reconnect(retry())
        .connect()
        .await
        .expect("connect to rtrouted")
}

/// Waits until `bus` has lost its connection and made it again.
async fn reconnected(bus: &Rbus) {
    let mut state = bus.state();
    within(10, state.wait_for(|s| *s == ConnectionState::Reconnecting))
        .await
        .unwrap();
    within(30, state.wait_for(|s| *s == ConnectionState::Connected))
        .await
        .unwrap();
}

async fn restart() {
    tokio::task::spawn_blocking(restart_broker).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l5_broker_restart_rust_provider_and_consumers_carry_on() {
    let _turn = TURN.lock().await;
    let provider_bus = connect("l5-provider").await;
    let counter = provider_bus.provide(Counter::default()).await.unwrap();
    let bus = connect("l5-consumer").await;
    let mut values = bus.subscribe_value::<u32>("Device.L5.Value").await.unwrap();
    let mut ticks = bus.subscribe::<Tick>("Device.L5.Tick!").await.unwrap();
    counter.value.set(1);
    assert_eq!(within(10, values.next()).await.unwrap().unwrap().new, 1);

    // Both connections notice, and come back.
    let provider_back = tokio::spawn({
        let bus = provider_bus.clone();
        async move { reconnected(&bus).await }
    });
    let consumer_back = tokio::spawn({
        let bus = bus.clone();
        async move { reconnected(&bus).await }
    });
    restart().await;
    provider_back.await.unwrap();
    consumer_back.await.unwrap();

    // The provider's names come back: a Rust consumer and rbuscli reach it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match bus.get::<u32>("Device.L5.Value").await {
            Ok(v) => break assert_eq!(v, 1),
            Err(e) if tokio::time::Instant::now() < deadline => {
                eprintln!("not back yet: {e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("provider did not come back: {e}"),
        }
    }
    bus.set("Device.L5.Value", 2u32).await.unwrap();
    assert_eq!(counter.value.get(), 2);
    let got = rbuscli(&["get", "Device.L5.Value"]).await;
    assert_eq!(
        rbuscli_values(&got),
        [("Device.L5.Value".to_owned(), "2".to_owned())],
        "{got}"
    );

    // Both subscriptions carry on.
    let change = within(10, values.next()).await.unwrap().unwrap();
    assert_eq!((change.new, change.old), (2, Some(1)));
    counter.tick.publish(Tick { n: 7 }).unwrap();
    assert_eq!(
        within(10, ticks.next()).await.unwrap().unwrap(),
        Tick { n: 7 }
    );

    drop(counter);
    bus.close().await.unwrap();
    provider_bus.close().await.unwrap();
}

const C_EVENT: &str = "Device.Provider1.Event1!";

/// Starts `rbusEventProvider` (30 s lifetime) and waits until it owns
/// Event1!.
fn start_c_provider() -> Peer {
    let peer = Peer::spawn("rbusEventProvider", &[]).unwrap();
    RawClient::connect(&test_inbox("l5"))
        .unwrap()
        .wait_for_owner(C_EVENT, "EventProvider", Duration::from_secs(10))
        .unwrap();
    peer
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l5_broker_restart_subscription_to_a_c_provider_resumes() {
    let _turn = TURN.lock().await;
    let provider = tokio::task::spawn_blocking(start_c_provider).await.unwrap();
    let bus = connect("l5-c-provider").await;
    let mut events = bus.subscribe::<Object>(C_EVENT).await.unwrap();
    within(10, events.next()).await.unwrap().unwrap();

    // The whole bus restarts. C clients don't register their elements again
    // (`rtConnection.c:424-439`), so the provider restarts too, as it would
    // under systemd.
    let back = tokio::spawn({
        let bus = bus.clone();
        async move { reconnected(&bus).await }
    });
    drop(provider);
    restart().await;
    back.await.unwrap();
    let _provider = tokio::task::spawn_blocking(start_c_provider).await.unwrap();
    loop {
        // Events from before the restart may still be in the buffer.
        let e = within(30, events.next()).await.unwrap().unwrap();
        if e.get("someText").is_some() {
            break;
        }
    }
    within(10, events.next()).await.unwrap().unwrap();
    bus.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l5_broker_restart_without_reconnect_closes() {
    let _turn = TURN.lock().await;
    let bus = common::connect("l5-no-reconnect").await;
    let mut state = bus.state();
    restart().await;
    within(10, state.wait_for(|s| *s == ConnectionState::Closed))
        .await
        .unwrap();
    assert!(matches!(
        bus.get::<u32>("Device.L5.Value").await,
        Err(Error::Disconnected)
    ));
}
