//! Subscriptions that outlive their provider (E7, E10, D11). Run with
//! `scripts/interop.sh`.
//!
//! The tests start and kill their own C providers, so they take turns.

mod common;

use std::time::Duration;

use common::{connect, within};
use rbus::{Object, Retry, SampleKind};
use rbus_testkit::broker::{Peer, RawClient, test_inbox};
use tokio::sync::Mutex;

static TURN: Mutex<()> = Mutex::const_new(());

const EVENT: &str = "Device.Provider1.Event1!";

/// Starts `rbusEventProvider` (30 s lifetime) and waits until it owns Event1!.
fn start_provider() -> Peer {
    let peer = Peer::spawn("rbusEventProvider", &[]).unwrap();
    RawClient::connect(&test_inbox("restart"))
        .unwrap()
        .wait_for_owner(EVENT, "EventProvider", Duration::from_secs(10))
        .unwrap();
    peer
}

fn wait_gone() {
    RawClient::connect(&test_inbox("restart"))
        .unwrap()
        .wait_for_no_owner(EVENT, "EventProvider", Duration::from_secs(10))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e10_subscribe_waits_for_a_late_provider() {
    let _turn = TURN.lock().await;
    wait_gone();
    let bus = connect("e10-retry").await;
    let retry = Retry::exponential()
        .initial(Duration::from_millis(200))
        .max_delay(Duration::from_secs(1))
        .max_elapsed(Duration::from_secs(20));
    let pending = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>(EVENT).retry(retry).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!pending.is_finished(), "subscribed with no provider");
    let _provider = tokio::task::spawn_blocking(start_provider).await.unwrap();
    let mut events = within(20, pending).await.unwrap().unwrap();
    within(10, events.next()).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d11_events_resume_after_the_provider_restarts() {
    let _turn = TURN.lock().await;
    wait_gone();
    let provider = tokio::task::spawn_blocking(start_provider).await.unwrap();
    let bus = connect("d11-restart").await;
    let mut events = bus.subscribe::<Object>(EVENT).await.unwrap();
    within(10, events.next()).await.unwrap().unwrap();

    // The restarted provider has forgotten us; its connect advisory makes us
    // subscribe again.
    drop(provider);
    tokio::task::spawn_blocking(wait_gone).await.unwrap();
    let _provider = tokio::task::spawn_blocking(start_provider).await.unwrap();
    loop {
        // Events queued before the restart may still be in the buffer.
        let e = within(15, events.next()).await.unwrap().unwrap();
        if e.get("someText").is_some() {
            break;
        }
    }
    // Drain, then make sure new ones keep coming.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    within(10, events.next()).await.unwrap().unwrap();
}

const PARAM: &str = "Device.Provider1.Param1";

/// Starts `rbusValueChangeProvider` and waits until it owns Param1. Its
/// component is also called EventProvider.
fn start_value_provider() -> Peer {
    let peer = Peer::spawn("rbusValueChangeProvider", &[]).unwrap();
    RawClient::connect(&test_inbox("restart"))
        .unwrap()
        .wait_for_owner(PARAM, "EventProvider", Duration::from_secs(10))
        .unwrap();
    peer
}

fn wait_value_provider_gone() {
    RawClient::connect(&test_inbox("restart"))
        .unwrap()
        .wait_for_no_owner(PARAM, "EventProvider", Duration::from_secs(10))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_interval_samples_resume_after_the_provider_restarts() {
    let _turn = TURN.lock().await;
    wait_gone();
    let provider = tokio::task::spawn_blocking(start_value_provider)
        .await
        .unwrap();
    let bus = connect("e7-restart").await;
    let mut samples = bus
        .subscribe_value::<i32>(PARAM)
        .interval(Duration::from_secs(1))
        .await
        .unwrap();
    within(5, samples.next()).await.unwrap().unwrap();

    drop(provider);
    tokio::task::spawn_blocking(wait_value_provider_gone)
        .await
        .unwrap();
    let _provider = tokio::task::spawn_blocking(start_value_provider)
        .await
        .unwrap();
    // Drain what came before the restart, then samples must keep coming.
    // Had the resubscription lost the interval, this provider would send
    // value changes instead.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    while tokio::time::timeout(Duration::from_millis(50), samples.next())
        .await
        .is_ok()
    {}
    for _ in 0..2 {
        let s = within(10, samples.next()).await.unwrap().unwrap();
        assert_eq!(s.kind, SampleKind::Interval);
    }
    samples.unsubscribe().await.unwrap();
}
