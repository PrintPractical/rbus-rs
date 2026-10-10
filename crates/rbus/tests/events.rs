//! Subscriptions to a real C provider (E1–E5, E9, D12). Run with `scripts/interop.sh`.

mod common;

use std::time::Duration;

use common::{connect, test_provider, within};
use futures_util::StreamExt;
use rbus::{Error, EventError, Object, Status};
use serde::Deserialize;

/// The data `rbusTestProvider` publishes with Event1! and Event2!.
#[derive(Deserialize, Debug)]
struct Tick {
    #[allow(dead_code)]
    buffer: String,
    index: i32,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e1_e5_general_events_decode_into_a_struct() {
    let _p = test_provider();
    let bus = connect("e5-general").await;
    let mut ticks = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .await
        .unwrap();
    let a = within(10, ticks.next()).await.unwrap().unwrap();
    // The Stream impl works with the usual combinators too.
    let b = within(10, StreamExt::next(&mut ticks))
        .await
        .unwrap()
        .unwrap();
    assert!(b.index > a.index, "{a:?} then {b:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e1_two_subscriptions_to_one_event_both_receive() {
    let _p = test_provider();
    let bus = connect("e1-two").await;
    let mut a = bus
        .subscribe::<Object>("Device.TestProvider.Event2!")
        .await
        .unwrap();
    let mut b = bus
        .subscribe::<Object>("Device.TestProvider.Event2!")
        .await
        .unwrap();
    within(10, a.next()).await.unwrap().unwrap();
    within(10, b.next()).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e3_value_change_after_a_set() {
    let _p = test_provider();
    let bus = connect("e3-value-change").await;
    let name = "Device.TestProvider.Value.INT32";
    let mut changes = bus.subscribe_value::<i32>(name).await.unwrap();
    bus.set(name, 4242).await.unwrap();
    // C providers poll their properties every second for changes.
    let change = within(10, changes.next()).await.unwrap().unwrap();
    assert_eq!(change.name, name);
    assert_eq!(change.new, 4242);
    assert_eq!(change.old, Some(-10_000_000));
    assert_eq!(change.by.as_deref(), Some(bus.component()));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e9_initial_value_comes_first() {
    let _p = test_provider();
    let bus = connect("e9-initial").await;
    let mut sub = bus
        .subscribe_value::<u32>("Device.TestProvider.Value.UINT32")
        .initial_value()
        .await
        .unwrap();
    let first = within(5, sub.next()).await.unwrap().unwrap();
    assert_eq!((first.new, first.old), (10_000_000, None));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e2_unsubscribe_and_subscribe_again() {
    let _p = test_provider();
    let bus = connect("e2-unsubscribe").await;
    let sub = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .await
        .unwrap();
    sub.unsubscribe().await.unwrap();
    // Dropping a stream unsubscribes too; a new one works right away.
    let sub = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .await
        .unwrap();
    drop(sub);
    let mut sub = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .await
        .unwrap();
    within(10, sub.next()).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e1_subscribe_errors_are_typed() {
    let _p = test_provider();
    let bus = connect("e1-errors").await;
    match bus
        .subscribe::<Object>("Device.TestProvider.ErrorSubHandlerEvent!")
        .await
    {
        Err(Error::Status { status, .. }) => assert_eq!(status, Status::AccessNotAllowed),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        bus.subscribe::<Object>("Device.Nobody.Event!").await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d12_a_slow_consumer_is_told_it_lagged() {
    let _p = test_provider();
    let bus = connect("d12-lagged").await;
    let mut sub = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .buffer(1)
        .await
        .unwrap();
    // Event1! fires every second; let three pile up in a buffer of one.
    tokio::time::sleep(Duration::from_millis(3500)).await;
    match within(5, sub.next()).await.unwrap() {
        Err(EventError::Lagged(n)) => assert!(n >= 1, "{n}"),
        other => panic!("expected Lagged, got {other:?}"),
    }
    within(5, sub.next()).await.unwrap().unwrap();
}
