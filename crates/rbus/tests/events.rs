//! Subscriptions to a real C provider (E1–E5, E7, E9, D12). Run with `scripts/interop.sh`.

mod common;

use std::time::{Duration, Instant};

use common::{connect, test_provider, within};
use futures_util::StreamExt;
use rbus::{Error, EventError, Object, SampleKind, Status, Value};
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

/// Nothing sets it, so its value stays the provider's default.
const UINT32: &str = "Device.TestProvider.Value.UINT32";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_interval_samples_arrive_whether_or_not_the_value_changes() {
    let _p = test_provider();
    let bus = connect("e7-interval").await;
    let mut samples = bus
        .subscribe_value::<u32>(UINT32)
        .interval(Duration::from_secs(1))
        .initial_value()
        .await
        .unwrap();
    let first = within(5, samples.next()).await.unwrap().unwrap();
    assert_eq!(
        (first.name.as_str(), first.value, first.kind),
        (UINT32, 10_000_000, SampleKind::Initial)
    );
    let started = Instant::now();
    for _ in 0..3 {
        let s = within(5, samples.next()).await.unwrap().unwrap();
        assert_eq!(
            (s.name.as_str(), s.value, s.kind),
            (UINT32, 10_000_000, SampleKind::Interval)
        );
    }
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "three samples in {:?}",
        started.elapsed()
    );
    // The provider only finds the subscription if the interval comes back
    // with the unsubscribe; otherwise it answers INVALID_INPUT.
    samples.unsubscribe().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_interval_raw_is_sent_as_is() {
    let _p = test_provider();
    let bus = connect("e7-raw").await;
    // C providers that publish on an interval themselves read it as seconds.
    let mut samples = bus
        .subscribe_value::<u32>(UINT32)
        .interval_raw(1)
        .await
        .unwrap();
    let s = within(5, samples.next()).await.unwrap().unwrap();
    assert_eq!((s.value, s.kind), (10_000_000, SampleKind::Interval));
    samples.unsubscribe().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_interval_and_plain_subscriptions_are_independent() {
    let _p = test_provider();
    let bus = connect("e7-independent").await;
    // Only this test sets it.
    let name = "Device.TestProvider.Value.INT16";
    let mut changes = bus.subscribe_value::<i16>(name).await.unwrap();
    let mut samples = bus
        .subscribe_value::<i16>(name)
        .interval(Duration::from_secs(1))
        .await
        .unwrap();
    bus.set(name, 1234i16).await.unwrap();
    let change = within(10, changes.next()).await.unwrap().unwrap();
    assert_eq!(change.new, 1234);
    loop {
        // A sample from before the set may come first.
        let s = within(5, samples.next()).await.unwrap().unwrap();
        assert_eq!(s.kind, SampleKind::Interval);
        if s.value == 1234 {
            break;
        }
    }
    // Dropping the interval subscription leaves the other one working.
    drop(samples);
    bus.set(name, 4321i16).await.unwrap();
    let change = within(10, changes.next()).await.unwrap().unwrap();
    assert_eq!((change.old, change.new), (Some(1234), 4321));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_interval_on_a_wildcard_gives_one_sample_per_instance() {
    let _p = test_provider();
    let bus = connect("e7-wildcard").await;
    let mut samples = bus
        .subscribe_value::<Value>("Device.TestProvider.PartialPath1.*.Param1")
        .interval(Duration::from_secs(1))
        .await
        .unwrap();
    let mut names = Vec::new();
    for _ in 0..2 {
        let s = within(5, samples.next()).await.unwrap().unwrap();
        assert_eq!(s.kind, SampleKind::Interval);
        names.push(s.name);
    }
    names.sort();
    assert_eq!(
        names,
        [
            "Device.TestProvider.PartialPath1.1.Param1",
            "Device.TestProvider.PartialPath1.2.Param1"
        ]
    );
    samples.unsubscribe().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_an_event_subscription_with_an_interval_still_gets_its_events() {
    let _p = test_provider();
    let bus = connect("e7-event").await;
    // This provider takes the interval and keeps publishing at its own pace.
    let mut ticks = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .interval(Duration::from_secs(1))
        .await
        .unwrap();
    within(10, ticks.next()).await.unwrap().unwrap();
    ticks.unsubscribe().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e7_bad_intervals_fail_before_anything_is_sent() {
    let bus = connect("e7-bad").await;
    for every in [
        Duration::ZERO,
        Duration::from_millis(100),
        Duration::from_millis(1500),
    ] {
        let err = bus
            .subscribe_value::<u32>(UINT32)
            .interval(every)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    }
    let err = bus
        .subscribe::<Object>("Device.TestProvider.Event1!")
        .interval_raw(0)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
}
