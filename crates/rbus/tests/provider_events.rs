//! The subscription registry (M5) through the real broker: a Rust provider's
//! events reach Rust consumers and `rbuscli`. Each test serves the sample
//! model of `common/model.rs` under its own root. Run with
//! `scripts/interop.sh`.

mod common;

use std::time::Duration;

use common::model::Sample;
use common::{Rbuscli, connect, rbuscli, rbuscli_events, within};
use rbus::provider::SubscriptionEvent;
use rbus::{Object, RowEvent, RowId, Status, Value};

#[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq)]
struct Alarm {
    level: u32,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p4_general_events_reach_their_subscribers() {
    let provider_bus = connect("p4-provider").await;
    let p = Sample::start(&provider_bus, "P4").await;
    // Nobody subscribed: publishing does nothing (D10).
    p.publish("Device.P4.Alarm!", &Alarm { level: 1 }).unwrap();
    let bus = connect("p4-consumer").await;
    let mut alarms = bus.subscribe::<Alarm>("Device.P4.Alarm!").await.unwrap();
    p.publish("Device.P4.Alarm!", &Alarm { level: 3 }).unwrap();
    assert_eq!(
        within(5, alarms.next()).await.unwrap().unwrap(),
        Alarm { level: 3 }
    );
    let err = p
        .publish("Device.P4.Name", &Alarm { level: 3 })
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementDoesNotExist), "{err:?}");
    assert!(matches!(
        p.publish("Device.P4.Alarm!", &3u32),
        Err(rbus::Error::Type(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p9_p15_value_changes_after_sets_with_the_initial_value_first() {
    let provider_bus = connect("p9-provider").await;
    let p = Sample::start(&provider_bus, "P9").await;
    let bus = connect("p9-consumer").await;
    let mut names = bus
        .subscribe_value::<String>("Device.P9.Name")
        .initial_value()
        .await
        .unwrap();
    let first = within(5, names.next()).await.unwrap().unwrap();
    assert_eq!((first.new.as_str(), first.old), ("rust", None));
    // A C consumer's set...
    let out = rbuscli(&["set", "Device.P9.Name", "string", "from-c"]).await;
    assert!(out.contains("succeeded"), "{out}");
    let c = within(5, names.next()).await.unwrap().unwrap();
    assert_eq!((c.new.as_str(), c.old.as_deref()), ("from-c", Some("rust")));
    assert!(c.by.as_deref().unwrap().starts_with("rbuscli"), "{c:?}");
    // ... a Rust consumer's set...
    bus.set("Device.P9.Name", "from-rust").await.unwrap();
    let r = within(5, names.next()).await.unwrap().unwrap();
    assert_eq!(r.by.as_deref(), Some(bus.component()));
    // ... and the provider's own report (P11), by its component.
    p.publish_value_change("Device.P9.Name", "from-rust", "from-provider")
        .unwrap();
    let own = within(5, names.next()).await.unwrap().unwrap();
    assert_eq!(own.new, "from-provider");
    assert_eq!(own.by.as_deref(), Some(provider_bus.component()));
    // Setting an unchanged value publishes nothing; the next change arrives
    // next.
    bus.set("Device.P9.Name", "from-rust").await.unwrap();
    bus.set("Device.P9.Name", "from-rust").await.unwrap();
    let next = within(5, names.next()).await.unwrap().unwrap();
    assert_eq!(next.old.as_deref(), Some("from-provider"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p10_polled_values_publish_without_sets() {
    let provider_bus = connect("p10-provider").await;
    let p = Sample::start(&provider_bus, "P10").await;
    let bus = connect("p10-consumer").await;
    let mut uptime = bus
        .subscribe_value::<u32>("Device.P10.Uptime")
        .await
        .unwrap();
    let a = within(5, uptime.next()).await.unwrap().unwrap();
    let b = within(5, uptime.next()).await.unwrap().unwrap();
    assert!(b.new > a.new, "{a:?} {b:?}");
    assert_eq!(a.by.as_deref(), Some(provider_bus.component()));
    // notify_changed reads the value again and publishes the change.
    p.notify_changed("Device.P10.Uptime").await.unwrap();
    let c = within(5, uptime.next()).await.unwrap().unwrap();
    assert!(c.new > b.new);
    let err = p.notify_changed("Device.P10.Nope").await.unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementDoesNotExist));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p12_filtered_subscriptions_from_c_get_only_flips() {
    let provider_bus = connect("p12-provider").await;
    let p = Sample::start(&provider_bus, "P12").await;
    let mut cli = Rbuscli::start();
    cli.send("sub Device.P12.Radio.1.Channel > 10");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let bus = connect("p12-consumer").await;
    for ch in [6, 11, 12, 3, 4] {
        bus.set("Device.P12.Radio.1.Channel", ch as u32)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let events = rbuscli_events(&cli.finish().await);
    assert_eq!(events.len(), 2, "{events:#?}");
    assert!(
        events[0].contains("value:11") && events[0].contains("value:true"),
        "{events:#?}"
    );
    assert!(
        events[1].contains("value:3") && events[1].contains("value:false"),
        "{events:#?}"
    );
    // rbuscli quit: the broker's advisory removed its subscription.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(p.subscribers(None).is_empty(), "{:?}", p.subscribers(None));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt9_row_events_for_consumer_and_provider_rows() {
    let provider_bus = connect("pt9-provider").await;
    let p = Sample::start(&provider_bus, "PT9").await;
    let bus = connect("pt9-consumer").await;
    let mut rows = bus.subscribe_table("Device.PT9.Radio.").await.unwrap();
    let added = bus
        .table("Device.PT9.Radio.")
        .add_row()
        .alias("x")
        .await
        .unwrap();
    match within(5, rows.next()).await.unwrap().unwrap() {
        RowEvent::Added { table, row } => {
            assert_eq!(table, "Device.PT9.Radio.");
            assert_eq!(row, added);
        }
        other => panic!("{other:?}"),
    }
    p.add_row("Device.PT9.Radio.", 20).unwrap();
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Added {
            row: RowId { inst: 20, .. },
            ..
        }
    ));
    let out = rbuscli(&["delrow", "Device.PT9.Radio.20."]).await;
    assert!(out.contains("deleted"), "{out}");
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Removed {
            row: RowId { inst: 20, .. },
            ..
        }
    ));
    // Wildcard and alias subscriptions to values inside rows.
    let mut statuses = bus
        .subscribe_value::<u32>("Device.PT9.Radio.*.Channel")
        .await
        .unwrap();
    bus.set("Device.PT9.Radio.[x].Channel", 1u32).await.unwrap();
    let change = within(5, statuses.next()).await.unwrap().unwrap();
    assert_eq!(
        change.name,
        format!("Device.PT9.Radio.{}.Channel", added.inst)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p8_the_provider_sees_its_subscribers_come_and_go() {
    let provider_bus = connect("p8-provider").await;
    let p = Sample::start(&provider_bus, "P8").await;
    let mut events = p.subscription_events();
    let bus = connect("p8-consumer").await;
    let sub = bus.subscribe::<Object>("Device.P8.Alarm!").await.unwrap();
    let added = match within(5, events.next()).await.unwrap().unwrap() {
        SubscriptionEvent::Added(info) => info,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        (added.event.as_str(), added.inbox.as_str()),
        ("Device.P8.Alarm!", bus.inbox())
    );
    assert_eq!(
        p.subscribers(Some("Device.P8.Alarm!")),
        std::slice::from_ref(&added)
    );
    sub.unsubscribe().await.unwrap();
    assert!(matches!(
        within(5, events.next()).await.unwrap().unwrap(),
        SubscriptionEvent::Removed(info) if info == added
    ));
    // A consumer that goes away without unsubscribing is pruned too.
    let other = connect("p8-other").await;
    let _sub = other
        .subscribe_value::<String>("Device.P8.Name")
        .await
        .unwrap();
    assert!(matches!(
        within(5, events.next()).await.unwrap().unwrap(),
        SubscriptionEvent::Added(_)
    ));
    drop(_sub);
    other.close().await.unwrap();
    assert!(matches!(
        within(5, events.next()).await.unwrap().unwrap(),
        SubscriptionEvent::Removed(_)
    ));
    drop(p);
    assert!(
        within(5, events.next()).await.is_none(),
        "the stream ends with the provider"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d9_intervals_are_refused_and_duplicates_reported() {
    let provider_bus = connect("d9-provider").await;
    let _p = Sample::start(&provider_bus, "D9").await;
    let mut cli = Rbuscli::start();
    cli.send("subinterval Device.D9.Uptime 2");
    cli.send("sub Device.D9.Nope");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let out = cli.finish().await;
    // INVALID_OPERATION (9).
    assert!(out.contains("Invalid Subscription err:9"), "{out}");
    let bus = connect("d9-consumer").await;
    let _a = bus.subscribe::<Object>("Device.D9.Alarm!").await.unwrap();
    // Each Rust subscription has its own component id, so a second one is a
    // second subscription, not a duplicate.
    let _b = bus.subscribe::<Object>("Device.D9.Alarm!").await.unwrap();
    assert_eq!(_p.subscribers(Some("Device.D9.Alarm!")).len(), 2);
    let err = bus
        .subscribe_value::<String>("Device.D9.Radio.9.Status")
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementDoesNotExist), "{err:?}");
    let v = bus.get::<Value>("Device.D9.Enable").await.unwrap();
    assert_eq!(v, Value::Bool(true));
}
