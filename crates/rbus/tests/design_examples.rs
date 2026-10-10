//! The design.md section 4 examples implemented so far (M2, M3), run against
//! real providers. Element names are swapped for ones the C peers serve. The
//! `name!` macro (M6) and `.reconnect(..)` (M7) arrive with their milestones.
//! Run with `scripts/interop.sh`.

mod common;

use std::time::Duration;

use common::{sample_provider, test_provider};
use rbus::{ConnectionState, Error, MethodName, Object, PropertyName, Rbus, Value};
use rbus_testkit::broker::broker_socket;
use serde::{Deserialize, Serialize};

/// 4.4 Connecting and lifecycle.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_4_connecting_and_lifecycle() -> rbus::Result<()> {
    let _p = test_provider();
    let bus = Rbus::connect("com.example.wifi").await?; // unix:///tmp/rtrouted
    bus.close().await?;

    let bus = Rbus::builder("com.example.wifi")
        .socket(broker_socket()) // L1: unix socket only
        .default_timeout(Duration::from_secs(5)) // L6
        .connect()
        .await?;

    // Per-call limits are plain tokio, and cancel-safe.
    let up: u32 = tokio::time::timeout(
        Duration::from_secs(1),
        bus.get("Device.TestProvider.Value.UINT32"),
    )
    .await
    .expect("in time")?;
    assert_eq!(up, 10_000_000);

    let state = bus.state(); // watch::Receiver<ConnectionState>
    assert_eq!(*state.borrow(), ConnectionState::Connected);
    bus.close().await?; // L13
    assert_eq!(*state.borrow(), ConnectionState::Closed);
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct RebootArgs {
    delay: u32,
    reason: String,
}

/// 4.5 Names and values.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_5_names_and_values() -> rbus::Result<()> {
    const UPTIME: PropertyName = PropertyName::from_static("Device.TestProvider.Value.UINT32");
    const METHOD: MethodName = MethodName::from_static("Device.TestProvider.Method11()");

    let _p = test_provider();
    let bus = Rbus::connect("com.example.names").await?;

    let up: u32 = bus.get(UPTIME).await?; // T: RbusType
    assert_eq!(up, 10_000_000);
    // A typed method name goes straight through...
    assert!(matches!(
        bus.invoke::<_, ()>(METHOD, ()).await,
        Err(Error::Method { .. })
    ));
    // ...and a string is checked at run time.
    assert!(matches!(
        bus.invoke::<_, ()>("Device.TestProvider.Method11", ())
            .await,
        Err(Error::Name(_))
    ));

    let obj: Object = rbus::to_object(&RebootArgs {
        delay: 5,
        reason: "update".into(),
    })?;
    let back: RebootArgs = rbus::from_object(obj)?;
    assert_eq!(back.reason, "update");
    Ok(())
}

#[derive(Deserialize)]
struct Named {
    name: String,
}

/// 4.6 Consumer API: get, set and methods.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_6_consumer_get_set_invoke() -> rbus::Result<()> {
    let _s = sample_provider();
    let _t = test_provider();
    let bus = Rbus::connect("com.example.consumer").await?;

    // Get (C1, C2)
    let maker: String = bus
        .get("Device.DeviceInfo.SampleProvider.Manufacturer")
        .await?;
    assert_eq!(maker, "COMCAST");
    let values = bus
        .get_many([
            "Device.DeviceInfo.SampleProvider.ModelName",
            "Device.TestProvider.Value.STRING",
        ])
        .await?; // IndexMap<PropertyName, Value>
    assert_eq!(
        values["Device.TestProvider.Value.STRING"],
        Value::String("short string".into())
    );

    // Set (C6, C7, C9): one transaction; the commit flag never appears.
    bus.set("Device.SampleProvider.AllTypes.BoolData", true)
        .await?;
    bus.set_many()
        .set("Device.SampleProvider.AllTypes.BoolData", false)
        .set("Device.SampleProvider.AllTypes.UInt32Data", 36u32)
        .await?;
    let r = bus
        .set_many()
        .set("Device.SampleProvider.AllTypes.BoolData", true)
        .set("Device.DeviceInfo.SampleProvider.ModelName", "XB7")
        .await;
    assert!(
        matches!(&r, Err(Error::SetFailed { name, .. }) if name == "Device.DeviceInfo.SampleProvider.ModelName"),
        "{r:?}"
    );

    // Methods (M1, M3): serde in, serde out.
    let reply: Named = bus.invoke("Device.TestProvider.MethodAsync1()", ()).await?;
    assert_eq!(reply.name, "MethodAsync1()");
    Ok(())
}

#[derive(Deserialize)]
struct PartialRow {
    #[serde(rename = "Param1")]
    param1: String,
}

/// 4.6 Consumer API: trees, tables and discovery.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_6_trees_tables_discovery() -> rbus::Result<()> {
    let _t = test_provider();
    let bus = Rbus::connect("com.example.trees").await?;

    // Partial path and wildcard (C3, C4): a tree you can walk or deserialize
    let tree = bus.get_tree("Device.TestProvider.PartialPath1.").await?;
    let rows: Vec<PartialRow> = tree.deserialize()?;
    assert_eq!(rows.len(), 2);
    assert!(rows[1].param1.ends_with(".2.Param1"));
    let params = bus
        .get_tree("Device.TestProvider.PartialPath1.*.Param1")
        .await?;
    for (name, value) in params.iter() {
        assert_eq!(value, &Value::String(name.to_owned()));
    }
    assert!(tree.failures().is_empty()); // C5

    // Tables (T1–T3)
    let hosts = bus.table("Device.TestProvider.Table1.");
    let row = hosts.add_row().alias("design-guest").await?;
    assert!(hosts.rows().await?.iter().any(|r| r.inst == row.inst));
    hosts.remove_row(&row).await?;

    // Discovery (D1, D2, D4, D5)
    let owner = bus
        .discover()
        .owner("Device.TestProvider.Value.INT32")
        .await?;
    assert_eq!(owner, "TestProvider");
    let names = bus.discover().elements("TestProvider").await?;
    assert!(!names.is_empty());
    let comps = bus.discover().components().await?;
    assert!(comps.contains(&owner));
    let _adv = bus.discover().advisories(); // Stream<Advisory>
    Ok(())
}

#[derive(Deserialize)]
struct Tick {
    index: i32,
}

/// 4.7 Subscriptions as typed streams.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_7_subscriptions() -> rbus::Result<()> {
    use rbus::{EventError, Retry, RowEvent};

    let _t = test_provider();
    let bus = Rbus::connect("com.example.events").await?;

    // Property value changes (E3, E9)
    let mut enable = bus
        .subscribe_value::<u32>("Device.TestProvider.Value.UINT32")
        .initial_value() // the first item is the current value
        .await?;
    match tokio::time::timeout(Duration::from_secs(5), enable.next())
        .await
        .expect("initial value")
    {
        Some(Ok(change)) => assert_eq!((change.old, change.new), (None, 10_000_000)),
        Some(Err(EventError::Lagged(n))) => panic!("dropped {n}"),
        other => panic!("{other:?}"),
    }

    // Table rows (E4)
    let mut rows = bus.subscribe_table("Device.TestProvider.Table1.").await?; // Added(RowId) or Removed(RowId)
    let table = bus.table("Device.TestProvider.Table1.");
    let row = table.add_row().alias("design-e4").await?;
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(10), rows.next())
            .await
            .expect("row event")
            .expect("open")
            .expect("decodes");
        if matches!(&ev, RowEvent::Added { row: r, .. } if r.inst == row.inst) {
            break;
        }
    }
    table.remove_row(&row).await?;

    // General events, decoded with serde (E5, E10)
    let mut ticks = bus
        .subscribe::<Tick>("Device.TestProvider.Event1!")
        .retry(Retry::exponential().max_elapsed(Duration::from_secs(60)))
        .buffer(128)
        .await?;
    let tick = tokio::time::timeout(Duration::from_secs(10), ticks.next())
        .await
        .expect("an event")
        .expect("open")
        .expect("decodes");
    assert!(tick.index >= 0);

    ticks.unsubscribe().await?; // or just drop it
    Ok(())
}
