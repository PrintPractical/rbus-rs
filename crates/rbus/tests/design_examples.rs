//! The design.md section 4 examples, run against real providers and
//! consumers. Consumer examples swap element names for ones the C peers
//! serve; provider examples keep their names and are checked from `rbuscli`
//! and Rust consumers. `.reconnect(..)` arrives with M7. Run with
//! `scripts/interop.sh`.

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
    use rbus::{EventName, TablePath, name};

    const UPTIME: PropertyName = name!("Device.TestProvider.Value.UINT32");
    const METHOD: MethodName = name!("Device.TestProvider.Method11()");
    const ALARM: EventName = name!("Device.TestProvider.Event1!");
    const ROWS: TablePath = name!("Device.TestProvider.Table1.");
    assert_eq!(
        (ALARM.as_str(), ROWS.as_str()),
        ("Device.TestProvider.Event1!", "Device.TestProvider.Table1.")
    );
    // `bus.invoke(UPTIME, ())` doesn't compile: the `compile_fail` doctest
    // in src/name.rs.

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

/// The 4.8 and 4.9 examples both serve `Device.WiFi.`, so they take turns.
static WIFI: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The 4.8 model, as design.md writes it.
mod wifi {
    use rbus::provider::{Context, Event, Fault, Prop, Table};
    use serde::Serialize;

    #[derive(Serialize, Clone, Debug, PartialEq)]
    pub struct Alarm {
        pub level: u8,
        pub text: String,
    }

    #[derive(rbus::Model, Default)]
    #[rbus(path = "Device.WiFi.")]
    pub struct Wifi {
        pub radio_number_of_entries: Prop<u32>, // Device.WiFi.RadioNumberOfEntries (read-only)
        #[rbus(writable)]
        pub enable: Prop<bool>, // Device.WiFi.Enable
        #[rbus(name = "Radio", add_row = Wifi::new_radio, remove_row)]
        pub radios: Table<Radio>, // Device.WiFi.Radio.{i}.  consumers may add and remove rows (PT2)
        pub alarm: Event<Alarm>,                // Device.WiFi.Alarm!
    }

    #[derive(rbus::Row, Clone, PartialEq, Default)]
    pub struct Radio {
        #[rbus(writable)]
        pub enable: bool, // Device.WiFi.Radio.{i}.Enable
        #[rbus(writable, validate = valid_channel)]
        pub channel: u32,
        pub status: String,
    }

    impl Wifi {
        async fn new_radio(&self, _cx: &Context, _alias: Option<&str>) -> Result<Radio, Fault> {
            Ok(Radio::default())
        }
    }

    fn valid_channel(ch: &u32) -> Result<(), Fault> {
        matches!(ch, 1..=13 | 36..=165)
            .then_some(())
            .ok_or(Fault::invalid_value("channel"))
    }
}

#[derive(Deserialize, Debug, PartialEq)]
struct AlarmData {
    level: u8,
    text: String,
}

/// 4.8 Provider: state-backed models.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_8_state_backed_model() -> rbus::Result<()> {
    use common::{rbuscli, rbuscli_values, within};
    use rbus::RowEvent;
    use wifi::{Alarm, Radio, Wifi};

    let turn = common::Turn::take(&WIFI).await;
    let bus = Rbus::connect("com.example.wifi").await?;
    let wifi = bus.provide(Wifi::default()).await?; // Provided<Wifi>: derefs to &Wifi; dropping it unregisters

    let consumer = Rbus::connect("com.example.wifi-watcher").await?;
    let mut enable = consumer
        .subscribe_value::<bool>("Device.WiFi.Enable")
        .await?;
    let mut rows = consumer.subscribe_table("Device.WiFi.Radio.").await?;
    let mut alarms = consumer
        .subscribe::<AlarmData>("Device.WiFi.Alarm!")
        .await?;

    // The app changes state through the same handle, and rbus does the
    // protocol work.
    wifi.enable.set(true); // VALUE_CHANGED to subscribers if the value changed (P9, P11)
    let change = within(5, enable.next()).await.unwrap().unwrap();
    assert_eq!((change.old, change.new), (Some(false), true));
    assert_eq!(change.by.as_deref(), Some(bus.component()));

    let id = wifi.radios.insert(Radio {
        channel: 36,
        ..Default::default()
    })?; // OBJECT_CREATED (PT3, PT9)
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Added { row, .. } if row.inst == id
    ));
    let mut status = consumer
        .subscribe_value::<String>(format!("Device.WiFi.Radio.{id}.Status"))
        .await?;
    wifi.radios.update(id, |r| r.status = "Up".into())?; // VALUE_CHANGED for each field that changed
    let up = within(5, status.next()).await.unwrap().unwrap();
    assert_eq!((up.old.as_deref(), up.new.as_str()), (Some(""), "Up"));

    // Consumers see the stored state, and their sets are checked.
    let got = rbuscli(&["get", &format!("Device.WiFi.Radio.{id}.")]).await;
    let values = rbuscli_values(&got);
    assert!(
        values
            .iter()
            .any(|(n, v)| n.ends_with(".Channel") && v == "36"),
        "{got}"
    );
    let out = rbuscli(&[
        "set",
        &format!("Device.WiFi.Radio.{id}.Channel"),
        "uint",
        "14",
    ])
    .await;
    assert!(!out.contains("succeeded"), "{out}");
    assert_eq!(wifi.radios.get(id).unwrap().channel, 36);
    consumer
        .set(format!("Device.WiFi.Radio.{id}.Channel"), 11u32)
        .await?;
    assert_eq!(wifi.radios.get(id).unwrap().channel, 11);
    let out = rbuscli(&["set", "Device.WiFi.RadioNumberOfEntries", "uint", "3"]).await;
    assert!(!out.contains("succeeded"), "{out}");

    // Rows from either side: a consumer's row goes through `new_radio`.
    let added = consumer
        .table("Device.WiFi.Radio.")
        .add_row()
        .alias("guest")
        .await?;
    assert_eq!(added.inst, id + 1);
    assert!(wifi.radios.find("guest").is_some());
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Added { row, .. } if row == added
    ));

    wifi.radios.remove(id)?; // OBJECT_DELETED
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Removed { row, .. } if row.inst == id
    ));
    wifi.alarm.publish(Alarm {
        level: 3,
        text: "DFS".into(),
    })?; // no-op when nobody is subscribed
    assert_eq!(
        within(5, alarms.next()).await.unwrap().unwrap(),
        AlarmData {
            level: 3,
            text: "DFS".into()
        }
    );
    turn.release(wifi, "Device.WiFi.Enable").await;
    Ok(())
}

/// The 4.9 impl block, as design.md writes it, over a fake radio HAL.
mod radio_driver {
    use rbus::provider::{Context, Fault};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicI32, Ordering};

    #[derive(Default)]
    pub struct Hal {
        power: AtomicI32,
    }

    impl Hal {
        pub async fn noise(&self, radio: u32) -> Result<i32, std::io::Error> {
            if radio > 8 {
                return Err(std::io::Error::other("no such radio"));
            }
            Ok(-90 - radio as i32)
        }
    }

    #[derive(Deserialize)]
    pub struct ResetArgs {
        pub delay: u32,
    }

    #[derive(Serialize)]
    pub struct ResetReply {
        pub radio: u32,
        pub after: u32,
    }

    pub struct RadioDriver {
        hal: Hal,
    }

    impl RadioDriver {
        pub fn new(hal: Hal) -> Self {
            RadioDriver { hal }
        }
    }

    #[rbus::provider(path = "Device.WiFi.Radio.{i}.")]
    impl RadioDriver {
        #[rbus(get)] // Device.WiFi.Radio.{i}.Noise
        async fn noise(&self, cx: &Context) -> Result<i32, Fault> {
            let [radio] = cx.instances()?; // the {i} values; the count is checked
            self.hal.noise(radio).await.map_err(Fault::internal)
        }

        #[rbus(get)]
        async fn transmit_power(&self, _cx: &Context) -> Result<i32, Fault> {
            Ok(self.hal.power.load(Ordering::Relaxed))
        }
        #[rbus(set = "TransmitPower")]
        async fn set_transmit_power(&self, _cx: &Context, dbm: i32) -> Result<(), Fault> {
            if !(0..=30).contains(&dbm) {
                return Err(Fault::invalid_value("0..=30 dBm"));
            }
            self.hal.power.store(dbm, Ordering::Relaxed);
            Ok(())
        }

        #[rbus(method)] // Device.WiFi.Radio.{i}.Reset()
        async fn reset(&self, cx: &Context, args: ResetArgs) -> Result<ResetReply, Fault> {
            let [radio] = cx.instances()?;
            Ok(ResetReply {
                radio,
                after: args.delay,
            })
        }
    }
}

/// 4.9 Provider: an impl block under the 4.8 model's rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_9_impl_block_under_a_model() -> rbus::Result<()> {
    use common::{rbuscli, rbuscli_values};
    use radio_driver::{Hal, RadioDriver};
    use wifi::{Radio, Wifi};

    let turn = common::Turn::take(&WIFI).await;
    let bus = Rbus::connect("com.example.wifi").await?;
    let hal = Hal::default();
    let wifi = bus.provide(Wifi::default()).await?; // several providers can mount under one component;
    let driver = bus.provide(RadioDriver::new(hal)).await?; // overlapping names are rejected at registration
    let radio = wifi.radios.insert(Radio::default())?;

    let consumer = Rbus::connect("com.example.wifi-tool").await?;
    let noise: i32 = consumer
        .get(format!("Device.WiFi.Radio.{radio}.Noise"))
        .await?;
    assert_eq!(noise, -91);
    // One partial-path get spans both providers.
    let tree = consumer
        .get_tree(format!("Device.WiFi.Radio.{radio}."))
        .await?;
    let names: Vec<String> = tree.iter().map(|(n, _)| n.to_string()).collect();
    assert!(names.iter().any(|n| n.ends_with(".Enable")), "{names:?}");
    assert!(names.iter().any(|n| n.ends_with(".Noise")), "{names:?}");

    let out = rbuscli(&[
        "set",
        &format!("Device.WiFi.Radio.{radio}.TransmitPower"),
        "int",
        "20",
    ])
    .await;
    assert!(out.contains("succeeded"), "{out}");
    let out = rbuscli(&["get", &format!("Device.WiFi.Radio.{radio}.TransmitPower")]).await;
    assert_eq!(rbuscli_values(&out)[0].1, "20", "{out}");
    let err = consumer
        .set(format!("Device.WiFi.Radio.{radio}.TransmitPower"), 99i32)
        .await
        .unwrap_err();
    assert_eq!(
        err.status(),
        Some(rbus::Status::InvalidParameterValue),
        "{err:?}"
    );

    #[derive(Serialize)]
    struct Args {
        delay: u32,
    }
    #[derive(Deserialize)]
    struct Reply {
        radio: u32,
        after: u32,
    }
    let reply: Reply = consumer
        .invoke(
            format!("Device.WiFi.Radio.{radio}.Reset()"),
            Args { delay: 4 },
        )
        .await?;
    assert_eq!((reply.radio, reply.after), (radio, 4));
    // Overlapping names are rejected.
    let again = bus
        .provide(RadioDriver::new(Hal::default()))
        .await
        .unwrap_err();
    assert_eq!(again.status(), Some(rbus::Status::ElementNameDuplicate));
    drop(driver);
    turn.release(wifi, "Device.WiFi.Enable").await;
    Ok(())
}

/// 4.9 Provider: the `DataModel` builder.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_9_data_model_builder() -> rbus::Result<()> {
    use common::{rbuscli, rbuscli_values};
    use rbus::provider::DataModel;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

    let bus = Rbus::connect("com.example.sample").await?;
    let counter = Arc::new(AtomicU32::new(5));
    let model = DataModel::new()
        .property("Device.Sample.Counter", move |_cx| {
            let c = counter.clone();
            async move { Ok(c.load(Relaxed)) }
        })
        .method("Device.Sample.Reset()", |_cx, _args: Object| async {
            Ok(Object::new())
        });
    let _provided = bus.provide(model).await?;

    let out = rbuscli(&["get", "Device.Sample.Counter"]).await;
    assert_eq!(rbuscli_values(&out)[0].1, "5", "{out}");
    let consumer = Rbus::connect("com.example.sample-tool").await?;
    let () = consumer.invoke("Device.Sample.Reset()", ()).await?;
    Ok(())
}

/// The 4.11 bridge, as design.md writes it, with a stand-in for the
/// component that serves `Device.DeviceInfo.UpTime`.
mod bridge {
    use rbus::Rbus;
    use rbus::provider::{Event, Prop};
    use serde::Serialize;

    #[derive(rbus::Model, Default)]
    #[rbus(path = "Device.Bridge.")]
    struct Bridge {
        last_uptime: Prop<u32>,
        rebooted: Event<Rebooted>,
    }

    #[derive(Serialize)]
    struct Rebooted {
        uptime_before: u32,
    }

    pub async fn main(bus: Rbus) -> rbus::Result<()> {
        let bridge = bus.provide(Bridge::default()).await?;

        let mut uptime = bus
            .subscribe_value::<u32>("Device.DeviceInfo.UpTime")
            .initial_value()
            .await?;
        while let Some(item) = uptime.next().await {
            let Ok(change) = item else { continue }; // skip Lagged
            let last = bridge.last_uptime.get();
            if change.new < last {
                bridge.rebooted.publish(Rebooted {
                    uptime_before: last,
                })?;
            }
            bridge.last_uptime.set(change.new);
        }
        Ok(())
    }
}

/// 4.11 Consumer and provider in one app.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_11_bridge_consumes_and_provides() -> rbus::Result<()> {
    use common::within;
    use rbus::provider::Prop;

    // The device: its uptime goes 100, 200, then 5 after a reboot.
    #[derive(rbus::Model, Default)]
    #[rbus(path = "Device.DeviceInfo.")]
    struct DeviceInfo {
        #[rbus(name = "UpTime")]
        up_time: Prop<u32>,
    }
    let device_bus = Rbus::connect("com.example.device").await?;
    let device = device_bus.provide(DeviceInfo::default()).await?;
    device.up_time.set(100);

    let bridge = tokio::spawn(bridge::main(Rbus::connect("com.example.bridge").await?));
    let watcher = Rbus::connect("com.example.bridge-watcher").await?;
    #[derive(Deserialize)]
    struct Rebooted {
        uptime_before: u32,
    }
    let mut reboots = watcher
        .subscribe::<Rebooted>("Device.Bridge.Rebooted!")
        .retry(rbus::Retry::exponential().max_elapsed(Duration::from_secs(10)))
        .await?;
    let mut last = watcher
        .subscribe_value::<u32>("Device.Bridge.LastUptime")
        .initial_value()
        .await?;
    while within(5, last.next()).await.unwrap().unwrap().new != 100 {}
    device.up_time.set(200);
    while within(5, last.next()).await.unwrap().unwrap().new != 200 {}
    device.up_time.set(5);
    let reboot = within(5, reboots.next()).await.unwrap().unwrap();
    assert_eq!(reboot.uptime_before, 200);
    bridge.abort();
    Ok(())
}
