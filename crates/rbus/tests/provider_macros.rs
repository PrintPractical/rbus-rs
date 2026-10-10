//! Providers written with the macros (M6), served through the real broker
//! to Rust consumers and `rbuscli`. Each test serves its own root. Run with
//! `scripts/interop.sh`.

mod common;

use std::time::Duration;

use common::{Rbuscli, Turn, connect, rbuscli, rbuscli_events, rbuscli_values, within};
use rbus::provider::{Context, Fault, Prop, Table};
use rbus::{RowEvent, RowId, Status};

#[derive(rbus::Model, Default)]
#[rbus(path = "Device.P11.")]
struct Light {
    #[rbus(writable)]
    level: Prop<u32>,
    name: Prop<String>,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p11_prop_changes_reach_c_and_rust_subscribers() {
    let provider_bus = connect("p11-provider").await;
    let light = provider_bus.provide(Light::default()).await.unwrap();
    let bus = connect("p11-consumer").await;
    let mut levels = bus
        .subscribe_value::<u32>("Device.P11.Level")
        .initial_value()
        .await
        .unwrap();
    let mut cli = Rbuscli::start();
    cli.send("sub Device.P11.Level");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(within(5, levels.next()).await.unwrap().unwrap().new, 0);

    // The app's change is published at once, by the provider.
    light.level.set(5);
    let c = within(5, levels.next()).await.unwrap().unwrap();
    assert_eq!((c.old, c.new), (Some(0), 5));
    assert_eq!(c.by.as_deref(), Some(provider_bus.component()));
    // Setting the same value again publishes nothing.
    light.level.set(5);
    // A consumer's set is published once, by the setter.
    let out = rbuscli(&["set", "Device.P11.Level", "uint", "7"]).await;
    assert!(out.contains("succeeded"), "{out}");
    let c = within(5, levels.next()).await.unwrap().unwrap();
    assert_eq!((c.old, c.new), (Some(5), 7));
    assert!(c.by.as_deref().unwrap().starts_with("rbuscli"), "{c:?}");
    assert_eq!(light.level.get(), 7);
    light.level.update(|l| *l += 1);
    let c = within(5, levels.next()).await.unwrap().unwrap();
    assert_eq!(c.new, 8);
    // Read-only to consumers, writable by the app.
    light.name.set("lamp".into());
    let out = rbuscli(&["set", "Device.P11.Name", "string", "x"]).await;
    assert!(!out.contains("succeeded"), "{out}");
    let name: String = bus.get("Device.P11.Name").await.unwrap();
    assert_eq!(name, "lamp");

    // rbuscli saw the same three changes, each once.
    let events = rbuscli_events(&cli.finish().await);
    let by: Vec<&str> = events
        .iter()
        .filter_map(|e| e.lines().last()?.rsplit("value:").next())
        .collect();
    let me = provider_bus.component();
    assert!(
        by.len() == 3 && by[0] == me && by[1].starts_with("rbuscli") && by[2] == me,
        "{events:#?}"
    );
}

/// The tests serving `Network` take turns.
static PT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(rbus::Model, Default)]
#[rbus(path = "Device.PT.")]
struct Network {
    #[rbus(name = "Host", add_row = Network::new_host, remove_row = Network::drop_host)]
    hosts: Table<Host>,
    #[rbus(name = "Static")]
    fixed: Table<Address>,
}

#[derive(rbus::Row, Clone, PartialEq, Default, Debug)]
struct Host {
    #[rbus(writable)]
    name: String,
    active: bool,
    #[rbus(name = "IPv4Address", add_row, remove_row)]
    addresses: Table<Address>,
    #[rbus(object)]
    stats: Stats,
}

#[derive(rbus::Row, Clone, PartialEq, Default, Debug)]
struct Address {
    #[rbus(writable, name = "IPAddress")]
    ip: String,
}

#[derive(rbus::Row, Clone, PartialEq, Default, Debug)]
struct Stats {
    bytes_sent: u64,
}

impl Network {
    async fn new_host(&self, _cx: &Context, alias: Option<&str>) -> Result<Host, Fault> {
        if alias == Some("forbidden") {
            return Err(Fault::access_denied());
        }
        Ok(Host {
            name: alias.unwrap_or("unnamed").into(),
            ..Host::default()
        })
    }

    async fn drop_host(&self, _cx: &Context, row: &RowId) -> Result<(), Fault> {
        match self.hosts.with(row.inst, |h| h.active) {
            Some(true) => Err(Fault::invalid_input("host is active")),
            _ => Ok(()),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt1_pt5_nested_tables_from_row_structs() {
    let turn = Turn::take(&PT).await;
    let provider_bus = connect("pt1-provider").await;
    let net = provider_bus.provide(Network::default()).await.unwrap();
    // The app adds a row with rows of its own, before and after.
    let early = Host {
        name: "router".into(),
        ..Host::default()
    };
    early
        .addresses
        .insert(Address {
            ip: "10.0.0.1".into(),
        })
        .unwrap();
    let h = net.hosts.insert(early).unwrap();
    net.hosts
        .with(h, |host| {
            host.addresses.insert(Address {
                ip: "10.0.0.2".into(),
            })
        })
        .unwrap()
        .unwrap();

    let out = rbuscli(&["getnames", "Device.PT.Host.", "false"]).await;
    for name in [
        "Device.PT.Host.1.Name",
        "Device.PT.Host.1.IPv4Address.2.IPAddress",
        "Device.PT.Host.1.Stats.BytesSent",
    ] {
        assert!(out.contains(name), "{name}: {out}");
    }
    // A consumer adds a nested row through the default hook.
    let bus = connect("pt1-consumer").await;
    let nested = bus
        .table(format!("Device.PT.Host.{h}.IPv4Address."))
        .add_row()
        .await
        .unwrap();
    assert_eq!(nested.inst, 3);
    bus.set(
        format!("Device.PT.Host.{h}.IPv4Address.3.IPAddress"),
        "10.0.0.3",
    )
    .await
    .unwrap();
    let tree = bus.get_tree(format!("Device.PT.Host.{h}.")).await.unwrap();
    let ips: Vec<String> = tree
        .iter()
        .filter(|(n, _)| n.ends_with(".IPAddress"))
        .map(|(_, v)| format!("{v:?}"))
        .collect();
    assert_eq!(ips.len(), 3, "{ips:?}");
    let wildcard = rbuscli(&["get", "Device.PT.Host.*.IPv4Address.*.IPAddress"]).await;
    assert_eq!(rbuscli_values(&wildcard).len(), 3, "{wildcard}");
    // Removing the outer row removes the nested rows with it.
    net.hosts.remove(h).unwrap();
    let err = bus
        .get::<String>(format!("Device.PT.Host.{h}.IPv4Address.1.IPAddress"))
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementDoesNotExist), "{err:?}");
    // A Fixed table refuses consumers' rows.
    let out = rbuscli(&["addrow", "Device.PT.Static."]).await;
    assert!(!out.contains(" added"), "{out}");
    turn.release(net, "Device.PT.Host.{i}.").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt3_pt9_app_rows_publish_their_events() {
    let turn = Turn::take(&PT).await;
    let provider_bus = connect("pt3-provider").await;
    let net = provider_bus
        .provide(Network {
            hosts: Table::new(),
            fixed: Table::new(),
        })
        .await
        .unwrap();
    let bus = connect("pt3-consumer").await;
    let mut rows = bus.subscribe_table("Device.PT.Host.").await.unwrap();
    let mut names = bus
        .subscribe_value::<String>("Device.PT.Host.*.Name")
        .await
        .unwrap();
    let mut cli = Rbuscli::start();
    cli.send("sub Device.PT.Host.");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let id = net
        .hosts
        .insert(Host {
            name: "printer".into(),
            ..Host::default()
        })
        .unwrap();
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Added { row, .. } if row.inst == id
    ));
    // Every changed field is published; unchanged ones are not.
    net.hosts
        .update(id, |h| {
            h.name = "scanner".into();
            h.stats.bytes_sent = 10;
        })
        .unwrap();
    let change = within(5, names.next()).await.unwrap().unwrap();
    assert_eq!(change.name, format!("Device.PT.Host.{id}.Name"));
    assert_eq!(
        (change.old.as_deref(), change.new.as_str()),
        (Some("printer"), "scanner")
    );
    // A consumer's row arrives the same way, once.
    let added = bus
        .table("Device.PT.Host.")
        .add_row()
        .alias("tv")
        .await
        .unwrap();
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Added { row, .. } if row == added
    ));
    net.hosts.remove(id).unwrap();
    assert!(matches!(
        within(5, rows.next()).await.unwrap().unwrap(),
        RowEvent::Removed { row, .. } if row.inst == id
    ));
    let events = rbuscli_events(&cli.finish().await);
    let kinds: Vec<&str> = events
        .iter()
        .map(|e| e.lines().next().unwrap().rsplit(' ').next().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "RBUS_EVENT_OBJECT_CREATED",
            "RBUS_EVENT_OBJECT_CREATED",
            "RBUS_EVENT_OBJECT_DELETED"
        ],
        "{events:#?}"
    );
    tokio::time::timeout(Duration::from_millis(300), rows.next())
        .await
        .expect_err("no second event for the consumer's row");
    turn.release(net, "Device.PT.Host.{i}.").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt4_pt2_aliases_and_row_hooks() {
    let turn = Turn::take(&PT).await;
    let provider_bus = connect("pt4-provider").await;
    let net = provider_bus.provide(Network::default()).await.unwrap();
    let lan = net
        .hosts
        .insert_with_alias(
            "lan",
            Host {
                name: "desk".into(),
                ..Host::default()
            },
        )
        .unwrap();
    // Aliases work in consumers' names...
    let out = rbuscli(&["get", "Device.PT.Host.[lan].Name"]).await;
    assert_eq!(rbuscli_values(&out)[0].1, "desk", "{out}");
    let bus = connect("pt4-consumer").await;
    bus.set("Device.PT.Host.[lan].Name", "laptop")
        .await
        .unwrap();
    assert_eq!(net.hosts.get(lan).unwrap().name, "laptop");
    // ... are unique whoever adds the row...
    let err = net
        .hosts
        .insert_with_alias("lan", Host::default())
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementNameDuplicate));
    let err = bus
        .table("Device.PT.Host.")
        .add_row()
        .alias("lan")
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementNameDuplicate), "{err:?}");
    // ... and reach the add_row hook.
    let out = rbuscli(&["addrow", "Device.PT.Host.", "kitchen"]).await;
    assert!(out.contains(" added"), "{out}");
    let kitchen = net.hosts.find("kitchen").unwrap();
    assert_eq!(net.hosts.get(kitchen).unwrap().name, "kitchen");
    // Hooks can refuse.
    let err = bus
        .table("Device.PT.Host.")
        .add_row()
        .alias("forbidden")
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::AccessNotAllowed), "{err:?}");
    net.hosts.update(kitchen, |h| h.active = true).unwrap();
    let err = bus
        .table("Device.PT.Host.")
        .remove_row(&RowId::from(kitchen))
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::InvalidInput), "{err:?}");
    net.hosts.update(kitchen, |h| h.active = false).unwrap();
    let out = rbuscli(&["delrow", "Device.PT.Host.[kitchen]."]).await;
    assert!(out.contains("deleted"), "{out}");
    assert!(net.hosts.find("kitchen").is_none());
    assert_eq!(net.hosts.len(), 1);
    turn.release(net, "Device.PT.Host.{i}.").await;
}
