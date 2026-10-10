//! `Prop`, `Table` and `Event` before registration, and the code the derive
//! macros generate, driven through the `Model`, `Row` and `Provider` traits.
//! Registered models are tested in `engine_tests::provider` and the
//! interop suite.

use rbus_wire::value::{Object, Value};
use tracing::Span;

use super::*;
use crate::provider::{
    Access, Change, Context, ElementKind, Model, Provider, Rows, SubscriberInfo, Transaction,
};
use crate::{PropertyName, TablePath};

fn cx() -> Context {
    Context::new(Some("c".into()), "rbus.c.INBOX.1".into(), Span::none())
}

#[derive(serde::Serialize)]
struct Alarm {
    level: u8,
}

#[test]
fn props_hold_and_change_values() {
    let p: Prop<u32> = Prop::default();
    assert_eq!(p.get(), 0);
    p.set(3);
    p.update(|v| *v += 1);
    p.update(|_| {});
    assert_eq!(p.with(|v| *v * 2), 8);
    p.store(9);
    assert_eq!(p.value(), Value::U32(9));
    p.bind("A.P", None);
    assert!(format!("{p:?}").contains("value: 9"));
    let q: Prop<String> = String::from("x").into();
    assert_eq!(q.get(), "x");
}

#[test]
fn events_before_registration() {
    let e: Event<Alarm> = Event::default();
    e.publish(Alarm { level: 1 }).unwrap();
    e.publish(&Alarm { level: 2 }).unwrap();
    assert!(e.subscribers().is_empty());
    let f = e.clone();
    assert!(e == f);
    e.bind("A.E!", None);
    assert!(format!("{f:?}").contains("Event"));
    // Data that isn't an object is a type error, registered or not.
    let n: Event<u32> = Event::new();
    assert!(matches!(n.publish(3), Err(Error::Type(_))));
}

#[derive(crate::Row, Clone, PartialEq, Default, Debug)]
struct Host {
    #[rbus(writable, validate = short)]
    name: String,
    up: bool,
    #[rbus(object)]
    stats: Stats,
    #[rbus(add_row, remove_row = Host::forget)]
    ip: Table<Ip>,
    #[rbus(name = "Seen")]
    seen: Event<Object>,
    #[rbus(skip)]
    _note: u8,
}

impl Host {
    async fn forget(&self, _cx: &Context, row: &RowId) -> Result<(), Fault> {
        if row.inst == 99 {
            return Err(Fault::access_denied());
        }
        Ok(())
    }
}

fn short(name: &str) -> Result<(), Fault> {
    if name.len() > 8 {
        return Err(Fault::invalid_value("name too long"));
    }
    Ok(())
}

#[derive(crate::Row, Clone, PartialEq, Default, Debug)]
struct Stats {
    #[rbus(writable)]
    bytes: u64,
}

#[derive(crate::Row, Clone, PartialEq, Default, Debug)]
struct Ip {
    #[rbus(writable)]
    address: String,
}

#[derive(crate::Model, Default)]
#[rbus(path = "Device.Net.", on_commit = Net::commit, on_subscribe = Net::subscribing)]
struct Net {
    #[rbus(writable)]
    enable: Prop<bool>,
    host_number_of_entries: Prop<u32>,
    #[rbus(name = "Host", add_row = Net::new_host, remove_row)]
    hosts: Table<Host>,
    #[rbus(name = "Fixed")]
    fixed: Table<Ip>,
    alarm: Event<Alarm>,
    info: Info,
    #[rbus(skip)]
    commits: std::sync::atomic::AtomicU32,
}

#[derive(crate::Model, Default)]
struct Info {
    #[rbus(writable)]
    location: Prop<String>,
}

impl Net {
    async fn new_host(&self, _cx: &Context, alias: Option<&str>) -> Result<Host, Fault> {
        match alias {
            Some("bad") => Err(Fault::invalid_input("bad alias")),
            _ => Ok(Host {
                name: alias.unwrap_or("new").into(),
                ..Host::default()
            }),
        }
    }

    async fn commit(&self, _cx: &Context, tx: &Transaction) -> Result<(), Fault> {
        self.commits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if tx
            .get("Device.Net.Info.Location")
            .is_some_and(|c| c.value() == &Value::from("veto"))
        {
            return Err(Fault::access_denied());
        }
        Ok(())
    }

    async fn subscribing(&self, _cx: &Context, sub: &SubscriberInfo) -> Result<(), Fault> {
        match sub.event.as_str() {
            "Device.Net.Alarm!" => Err(Fault::access_denied()),
            _ => Ok(()),
        }
    }
}

fn tx(changes: &[(&'static str, Value)]) -> Transaction {
    Transaction {
        changes: changes
            .iter()
            .map(|(n, v)| Change {
                name: PropertyName::from_static(n),
                value: v.clone(),
                instances: Vec::new(),
            })
            .collect(),
    }
}

async fn get(net: &Net, name: &'static str) -> Result<Value, Fault> {
    Provider::get(net, &cx(), &PropertyName::from_static(name)).await
}

#[test]
fn tables_before_registration() {
    let t: Table<Ip> = Table::new();
    assert!(t.is_empty());
    let a = t
        .insert(Ip {
            address: "a".into(),
        })
        .unwrap();
    let b = t
        .insert_with_alias(
            "lan",
            Ip {
                address: "b".into(),
            },
        )
        .unwrap();
    assert_eq!((a, b), (1, 2));
    let dup = t.insert_with_alias("lan", Ip::default()).unwrap_err();
    assert_eq!(dup.status(), Some(Status::ElementNameDuplicate));
    assert_eq!(t.find("lan"), Some(2));
    assert_eq!(t.find("wan"), None);
    t.update(1, |ip| ip.address = "c".into()).unwrap();
    assert_eq!(t.get(1).unwrap().address, "c");
    assert_eq!(t.with(2, |ip| ip.address.clone()).unwrap(), "b");
    assert_eq!(t.len(), 2);
    assert_eq!(
        t.ids()[1],
        RowId {
            inst: 2,
            alias: Some("lan".into())
        }
    );
    assert_eq!(t.rows().len(), 2);
    assert!(t.subscribers().is_empty());
    let missing = t.update(7, |_| {}).unwrap_err();
    assert_eq!(missing.status(), Some(Status::ElementDoesNotExist));
    assert_eq!(
        t.remove(7).unwrap_err().status(),
        Some(Status::ElementDoesNotExist)
    );
    assert_eq!(t.remove(1).unwrap().address, "c");
    // Numbers are never reused.
    assert_eq!(t.insert(Ip::default()).unwrap(), 3);
    // Clones are the same table.
    let same = t.clone();
    assert!(same == t && Table::<Ip>::default() != t);
    assert!(format!("{t:?}").contains("rows"));
}

#[test]
fn the_derive_describes_the_schema() {
    let schema = Net::default().schema();
    let names: Vec<(&str, ElementKind)> = schema
        .elements()
        .iter()
        .map(|e| (e.name(), e.kind()))
        .collect();
    use ElementKind::{Event, Property, Table};
    assert_eq!(
        names,
        [
            ("Device.Net.Enable", Property(Access::ReadWrite)),
            ("Device.Net.HostNumberOfEntries", Property(Access::ReadOnly)),
            ("Device.Net.Host.{i}.", Table(Rows::Dynamic)),
            ("Device.Net.Host.{i}.Name", Property(Access::ReadWrite)),
            ("Device.Net.Host.{i}.Up", Property(Access::ReadOnly)),
            (
                "Device.Net.Host.{i}.Stats.Bytes",
                Property(Access::ReadWrite)
            ),
            ("Device.Net.Host.{i}.Ip.{i}.", Table(Rows::Dynamic)),
            (
                "Device.Net.Host.{i}.Ip.{i}.Address",
                Property(Access::ReadWrite)
            ),
            ("Device.Net.Host.{i}.Seen", Event),
            ("Device.Net.Fixed.{i}.", Table(Rows::Fixed)),
            ("Device.Net.Fixed.{i}.Address", Property(Access::ReadWrite)),
            ("Device.Net.Alarm!", Event),
            ("Device.Net.Info.Location", Property(Access::ReadWrite)),
        ]
    );
    assert!(!schema.is_serialized());
}

#[tokio::test]
async fn the_derive_serves_gets_and_sets() {
    let net = Net::default();
    net.enable.set(true);
    let id = net
        .hosts
        .insert(Host {
            name: "pc".into(),
            ..Host::default()
        })
        .unwrap();
    assert_eq!(
        get(&net, "Device.Net.Enable").await.unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        get(&net, "Device.Net.Host.1.Name").await.unwrap(),
        Value::from("pc")
    );
    assert_eq!(
        get(&net, "Device.Net.Host.1.Stats.Bytes").await.unwrap(),
        Value::U64(0)
    );
    for missing in [
        "Device.Net.Nope",
        "Device.Net.Host.2.Name",
        "Device.Net.Host.x.Name",
        "Device.Net.Host.1.Nope",
        "Device.Other",
    ] {
        let err = get(&net, missing).await.unwrap_err();
        assert_eq!(err.status(), Status::ElementDoesNotExist, "{missing}");
    }

    let cx = cx();
    Provider::set(
        &net,
        &cx,
        tx(&[
            ("Device.Net.Enable", Value::Bool(false)),
            ("Device.Net.Host.1.Name", Value::from("laptop")),
            ("Device.Net.Host.1.Stats.Bytes", Value::U64(5)),
            ("Device.Net.Info.Location", Value::from("hall")),
        ]),
    )
    .await
    .unwrap();
    assert!(!net.enable.get());
    assert_eq!(net.hosts.get(id).unwrap().name, "laptop");
    assert_eq!(net.hosts.get(id).unwrap().stats.bytes, 5);
    assert_eq!(net.info.location.get(), "hall");

    // Any failure leaves everything as it was, and names the parameter.
    let cases: [(&'static str, Value, Status); 5] = [
        (
            "Device.Net.Host.1.Name",
            Value::from("much too long"),
            Status::InvalidParameterValue,
        ),
        (
            "Device.Net.Enable",
            Value::U32(1),
            Status::InvalidParameterType,
        ),
        (
            "Device.Net.HostNumberOfEntries",
            Value::U32(1),
            Status::NotWritable,
        ),
        (
            "Device.Net.Host.1.Up",
            Value::Bool(true),
            Status::NotWritable,
        ),
        (
            "Device.Net.Host.9.Name",
            Value::from("x"),
            Status::ElementDoesNotExist,
        ),
    ];
    for (name, value, status) in cases {
        let err = Provider::set(
            &net,
            &cx,
            tx(&[
                ("Device.Net.Info.Location", Value::from("x")),
                (name, value),
            ]),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (err.name().unwrap().as_str(), err.fault().status()),
            (name, status)
        );
        assert_eq!(net.info.location.get(), "hall");
    }
    // The commit hook runs after the checks, before anything is stored.
    let commits = net.commits.load(std::sync::atomic::Ordering::Relaxed);
    let err = Provider::set(
        &net,
        &cx,
        tx(&[
            ("Device.Net.Enable", Value::Bool(true)),
            ("Device.Net.Info.Location", Value::from("veto")),
        ]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.fault().status(), Status::AccessNotAllowed);
    assert_eq!(net.info.location.get(), "hall");
    assert!(!net.enable.get());
    assert_eq!(
        net.commits.load(std::sync::atomic::Ordering::Relaxed),
        commits + 1
    );
}

#[tokio::test]
async fn the_derive_adds_and_removes_rows_for_consumers() {
    let net = Net::default();
    let cx = cx();
    let hosts = TablePath::from_static("Device.Net.Host.");
    assert_eq!(
        Provider::add_row(&net, &cx, &hosts, Some("tv"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(net.hosts.get(1).unwrap().name, "tv");
    let err = Provider::add_row(&net, &cx, &hosts, Some("bad"))
        .await
        .unwrap_err();
    assert_eq!(err.status(), Status::InvalidInput);
    // Nested tables: default rows, and a remove hook.
    let ips = TablePath::from_static("Device.Net.Host.1.Ip.");
    assert_eq!(Provider::add_row(&net, &cx, &ips, None).await.unwrap(), 1);
    assert_eq!(net.hosts.get(1).unwrap().ip.len(), 1);
    Provider::remove_row(&net, &cx, &ips, &RowId::from(1))
        .await
        .unwrap();
    assert!(net.hosts.get(1).unwrap().ip.is_empty());
    let err = Provider::remove_row(&net, &cx, &ips, &RowId::from(99))
        .await
        .unwrap_err();
    assert_eq!(err.status(), Status::AccessNotAllowed);
    let err = Provider::remove_row(&net, &cx, &ips, &RowId::from(5))
        .await
        .unwrap_err();
    assert_eq!(err.status(), Status::ElementDoesNotExist);
    // Tables without hooks, and paths that lead nowhere.
    for (table, status) in [
        ("Device.Net.Fixed.", Status::InvalidOperation),
        ("Device.Net.Host.7.Ip.", Status::ElementDoesNotExist),
        ("Device.Net.Info.", Status::InvalidOperation),
        ("Device.Net.Host.1.Stats.", Status::InvalidOperation),
        ("Device.Other.", Status::ElementDoesNotExist),
    ] {
        let t = TablePath::parse(table).unwrap();
        let add = Provider::add_row(&net, &cx, &t, None).await.unwrap_err();
        let remove = Provider::remove_row(&net, &cx, &t, &RowId::from(1))
            .await
            .unwrap_err();
        assert_eq!((add.status(), remove.status()), (status, status), "{table}");
    }
    Provider::remove_row(&net, &cx, &hosts, &RowId::from(1))
        .await
        .unwrap();
    assert!(net.hosts.is_empty());
}

#[tokio::test]
async fn the_derive_hooks_subscriptions() {
    let net = Net::default();
    let sub = |event: &str| SubscriberInfo {
        event: event.into(),
        inbox: "i".into(),
        component_id: 1,
        filter: None,
        id: 1,
    };
    let refused = net.on_subscribe(&cx(), &sub("Device.Net.Alarm!")).await;
    assert_eq!(refused.unwrap_err().status(), Status::AccessNotAllowed);
    net.on_subscribe(&cx(), &sub("Device.Net.Enable"))
        .await
        .unwrap();
}

#[test]
fn rows_report_their_changes() {
    let old = Host::default();
    let new = Host {
        name: "x".into(),
        up: true,
        stats: Stats { bytes: 1 },
        ..old.clone()
    };
    let mut changes = Vec::new();
    new.diff(&old, "", &mut changes);
    assert_eq!(
        changes,
        [
            ("Name".to_owned(), Value::from(""), Value::from("x")),
            ("Up".to_owned(), Value::Bool(false), Value::Bool(true)),
            ("Stats.Bytes".to_owned(), Value::U64(0), Value::U64(1)),
        ]
    );
    assert_eq!(
        Model::get(&Info::default(), &[]).unwrap_err().status(),
        Status::ElementDoesNotExist
    );
}
