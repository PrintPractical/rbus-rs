//! Partial-path and wildcard gets through real C providers (C3, C4, C5).
//! Run with `scripts/interop.sh`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{connect, sample_provider, test_provider};
use rbus::Value;
use rbus::wire::body::StatusResponse;
use rbus::wire::message::response_payload;
use rbus_testkit::broker::{FLAG_REQUEST, RawClient, TOPIC_SUBSCRIBE, test_inbox};
use serde::Deserialize;

const PP: &str = "Device.TestProvider.PartialPath1.";

/// The C5 test adds a broken owner under `Device.SampleProvider.`; tests that
/// read that subtree take turns with it.
static SAMPLE_SUBTREE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Row {
    param1: String,
    param2: String,
    sub_object1: SubObject1,
    sub_table: Vec<SubRow>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct SubObject1 {
    param3: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct SubRow {
    param5: String,
    sub_object2: SubObject2,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct SubObject2 {
    param7: String,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c3_partial_path_get_returns_a_nested_tree() {
    let _p = test_provider();
    let bus = connect("c3-tree").await;
    let tree = bus.get_tree(PP).await.unwrap();
    assert!(tree.failures().is_empty());
    // Two rows, with one and three sub-table rows: 7 + 13 values. The
    // provider sets each value to its own name.
    assert_eq!(
        tree.len(),
        20,
        "{:?}",
        tree.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    for (name, value) in &tree {
        assert_eq!(value, &Value::String(name.to_owned()));
    }
    let rows: Vec<Row> = tree.deserialize().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].param1, format!("{PP}1.Param1"));
    assert_eq!(rows[0].param2, format!("{PP}1.Param2"));
    assert_eq!(
        rows[0].sub_object1.param3,
        format!("{PP}1.SubObject1.Param3")
    );
    assert_eq!(rows[1].sub_table.len(), 3);
    assert_eq!(
        rows[1].sub_table[2].param5,
        format!("{PP}2.SubTable.3.Param5")
    );
    assert_eq!(
        rows[1].sub_table[0].sub_object2.param7,
        format!("{PP}2.SubTable.1.SubObject2.Param7")
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c3_partial_path_spanning_two_components() {
    let _turn = SAMPLE_SUBTREE.lock().await;
    let _p = sample_provider();
    let bus = connect("c3-two").await;
    let tree = bus.get_tree("Device.SampleProvider.").await.unwrap();
    assert!(tree.failures().is_empty(), "{:?}", tree.failures());
    assert_eq!(
        tree.get("Device.SampleProvider.AllTypes.UInt64Data"),
        Some(&Value::U64(u64::MAX))
    );
    assert_eq!(
        tree.get("Device.SampleProvider.NestedObject1.TestParam"),
        Some(&Value::String("Testing Rbus Nested Object".into()))
    );
}

#[derive(Deserialize, Debug)]
struct OnlyParam1 {
    #[serde(rename = "Param1")]
    param1: String,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c4_wildcard_get_across_rows() {
    let _p = test_provider();
    let bus = connect("c4-wildcard").await;
    let tree = bus.get_tree(format!("{PP}*.Param1")).await.unwrap();
    let names: Vec<&str> = tree.iter().map(|(n, _)| n).collect();
    assert_eq!(names, [format!("{PP}1.Param1"), format!("{PP}2.Param1")]);
    let by_row: BTreeMap<u32, OnlyParam1> = tree.deserialize().unwrap();
    assert_eq!(by_row[&2].param1, format!("{PP}2.Param1"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c3_c4_nothing_owns_the_path() {
    let bus = connect("c3-none").await;
    assert!(matches!(
        bus.get_tree("Device.Nobody.Here.").await,
        Err(rbus::Error::NotFound(_))
    ));
}

/// A provider that owns `Device.SampleProvider.Broken.Param` and answers every
/// request with BUS_ERROR. It runs until `stop` is set.
fn broken_provider(stop: Arc<AtomicBool>) -> std::sync::mpsc::Receiver<()> {
    let (ready_tx, ready) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut c = RawClient::connect(&test_inbox("broken")).unwrap();
        let component = "rbus-rs.test.broken";
        c.send(
            0,
            TOPIC_SUBSCRIBE,
            "",
            format!(r#"{{"add":1,"topic":"{component}","route_id":10000}}"#).as_bytes(),
        )
        .unwrap();
        let alias = r#"{"add":1,"topic":"Device.SampleProvider.Broken.Param","route_id":10000}"#;
        c.request(TOPIC_SUBSCRIBE, alias.as_bytes()).unwrap();
        ready_tx.send(()).unwrap();
        let failure = response_payload(&StatusResponse { status: 1 });
        while !stop.load(Ordering::Relaxed) {
            if let Ok(f) = c.recv() {
                if f.header.flags & FLAG_REQUEST != 0 {
                    c.respond(&f, 0x10, &failure).unwrap();
                }
            }
        }
    });
    ready
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c5_a_failing_component_is_reported_not_masked() {
    let _turn = SAMPLE_SUBTREE.lock().await;
    let _p = sample_provider();
    let stop = Arc::new(AtomicBool::new(false));
    broken_provider(stop.clone()).recv().unwrap();
    let bus = connect("c5-partial").await;
    let tree = bus.get_tree("Device.SampleProvider.").await.unwrap();
    stop.store(true, Ordering::Relaxed);
    // Its routes go when its connection does.
    tokio::time::sleep(std::time::Duration::from_millis(5500)).await;
    // The two good components still answered.
    assert!(
        tree.get("Device.SampleProvider.AllTypes.BoolData")
            .is_some()
    );
    assert!(
        tree.get("Device.SampleProvider.SampleData.BoolData")
            .is_some()
    );
    let failures = tree.failures();
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].component, "rbus-rs.test.broken");
    assert_eq!(failures[0].error.status(), Some(rbus::Status::BusError));
}
