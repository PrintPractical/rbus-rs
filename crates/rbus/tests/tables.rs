//! Tables through a real C provider (T1–T3, E4). Run with `scripts/interop.sh`.
//!
//! The tests share `Device.TestProvider.Table1.`, so each uses its own aliases
//! and ignores other tests' rows.

mod common;

use common::{connect, test_provider, within};
use rbus::{Error, RowEvent, RowRef};

const T1: &str = "Device.TestProvider.Table1.";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn t1_t3_add_list_and_remove_rows() {
    let _p = test_provider();
    let bus = connect("t1-rows").await;
    let table = bus.table(T1);
    let a = table.add_row().alias("rs-t1-a").await.unwrap();
    assert_eq!(a.alias.as_deref(), Some("rs-t1-a"));
    let b = table.add_row().await.unwrap();
    assert_ne!(a.inst, b.inst);
    let rows = table.rows().await.unwrap();
    assert!(rows.contains(&a), "{rows:?}");
    assert!(rows.iter().any(|r| r.inst == b.inst));

    table
        .remove_row(RowRef::Alias("rs-t1-a".into()))
        .await
        .unwrap();
    table.remove_row(&b).await.unwrap();
    let rows = table.rows().await.unwrap();
    assert!(
        !rows.iter().any(|r| r.inst == a.inst || r.inst == b.inst),
        "{rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn t2_removing_a_missing_row_fails() {
    let _p = test_provider();
    let bus = connect("t2-missing").await;
    let r = bus
        .table(T1)
        .remove_row(RowRef::Alias("rs-no-such-row".into()))
        .await;
    assert!(
        matches!(r, Err(Error::Status { .. } | Error::NotFound(_))),
        "{r:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn t1_nested_tables_by_alias() {
    let _p = test_provider();
    let bus = connect("t1-nested").await;
    let outer = bus.table(T1).add_row().alias("rs-outer").await.unwrap();
    let inner = bus.table(format!("{T1}[rs-outer].Table2."));
    let row = inner.add_row().alias("rs-inner").await.unwrap();
    assert!(inner.rows().await.unwrap().contains(&row));
    bus.table(T1).remove_row(&outer).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn e4_row_events_arrive_for_adds_and_removes() {
    let _p = test_provider();
    let bus = connect("e4-rows").await;
    let mut events = bus.subscribe_table(T1).await.unwrap();
    let table = bus.table(T1);
    let row = table.add_row().alias("rs-e4").await.unwrap();
    loop {
        match within(10, events.next()).await.unwrap().unwrap() {
            RowEvent::Added { table, row: r } if r.alias.as_deref() == Some("rs-e4") => {
                assert_eq!(table, T1);
                assert_eq!(r.inst, row.inst);
                break;
            }
            _ => {} // another test's row
        }
    }
    table.remove_row(&row).await.unwrap();
    loop {
        if let RowEvent::Removed { row: r, .. } = within(10, events.next()).await.unwrap().unwrap()
        {
            if r.inst == row.inst {
                break;
            }
        }
    }
    events.unsubscribe().await.unwrap();
}
