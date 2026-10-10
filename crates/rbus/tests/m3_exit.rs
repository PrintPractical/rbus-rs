//! The M3 exit test: value-change and general events from a C provider, and
//! rows added with `rbuscli` arriving as `Added`. Run with `scripts/interop.sh`.

mod common;

use common::{connect, test_provider, within};
use rbus::{Object, RowEvent};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m3_exit_events_and_rows_from_c_peers() {
    let _p = test_provider();
    let bus = connect("m3-exit").await;

    // VCParam changes on every read, so the provider's own change detection
    // publishes it about every two seconds.
    let mut changes = bus
        .subscribe_value::<String>("Device.TestProvider.VCParam")
        .await
        .unwrap();
    let c = within(10, changes.next()).await.unwrap().unwrap();
    assert!(
        c.old.is_some() && c.old.as_deref() != Some(c.new.as_str()),
        "{c:?}"
    );

    let mut general = bus
        .subscribe::<Object>("Device.TestProvider.Event1!")
        .await
        .unwrap();
    within(10, general.next()).await.unwrap().unwrap();

    let table = "Device.TestProvider.Table1.";
    let mut rows = bus.subscribe_table(table).await.unwrap();
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("rbuscli")
            .args(["addrow", table, "rs-exit"])
            .output()
            .unwrap()
            .status
    })
    .await
    .unwrap();
    assert!(status.success());
    loop {
        if let RowEvent::Added { row, .. } = within(10, rows.next()).await.unwrap().unwrap() {
            if row.alias.as_deref() == Some("rs-exit") {
                bus.table(table).remove_row(&row).await.unwrap();
                break;
            }
        }
    }
}
