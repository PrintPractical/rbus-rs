//! rbus's own C test consumer (`rbusTestConsumer`) against the Rust
//! conformance provider (`conformance/`), one group at a time. A group passes
//! when the C consumer counts no failures, so C consumers can't tell the Rust
//! provider from `rbusTestProvider`. Run with `scripts/interop.sh`.

mod common;
mod conformance;

use std::process::Command;
use std::time::Duration;

use conformance::Conformance;
use rbus::Rbus;
use rbus_testkit::broker::{RawClient, broker_socket, test_inbox};

/// The groups share one data model and the C consumer's names, so they run
/// one at a time, each against a fresh provider.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Pass and fail counts from the consumer's results table:
/// `# Value          |41             |0               #`.
fn counts(stdout: &str, group: &str) -> Option<(u32, u32)> {
    stdout.lines().find_map(|line| {
        let cols: Vec<&str> = line
            .trim_matches(|c| c == '#' || c == ' ')
            .split('|')
            .collect();
        match cols.as_slice() {
            [name, pass, fail] if name.trim() == group => {
                Some((pass.trim().parse().ok()?, fail.trim().parse().ok()?))
            }
            _ => None,
        }
    })
}

/// Runs one `rbusTestConsumer` group against a fresh conformance provider.
/// It must pass as many checks as it does against the C `rbusTestProvider`
/// (counted at the pinned rbus commit), and fail none.
async fn run_group(group: &'static str, c_provider_passes: u32) {
    let _one = ONE_AT_A_TIME.lock().await;
    // The C rbusTestProvider of another test binary must be gone: it owns the
    // same names.
    let mut raw = RawClient::connect(&test_inbox("conformance")).unwrap();
    raw.wait_for_no_owner(
        "Device.TestProvider.BigBytes",
        "TestProvider",
        Duration::from_secs(15),
    )
    .unwrap();
    // The same component name as the C provider: value-change events name
    // it as `by`, and `--ValueChange` checks that.
    let bus = Rbus::builder("TestProvider")
        .socket(broker_socket())
        .connect()
        .await
        .unwrap();
    let provider = Conformance::start(&bus).await.unwrap();
    let out = tokio::task::spawn_blocking(move || {
        Command::new("rbusTestConsumer")
            .arg(format!("--{group}"))
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    drop(provider);
    let stdout = String::from_utf8_lossy(&out.stdout);
    // The whole output, for when a group fails.
    let log = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("rbusTestConsumer-{group}.log"));
    let _ = std::fs::write(&log, stdout.as_bytes());
    let tail: String = {
        let lines: Vec<&str> = stdout.lines().collect();
        lines[lines.len().saturating_sub(60)..].join("\n")
    };
    assert_eq!(
        counts(&stdout, group),
        Some((c_provider_passes, 0)),
        "rbusTestConsumer --{group}: (pass, fail)\n{tail}"
    );
}

#[test]
fn results_table_is_parsed() {
    let out = "# Test Name      |Pass Count     |Fail Count      #\n\
               # ---------------|---------------|--------------- #\n\
               # Value          |41             |2               #";
    assert_eq!(counts(out, "Value"), Some((41, 2)));
    assert_eq!(counts(out, "Tables"), None);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p1_v1_rbus_test_consumer_value_group() {
    run_group("Value", 48).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p5_rbus_test_consumer_methods_group() {
    run_group("Methods", 55).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt2_pt10_rbus_test_consumer_tables_group() {
    run_group("Tables", 1228).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt8_rbus_test_consumer_partial_path_group() {
    run_group("PartialPath", 252).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p12_rbus_test_consumer_filter_group() {
    run_group("Filter", 54).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p9_p10_p12_rbus_test_consumer_value_change_group() {
    run_group("ValueChange", 66).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt9_p9_rbus_test_consumer_events_group() {
    run_group("Events", 69).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p4_p8_rbus_test_consumer_subscribe_group() {
    run_group("Subscribe", 53).await;
}
