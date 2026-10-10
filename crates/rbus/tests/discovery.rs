//! Discovery through the real broker (D1, D2, D4, D5). Run with `scripts/interop.sh`.

mod common;

use common::{connect, sample_provider, test_provider, within};
use rbus::{Advisory, Error};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d1_owner_of_an_element() {
    let _p = test_provider();
    let bus = connect("d1-owner").await;
    let owner = bus
        .discover()
        .owner("Device.TestProvider.Value.INT32")
        .await
        .unwrap();
    assert_eq!(owner, "TestProvider");
    assert!(matches!(
        bus.discover().owner("Device.Nobody.Owns.This").await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d1_owners_of_a_partial_path() {
    let _p = sample_provider();
    let bus = connect("d1-owners").await;
    let mut owners = bus
        .discover()
        .owners("Device.SampleProvider.")
        .await
        .unwrap();
    owners.sort();
    assert_eq!(owners, ["Second-Provider-for-Sample", "rbusSampleProvider"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d2_elements_of_a_component() {
    let _p = sample_provider();
    let bus = connect("d2-elements").await;
    let names = bus.discover().elements("rbusSampleProvider").await.unwrap();
    assert!(
        names
            .iter()
            .any(|n| n == "Device.SampleProvider.SampleData.IntData"),
        "{names:?}"
    );
    assert!(!names.iter().any(|n| n == "rbusSampleProvider"));
    assert!(
        !names
            .iter()
            .any(|n| n.starts_with("Device.SampleProvider.AllTypes."))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d4_components_lists_providers_and_consumers() {
    let _p = test_provider();
    let bus = connect("d4-components").await;
    let comps = bus.discover().components().await.unwrap();
    assert!(comps.iter().any(|c| c == "TestProvider"), "{comps:?}");
    assert!(comps.iter().any(|c| c == bus.component()), "{comps:?}");
    assert!(!comps.iter().any(|c| c.contains(".INBOX.")), "{comps:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn d5_advisories_report_clients_coming_and_going() {
    let bus = connect("d5-advisories").await;
    let mut adv = bus.discover().advisories();
    // A round trip makes sure the broker has our advisory route.
    bus.discover().components().await.unwrap();
    let other = connect("d5-other").await;
    let inbox = other.inbox().to_owned();
    loop {
        if let Advisory::Connected { inbox: i } = within(5, adv.next()).await.unwrap().unwrap() {
            if i == inbox {
                break;
            }
        }
    }
    drop(other);
    loop {
        if let Advisory::Disconnected { inbox: i } = within(5, adv.next()).await.unwrap().unwrap() {
            if i == inbox {
                break;
            }
        }
    }
}
