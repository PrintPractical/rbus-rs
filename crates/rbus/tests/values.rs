//! Value types through real C providers (V1–V6). Run with `scripts/interop.sh`.

mod common;

use bytes::Bytes;
use common::{connect, sample_provider, test_provider};
use rbus::{Error, Object, Property, TypeError, Value};
use serde::Deserialize;

const TP: &str = "Device.TestProvider.Value";
const ALL: &str = "Device.SampleProvider.AllTypes";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v1_get_every_scalar_type_from_a_c_provider() {
    let _p = test_provider();
    let bus = connect("v1-get").await;
    let get = |leaf: &str| format!("{TP}.{leaf}");
    assert!(bus.get::<bool>(get("BOOLEAN")).await.unwrap());
    assert_eq!(
        bus.get::<Value>(get("CHAR")).await.unwrap(),
        Value::Char(b'a')
    );
    assert_eq!(
        bus.get::<Value>(get("BYTE")).await.unwrap(),
        Value::Byte(0x0a)
    );
    assert_eq!(bus.get::<u8>(get("BYTE")).await.unwrap(), 0x0a);
    assert_eq!(bus.get::<i16>(get("INT16")).await.unwrap(), -10_000);
    assert_eq!(bus.get::<u16>(get("UINT16")).await.unwrap(), 10_000);
    assert_eq!(bus.get::<i32>(get("INT32")).await.unwrap(), -10_000_000);
    assert_eq!(bus.get::<u32>(get("UINT32")).await.unwrap(), 10_000_000);
    assert_eq!(
        bus.get::<i64>(get("INT64")).await.unwrap(),
        -12_345_678_900_001
    );
    assert_eq!(
        bus.get::<u64>(get("UINT64")).await.unwrap(),
        98_765_432_100_023
    );
    assert_eq!(bus.get::<f32>(get("SINGLE")).await.unwrap(), -0.000001);
    assert_eq!(
        bus.get::<f64>(get("DOUBLE")).await.unwrap(),
        -0.000000000000001
    );
    assert_eq!(
        bus.get::<String>(get("STRING")).await.unwrap(),
        "short string"
    );
    assert_eq!(
        bus.get::<Bytes>(get("BYTES")).await.unwrap(),
        Bytes::from_static(b"short string")
    );
    // Wider integer types accept narrower values.
    assert_eq!(bus.get::<i64>(get("INT16")).await.unwrap(), -10_000);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v1_large_string_and_bytes_values() {
    let _p = test_provider();
    let bus = connect("v1-big").await;
    // rbusTestProvider's getBigHandler tests `strstr(..) == 0`, so BigString
    // serves the bytes and BigBytes the string.
    let s: String = bus.get("Device.TestProvider.BigBytes").await.unwrap();
    assert_eq!(s.len(), 999);
    assert!(s.starts_with(" !\"#$%"));
    let b: Vec<u8> = bus.get("Device.TestProvider.BigString").await.unwrap();
    assert_eq!(b.len(), 1000);
    assert_eq!(&b[..999], s.as_bytes());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v1_set_and_get_back_every_scalar_type() {
    let _p = sample_provider();
    let bus = connect("v1-set").await;
    let cases: Vec<(&str, Value)> = vec![
        ("BoolData", Value::Bool(false)),
        ("CharData", Value::Char(b'R')),
        ("ByteData", Value::Byte(0x7e)),
        ("Int16Data", Value::I16(-1234)),
        ("UInt16Data", Value::U16(4321)),
        ("Int32Data", Value::I32(-7)),
        ("UInt32Data", Value::U32(7)),
        ("Int64Data", Value::I64(-1 << 40)),
        ("UInt64Data", Value::U64(1 << 40)),
        ("SingleData", Value::F32(1.25)),
        ("DoubleData", Value::F64(-2.5)),
        ("StringData", Value::String("set from rust".into())),
        (
            "BytesData",
            Value::Bytes(Bytes::from_static(b"\x00\x01rust")),
        ),
    ];
    for (leaf, value) in cases {
        let name = format!("{ALL}.{leaf}");
        bus.set(name.as_str(), value.clone()).await.unwrap();
        let back: Value = bus.get(name.as_str()).await.unwrap();
        match (value, back) {
            // The provider's SingleData getter divides by 0.03 on every read.
            (Value::F32(sent), Value::F32(got)) => {
                assert_eq!(got, (f64::from(sent) / 0.03) as f32, "{leaf}")
            }
            // Its BytesData getter returns the whole 8 KiB buffer.
            (Value::Bytes(sent), Value::Bytes(got)) => {
                assert_eq!(got.len(), 8192, "{leaf}");
                assert!(got.starts_with(&sent), "{leaf}");
                assert!(got[sent.len()..].iter().all(|&b| b == 0), "{leaf}");
            }
            (sent, got) => assert_eq!(got, sent, "{leaf}"),
        }
    }
}

#[cfg(feature = "time")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v2_datetime_as_offset_datetime() {
    use time::OffsetDateTime;
    use time::macros::datetime;

    let _t = test_provider();
    let _s = sample_provider();
    let bus = connect("v2-datetime").await;
    // rbusTestProvider means to store localtime(100), but its helper marshals
    // all three test times into the first slot, so the last, 500, wins. The
    // container runs in UTC.
    let t: OffsetDateTime = bus.get(format!("{TP}.DATETIME")).await.unwrap();
    assert_eq!(t, datetime!(1970-01-01 00:08:20 UTC));
    for t in [
        datetime!(2024-02-29 13:45:07 UTC),
        datetime!(1999-12-31 23:59:59 -03:30),
        datetime!(2038-01-19 03:14:08 +05:45),
    ] {
        bus.set(format!("{ALL}.DateTimeData"), t).await.unwrap();
        let back: OffsetDateTime = bus.get(format!("{ALL}.DateTimeData")).await.unwrap();
        assert_eq!(back, t);
    }
}

#[derive(Deserialize, Debug, PartialEq)]
struct Level1 {
    property_level_1_string: String,
    property_level_1_object: Level2,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Level2 {
    property_level_2_string: String,
    property_level_2_object: Level3,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Level3 {
    property_level_3_string: String,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v3_get_a_nested_object_and_deserialize_it() {
    let _p = test_provider();
    let bus = connect("v3-object").await;
    // testValueHelper.c gives "object 1" the properties it built for
    // "object 2": three levels of objects nested as property values.
    let obj: Object = bus.get(format!("{TP}.OBJECT")).await.unwrap();
    assert_eq!(obj.name, "object 1");
    let Some(Value::Object(level2)) = obj.get("property_level_1_object") else {
        panic!("no nested object: {obj:?}")
    };
    assert_eq!(level2.name, "depth 2");
    let typed: Level1 = rbus::from_object(obj).unwrap();
    assert_eq!(
        typed,
        Level1 {
            property_level_1_string: "level 1".into(),
            property_level_1_object: Level2 {
                property_level_2_string: "level 2".into(),
                property_level_2_object: Level3 {
                    property_level_3_string: "level 3".into()
                },
            },
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v4_get_a_property_list() {
    let _p = test_provider();
    let bus = connect("v4-properties").await;
    let props: Vec<Property> = bus.get(format!("{TP}.PROPERTY")).await.unwrap();
    assert_eq!(
        props.first(),
        Some(&Property::new(
            "property 1",
            Value::String("property 1 value 1".into())
        ))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v5_conversions_check_type_and_range() {
    let _p = test_provider();
    let bus = connect("v5-types").await;
    assert!(matches!(
        bus.get::<String>(format!("{TP}.INT32")).await,
        Err(Error::Type(TypeError::Mismatch {
            expected: "string",
            found: "i32"
        }))
    ));
    assert!(matches!(
        bus.get::<u8>(format!("{TP}.INT16")).await,
        Err(Error::Type(TypeError::OutOfRange { .. }))
    ));
    assert_eq!(
        bus.get::<Option<u32>>(format!("{TP}.UINT32"))
            .await
            .unwrap(),
        Some(10_000_000)
    );
}
