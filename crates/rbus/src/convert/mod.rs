//! serde ↔ bus values (V6).
//!
//! Any `Serialize` type maps to an rbus [`Object`]; any `Deserialize` type can
//! be read back from one. This is how method arguments and results, and event
//! payloads, become plain Rust structs.
//!
//! | Rust (serde) | Bus |
//! |---|---|
//! | `bool`, `i8`–`i64`, `u8`–`u64`, `f32`, `f64` | the scalar of the same type |
//! | `String`, `&str`, `char` | `String` |
//! | bytes (`serde_bytes`, [`bytes::Bytes`]) | `Bytes` |
//! | `None`, `()`, unit structs | `None` |
//! | structs and maps | an object whose properties are the fields |
//! | `Vec`, tuples | a multi-instance object with properties `1`, `2`, ... |
//! | unit enum variants | the variant name as a `String` |
//! | other enum variants | an object with one property: `{variant: data}` |
//! | [`Value`], [`Object`] fields | themselves, exactly |
//!
//! Reading is lenient where it is safe: integers convert between widths when
//! they fit, a multi-instance object reads as a sequence or a map, a DateTime
//! reads as an ISO-8601 string, and unknown properties are ignored (unless the
//! type says `deny_unknown_fields`).
//!
//! ```
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, PartialEq, Debug)]
//! struct RebootArgs { delay: u32, reason: String }
//!
//! let args = RebootArgs { delay: 5, reason: "update".into() };
//! let obj = rbus::to_object(&args)?;
//! assert_eq!(obj.get("delay"), Some(&rbus::Value::U32(5)));
//! let back: RebootArgs = rbus::from_object(obj)?;
//! assert_eq!(back, args);
//! # Ok::<(), rbus::TypeError>(())
//! ```

mod de;
mod ser;

use rbus_wire::value::{Object, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{TypeError, kind_of};

/// serde name of `rbus_wire::value::Value`.
const VALUE: &str = "$rbus::Value";
/// serde name of `rbus_wire::value::Object`.
const OBJECT: &str = "$rbus::Object";

fn seq_index(i: usize) -> String {
    (i + 1).to_string()
}

/// Serializes `value` into a bus [`Value`].
pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value, TypeError> {
    value.serialize(ser::ValueSerializer)
}

/// Deserializes a `T` from a bus [`Value`].
pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, TypeError> {
    T::deserialize(de::Node::Natural(value))
}

/// Serializes `value`, which must be a struct, map, sequence or unit, into an
/// [`Object`]. Unit becomes an empty object.
pub fn to_object<T: Serialize + ?Sized>(value: &T) -> Result<Object, TypeError> {
    match to_value(value)? {
        Value::Object(o) => Ok(o),
        Value::None => Ok(Object::new()),
        v => Err(TypeError::Mismatch {
            expected: "a struct, map or sequence",
            found: kind_of(&v),
        }),
    }
}

/// Deserializes a `T` from an [`Object`].
pub fn from_object<T: DeserializeOwned>(object: Object) -> Result<T, TypeError> {
    from_value(Value::Object(object))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use de::iso8601;
    use rbus_wire::value::{DateTime, ObjectKind, Property};
    use serde::Deserialize;
    use std::collections::BTreeMap;

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    enum Mode {
        Off,
        Fixed(u8),
        Pair(i32, i32),
        Range { lo: u16, hi: u16 },
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Inner {
        flag: bool,
        name: String,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Unit;

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Wrapper(u64);

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Pair(i8, String);

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Everything {
        b: bool,
        i8: i8,
        i16: i16,
        i32: i32,
        i64: i64,
        u8: u8,
        u16: u16,
        u32: u32,
        u64: u64,
        f32: f32,
        f64: f64,
        c: char,
        s: String,
        bytes: Bytes,
        none: Option<u32>,
        some: Option<u32>,
        unit: (),
        unit_struct: Unit,
        newtype: Wrapper,
        tuple: (u8, String),
        tuple_struct: Pair,
        list: Vec<Inner>,
        map: BTreeMap<String, i32>,
        int_keys: BTreeMap<u32, bool>,
        inner: Inner,
        modes: Vec<Mode>,
        raw: Value,
        raw_object: Object,
    }

    fn everything() -> Everything {
        Everything {
            b: true,
            i8: -8,
            i16: -16,
            i32: -32,
            i64: -64,
            u8: 8,
            u16: 16,
            u32: 32,
            u64: 64,
            f32: 0.5,
            f64: -0.25,
            c: 'é',
            s: "text".into(),
            bytes: Bytes::from_static(b"\x00\xff"),
            none: None,
            some: Some(1),
            unit: (),
            unit_struct: Unit,
            newtype: Wrapper(9),
            tuple: (1, "one".into()),
            tuple_struct: Pair(-1, "x".into()),
            list: vec![
                Inner {
                    flag: true,
                    name: "a".into(),
                },
                Inner {
                    flag: false,
                    name: "b".into(),
                },
            ],
            map: [("k".to_string(), 1)].into(),
            int_keys: [(7, true)].into(),
            inner: Inner {
                flag: false,
                name: "in".into(),
            },
            modes: vec![
                Mode::Off,
                Mode::Fixed(3),
                Mode::Pair(1, 2),
                Mode::Range { lo: 1, hi: 9 },
            ],
            raw: Value::Char(b'z'),
            raw_object: exotic_object(),
        }
    }

    /// An object with every value type, a name, a kind and children.
    fn exotic_object() -> Object {
        let mut child = Object::new().with("leaf", Value::Byte(1));
        child.name = "child".into();
        child.kind = ObjectKind::MultiInstance;
        let mut o = Object::new()
            .with("bool", true)
            .with("char", Value::Char(b'c'))
            .with("byte", Value::Byte(0xfe))
            .with("i8", -1i8)
            .with("u8", 1u8)
            .with("i16", -2i16)
            .with("u16", 2u16)
            .with("i32", -3i32)
            .with("u32", 3u32)
            .with("i64", -4i64)
            .with("u64", 4u64)
            .with("f32", 1.5f32)
            .with("f64", 2.5f64)
            .with(
                "dt",
                DateTime {
                    year: 124,
                    mday: 2,
                    tz_hour: 1,
                    tz_west: true,
                    ..DateTime::default()
                },
            )
            .with("str", "s")
            .with("bytes", Bytes::from_static(b"b"))
            .with("props", vec![Property::new("p", Value::Properties(vec![]))])
            .with("obj", Object::new())
            .with("none", Value::None);
        o.name = "top".into();
        o.children.push(child);
        o
    }

    #[test]
    fn structs_round_trip() {
        let e = everything();
        let obj = to_object(&e).unwrap();
        // Spot-check the bus shape.
        assert_eq!(obj.get("u16"), Some(&Value::U16(16)));
        assert_eq!(obj.get("c"), Some(&Value::String("é".into())));
        assert_eq!(obj.get("none"), Some(&Value::None));
        assert_eq!(obj.get("raw"), Some(&Value::Char(b'z')));
        assert_eq!(obj.get("raw_object"), Some(&Value::Object(exotic_object())));
        let Some(Value::Object(list)) = obj.get("list") else {
            panic!("list is not an object")
        };
        assert_eq!(list.kind, ObjectKind::MultiInstance);
        assert_eq!(list.properties[1].name, "2");
        let Some(Value::Object(modes)) = obj.get("modes") else {
            panic!()
        };
        assert_eq!(modes.properties[0].value, Value::String("Off".into()));
        assert_eq!(
            modes.properties[1].value,
            Value::Object(Object::new().with("Fixed", 3u8))
        );
        let back: Everything = from_object(obj).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn values_round_trip_exactly() {
        let Value::Object(all) = Value::Object(exotic_object()) else {
            unreachable!()
        };
        for p in all.properties {
            assert_eq!(
                from_value::<Value>(to_value(&p.value).unwrap()).unwrap(),
                p.value
            );
        }
        let o = exotic_object();
        assert_eq!(from_object::<Object>(o.clone()).unwrap(), o);
        // Property lists by themselves.
        let props = vec![Property::new("x", Value::I32(1))];
        assert_eq!(
            from_value::<Vec<Property>>(Value::Properties(props.clone())).unwrap(),
            props
        );
    }

    #[test]
    fn unit_and_scalars() {
        assert_eq!(to_object(&()).unwrap(), Object::new());
        assert!(matches!(
            to_object(&5u8),
            Err(TypeError::Mismatch { found: "u8", .. })
        ));
        from_object::<()>(Object::new().with("ignored", 1)).unwrap();
        from_value::<()>(Value::None).unwrap();
        assert!(from_value::<()>(Value::I32(1)).is_err());
        assert_eq!(to_value(&5i128).unwrap(), Value::I64(5));
        assert_eq!(to_value(&5u128).unwrap(), Value::U64(5));
        assert!(to_value(&i128::MAX).is_err());
        assert!(to_value(&u128::MAX).is_err());
        assert_eq!(from_value::<i128>(Value::I64(-1)).unwrap(), -1);
        assert_eq!(from_value::<u128>(Value::U64(1)).unwrap(), 1);
    }

    #[test]
    fn lenient_reads() {
        // Integer widths convert when the value fits.
        assert_eq!(from_value::<u64>(Value::U8(3)).unwrap(), 3);
        assert!(from_value::<u8>(Value::I32(-1)).is_err());
        // Char and DateTime read as text; byte strings read from strings.
        assert_eq!(from_value::<String>(Value::Char(b'q')).unwrap(), "q");
        assert_eq!(from_value::<char>(Value::Char(b'q')).unwrap(), 'q');
        assert_eq!(from_value::<u8>(Value::Byte(7)).unwrap(), 7);
        let dt = DateTime {
            year: 124,
            mon: 1,
            mday: 29,
            hour: 13,
            min: 45,
            sec: 7,
            ..DateTime::default()
        };
        assert_eq!(
            from_value::<String>(Value::DateTime(dt)).unwrap(),
            "2024-02-29T13:45:07Z"
        );
        let east = DateTime {
            tz_hour: 5,
            tz_min: 30,
            ..dt
        };
        assert_eq!(iso8601(&east), "2024-02-29T13:45:07+05:30");
        assert_eq!(
            iso8601(&DateTime {
                tz_west: true,
                ..east
            }),
            "2024-02-29T13:45:07-05:30"
        );
        let b: serde_bytes_like::Buf = from_value(Value::String("ab".into())).unwrap();
        assert_eq!(b.0, b"ab");
        let v: Vec<u8> = from_value(Value::Bytes(Bytes::from_static(b"\x01\x02"))).unwrap();
        assert_eq!(v, [1, 2]);
        // A property list reads as a map or a sequence.
        let props = Value::Properties(vec![Property::new("a", Value::I32(1))]);
        let m: BTreeMap<String, i32> = from_value(props.clone()).unwrap();
        assert_eq!(m["a"], 1);
        let s: Vec<Property> = from_value(props).unwrap();
        assert_eq!(s, [Property::new("a", Value::I32(1))]);
        // Non-numeric text for an integer key is reported, not dropped.
        assert!(from_object::<BTreeMap<u8, u8>>(Object::new().with("x", 1u8)).is_err());
        // A single-instance object reads as a sequence of its values, and a
        // multi-instance object as a map.
        let s: Vec<i32> = from_object(Object::new().with("x", 1).with("y", 2)).unwrap();
        assert_eq!(s, [1, 2]);
        let mut multi = to_object(&vec![5u8]).unwrap();
        let m: BTreeMap<String, u8> = from_object(multi.clone()).unwrap();
        assert_eq!(m["1"], 5);
        // Children are reachable by name.
        let mut child = Object::new().with("flag", true).with("name", "c");
        child.name = "inner".into();
        multi.children.push(child);
        multi.kind = ObjectKind::Single;
        #[derive(Deserialize)]
        struct WithChild {
            inner: Inner,
        }
        assert!(from_object::<WithChild>(multi).unwrap().inner.flag);
        // Datetime fields read as a struct.
        let d: DateTime = from_value(Value::DateTime(dt)).unwrap();
        assert_eq!(d, dt);
        // Missing options are None.
        #[derive(Deserialize)]
        struct Opt {
            x: Option<u8>,
        }
        assert_eq!(from_object::<Opt>(Object::new()).unwrap().x, None);
    }

    #[test]
    fn enum_errors() {
        assert!(from_value::<Mode>(Value::I32(1)).is_err());
        assert!(from_object::<Mode>(Object::new().with("a", 1).with("b", 2)).is_err());
        let e = from_value::<Mode>(Value::String("Nope".into())).unwrap_err();
        assert!(e.to_string().contains("unknown variant"), "{e}");
        // Exact forms reject malformed data.
        assert!(to_value(&Fake::Char(300)).is_err());
        assert!(to_value(&Fake::DateTime(1)).is_err());
        assert!(to_value(&Fake::Properties(vec![1])).is_err());
        assert!(to_value(&FakeObject { name: 1 }).is_err());
        assert!(
            to_value(&FakeObject2 {
                kind: "Weird".into()
            })
            .is_err()
        );
    }

    #[test]
    fn map_keys_must_be_scalar() {
        let m: BTreeMap<(u8, u8), u8> = [((1, 2), 3)].into();
        assert!(to_value(&m).is_err());
        for k in [
            to_value(&BTreeMap::from([(-1i8, 0)])),
            to_value(&BTreeMap::from([(1u8, 0)])),
            to_value(&BTreeMap::from([(-1i16, 0)])),
            to_value(&BTreeMap::from([(1u16, 0)])),
            to_value(&BTreeMap::from([(-1i32, 0)])),
            to_value(&BTreeMap::from([(-1i64, 0)])),
            to_value(&BTreeMap::from([(1u64, 0)])),
        ] {
            let Value::Object(o) = k.unwrap() else {
                panic!()
            };
            assert!(o.properties[0].name.len() <= 2);
        }
    }

    /// Mimics `Value`'s serde name with wrong payloads.
    #[derive(Serialize)]
    #[serde(rename = "$rbus::Value")]
    enum Fake {
        Char(u16),
        DateTime(u8),
        Properties(Vec<u8>),
    }

    #[derive(Serialize)]
    #[serde(rename = "$rbus::Object")]
    struct FakeObject {
        name: u8,
    }

    #[derive(Serialize)]
    #[serde(rename = "$rbus::Object")]
    struct FakeObject2 {
        kind: String,
    }

    /// A type that deserializes through `deserialize_byte_buf`.
    mod serde_bytes_like {
        pub struct Buf(pub Vec<u8>);
        impl<'de> serde::Deserialize<'de> for Buf {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl serde::de::Visitor<'_> for V {
                    type Value = Buf;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str("bytes")
                    }
                    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Buf, E> {
                        Ok(Buf(v))
                    }
                }
                d.deserialize_byte_buf(V)
            }
        }
    }
}
