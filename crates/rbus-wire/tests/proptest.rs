//! Property tests: arbitrary values, filters, events and frames survive an
//! encode/decode round trip, and the frame decoder copes with any chunking.

use bytes::Bytes;
use proptest::prelude::*;
use rbus_wire::event::{Event, EventKind, Filter, RelationOp};
use rbus_wire::frame::{Flags, Frame, FrameDecoder, Header};
use rbus_wire::msgpack::{Reader, Writer};
use rbus_wire::value::{DateTime, Object, ObjectKind, Property, Value};

/// C strings end at the first NUL, so values never contain one.
fn c_string() -> impl Strategy<Value = String> {
    "[^\u{0}]{0,40}"
}

fn datetime() -> impl Strategy<Value = DateTime> {
    (any::<[i32; 11]>(), any::<bool>()).prop_map(|(f, tz_west)| DateTime {
        sec: f[0],
        min: f[1],
        hour: f[2],
        mday: f[3],
        mon: f[4],
        year: f[5],
        wday: f[6],
        yday: f[7],
        isdst: f[8],
        tz_hour: f[9],
        tz_min: f[10],
        tz_west,
    })
}

fn scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<bool>().prop_map(Value::Bool),
        any::<u8>().prop_map(Value::Char),
        any::<u8>().prop_map(Value::Byte),
        any::<i8>().prop_map(Value::I8),
        any::<u8>().prop_map(Value::U8),
        any::<i16>().prop_map(Value::I16),
        any::<u16>().prop_map(Value::U16),
        any::<i32>().prop_map(Value::I32),
        any::<u32>().prop_map(Value::U32),
        any::<i64>().prop_map(Value::I64),
        any::<u64>().prop_map(Value::U64),
        // NaN != NaN, so stick to values that compare equal to themselves.
        (-1e30f32..1e30).prop_map(Value::F32),
        (-1e300f64..1e300).prop_map(Value::F64),
        datetime().prop_map(Value::DateTime),
        c_string().prop_map(Value::String),
        proptest::collection::vec(any::<u8>(), 0..300).prop_map(|v| Value::Bytes(Bytes::from(v))),
        Just(Value::None),
    ]
}

fn value() -> impl Strategy<Value = Value> {
    scalar().prop_recursive(3, 24, 4, |inner| {
        let prop = (c_string(), inner.clone()).prop_map(|(n, v)| Property::new(n, v));
        let props = proptest::collection::vec(prop, 0..4).boxed();
        prop_oneof![
            props.clone().prop_map(Value::Properties),
            (c_string(), any::<bool>(), props).prop_map(|(name, multi, properties)| {
                Value::Object(Object {
                    name,
                    kind: if multi {
                        ObjectKind::MultiInstance
                    } else {
                        ObjectKind::Single
                    },
                    properties,
                    children: vec![],
                })
            }),
        ]
    })
}

fn object() -> impl Strategy<Value = Object> {
    let leaf = (
        c_string(),
        proptest::collection::vec((c_string(), value()), 0..3),
    )
        .prop_map(|(name, props)| Object {
            name,
            kind: ObjectKind::Single,
            properties: props
                .into_iter()
                .map(|(n, v)| Property::new(n, v))
                .collect(),
            children: vec![],
        });
    leaf.prop_recursive(2, 8, 3, |inner| {
        (inner.clone(), proptest::collection::vec(inner, 0..3)).prop_map(|(mut o, children)| {
            o.children = children;
            o
        })
    })
}

fn filter() -> impl Strategy<Value = Filter> {
    let ops = prop_oneof![
        Just(RelationOp::GreaterThan),
        Just(RelationOp::GreaterThanOrEqual),
        Just(RelationOp::LessThan),
        Just(RelationOp::LessThanOrEqual),
        Just(RelationOp::Equal),
        Just(RelationOp::NotEqual),
    ];
    let leaf = (ops, scalar()).prop_map(|(op, value)| Filter::Relation { op, value });
    leaf.prop_recursive(3, 12, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(l, r)| Filter::And(Box::new(l), Box::new(r))),
            (inner.clone(), inner.clone()).prop_map(|(l, r)| Filter::Or(Box::new(l), Box::new(r))),
            inner.prop_map(|f| Filter::Not(Box::new(f))),
        ]
    })
}

fn topic() -> impl Strategy<Value = String> {
    "[A-Za-z0-9_.{}!()\\[\\]-]{0,255}"
}

proptest! {
    #[test]
    fn values_round_trip(name in c_string(), v in value()) {
        let p = Property::new(name, v);
        let mut w = Writer::new();
        p.encode(&mut w);
        let bytes = Bytes::from(w.into_bytes());
        let mut r = Reader::from_bytes(&bytes);
        prop_assert_eq!(Property::decode(&mut r).unwrap(), p);
        prop_assert!(r.is_empty());
    }

    #[test]
    fn events_round_trip(
        name in c_string(),
        kind in -2i32..9,
        data in proptest::option::of(object()),
        filter in proptest::option::of(filter()),
        interval in any::<u32>(),
        duration in any::<u32>(),
        component_id in any::<i32>(),
    ) {
        let ev = Event { name, kind: EventKind::from_code(kind), data, filter, interval, duration, component_id };
        let mut w = Writer::new();
        ev.encode(&mut w);
        let bytes = w.into_bytes();
        prop_assert_eq!(Event::decode(&mut Reader::new(&bytes)).unwrap(), ev);
    }

    #[test]
    fn integers_round_trip(v in any::<i64>()) {
        let mut w = Writer::new();
        w.int(v);
        let bytes = w.into_bytes();
        prop_assert_eq!(Reader::new(&bytes).int().unwrap(), v);
    }

    #[test]
    fn frames_survive_any_chunking(
        frames in proptest::collection::vec(
            (any::<u32>(), any::<u32>(), any::<u32>(), topic(), topic(),
             proptest::collection::vec(any::<u8>(), 1..2000), any::<bool>()),
            1..5,
        ),
        cuts in proptest::collection::vec(any::<prop::sample::Index>(), 0..10),
    ) {
        let frames: Vec<Frame> = frames
            .into_iter()
            .map(|(seq, flags, cd, t, r, payload, rt)| {
                let mut h = Header::new(seq, Flags(flags), t, r);
                h.control_data = cd;
                if rt {
                    h.roundtrip = Some([seq.into(), 1, 2, 3, u64::MAX]);
                }
                Frame::new(h, payload)
            })
            .collect();
        let stream: Vec<u8> = frames.iter().flat_map(|f| f.encode().unwrap().to_vec()).collect();
        let mut points: Vec<usize> = cuts.iter().map(|i| i.index(stream.len())).collect();
        points.push(stream.len());
        points.sort_unstable();
        let mut dec = FrameDecoder::new();
        let mut got = Vec::new();
        let mut start = 0;
        for end in points {
            dec.push(&stream[start..end]);
            start = end;
            while let Some(f) = dec.next_frame().unwrap() {
                got.push(f);
            }
        }
        prop_assert_eq!(got, frames);
        prop_assert_eq!(dec.buffered(), 0);
    }

    #[test]
    fn decoders_never_panic_on_garbage(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
        let mut dec = FrameDecoder::new();
        dec.push(&bytes);
        let _ = dec.next_frame();
        let b = Bytes::from(bytes);
        let mut r = Reader::from_bytes(&b);
        let _ = Property::decode(&mut r);
        let _ = Reader::from_bytes(&b).split_meta();
        let _ = Event::decode(&mut Reader::from_bytes(&b));
        let _ = Filter::decode(&mut Reader::from_bytes(&b));
    }
}
