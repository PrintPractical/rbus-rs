//! Input from a hostile or broken peer: every decoder fails cleanly instead
//! of panicking, overflowing the stack or allocating without bound.

use bytes::Bytes;
use proptest::prelude::*;
use rbus_wire::body::{
    AddRowRequest, DeleteRowRequest, ElementNamesResponse, GetRequest, GetResponse, NamesRequest,
    RowNamesResponse, RpcRequest, RpcResponse, SetRequest, SubscribeRequest, SubscribeResponse,
};
use rbus_wire::event::{Event, Filter};
use rbus_wire::frame::{Flags, Frame, FrameDecoder, Header, MAX_TOPIC_LEN};
use rbus_wire::message::{Body, split_event_payload, split_method_payload};
use rbus_wire::msgpack::{Reader, Writer};
use rbus_wire::value::{Object, Value, decode_properties};
use rbus_wire::{Error, Limits};

const OBJECT: i32 = 0x511;
const PROPERTIES: i32 = 0x510;
const EXPR_LOGIC: i32 = 1;
const LOGIC_NOT: i32 = 2;

/// An object value nested `levels` deep, each holding the next as its only
/// child.
fn nested_objects(levels: usize) -> Vec<u8> {
    let mut w = Writer::new();
    w.i32(OBJECT);
    for _ in 0..levels {
        w.str("o");
        w.i32(0); // single instance
        w.i32(0); // no properties
        w.i32(1); // one child
    }
    w.into_bytes()
}

/// A property-list value whose only property holds the next list.
fn nested_properties(levels: usize) -> Vec<u8> {
    let mut w = Writer::new();
    w.i32(PROPERTIES);
    for _ in 0..levels {
        w.i32(1);
        w.str("p");
        w.i32(PROPERTIES);
    }
    w.into_bytes()
}

fn assert_too_deep(r: Result<impl std::fmt::Debug, Error>) {
    match r {
        Err(Error::LimitExceeded {
            what: "nesting depth",
            ..
        }) => {}
        other => panic!("expected a depth error, got {other:?}"),
    }
}

#[test]
fn deep_nesting_is_refused_without_overflowing_the_stack() {
    // A million levels would overflow any stack if decoded recursively.
    let deep = nested_objects(1_000_000);
    assert_too_deep(Value::decode(&mut Reader::new(&deep)));
    let deep = nested_properties(1_000_000);
    assert_too_deep(Value::decode(&mut Reader::new(&deep)));

    let mut w = Writer::new();
    for _ in 0..1_000_000 {
        w.i32(EXPR_LOGIC);
        w.i32(LOGIC_NOT);
    }
    assert_too_deep(Filter::decode(&mut Reader::new(&w.into_bytes())));
}

#[test]
fn nesting_up_to_the_limit_decodes() {
    let limits = Limits::default().with_max_depth(8);
    // Eight objects, the last with no children.
    let mut w = Writer::new();
    w.i32(OBJECT);
    for level in 0..8 {
        w.str("o");
        w.i32(0);
        w.i32(0);
        w.i32(i32::from(level < 7));
    }
    let bytes = w.into_bytes();
    let v = Value::decode(&mut Reader::new(&bytes).with_limits(&limits)).unwrap();
    assert!(matches!(v, Value::Object(_)));
    let tighter = limits.with_max_depth(7);
    assert_too_deep(Value::decode(
        &mut Reader::new(&bytes).with_limits(&tighter),
    ));
}

#[test]
fn item_counts_are_limited() {
    // Three properties, against a limit of two.
    let mut w = Writer::new();
    w.i32(3);
    for name in ["a", "b", "c"] {
        w.str(name);
        Value::Bool(true).encode(&mut w);
    }
    let bytes = w.into_bytes();
    let limits = Limits::default().with_max_items(2);
    let got = decode_properties(&mut Reader::new(&bytes).with_limits(&limits));
    assert!(
        matches!(
            got,
            Err(Error::LimitExceeded {
                what: "item count",
                max: 2
            })
        ),
        "{got:?}"
    );
    assert_eq!(
        decode_properties(&mut Reader::new(&bytes)).unwrap().len(),
        3
    );

    // The budget is shared by every list in one payload.
    let mut w = Writer::new();
    for _ in 0..2 {
        w.i32(OBJECT);
        w.str("o");
        w.i32(0);
        w.i32(0);
        w.i32(0);
    }
    let bytes = w.into_bytes();
    let mut r = Reader::new(&bytes).with_limits(&Limits::default().with_max_items(0));
    Value::decode(&mut r).unwrap();
    Value::decode(&mut r).unwrap();
}

#[test]
fn a_count_larger_than_the_input_fails_at_once() {
    let mut w = Writer::new();
    w.i32(i32::MAX);
    let bytes = w.into_bytes();
    assert!(matches!(
        decode_properties(&mut Reader::new(&bytes)),
        Err(Error::Truncated(_))
    ));
    // Negative counts mean no items, as in C.
    let mut w = Writer::new();
    w.i32(-5);
    let bytes = w.into_bytes();
    assert!(
        decode_properties(&mut Reader::new(&bytes))
            .unwrap()
            .is_empty()
    );
}

/// A raw header with `topic` and `reply` as given, bytes and all.
fn raw_frame(topic: &[u8], reply: &[u8], payload: &[u8]) -> Vec<u8> {
    let header_len = 32 + topic.len() + reply.len();
    let mut b = Vec::new();
    b.extend_from_slice(&0xAAAAu16.to_be_bytes());
    b.extend_from_slice(&2u16.to_be_bytes());
    b.extend_from_slice(&(header_len as u16).to_be_bytes());
    b.extend_from_slice(&7u32.to_be_bytes()); // seq
    b.extend_from_slice(&Flags::REQUEST.0.to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes()); // control data
    b.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    for s in [topic, reply] {
        b.extend_from_slice(&(s.len() as u32).to_be_bytes());
        b.extend_from_slice(s);
    }
    b.extend_from_slice(&0xAAAAu16.to_be_bytes());
    b.extend_from_slice(payload);
    b
}

#[test]
fn a_topic_that_is_not_utf8_decodes_lossily() {
    // Each bad byte becomes a three-byte replacement character; the header
    // length must still come from the bytes on the wire.
    let mut dec = FrameDecoder::new();
    dec.push(&raw_frame(b"Device.X", &[0xff], b"{}"));
    dec.push(&raw_frame(&[0xff; 20], b"r", b"{}"));
    let f = dec.next_frame().unwrap().unwrap();
    assert_eq!(f.header.reply_topic, "\u{fffd}");
    assert_eq!((f.header.seq, f.header.roundtrip), (7, None));
    let f = dec.next_frame().unwrap().unwrap();
    assert_eq!(f.header.topic.chars().count(), 20);
    assert_eq!(&f.payload[..], b"{}");
}

#[test]
fn an_oversized_topic_is_refused() {
    let topic = vec![b'a'; MAX_TOPIC_LEN];
    let mut dec = FrameDecoder::new();
    dec.push(&raw_frame(&topic, b"r", b"{}"));
    assert!(matches!(
        dec.next_frame(),
        Err(Error::TopicTooLong(MAX_TOPIC_LEN))
    ));
}

#[test]
fn the_payload_limit_is_configurable() {
    let frame = Frame::new(Header::new(1, Flags::NONE, "t", "r"), vec![1u8; 100]);
    let bytes = frame.encode().unwrap();
    let mut dec = FrameDecoder::with_limits(&Limits::default().with_max_payload(99));
    dec.push(&bytes);
    assert!(matches!(
        dec.next_frame(),
        Err(Error::PayloadTooLarge { len: 100, max: 99 })
    ));
    let mut dec = FrameDecoder::with_limits(&Limits::default().with_max_payload(100));
    dec.push(&bytes);
    assert_eq!(dec.next_frame().unwrap().unwrap(), frame);
}

/// Decodes `bytes` with every decoder, ignoring the results: none may panic.
fn decode_everything(bytes: &[u8]) {
    let limits = Limits::default();
    let _ = Value::decode(&mut Reader::new(bytes));
    let _ = Object::decode(&mut Reader::new(bytes));
    let _ = Filter::decode(&mut Reader::new(bytes));
    let _ = Event::decode(&mut Reader::new(bytes));
    let _ = decode_properties(&mut Reader::new(bytes));
    fn body<B: Body>(bytes: &[u8]) {
        let _ = B::decode(&mut Reader::new(bytes));
    }
    body::<GetRequest>(bytes);
    body::<GetResponse>(bytes);
    body::<SetRequest>(bytes);
    body::<NamesRequest>(bytes);
    body::<ElementNamesResponse>(bytes);
    body::<RowNamesResponse>(bytes);
    body::<AddRowRequest>(bytes);
    body::<DeleteRowRequest>(bytes);
    body::<RpcRequest>(bytes);
    body::<RpcResponse>(bytes);
    body::<SubscribeRequest>(bytes);
    let _ = SubscribeResponse::decode_for(&mut Reader::new(bytes), true);
    let _ = SubscribeResponse::decode_for(&mut Reader::new(bytes), false);
    let payload = Bytes::copy_from_slice(bytes);
    let _ = split_method_payload(&payload, &limits);
    let _ = split_event_payload(&payload, &limits);
    let mut dec = FrameDecoder::new();
    dec.push(bytes);
    while let Ok(Some(_)) = dec.next_frame() {}
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        decode_everything(&bytes);
    }

    #[test]
    fn arbitrary_frames_never_panic(
        topic in proptest::collection::vec(any::<u8>(), 0..300),
        reply in proptest::collection::vec(any::<u8>(), 0..40),
        payload in proptest::collection::vec(any::<u8>(), 0..64),
        cut in 0usize..400,
    ) {
        let mut raw = raw_frame(&topic, &reply, &payload);
        // Corrupt one byte of the header, sometimes.
        if cut < raw.len() {
            raw[cut] ^= 0x5a;
        }
        let mut dec = FrameDecoder::new();
        dec.push(&raw);
        while let Ok(Some(_)) = dec.next_frame() {}
    }
}
