//! Every frame recorded from real C <-> rtrouted traffic (tests/fixtures,
//! recorded with `scripts/interop.sh record`) must decode, and re-encode to the
//! exact same bytes: header, broker JSON, every rbus body type and events.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use bytes::Bytes;
use rbus_wire::Limits;
use rbus_wire::body::*;
use rbus_wire::control::{
    self, DiscoveryReply, Expression, NameList, RegisteredComponents, ResultReply, Route,
};
use rbus_wire::event::Event;
use rbus_wire::frame::{Flags, FrameDecoder};
use rbus_wire::message::{
    self, Body, EventMeta, Method, request_payload, response_payload, split_event_payload,
    split_method_payload,
};
use rbus_wire::msgpack::{Reader, Writer};
use rbus_wire::value::Value;

fn fixtures() -> Vec<(String, Vec<serde_json::Value>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut out: Vec<_> = std::fs::read_dir(&dir)
        .expect("fixtures directory")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .map(|p| {
            let name = p.file_stem().unwrap().to_string_lossy().into_owned();
            let lines = std::fs::read_to_string(&p)
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            (name, lines)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(out.len() >= 14, "expected the recorded scenarios");
    out
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// What a response's layout depends on, remembered from its request.
#[derive(Clone)]
struct Pending {
    method: Method,
    publish_on_subscribe: bool,
    names_mode: NamesMode,
}

fn check<B: Body + PartialEq + std::fmt::Debug>(r: &mut Reader<'_>, what: &'static str) -> B {
    let body = B::decode(r).unwrap_or_else(|e| panic!("{what}: {e}"));
    expect_end(r, what).unwrap();
    body
}

#[test]
fn every_recorded_frame_round_trips() {
    let mut seen: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut count = |k: &'static str| *seen.entry(k).or_default() += 1;

    for (scenario, lines) in fixtures() {
        let mut pending: HashMap<(String, u32), Pending> = HashMap::new();
        for line in &lines {
            let raw = unhex(line["frame"].as_str().unwrap());
            let ctx = format!("{scenario} #{}", line["n"]);

            // Frame: decode through the streaming decoder, re-encode exactly.
            let mut dec = FrameDecoder::new();
            dec.push(&raw);
            let frame = dec
                .next_frame()
                .unwrap()
                .unwrap_or_else(|| panic!("{ctx}: incomplete"));
            assert_eq!(dec.buffered(), 0, "{ctx}");
            assert_eq!(
                frame.encode_unchecked().unwrap(),
                raw,
                "{ctx}: header re-encode"
            );
            let h = &frame.header;
            let p = &frame.payload;
            count(if h.roundtrip.is_some() {
                "header/72"
            } else {
                "header/32"
            });

            if p.is_empty() {
                assert!(
                    h.flags.contains(Flags::UNDELIVERABLE),
                    "{ctx}: empty payload"
                );
                count("undeliverable");
                continue;
            }

            if !h.flags.contains(Flags::RAW_BINARY) {
                check_control(&ctx, h.topic.as_str(), h.flags, p);
                count("control json");
                continue;
            }

            if h.flags.contains(Flags::REQUEST) {
                let (mut r, meta) = split_method_payload(p, &Limits::DEFAULT).unwrap();
                let mut note = Pending {
                    method: meta.method.clone(),
                    publish_on_subscribe: false,
                    names_mode: NamesMode::Elements,
                };
                let re = match &meta.method {
                    Method::GetParameterValues => {
                        re_request(&meta, &check::<GetRequest>(&mut r, "get"))
                    }
                    Method::SetParameterValues => {
                        re_request(&meta, &check::<SetRequest>(&mut r, "set"))
                    }
                    Method::GetParameterNames => {
                        let req = check::<NamesRequest>(&mut r, "names");
                        note.names_mode = req.mode;
                        re_request(&meta, &req)
                    }
                    Method::AddTableRow => {
                        re_request(&meta, &check::<AddRowRequest>(&mut r, "addrow"))
                    }
                    Method::DeleteTableRow => {
                        re_request(&meta, &check::<DeleteRowRequest>(&mut r, "delrow"))
                    }
                    Method::Rpc => re_request(&meta, &check::<RpcRequest>(&mut r, "rpc")),
                    Method::Subscribe | Method::Unsubscribe => {
                        let req = check::<SubscribeRequest>(&mut r, "subscribe");
                        note.publish_on_subscribe = req.publish_on_subscribe;
                        re_request(&meta, &req)
                    }
                    other => panic!("{ctx}: unexpected request method {other:?}"),
                };
                assert_eq!(
                    &re[..],
                    &p[..],
                    "{ctx}: {:?} request re-encode",
                    meta.method
                );
                count(leak(format!("request {}", meta.method.as_str())));
                pending.insert((h.reply_topic.clone(), h.seq), note);
                continue;
            }

            if h.flags.contains(Flags::RESPONSE) {
                let (mut r, meta) = split_method_payload(p, &Limits::DEFAULT).unwrap();
                assert_eq!(meta.method, Method::Response, "{ctx}");
                let req = pending
                    .get(&(h.topic.clone(), h.seq))
                    .unwrap_or_else(|| panic!("{ctx}: response without a recorded request"))
                    .clone();
                let re = match req.method {
                    Method::GetParameterValues => {
                        response_payload(&check::<GetResponse>(&mut r, "get response"))
                    }
                    Method::SetParameterValues => {
                        response_payload(&check::<SetResponse>(&mut r, "set response"))
                    }
                    Method::GetParameterNames => match req.names_mode {
                        NamesMode::Rows => {
                            response_payload(&check::<RowNamesResponse>(&mut r, "rows response"))
                        }
                        NamesMode::Elements => response_payload(&check::<ElementNamesResponse>(
                            &mut r,
                            "names response",
                        )),
                    },
                    Method::AddTableRow => {
                        response_payload(&check::<AddRowResponse>(&mut r, "addrow response"))
                    }
                    Method::DeleteTableRow => {
                        response_payload(&check::<StatusResponse>(&mut r, "delrow response"))
                    }
                    Method::Rpc => response_payload(&check::<RpcResponse>(&mut r, "rpc response")),
                    Method::Subscribe | Method::Unsubscribe => {
                        let resp = SubscribeResponse::decode_for(&mut r, req.publish_on_subscribe)
                            .unwrap_or_else(|e| panic!("{ctx}: {e}"));
                        expect_end(&r, "subscribe response").unwrap();
                        if req.publish_on_subscribe && resp.status == 0 {
                            assert!(matches!(resp.initial, InitialValue::Value(_)), "{ctx}");
                            count("initial value");
                        }
                        let mut w = Writer::new();
                        resp.encode(&mut w);
                        w.finish_with_meta(|m| message::MethodMeta::response().encode(m))
                    }
                    other => panic!("{ctx}: response to {other:?}"),
                };
                assert_eq!(
                    &re[..],
                    &p[..],
                    "{ctx}: {:?} response re-encode",
                    req.method
                );
                count(leak(format!("response {}", req.method.as_str())));
                continue;
            }

            // Neither request nor response: an event.
            let (mut r, meta) = split_event_payload(p, &Limits::DEFAULT).unwrap();
            let event = Event::decode(&mut r).unwrap_or_else(|e| panic!("{ctx}: {e}"));
            expect_end(&r, "event").unwrap();
            assert!(meta.rbus2, "{ctx}");
            assert_eq!(
                meta.object_name, h.reply_topic,
                "{ctx}: meta names the publisher"
            );
            assert_eq!(re_event(&event, &meta), &p[..], "{ctx}: event re-encode");
            count(leak(format!("event {:?}", event.kind)));
        }
    }

    println!("{seen:#?}");
    for must in [
        "header/32",
        "header/72",
        "undeliverable",
        "control json",
        "request METHOD_GETPARAMETERVALUES",
        "response METHOD_GETPARAMETERVALUES",
        "request METHOD_SETPARAMETERVALUES",
        "response METHOD_SETPARAMETERVALUES",
        "request METHOD_GETPARAMETERNAMES",
        "response METHOD_GETPARAMETERNAMES",
        "request METHOD_ADDTBLROW",
        "response METHOD_ADDTBLROW",
        "request METHOD_DELETETBLROW",
        "response METHOD_DELETETBLROW",
        "request METHOD_RPC",
        "response METHOD_RPC",
        "request METHOD_SUBSCRIBE",
        "response METHOD_SUBSCRIBE",
        "request METHOD_UNSUBSCRIBE",
        "initial value",
        "event General",
        "event ValueChanged",
    ] {
        assert!(
            seen.contains_key(must),
            "no fixture covers {must}: {seen:#?}"
        );
    }
}

fn re_request(meta: &message::MethodMeta, body: &impl Body) -> Vec<u8> {
    request_payload(meta.method.clone(), meta.trace.clone(), body)
}

fn re_event(event: &Event, meta: &EventMeta) -> Vec<u8> {
    let mut w = Writer::new();
    event.encode(&mut w);
    w.finish_with_meta(|m| meta.encode(m))
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn check_control(ctx: &str, topic: &str, flags: Flags, payload: &Bytes) {
    let re = if flags.contains(Flags::RESPONSE) {
        let text = std::str::from_utf8(payload).unwrap();
        if text.matches("\"count\"").count() > 1 {
            // rtrouted's duplicate-count bug: check the items survived instead.
            let reply = control::decode::<DiscoveryReply>(payload).unwrap();
            assert!(!reply.items.is_empty(), "{ctx}");
            assert!(
                reply.items.iter().all(|i| text.contains(i.as_str())),
                "{ctx}"
            );
            return;
        } else if text.contains("\"count\"") {
            control::encode(&control::decode::<DiscoveryReply>(payload).unwrap())
        } else {
            control::encode(&control::decode::<ResultReply>(payload).unwrap())
        }
    } else {
        match topic {
            control::TOPIC_SUBSCRIBE => {
                control::encode(&control::decode::<Route>(payload).unwrap())
            }
            control::TOPIC_QUERY | control::TOPIC_ENUMERATE_ELEMENTS => {
                control::encode(&control::decode::<Expression>(payload).unwrap())
            }
            control::TOPIC_TRACE_ORIGIN => {
                control::encode(&control::decode::<NameList>(payload).unwrap())
            }
            control::TOPIC_REGISTERED_COMPONENTS => {
                control::encode(&control::decode::<RegisteredComponents>(payload).unwrap())
            }
            other => panic!("{ctx}: unexpected control topic {other}"),
        }
    };
    let re = re.unwrap();
    assert_eq!(&re[..], &payload[..], "{ctx}: control re-encode");
}

#[test]
fn recorded_values_decode_to_expected_rust_values() {
    // Spot-check semantics, not just bytes: the all-types provider's values.
    let (_, lines) = fixtures()
        .into_iter()
        .find(|(n, _)| n == "get_all_types")
        .unwrap();
    let resp = lines
        .iter()
        .map(|l| Bytes::from(unhex(l["frame"].as_str().unwrap())))
        .filter_map(|raw| {
            let mut dec = FrameDecoder::new();
            dec.push(&raw);
            let f = dec.next_frame().unwrap().unwrap();
            (f.header.flags.contains(Flags::RESPONSE | Flags::RAW_BINARY) && f.payload.len() > 1000)
                .then_some(f.payload)
        })
        .next()
        .expect("the all-types response");
    let (mut r, _) = split_method_payload(&resp, &Limits::DEFAULT).unwrap();
    let get = GetResponse::decode(&mut r).unwrap();
    let by_name: HashMap<_, _> = get
        .properties
        .iter()
        .map(|p| {
            (
                p.name.rsplit('.').next().unwrap().to_owned(),
                p.value.clone(),
            )
        })
        .collect();
    assert_eq!(by_name["BoolData"], Value::Bool(true));
    assert_eq!(by_name["CharData"], Value::Char(b'k'));
    assert_eq!(by_name["Int16Data"], Value::I16(-1));
    assert_eq!(by_name["UInt16Data"], Value::U16(u16::MAX));
    assert_eq!(by_name["UInt32Data"], Value::U32(u32::MAX));
    assert_eq!(by_name["UInt64Data"], Value::U64(u64::MAX));
    assert_eq!(by_name["DoubleData"], Value::F64(std::f64::consts::PI));
    assert_eq!(by_name["StringData"], Value::String("AllTypes".into()));
    match &by_name["BytesData"] {
        Value::Bytes(b) => {
            assert_eq!(&b[..12], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
            assert!(
                resp.as_ptr() <= b.as_ptr() && b.as_ptr() < resp[resp.len()..].as_ptr(),
                "bytes values share the payload buffer"
            );
        }
        other => panic!("BytesData decoded as {other:?}"),
    }
}

#[test]
fn recorded_interval_events_carry_the_property_by_name() {
    // `rbuscli subinterval Device.Provider1.Param1 1`: the C provider's own
    // interval publisher sends INTERVAL events, not value changes.
    let (_, lines) = fixtures()
        .into_iter()
        .find(|(n, _)| n == "events_interval")
        .unwrap();
    let mut subscribes = Vec::new();
    let mut events = Vec::new();
    for l in &lines {
        let mut dec = FrameDecoder::new();
        dec.push(&unhex(l["frame"].as_str().unwrap()));
        let f = dec.next_frame().unwrap().unwrap();
        // The recorder sees each frame twice: sent to the broker, then
        // passed on by it. Requests are counted as sent.
        if f.header.topic == "Device.Provider1.Param1" && l["dir"] == "to_broker" {
            let (mut r, meta) = split_method_payload(&f.payload, &Limits::DEFAULT).unwrap();
            if matches!(meta.method, Method::Subscribe | Method::Unsubscribe) {
                let req = SubscribeRequest::decode(&mut r).unwrap();
                subscribes.push((meta.method, req.options.unwrap().interval));
            }
        } else if let Ok((mut r, meta)) = split_event_payload(&f.payload, &Limits::DEFAULT) {
            if meta.event_name == "Device.Provider1.Param1" {
                events.push(Event::decode(&mut r).unwrap());
            }
        }
    }
    assert_eq!(
        subscribes,
        [(Method::Subscribe, 1), (Method::Unsubscribe, 1)]
    );
    assert!(events.len() >= 2, "{events:?}");
    for ev in &events {
        assert_eq!(ev.kind, rbus_wire::event::EventKind::Interval, "{ev:?}");
        assert_eq!(ev.interval, 1);
        let data = ev.data.as_ref().unwrap();
        assert_eq!(data.properties.len(), 1, "{data:?}");
        assert_eq!(data.properties[0].name, "Device.Provider1.Param1");
        assert!(
            matches!(data.properties[0].value, Value::I32(_)),
            "{data:?}"
        );
    }
}
