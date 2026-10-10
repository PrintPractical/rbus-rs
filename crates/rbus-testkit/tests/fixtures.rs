//! Checks the recorded wire fixtures (crates/rbus-wire/tests/fixtures) are
//! complete and self-consistent. Re-record with `scripts/interop.sh record`.

use std::path::PathBuf;

use rbus_testkit::frame::read_frame;

const SCENARIOS: &[&str] = &[
    "get_single",
    "get_multi",
    "get_all_types",
    "set_values",
    "undeliverable",
    "tables",
    "methods",
    "events_general",
    "events_value_change",
    "discovery",
    "datetime_value",
    "subscribe_filter_initial",
    "get_names",
    "get_single_rt",
];

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rbus-wire/tests/fixtures")
}

/// Pulls a string or number field out of one fixture line. The lines are
/// written by our own recorder with a fixed shape, so this needs no JSON parser.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    let key = format!("\"{name}\":");
    let start = line
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} in {line}"))
        + key.len();
    let rest = &line[start..];
    if let Some(s) = rest.strip_prefix('"') {
        &s[..s.find('"').unwrap()]
    } else {
        &rest[..rest.find([',', '}']).unwrap()]
    }
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

struct Logged {
    dir: String,
    flags: u32,
    topic: String,
    fixed: usize,
    payload: Vec<u8>,
}

fn load(scenario: &str) -> Vec<Logged> {
    let path = fixture_dir().join(format!("{scenario}.jsonl"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e} (run scripts/interop.sh record)", path.display()));
    text.lines()
        .map(|line| {
            let bytes = unhex(field(line, "frame"));
            let frame = read_frame(&mut bytes.as_slice()).unwrap().unwrap();
            let h = &frame.header;
            assert_eq!(frame.bytes, bytes, "trailing bytes after frame in {line}");
            assert_eq!(field(line, "seq"), h.seq.to_string());
            assert_eq!(field(line, "flags"), h.flags.to_string());
            assert_eq!(field(line, "topic"), h.topic);
            assert_eq!(field(line, "header_len"), h.header_len.to_string());
            assert_eq!(field(line, "payload_len"), h.payload_len.to_string());
            assert_eq!(h.version, 2);
            Logged {
                dir: field(line, "dir").to_owned(),
                flags: h.flags,
                topic: h.topic.clone(),
                fixed: h.fixed_len(),
                payload: frame.payload().to_vec(),
            }
        })
        .collect()
}

#[test]
fn every_scenario_was_recorded_and_parses() {
    for s in SCENARIOS {
        let frames = load(s);
        assert!(!frames.is_empty(), "{s} is empty");
        assert!(
            frames.iter().any(|f| f.dir == "from_broker"),
            "{s} has no broker replies"
        );
    }
}

#[test]
fn header_sizes_match_the_build() {
    for s in SCENARIOS {
        let want = if s.ends_with("_rt") { 72 } else { 32 };
        for f in load(s) {
            assert_eq!(
                f.fixed, want,
                "{s}: {} has a {}-byte header",
                f.topic, f.fixed
            );
        }
    }
}

#[test]
fn undeliverable_reply_has_flags_and_no_payload() {
    let frames = load("undeliverable");
    let reply = frames
        .iter()
        .find(|f| f.dir == "from_broker" && f.flags & 0x04 != 0)
        .expect("no undeliverable reply");
    assert_eq!(reply.flags & 0x06, 0x06, "response + undeliverable");
    assert!(reply.payload.is_empty());
}

#[test]
fn rbus_payloads_end_with_a_meta_trailer() {
    // rbus payloads end with a meta trailer: msgpack int32 0xd2 + big-endian offset of the
    // meta section, bit 31 clear. Check every RawBinary frame that has a payload.
    let mut checked = 0;
    for s in SCENARIOS {
        for f in load(s) {
            if f.flags & 0x10 == 0 || f.payload.len() < 5 {
                continue;
            }
            let t = &f.payload[f.payload.len() - 5..];
            assert_eq!(t[0], 0xd2, "{s}: {} lacks a meta trailer", f.topic);
            let offset = u32::from_be_bytes([t[1], t[2], t[3], t[4]]);
            assert_eq!(offset & 0x8000_0000, 0, "{s}: bit 31 set");
            assert!(
                (offset as usize) < f.payload.len() - 5,
                "{s}: offset past end"
            );
            checked += 1;
        }
    }
    assert!(checked > 20, "only {checked} rbus payloads found");
}
