//! rbus values and their wire encoding ("value records").
//!
//! A value record is `str name, int32 type tag, body`. The body depends on the
//! type, mirroring `rbusValue_appendToMessage` in the C library:
//!
//! | type | body |
//! |---|---|
//! | 16- and 32-bit integers | msgpack int of the value cast to `int32_t` |
//! | 64-bit integers | msgpack int of the value cast to `int64_t` |
//! | single, double | float64 |
//! | property list | int32 count + value records |
//! | object | name, kind, property list, int32 child count, children |
//! | everything else | `bin` of the C value's raw memory |
//!
//! "Raw memory" means: bool, char, byte, int8 and uint8 are one byte; a string
//! is its bytes plus a NUL; a DateTime is the 48-byte C struct in host byte
//! order. Legacy CCSP peers send type tags 0–11 with the value as a string;
//! those are parsed on input and never produced.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::msgpack::{Reader, Writer};

/// The `rbusValueType_t` type tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum TypeTag {
    /// `RBUS_BOOLEAN`
    Bool = 0x500,
    /// `RBUS_CHAR`
    Char = 0x501,
    /// `RBUS_BYTE`
    Byte = 0x502,
    /// `RBUS_INT8`
    I8 = 0x503,
    /// `RBUS_UINT8`
    U8 = 0x504,
    /// `RBUS_INT16`
    I16 = 0x505,
    /// `RBUS_UINT16`
    U16 = 0x506,
    /// `RBUS_INT32`
    I32 = 0x507,
    /// `RBUS_UINT32`
    U32 = 0x508,
    /// `RBUS_INT64`
    I64 = 0x509,
    /// `RBUS_UINT64`
    U64 = 0x50A,
    /// `RBUS_SINGLE`
    F32 = 0x50B,
    /// `RBUS_DOUBLE`
    F64 = 0x50C,
    /// `RBUS_DATETIME`
    DateTime = 0x50D,
    /// `RBUS_STRING`
    String = 0x50E,
    /// `RBUS_BYTES`
    Bytes = 0x50F,
    /// `RBUS_PROPERTY`
    Properties = 0x510,
    /// `RBUS_OBJECT`
    Object = 0x511,
    /// `RBUS_NONE`
    None = 0x512,
}

impl TypeTag {
    /// The tag for a wire code, if it is a current (non-legacy) tag.
    pub fn from_code(code: i32) -> Option<TypeTag> {
        use TypeTag::*;
        const ALL: [TypeTag; 19] = [
            Bool, Char, Byte, I8, U8, I16, U16, I32, U32, I64, U64, F32, F64, DateTime, String,
            Bytes, Properties, Object, None,
        ];
        ALL.iter().copied().find(|t| *t as i32 == code)
    }
}

/// A value carried on the bus.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `RBUS_BOOLEAN`
    Bool(bool),
    /// `RBUS_CHAR`: a C `char`, carried as its raw byte.
    Char(u8),
    /// `RBUS_BYTE`
    Byte(u8),
    /// `RBUS_INT8`
    I8(i8),
    /// `RBUS_UINT8`
    U8(u8),
    /// `RBUS_INT16`
    I16(i16),
    /// `RBUS_UINT16`
    U16(u16),
    /// `RBUS_INT32`
    I32(i32),
    /// `RBUS_UINT32`
    U32(u32),
    /// `RBUS_INT64`
    I64(i64),
    /// `RBUS_UINT64`
    U64(u64),
    /// `RBUS_SINGLE`
    F32(f32),
    /// `RBUS_DOUBLE`
    F64(f64),
    /// `RBUS_DATETIME`
    DateTime(DateTime),
    /// `RBUS_STRING`
    String(String),
    /// `RBUS_BYTES`. Decoded from a [`Bytes`] payload without copying.
    Bytes(Bytes),
    /// `RBUS_PROPERTY`: a property list.
    Properties(Vec<Property>),
    /// `RBUS_OBJECT`
    Object(Object),
    /// `RBUS_NONE`: no value.
    None,
}

impl Value {
    /// This value's type tag.
    pub fn type_tag(&self) -> TypeTag {
        match self {
            Value::Bool(_) => TypeTag::Bool,
            Value::Char(_) => TypeTag::Char,
            Value::Byte(_) => TypeTag::Byte,
            Value::I8(_) => TypeTag::I8,
            Value::U8(_) => TypeTag::U8,
            Value::I16(_) => TypeTag::I16,
            Value::U16(_) => TypeTag::U16,
            Value::I32(_) => TypeTag::I32,
            Value::U32(_) => TypeTag::U32,
            Value::I64(_) => TypeTag::I64,
            Value::U64(_) => TypeTag::U64,
            Value::F32(_) => TypeTag::F32,
            Value::F64(_) => TypeTag::F64,
            Value::DateTime(_) => TypeTag::DateTime,
            Value::String(_) => TypeTag::String,
            Value::Bytes(_) => TypeTag::Bytes,
            Value::Properties(_) => TypeTag::Properties,
            Value::Object(_) => TypeTag::Object,
            Value::None => TypeTag::None,
        }
    }

    /// Writes the type tag and body (everything after the record's name).
    pub fn encode(&self, w: &mut Writer) {
        w.i32(self.type_tag() as i32);
        match self {
            Value::Bool(v) => w.bin(&[u8::from(*v)]),
            Value::Char(v) | Value::Byte(v) | Value::U8(v) => w.bin(&[*v]),
            Value::I8(v) => w.bin(&[*v as u8]),
            Value::I16(v) => w.i32(i32::from(*v)),
            Value::U16(v) => w.i32(i32::from(*v)),
            Value::I32(v) => w.i32(*v),
            Value::U32(v) => w.i32(*v as i32),
            Value::I64(v) => w.i64(*v),
            Value::U64(v) => w.i64(*v as i64),
            Value::F32(v) => w.f64(f64::from(*v)),
            Value::F64(v) => w.f64(*v),
            Value::DateTime(v) => w.bin(&v.to_bytes()),
            Value::String(v) => {
                let mut bytes = Vec::with_capacity(v.len() + 1);
                bytes.extend_from_slice(v.as_bytes());
                bytes.push(0);
                w.bin(&bytes);
            }
            Value::Bytes(v) => w.bin(v),
            Value::Properties(v) => encode_properties(v, w),
            Value::Object(v) => v.encode(w),
            Value::None => w.bin(&[]),
        }
    }

    /// Reads a type tag and body (`rbusValue_initFromMessage`).
    pub fn decode(r: &mut Reader<'_>) -> Result<Value> {
        let code = r.i32()?;
        if (LEGACY_FIRST..=LEGACY_LAST).contains(&code) {
            return decode_legacy(code, &r.str()?);
        }
        let tag = TypeTag::from_code(code).ok_or(Error::UnknownValueType(code))?;
        Ok(match tag {
            TypeTag::I16 => Value::I16(r.i32()? as i16),
            TypeTag::U16 => Value::U16(r.i32()? as u16),
            TypeTag::I32 => Value::I32(r.i32()?),
            TypeTag::U32 => Value::U32(r.i32()? as u32),
            TypeTag::I64 => Value::I64(r.i64()?),
            TypeTag::U64 => Value::U64(r.i64()? as u64),
            TypeTag::F32 => Value::F32(r.f64()? as f32),
            TypeTag::F64 => Value::F64(r.f64()?),
            TypeTag::Properties => Value::Properties(decode_properties(r)?),
            TypeTag::Object => Value::Object(Object::decode(r)?),
            TypeTag::Bool => Value::Bool(one_byte(r, "bool")? != 0),
            TypeTag::Char => Value::Char(one_byte(r, "char")?),
            TypeTag::Byte => Value::Byte(one_byte(r, "byte")?),
            TypeTag::I8 => Value::I8(one_byte(r, "int8")? as i8),
            TypeTag::U8 => Value::U8(one_byte(r, "uint8")?),
            TypeTag::DateTime => Value::DateTime(DateTime::from_bytes(r.bin()?)?),
            TypeTag::String => {
                let b = r.bin()?;
                let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
                Value::String(String::from_utf8_lossy(&b[..end]).into_owned())
            }
            TypeTag::Bytes => Value::Bytes(r.bin_bytes()?),
            TypeTag::None => {
                r.bin()?;
                Value::None
            }
        })
    }
}

fn one_byte(r: &mut Reader<'_>, ty: &'static str) -> Result<u8> {
    match r.bin()? {
        [b] => Ok(*b),
        other => Err(Error::InvalidValue {
            ty,
            reason: format!("expected 1 byte, got {}", other.len()),
        }),
    }
}

/// A named value.
#[derive(Debug, Clone, PartialEq)]
pub struct Property {
    /// Fully qualified element name (or a key inside an object).
    pub name: String,
    /// The value.
    pub value: Value,
}

impl Property {
    /// A property.
    pub fn new(name: impl Into<String>, value: Value) -> Self {
        Property {
            name: name.into(),
            value,
        }
    }

    /// Writes a value record: name, type tag, body.
    pub fn encode(&self, w: &mut Writer) {
        w.str(&self.name);
        self.value.encode(w);
    }

    /// Reads a value record.
    pub fn decode(r: &mut Reader<'_>) -> Result<Property> {
        let name = r.str()?;
        let value = Value::decode(r)?;
        Ok(Property { name, value })
    }
}

/// Writes an int32 count followed by that many value records.
pub fn encode_properties(props: &[Property], w: &mut Writer) {
    w.i32(props.len() as i32);
    for p in props {
        p.encode(w);
    }
}

/// Reads an int32 count followed by that many value records.
pub fn decode_properties(r: &mut Reader<'_>) -> Result<Vec<Property>> {
    let count = r.i32()?;
    let mut props = Vec::with_capacity(usize::try_from(count).unwrap_or(0).min(1024));
    for _ in 0..count {
        props.push(Property::decode(r)?);
    }
    Ok(props)
}

/// Whether an object is a single instance or a table row set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ObjectKind {
    /// `RBUS_OBJECT_SINGLE_INSTANCE`
    #[default]
    Single,
    /// `RBUS_OBJECT_MULTI_INSTANCE`
    MultiInstance,
}

/// A structured value: named properties plus child objects. Method arguments,
/// method results and event data are objects.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Object {
    /// Object name; often empty.
    pub name: String,
    /// Single or multi-instance.
    pub kind: ObjectKind,
    /// Properties in order. Names are normally unique, but the wire allows repeats.
    pub properties: Vec<Property>,
    /// Child objects in order.
    pub children: Vec<Object>,
}

impl Object {
    /// An empty, unnamed, single-instance object.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a property, builder style.
    pub fn with(mut self, name: impl Into<String>, value: Value) -> Self {
        self.properties.push(Property::new(name, value));
        self
    }

    /// The value of the first property called `name`.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.properties
            .iter()
            .find(|p| p.name == name)
            .map(|p| &p.value)
    }

    /// Writes the object (`rbusObject_appendToMessage`).
    pub fn encode(&self, w: &mut Writer) {
        w.str(&self.name);
        w.i32(match self.kind {
            ObjectKind::Single => 0,
            ObjectKind::MultiInstance => 1,
        });
        encode_properties(&self.properties, w);
        w.i32(self.children.len() as i32);
        for child in &self.children {
            child.encode(w);
        }
    }

    /// Reads an object (`rbusObject_initFromMessage`).
    pub fn decode(r: &mut Reader<'_>) -> Result<Object> {
        let name = r.str()?;
        let kind = match r.i32()? {
            1 => ObjectKind::MultiInstance,
            _ => ObjectKind::Single,
        };
        let properties = decode_properties(r)?;
        let count = r.i32()?;
        let mut children = Vec::new();
        for _ in 0..count {
            children.push(Object::decode(r)?);
        }
        Ok(Object {
            name,
            kind,
            properties,
            children,
        })
    }
}

/// The C `rbusDateTime_t`: a `struct tm` with 32-bit fields plus a timezone
/// offset. Fields keep C's conventions (`year` counts from 1900, `mon` is 0–11).
/// As in C, an all-zero value means "no date" and `year == 0` is special-cased.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DateTime {
    /// Seconds, 0–60.
    pub sec: i32,
    /// Minutes, 0–59.
    pub min: i32,
    /// Hours, 0–23.
    pub hour: i32,
    /// Day of the month, 1–31.
    pub mday: i32,
    /// Month, 0–11.
    pub mon: i32,
    /// Years since 1900.
    pub year: i32,
    /// Day of the week, 0–6 (Sunday = 0).
    pub wday: i32,
    /// Day of the year, 0–365.
    pub yday: i32,
    /// Daylight saving flag. C peers often leave it uninitialised; it is kept
    /// as received so values round-trip unchanged.
    pub isdst: i32,
    /// Timezone offset hours.
    pub tz_hour: i32,
    /// Timezone offset minutes.
    pub tz_min: i32,
    /// True when the offset is west of UTC (negative).
    pub tz_west: bool,
}

/// Size of the C struct on the wire: nine `int32_t`, two `int32_t`, a `bool`
/// and three bytes of padding.
pub const DATETIME_LEN: usize = 48;

impl DateTime {
    /// The struct's memory image in host byte order, as C sends it. rbus is
    /// host-local IPC, so both ends share the byte order.
    pub fn to_bytes(&self) -> [u8; DATETIME_LEN] {
        let mut out = [0u8; DATETIME_LEN];
        let fields = [
            self.sec,
            self.min,
            self.hour,
            self.mday,
            self.mon,
            self.year,
            self.wday,
            self.yday,
            self.isdst,
            self.tz_hour,
            self.tz_min,
        ];
        for (i, f) in fields.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&f.to_ne_bytes());
        }
        out[44] = u8::from(self.tz_west);
        out
    }

    /// Parses the struct's memory image. Padding bytes are ignored.
    pub fn from_bytes(b: &[u8]) -> Result<DateTime> {
        if b.len() < 45 {
            return Err(Error::InvalidValue {
                ty: "datetime",
                reason: format!("expected {DATETIME_LEN} bytes, got {}", b.len()),
            });
        }
        let f = |i: usize| i32::from_ne_bytes(b[i * 4..i * 4 + 4].try_into().expect("4 bytes"));
        Ok(DateTime {
            sec: f(0),
            min: f(1),
            hour: f(2),
            mday: f(3),
            mon: f(4),
            year: f(5),
            wday: f(6),
            yday: f(7),
            isdst: f(8),
            tz_hour: f(9),
            tz_min: f(10),
            tz_west: b[44] != 0,
        })
    }

    /// Parses the ISO-8601 form legacy peers send: `YYYY-MM-DDTHH:MM:SS`
    /// (or with a space instead of `T`), optionally followed by `Z` or
    /// `±HH:MM`. `0000-…` means "no date", as in C.
    pub fn parse_iso8601(s: &str) -> Option<DateTime> {
        if s.starts_with("0000-") {
            return Some(DateTime::default());
        }
        let b = s.as_bytes();
        if b.len() < 19 {
            return None;
        }
        let num = |range: std::ops::Range<usize>| -> Option<i32> {
            let part = s.get(range)?;
            part.bytes()
                .all(|c| c.is_ascii_digit())
                .then(|| part.parse().ok())?
        };
        let seps_ok = b[4] == b'-'
            && b[7] == b'-'
            && (b[10] == b'T' || b[10] == b' ')
            && b[13] == b':'
            && b[16] == b':';
        if !seps_ok {
            return None;
        }
        let year = num(0..4)?;
        let mon = num(5..7)?;
        let mday = num(8..10)?;
        let mut dt = DateTime {
            sec: num(17..19)?,
            min: num(14..16)?,
            hour: num(11..13)?,
            mday,
            mon: mon - 1,
            year: year - 1900,
            wday: weekday(year, mon, mday),
            yday: day_of_year(year, mon, mday),
            ..DateTime::default()
        };
        let rest = &s[19..];
        if rest.len() == 6 && (rest.starts_with('+') || rest.starts_with('-')) {
            dt.tz_west = rest.starts_with('-');
            dt.tz_hour = num(20..22)?;
            dt.tz_min = num(23..25)?;
        }
        Some(dt)
    }
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn day_of_year(year: i32, mon: i32, mday: i32) -> i32 {
    const CUM: [i32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let m = (mon.clamp(1, 12) - 1) as usize;
    CUM[m] + mday - 1 + i32::from(m >= 2 && is_leap(year))
}

/// Sakamoto's algorithm; Sunday = 0, like `struct tm`.
fn weekday(year: i32, mon: i32, mday: i32) -> i32 {
    const T: [i32; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let y = if mon < 3 { year - 1 } else { year };
    let m = (mon.clamp(1, 12) - 1) as usize;
    (y + y / 4 - y / 100 + y / 400 + T[m] + mday).rem_euclid(7)
}

const LEGACY_FIRST: i32 = 0;
const LEGACY_LAST: i32 = 11;

/// Parses a legacy CCSP value (`_parse_rbusData_to_value` in rbus.c).
fn decode_legacy(code: i32, s: &str) -> Result<Value> {
    let bad = |ty: &'static str| Error::InvalidLegacyValue {
        ty,
        value: s.to_owned(),
    };
    Ok(match code {
        0 => Value::String(s.to_owned()),
        1 => Value::I32(s.parse().map_err(|_| bad("int"))?),
        2 => Value::U32(parse_unsigned(s).ok_or_else(|| bad("unsignedInt"))?),
        3 => Value::Bool(match s {
            "1" => true,
            "0" => false,
            _ if s.eq_ignore_ascii_case("true") => true,
            _ if s.eq_ignore_ascii_case("false") => false,
            _ => return Err(bad("boolean")),
        }),
        4 => Value::DateTime(DateTime::parse_iso8601(s).ok_or_else(|| bad("dateTime"))?),
        // C never decodes base64 here; it keeps the string's bytes.
        5 => Value::Bytes(Bytes::copy_from_slice(s.as_bytes())),
        6 => Value::I64(s.parse().map_err(|_| bad("long"))?),
        7 => Value::U64(parse_unsigned(s).ok_or_else(|| bad("unsignedLong"))?),
        8 => Value::F32(s.parse().map_err(|_| bad("float"))?),
        9 => Value::F64(s.parse().map_err(|_| bad("double"))?),
        // C takes the first character (`sscanf("%c")`).
        10 => Value::Byte(*s.as_bytes().first().ok_or_else(|| bad("byte"))?),
        _ => return Err(bad("none")),
    })
}

fn parse_unsigned<T: std::str::FromStr>(s: &str) -> Option<T> {
    if s.starts_with('-') {
        return None;
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(v: &Value) -> Vec<u8> {
        let mut w = Writer::new();
        Property::new("p", v.clone()).encode(&mut w);
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        assert_eq!(
            Property::decode(&mut r).unwrap(),
            Property::new("p", v.clone())
        );
        assert!(r.is_empty());
        bytes
    }

    #[test]
    fn every_type_round_trips() {
        let values = [
            Value::Bool(true),
            Value::Char(b'k'),
            Value::Byte(13),
            Value::I8(-5),
            Value::U8(250),
            Value::I16(-1),
            Value::U16(65535),
            Value::I32(i32::MIN),
            Value::U32(u32::MAX),
            Value::I64(i64::MIN),
            Value::U64(u64::MAX),
            Value::F32(104.719_76),
            Value::F64(std::f64::consts::PI),
            Value::DateTime(DateTime {
                year: 126,
                mon: 9,
                mday: 10,
                tz_west: true,
                ..DateTime::default()
            }),
            Value::String("AllTypes".into()),
            Value::Bytes(Bytes::from_static(&[1, 2, 3])),
            Value::Properties(vec![Property::new("a", Value::I32(1))]),
            Value::Object(Object::new().with("x", Value::None)),
            Value::None,
        ];
        for v in &values {
            round_trip(v);
        }
    }

    #[test]
    fn matches_recorded_c_encodings() {
        // From the get_all_types fixture.
        assert_eq!(
            &round_trip(&Value::Bool(true))[3..],
            &[0xcd, 0x05, 0x00, 0xc4, 1, 1]
        );
        assert_eq!(&round_trip(&Value::I16(-1))[3..], &[0xcd, 0x05, 0x05, 0xff]);
        assert_eq!(
            &round_trip(&Value::U32(u32::MAX))[3..],
            &[0xcd, 0x05, 0x08, 0xff]
        );
        assert_eq!(
            &round_trip(&Value::U64(u64::MAX))[3..],
            &[0xcd, 0x05, 0x0a, 0xff]
        );
        assert_eq!(
            &round_trip(&Value::String("AllTypes".into()))[3..],
            &[
                0xcd, 0x05, 0x0e, 0xc4, 9, b'A', b'l', b'l', b'T', b'y', b'p', b'e', b's', 0
            ]
        );
        assert_eq!(&round_trip(&Value::None)[3..], &[0xcd, 0x05, 0x12, 0xc4, 0]);
    }

    #[test]
    fn datetime_layout() {
        // From the datetime_value fixture: 2026-10-10T12:34:56-05:30 as C stores it.
        let dt = DateTime {
            sec: 56,
            min: 34,
            hour: 12,
            mday: 10,
            mon: 9,
            year: 126,
            wday: 6,
            yday: 282,
            isdst: -753_059_936,
            tz_hour: 5,
            tz_min: 30,
            tz_west: true,
        };
        let b = dt.to_bytes();
        assert_eq!(b.len(), 48);
        assert_eq!(&b[..4], &56i32.to_ne_bytes());
        assert_eq!(b[44], 1);
        assert_eq!(&b[45..], &[0, 0, 0]);
        assert_eq!(DateTime::from_bytes(&b).unwrap(), dt);
        assert!(DateTime::from_bytes(&b[..44]).is_err());
    }

    #[test]
    fn iso8601_parsing_matches_c() {
        let dt = DateTime::parse_iso8601("2026-10-10T12:34:56-05:30").unwrap();
        assert_eq!(
            (dt.year, dt.mon, dt.mday, dt.hour, dt.min, dt.sec),
            (126, 9, 10, 12, 34, 56)
        );
        assert_eq!(
            (dt.wday, dt.yday),
            (6, 282),
            "matches what C's strptime filled in"
        );
        assert_eq!((dt.tz_hour, dt.tz_min, dt.tz_west), (5, 30, true));
        let z = DateTime::parse_iso8601("2024-03-01 00:00:00Z").unwrap();
        assert_eq!((z.tz_hour, z.tz_west, z.yday, z.wday), (0, false, 60, 5));
        let east = DateTime::parse_iso8601("2025-01-01T00:00:00+01:00").unwrap();
        assert!(!east.tz_west);
        assert_eq!(
            DateTime::parse_iso8601("0000-00-00T00:00:00Z"),
            Some(DateTime::default())
        );
        assert_eq!(DateTime::parse_iso8601("2026-10-10"), None);
        assert_eq!(DateTime::parse_iso8601("2026/10/10T12:34:56Z"), None);
        assert_eq!(DateTime::parse_iso8601("20x6-10-10T12:34:56Z"), None);
    }

    fn legacy(code: i32, s: &str) -> Result<Value> {
        let mut w = Writer::new();
        w.i32(code);
        w.str(s);
        let bytes = w.into_bytes();
        Value::decode(&mut Reader::new(&bytes))
    }

    #[test]
    fn legacy_values_are_parsed() {
        assert_eq!(legacy(0, "hi").unwrap(), Value::String("hi".into()));
        assert_eq!(legacy(1, "-42").unwrap(), Value::I32(-42));
        assert_eq!(legacy(2, "4294967295").unwrap(), Value::U32(u32::MAX));
        assert_eq!(legacy(3, "TRUE").unwrap(), Value::Bool(true));
        assert_eq!(legacy(3, "0").unwrap(), Value::Bool(false));
        assert_eq!(legacy(3, "1").unwrap(), Value::Bool(true));
        assert_eq!(legacy(3, "False").unwrap(), Value::Bool(false));
        assert!(matches!(
            legacy(4, "2026-10-10T12:34:56Z").unwrap(),
            Value::DateTime(_)
        ));
        assert_eq!(
            legacy(5, "aGk=").unwrap(),
            Value::Bytes(Bytes::from_static(b"aGk="))
        );
        assert_eq!(legacy(6, "-9").unwrap(), Value::I64(-9));
        assert_eq!(
            legacy(7, "18446744073709551615").unwrap(),
            Value::U64(u64::MAX)
        );
        assert_eq!(legacy(8, "1.5").unwrap(), Value::F32(1.5));
        assert_eq!(legacy(9, "2.25").unwrap(), Value::F64(2.25));
        assert_eq!(legacy(10, "A").unwrap(), Value::Byte(b'A'));
        for (code, s) in [
            (1, "x"),
            (2, "-1"),
            (3, "yes"),
            (4, "bad"),
            (7, "-5"),
            (10, ""),
            (11, ""),
        ] {
            assert!(
                matches!(legacy(code, s), Err(Error::InvalidLegacyValue { .. })),
                "{code} {s:?}"
            );
        }
    }

    #[test]
    fn bad_values_are_errors() {
        let mut w = Writer::new();
        w.i32(0x999);
        let b = w.into_bytes();
        assert!(matches!(
            Value::decode(&mut Reader::new(&b)),
            Err(Error::UnknownValueType(0x999))
        ));
        let mut w = Writer::new();
        w.i32(TypeTag::Bool as i32);
        w.bin(&[1, 2]);
        let b = w.into_bytes();
        assert!(matches!(
            Value::decode(&mut Reader::new(&b)),
            Err(Error::InvalidValue { ty: "bool", .. })
        ));
    }

    #[test]
    fn objects_nest_and_lookup() {
        let child = Object {
            name: "child".into(),
            kind: ObjectKind::MultiInstance,
            ..Object::default()
        };
        let obj = Object {
            name: "parent".into(),
            children: vec![child],
            ..Object::new().with("a", Value::I32(1))
        };
        assert_eq!(obj.get("a"), Some(&Value::I32(1)));
        assert_eq!(obj.get("b"), None);
        let mut w = Writer::new();
        obj.encode(&mut w);
        let b = w.into_bytes();
        assert_eq!(Object::decode(&mut Reader::new(&b)).unwrap(), obj);
    }

    #[test]
    fn type_tags() {
        assert_eq!(TypeTag::from_code(0x50E), Some(TypeTag::String));
        assert_eq!(TypeTag::from_code(0x513), None);
        assert_eq!(Value::Char(1).type_tag(), TypeTag::Char);
    }

    #[test]
    fn weekday_and_yday() {
        assert_eq!(weekday(2000, 1, 1), 6);
        assert_eq!(day_of_year(2000, 12, 31), 365);
        assert_eq!(day_of_year(2001, 12, 31), 364);
        assert!(is_leap(2000) && !is_leap(1900) && is_leap(2024) && !is_leap(2023));
    }
}
