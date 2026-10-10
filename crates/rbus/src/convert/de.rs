//! [`Value`] → `Deserialize`.

use rbus_wire::value::{DateTime, Object, ObjectKind, Property, Value};
use serde::de::{self, DeserializeSeed, IntoDeserializer, Visitor};

use crate::error::{TypeError, kind_of};

use super::{OBJECT, VALUE};

type Result<T> = std::result::Result<T, TypeError>;

/// A deserializer over bus data.
///
/// `Natural` presents a value the way application types expect it: objects as
/// maps (or sequences, for multi-instance objects), scalars as themselves. The
/// other forms present `rbus_wire` types in their exact serde shape, so that
/// `Value` and `Object` fields round-trip without loss.
pub(crate) enum Node {
    Natural(Value),
    ValueEnum(Value),
    Object(Object),
    Property(Property),
    List(Vec<Node>),
}

/// `YYYY-MM-DDTHH:MM:SS` plus `Z` or `±HH:MM`: how DateTime values read as text.
pub(crate) fn iso8601(d: &DateTime) -> String {
    let date = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        d.year + 1900,
        d.mon + 1,
        d.mday,
        d.hour,
        d.min,
        d.sec
    );
    if d.tz_hour == 0 && d.tz_min == 0 {
        date + "Z"
    } else {
        let sign = if d.tz_west { '-' } else { '+' };
        format!("{date}{sign}{:02}:{:02}", d.tz_hour, d.tz_min)
    }
}

fn datetime_fields(d: &DateTime) -> Value {
    let mut o = Object::new();
    for (name, v) in [
        ("sec", d.sec),
        ("min", d.min),
        ("hour", d.hour),
        ("mday", d.mday),
        ("mon", d.mon),
        ("year", d.year),
        ("wday", d.wday),
        ("yday", d.yday),
        ("isdst", d.isdst),
        ("tz_hour", d.tz_hour),
        ("tz_min", d.tz_min),
    ] {
        o = o.with(name, v);
    }
    Value::Object(o.with("tz_west", d.tz_west))
}

/// The values of an object, in order: properties, then children.
fn object_values(o: Object) -> Vec<Node> {
    o.properties
        .into_iter()
        .map(|p| Node::Natural(p.value))
        .chain(
            o.children
                .into_iter()
                .map(|c| Node::Natural(Value::Object(c))),
        )
        .collect()
}

/// The entries of an object, in order: properties, then children by name.
fn object_entries(o: Object) -> Vec<(String, Node)> {
    o.properties
        .into_iter()
        .map(|p| (p.name, Node::Natural(p.value)))
        .chain(
            o.children
                .into_iter()
                .map(|c| (c.name.clone(), Node::Natural(Value::Object(c)))),
        )
        .collect()
}

fn property_entries(list: Vec<Property>) -> Vec<(String, Node)> {
    list.into_iter()
        .map(|p| (p.name, Node::Natural(p.value)))
        .collect()
}

fn variant_name(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "Bool",
        Value::Char(_) => "Char",
        Value::Byte(_) => "Byte",
        Value::I8(_) => "I8",
        Value::U8(_) => "U8",
        Value::I16(_) => "I16",
        Value::U16(_) => "U16",
        Value::I32(_) => "I32",
        Value::U32(_) => "U32",
        Value::I64(_) => "I64",
        Value::U64(_) => "U64",
        Value::F32(_) => "F32",
        Value::F64(_) => "F64",
        Value::DateTime(_) => "DateTime",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::Properties(_) => "Properties",
        Value::Object(_) => "Object",
        Value::None => "None",
    }
}

impl Node {
    fn natural_any<'de, V: Visitor<'de>>(v: Value, visitor: V) -> Result<V::Value> {
        match v {
            Value::Bool(b) => visitor.visit_bool(b),
            Value::Char(c) => visitor.visit_char(c.into()),
            Value::Byte(n) | Value::U8(n) => visitor.visit_u8(n),
            Value::I8(n) => visitor.visit_i8(n),
            Value::I16(n) => visitor.visit_i16(n),
            Value::U16(n) => visitor.visit_u16(n),
            Value::I32(n) => visitor.visit_i32(n),
            Value::U32(n) => visitor.visit_u32(n),
            Value::I64(n) => visitor.visit_i64(n),
            Value::U64(n) => visitor.visit_u64(n),
            Value::F32(f) => visitor.visit_f32(f),
            Value::F64(f) => visitor.visit_f64(f),
            Value::DateTime(d) => visitor.visit_string(iso8601(&d)),
            Value::String(s) => visitor.visit_string(s),
            Value::Bytes(b) => visitor.visit_bytes(&b),
            Value::Properties(p) => visitor.visit_map(Entries::new(property_entries(p))),
            Value::Object(o) if o.kind == ObjectKind::MultiInstance => {
                visitor.visit_seq(Items::new(object_values(o)))
            }
            Value::Object(o) => visitor.visit_map(Entries::new(object_entries(o))),
            Value::None => visitor.visit_unit(),
        }
    }
}

macro_rules! forward_to_any {
    ($($method:ident)*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
            self.deserialize_any(visitor)
        }
    )*};
}

impl<'de> de::Deserializer<'de> for Node {
    type Error = TypeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            Node::Natural(v) => Node::natural_any(v, visitor),
            Node::ValueEnum(v) => visitor.visit_enum(ValueEnum(v)),
            Node::Object(o) => {
                let kind = match o.kind {
                    ObjectKind::Single => "Single",
                    ObjectKind::MultiInstance => "MultiInstance",
                };
                visitor.visit_map(Entries::new(vec![
                    ("name".into(), Node::Natural(Value::String(o.name))),
                    ("kind".into(), Node::Natural(Value::String(kind.into()))),
                    (
                        "properties".into(),
                        Node::List(o.properties.into_iter().map(Node::Property).collect()),
                    ),
                    (
                        "children".into(),
                        Node::List(o.children.into_iter().map(Node::Object).collect()),
                    ),
                ]))
            }
            Node::Property(p) => visitor.visit_map(Entries::new(vec![
                ("name".into(), Node::Natural(Value::String(p.name))),
                ("value".into(), Node::ValueEnum(p.value)),
            ])),
            Node::List(items) => visitor.visit_seq(Items::new(items)),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            Node::Natural(Value::None) => visitor.visit_none(),
            node => visitor.visit_some(node),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            // A method that returns nothing still sends an (empty) object.
            Node::Natural(Value::None | Value::Object(_)) => visitor.visit_unit(),
            node => node.deserialize_any(visitor),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            Node::Natural(Value::Object(o)) => visitor.visit_seq(Items::new(object_values(o))),
            Node::Natural(Value::Properties(p)) => {
                visitor.visit_seq(Items::new(p.into_iter().map(Node::Property).collect()))
            }
            Node::Natural(Value::Bytes(b)) => visitor.visit_seq(Items::new(
                b.iter().map(|&b| Node::Natural(Value::U8(b))).collect(),
            )),
            node => node.deserialize_any(visitor),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            Node::Natural(Value::Object(o)) => visitor.visit_map(Entries::new(object_entries(o))),
            node => node.deserialize_any(visitor),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self {
            Node::Natural(Value::Object(o)) if name == OBJECT => {
                Node::Object(o).deserialize_any(visitor)
            }
            Node::Natural(Value::DateTime(d)) => {
                Node::Natural(datetime_fields(&d)).deserialize_any(visitor)
            }
            node => node.deserialize_map(visitor),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self {
            Node::Natural(v) if name == VALUE => visitor.visit_enum(ValueEnum(v)),
            Node::Natural(Value::String(s)) => visitor.visit_enum(s.into_deserializer()),
            Node::Natural(Value::Object(o)) if o.properties.len() + o.children.len() == 1 => {
                let (variant, data) = object_entries(o).pop().expect("one entry");
                visitor.visit_enum(Tagged(variant, data))
            }
            Node::Natural(v) => Err(TypeError::Custom(format!(
                "expected an enum (a string or a one-property object), found {}",
                kind_of(&v)
            ))),
            node => node.deserialize_any(visitor),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            Node::Natural(Value::Char(c)) => visitor.visit_string(char::from(c).to_string()),
            node => node.deserialize_any(visitor),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self {
            Node::Natural(Value::String(s)) => visitor.visit_byte_buf(s.into_bytes()),
            node => node.deserialize_any(visitor),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }

    forward_to_any! {
        deserialize_bool deserialize_i8 deserialize_i16 deserialize_i32 deserialize_i64
        deserialize_u8 deserialize_u16 deserialize_u32 deserialize_u64 deserialize_f32
        deserialize_f64 deserialize_char deserialize_identifier deserialize_i128 deserialize_u128
    }
}

/// Map access over named nodes.
struct Entries {
    entries: std::vec::IntoIter<(String, Node)>,
    value: Option<Node>,
}

impl Entries {
    fn new(entries: Vec<(String, Node)>) -> Self {
        Entries {
            entries: entries.into_iter(),
            value: None,
        }
    }
}

impl<'de> de::MapAccess<'de> for Entries {
    type Error = TypeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.value = Some(value);
        seed.deserialize(Key(key)).map(Some)
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(&mut self, seed: S) -> Result<S::Value> {
        seed.deserialize(self.value.take().expect("next_key before next_value"))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

/// A map key. Keys are strings on the bus; integer keys are parsed back.
struct Key(String);

macro_rules! parse_key {
    ($($method:ident => $visit:ident),*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
            match self.0.parse() {
                Ok(n) => visitor.$visit(n),
                Err(_) => visitor.visit_string(self.0),
            }
        }
    )*};
}

impl<'de> de::Deserializer<'de> for Key {
    type Error = TypeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(self.0)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    parse_key! {
        deserialize_i8 => visit_i8, deserialize_i16 => visit_i16, deserialize_i32 => visit_i32,
        deserialize_i64 => visit_i64, deserialize_u8 => visit_u8, deserialize_u16 => visit_u16,
        deserialize_u32 => visit_u32, deserialize_u64 => visit_u64
    }

    serde::forward_to_deserialize_any! {
        bool i128 u128 f32 f64 char str string bytes byte_buf option unit unit_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// Sequence access over nodes.
struct Items(std::vec::IntoIter<Node>);

impl Items {
    fn new(items: Vec<Node>) -> Self {
        Items(items.into_iter())
    }
}

impl<'de> de::SeqAccess<'de> for Items {
    type Error = TypeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        self.0.next().map(|n| seed.deserialize(n)).transpose()
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}

/// An `rbus_wire::value::Value` in its exact form: the variant name, then the payload.
struct ValueEnum(Value);

impl<'de> de::EnumAccess<'de> for ValueEnum {
    type Error = TypeError;
    type Variant = Payload;

    fn variant_seed<S: DeserializeSeed<'de>>(self, seed: S) -> Result<(S::Value, Payload)> {
        let tag = seed.deserialize(variant_name(&self.0).into_deserializer())?;
        let payload = match self.0 {
            Value::Char(c) | Value::Byte(c) => Node::Natural(Value::U8(c)),
            Value::DateTime(d) => Node::Natural(datetime_fields(&d)),
            Value::Properties(p) => Node::List(p.into_iter().map(Node::Property).collect()),
            Value::Object(o) => Node::Object(o),
            v => Node::Natural(v),
        };
        Ok((tag, Payload(payload)))
    }
}

/// An application enum read from a one-property object: `{variant: data}`.
struct Tagged(String, Node);

impl<'de> de::EnumAccess<'de> for Tagged {
    type Error = TypeError;
    type Variant = Payload;

    fn variant_seed<S: DeserializeSeed<'de>>(self, seed: S) -> Result<(S::Value, Payload)> {
        let tag = seed.deserialize(self.0.into_deserializer())?;
        Ok((tag, Payload(self.1)))
    }
}

struct Payload(Node);

impl<'de> de::VariantAccess<'de> for Payload {
    type Error = TypeError;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self.0)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value> {
        de::Deserializer::deserialize_seq(self.0, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        de::Deserializer::deserialize_map(self.0, visitor)
    }
}
