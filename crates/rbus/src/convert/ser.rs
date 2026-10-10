//! `Serialize` → [`Value`].

use rbus_wire::value::{DateTime, Object, ObjectKind, Property, Value};
use serde::ser::{self, Serialize};

use crate::error::TypeError;

use super::{OBJECT, VALUE, seq_index};

type Result<T> = std::result::Result<T, TypeError>;

/// Serializes into a [`Value`].
pub(crate) struct ValueSerializer;

fn object_value(properties: Vec<Property>, kind: ObjectKind) -> Value {
    Value::Object(Object {
        kind,
        properties,
        ..Object::default()
    })
}

impl ser::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = TypeError;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = Variant<SeqSerializer>;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = Variant<MapSerializer>;

    fn serialize_bool(self, v: bool) -> Result<Value> {
        Ok(Value::Bool(v))
    }
    fn serialize_i8(self, v: i8) -> Result<Value> {
        Ok(Value::I8(v))
    }
    fn serialize_i16(self, v: i16) -> Result<Value> {
        Ok(Value::I16(v))
    }
    fn serialize_i32(self, v: i32) -> Result<Value> {
        Ok(Value::I32(v))
    }
    fn serialize_i64(self, v: i64) -> Result<Value> {
        Ok(Value::I64(v))
    }
    fn serialize_i128(self, v: i128) -> Result<Value> {
        i64::try_from(v)
            .map(Value::I64)
            .map_err(|_| TypeError::OutOfRange {
                expected: "i64",
                value: v.to_string(),
            })
    }
    fn serialize_u8(self, v: u8) -> Result<Value> {
        Ok(Value::U8(v))
    }
    fn serialize_u16(self, v: u16) -> Result<Value> {
        Ok(Value::U16(v))
    }
    fn serialize_u32(self, v: u32) -> Result<Value> {
        Ok(Value::U32(v))
    }
    fn serialize_u64(self, v: u64) -> Result<Value> {
        Ok(Value::U64(v))
    }
    fn serialize_u128(self, v: u128) -> Result<Value> {
        u64::try_from(v)
            .map(Value::U64)
            .map_err(|_| TypeError::OutOfRange {
                expected: "u64",
                value: v.to_string(),
            })
    }
    fn serialize_f32(self, v: f32) -> Result<Value> {
        Ok(Value::F32(v))
    }
    fn serialize_f64(self, v: f64) -> Result<Value> {
        Ok(Value::F64(v))
    }
    /// A Rust `char` is Unicode, an `RBUS_CHAR` is one byte: chars become strings.
    fn serialize_char(self, v: char) -> Result<Value> {
        Ok(Value::String(v.to_string()))
    }
    fn serialize_str(self, v: &str) -> Result<Value> {
        Ok(Value::String(v.to_owned()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Value> {
        Ok(Value::Bytes(bytes::Bytes::copy_from_slice(v)))
    }
    fn serialize_none(self) -> Result<Value> {
        Ok(Value::None)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Value> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<Value> {
        Ok(Value::None)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Value> {
        Ok(Value::None)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Value> {
        if name == VALUE {
            // `Value::None`.
            return Ok(Value::None);
        }
        Ok(Value::String(variant.to_owned()))
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Value> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value> {
        let inner = value.serialize(ValueSerializer)?;
        if name == VALUE {
            return exact_value(variant, inner);
        }
        Ok(object_value(
            vec![Property::new(variant, inner)],
            ObjectKind::Single,
        ))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<SeqSerializer> {
        Ok(SeqSerializer(Vec::with_capacity(len.unwrap_or(0))))
    }
    fn serialize_tuple(self, len: usize) -> Result<SeqSerializer> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<SeqSerializer> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Variant<SeqSerializer>> {
        Ok(Variant(variant, self.serialize_seq(Some(len))?))
    }
    fn serialize_map(self, len: Option<usize>) -> Result<MapSerializer> {
        Ok(MapSerializer {
            properties: Vec::with_capacity(len.unwrap_or(0)),
            key: None,
        })
    }
    fn serialize_struct(self, name: &'static str, len: usize) -> Result<StructSerializer> {
        Ok(StructSerializer {
            exact: name == OBJECT,
            properties: Vec::with_capacity(len),
        })
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Variant<MapSerializer>> {
        Ok(Variant(variant, self.serialize_map(Some(len))?))
    }
}

/// Rebuilds a `Value` that serialized itself as `$rbus::Value::<variant>(inner)`.
/// Most variants arrive with the right type already; the rest are re-typed.
fn exact_value(variant: &str, inner: Value) -> Result<Value> {
    let bad = || TypeError::Custom(format!("malformed {VALUE}::{variant}"));
    Ok(match (variant, inner) {
        ("Char", Value::U8(c)) => Value::Char(c),
        ("Byte", Value::U8(b)) => Value::Byte(b),
        ("DateTime", Value::Object(o)) => {
            Value::DateTime(datetime_from_fields(&o).ok_or_else(bad)?)
        }
        ("Properties", Value::Object(o)) => Value::Properties(
            o.properties
                .into_iter()
                .map(|p| property_from_struct(p.value).ok_or_else(bad))
                .collect::<Result<_>>()?,
        ),
        ("Char" | "Byte" | "DateTime" | "Properties", _) => return Err(bad()),
        (_, inner) => inner,
    })
}

fn datetime_from_fields(o: &Object) -> Option<DateTime> {
    let int = |name| match o.get(name)? {
        Value::I32(n) => Some(*n),
        _ => None,
    };
    Some(DateTime {
        sec: int("sec")?,
        min: int("min")?,
        hour: int("hour")?,
        mday: int("mday")?,
        mon: int("mon")?,
        year: int("year")?,
        wday: int("wday")?,
        yday: int("yday")?,
        isdst: int("isdst")?,
        tz_hour: int("tz_hour")?,
        tz_min: int("tz_min")?,
        tz_west: match o.get("tz_west")? {
            Value::Bool(b) => *b,
            _ => return None,
        },
    })
}

/// A `Property` that went through the serializer as a struct.
fn property_from_struct(v: Value) -> Option<Property> {
    let Value::Object(o) = v else { return None };
    let mut name = None;
    let mut value = None;
    for p in o.properties {
        match (p.name.as_str(), p.value) {
            ("name", Value::String(s)) => name = Some(s),
            ("value", v) => value = Some(v),
            _ => return None,
        }
    }
    Some(Property::new(name?, value?))
}

/// Sequences become multi-instance objects with properties `1`, `2`, ...
pub(crate) struct SeqSerializer(Vec<Value>);

impl SeqSerializer {
    fn finish(self) -> Value {
        let properties = self
            .0
            .into_iter()
            .enumerate()
            .map(|(i, v)| Property::new(seq_index(i), v))
            .collect();
        object_value(properties, ObjectKind::MultiInstance)
    }
}

impl ser::SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.0.push(value.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value> {
        Ok(self.finish())
    }
}

impl ser::SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<Value> {
        Ok(self.finish())
    }
}

impl ser::SerializeTupleStruct for SeqSerializer {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        ser::SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<Value> {
        Ok(self.finish())
    }
}

/// Maps become objects; keys must be strings, chars or integers.
pub(crate) struct MapSerializer {
    properties: Vec<Property>,
    key: Option<String>,
}

impl ser::SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        self.key = Some(match key.serialize(ValueSerializer)? {
            Value::String(s) => s,
            Value::I8(n) => n.to_string(),
            Value::U8(n) => n.to_string(),
            Value::I16(n) => n.to_string(),
            Value::U16(n) => n.to_string(),
            Value::I32(n) => n.to_string(),
            Value::U32(n) => n.to_string(),
            Value::I64(n) => n.to_string(),
            Value::U64(n) => n.to_string(),
            v => {
                return Err(TypeError::Custom(format!(
                    "map keys must be strings or integers, not {}",
                    crate::error::kind_of(&v)
                )));
            }
        });
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        let key = self
            .key
            .take()
            .expect("serialize_key before serialize_value");
        self.properties
            .push(Property::new(key, value.serialize(ValueSerializer)?));
        Ok(())
    }
    fn end(self) -> Result<Value> {
        Ok(object_value(self.properties, ObjectKind::Single))
    }
}

/// Structs become objects, except `rbus_wire::value::Object` itself, which is
/// rebuilt exactly from its fields.
pub(crate) struct StructSerializer {
    exact: bool,
    properties: Vec<Property>,
}

impl ser::SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.properties
            .push(Property::new(key, value.serialize(ValueSerializer)?));
        Ok(())
    }
    fn end(self) -> Result<Value> {
        if !self.exact {
            return Ok(object_value(self.properties, ObjectKind::Single));
        }
        exact_object(self.properties)
            .map(Value::Object)
            .ok_or_else(|| TypeError::Custom(format!("malformed {OBJECT}")))
    }
}

fn exact_object(fields: Vec<Property>) -> Option<Object> {
    let mut obj = Object::default();
    for f in fields {
        match (f.name.as_str(), f.value) {
            ("name", Value::String(s)) => obj.name = s,
            ("kind", Value::String(k)) => {
                obj.kind = match k.as_str() {
                    "Single" => ObjectKind::Single,
                    "MultiInstance" => ObjectKind::MultiInstance,
                    _ => return None,
                }
            }
            ("properties", Value::Object(list)) => {
                obj.properties = list
                    .properties
                    .into_iter()
                    .map(|p| property_from_struct(p.value))
                    .collect::<Option<_>>()?;
            }
            ("children", Value::Object(list)) => {
                obj.children = list
                    .properties
                    .into_iter()
                    .map(|p| match p.value {
                        Value::Object(o) => Some(o),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
            }
            _ => return None,
        }
    }
    Some(obj)
}

/// Enum variants with data become a one-property object: `{variant: data}`.
pub(crate) struct Variant<S>(&'static str, S);

impl ser::SerializeTupleVariant for Variant<SeqSerializer> {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        ser::SerializeSeq::serialize_element(&mut self.1, value)
    }
    fn end(self) -> Result<Value> {
        Ok(object_value(
            vec![Property::new(self.0, self.1.finish())],
            ObjectKind::Single,
        ))
    }
}

impl ser::SerializeStructVariant for Variant<MapSerializer> {
    type Ok = Value;
    type Error = TypeError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.1
            .properties
            .push(Property::new(key, value.serialize(ValueSerializer)?));
        Ok(())
    }
    fn end(self) -> Result<Value> {
        Ok(object_value(
            vec![Property::new(self.0, ser::SerializeMap::end(self.1)?)],
            ObjectKind::Single,
        ))
    }
}
