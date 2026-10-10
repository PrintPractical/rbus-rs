//! [`RbusType`]: Rust types that map to a single bus value.

use bytes::Bytes;
use rbus_wire::value::{DateTime, Object, Property, Value};

use crate::error::TypeError;

/// A Rust type that converts to and from one bus [`Value`].
///
/// This is what `bus.get::<T>()` and `bus.set(name, value)` use. Integers
/// convert between widths and signedness when the value fits, so a `u32`
/// property can be read as `u64` or `i64`; anything else must match.
/// Structured data goes through serde instead ([`crate::to_object`]).
///
/// ```
/// use rbus::{RbusType, Value};
///
/// use rbus::IntoValue;
///
/// assert_eq!(7u32.into_value(), Value::U32(7));
/// assert_eq!(u8::from_value(Value::I32(200)).unwrap(), 200);
/// assert!(u8::from_value(Value::I32(300)).is_err());
/// ```
pub trait RbusType: IntoValue + Sized + Send + 'static {
    /// Converts from a bus value.
    fn from_value(v: Value) -> Result<Self, TypeError>;
}

/// Anything that can be sent as a value: every [`RbusType`], plus `&str`.
/// This is the bound of `bus.set()`, and the first half of [`RbusType`].
pub trait IntoValue {
    /// Converts into a bus value.
    fn into_value(self) -> Value;
}

impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::String(self.to_owned())
    }
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl RbusType for Value {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        Ok(v)
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::Bool(self)
    }
}

impl RbusType for bool {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::Bool(b) => Ok(b),
            v => Err(TypeError::mismatch("bool", &v)),
        }
    }
}

/// Widens any integer value to `i128`, which holds every rbus integer.
fn integer(v: &Value) -> Option<i128> {
    Some(match *v {
        Value::Byte(n) | Value::U8(n) => n.into(),
        Value::I8(n) => n.into(),
        Value::I16(n) => n.into(),
        Value::U16(n) => n.into(),
        Value::I32(n) => n.into(),
        Value::U32(n) => n.into(),
        Value::I64(n) => n.into(),
        Value::U64(n) => n.into(),
        _ => return None,
    })
}

macro_rules! integer_type {
    ($($t:ident => $variant:ident),*) => {$(
        impl IntoValue for $t {
            fn into_value(self) -> Value {
                Value::$variant(self)
            }
        }

        impl RbusType for $t {
            fn from_value(v: Value) -> Result<Self, TypeError> {
                let n = integer(&v).ok_or_else(|| TypeError::mismatch(stringify!($t), &v))?;
                $t::try_from(n).map_err(|_| TypeError::OutOfRange {
                    expected: stringify!($t),
                    value: n.to_string(),
                })
            }
        }
    )*};
}

integer_type!(i8 => I8, u8 => U8, i16 => I16, u16 => U16, i32 => I32, u32 => U32, i64 => I64, u64 => U64);

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::F64(self)
    }
}

impl RbusType for f64 {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::F64(f) => Ok(f),
            Value::F32(f) => Ok(f.into()),
            v => Err(TypeError::mismatch("f64", &v)),
        }
    }
}

impl IntoValue for f32 {
    fn into_value(self) -> Value {
        Value::F32(self)
    }
}

impl RbusType for f32 {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::F32(f) => Ok(f),
            // RBUS sends every float as a double; narrowing is expected.
            Value::F64(f) => Ok(f as f32),
            v => Err(TypeError::mismatch("f32", &v)),
        }
    }
}

impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::String(self)
    }
}

impl RbusType for String {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::String(s) => Ok(s),
            v => Err(TypeError::mismatch("string", &v)),
        }
    }
}

impl IntoValue for Bytes {
    fn into_value(self) -> Value {
        Value::Bytes(self)
    }
}

impl RbusType for Bytes {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::Bytes(b) => Ok(b),
            v => Err(TypeError::mismatch("bytes", &v)),
        }
    }
}

/// `RBUS_BYTES`. Use [`Bytes`] to avoid the copy on input.
impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::Bytes(self.into())
    }
}

impl RbusType for Vec<u8> {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        Bytes::from_value(v).map(Vec::from)
    }
}

impl IntoValue for DateTime {
    fn into_value(self) -> Value {
        Value::DateTime(self)
    }
}

impl RbusType for DateTime {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::DateTime(d) => Ok(d),
            v => Err(TypeError::mismatch("datetime", &v)),
        }
    }
}

impl IntoValue for Object {
    fn into_value(self) -> Value {
        Value::Object(self)
    }
}

impl RbusType for Object {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::Object(o) => Ok(o),
            v => Err(TypeError::mismatch("object", &v)),
        }
    }
}

/// `RBUS_PROPERTY`: a property list.
impl IntoValue for Vec<Property> {
    fn into_value(self) -> Value {
        Value::Properties(self)
    }
}

impl RbusType for Vec<Property> {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::Properties(p) => Ok(p),
            v => Err(TypeError::mismatch("property list", &v)),
        }
    }
}

/// `None` is `RBUS_NONE`.
impl<T: RbusType> IntoValue for Option<T> {
    fn into_value(self) -> Value {
        self.map_or(Value::None, T::into_value)
    }
}

impl<T: RbusType> RbusType for Option<T> {
    fn from_value(v: Value) -> Result<Self, TypeError> {
        match v {
            Value::None => Ok(None),
            v => T::from_value(v).map(Some),
        }
    }
}

#[cfg(feature = "time")]
mod time_impl {
    use super::*;
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    fn invalid(what: impl std::fmt::Display) -> TypeError {
        TypeError::Custom(format!("invalid RBUS datetime: {what}"))
    }

    /// `RBUS_DATETIME` (V2). Fractional seconds are dropped. The day of the week
    /// and of the year are recomputed from the date on output, and ignored on input.
    impl IntoValue for OffsetDateTime {
        fn into_value(self) -> Value {
            let offset = self.offset();
            let (h, m, _) = offset.as_hms();
            Value::DateTime(DateTime {
                sec: self.second().into(),
                min: self.minute().into(),
                hour: self.hour().into(),
                mday: self.day().into(),
                mon: i32::from(u8::from(self.month())) - 1,
                year: self.year() - 1900,
                wday: self.weekday().number_days_from_sunday().into(),
                yday: i32::from(self.ordinal()) - 1,
                isdst: 0,
                tz_hour: i32::from(h).abs(),
                tz_min: i32::from(m).abs(),
                tz_west: offset.is_negative(),
            })
        }
    }

    impl RbusType for OffsetDateTime {
        fn from_value(v: Value) -> Result<Self, TypeError> {
            let d = DateTime::from_value(v)?;
            let byte =
                |n: i32, what: &str| u8::try_from(n).map_err(|_| invalid(format!("{what} {n}")));
            let month = Month::try_from(byte(d.mon + 1, "month")?).map_err(invalid)?;
            let date = Date::from_calendar_date(d.year + 1900, month, byte(d.mday, "day")?)
                .map_err(invalid)?;
            let time = Time::from_hms(
                byte(d.hour, "hour")?,
                byte(d.min, "minute")?,
                // A leap second (60) is folded into the next minute's 59.
                byte(d.sec.min(59), "second")?,
            )
            .map_err(invalid)?;
            let sign = if d.tz_west { -1 } else { 1 };
            let offset = UtcOffset::from_hms(
                i8::try_from(sign * d.tz_hour).map_err(|_| invalid("offset"))?,
                i8::try_from(sign * d.tz_min).map_err(|_| invalid("offset"))?,
                0,
            )
            .map_err(invalid)?;
            Ok(PrimitiveDateTime::new(date, time).assume_offset(offset))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use time::macros::datetime;

        #[test]
        fn round_trips_with_offsets() {
            for t in [
                datetime!(2024-02-29 13:45:07 UTC),
                datetime!(1970-01-01 00:01:40 +05:30),
                datetime!(1999-12-31 23:59:59 -03:15),
            ] {
                let v = t.into_value();
                assert_eq!(OffsetDateTime::from_value(v).unwrap(), t);
            }
            let Value::DateTime(d) = datetime!(2024-02-29 13:45:07 -03:15).into_value() else {
                unreachable!()
            };
            assert_eq!((d.year, d.mon, d.mday, d.wday, d.yday), (124, 1, 29, 4, 59));
            assert_eq!((d.tz_hour, d.tz_min, d.tz_west), (3, 15, true));
        }

        #[test]
        fn rejects_unset_and_bad_dates() {
            // C's "no date" is all zeroes: day 0 is not a date.
            assert!(OffsetDateTime::from_value(Value::DateTime(DateTime::default())).is_err());
            let ok = DateTime {
                mday: 1,
                year: 100,
                ..DateTime::default()
            };
            OffsetDateTime::from_value(Value::DateTime(ok)).unwrap();
            for bad in [
                DateTime { mon: 12, ..ok },
                DateTime { hour: 24, ..ok },
                DateTime { mday: -1, ..ok },
                DateTime { tz_hour: 30, ..ok },
                DateTime {
                    tz_hour: 1000,
                    ..ok
                },
                DateTime { tz_min: 1000, ..ok },
            ] {
                assert!(
                    OffsetDateTime::from_value(Value::DateTime(bad)).is_err(),
                    "{bad:?}"
                );
            }
            let leap = DateTime { sec: 60, ..ok };
            assert_eq!(
                OffsetDateTime::from_value(Value::DateTime(leap))
                    .unwrap()
                    .second(),
                59
            );
            assert!(OffsetDateTime::from_value(Value::Bool(true)).is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: RbusType + PartialEq + std::fmt::Debug + Clone>(v: T) {
        assert_eq!(T::from_value(v.clone().into_value()).unwrap(), v);
    }

    #[test]
    fn scalars_round_trip() {
        round_trip(true);
        round_trip(-5i8);
        round_trip(5u8);
        round_trip(-5i16);
        round_trip(5u16);
        round_trip(-5i32);
        round_trip(5u32);
        round_trip(i64::MIN);
        round_trip(u64::MAX);
        round_trip(1.5f32);
        round_trip(-2.25f64);
        round_trip(String::from("hello"));
        round_trip(Bytes::from_static(b"\x00\x01"));
        round_trip(vec![1u8, 2, 3]);
        round_trip(DateTime::default());
        round_trip(Object::new().with("a", 1i32));
        round_trip(vec![Property::new("p", Value::U8(1))]);
        round_trip(Some(3u32));
        round_trip(None::<u32>);
        round_trip(Value::None);
    }

    #[test]
    fn str_is_an_input_value() {
        assert_eq!(IntoValue::into_value("x"), Value::String("x".into()));
    }

    #[test]
    fn integers_convert_when_they_fit() {
        assert_eq!(u64::from_value(Value::U32(7)).unwrap(), 7);
        assert_eq!(i64::from_value(Value::U64(7)).unwrap(), 7);
        assert_eq!(u8::from_value(Value::Byte(0xff)).unwrap(), 0xff);
        assert_eq!(i16::from_value(Value::I8(-3)).unwrap(), -3);
        assert_eq!(u16::from_value(Value::U16(9)).unwrap(), 9);
        assert_eq!(i32::from_value(Value::I64(-9)).unwrap(), -9);
        assert_eq!(
            u32::from_value(Value::I32(-1)).unwrap_err(),
            TypeError::OutOfRange {
                expected: "u32",
                value: "-1".into()
            }
        );
        assert_eq!(
            u32::from_value(Value::String("1".into())).unwrap_err(),
            TypeError::Mismatch {
                expected: "u32",
                found: "string"
            }
        );
    }

    #[test]
    fn floats_and_mismatches() {
        assert_eq!(f64::from_value(Value::F32(0.5)).unwrap(), 0.5);
        assert_eq!(f32::from_value(Value::F64(0.5)).unwrap(), 0.5);
        for v in [
            bool::from_value(Value::I32(1)).map(drop),
            f64::from_value(Value::I32(1)).map(drop),
            f32::from_value(Value::I32(1)).map(drop),
            String::from_value(Value::Char(b'a')).map(drop),
            Bytes::from_value(Value::String("a".into())).map(drop),
            DateTime::from_value(Value::None).map(drop),
            Object::from_value(Value::None).map(drop),
            Vec::<Property>::from_value(Value::None).map(drop),
            Option::<u8>::from_value(Value::Bool(true)).map(drop),
        ] {
            assert!(matches!(v, Err(TypeError::Mismatch { .. })));
        }
    }
}
