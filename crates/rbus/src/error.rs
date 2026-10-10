//! The error type of every bus operation.

use rbus_wire::value::{Object, Value};

pub use rbus_wire::status::Status;

use crate::name::{MethodName, Name, NameError};

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything a bus operation can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The provider or the broker answered with an error status.
    #[error("{name}: {status}")]
    Status {
        /// The element the request was about.
        name: Name,
        /// The status returned.
        status: Status,
    },

    /// A set failed. `name` is the parameter the provider reported as failing
    /// (C9); sets sent to other providers before it may have been applied.
    #[error("set failed at {name}: {status}")]
    SetFailed {
        /// The parameter that failed.
        name: Name,
        /// Why it failed.
        status: Status,
    },

    /// A method returned an error (M3). C providers describe the failure in the
    /// output object's `error_code` and `error_string` properties.
    #[error("method {method} failed: {status}{}", message.as_deref().map(|m| format!(" ({m})")).unwrap_or_default())]
    Method {
        /// The method that failed.
        method: MethodName,
        /// The status returned.
        status: Status,
        /// The provider's `error_string`, if any.
        message: Option<String>,
        /// The whole output object, including `error_code`. Boxed to keep
        /// `Error` small.
        detail: Box<Object>,
    },

    /// No provider owns the name: the broker found no route for it.
    #[error("no provider for {0}")]
    NotFound(Name),

    /// The operation did not complete within its timeout.
    #[error("{op} timed out")]
    Timeout {
        /// The operation, such as `"get"`.
        op: &'static str,
    },

    /// The connection to the broker is closed.
    #[error("disconnected from broker")]
    Disconnected,

    /// The peer replied with something this crate can't interpret.
    #[error("unexpected reply: {0}")]
    Protocol(String),

    /// The socket failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A frame or payload could not be encoded or decoded.
    #[error(transparent)]
    Wire(#[from] rbus_wire::Error),

    /// A value does not have the type the caller asked for.
    #[error(transparent)]
    Type(#[from] TypeError),

    /// An invalid element or component name.
    #[error(transparent)]
    Name(#[from] NameError),
}

impl Error {
    /// The rbus status carried by this error, if any. [`Error::NotFound`]
    /// reports [`Status::DestinationNotFound`] and [`Error::Timeout`] reports
    /// [`Status::Timeout`].
    pub fn status(&self) -> Option<Status> {
        match self {
            Error::Status { status, .. }
            | Error::SetFailed { status, .. }
            | Error::Method { status, .. } => Some(*status),
            Error::NotFound(_) => Some(Status::DestinationNotFound),
            Error::Timeout { .. } => Some(Status::Timeout),
            _ => None,
        }
    }
}

/// A value of the wrong type, or one that does not fit the requested type.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TypeError {
    /// The value has a different type.
    #[error("expected {expected}, found {found}")]
    Mismatch {
        /// The requested type.
        expected: &'static str,
        /// The type of the value received.
        found: &'static str,
    },
    /// The value has a compatible type but does not fit.
    #[error("{value} does not fit in {expected}")]
    OutOfRange {
        /// The requested type.
        expected: &'static str,
        /// The value received.
        value: String,
    },
    /// A serde mapping failed.
    #[error("{0}")]
    Custom(String),
}

impl TypeError {
    pub(crate) fn mismatch(expected: &'static str, found: &Value) -> TypeError {
        TypeError::Mismatch {
            expected,
            found: kind_of(found),
        }
    }
}

impl serde::ser::Error for TypeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        TypeError::Custom(msg.to_string())
    }
}

impl serde::de::Error for TypeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        TypeError::Custom(msg.to_string())
    }
}

/// A short name for a value's type, for error messages.
pub(crate) fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "bool",
        Value::Char(_) => "char",
        Value::Byte(_) => "byte",
        Value::I8(_) => "i8",
        Value::U8(_) => "u8",
        Value::I16(_) => "i16",
        Value::U16(_) => "u16",
        Value::I32(_) => "i32",
        Value::U32(_) => "u32",
        Value::I64(_) => "i64",
        Value::U64(_) => "u64",
        Value::F32(_) => "f32",
        Value::F64(_) => "f64",
        Value::DateTime(_) => "datetime",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::Properties(_) => "property list",
        Value::Object(_) => "object",
        Value::None => "none",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_status() {
        let name = Name::parse("Device.A").unwrap();
        let method = MethodName::parse("Device.M()").unwrap();
        let cases: Vec<(Error, &str, Option<Status>)> = vec![
            (
                Error::Status {
                    name: name.clone(),
                    status: Status::NotReadable,
                },
                "Device.A: NotReadable (28)",
                Some(Status::NotReadable),
            ),
            (
                Error::SetFailed {
                    name: name.clone(),
                    status: Status::NotWritable,
                },
                "set failed at Device.A: NotWritable (27)",
                Some(Status::NotWritable),
            ),
            (
                Error::Method {
                    method: method.clone(),
                    status: Status::InvalidMethod,
                    message: Some("bad".into()),
                    detail: Box::default(),
                },
                "method Device.M() failed: InvalidMethod (22) (bad)",
                Some(Status::InvalidMethod),
            ),
            (
                Error::Method {
                    method,
                    status: Status::BusError,
                    message: None,
                    detail: Box::default(),
                },
                "method Device.M() failed: BusError (1)",
                Some(Status::BusError),
            ),
            (
                Error::NotFound(name),
                "no provider for Device.A",
                Some(Status::DestinationNotFound),
            ),
            (
                Error::Timeout { op: "get" },
                "get timed out",
                Some(Status::Timeout),
            ),
            (Error::Disconnected, "disconnected from broker", None),
            (Error::Protocol("x".into()), "unexpected reply: x", None),
        ];
        for (e, text, status) in cases {
            assert_eq!(e.to_string(), text);
            assert_eq!(e.status(), status);
        }
        let e: Error = TypeError::mismatch("u32", &Value::String("x".into())).into();
        assert_eq!(e.to_string(), "expected u32, found string");
        let e: Error = std::io::Error::other("boom").into();
        assert_eq!(e.to_string(), "boom");
    }

    #[test]
    fn every_kind_has_a_name() {
        use rbus_wire::value::{DateTime, Property};
        let all = [
            Value::Bool(true),
            Value::Char(b'a'),
            Value::Byte(1),
            Value::I8(1),
            Value::U8(1),
            Value::I16(1),
            Value::U16(1),
            Value::I32(1),
            Value::U32(1),
            Value::I64(1),
            Value::U64(1),
            Value::F32(1.0),
            Value::F64(1.0),
            Value::DateTime(DateTime::default()),
            Value::String(String::new()),
            Value::Bytes(Default::default()),
            Value::Properties(Vec::<Property>::new()),
            Value::Object(Object::new()),
            Value::None,
        ];
        let names: std::collections::HashSet<_> = all.iter().map(kind_of).collect();
        assert_eq!(names.len(), all.len());
    }
}
