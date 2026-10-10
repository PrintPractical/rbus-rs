//! Errors a provider returns; the engine turns them into wire statuses.

use std::fmt;

use rbus_wire::value::Object;

use crate::error::Status;
use crate::name::PropertyName;

/// Why a provider could not serve a request. The engine sends its
/// [`Status`] to the consumer.
///
/// For a method, the message and the detail object also reach the consumer:
/// the detail is the method's output, and the engine adds `error_code` and
/// `error_string` to it, as C providers do. A Rust consumer sees them as
/// [`Error::Method`](crate::Error::Method).
///
/// ```
/// use rbus::provider::Fault;
/// use rbus::{Object, Status, Value};
///
/// let f = Fault::invalid_value("channel must be 1-13");
/// assert_eq!(f.status(), Status::InvalidParameterValue);
///
/// let out = Object::new().with("retry_after", Value::U32(5));
/// let f = Fault::custom(Status::InvalidOperation, "busy").with_detail(out);
/// assert_eq!(f.message(), Some("busy"));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Fault {
    status: Status,
    message: Option<String>,
    /// Boxed to keep `Result<_, Fault>` small.
    detail: Option<Box<Object>>,
}

impl Fault {
    /// A fault with `status` and no message.
    pub fn new(status: Status) -> Self {
        Fault {
            status,
            message: None,
            detail: None,
        }
    }

    /// A fault with `status` and a message.
    pub fn custom(status: Status, message: impl Into<String>) -> Self {
        Fault::new(status).with_message(message)
    }

    /// The element does not exist (`ElementDoesNotExist`).
    pub fn not_found() -> Self {
        Fault::new(Status::ElementDoesNotExist)
    }

    /// The element can't be set (`NotWritable`).
    pub fn not_writable() -> Self {
        Fault::new(Status::NotWritable)
    }

    /// A value was rejected (`InvalidParameterValue`).
    pub fn invalid_value(message: impl Into<String>) -> Self {
        Fault::custom(Status::InvalidParameterValue, message)
    }

    /// A value had the wrong type (`InvalidParameterType`).
    pub fn invalid_type(message: impl Into<String>) -> Self {
        Fault::custom(Status::InvalidParameterType, message)
    }

    /// The request's input was malformed (`InvalidInput`).
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Fault::custom(Status::InvalidInput, message)
    }

    /// The caller may not do this (`AccessNotAllowed`).
    pub fn access_denied() -> Self {
        Fault::new(Status::AccessNotAllowed)
    }

    /// The provider doesn't support this operation (`InvalidOperation`), as
    /// a C provider answers when a handler is missing.
    pub fn not_allowed() -> Self {
        Fault::new(Status::InvalidOperation)
    }

    /// The method isn't implemented (`InvalidMethod`).
    pub fn invalid_method() -> Self {
        Fault::new(Status::InvalidMethod)
    }

    /// Something went wrong inside the provider (`BusError`). The error is
    /// logged through `tracing`, and its text becomes the message.
    pub fn internal(err: impl std::error::Error) -> Self {
        tracing::warn!(error = %err, "provider error");
        Fault::custom(Status::BusError, err.to_string())
    }

    /// Sets the message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Attaches a method's output object, sent along with the failure.
    pub fn with_detail(mut self, detail: Object) -> Self {
        self.detail = Some(Box::new(detail));
        self
    }

    /// The status sent to the consumer.
    pub fn status(&self) -> Status {
        self.status
    }

    /// The message, if any.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// The method output attached with [`Fault::with_detail`].
    pub fn detail(&self) -> Option<&Object> {
        self.detail.as_deref()
    }

    pub(crate) fn into_detail(self) -> (Status, Option<String>, Option<Object>) {
        (self.status, self.message, self.detail.map(|d| *d))
    }
}

impl From<Status> for Fault {
    fn from(status: Status) -> Self {
        Fault::new(status)
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.status)?;
        if let Some(m) = &self.message {
            write!(f, ": {m}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Fault {}

/// Why a [`Transaction`](crate::provider::Transaction) failed, and at which
/// parameter (C9). Without a name, the engine blames the first one.
#[derive(Debug, Clone, PartialEq)]
pub struct SetFault {
    name: Option<PropertyName>,
    fault: Fault,
}

impl SetFault {
    /// A failure at the parameter `name`.
    pub fn at(name: &PropertyName, fault: impl Into<Fault>) -> Self {
        SetFault {
            name: Some(name.clone()),
            fault: fault.into(),
        }
    }

    /// The provider can't be set at all (`NotWritable`).
    pub fn not_writable() -> Self {
        Fault::not_writable().into()
    }

    /// The failing parameter, if the provider named one.
    pub fn name(&self) -> Option<&PropertyName> {
        self.name.as_ref()
    }

    /// The fault.
    pub fn fault(&self) -> &Fault {
        &self.fault
    }
}

impl From<Fault> for SetFault {
    fn from(fault: Fault) -> Self {
        SetFault { name: None, fault }
    }
}

impl From<Status> for SetFault {
    fn from(status: Status) -> Self {
        Fault::new(status).into()
    }
}

impl fmt::Display for SetFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.name {
            Some(n) => write!(f, "{n}: {}", self.fault),
            None => self.fault.fmt(f),
        }
    }
}

impl std::error::Error for SetFault {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_pick_the_c_status() {
        let cases = [
            (Fault::not_found(), Status::ElementDoesNotExist),
            (Fault::not_writable(), Status::NotWritable),
            (Fault::invalid_value("v"), Status::InvalidParameterValue),
            (Fault::invalid_type("t"), Status::InvalidParameterType),
            (Fault::invalid_input("i"), Status::InvalidInput),
            (Fault::access_denied(), Status::AccessNotAllowed),
            (Fault::not_allowed(), Status::InvalidOperation),
            (Fault::invalid_method(), Status::InvalidMethod),
            (Fault::from(Status::Timeout), Status::Timeout),
        ];
        for (f, s) in cases {
            assert_eq!(f.status(), s);
        }
        let e = std::io::Error::other("disk on fire");
        let f = Fault::internal(e);
        assert_eq!(f.status(), Status::BusError);
        assert_eq!(f.to_string(), "BusError (1): disk on fire");
        assert_eq!(Fault::not_found().to_string(), "ElementDoesNotExist (17)");
        assert!(f.detail().is_none());
        let (status, message, detail) = f.with_detail(Object::new()).into_detail();
        assert_eq!(
            (status, message.as_deref(), detail),
            (Status::BusError, Some("disk on fire"), Some(Object::new()))
        );
    }

    #[test]
    fn set_faults_name_the_parameter() {
        let p = PropertyName::from_static("A.B");
        let f = SetFault::at(&p, Fault::invalid_value("no"));
        assert_eq!(f.name(), Some(&p));
        assert_eq!(f.to_string(), "A.B: InvalidParameterValue (30): no");
        let g = SetFault::not_writable();
        assert_eq!((g.name(), g.fault().status()), (None, Status::NotWritable));
        assert_eq!(g.to_string(), "NotWritable (27)");
        assert_eq!(
            SetFault::from(Status::BusError).fault().status(),
            Status::BusError
        );
    }
}
