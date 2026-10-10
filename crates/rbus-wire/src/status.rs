//! Status codes carried in responses (`rbusError_t`), including the legacy CCSP
//! codes some peers still send.

use std::fmt;

/// An rbus status code. Responses carry it as a raw `int32`; [`Status::from_wire`]
/// interprets it, mapping legacy CCSP codes (100 and up) the way the C library
/// does. Only current codes are ever sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Status {
    /// 0: success.
    Success,
    /// 1: general error.
    BusError,
    /// 2: invalid input.
    InvalidInput,
    /// 3: bus not initialised.
    NotInitialized,
    /// 4: out of resources.
    OutOfResources,
    /// 5: no route to the destination element.
    DestinationNotFound,
    /// 6: destination not reachable.
    DestinationNotReachable,
    /// 7: destination failed to respond.
    DestinationResponseFailure,
    /// 8: invalid response from the destination.
    InvalidResponseFromDestination,
    /// 9: invalid operation.
    InvalidOperation,
    /// 10: invalid event.
    InvalidEvent,
    /// 11: invalid handle.
    InvalidHandle,
    /// 12: session already exists (also used for invalid state).
    SessionAlreadyExist,
    /// 13: component name already in use.
    ComponentNameDuplicate,
    /// 14: element name already registered.
    ElementNameDuplicate,
    /// 15: no element name given.
    ElementNameMissing,
    /// 16: component does not exist.
    ComponentDoesNotExist,
    /// 17: element does not exist.
    ElementDoesNotExist,
    /// 18: access not allowed.
    AccessNotAllowed,
    /// 19: invalid context.
    InvalidContext,
    /// 20: timed out.
    Timeout,
    /// 21: the provider will respond asynchronously (internal to C providers).
    AsyncResponse,
    /// 22: invalid method.
    InvalidMethod,
    /// 23: no subscribers.
    NoSubscribers,
    /// 24: subscription already exists.
    SubscriptionAlreadyExist,
    /// 25: invalid namespace.
    InvalidNamespace,
    /// 26: direct connection does not exist.
    DirectConnectionNotExist,
    /// 27: element is not writable.
    NotWritable,
    /// 28: element is not readable.
    NotReadable,
    /// 29: invalid parameter type.
    InvalidParameterType,
    /// 30: invalid parameter value.
    InvalidParameterValue,
    /// A code this crate does not know, kept as received.
    Other(i32),
}

const CURRENT: [Status; 31] = [
    Status::Success,
    Status::BusError,
    Status::InvalidInput,
    Status::NotInitialized,
    Status::OutOfResources,
    Status::DestinationNotFound,
    Status::DestinationNotReachable,
    Status::DestinationResponseFailure,
    Status::InvalidResponseFromDestination,
    Status::InvalidOperation,
    Status::InvalidEvent,
    Status::InvalidHandle,
    Status::SessionAlreadyExist,
    Status::ComponentNameDuplicate,
    Status::ElementNameDuplicate,
    Status::ElementNameMissing,
    Status::ComponentDoesNotExist,
    Status::ElementDoesNotExist,
    Status::AccessNotAllowed,
    Status::InvalidContext,
    Status::Timeout,
    Status::AsyncResponse,
    Status::InvalidMethod,
    Status::NoSubscribers,
    Status::SubscriptionAlreadyExist,
    Status::InvalidNamespace,
    Status::DirectConnectionNotExist,
    Status::NotWritable,
    Status::NotReadable,
    Status::InvalidParameterType,
    Status::InvalidParameterValue,
];

impl Status {
    /// Interprets a wire code. Legacy CCSP codes map as `CCSPError_to_rbusError`
    /// does (any other code above 100 becomes [`Status::BusError`]).
    pub fn from_wire(code: i32) -> Status {
        if let Some(s) = usize::try_from(code).ok().and_then(|i| CURRENT.get(i)) {
            return *s;
        }
        match code {
            100 => Status::Success,
            101 | 190 | 9004 => Status::OutOfResources,
            191 => Status::Timeout,
            192 => Status::DestinationNotFound,
            9005 => Status::InvalidNamespace,
            9006 => Status::InvalidParameterType,
            9007 => Status::InvalidParameterValue,
            9008 => Status::NotWritable,
            c if c > 100 => Status::BusError,
            c => Status::Other(c),
        }
    }

    /// The current (non-legacy) wire code.
    pub fn code(self) -> i32 {
        match self {
            Status::Other(c) => c,
            s => CURRENT.iter().position(|x| *x == s).expect("listed") as i32,
        }
    }

    /// The text the C library gives this status (`rbusError_ToString`,
    /// `rbus.c:6505-6533`). C providers put it in a failed method's
    /// `error_string`; codes C has no text for read "unknown error".
    pub fn c_message(self) -> &'static str {
        match self {
            Status::Success => "ok",
            Status::BusError => "generic error",
            Status::InvalidInput => "invalid input",
            Status::NotInitialized => "not initialized",
            Status::OutOfResources => "out of resources",
            Status::DestinationNotFound => "destination not found",
            Status::DestinationNotReachable => "destination not reachable",
            Status::DestinationResponseFailure => "destination response failure",
            Status::InvalidResponseFromDestination => "invalid response from destination",
            Status::InvalidOperation => "invalid operation",
            Status::InvalidEvent => "invalid event",
            Status::InvalidHandle => "invalid handle",
            Status::SessionAlreadyExist => "session already exists",
            Status::ComponentNameDuplicate => "duplicate component name",
            Status::ElementNameDuplicate => "duplicate element name",
            Status::ElementNameMissing => "name missing",
            Status::ComponentDoesNotExist => "component does not exist",
            Status::ElementDoesNotExist => "element name does not exist",
            Status::AccessNotAllowed => "access denied",
            Status::InvalidContext => "invalid context",
            Status::Timeout => "timeout",
            Status::AsyncResponse => "async operation in progress",
            _ => "unknown error",
        }
    }

    /// True for [`Status::Success`].
    pub fn is_success(self) -> bool {
        self == Status::Success
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Other(c) => write!(f, "unknown status {c}"),
            s => write!(f, "{s:?} ({})", s.code()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_codes_round_trip() {
        for (i, s) in CURRENT.iter().enumerate() {
            assert_eq!(Status::from_wire(i as i32), *s);
            assert_eq!(s.code(), i as i32);
        }
        assert_eq!(Status::from_wire(22), Status::InvalidMethod);
        assert_eq!(Status::from_wire(30), Status::InvalidParameterValue);
    }

    #[test]
    fn legacy_codes_map_like_c() {
        assert_eq!(Status::from_wire(100), Status::Success);
        assert_eq!(Status::from_wire(101), Status::OutOfResources);
        assert_eq!(Status::from_wire(102), Status::BusError);
        assert_eq!(Status::from_wire(190), Status::OutOfResources);
        assert_eq!(Status::from_wire(191), Status::Timeout);
        assert_eq!(Status::from_wire(192), Status::DestinationNotFound);
        assert_eq!(Status::from_wire(193), Status::BusError);
        assert_eq!(Status::from_wire(9004), Status::OutOfResources);
        assert_eq!(Status::from_wire(9005), Status::InvalidNamespace);
        assert_eq!(Status::from_wire(9006), Status::InvalidParameterType);
        assert_eq!(Status::from_wire(9007), Status::InvalidParameterValue);
        assert_eq!(Status::from_wire(9008), Status::NotWritable);
        assert_eq!(Status::from_wire(31), Status::Other(31));
        assert_eq!(Status::from_wire(-1), Status::Other(-1));
        assert_eq!(Status::Other(-1).code(), -1);
    }

    #[test]
    fn display() {
        assert_eq!(Status::NotWritable.to_string(), "NotWritable (27)");
        assert_eq!(Status::Other(42).to_string(), "unknown status 42");
        assert!(Status::Success.is_success() && !Status::BusError.is_success());
    }

    #[test]
    fn c_messages_match_rbus_error_to_string() {
        assert_eq!(Status::Success.c_message(), "ok");
        assert_eq!(Status::InvalidOperation.c_message(), "invalid operation");
        assert_eq!(
            Status::ElementDoesNotExist.c_message(),
            "element name does not exist"
        );
        assert_eq!(
            Status::AsyncResponse.c_message(),
            "async operation in progress"
        );
        let named = CURRENT.iter().filter(|s| s.c_message() != "unknown error");
        assert_eq!(named.count(), 22);
        assert_eq!(Status::NotWritable.c_message(), "unknown error");
        assert_eq!(Status::Other(99).c_message(), "unknown error");
    }
}
