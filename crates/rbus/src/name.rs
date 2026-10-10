//! Typed element names.
//!
//! RBUS marks the kind of an element with a suffix: methods end in `()`, events
//! in `!`, and tables (and other partial paths) in `.`. Each kind has its own
//! type here, so a property name can't be passed where a method is expected.
//!
//! Names are checked once, when they are created:
//!
//! - in `const` context with `from_static`, which fails the build on a bad name;
//! - at run time with `parse` or `TryFrom`, which returns a [`NameError`].
//!
//! Every API that takes a name accepts the typed name or any string through
//! [`IntoName`]; strings are validated at the call.
//!
//! ```
//! use rbus::{MethodName, PropertyName};
//!
//! const UPTIME: PropertyName = PropertyName::from_static("Device.DeviceInfo.UpTime");
//! const REBOOT: MethodName = MethodName::from_static("Device.Reboot()");
//!
//! assert!(PropertyName::parse("Device.Reboot()").is_err());
//! assert_eq!(UPTIME.as_str(), "Device.DeviceInfo.UpTime");
//! # let _ = REBOOT;
//! ```

use std::borrow::{Borrow, Cow};
use std::fmt;
use std::ops::Deref;

/// The longest name the broker routes (its topic limit, less the NUL).
pub const MAX_NAME_LEN: usize = 255;

/// A name that is not valid for the kind of element it names.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} name {name:?}: {reason}")]
pub struct NameError {
    /// The rejected name.
    pub name: String,
    /// The kind of name that was expected, such as `"property"`.
    pub kind: &'static str,
    /// What is wrong with it.
    pub reason: &'static str,
}

#[derive(Clone, Copy)]
enum Kind {
    Any,
    Property,
    Method,
    Event,
    Table,
}

impl Kind {
    const fn label(self) -> &'static str {
        match self {
            Kind::Any => "element",
            Kind::Property => "property",
            Kind::Method => "method",
            Kind::Event => "event",
            Kind::Table => "table",
        }
    }
}

const fn ends_with(b: &[u8], suffix: &[u8]) -> bool {
    if b.len() < suffix.len() {
        return false;
    }
    let mut i = 0;
    while i < suffix.len() {
        if b[b.len() - suffix.len() + i] != suffix[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Checks `b` as a name of `kind`; usable in `const` context.
const fn check(b: &[u8], kind: Kind) -> Result<(), &'static str> {
    if b.is_empty() {
        return Err("it is empty");
    }
    if b.len() > MAX_NAME_LEN {
        return Err("it is longer than 255 bytes");
    }
    if b[0] == b'.' {
        return Err("it starts with '.'");
    }
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c <= b' ' || c == 0x7f {
            return Err("it contains whitespace or a control character");
        }
        if c == b'.' && i + 1 < b.len() && b[i + 1] == b'.' {
            return Err("it contains an empty path segment");
        }
        i += 1;
    }
    let method = ends_with(b, b"()");
    let event = ends_with(b, b"!");
    let partial = ends_with(b, b".");
    match kind {
        Kind::Any => Ok(()),
        Kind::Property if method => Err("it ends in \"()\", which marks a method"),
        Kind::Property if event => Err("it ends in '!', which marks an event"),
        Kind::Property if partial => Err("it ends in '.', which marks a partial path"),
        Kind::Property => Ok(()),
        Kind::Method if !method => Err("a method name must end in \"()\""),
        Kind::Method if b.len() == 2 || b[b.len() - 3] == b'.' => {
            Err("it has no method name before \"()\"")
        }
        Kind::Method => Ok(()),
        Kind::Event if !event => Err("an event name must end in '!'"),
        Kind::Event if b.len() == 1 || b[b.len() - 2] == b'.' => {
            Err("it has no event name before '!'")
        }
        Kind::Event => Ok(()),
        Kind::Table if !partial => Err("a table path must end in '.'"),
        Kind::Table => Ok(()),
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Implemented by the name types. Sealed: the set of kinds is fixed.
pub trait ElementName:
    sealed::Sealed + Clone + fmt::Debug + fmt::Display + AsRef<str> + Into<Name>
{
    /// Validates `name` as this kind.
    fn parse(name: impl Into<Cow<'static, str>>) -> Result<Self, NameError>;
    /// The name as text.
    fn as_str(&self) -> &str;
}

/// Anything an API accepts as a name of kind `N`: the typed name itself, or a
/// string that is validated at the call.
pub trait IntoName<N> {
    /// Converts, validating strings.
    fn into_name(self) -> Result<N, NameError>;
}

macro_rules! name_type {
    ($(#[$doc:meta])* $ty:ident, $kind:expr) => {
        $(#[$doc])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $ty(Cow<'static, str>);

        impl $ty {
            /// Validates a name at run time.
            pub fn parse(name: impl Into<Cow<'static, str>>) -> Result<Self, NameError> {
                let name = name.into();
                match check(name.as_bytes(), $kind) {
                    Ok(()) => Ok($ty(name)),
                    Err(reason) => Err(NameError {
                        name: name.into_owned(),
                        kind: $kind.label(),
                        reason,
                    }),
                }
            }

            /// A name from a literal, checked at compile time when used in a
            /// `const`. Panics on an invalid name.
            pub const fn from_static(name: &'static str) -> Self {
                match check(name.as_bytes(), $kind) {
                    Ok(()) => $ty(Cow::Borrowed(name)),
                    Err(reason) => panic!("{}", reason),
                }
            }

            /// The name as text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl sealed::Sealed for $ty {}

        impl ElementName for $ty {
            fn parse(name: impl Into<Cow<'static, str>>) -> Result<Self, NameError> {
                $ty::parse(name)
            }
            fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&*self.0, f)
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl Deref for $ty {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $ty {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<str> for $ty {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl PartialEq<str> for $ty {
            fn eq(&self, other: &str) -> bool {
                *self.0 == *other
            }
        }

        impl PartialEq<&str> for $ty {
            fn eq(&self, other: &&str) -> bool {
                *self.0 == **other
            }
        }

        impl TryFrom<&str> for $ty {
            type Error = NameError;
            fn try_from(name: &str) -> Result<Self, NameError> {
                $ty::parse(name.to_owned())
            }
        }

        impl TryFrom<String> for $ty {
            type Error = NameError;
            fn try_from(name: String) -> Result<Self, NameError> {
                $ty::parse(name)
            }
        }

        impl std::str::FromStr for $ty {
            type Err = NameError;
            fn from_str(name: &str) -> Result<Self, NameError> {
                $ty::parse(name.to_owned())
            }
        }

        impl IntoName<$ty> for $ty {
            fn into_name(self) -> Result<$ty, NameError> {
                Ok(self)
            }
        }

        impl IntoName<$ty> for &$ty {
            fn into_name(self) -> Result<$ty, NameError> {
                Ok(self.clone())
            }
        }

        impl IntoName<$ty> for &str {
            fn into_name(self) -> Result<$ty, NameError> {
                $ty::parse(self.to_owned())
            }
        }

        impl IntoName<$ty> for String {
            fn into_name(self) -> Result<$ty, NameError> {
                $ty::parse(self)
            }
        }

        impl IntoName<$ty> for &String {
            fn into_name(self) -> Result<$ty, NameError> {
                $ty::parse(self.clone())
            }
        }
    };
}

name_type!(
    /// Any element name: a property, method, event, table or partial path,
    /// with instance numbers, `[alias]` and `*` allowed. Errors report names
    /// with this type.
    Name,
    Kind::Any
);

name_type!(
    /// A property (parameter) name, such as `Device.WiFi.Radio.1.Enable`.
    PropertyName,
    Kind::Property
);

name_type!(
    /// A method name, ending in `()`: `Device.Reboot()`.
    MethodName,
    Kind::Method
);

name_type!(
    /// An event name, ending in `!`: `Device.WiFi.Alarm!`.
    EventName,
    Kind::Event
);

name_type!(
    /// A table path (or other partial path), ending in `.`: `Device.Hosts.Host.`.
    TablePath,
    Kind::Table
);

macro_rules! into_any_name {
    ($($ty:ident),*) => {$(
        impl From<$ty> for Name {
            fn from(n: $ty) -> Name {
                Name(n.0)
            }
        }
    )*};
}

into_any_name!(PropertyName, MethodName, EventName, TablePath);

/// Checks a component name: non-empty, at most 255 bytes, no whitespace or
/// control characters.
pub(crate) fn check_component(name: &str) -> Result<(), NameError> {
    let bad = |reason| NameError {
        name: name.to_owned(),
        kind: "component",
        reason,
    };
    if name.is_empty() {
        return Err(bad("it is empty"));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(bad("it is longer than 255 bytes"));
    }
    if name.bytes().any(|c| c <= b' ' || c == 0x7f) {
        return Err(bad("it contains whitespace or a control character"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const P: PropertyName = PropertyName::from_static("Device.WiFi.Radio.1.Enable");
    const M: MethodName = MethodName::from_static("Device.Reboot()");
    const E: EventName = EventName::from_static("Device.WiFi.Alarm!");
    const T: TablePath = TablePath::from_static("Device.Hosts.Host.");

    #[test]
    fn valid_names() {
        assert_eq!(P.as_str(), "Device.WiFi.Radio.1.Enable");
        assert_eq!(&*M, "Device.Reboot()");
        assert_eq!(E.to_string(), "Device.WiFi.Alarm!");
        assert_eq!(format!("{T:?}"), "\"Device.Hosts.Host.\"");
        for n in [
            "Device.IP.Interface.*.Status",
            "Device.Hosts.Host.[guest].Name",
            "X",
        ] {
            PropertyName::parse(n).unwrap();
            Name::parse(n).unwrap();
        }
        MethodName::parse("Device.Table1.1.Method1()").unwrap();
        Name::parse("Device.").unwrap();
        Name::parse("Device.Reboot()").unwrap();
    }

    #[test]
    fn invalid_names() {
        let cases: &[(&str, &str, Result<(), NameError>)] = &[
            ("", "property", PropertyName::parse("").map(drop)),
            (".A", "property", PropertyName::parse(".A").map(drop)),
            ("A..B", "property", PropertyName::parse("A..B").map(drop)),
            ("A B", "property", PropertyName::parse("A B").map(drop)),
            ("A\u{7f}", "element", Name::parse("A\u{7f}").map(drop)),
            ("A()", "property", PropertyName::parse("A()").map(drop)),
            ("A!", "property", PropertyName::parse("A!").map(drop)),
            ("A.", "property", PropertyName::parse("A.").map(drop)),
            ("A", "method", MethodName::parse("A").map(drop)),
            ("()", "method", MethodName::parse("()").map(drop)),
            ("A.()", "method", MethodName::parse("A.()").map(drop)),
            ("A", "event", EventName::parse("A").map(drop)),
            ("!", "event", EventName::parse("!").map(drop)),
            ("A.!", "event", EventName::parse("A.!").map(drop)),
            ("A", "table", TablePath::parse("A").map(drop)),
        ];
        for (name, kind, res) in cases {
            let e = res.clone().unwrap_err();
            assert_eq!((e.name.as_str(), e.kind), (*name, *kind));
            assert!(e.to_string().starts_with(&format!("invalid {kind} name")));
        }
        let long = "A".repeat(256);
        assert_eq!(
            Name::parse(long.clone()).unwrap_err().reason,
            "it is longer than 255 bytes"
        );
        Name::parse("A".repeat(255)).unwrap();
    }

    #[test]
    fn conversions() {
        let p: PropertyName = "Device.A".try_into().unwrap();
        let q: PropertyName = String::from("Device.A").try_into().unwrap();
        let r: PropertyName = "Device.A".parse().unwrap();
        assert_eq!(p, q);
        assert_eq!(q, r);
        assert!(p == "Device.A");
        assert!(p == *"Device.A");
        assert_eq!(p.as_ref() as &str, "Device.A");
        assert_eq!(ElementName::as_str(&p), "Device.A");
        assert_eq!(<PropertyName as ElementName>::parse("Device.A").unwrap(), p);
        let s = String::from("Device.A");
        for n in [
            IntoName::<PropertyName>::into_name("Device.A").unwrap(),
            IntoName::<PropertyName>::into_name(&s).unwrap(),
            IntoName::<PropertyName>::into_name(s.clone()).unwrap(),
            IntoName::<PropertyName>::into_name(&p).unwrap(),
            IntoName::<PropertyName>::into_name(p.clone()).unwrap(),
        ] {
            assert_eq!(n, p);
        }
        let any: Name = p.clone().into();
        assert_eq!(any.as_str(), "Device.A");
        let _: Name = M.into();
        let _: Name = E.into();
        let _: Name = T.into();
        // Typed names look up `str` keys.
        let map: HashMap<PropertyName, i32> = [(p, 1)].into();
        assert_eq!(map["Device.A"], 1);
    }

    #[test]
    fn component_names() {
        check_component("com.example.wifi").unwrap();
        assert_eq!(check_component("").unwrap_err().kind, "component");
        assert!(check_component("a b").is_err());
        assert!(check_component(&"c".repeat(256)).is_err());
    }
}
