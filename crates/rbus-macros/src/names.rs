//! Name rules, mirrored from `rbus::name` so they can be checked while
//! compiling.

/// The longest name the broker routes.
const MAX_NAME_LEN: usize = 255;

/// The kind of element a name's suffix marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Property,
    Method,
    Event,
    Table,
}

impl Kind {
    /// The `rbus` type that holds a name of this kind.
    pub(crate) fn type_name(self) -> &'static str {
        match self {
            Kind::Property => "PropertyName",
            Kind::Method => "MethodName",
            Kind::Event => "EventName",
            Kind::Table => "TablePath",
        }
    }
}

/// Checks an element name and returns its kind, as `rbus::name` does at
/// run time.
pub(crate) fn check(name: &str) -> Result<Kind, &'static str> {
    let b = name.as_bytes();
    if b.is_empty() {
        return Err("it is empty");
    }
    if b.len() > MAX_NAME_LEN {
        return Err("it is longer than 255 bytes");
    }
    if b[0] == b'.' {
        return Err("it starts with '.'");
    }
    if b.iter().any(|&c| c <= b' ' || c == 0x7f) {
        return Err("it contains whitespace or a control character");
    }
    if name.contains("..") {
        return Err("it contains an empty path segment");
    }
    if let Some(rest) = name.strip_suffix("()") {
        if rest.is_empty() || rest.ends_with('.') {
            return Err("it has no method name before \"()\"");
        }
        return Ok(Kind::Method);
    }
    if let Some(rest) = name.strip_suffix('!') {
        if rest.is_empty() || rest.ends_with('.') {
            return Err("it has no event name before '!'");
        }
        return Ok(Kind::Event);
    }
    if name.ends_with('.') {
        return Ok(Kind::Table);
    }
    Ok(Kind::Property)
}

/// Checks one path segment a member is named with: no dots, wildcards,
/// aliases or templates.
pub(crate) fn check_segment(seg: &str) -> Result<(), &'static str> {
    if seg.is_empty() {
        return Err("a name can't be empty");
    }
    if seg.contains(['.', '*', '[', ']', '{', '}', '(', ')']) {
        return Err("a member name is one path segment: no '.', '*', '[', ']', '{', '}' or \"()\"");
    }
    if seg.bytes().any(|c| c <= b' ' || c == 0x7f) {
        return Err("a name can't contain whitespace or control characters");
    }
    Ok(())
}

/// TR-181 naming: `radio_number_of_entries` becomes `RadioNumberOfEntries`.
pub(crate) fn pascal_case(ident: &str) -> String {
    ident
        .trim_start_matches("r#")
        .split('_')
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                Some(first) => first.to_uppercase().chain(c).collect(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_checked_like_rbus_does() {
        assert_eq!(check("Device.A.B"), Ok(Kind::Property));
        assert_eq!(check("Device.Reboot()"), Ok(Kind::Method));
        assert_eq!(check("Device.Alarm!"), Ok(Kind::Event));
        assert_eq!(check("Device.Host."), Ok(Kind::Table));
        for bad in ["", ".A", "A..B", "A B", "()", "A.()", "!", "A.!"] {
            assert!(check(bad).is_err(), "{bad}");
        }
        assert!(check(&"A".repeat(256)).is_err());
        assert!(check_segment("Radio").is_ok());
        for bad in ["", "A.B", "A*", "[a]", "{i}", "A()", "A B"] {
            assert!(check_segment(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn pascal_case_follows_tr181() {
        assert_eq!(
            pascal_case("radio_number_of_entries"),
            "RadioNumberOfEntries"
        );
        assert_eq!(pascal_case("enable"), "Enable");
        assert_eq!(pascal_case("r#type"), "Type");
        assert_eq!(pascal_case("last__uptime_"), "LastUptime");
    }
}
