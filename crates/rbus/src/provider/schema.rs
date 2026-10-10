//! The elements a provider registers.

use std::time::Duration;

/// Who may read and write a property.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Access {
    /// Consumers may get it; sets fail with `NotWritable`.
    ReadOnly,
    /// Consumers may get and set it.
    ReadWrite,
    /// Consumers may set it; gets fail, as for a C property with no getter.
    WriteOnly,
}

impl Access {
    pub(crate) fn readable(self) -> bool {
        self != Access::WriteOnly
    }

    pub(crate) fn writable(self) -> bool {
        self != Access::ReadOnly
    }
}

/// Who may add and remove a table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rows {
    /// Only the provider adds and removes rows, through
    /// [`Provided::add_row`](crate::provider::Provided::add_row).
    Fixed,
    /// Consumers may also add and remove rows; the engine calls
    /// [`Provider::add_row`](crate::provider::Provider::add_row) and
    /// [`Provider::remove_row`](crate::provider::Provider::remove_row).
    Dynamic,
}

/// What kind of element a [`Schema`] entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ElementKind {
    /// A property (parameter) with a value.
    Property(Access),
    /// A table; each `{i}` in a name below it is one of its rows.
    Table(Rows),
    /// A method, invoked with [`Provider::invoke`](crate::provider::Provider::invoke).
    Method,
    /// An event (published from M5 on).
    Event,
}

/// One registered element: a name template and its kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Element {
    name: String,
    kind: ElementKind,
    poll: Option<Duration>,
}

impl Element {
    /// The name template, with `{i}` for each table row: `Device.WiFi.Radio.{i}.Enable`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The kind of element.
    pub fn kind(&self) -> ElementKind {
        self.kind
    }

    /// How often the engine polls this property for value changes, if at all.
    pub fn poll(&self) -> Option<Duration> {
        self.poll
    }
}

/// The data model a provider serves: element names with `{i}` standing for
/// table rows, in the order they should be listed.
///
/// ```
/// use rbus::provider::{Access, Rows, Schema};
///
/// let schema = Schema::new()
///     .property("Device.Sample.Enable", Access::ReadWrite)
///     .table("Device.Sample.Host.{i}.", Rows::Dynamic)
///     .property("Device.Sample.Host.{i}.Name", Access::ReadOnly)
///     .method("Device.Sample.Host.{i}.Reset()")
///     .event("Device.Sample.Alarm!");
/// assert_eq!(schema.elements().len(), 5);
/// ```
///
/// Names are checked when the provider is registered. A table may be written
/// `Device.Sample.Host.` or `Device.Sample.Host.{i}.`. A `{i}` below a table
/// that isn't listed makes it a [`Rows::Fixed`] table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    elements: Vec<Element>,
}

impl Schema {
    /// An empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    fn with(mut self, name: impl Into<String>, kind: ElementKind) -> Self {
        self.elements.push(Element {
            name: name.into(),
            kind,
            poll: None,
        });
        self
    }

    /// Adds a property.
    pub fn property(self, name: impl Into<String>, access: Access) -> Self {
        self.with(name, ElementKind::Property(access))
    }

    /// Adds a table.
    pub fn table(self, path: impl Into<String>, rows: Rows) -> Self {
        let mut path: String = path.into();
        if !path.ends_with("{i}.") {
            path.push_str("{i}.");
        }
        self.with(path, ElementKind::Table(rows))
    }

    /// Adds a method; its name ends in `()`.
    pub fn method(self, name: impl Into<String>) -> Self {
        self.with(name, ElementKind::Method)
    }

    /// Adds an event. Event names normally end in `!`, but C providers
    /// also register plain names (`Device.X.{i}.Event`), so either works.
    pub fn event(self, name: impl Into<String>) -> Self {
        self.with(name, ElementKind::Event)
    }

    /// Polls the property added last every `every`, and publishes a
    /// value-change event to its subscribers when the value differs (P10).
    /// C does this for every subscribed property (every 2 s); here it is
    /// opt-in, for values that change without the provider knowing, such as
    /// hardware counters. Changes made by a consumer's set are published
    /// without polling, and the provider can publish its own with
    /// [`Provided::notify_changed`](crate::provider::Provided::notify_changed)
    /// or [`Provided::publish_value_change`](crate::provider::Provided::publish_value_change).
    ///
    /// Registration fails if the last element isn't a property.
    pub fn poll(mut self, every: Duration) -> Self {
        if let Some(last) = self.elements.last_mut() {
            last.poll = Some(every);
        }
        self
    }

    /// The elements, in order.
    pub fn elements(&self) -> &[Element] {
        &self.elements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_are_written_with_their_row_template() {
        let s = Schema::new()
            .table("A.T.", Rows::Fixed)
            .table("A.U.{i}.", Rows::Dynamic);
        let names: Vec<_> = s.elements().iter().map(Element::name).collect();
        assert_eq!(names, ["A.T.{i}.", "A.U.{i}."]);
        assert_eq!(s.elements()[1].kind(), ElementKind::Table(Rows::Dynamic));
        assert!(Access::ReadWrite.readable() && Access::ReadWrite.writable());
        assert!(!Access::WriteOnly.readable() && !Access::ReadOnly.writable());
        let p = Schema::new()
            .property("A.P", Access::ReadOnly)
            .poll(Duration::from_secs(2));
        assert_eq!(p.elements()[0].poll(), Some(Duration::from_secs(2)));
        assert_eq!(s.elements()[0].poll(), None);
        assert!(Schema::new().poll(Duration::ZERO).elements().is_empty());
    }
}
