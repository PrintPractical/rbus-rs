//! Event subscription filters and event bodies.

use crate::error::{Error, Result};
use crate::msgpack::{Reader, Writer};
use crate::value::{Object, Value};

/// Relational operator of a filter (`rbusFilter_RelationOperator_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationOp {
    /// `>`
    GreaterThan,
    /// `>=`
    GreaterThanOrEqual,
    /// `<`
    LessThan,
    /// `<=`
    LessThanOrEqual,
    /// `==`
    Equal,
    /// `!=`
    NotEqual,
}

const RELATION_OPS: [RelationOp; 6] = [
    RelationOp::GreaterThan,
    RelationOp::GreaterThanOrEqual,
    RelationOp::LessThan,
    RelationOp::LessThanOrEqual,
    RelationOp::Equal,
    RelationOp::NotEqual,
];

/// A subscription filter: a relation against a value, or a logical combination.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// `value <op> operand`.
    Relation {
        /// The operator.
        op: RelationOp,
        /// The right-hand operand.
        value: Value,
    },
    /// Both filters match.
    And(Box<Filter>, Box<Filter>),
    /// Either filter matches.
    Or(Box<Filter>, Box<Filter>),
    /// The filter does not match.
    Not(Box<Filter>),
}

const EXPR_RELATION: i32 = 0;
const EXPR_LOGIC: i32 = 1;
const LOGIC_AND: i32 = 0;
const LOGIC_OR: i32 = 1;
const LOGIC_NOT: i32 = 2;

impl Filter {
    /// Writes the filter (`rbusFilter_AppendToMessage`).
    pub fn encode(&self, w: &mut Writer) {
        match self {
            Filter::Relation { op, value } => {
                w.i32(EXPR_RELATION);
                w.i32(RELATION_OPS.iter().position(|o| o == op).expect("listed") as i32);
                // The operand is a value record whose name is always "filter".
                w.str("filter");
                value.encode(w);
            }
            Filter::And(l, r) | Filter::Or(l, r) => {
                w.i32(EXPR_LOGIC);
                w.i32(if matches!(self, Filter::And(..)) {
                    LOGIC_AND
                } else {
                    LOGIC_OR
                });
                l.encode(w);
                r.encode(w);
            }
            Filter::Not(inner) => {
                w.i32(EXPR_LOGIC);
                w.i32(LOGIC_NOT);
                inner.encode(w);
            }
        }
    }

    /// Reads a filter (`rbusFilter_InitFromMessage`).
    pub fn decode(r: &mut Reader<'_>) -> Result<Filter> {
        let invalid = |what: &str, code: i32| Error::InvalidValue {
            ty: "filter",
            reason: format!("unknown {what} {code}"),
        };
        match r.i32()? {
            EXPR_RELATION => {
                let code = r.i32()?;
                let op = usize::try_from(code)
                    .ok()
                    .and_then(|i| RELATION_OPS.get(i))
                    .copied()
                    .ok_or_else(|| invalid("relation operator", code))?;
                let _name = r.str()?;
                Ok(Filter::Relation {
                    op,
                    value: Value::decode(r)?,
                })
            }
            EXPR_LOGIC => match r.i32()? {
                LOGIC_NOT => Ok(Filter::Not(Box::new(Filter::decode(r)?))),
                code @ (LOGIC_AND | LOGIC_OR) => {
                    let left = Box::new(Filter::decode(r)?);
                    let right = Box::new(Filter::decode(r)?);
                    Ok(if code == LOGIC_AND {
                        Filter::And(left, right)
                    } else {
                        Filter::Or(left, right)
                    })
                }
                code => Err(invalid("logic operator", code)),
            },
            code => Err(invalid("expression type", code)),
        }
    }
}

/// The kind of event (`rbusEventType_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// A table row was added.
    ObjectCreated,
    /// A table row was removed.
    ObjectDeleted,
    /// A property value changed.
    ValueChanged,
    /// A provider-defined event.
    General,
    /// The current value, sent right after subscribing.
    InitialValue,
    /// A periodic event of an interval subscription.
    Interval,
    /// The last event of a subscription with a duration.
    DurationComplete,
    /// A kind this crate does not know.
    Other(i32),
}

impl EventKind {
    /// Wire code.
    pub fn code(self) -> i32 {
        match self {
            EventKind::ObjectCreated => 0,
            EventKind::ObjectDeleted => 1,
            EventKind::ValueChanged => 2,
            EventKind::General => 3,
            EventKind::InitialValue => 4,
            EventKind::Interval => 5,
            EventKind::DurationComplete => 6,
            EventKind::Other(c) => c,
        }
    }

    /// From a wire code.
    pub fn from_code(code: i32) -> EventKind {
        match code {
            0 => EventKind::ObjectCreated,
            1 => EventKind::ObjectDeleted,
            2 => EventKind::ValueChanged,
            3 => EventKind::General,
            4 => EventKind::InitialValue,
            5 => EventKind::Interval,
            6 => EventKind::DurationComplete,
            c => EventKind::Other(c),
        }
    }
}

/// An event as carried in event messages and initial-value responses
/// (`rbusEventData_appendToMessage`). The filter, interval and duration echo
/// the subscription so the subscriber can match it.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// The event's actual name (the instance name for wildcard subscriptions).
    pub name: String,
    /// What happened.
    pub kind: EventKind,
    /// Event data, if any.
    pub data: Option<Object>,
    /// The subscription's filter.
    pub filter: Option<Filter>,
    /// The subscription's interval in seconds (0 = none).
    pub interval: u32,
    /// The subscription's duration in seconds (0 = none).
    pub duration: u32,
    /// The subscriber's component id, echoed from the subscribe request.
    pub component_id: i32,
}

impl Event {
    /// Writes the event body.
    pub fn encode(&self, w: &mut Writer) {
        w.str(&self.name);
        w.i32(self.kind.code());
        w.flag(self.data.is_some());
        if let Some(data) = &self.data {
            data.encode(w);
        }
        w.flag(self.filter.is_some());
        if let Some(filter) = &self.filter {
            filter.encode(w);
        }
        w.i32(self.interval as i32);
        w.i32(self.duration as i32);
        w.i32(self.component_id);
    }

    /// Reads an event body.
    pub fn decode(r: &mut Reader<'_>) -> Result<Event> {
        let name = r.str()?;
        let kind = EventKind::from_code(r.i32()?);
        let data = if r.flag()? {
            Some(Object::decode(r)?)
        } else {
            None
        };
        let filter = if r.flag()? {
            Some(Filter::decode(r)?)
        } else {
            None
        };
        Ok(Event {
            name,
            kind,
            data,
            filter,
            interval: r.i32()? as u32,
            duration: r.i32()? as u32,
            component_id: r.i32()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_round_trip(f: &Filter) -> Vec<u8> {
        let mut w = Writer::new();
        f.encode(&mut w);
        let b = w.into_bytes();
        let mut r = Reader::new(&b);
        assert_eq!(Filter::decode(&mut r).unwrap(), *f);
        assert!(r.is_empty());
        b
    }

    #[test]
    fn relation_filter_matches_recorded_bytes() {
        // From subscribe_filter_initial: `Device.Provider1.Param1 > 2`.
        let f = Filter::Relation {
            op: RelationOp::GreaterThan,
            value: Value::I32(2),
        };
        assert_eq!(
            filter_round_trip(&f),
            b"\x00\x00\xa7filter\x00\xcd\x05\x07\x02".to_vec()
        );
    }

    #[test]
    fn logic_filters_round_trip() {
        let gt = || Filter::Relation {
            op: RelationOp::GreaterThanOrEqual,
            value: Value::I32(1),
        };
        let ne = Filter::Relation {
            op: RelationOp::NotEqual,
            value: Value::String("x".into()),
        };
        for op in RELATION_OPS {
            filter_round_trip(&Filter::Relation {
                op,
                value: Value::None,
            });
        }
        filter_round_trip(&Filter::And(Box::new(gt()), Box::new(ne.clone())));
        filter_round_trip(&Filter::Or(Box::new(gt()), Box::new(ne)));
        let not = filter_round_trip(&Filter::Not(Box::new(gt())));
        assert_eq!(&not[..2], &[1, 2], "NOT has only a left operand");
    }

    #[test]
    fn bad_filters_are_errors() {
        for bytes in [&[5u8][..], &[0, 9], &[1, 7]] {
            assert!(matches!(
                Filter::decode(&mut Reader::new(bytes)),
                Err(Error::InvalidValue { ty: "filter", .. })
            ));
        }
    }

    #[test]
    fn event_round_trips() {
        let ev = Event {
            name: "Device.Provider1.Event1!".into(),
            kind: EventKind::General,
            data: Some(Object::new().with("someText", Value::String("Hello Earth".into()))),
            filter: None,
            interval: 0,
            duration: 0,
            component_id: 1,
        };
        let mut w = Writer::new();
        ev.encode(&mut w);
        let b = w.into_bytes();
        assert_eq!(Event::decode(&mut Reader::new(&b)).unwrap(), ev);
        let bare = Event {
            data: None,
            filter: Some(Filter::Not(Box::new(Filter::Relation {
                op: RelationOp::Equal,
                value: Value::Bool(true),
            }))),
            kind: EventKind::Other(42),
            interval: u32::MAX,
            ..ev
        };
        let mut w = Writer::new();
        bare.encode(&mut w);
        let b = w.into_bytes();
        assert_eq!(Event::decode(&mut Reader::new(&b)).unwrap(), bare);
    }

    #[test]
    fn event_kind_codes() {
        for c in -1..8 {
            assert_eq!(EventKind::from_code(c).code(), c);
        }
    }
}
