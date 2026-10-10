//! The data model every provider on a connection serves, as the engine sees
//! it: a tree of the registered name templates, plus the table rows that
//! exist. The engine owns the rows (as the C library does), so it can resolve
//! `[alias]` names, list rows and walk partial paths without asking providers.
//!
//! Names are matched the way `rbus.c` matches them:
//!
//! - **Exact names** (`Device.T.3.Prop`, `Device.T.[lan].Prop`) resolve to one
//!   element. The row must exist.
//! - **Partial paths** end in `.` and name an object, a table or a row; a get
//!   returns every readable property below it, in schema order and row order
//!   (`_get_recursive_partialpath_handler`).
//! - **Wildcards** put `*` where a row goes and fan out over every row
//!   (`_get_recursive_wildcard_handler_internal`). A trailing `.*` reads as
//!   `.`.
//!
//! Results always use canonical names: instance numbers, never aliases.

use std::collections::HashMap;
use std::time::Duration;

use rbus_wire::body::ElementName;

use crate::error::Status;
use crate::name::NameError;
use crate::provider::schema::{Access, Element, ElementKind, Rows};
use crate::table::RowId;

/// Identifies one registered provider.
pub(crate) type MountId = u64;

/// Index of a node in [`Tree::nodes`].
pub(crate) type NodeId = usize;

const ROOT: NodeId = 0;

/// `rbusAccess_t` bits.
const ACCESS_GET: u32 = 1;
const ACCESS_SET: u32 = 2;
const ACCESS_ADDROW: u32 = 4;
const ACCESS_REMOVEROW: u32 = 8;
const ACCESS_SUBSCRIBE: u32 = 16;
const ACCESS_INVOKE: u32 = 32;

/// `rbusElementType_t`, plus 0 for objects and rows.
const TYPE_OBJECT: i32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    /// A path segment with children; implicit unless it is the root.
    Object,
    Property(Access),
    /// A table. Its children are the elements of each row (the `{i}` level is
    /// folded into the table node).
    Table(Rows),
    Method,
    Event,
}

impl NodeKind {
    fn wire_type(self) -> i32 {
        match self {
            NodeKind::Object => TYPE_OBJECT,
            NodeKind::Property(_) => 1,
            NodeKind::Table(_) => 2,
            NodeKind::Event => 3,
            NodeKind::Method => 4,
        }
    }

    fn access(self) -> u32 {
        match self {
            NodeKind::Object => 0,
            NodeKind::Property(a) => {
                let mut bits = ACCESS_SUBSCRIBE;
                if a.readable() {
                    bits |= ACCESS_GET;
                }
                if a.writable() {
                    bits |= ACCESS_SET;
                }
                bits
            }
            NodeKind::Table(Rows::Dynamic) => ACCESS_ADDROW | ACCESS_REMOVEROW | ACCESS_SUBSCRIBE,
            NodeKind::Table(Rows::Fixed) => ACCESS_SUBSCRIBE,
            NodeKind::Method => ACCESS_INVOKE,
            NodeKind::Event => ACCESS_SUBSCRIBE,
        }
    }

    fn has_children(self) -> bool {
        matches!(self, NodeKind::Object | NodeKind::Table(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    seg: String,
    pub(crate) kind: NodeKind,
    /// The provider that registered this element; `None` for implicit
    /// objects and tables.
    pub(crate) owner: Option<MountId>,
    /// How often to poll a property for value changes.
    pub(crate) poll: Option<Duration>,
    children: Vec<NodeId>,
}

/// Why a schema could not be added to the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaError {
    /// A name is malformed.
    Name(NameError),
    /// A name is already registered.
    Duplicate(String),
    /// A name clashes with the shape of another (an object used as a table,
    /// a property with children, ...).
    Conflict(String),
}

/// Splits a name into segments; the flag says whether it ended in `.`.
fn split(name: &str) -> (Vec<&str>, bool) {
    match name.strip_suffix('.') {
        Some(rest) => (rest.split('.').collect(), true),
        None => (name.split('.').collect(), false),
    }
}

/// True if `seg` is a usable path segment: not empty, no wildcard, alias or
/// template characters.
fn plain(seg: &str) -> bool {
    !seg.is_empty() && !seg.contains(['*', '[', ']', '{', '}'])
}

mod resolve;
mod tree;

#[cfg(test)]
mod tests;

pub(crate) use resolve::{Found, Model, Resolved, Target};
pub(crate) use tree::Tree;
