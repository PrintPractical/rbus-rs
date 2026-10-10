//! Limits on what the decoders accept from a peer.
//!
//! Every frame comes from another process on the bus, so a decoder must not
//! let one frame exhaust memory or the stack. C recurses and allocates
//! without bounds; these limits cost no interop with real peers.

/// Limits on what the decoders accept from a peer.
///
/// ```
/// use rbus_wire::Limits;
///
/// let limits = Limits::default()
///     .with_max_payload(16 * 1024 * 1024)
///     .with_max_depth(32);
/// assert_eq!(limits.max_items, Limits::default().max_items);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Limits {
    /// The largest frame payload, in bytes. Default 8 MiB.
    pub max_payload: usize,
    /// The deepest nesting of objects, property lists and filters. Default 64.
    pub max_depth: u32,
    /// The most items (properties, children, names, rows) one payload may
    /// announce, in total. Default 100 000. Decoded items take far more
    /// memory than their encoding, so this bounds what one payload costs.
    pub max_items: usize,
}

impl Limits {
    /// The defaults: 8 MiB payloads, nesting 64 deep, 100 000 items.
    pub const DEFAULT: Limits = Limits {
        max_payload: 8 * 1024 * 1024,
        max_depth: 64,
        max_items: 100_000,
    };

    /// Sets [`Limits::max_payload`].
    pub fn with_max_payload(mut self, bytes: usize) -> Limits {
        self.max_payload = bytes;
        self
    }

    /// Sets [`Limits::max_depth`].
    pub fn with_max_depth(mut self, depth: u32) -> Limits {
        self.max_depth = depth;
        self
    }

    /// Sets [`Limits::max_items`].
    pub fn with_max_items(mut self, items: usize) -> Limits {
        self.max_items = items;
        self
    }
}

impl Default for Limits {
    fn default() -> Limits {
        Limits::DEFAULT
    }
}
