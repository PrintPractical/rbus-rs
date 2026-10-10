//! Errors produced while encoding or decoding wire data.

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong turning bytes into frames and payloads, or back.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The input ended before the item being read was complete.
    #[error("unexpected end of data while reading {0}")]
    Truncated(&'static str),

    /// A frame header did not start or end with the 0xAAAA marker.
    #[error("bad header marker {found:#06x}")]
    BadMarker {
        /// The value found where the marker was expected.
        found: u16,
    },

    /// The peer speaks a header version other than 2.
    #[error("unsupported header version {0}")]
    UnsupportedVersion(u16),

    /// The header's own length field is inconsistent with its contents.
    #[error("bad header length {0}")]
    BadHeaderLength(usize),

    /// A topic or reply topic is too long for the 256-byte limit of rtrouted.
    #[error("topic of {0} bytes exceeds the 255-byte limit")]
    TopicTooLong(usize),

    /// A payload larger than the decoder's configured limit.
    #[error("payload of {len} bytes exceeds the {max}-byte limit")]
    PayloadTooLarge {
        /// Announced payload length.
        len: usize,
        /// Configured maximum.
        max: usize,
    },

    /// Refusing to encode a frame with an empty payload: rtrouted stalls on them.
    #[error("frames must not have an empty payload (rtrouted stalls on them)")]
    EmptyPayload,

    /// A msgpack item of an unexpected type.
    #[error("expected {expected}, found msgpack type byte {found:#04x}")]
    UnexpectedType {
        /// What the reader wanted.
        expected: &'static str,
        /// The type byte that was found.
        found: u8,
    },

    /// An rbus payload without the trailing meta-section offset.
    #[error("missing or invalid meta trailer")]
    BadMetaTrailer,

    /// A value record with a type tag this crate does not know.
    #[error("unknown value type tag {0:#x}")]
    UnknownValueType(i32),

    /// A value whose body does not fit its type (for example a 3-byte bool).
    #[error("invalid {ty} value: {reason}")]
    InvalidValue {
        /// The value type being decoded.
        ty: &'static str,
        /// What was wrong.
        reason: String,
    },

    /// A legacy (CCSP) string value that does not parse as its declared type.
    #[error("legacy value {value:?} is not a valid {ty}")]
    InvalidLegacyValue {
        /// The legacy type name.
        ty: &'static str,
        /// The string received.
        value: String,
    },

    /// A payload went past one of the decoder's [`Limits`](crate::Limits).
    #[error("{what} exceeds the limit of {max}")]
    LimitExceeded {
        /// What was too large: `"nesting depth"` or `"item count"`.
        what: &'static str,
        /// The configured limit.
        max: usize,
    },

    /// A broker control message that is not valid JSON of the expected shape.
    #[error("invalid control message: {0}")]
    Json(#[from] serde_json::Error),
}
