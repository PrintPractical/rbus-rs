//! Minimal rtmessage frame splitting.
//!
//! A frame is a header followed by its payload. All integers are big-endian:
//!
//! ```text
//! u16 0xAAAA | u16 version | u16 header_len | u32 seq | u32 flags | u32 control_data
//! | u32 payload_len | u32 len + topic | u32 len + reply_topic | [5 x u64] | u16 0xAAAA
//! ```

use std::io::{self, Read};

/// Start and end marker of every header.
pub const MARKER: u16 = 0xAAAA;
/// Bytes before `header_len` is known: marker, version, header_len.
const PREAMBLE: usize = 6;
/// Smallest valid header: fixed fields, two empty strings, end marker.
const MIN_HEADER: usize = 32;

/// Header fields of one frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Protocol version; rtrouted only accepts 2.
    pub version: u16,
    /// Total header length in bytes, including both markers.
    pub header_len: u16,
    /// Per-connection request sequence number.
    pub seq: u32,
    /// Flag bits (request, response, undeliverable, raw binary, ...).
    pub flags: u32,
    /// Route id, rewritten by the broker to the receiving route.
    pub control_data: u32,
    /// Payload length in bytes.
    pub payload_len: u32,
    /// Destination topic.
    pub topic: String,
    /// Reply topic (usually the sender's inbox).
    pub reply_topic: String,
}

impl Header {
    /// Size of the header without its two strings: 32, or 72 when the peer
    /// was built with `MSG_ROUNDTRIP_TIME`.
    pub fn fixed_len(&self) -> usize {
        usize::from(self.header_len) - self.topic.len() - self.reply_topic.len()
    }
}

/// One complete frame: its parsed header and the raw bytes (header + payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Parsed header fields.
    pub header: Header,
    /// The frame exactly as it appeared on the wire.
    pub bytes: Vec<u8>,
}

impl Frame {
    /// The payload part of [`Frame::bytes`].
    pub fn payload(&self) -> &[u8] {
        &self.bytes[usize::from(self.header.header_len)..]
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn be_u16(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([b[at], b[at + 1]])
}

fn be_u32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// Parses the header at the start of `bytes`, which must hold at least
/// `header_len` bytes.
pub fn parse_header(bytes: &[u8]) -> io::Result<Header> {
    if bytes.len() < MIN_HEADER {
        return Err(invalid(format!("header too short: {} bytes", bytes.len())));
    }
    if be_u16(bytes, 0) != MARKER {
        return Err(invalid("bad start marker"));
    }
    let header_len = be_u16(bytes, 4);
    let hl = usize::from(header_len);
    if hl < MIN_HEADER || bytes.len() < hl {
        return Err(invalid(format!("bad header_len {hl}")));
    }
    if be_u16(bytes, hl - 2) != MARKER {
        return Err(invalid("bad end marker"));
    }
    let read_str = |at: usize| -> io::Result<(String, usize)> {
        if at + 4 > hl {
            return Err(invalid("string length past header end"));
        }
        let len = be_u32(bytes, at) as usize;
        let start = at + 4;
        if start + len > hl {
            return Err(invalid("string past header end"));
        }
        let s = String::from_utf8_lossy(&bytes[start..start + len]).into_owned();
        Ok((s, start + len))
    };
    let (topic, next) = read_str(22)?;
    let (reply_topic, _) = read_str(next)?;
    Ok(Header {
        version: be_u16(bytes, 2),
        header_len,
        seq: be_u32(bytes, 6),
        flags: be_u32(bytes, 10),
        control_data: be_u32(bytes, 14),
        payload_len: be_u32(bytes, 18),
        topic,
        reply_topic,
    })
}

/// Reads the next frame from `r`. Returns `Ok(None)` on a clean end of stream
/// between frames.
pub fn read_frame(r: &mut impl Read) -> io::Result<Option<Frame>> {
    let mut bytes = vec![0u8; PREAMBLE];
    let mut filled = 0;
    while filled < PREAMBLE {
        match r.read(&mut bytes[filled..])? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            n => filled += n,
        }
    }
    let hl = usize::from(be_u16(&bytes, 4));
    if hl < MIN_HEADER {
        return Err(invalid(format!("bad header_len {hl}")));
    }
    bytes.resize(hl, 0);
    r.read_exact(&mut bytes[PREAMBLE..])?;
    let header = parse_header(&bytes)?;
    let payload_len = header.payload_len as usize;
    bytes.resize(hl + payload_len, 0);
    r.read_exact(&mut bytes[hl..])?;
    Ok(Some(Frame { header, bytes }))
}

/// Encodes a frame with a 32-byte fixed header. Only for tests: the real
/// encoder lives in `rbus-wire`.
pub fn encode_frame(
    seq: u32,
    flags: u32,
    control_data: u32,
    topic: &str,
    reply_topic: &str,
    payload: &[u8],
) -> Vec<u8> {
    let header_len = MIN_HEADER + topic.len() + reply_topic.len();
    let mut b = Vec::with_capacity(header_len + payload.len());
    b.extend_from_slice(&MARKER.to_be_bytes());
    b.extend_from_slice(&2u16.to_be_bytes());
    b.extend_from_slice(&(header_len as u16).to_be_bytes());
    b.extend_from_slice(&seq.to_be_bytes());
    b.extend_from_slice(&flags.to_be_bytes());
    b.extend_from_slice(&control_data.to_be_bytes());
    b.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    for s in [topic, reply_topic] {
        b.extend_from_slice(&(s.len() as u32).to_be_bytes());
        b.extend_from_slice(s.as_bytes());
    }
    b.extend_from_slice(&MARKER.to_be_bytes());
    b.extend_from_slice(payload);
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trips_through_read_frame() {
        let bytes = encode_frame(7, 0x11, 3, "Device.X", "rbus.t.INBOX.1", b"{\"a\":1}");
        let mut cur = Cursor::new(bytes.clone());
        let frame = read_frame(&mut cur).unwrap().unwrap();
        assert_eq!(frame.bytes, bytes);
        assert_eq!(frame.header.seq, 7);
        assert_eq!(frame.header.flags, 0x11);
        assert_eq!(frame.header.control_data, 3);
        assert_eq!(frame.header.topic, "Device.X");
        assert_eq!(frame.header.reply_topic, "rbus.t.INBOX.1");
        assert_eq!(frame.header.fixed_len(), 32);
        assert_eq!(frame.payload(), b"{\"a\":1}");
        assert!(read_frame(&mut cur).unwrap().is_none());
    }

    #[test]
    fn reads_back_to_back_frames() {
        let mut bytes = encode_frame(1, 0, 0, "a", "", b"x");
        bytes.extend(encode_frame(2, 0, 0, "b", "", b""));
        let mut cur = Cursor::new(bytes);
        assert_eq!(read_frame(&mut cur).unwrap().unwrap().header.topic, "a");
        assert_eq!(read_frame(&mut cur).unwrap().unwrap().header.seq, 2);
        assert!(read_frame(&mut cur).unwrap().is_none());
    }

    #[test]
    fn rejects_bad_markers_and_lengths() {
        let mut bytes = encode_frame(1, 0, 0, "a", "b", b"");
        let end = bytes.len() - 2;
        bytes[end] = 0;
        assert!(parse_header(&bytes).is_err(), "bad end marker");
        bytes[0] = 0;
        assert!(parse_header(&bytes).is_err(), "bad start marker");
        assert!(parse_header(&[0xAA; 10]).is_err(), "too short");
        let mut short = encode_frame(1, 0, 0, "", "", b"");
        short[5] = 4;
        assert!(
            read_frame(&mut Cursor::new(short)).is_err(),
            "header_len below minimum"
        );
    }

    #[test]
    fn truncated_stream_is_an_error() {
        let bytes = encode_frame(1, 0, 0, "topic", "", b"payload");
        let mut cur = Cursor::new(bytes[..bytes.len() - 3].to_vec());
        assert!(read_frame(&mut cur).is_err());
        let mut cur = Cursor::new(bytes[..3].to_vec());
        assert!(read_frame(&mut cur).is_err());
    }
}
