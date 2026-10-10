//! rtmessage framing: the header every message on the broker socket carries.
//!
//! ```text
//! u16 0xAAAA | u16 version=2 | u16 header_len | u32 seq | u32 flags | u32 control_data
//! | u32 payload_len | u32 len + topic | u32 len + reply_topic | [5 x u64] | u16 0xAAAA
//! ```
//!
//! All integers are big-endian and strings carry no NUL. The five `u64`
//! timestamps exist only when the peer was built with `MSG_ROUNDTRIP_TIME`,
//! which makes the fixed part 72 bytes instead of 32. We always send 32-byte
//! headers and accept both.

use std::fmt;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

/// Start and end marker of every header.
pub const MARKER: u16 = 0xAAAA;
/// The only header version rtrouted accepts.
pub const VERSION: u16 = 2;
/// Fixed header size without the two topic strings.
pub const FIXED_HEADER_LEN: usize = 32;
/// Fixed header size when the peer was built with `MSG_ROUNDTRIP_TIME`.
pub const FIXED_HEADER_LEN_ROUNDTRIP: usize = 72;
/// Topics and reply topics must be shorter than this (rtMessageHeader.c).
pub const MAX_TOPIC_LEN: usize = 256;
/// Default limit for payloads accepted by [`FrameDecoder`].
pub const DEFAULT_MAX_PAYLOAD: usize = 64 * 1024 * 1024;

const PREAMBLE_LEN: usize = 6;

/// Header flag bits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Flags(pub u32);

impl Flags {
    /// No flags: a plain message (broker control, inbox registration).
    pub const NONE: Flags = Flags(0);
    /// The sender expects a response with the same sequence number.
    pub const REQUEST: Flags = Flags(0x01);
    /// A response to a request.
    pub const RESPONSE: Flags = Flags(0x02);
    /// Set by rtrouted when no route matched a request.
    pub const UNDELIVERABLE: Flags = Flags(0x04);
    /// Benchmark marker, unused by rbus.
    pub const TAINTED: Flags = Flags(0x08);
    /// The payload is an rbus msgpack message (not broker JSON).
    pub const RAW_BINARY: Flags = Flags(0x10);
    /// Encrypted payload (SPAKE2+ builds only; unsupported).
    pub const ENCRYPTED: Flags = Flags(0x20);

    /// True when every bit of `other` is set.
    pub fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for Flags {
    type Output = Flags;
    fn bitor(self, rhs: Flags) -> Flags {
        Flags(self.0 | rhs.0)
    }
}

impl fmt::Debug for Flags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const NAMES: [(Flags, &str); 6] = [
            (Flags::REQUEST, "REQUEST"),
            (Flags::RESPONSE, "RESPONSE"),
            (Flags::UNDELIVERABLE, "UNDELIVERABLE"),
            (Flags::TAINTED, "TAINTED"),
            (Flags::RAW_BINARY, "RAW_BINARY"),
            (Flags::ENCRYPTED, "ENCRYPTED"),
        ];
        let mut parts: Vec<String> = NAMES
            .iter()
            .filter(|(flag, _)| self.contains(*flag))
            .map(|(_, name)| (*name).to_owned())
            .collect();
        let known = NAMES.iter().fold(0, |acc, (flag, _)| acc | flag.0);
        if self.0 & !known != 0 {
            parts.push(format!("{:#x}", self.0 & !known));
        }
        if parts.is_empty() {
            f.write_str("NONE")
        } else {
            f.write_str(&parts.join(" | "))
        }
    }
}

/// A frame header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Per-connection sequence number; responses echo the request's.
    pub seq: u32,
    /// Flag bits.
    pub flags: Flags,
    /// Route id. The broker overwrites it with the receiving route's id, and
    /// clients dispatch non-response messages by it.
    pub control_data: u32,
    /// Destination topic (element name, component name or inbox).
    pub topic: String,
    /// Where replies go: normally the sender's inbox.
    pub reply_topic: String,
    /// `MSG_ROUNDTRIP_TIME` timestamps T1..T5, present only in 72-byte headers.
    pub roundtrip: Option<[u64; 5]>,
}

impl Header {
    /// A header with a 32-byte fixed part.
    pub fn new(
        seq: u32,
        flags: Flags,
        topic: impl Into<String>,
        reply_topic: impl Into<String>,
    ) -> Self {
        Header {
            seq,
            flags,
            control_data: 0,
            topic: topic.into(),
            reply_topic: reply_topic.into(),
            roundtrip: None,
        }
    }

    /// Size of the encoded header.
    pub fn encoded_len(&self) -> usize {
        let fixed = if self.roundtrip.is_some() {
            FIXED_HEADER_LEN_ROUNDTRIP
        } else {
            FIXED_HEADER_LEN
        };
        fixed + self.topic.len() + self.reply_topic.len()
    }

    /// Appends the encoded header for a payload of `payload_len` bytes.
    pub fn encode_into(&self, payload_len: usize, out: &mut impl BufMut) -> Result<()> {
        for topic in [&self.topic, &self.reply_topic] {
            if topic.len() >= MAX_TOPIC_LEN {
                return Err(Error::TopicTooLong(topic.len()));
            }
        }
        let payload_len = u32::try_from(payload_len).map_err(|_| Error::PayloadTooLarge {
            len: payload_len,
            max: u32::MAX as usize,
        })?;
        out.put_u16(MARKER);
        out.put_u16(VERSION);
        out.put_u16(self.encoded_len() as u16);
        out.put_u32(self.seq);
        out.put_u32(self.flags.0);
        out.put_u32(self.control_data);
        out.put_u32(payload_len);
        for s in [&self.topic, &self.reply_topic] {
            out.put_u32(s.len() as u32);
            out.put_slice(s.as_bytes());
        }
        if let Some(ts) = &self.roundtrip {
            for t in ts {
                out.put_u64(*t);
            }
        }
        out.put_u16(MARKER);
        Ok(())
    }

    /// Decodes a complete header. `bytes` must hold at least the header length
    /// announced in its preamble; returns the header and the payload length.
    pub fn decode(bytes: &[u8]) -> Result<(Header, usize)> {
        let header_len = Self::peek_len(bytes)?;
        let b = bytes.get(..header_len).ok_or(Error::Truncated("header"))?;
        let marker = be_u16(b, header_len - 2);
        if marker != MARKER {
            return Err(Error::BadMarker { found: marker });
        }
        let mut pos = 22;
        let string = |pos: &mut usize| -> Result<String> {
            let len_end = *pos + 4;
            if len_end > header_len - 2 {
                return Err(Error::BadHeaderLength(header_len));
            }
            let len = be_u32(b, *pos) as usize;
            let end = len_end
                .checked_add(len)
                .filter(|&e| e <= header_len - 2)
                .ok_or(Error::BadHeaderLength(header_len))?;
            let s = String::from_utf8_lossy(&b[len_end..end]).into_owned();
            *pos = end;
            Ok(s)
        };
        let topic = string(&mut pos)?;
        let reply_topic = string(&mut pos)?;
        let roundtrip = match header_len - topic.len() - reply_topic.len() {
            FIXED_HEADER_LEN => None,
            FIXED_HEADER_LEN_ROUNDTRIP => {
                let mut ts = [0u64; 5];
                for (i, t) in ts.iter_mut().enumerate() {
                    let at = pos + i * 8;
                    *t = u64::from_be_bytes(b[at..at + 8].try_into().expect("8 bytes"));
                }
                Some(ts)
            }
            _ => return Err(Error::BadHeaderLength(header_len)),
        };
        let header = Header {
            seq: be_u32(b, 6),
            flags: Flags(be_u32(b, 10)),
            control_data: be_u32(b, 14),
            topic,
            reply_topic,
            roundtrip,
        };
        Ok((header, be_u32(b, 18) as usize))
    }

    /// Validates the 6-byte preamble and returns the full header length.
    pub fn peek_len(bytes: &[u8]) -> Result<usize> {
        let p = bytes
            .get(..PREAMBLE_LEN)
            .ok_or(Error::Truncated("header preamble"))?;
        let marker = be_u16(p, 0);
        if marker != MARKER {
            return Err(Error::BadMarker { found: marker });
        }
        let version = be_u16(p, 2);
        if version != VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        let header_len = usize::from(be_u16(p, 4));
        if header_len < FIXED_HEADER_LEN {
            return Err(Error::BadHeaderLength(header_len));
        }
        Ok(header_len)
    }
}

/// A complete message: header plus payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The header.
    pub header: Header,
    /// The payload: broker JSON, or an rbus msgpack message when the
    /// [`Flags::RAW_BINARY`] flag is set. Decoded frames share the receive
    /// buffer instead of copying it.
    pub payload: Bytes,
}

impl Frame {
    /// A frame. See [`Frame::encode`] for the rules it must satisfy.
    pub fn new(header: Header, payload: impl Into<Bytes>) -> Self {
        Frame {
            header,
            payload: payload.into(),
        }
    }

    /// Encodes the frame for sending. Fails on an empty payload, because
    /// rtrouted does not dispatch such frames until more data arrives.
    pub fn encode(&self) -> Result<Bytes> {
        if self.payload.is_empty() {
            return Err(Error::EmptyPayload);
        }
        self.encode_unchecked()
    }

    /// Encodes the frame, allowing an empty payload. Only the broker sends
    /// those (undeliverable replies); this exists for tests and tooling.
    pub fn encode_unchecked(&self) -> Result<Bytes> {
        let mut out = BytesMut::with_capacity(self.header.encoded_len() + self.payload.len());
        self.header.encode_into(self.payload.len(), &mut out)?;
        out.put_slice(&self.payload);
        Ok(out.freeze())
    }
}

/// Incremental frame decoder for a byte stream. Feed it whatever the socket
/// returns; it yields frames as soon as they are complete. Payloads are split
/// off the receive buffer without copying.
#[derive(Debug)]
pub struct FrameDecoder {
    buf: BytesMut,
    max_payload: usize,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    /// A decoder with [`DEFAULT_MAX_PAYLOAD`].
    pub fn new() -> Self {
        Self::with_max_payload(DEFAULT_MAX_PAYLOAD)
    }

    /// A decoder that rejects payloads larger than `max_payload` bytes.
    pub fn with_max_payload(max_payload: usize) -> Self {
        FrameDecoder {
            buf: BytesMut::new(),
            max_payload,
        }
    }

    /// Appends received bytes.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The receive buffer, for reading from a socket straight into it
    /// (`AsyncReadExt::read_buf`) instead of going through [`FrameDecoder::push`].
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Bytes received but not yet returned as a frame.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// The next complete frame, or `None` if more bytes are needed. After an
    /// error the stream is out of sync and the connection should be dropped.
    pub fn next_frame(&mut self) -> Result<Option<Frame>> {
        if self.buf.len() < PREAMBLE_LEN {
            return Ok(None);
        }
        let header_len = Header::peek_len(&self.buf)?;
        if self.buf.len() < header_len {
            return Ok(None);
        }
        let (header, payload_len) = Header::decode(&self.buf)?;
        if payload_len > self.max_payload {
            return Err(Error::PayloadTooLarge {
                len: payload_len,
                max: self.max_payload,
            });
        }
        let total = header_len + payload_len;
        if self.buf.len() < total {
            return Ok(None);
        }
        let mut frame = self.buf.split_to(total);
        let payload = frame.split_off(header_len).freeze();
        Ok(Some(Frame { header, payload }))
    }
}

fn be_u16(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([b[at], b[at + 1]])
}

fn be_u32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Frame {
        Frame::new(
            Header::new(
                3,
                Flags::REQUEST | Flags::RAW_BINARY,
                "Device.X",
                "rbus.t.INBOX.1",
            ),
            b"payload".to_vec(),
        )
    }

    #[test]
    fn encodes_the_documented_layout() {
        let bytes = sample().encode().unwrap();
        assert_eq!(&bytes[..6], &[0xaa, 0xaa, 0x00, 0x02, 0x00, 32 + 8 + 14]);
        assert_eq!(&bytes[6..10], &3u32.to_be_bytes());
        assert_eq!(&bytes[10..14], &0x11u32.to_be_bytes());
        assert_eq!(&bytes[18..22], &7u32.to_be_bytes());
        assert_eq!(&bytes[22..26], &8u32.to_be_bytes());
        assert_eq!(&bytes[26..34], b"Device.X");
        assert_eq!(&bytes[52..54], &[0xaa, 0xaa]);
        assert_eq!(&bytes[54..], b"payload");
    }

    #[test]
    fn decoder_round_trips_byte_by_byte() {
        let frame = sample();
        let mut stream = frame.encode().unwrap().to_vec();
        let mut second = frame.clone();
        second.header.seq = 4;
        stream.extend(second.encode().unwrap());
        let mut dec = FrameDecoder::new();
        let mut got = Vec::new();
        for b in &stream {
            dec.push(std::slice::from_ref(b));
            while let Some(f) = dec.next_frame().unwrap() {
                got.push(f);
            }
        }
        assert_eq!(got, vec![frame, second]);
        assert_eq!(dec.buffered(), 0);
    }

    #[test]
    fn roundtrip_headers_decode_and_re_encode() {
        let mut frame = sample();
        frame.header.roundtrip = Some([1, 2, 3, 4, 5]);
        let bytes = frame.encode().unwrap();
        assert_eq!(
            usize::from(u16::from_be_bytes([bytes[4], bytes[5]])),
            72 + 22
        );
        let mut dec = FrameDecoder::new();
        dec.push(&bytes);
        let back = dec.next_frame().unwrap().unwrap();
        assert_eq!(back, frame);
        assert_eq!(back.encode().unwrap(), bytes);
    }

    #[test]
    fn empty_payloads_are_refused_unless_unchecked() {
        let frame = Frame::new(Header::new(1, Flags::NONE, "t", ""), Vec::new());
        assert!(matches!(frame.encode(), Err(Error::EmptyPayload)));
        let bytes = frame.encode_unchecked().unwrap();
        let mut dec = FrameDecoder::new();
        dec.push(&bytes);
        assert_eq!(dec.next_frame().unwrap().unwrap(), frame);
    }

    #[test]
    fn topic_length_limit() {
        let long = "x".repeat(MAX_TOPIC_LEN);
        let frame = Frame::new(Header::new(1, Flags::NONE, long, ""), b"x".to_vec());
        assert!(matches!(frame.encode(), Err(Error::TopicTooLong(256))));
        let ok = Frame::new(
            Header::new(1, Flags::NONE, "x".repeat(255), ""),
            b"x".to_vec(),
        );
        assert!(ok.encode().is_ok());
    }

    #[test]
    fn rejects_bad_markers_versions_and_lengths() {
        let good = sample().encode().unwrap().to_vec();
        let mut bad = good.clone();
        bad[0] = 0;
        assert!(matches!(
            Header::peek_len(&bad),
            Err(Error::BadMarker { .. })
        ));
        let mut bad = good.clone();
        bad[3] = 3;
        assert!(matches!(
            Header::peek_len(&bad),
            Err(Error::UnsupportedVersion(3))
        ));
        let mut bad = good.clone();
        bad[5] = 10;
        assert!(matches!(
            Header::peek_len(&bad),
            Err(Error::BadHeaderLength(10))
        ));
        let mut bad = good.clone();
        let end = usize::from(bad[5]) - 2;
        bad[end] = 0;
        assert!(matches!(Header::decode(&bad), Err(Error::BadMarker { .. })));
        let mut bad = good.clone();
        bad[22..26].copy_from_slice(&200u32.to_be_bytes());
        assert!(matches!(
            Header::decode(&bad),
            Err(Error::BadHeaderLength(_))
        ));
        let mut bad = good.clone();
        bad[5] += 1; // header_len that matches neither 32 nor 72 + strings
        bad.insert(52, 0);
        assert!(matches!(
            Header::decode(&bad),
            Err(Error::BadHeaderLength(_))
        ));
        assert!(matches!(
            Header::peek_len(&good[..3]),
            Err(Error::Truncated(_))
        ));
        assert!(matches!(
            Header::decode(&good[..20]),
            Err(Error::Truncated(_))
        ));
    }

    #[test]
    fn decoder_enforces_payload_limit_and_waits_for_data() {
        let bytes = sample().encode().unwrap();
        let mut dec = FrameDecoder::with_max_payload(3);
        dec.push(&bytes);
        assert!(matches!(
            dec.next_frame(),
            Err(Error::PayloadTooLarge { len: 7, max: 3 })
        ));
        let mut dec = FrameDecoder::default();
        dec.push(&bytes[..4]);
        assert!(dec.next_frame().unwrap().is_none());
        dec.push(&bytes[4..30]);
        assert!(dec.next_frame().unwrap().is_none());
        dec.push(&bytes[30..bytes.len() - 1]);
        assert!(dec.next_frame().unwrap().is_none());
        dec.push(&bytes[bytes.len() - 1..]);
        assert!(dec.next_frame().unwrap().is_some());
    }

    #[test]
    fn flags_debug_and_ops() {
        assert_eq!(format!("{:?}", Flags::NONE), "NONE");
        assert_eq!(
            format!(
                "{:?}",
                Flags::RESPONSE | Flags::UNDELIVERABLE | Flags::RAW_BINARY
            ),
            "RESPONSE | UNDELIVERABLE | RAW_BINARY"
        );
        assert_eq!(format!("{:?}", Flags(0x101)), "REQUEST | 0x100");
        assert_eq!(
            format!("{:?}", Flags::TAINTED | Flags::ENCRYPTED),
            "TAINTED | ENCRYPTED"
        );
        assert!(Flags(0x13).contains(Flags::REQUEST | Flags::RAW_BINARY));
        assert!(!Flags::REQUEST.contains(Flags::RESPONSE));
    }
}
