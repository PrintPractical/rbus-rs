//! The positional msgpack stream used by rbus payloads.
//!
//! An rbus payload is a sequence of msgpack items read in a fixed order, with no
//! keys. It ends with a *meta section* (method name or event name, trace context)
//! whose start offset is stored in the last five bytes as a msgpack int32 in its
//! 5-byte form: `0xd2` followed by a big-endian `u32`.
//!
//! Item encoding is done by [`rmp`], whose choices match msgpack-c byte for byte
//! (smallest integer form, with unsigned markers for positive values; `str8` for
//! strings of 32–255 bytes; `bin8/16/32`; float64). This module adds only the
//! rbus conventions on top:
//!
//! - strings are `str` items that **include the trailing NUL**, and are read as
//!   C strings (up to the first NUL);
//! - the meta-section trailer;
//! - C-style truncating `int32_t` reads, and float32 accepted where float64 is
//!   expected.

use bytes::Bytes;
use rmp::Marker;
use rmp::decode::{self, NumValueReadError, ValueReadError};
use rmp::encode;

use crate::error::{Error, Result};
use crate::limits::Limits;

const META_TRAILER_LEN: usize = 5;

/// Builds an rbus payload.
#[derive(Debug, Default, Clone)]
pub struct Writer {
    buf: Vec<u8>,
}

// Writing into a Vec<u8> cannot fail; rmp's Result is only for io::Write sinks.
const VEC_WRITE: &str = "writing msgpack to a Vec<u8> cannot fail";

impl Writer {
    /// An empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes written so far.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Consumes the writer, returning the bytes without a meta section. Used for
    /// nested messages such as the subscription payload.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Writes the meta section with `meta` and returns the finished payload.
    pub fn finish_with_meta(mut self, meta: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let offset = self.buf.len();
        meta(&mut self);
        // msgpack-c packs `offset | 0x80000000` to force the 5-byte int32 form,
        // then clears bit 31 again; the result is 0xd2 + the plain offset.
        self.buf.push(Marker::I32.to_u8());
        self.buf
            .extend_from_slice(&(offset as u32 & 0x7fff_ffff).to_be_bytes());
        self.buf
    }

    /// Writes an integer in msgpack-c's smallest form (`msgpack_pack_int64`;
    /// identical to `msgpack_pack_int32` for values in `i32` range).
    pub fn int(&mut self, v: i64) {
        encode::write_sint(&mut self.buf, v).expect(VEC_WRITE);
    }

    /// Writes a C `int32_t` (`rbusMessage_SetInt32`).
    pub fn i32(&mut self, v: i32) {
        self.int(i64::from(v));
    }

    /// Writes a C `int64_t` (`rbusMessage_SetInt64`).
    pub fn i64(&mut self, v: i64) {
        self.int(v);
    }

    /// Writes a float64.
    pub fn f64(&mut self, v: f64) {
        encode::write_f64(&mut self.buf, v).expect(VEC_WRITE);
    }

    /// Writes a string as a msgpack `str` with a trailing NUL, as C does.
    pub fn str(&mut self, s: &str) {
        encode::write_str_len(&mut self.buf, (s.len() + 1) as u32).expect(VEC_WRITE);
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
    }

    /// Writes a msgpack `bin`.
    pub fn bin(&mut self, data: &[u8]) {
        encode::write_bin(&mut self.buf, data).expect(VEC_WRITE);
    }

    /// Writes a C boolean flag as int32 0 or 1.
    pub fn flag(&mut self, v: bool) {
        self.i32(i32::from(v));
    }
}

/// Reads an rbus payload item by item. A failed read does not consume input.
///
/// A reader made with [`Reader::from_bytes`] hands out `bin` items as
/// [`Bytes`] slices of the original buffer, without copying.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    source: Option<&'a Bytes>,
    limits: Limits,
    depth: u32,
    items: usize,
}

impl<'a> Reader<'a> {
    /// Reads the whole of `buf` (no meta section), with the default
    /// [`Limits`].
    pub fn new(buf: &'a [u8]) -> Self {
        Reader {
            buf,
            pos: 0,
            source: None,
            limits: Limits::DEFAULT,
            depth: 0,
            items: 0,
        }
    }

    /// Reads the whole of `bytes`; `bin` items become zero-copy slices of it.
    pub fn from_bytes(bytes: &'a Bytes) -> Self {
        Reader {
            source: Some(bytes),
            ..Reader::new(bytes)
        }
    }

    /// Decodes under `limits` instead of the defaults.
    pub fn with_limits(mut self, limits: &Limits) -> Self {
        self.limits = *limits;
        self
    }

    fn sub(&self, buf: &'a [u8]) -> Reader<'a> {
        Reader {
            buf,
            pos: 0,
            source: self.source,
            limits: self.limits,
            depth: 0,
            items: 0,
        }
    }

    /// Runs `f` one nesting level deeper: for the containers that recurse
    /// (objects, property lists, filters). Fails past
    /// [`Limits::max_depth`] instead of overflowing the stack.
    pub fn nested<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.depth >= self.limits.max_depth {
            return Err(Error::LimitExceeded {
                what: "nesting depth",
                max: self.limits.max_depth as usize,
            });
        }
        self.depth += 1;
        let out = f(self);
        self.depth -= 1;
        out
    }

    /// Reads an `int32` item count, as C does before a list. A negative
    /// count reads as zero, as C's loops do. The count is charged against
    /// [`Limits::max_items`], and can't exceed the bytes left, since every
    /// item takes at least one.
    pub fn count(&mut self, what: &'static str) -> Result<usize> {
        let n = usize::try_from(self.i32()?).unwrap_or(0);
        if n > self.buf.len() - self.pos {
            return Err(Error::Truncated(what));
        }
        self.items = self
            .items
            .checked_add(n)
            .filter(|&total| total <= self.limits.max_items)
            .ok_or(Error::LimitExceeded {
                what: "item count",
                max: self.limits.max_items,
            })?;
        Ok(n)
    }

    /// Splits this reader's whole input into its body and its meta section,
    /// using the trailer.
    pub fn split_meta(self) -> Result<(Reader<'a>, Reader<'a>)> {
        let payload = self.buf;
        let len = payload.len();
        if len < META_TRAILER_LEN || payload[len - META_TRAILER_LEN] != Marker::I32.to_u8() {
            return Err(Error::BadMetaTrailer);
        }
        let t = &payload[len - 4..];
        let offset = u32::from_be_bytes([t[0], t[1], t[2], t[3]]) as usize;
        let meta_end = len - META_TRAILER_LEN;
        if offset > meta_end {
            return Err(Error::BadMetaTrailer);
        }
        Ok((
            self.sub(&payload[..offset]),
            self.sub(&payload[offset..meta_end]),
        ))
    }

    /// True when every item has been read.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    /// Runs one rmp read against the unread input, advancing only on success.
    fn read<T>(
        &mut self,
        expected: &'static str,
        f: impl FnOnce(&mut &'a [u8]) -> std::result::Result<T, ReadFailure>,
    ) -> Result<T> {
        let mut rest = &self.buf[self.pos..];
        let before = rest.len();
        match f(&mut rest) {
            Ok(v) => {
                self.pos += before - rest.len();
                Ok(v)
            }
            Err(ReadFailure::Type) => Err(Error::UnexpectedType {
                expected,
                found: self.buf[self.pos],
            }),
            Err(ReadFailure::Eof) => Err(Error::Truncated(expected)),
        }
    }

    /// Reads any msgpack integer as `i64`. `uint64` values above `i64::MAX` are
    /// reinterpreted, as msgpack-c's `via.i64` does.
    pub fn int(&mut self) -> Result<i64> {
        self.read("integer", |rd| {
            let v: i128 = decode::read_int(rd).map_err(ReadFailure::from)?;
            Ok(v as i64)
        })
    }

    /// Reads a C `int32_t`, truncating as `(int32_t)via.i64` does.
    pub fn i32(&mut self) -> Result<i32> {
        Ok(self.int()? as i32)
    }

    /// Reads a C `int64_t`.
    pub fn i64(&mut self) -> Result<i64> {
        self.int()
    }

    /// Reads a C boolean flag (any non-zero int32 is true).
    pub fn flag(&mut self) -> Result<bool> {
        Ok(self.i32()? != 0)
    }

    /// Reads a float (float64, or float32 widened).
    pub fn f64(&mut self) -> Result<f64> {
        let marker = self.buf.get(self.pos).map(|&b| Marker::from_u8(b));
        self.read("float", |rd| match marker {
            Some(Marker::F32) => Ok(f64::from(decode::read_f32(rd).map_err(ReadFailure::from)?)),
            Some(Marker::F64) => decode::read_f64(rd).map_err(ReadFailure::from),
            Some(_) => Err(ReadFailure::Type),
            None => Err(ReadFailure::Eof),
        })
    }

    /// Reads a msgpack `str` as the C string it carries: everything before the
    /// first NUL. Invalid UTF-8 is replaced, as the C side has no encoding.
    pub fn str(&mut self) -> Result<String> {
        let bytes = self.read("string", |rd| {
            let len = decode::read_str_len(rd).map_err(ReadFailure::from)? as usize;
            take(rd, len)
        })?;
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        Ok(String::from_utf8_lossy(&bytes[..end]).into_owned())
    }

    /// Reads a msgpack `bin`.
    pub fn bin(&mut self) -> Result<&'a [u8]> {
        self.read("binary", |rd| {
            let len = decode::read_bin_len(rd).map_err(ReadFailure::from)? as usize;
            take(rd, len)
        })
    }

    /// Reads a msgpack `bin` as [`Bytes`]: a zero-copy slice when the reader was
    /// made with [`Reader::from_bytes`], a copy otherwise.
    pub fn bin_bytes(&mut self) -> Result<Bytes> {
        let slice = self.bin()?;
        Ok(match self.source {
            Some(src) => src.slice_ref(slice),
            None => Bytes::copy_from_slice(slice),
        })
    }
}

fn take<'a>(rd: &mut &'a [u8], len: usize) -> std::result::Result<&'a [u8], ReadFailure> {
    if rd.len() < len {
        return Err(ReadFailure::Eof);
    }
    let (head, tail) = rd.split_at(len);
    *rd = tail;
    Ok(head)
}

/// Why an rmp read failed, reduced to what callers can act on.
enum ReadFailure {
    /// The item had a different msgpack type.
    Type,
    /// The input ended mid-item.
    Eof,
}

impl<E: decode::RmpReadErr> From<ValueReadError<E>> for ReadFailure {
    fn from(e: ValueReadError<E>) -> Self {
        match e {
            ValueReadError::TypeMismatch(_) => ReadFailure::Type,
            ValueReadError::InvalidMarkerRead(_) | ValueReadError::InvalidDataRead(_) => {
                ReadFailure::Eof
            }
        }
    }
}

impl<E: decode::RmpReadErr> From<NumValueReadError<E>> for ReadFailure {
    fn from(e: NumValueReadError<E>) -> Self {
        match e {
            NumValueReadError::InvalidMarkerRead(_) | NumValueReadError::InvalidDataRead(_) => {
                ReadFailure::Eof
            }
            // An i128 holds every msgpack integer, so OutOfRange cannot happen.
            NumValueReadError::TypeMismatch(_) | NumValueReadError::OutOfRange => ReadFailure::Type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_bytes(v: i64) -> Vec<u8> {
        let mut w = Writer::new();
        w.int(v);
        w.into_bytes()
    }

    #[test]
    fn integers_use_msgpack_c_forms() {
        let cases: &[(i64, &[u8])] = &[
            (0, &[0x00]),
            (127, &[0x7f]),
            (128, &[0xcc, 0x80]),
            (255, &[0xcc, 0xff]),
            (256, &[0xcd, 0x01, 0x00]),
            (1287, &[0xcd, 0x05, 0x07]),
            (65535, &[0xcd, 0xff, 0xff]),
            (65536, &[0xce, 0x00, 0x01, 0x00, 0x00]),
            (1 << 32, &[0xcf, 0, 0, 0, 1, 0, 0, 0, 0]),
            (-1, &[0xff]),
            (-32, &[0xe0]),
            (-33, &[0xd0, 0xdf]),
            (-128, &[0xd0, 0x80]),
            (-129, &[0xd1, 0xff, 0x7f]),
            (-32768, &[0xd1, 0x80, 0x00]),
            (-32769, &[0xd2, 0xff, 0xff, 0x7f, 0xff]),
            (i64::from(i32::MIN), &[0xd2, 0x80, 0, 0, 0]),
            (
                i64::from(i32::MIN) - 1,
                &[0xd3, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff],
            ),
        ];
        for (v, want) in cases {
            assert_eq!(int_bytes(*v), *want, "encoding {v}");
            assert_eq!(Reader::new(want).int().unwrap(), *v, "decoding {v}");
        }
    }

    #[test]
    fn uint64_reinterprets_as_i64() {
        let bytes = [0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(Reader::new(&bytes).int().unwrap(), -1);
        assert_eq!(
            Reader::new(&[0xce, 0xff, 0xff, 0xff, 0xff]).i32().unwrap(),
            -1
        );
    }

    #[test]
    fn strings_carry_a_trailing_nul() {
        let mut w = Writer::new();
        w.str("");
        w.str("abc");
        w.str(&"x".repeat(31));
        w.str(&"y".repeat(300));
        let b = w.into_bytes();
        assert_eq!(&b[..2], &[0xa1, 0x00]);
        assert_eq!(&b[2..7], &[0xa4, b'a', b'b', b'c', 0]);
        assert_eq!(&b[7..9], &[0xd9, 32]);
        assert_eq!(&b[41..44], &[0xda, 0x01, 0x2d]);
        let mut r = Reader::new(&b);
        assert_eq!(r.str().unwrap(), "");
        assert_eq!(r.str().unwrap(), "abc");
        assert_eq!(r.str().unwrap(), "x".repeat(31));
        assert_eq!(r.str().unwrap(), "y".repeat(300));
        assert!(r.is_empty());
    }

    #[test]
    fn strings_stop_at_the_first_nul_and_accept_missing_nul() {
        let mut r = Reader::new(&[
            0xa3, b'a', 0, b'b', 0xa2, b'h', b'i', 0xdb, 0, 0, 0, 1, b'z',
        ]);
        assert_eq!(r.str().unwrap(), "a");
        assert_eq!(r.str().unwrap(), "hi");
        assert_eq!(r.str().unwrap(), "z");
    }

    #[test]
    fn bin_lengths() {
        for len in [0usize, 255, 256, 65535, 65536] {
            let data = vec![7u8; len];
            let mut w = Writer::new();
            w.bin(&data);
            let b = w.into_bytes();
            let want_tag = match len {
                0..=255 => 0xc4,
                256..=65535 => 0xc5,
                _ => 0xc6,
            };
            assert_eq!(b[0], want_tag);
            assert_eq!(Reader::new(&b).bin().unwrap(), &data[..]);
        }
    }

    #[test]
    fn floats() {
        let mut w = Writer::new();
        w.f64(3.5);
        let b = w.into_bytes();
        assert_eq!(b[0], 0xcb);
        assert_eq!(Reader::new(&b).f64().unwrap(), 3.5);
        let f32_bytes = [0xca, 0x40, 0x20, 0x00, 0x00];
        assert_eq!(Reader::new(&f32_bytes).f64().unwrap(), 2.5);
    }

    #[test]
    fn meta_trailer_round_trip() {
        let mut w = Writer::new();
        w.i32(7);
        w.str("body");
        let payload = w.finish_with_meta(|m| {
            m.str("METHOD_RPC");
            m.str("");
        });
        let t = &payload[payload.len() - 5..];
        assert_eq!(t, &[0xd2, 0, 0, 0, 7]);
        let (mut body, mut meta) = Reader::new(&payload).split_meta().unwrap();
        assert_eq!(body.i32().unwrap(), 7);
        assert_eq!(body.str().unwrap(), "body");
        assert!(body.is_empty());
        assert_eq!(meta.str().unwrap(), "METHOD_RPC");
        assert_eq!(meta.str().unwrap(), "");
        assert!(meta.is_empty());
    }

    #[test]
    fn bad_meta_trailers() {
        for bad in [
            &[1u8, 2][..],
            &[0, 0xd3, 0, 0, 0, 0],
            &[0, 0xd2, 0, 0, 0, 9],
        ] {
            assert!(matches!(
                Reader::new(bad).split_meta(),
                Err(Error::BadMetaTrailer)
            ));
        }
    }

    #[test]
    fn type_mismatches_and_truncation_are_errors() {
        assert!(matches!(
            Reader::new(&[0xa1, 0]).int(),
            Err(Error::UnexpectedType {
                expected: "integer",
                found: 0xa1
            })
        ));
        assert!(matches!(
            Reader::new(&[0x01]).str(),
            Err(Error::UnexpectedType { .. })
        ));
        assert!(matches!(
            Reader::new(&[0x01]).bin(),
            Err(Error::UnexpectedType { .. })
        ));
        assert!(matches!(
            Reader::new(&[0x01]).f64(),
            Err(Error::UnexpectedType { .. })
        ));
        assert!(matches!(Reader::new(&[]).int(), Err(Error::Truncated(_))));
        assert!(matches!(
            Reader::new(&[0xcd, 0x01]).int(),
            Err(Error::Truncated(_))
        ));
        assert!(matches!(
            Reader::new(&[0xa5, b'a']).str(),
            Err(Error::Truncated(_))
        ));
        assert!(matches!(
            Reader::new(&[0xc4, 3, 1]).bin(),
            Err(Error::Truncated(_))
        ));
        let mut r = Reader::new(&[0xa1, 0]);
        assert!(r.int().is_err());
        assert_eq!(
            r.str().unwrap(),
            "",
            "a failed read does not consume the item"
        );
    }

    #[test]
    fn bin_bytes_is_zero_copy_from_bytes() {
        let mut w = Writer::new();
        w.bin(b"blob");
        let src = Bytes::from(w.into_bytes());
        let got = Reader::from_bytes(&src).bin_bytes().unwrap();
        assert_eq!(&got[..], b"blob");
        assert_eq!(got.as_ptr(), src[2..].as_ptr(), "shares the source buffer");
        let copied = Reader::new(&src).bin_bytes().unwrap();
        assert_eq!(copied, got);
    }

    #[test]
    fn flags() {
        let mut w = Writer::new();
        w.flag(true);
        w.flag(false);
        w.i64(5);
        let b = w.into_bytes();
        let mut r = Reader::new(&b);
        assert!(r.flag().unwrap());
        assert!(!r.flag().unwrap());
        assert_eq!(r.i64().unwrap(), 5);
    }
}
