use byteorder::{LittleEndian, ReadBytesExt};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("unexpected end of data at offset {offset}: needed {needed} bytes, have {remaining}")]
    Eof {
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    #[error("invalid varint at offset {0}")]
    BadVarInt(usize),
    #[error("count {0} exceeds sanity limit at offset {1}")]
    CountOverflow(u64, usize),
}

/// Zero-copy cursor over a byte slice for Bitcoin wire-format parsing.
/// All reads are bounds-checked; performance-critical paths use fixed-size
/// reads that the compiler can vectorize.
pub struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

// Sanity limit: a block can contain at most ~4M weight units; a transaction
// count above a few hundred thousand per block is impossible on mainnet.
const MAX_ITEMS: u64 = 4_000_000;

impl<'a> Cursor<'a> {
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        Cursor { data, pos: 0 }
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    #[inline]
    fn need(&self, n: usize) -> Result<(), ParseError> {
        if self.remaining() < n {
            return Err(ParseError::Eof {
                offset: self.pos,
                needed: n,
                remaining: self.remaining(),
            });
        }
        Ok(())
    }

    #[inline]
    pub fn read_u8(&mut self) -> Result<u8, ParseError> {
        self.need(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    #[inline]
    pub fn read_u16(&mut self) -> Result<u16, ParseError> {
        self.need(2)?;
        let mut s = &self.data[self.pos..self.pos + 2];
        self.pos += 2;
        Ok(s.read_u16::<LittleEndian>().unwrap())
    }

    #[inline]
    pub fn read_u32(&mut self) -> Result<u32, ParseError> {
        self.need(4)?;
        let mut s = &self.data[self.pos..self.pos + 4];
        self.pos += 4;
        Ok(s.read_u32::<LittleEndian>().unwrap())
    }

    #[inline]
    pub fn read_i32(&mut self) -> Result<i32, ParseError> {
        self.need(4)?;
        let mut s = &self.data[self.pos..self.pos + 4];
        self.pos += 4;
        Ok(s.read_i32::<LittleEndian>().unwrap())
    }

    #[inline]
    pub fn read_u64(&mut self) -> Result<u64, ParseError> {
        self.need(8)?;
        let mut s = &self.data[self.pos..self.pos + 8];
        self.pos += 8;
        Ok(s.read_u64::<LittleEndian>().unwrap())
    }

    #[inline]
    pub fn read_i64(&mut self) -> Result<i64, ParseError> {
        self.need(8)?;
        let mut s = &self.data[self.pos..self.pos + 8];
        self.pos += 8;
        Ok(s.read_i64::<LittleEndian>().unwrap())
    }

    #[inline]
    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], ParseError> {
        self.need(n)?;
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    #[inline]
    pub fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ParseError> {
        self.need(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(&self.data[self.pos..self.pos + N]);
        self.pos += N;
        Ok(out)
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> Result<(), ParseError> {
        self.need(n)?;
        self.pos += n;
        Ok(())
    }

    /// Bitcoin compact-size (varint).
    pub fn read_varint(&mut self) -> Result<u64, ParseError> {
        let start = self.pos;
        let first = self.read_u8()?;
        match first {
            0x00..=0xfc => Ok(first as u64),
            0xfd => Ok(self.read_u16()? as u64),
            0xfe => Ok(self.read_u32()? as u64),
            0xff => self.read_u64().map_err(|_| ParseError::BadVarInt(start)),
        }
    }

    /// Read a varint representing a count and enforce a sanity bound so a
    /// corrupt file can't trigger absurd allocations.
    #[inline]
    pub fn read_count(&mut self) -> Result<u64, ParseError> {
        let n = self.read_varint()?;
        if n > MAX_ITEMS {
            return Err(ParseError::CountOverflow(n, self.pos));
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_encodings() {
        let cases: &[(&[u8], u64)] = &[
            (&[0x00], 0),
            (&[0xfc], 252),
            (&[0xfd, 0xfd, 0x00], 253),
            (&[0xfd, 0xff, 0xff], 65535),
            (&[0xfe, 0x00, 0x00, 0x01, 0x00], 65536),
            (&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], u64::MAX),
        ];
        for (bytes, want) in cases {
            let mut c = Cursor::new(bytes);
            assert_eq!(c.read_varint().unwrap(), *want, "case {:?}", bytes);
        }
    }

    #[test]
    fn eof_errors() {
        let mut c = Cursor::new(&[0x01, 0x02]);
        assert!(c.read_u32().is_err());
        let mut c = Cursor::new(&[]);
        assert!(c.read_u8().is_err());
    }

    #[test]
    fn count_overflow_rejected() {
        let mut c = Cursor::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(c.read_count(), Err(ParseError::CountOverflow(..))));
        // Boundary: exactly at the limit passes.
        let mut buf = vec![0xff];
        buf.extend_from_slice(&4_000_000u64.to_le_bytes());
        let mut c = Cursor::new(&buf);
        assert_eq!(c.read_count().unwrap(), 4_000_000);
    }

    #[test]
    fn read_u16_u32_u64_le() {
        let mut c = Cursor::new(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        assert_eq!(c.read_u16().unwrap(), 0x0201);
        assert_eq!(c.read_u32().unwrap(), 0x06050403);
        assert_eq!(c.pos(), 6);
    }

    #[test]
    fn read_i32_i64_le() {
        let b32 = (-1i32).to_le_bytes();
        let mut c = Cursor::new(&b32);
        assert_eq!(c.read_i32().unwrap(), -1);
        let b64 = (-2i64).to_le_bytes();
        let mut c = Cursor::new(&b64);
        assert_eq!(c.read_i64().unwrap(), -2);
    }

    #[test]
    fn read_bytes_and_array() {
        let mut c = Cursor::new(&[9u8; 8]);
        let b: &[u8] = c.read_bytes(4).unwrap();
        assert_eq!(b, &[9u8; 4]);
        let arr: [u8; 4] = c.read_array().unwrap();
        assert_eq!(arr, [9u8; 4]);
        assert!(c.is_empty());
        assert!(matches!(c.read_u8(), Err(ParseError::Eof { .. })));
    }

    #[test]
    fn skip_advances_and_reports_eof() {
        let mut c = Cursor::new(&[0u8; 8]);
        c.skip(8).unwrap();
        assert!(c.is_empty());
        assert!(c.skip(1).is_err());
    }

    #[test]
    fn remaining_empty_pos_track_pos() {
        let mut c = Cursor::new(&[0u8; 10]);
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());
        assert_eq!(c.pos(), 0);
        c.skip(3).unwrap();
        assert_eq!(c.remaining(), 7);
        assert_eq!(c.pos(), 3);
    }

    #[test]
    fn eof_carries_offset_and_counts() {
        let mut c = Cursor::new(&[0u8; 1]);
        let e = c.read_u64().unwrap_err();
        match e {
            ParseError::Eof { offset, needed, remaining } => {
                assert_eq!(offset, 0);
                assert_eq!(needed, 8);
                assert_eq!(remaining, 1);
            }
            _ => panic!("wrong error variant"),
        }
    }
}
