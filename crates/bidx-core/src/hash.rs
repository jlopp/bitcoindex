use ripemd::Ripemd160;
use sha2::{Digest, Sha256};

/// A 32-byte hash. Stored internally in "natural" (little-endian) byte order,
/// which is exactly how hashes appear serialized in the Bitcoin wire format.
/// Conversion to the conventional big-endian display hex happens only at the
/// presentation boundary via [`Hash32::to_hex`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash32(pub [u8; 32]);

impl Default for Hash32 {
    fn default() -> Self {
        Hash32::ZERO
    }
}

impl Hash32 {
    pub const ZERO: Hash32 = Hash32([0u8; 32]);

    #[inline]
    pub fn from_bytes(b: [u8; 32]) -> Self {
        Hash32(b)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parse from conventional big-endian hex display format (as shown by
    /// block explorers). Reverses into internal little-endian order.
    pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
        let mut b = [0u8; 32];
        hex::decode_to_slice(s, &mut b)?;
        b.reverse();
        Ok(Hash32(b))
    }

    /// Conventional big-endian hex display format.
    pub fn to_hex(&self) -> String {
        let mut b = self.0;
        b.reverse();
        hex::encode(b)
    }

    /// Raw little-endian bytes hex (serialization order). Useful for debugging.
    pub fn to_raw_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash32({})", self.to_hex())
    }
}

impl std::fmt::Display for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl serde::Serialize for Hash32 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> serde::Deserialize<'de> for Hash32 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Hash32::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

#[inline]
pub fn dsha256(data: &[u8]) -> Hash32 {
    let h1 = Sha256::digest(data);
    let h2 = Sha256::digest(h1);
    let mut out = [0u8; 32];
    out.copy_from_slice(&h2);
    Hash32(out)
}

#[inline]
pub fn hash160(data: &[u8]) -> [u8; 20] {
    let h1 = Sha256::digest(data);
    let h2 = Ripemd160::digest(h1);
    let mut out = [0u8; 20];
    out.copy_from_slice(&h2);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        // Genesis block hash (display form).
        let hex = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
        let h = Hash32::from_hex(hex).unwrap();
        assert_eq!(h.to_hex(), hex);
    }

    #[test]
    fn dsha256_known() {
        // sha256d("") raw digest bytes (matches Python hashlib .hex()):
        //   5df6e0e2...4c9456
        // Our Hash32 stores exactly these bytes; to_hex() reverses them into
        // the "display" convention (the same reversal Bitcoin applies to
        // block/tx wire hashes when showing them big-endian).
        let h = dsha256(b"");
        assert_eq!(
            h.to_raw_hex(),
            "5df6e0e2761359d30a8275058e299fcc0381534545f55cf43e41983f5d4c9456"
        );
        assert_eq!(
            h.to_hex(),
            "56944c5d3f98413ef45cf54545538103cc9f298e0575820ad3591376e2e0f65d"
        );
    }

    #[test]
    fn hash160_known() {
        // hash160("") = b472a266d0bd89c13706a4132ccfb16f7c3b9fcb (well-known empty hash160)
        let h = hash160(b"");
        assert_eq!(hex::encode(h), "b472a266d0bd89c13706a4132ccfb16f7c3b9fcb");
    }

    #[test]
    fn default_is_zero_and_debug_display_format() {
        let h = Hash32::default();
        assert_eq!(*h.as_bytes(), [0u8; 32]);
        assert_eq!(format!("{:?}", h), format!("Hash32({})", h.to_hex()));
        assert_eq!(format!("{}", h), h.to_hex());
        assert_eq!(Hash32::ZERO, Hash32::default());
        assert_eq!(h, Hash32::from_bytes(*h.as_bytes()));
    }

    #[test]
    fn raw_hex_is_unreversed_and_display_is_reversed() {
        // Distinct bytes so reversal is observable.
        let mut b = [0u8; 32];
        for (i, b) in b.iter_mut().enumerate() {
            *b = i as u8;
        }
        let h = Hash32::from_bytes(b);
        assert_eq!(
            h.to_raw_hex(),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
        assert_eq!(
            h.to_hex(),
            "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"
        );
    }

    #[test]
    fn from_hex_rejects_bad_input() {
        // Odd length, non-hex, wrong length all must error (we don't silently
        // zero-pad — a hex parsing bug silently shifts every hash).
        assert!(Hash32::from_hex("abc").is_err());
        assert!(Hash32::from_hex("zz").is_err());
        assert!(Hash32::from_hex(&"00".repeat(31)).is_err());
        assert!(Hash32::from_hex(&"00".repeat(33)).is_err());
    }

    #[test]
    fn ordering_and_hash_trait_stable() {
        let a = Hash32::from_bytes([1u8; 32]);
        let b = Hash32::from_bytes([2u8; 32]);
        assert!(a < b);
        let mut set = std::collections::HashSet::new();
        set.insert(a);
        assert!(set.contains(&a));
        assert!(!set.contains(&b));
    }

    #[test]
    fn serde_json_roundtrip() {
        let h = Hash32::from_bytes([0x5A; 32]);
        let s = serde_json::to_string(&h).unwrap();
        assert_eq!(s, format!("\"{}\"", h.to_hex()));
        let back: Hash32 = serde_json::from_str(&s).unwrap();
        assert_eq!(back, h);
        assert!(serde_json::from_str::<Hash32>("123").is_err());
        assert!(serde_json::from_str::<Hash32>("\"zz\"").is_err());
        assert!(serde_json::from_str::<Hash32>(&format!("\"{}\"", "00".repeat(31))).is_err());
    }
}
