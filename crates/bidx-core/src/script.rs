//! ScriptPubKey classification and address-hash extraction.
//!
//! We never store encoded address strings in the hot path. Instead we store
//! (script_type, address_hash) and let the query layer render base58/bech32
/// strings when needed.
///
/// Script type codes stored in the DB (compact u8).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScriptType {
    Unknown = 0,
    /// Pay to public key: <pubkey> OP_CHECKSIG. address_hash = hash160(pubkey).
    P2PK = 1,
    /// OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG
    P2PKH = 2,
    /// OP_HASH160 <20> OP_EQUAL
    P2SH = 3,
    /// OP_0 <20>
    P2WPKH = 4,
    /// OP_0 <32>
    P2WSH = 5,
    /// OP_1 <32> (Taproot key-path). address_hash = x-only pubkey.
    P2TR = 6,
    /// OP_RETURN (unspendable, data carrier). address_hash = zeros.
    OpReturn = 7,
    /// Bare multisig m-of-n. address_hash = hash160 of full scriptPubKey.
    BareMultisig = 8,
    /// Other witness versions (OP_2..OP_16 programs). address_hash = program.
    WitnessUnknown = 9,
}

impl ScriptType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScriptType::Unknown => "unknown",
            ScriptType::P2PK => "p2pk",
            ScriptType::P2PKH => "p2pkh",
            ScriptType::P2SH => "p2sh",
            ScriptType::P2WPKH => "p2wpkh",
            ScriptType::P2WSH => "p2wsh",
            ScriptType::P2TR => "p2tr",
            ScriptType::OpReturn => "op_return",
            ScriptType::BareMultisig => "bare_multisig",
            ScriptType::WitnessUnknown => "witness_unknown",
        }
    }
}

/// Address-relevant hash, always stored as 32 bytes (right-padded with zeros).
pub type AddressHash = [u8; 32];

#[inline]
fn pad20(h: [u8; 20]) -> AddressHash {
    let mut out = [0u8; 32];
    out[..20].copy_from_slice(&h);
    out
}

#[inline]
fn pad_slice(b: &[u8]) -> AddressHash {
    let mut out = [0u8; 32];
    let n = b.len().min(32);
    out[..n].copy_from_slice(&b[..n]);
    out
}

/// Classify a scriptPubKey and extract the address-relevant hash.
/// Hot path: pure byte comparisons, no allocation.
pub fn classify_script(spk: &[u8]) -> (ScriptType, AddressHash) {
    let len = spk.len();

    // P2PKH: 76 a9 14 <20> 88 ac
    if len == 25
        && spk[0] == 0x76
        && spk[1] == 0xa9
        && spk[2] == 0x14
        && spk[23] == 0x88
        && spk[24] == 0xac
    {
        let mut h = [0u8; 20];
        h.copy_from_slice(&spk[3..23]);
        return (ScriptType::P2PKH, pad20(h));
    }

    // P2SH: a9 14 <20> 87
    if len == 23 && spk[0] == 0xa9 && spk[1] == 0x14 && spk[22] == 0x87 {
        let mut h = [0u8; 20];
        h.copy_from_slice(&spk[2..22]);
        return (ScriptType::P2SH, pad20(h));
    }

    // Segwit: OP_n <program>
    if (4..=42).contains(&len) {
        let op = spk[0];
        let push_len = spk[1] as usize;
        if push_len + 2 == len && (2..=40).contains(&push_len) {
            // OP_0
            if op == 0x00 {
                if push_len == 20 {
                    return (ScriptType::P2WPKH, pad_slice(&spk[2..22]));
                }
                if push_len == 32 {
                    return (ScriptType::P2WSH, pad_slice(&spk[2..34]));
                }
                return (ScriptType::WitnessUnknown, pad_slice(&spk[2..]));
            }
            // OP_1..OP_16
            if (0x51..=0x60).contains(&op) {
                if op == 0x51 && push_len == 32 {
                    return (ScriptType::P2TR, pad_slice(&spk[2..34]));
                }
                return (ScriptType::WitnessUnknown, pad_slice(&spk[2..]));
            }
        }
    }

    // OP_RETURN anywhere at the start
    if len >= 1 && spk[0] == 0x6a {
        return (ScriptType::OpReturn, [0u8; 32]);
    }

    // P2PK: <33|65 pubkey> OP_CHECKSIG
    if (len == 35 && spk[0] == 33 && spk[34] == 0xac)
        || (len == 67 && spk[0] == 65 && spk[66] == 0xac)
    {
        let pk_len = spk[0] as usize;
        let pk = &spk[1..1 + pk_len];
        // Compressed keys start 02/03, uncompressed start 04.
        if matches!(pk[0], 0x02..=0x04) {
            return (ScriptType::P2PK, pad20(crate::hash::hash160(pk)));
        }
    }

    // Bare multisig: OP_m <pubkeys...> OP_n OP_CHECKMULTISIG
    if is_bare_multisig(spk) {
        return (
            ScriptType::BareMultisig,
            pad20(crate::hash::hash160(spk)),
        );
    }

    (ScriptType::Unknown, [0u8; 32])
}

fn is_bare_multisig(spk: &[u8]) -> bool {
    let len = spk.len();
    if len < 3 || spk[len - 1] != 0xae {
        return false;
    }
    let m = spk[0];
    let n = spk[len - 2];
    if !(0x51..=0x60).contains(&m) || !(0x51..=0x60).contains(&n) {
        return false;
    }
    // m <= n and both between 1 and 16
    let (m, n) = (m - 0x50, n - 0x50);
    if m == 0 || n == 0 || m > n {
        return false;
    }
    // Walk the pubkey pushes.
    let mut i = 1;
    let mut count = 0u8;
    while i < len - 2 {
        let push = spk[i] as usize;
        if push != 33 && push != 65 {
            return false;
        }
        if i + 1 + push > len - 2 {
            return false;
        }
        count += 1;
        i += 1 + push;
    }
    i == len - 2 && count == n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_p2pkh() {
        // Standard P2PKH: OP_DUP OP_HASH160 0x14 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
        let mut spk = vec![0x76, 0xa9, 0x14];
        spk.extend_from_slice(&[0xab; 20]);
        spk.extend_from_slice(&[0x88, 0xac]);
        let (t, h) = classify_script(&spk);
        assert_eq!(t, ScriptType::P2PKH);
        assert_eq!(&h[..20], &[0xab; 20]);
        assert_eq!(&h[20..], &[0u8; 12]);
    }

    #[test]
    fn classifies_p2sh() {
        let mut spk = vec![0xa9, 0x14];
        spk.extend_from_slice(&[0xcd; 20]);
        spk.push(0x87);
        let (t, h) = classify_script(&spk);
        assert_eq!(t, ScriptType::P2SH);
        assert_eq!(&h[..20], &[0xcd; 20]);
    }

    #[test]
    fn classifies_segwit() {
        let mut wp = vec![0x00, 0x14];
        wp.extend_from_slice(&[0x11; 20]);
        assert_eq!(classify_script(&wp).0, ScriptType::P2WPKH);

        let mut ws = vec![0x00, 0x20];
        ws.extend_from_slice(&[0x22; 32]);
        assert_eq!(classify_script(&ws).0, ScriptType::P2WSH);

        let mut tr = vec![0x51, 0x20];
        tr.extend_from_slice(&[0x33; 32]);
        assert_eq!(classify_script(&tr).0, ScriptType::P2TR);
        assert_eq!(&classify_script(&tr).1[..32], &[0x33; 32]);
    }

    #[test]
    fn classifies_op_return_and_multisig() {
        assert_eq!(classify_script(&[0x6a, 0x04, 1, 2, 3, 4]).0, ScriptType::OpReturn);

        // 1-of-2 bare multisig with two compressed keys
        let mut ms = vec![0x51, 0x21];
        ms.extend_from_slice(&[0x02; 33]);
        ms.push(0x21);
        ms.extend_from_slice(&[0x03; 33]);
        ms.extend_from_slice(&[0x52, 0xae]);
        assert_eq!(classify_script(&ms).0, ScriptType::BareMultisig);
    }

    #[test]
    fn genesis_coinbase_is_p2pk() {
        // Genesis block coinbase scriptPubKey: 65-byte pubkey + OP_CHECKSIG
        let mut spk = vec![0x41];
        spk.extend_from_slice(&hex::decode("04678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5f").unwrap());
        spk.push(0xac);
        let (t, h) = classify_script(&spk);
        assert_eq!(t, ScriptType::P2PK);
        // hash160 of the genesis pubkey
        assert_eq!(&h[..20], &hex::decode("62e907b15cbf27d5425399ebf6f0fb50ebb88f18").unwrap()[..]);
    }
}
