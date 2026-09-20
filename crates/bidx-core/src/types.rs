use crate::hash::Hash32;
use serde::{Deserialize, Serialize};

/// Magic prefix preceding each block record in blk*.dat files (mainnet).
// Magic values are stored as the little-endian u32 read from the wire.
// Bitcoin source lists them in reverse (endianness) order; e.g. testnet4's
// chainparams constant 0x1c163f28 appears on disk as bytes 1c 16 3f 28,
// which u32::from_le_bytes reads as 0x283f161c.
pub const BLOCK_MAGIC: u32 = 0xD9B4_BEF9; // mainnet
pub const BLOCK_MAGIC_TESTNET3: u32 = 0x0709_110B;
pub const BLOCK_MAGIC_SIGNET: u32 = 0x40CF_030A;
pub const BLOCK_MAGIC_TESTNET4: u32 = 0x283F_161C;
pub const BLOCK_MAGIC_REGTEST: u32 = 0xDAB5_BFFA;

/// Returns true if `magic` is the wire magic of any known Bitcoin network.
/// The indexer does not validate consensus, so parsing testnet/signet/regtest
/// files is supported (useful for benchmarking & development).
pub fn is_known_network_magic(magic: u32) -> bool {
    matches!(
        magic,
        BLOCK_MAGIC
            | BLOCK_MAGIC_TESTNET3
            | BLOCK_MAGIC_SIGNET
            | BLOCK_MAGIC_TESTNET4
            | BLOCK_MAGIC_REGTEST
    )
}
pub const BLOCK_HEADER_LEN: usize = 80;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BlockHeader {
    pub version: i32,
    pub prev_hash: Hash32,
    pub merkle_root: Hash32,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
}

impl BlockHeader {
    /// Parse the 80-byte header. `raw` must be exactly the header bytes;
    /// the block hash is computed by the caller as dsha256(raw).
    pub fn parse(raw: &[u8; BLOCK_HEADER_LEN]) -> Self {
        use byteorder::{LittleEndian as LE, ReadBytesExt};
        let mut c = &raw[..];
        let version = c.read_i32::<LE>().unwrap();
        let mut prev = [0u8; 32];
        prev.copy_from_slice(&c[..32]);
        c = &c[32..];
        let mut merkle = [0u8; 32];
        merkle.copy_from_slice(&c[..32]);
        c = &c[32..];
        let time = c.read_u32::<LE>().unwrap();
        let bits = c.read_u32::<LE>().unwrap();
        let nonce = c.read_u32::<LE>().unwrap();
        BlockHeader {
            version,
            prev_hash: Hash32(prev),
            merkle_root: Hash32(merkle),
            time,
            bits,
            nonce,
        }
    }

    #[inline]
    pub fn hash(&self, raw: &[u8; BLOCK_HEADER_LEN]) -> Hash32 {
        crate::hash::dsha256(raw)
    }
}

/// A block's position inside a blk*.dat file. Used for O(1) re-reads
/// during the full-parse pass and for orphan detection.
#[derive(Debug, Clone, Copy)]
pub struct BlockLocation {
    pub file_id: u32,
    /// Offset of the 8-byte record prefix (magic + length).
    pub offset: u64,
    /// Total record length including the 8-byte prefix.
    pub record_len: u32,
}

impl BlockLocation {
    #[inline]
    pub fn data_offset(&self) -> u64 {
        self.offset + 8
    }
}

/// Result of the header pass: main chain assignment plus per-file offsets.
pub struct ChainIndex {
    /// height -> location, dense over the main chain.
    pub by_height: Vec<BlockLocation>,
    /// hash -> height, main chain only.
    pub hash_to_height: rustc_hash::FxHashMap<Hash32, u32>,
}

impl ChainIndex {
    pub fn tip_height(&self) -> Option<u32> {
        self.by_height.len().checked_sub(1).map(|n| n as u32)
    }
}

/// Canonical transaction identifier: (height, tx_index) is unique even where
/// txids collide (BIP30-era duplicates at heights 91842/91880, 91812/91817).
pub type TxPk = (u32, u32);

// ---- Row types handed off to the Parquet writers ----
// Hashes are kept as raw 32-byte arrays (wire/little-endian order) so the
// Arrow/Parquet layer can store them as FixedSizeBinary(32) directly.

#[derive(Debug, Clone)]
pub struct BlockRow {
    pub height: u32,
    pub hash: [u8; 32],
    pub prev_hash: [u8; 32],
    pub merkle_root: [u8; 32],
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
    pub version: i32,
    pub tx_count: u32,
    pub size: u32,
    pub weight: u32,
    pub total_fee: u64,
    pub coinbase_value: u64,
}

#[derive(Debug, Clone)]
pub struct TxRow {
    pub height: u32,
    pub tx_index: u32,
    pub txid: [u8; 32],
    pub version: i32,
    pub locktime: u32,
    pub size: u32,
    pub weight: u32,
    pub fee: u64,
    pub input_count: u32,
    pub output_count: u32,
    /// Marker/flag witness presence (BIP144).
    pub has_witness: bool,
}

#[derive(Debug, Clone)]
pub struct InputRow {
    pub height: u32,
    pub tx_index: u32,
    pub input_index: u32,
    pub txid: [u8; 32],
    pub prev_txid: [u8; 32],
    pub prev_vout: u32,
    pub script_sig_len: u32,
    pub sequence: u32,
    pub witness_items: u32,
    pub witness_bytes: u32,
    /// True for coinbase inputs (prev_txid == 0, prev_vout == 0xFFFFFFFF).
    pub is_coinbase: bool,
}

#[derive(Debug, Clone)]
pub struct OutputRow {
    pub height: u32,
    pub tx_index: u32,
    pub output_index: u32,
    pub txid: [u8; 32],
    pub value_sat: u64,
    pub script_pubkey_len: u32,
    pub script_type: u8,
    /// 20- or 32-byte address-relevant hash, right-padded with zeros.
    /// Meaning depends on script_type (see `script::ScriptType`).
    pub address_hash: [u8; 32],
}

/// Derived: an input spending a previous output, enriched with the spent
/// output's value and address. Produced via the UTXO store during parse.
#[derive(Debug, Clone)]
pub struct SpendRow {
    pub height: u32,
    pub spending_tx_index: u32,
    pub spending_input_index: u32,
    pub spending_txid: [u8; 32],
    pub spent_txid: [u8; 32],
    pub spent_vout: u32,
    pub spent_value_sat: u64,
    pub spent_script_type: u8,
    pub spent_address_hash: [u8; 32],
    /// Height at which the spent output was created; 0 if unknown (pre-genesis
    /// never occurs in practice; used as sentinel for missing UTXO).
    pub spent_height: u32,
}
