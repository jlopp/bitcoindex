//! RocksDB-backed UTXO store with per-block undo logging and tip state.
//!
//! Column families:
//!   default  : UTXO entries
//!              key   = txid(32) || vout(4, little-endian)      -> 36 bytes
//!              value = value_sat(8) || script_type(1) || height(4)
//!                      || address_hash(32)                     -> 45 bytes
//!   undo     : per-block undo records for reorg handling
//!              key   = height(4, big-endian, so scans are height-ordered)
//!              value = encoded BlockUndo (see below)
//!   meta     : tip state
//!              key   = b"tip"  ->  height(4 LE) || block_hash(32)
//!
//! BlockUndo encoding (all integers little-endian):
//!   n_spent(varint)  then n_spent × [key(36) || UtxoValue(45)]
//!   n_created(varint) then n_created × [key(36)]
//!
//! To disconnect a block we re-insert the spent outputs and delete the
//! created outpoints. The undo record is written atomically in the same
//! WriteBatch as the UTXO mutations, so a crash mid-apply can't leave the
//! undo log inconsistent with the set (we use the WAL for live mode).

use bidx_core::{Hash32, InputRow, OutputRow, SpendRow};
use parking_lot::Mutex;
use rocksdb::{Options, WriteBatch, DB};
use rustc_hash::FxHashMap;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UtxoError {
    #[error(transparent)]
    Rocks(#[from] rocksdb::Error),
    #[error("no undo record for height {0}")]
    NoUndo(u32),
    #[error("corrupt undo record at height {0}")]
    CorruptUndo(u32),
    #[error("tip state missing")]
    NoTip,
}

const CF_UNDO: &str = "undo";
const CF_META: &str = "meta";
const META_TIP_KEY: &[u8] = b"tip";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtxoValue {
    pub value_sat: u64,
    pub script_type: u8,
    pub height: u32,
    pub address_hash: [u8; 32],
}

impl UtxoValue {
    const ENCODED_LEN: usize = 8 + 1 + 4 + 32;

    fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut b = [0u8; Self::ENCODED_LEN];
        b[0..8].copy_from_slice(&self.value_sat.to_le_bytes());
        b[8] = self.script_type;
        b[9..13].copy_from_slice(&self.height.to_le_bytes());
        b[13..45].copy_from_slice(&self.address_hash);
        b
    }

    fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != Self::ENCODED_LEN {
            return None;
        }
        let mut value_sat = [0u8; 8];
        value_sat.copy_from_slice(&b[0..8]);
        let mut height = [0u8; 4];
        height.copy_from_slice(&b[9..13]);
        let mut address_hash = [0u8; 32];
        address_hash.copy_from_slice(&b[13..45]);
        Some(UtxoValue {
            value_sat: u64::from_le_bytes(value_sat),
            script_type: b[8],
            height: u32::from_le_bytes(height),
            address_hash,
        })
    }
}

#[inline]
fn make_key(txid: &[u8; 32], vout: u32) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[..32].copy_from_slice(txid);
    k[32..].copy_from_slice(&vout.to_le_bytes());
    k
}

#[inline]
fn undo_key(height: u32) -> [u8; 4] {
    height.to_be_bytes()
}

/// A record of everything needed to reverse one block's UTXO mutations.
#[derive(Debug, Default, Clone)]
pub struct BlockUndo {
    /// Outputs this block spent (must be re-inserted on disconnect).
    pub spent: Vec<([u8; 36], UtxoValue)>,
    /// Outpoints this block created (must be deleted on disconnect).
    pub created: Vec<[u8; 36]>,
}

impl BlockUndo {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.spent.len() * 81 + self.created.len() * 36 + 16);
        put_varint(&mut out, self.spent.len() as u64);
        for (key, val) in &self.spent {
            out.extend_from_slice(key);
            out.extend_from_slice(&val.encode());
        }
        put_varint(&mut out, self.created.len() as u64);
        for key in &self.created {
            out.extend_from_slice(key);
        }
        out
    }

    fn decode(mut b: &[u8]) -> Option<Self> {
        let n_spent = read_varint(&mut b)? as usize;
        let mut spent = Vec::with_capacity(n_spent);
        for _ in 0..n_spent {
            if b.len() < 81 {
                return None;
            }
            let mut key = [0u8; 36];
            key.copy_from_slice(&b[..36]);
            let val = UtxoValue::decode(&b[36..81])?;
            spent.push((key, val));
            b = &b[81..];
        }
        let n_created = read_varint(&mut b)? as usize;
        let mut created = Vec::with_capacity(n_created);
        for _ in 0..n_created {
            if b.len() < 36 {
                return None;
            }
            let mut key = [0u8; 36];
            key.copy_from_slice(&b[..36]);
            created.push(key);
            b = &b[36..];
        }
        Some(BlockUndo { spent, created })
    }
}

#[inline]
fn put_varint(out: &mut Vec<u8>, n: u64) {
    match n {
        0..=0xfc => out.push(n as u8),
        0xfd..=0xffff => {
            out.push(0xfd);
            out.extend_from_slice(&(n as u16).to_le_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xfe);
            out.extend_from_slice(&(n as u32).to_le_bytes());
        }
        _ => {
            out.push(0xff);
            out.extend_from_slice(&n.to_le_bytes());
        }
    }
}

#[inline]
fn read_varint(b: &mut &[u8]) -> Option<u64> {
    let first = *b.first()?;
    *b = &b[1..];
    match first {
        0x00..=0xfc => Some(first as u64),
        0xfd => {
            if b.len() < 2 { return None; }
            let v = u16::from_le_bytes([b[0], b[1]]) as u64;
            *b = &b[2..];
            Some(v)
        }
        0xfe => {
            if b.len() < 4 { return None; }
            let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as u64;
            *b = &b[4..];
            Some(v)
        }
        0xff => {
            if b.len() < 8 { return None; }
            let v = u64::from_le_bytes(b[..8].try_into().ok()?);
            *b = &b[8..];
            Some(v)
        }
    }
}

pub struct UtxoStore {
    db: DB,
    pending: Mutex<FxHashMap<[u8; 36], UtxoValue>>,
    /// When false, undo records are not written (bulk initial load).
    /// Live mode enables undo so reorgs can be reversed.
    undo_enabled: bool,
}

pub struct SpendResult {
    pub spends: Vec<SpendRow>,
    pub fees: Vec<u64>,
    pub missing: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipState {
    pub height: u32,
    pub hash: Hash32,
}

impl UtxoStore {
    /// Open in bulk-load mode (no WAL, no undo logging).
    pub fn open(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(path, false, false)
    }

    /// Open in live mode: WAL enabled (durable per-block) and undo logging.
    pub fn open_live(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(path, true, true)
    }

    fn open_with(path: &Path, wal: bool, undo_enabled: bool) -> Result<Self, UtxoError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(512 << 20);
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(256 << 20);
        opts.set_level_zero_file_num_compaction_trigger(8);
        opts.set_level_zero_slowdown_writes_trigger(20);
        opts.set_level_zero_stop_writes_trigger(30);
        opts.set_max_background_jobs(8);
        opts.increase_parallelism(8);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        // WAL stays enabled; we control per-write durability via WriteOptions.

        let cf_defs = vec![
            rocksdb::ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, Options::default()),
            rocksdb::ColumnFamilyDescriptor::new(CF_UNDO, Options::default()),
            rocksdb::ColumnFamilyDescriptor::new(CF_META, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&opts, path, cf_defs)?;
        let _ = wal; // WAL is always on; bulk mode uses WriteOptions::disable_wal per write.
        Ok(UtxoStore {
            db,
            pending: Mutex::new(FxHashMap::default()),
            undo_enabled,
        })
    }

    fn cf_undo(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_UNDO).expect("undo cf")
    }
    fn cf_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_META).expect("meta cf")
    }

    /// Apply one block forward. In undo-enabled (live) mode, also writes the
    /// undo record for `height` in the same batch so the set and its undo log
    /// are always consistent.
    pub fn apply_block(
        &self,
        height: u32,
        txs: &[bidx_core::TxRow],
        inputs: &[InputRow],
        outputs: &[OutputRow],
    ) -> Result<SpendResult, UtxoError> {
        let mut pending = self.pending.lock();
        pending.clear();

        let mut batch = WriteBatch::default();
        let mut spends = Vec::with_capacity(inputs.len());
        let mut fees = vec![0u64; txs.len()];
        let mut missing = 0u64;
        let mut undo = BlockUndo::default();

        for out in outputs {
            let key = make_key(&out.txid, out.output_index);
            pending.insert(
                key,
                UtxoValue {
                    value_sat: out.value_sat,
                    script_type: out.script_type,
                    height,
                    address_hash: out.address_hash,
                },
            );
            if self.undo_enabled {
                undo.created.push(key);
            }
        }

        for inp in inputs {
            if inp.is_coinbase {
                continue;
            }
            let key = make_key(&inp.prev_txid, inp.prev_vout);
            if let Some(v) = pending.remove(&key) {
                // Same-block spend: removed from pending, never hits the DB.
                // On disconnect, the created-outpoint deletion handles it, so
                // we drop it from `undo.created` to avoid a dangling delete.
                if self.undo_enabled {
                    undo.created.retain(|k| *k != key);
                    // We still record the spend row for the indexer, but the
                    // UTXO never existed outside this block so no undo.spent.
                }
                record_spend(&mut spends, &mut fees, height, inp, v);
            } else {
                match self.db.get_pinned(key)? {
                    Some(bytes) => {
                        if let Some(v) = UtxoValue::decode(bytes.as_ref()) {
                            batch.delete(key);
                            if self.undo_enabled {
                                undo.spent.push((key, v));
                            }
                            record_spend(&mut spends, &mut fees, height, inp, v);
                        } else {
                            missing += 1;
                        }
                    }
                    None => {
                        missing += 1;
                    }
                }
            }
        }

        for out in outputs.iter() {
            let f = &mut fees[out.tx_index as usize];
            *f = f.saturating_sub(out.value_sat);
        }

        for (key, val) in pending.drain() {
            batch.put(key, val.encode());
        }

        if self.undo_enabled {
            batch.put_cf(&self.cf_undo(), undo_key(height), undo.encode());
        }

        // Bulk mode: skip WAL for throughput (reproducible from source).
        // Live mode: sync WAL so per-block state survives a crash.
        let mut wo = rocksdb::WriteOptions::default();
        if self.undo_enabled {
            wo.set_sync(true);
        } else {
            wo.disable_wal(true);
        }
        self.db.write_opt(batch, &wo)?;

        Ok(SpendResult {
            spends,
            fees,
            missing,
        })
    }

    /// Reverse the block applied at `height`, restoring spent outputs and
    /// removing created outpoints. Consumes the undo record.
    pub fn disconnect_block(&self, height: u32) -> Result<(), UtxoError> {
        let cf = self.cf_undo();
        let raw = self
            .db
            .get_cf(&cf, undo_key(height))?
            .ok_or(UtxoError::NoUndo(height))?;
        let undo = BlockUndo::decode(&raw).ok_or(UtxoError::CorruptUndo(height))?;

        let mut batch = WriteBatch::default();
        // Restore spent outputs.
        for (key, val) in &undo.spent {
            batch.put(*key, val.encode());
        }
        // Remove created outpoints that remain in the set. Those spent
        // same-block were already removed (and not in `created`).
        for key in &undo.created {
            batch.delete(*key);
        }
        batch.delete_cf(&cf, undo_key(height));

        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(batch, &wo)?;
        Ok(())
    }

    /// Persist the current chain tip.
    pub fn set_tip(&self, tip: &TipState) -> Result<(), UtxoError> {
        let mut b = [0u8; 36];
        b[..4].copy_from_slice(&tip.height.to_le_bytes());
        b[4..].copy_from_slice(tip.hash.as_bytes());
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.db.put_cf_opt(&self.cf_meta(), META_TIP_KEY, b, &wo)?;
        Ok(())
    }

    /// Read the persisted tip, if any.
    pub fn tip(&self) -> Result<Option<TipState>, UtxoError> {
        match self.db.get_cf(&self.cf_meta(), META_TIP_KEY)? {
            Some(b) if b.len() == 36 => {
                let mut height = [0u8; 4];
                height.copy_from_slice(&b[..4]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&b[4..]);
                Ok(Some(TipState {
                    height: u32::from_le_bytes(height),
                    hash: Hash32::from_bytes(hash),
                }))
            }
            _ => Ok(None),
        }
    }

    pub fn flush(&self) -> Result<(), UtxoError> {
        self.db.flush()?;
        Ok(())
    }

    pub fn approx_size_on_disk(&self) -> u64 {
        self.db
            .property_value(rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE)
            .ok()
            .flatten()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }
}

#[inline]
fn record_spend(
    spends: &mut Vec<SpendRow>,
    fees: &mut [u64],
    height: u32,
    inp: &InputRow,
    v: UtxoValue,
) {
    spends.push(SpendRow {
        height,
        spending_tx_index: inp.tx_index,
        spending_input_index: inp.input_index,
        spending_txid: inp.txid,
        spent_txid: inp.prev_txid,
        spent_vout: inp.prev_vout,
        spent_value_sat: v.value_sat,
        spent_script_type: v.script_type,
        spent_address_hash: v.address_hash,
        spent_height: v.height,
    });
    let f = &mut fees[inp.tx_index as usize];
    *f = f.saturating_add(v.value_sat);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utxo_value_roundtrip() {
        let v = UtxoValue {
            value_sat: 123_456_789,
            script_type: 2,
            height: 800_000,
            address_hash: [0xab; 32],
        };
        let dec = UtxoValue::decode(&v.encode()).unwrap();
        assert_eq!(dec, v);
    }

    #[test]
    fn block_undo_roundtrip() {
        let mut u = BlockUndo::default();
        for i in 0..5u8 {
            let mut key = [0u8; 36];
            key[0] = i;
            key[35] = 255 - i;
            u.spent.push((
                key,
                UtxoValue {
                    value_sat: (i as u64) * 1_000_000,
                    script_type: i,
                    height: 100 + i as u32,
                    address_hash: [i; 32],
                },
            ));
        }
        for i in 0..3u8 {
            let mut key = [0u8; 36];
            key[10] = i;
            u.created.push(key);
        }
        let enc = u.encode();
        let dec = BlockUndo::decode(&enc).unwrap();
        assert_eq!(dec.spent.len(), 5);
        assert_eq!(dec.created.len(), 3);
        for (a, b) in u.spent.iter().zip(dec.spent.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1, b.1);
        }
        assert_eq!(u.created, dec.created);
    }

    #[test]
    fn block_undo_empty_and_varint_boundaries() {
        // Empty undo.
        let u = BlockUndo::default();
        let dec = BlockUndo::decode(&u.encode()).unwrap();
        assert!(dec.spent.is_empty() && dec.created.is_empty());

        // Varint boundary: 252 -> 1 byte, 253 -> 3 bytes.
        let mut out = Vec::new();
        put_varint(&mut out, 252);
        assert_eq!(out.len(), 1);
        out.clear();
        put_varint(&mut out, 253);
        assert_eq!(out.len(), 3);
        // Round-trip a large count.
        let mut out = Vec::new();
        put_varint(&mut out, 4_000_000);
        let mut slice: &[u8] = &out;
        assert_eq!(read_varint(&mut slice), Some(4_000_000));
    }

    #[test]
    fn block_undo_rejects_truncated() {
        let mut u = BlockUndo::default();
        u.spent.push(([1u8; 36], UtxoValue {
            value_sat: 1,
            script_type: 0,
            height: 0,
            address_hash: [0u8; 32],
        }));
        let enc = u.encode();
        // Truncate mid-record.
        assert!(BlockUndo::decode(&enc[..20]).is_none());
    }
}
