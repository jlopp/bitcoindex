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
use std::path::{Path, PathBuf};
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
    path: PathBuf,
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
            path: path.to_path_buf(),
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

    /// Filesystem path of the UTXO db.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Total bytes consumed by this UTXO db on disk, including WAL and MANIFEST.
    pub fn disk_size_bytes(&self) -> u64 {
        bidx_core::dir_size_bytes(&self.path)
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

    #[test]
    fn read_varint_all_classes_and_eof() {
        for (bytes, want) in [
            (&[0u8][..], Some(0u64)),
            (&[0xfc][..], Some(252u64)),
            (&[0xfd, 0xfd, 0x00][..], Some(253u64)),
            (&[0xfe, 0x00, 0x00, 0x01, 0x00][..], Some(65_536u64)),
            (&[0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..], Some(1u64)),
            // Truncations of each multi-byte form.
            (&[0xfd][..], None),
            (&[0xfd, 0xaa][..], None),
            (&[0xfe][..], None),
            (&[0xfe, 0, 0, 0][..], None),
            (&[0xff][..], None),
            (&[0xff, 0, 0, 0, 0, 0, 0, 0][..], None),
            // Empty input.
            (&[][..], None),
        ] {
            let mut s: &[u8] = bytes;
            assert_eq!(read_varint(&mut s), want, "case {:?}", bytes);
        }
        // put_varint ↔ read_varint roundtrips for big values.
        for n in [0u64, 252, 253, 65_535, 65_536, u32::MAX as u64, u64::MAX - 1] {
            let mut buf = Vec::new();
            put_varint(&mut buf, n);
            let mut s: &[u8] = &buf;
            assert_eq!(read_varint(&mut s), Some(n));
        }
    }

    #[test]
    fn utxo_value_decode_rejects_wrong_len() {
        for n in 0..45usize {
            assert!(UtxoValue::decode(&vec![0u8; n]).is_none());
        }
        assert!(UtxoValue::decode(&vec![0u8; 46]).is_none());
    }

    #[test]
    fn undo_key_is_big_endian_height_ordering() {
        // BE ordering lets prefix-scan cleanup work when we add it.
        assert!(undo_key(100) < undo_key(200));
        assert!(undo_key(u32::MAX - 1) < undo_key(u32::MAX));
        assert_eq!(undo_key(0xDEADBEEF_u32), [0xDE, 0xAD, 0xBE, 0xEF]);
    }

    /// Synthetic OutputRow for tests.
    fn out_row(
        height: u32,
        tx_index: u32,
        output_index: u32,
        txid: [u8; 32],
        value_sat: u64,
    ) -> OutputRow {
        OutputRow {
            height,
            tx_index,
            output_index,
            txid,
            value_sat,
            script_pubkey_len: 25,
            script_type: 1,
            address_hash: [0xEEu8; 32],
        }
    }

    fn inp_row(
        height: u32,
        tx_index: u32,
        input_index: u32,
        txid: [u8; 32],
        prev_txid: [u8; 32],
        prev_vout: u32,
        is_coinbase: bool,
    ) -> InputRow {
        InputRow {
            height,
            tx_index,
            input_index,
            txid,
            prev_txid,
            prev_vout,
            script_sig_len: 0,
            sequence: 0,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase,
        }
    }

    fn tx_row(height: u32, tx_index: u32, txid: [u8; 32]) -> bidx_core::TxRow {
        bidx_core::TxRow {
            height,
            tx_index,
            txid,
            version: 1,
            locktime: 0,
            size: 0,
            weight: 0,
            fee: 0,
            input_count: 1,
            output_count: 1,
            has_witness: false,
        }
    }

    /// Temp rocksdb path per test; unique to avoid race collisions.
    fn tmp_path(kind: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "bidx-utxo-{}-{}-{}",
            kind,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // RocksDB needs the parent dir to exist.
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Apply a block whose only output is sent to a later block which spends it.
    /// Confirms spend linking, fees, and missing UTXO accounting.
    /// the sparse fields we added. heavily Covered elsewhere; we focus on the
    /// previously branches: set_tip/tip roundtrip, missing-UTXO accounting,
    /// disconnect_block's "NoUndo" path, disk_size_bytes and path().
    #[test]
    fn apply_link_spend_disconnect_roundtrip() {
        let dir = tmp_path("live");
        let db = UtxoStore::open_live(&dir).unwrap();
        let fund_txid = [0x11u8; 32];
        let fund = out_row(0, 0, 0, fund_txid, 50_000);
        let b0_outputs = vec![fund.clone()];
        let r = db.apply_block(0, &[tx_row(0, 0, fund_txid)], &[], &b0_outputs).unwrap();
        assert!(r.spends.is_empty());
        assert_eq!(r.missing, 0);

        // Block 1 spends block 0's output via a coinbase + one regular input.
        let spend_txid = [0x22u8; 32];
        let spend_cb = inp_row(1, 0, 0, spend_txid, [0u8; 32], 0xFFFF_FFFF, true);
        let spend_in = inp_row(1, 1, 0, spend_txid, fund_txid, 0, false);
        let b1_inputs = vec![spend_cb.clone(), spend_in.clone()];
        let b1_outputs = vec![out_row(1, 1, 0, spend_txid, 45_000)];
        let r = db
            .apply_block(1, &[tx_row(1, 0, [0u8; 32]), tx_row(1, 1, spend_txid)], &b1_inputs, &b1_outputs)
            .unwrap();
        assert_eq!(r.spends.len(), 1, "coinbase inputs record no spend");
        let s = &r.spends[0];
        assert_eq!(s.spent_txid, fund_txid);
        assert_eq!(s.spent_vout, 0);
        assert_eq!(s.spent_value_sat, 50_000);
        assert_eq!(s.spent_height, 0);
        assert_eq!(s.spending_txid, spend_txid);
        assert_eq!(s.spending_tx_index, 1);
        // Fee = inputs(50k) - outputs(45k) = 5000 sat for tx_index=1 row only.
        assert_eq!(r.fees[1], 5_000);

        // tip roundtrip
        db.set_tip(&TipState { height: 1, hash: Hash32::from_bytes([0xEE; 32]) }).unwrap();
        let t = db.tip().unwrap().unwrap();
        assert_eq!(t.height, 1);
        assert_eq!(t.hash.0, [0xEE; 32]);

        // Disconnect block 1 and verify the funded output comes back.
        db.disconnect_block(1).unwrap();
        let (v, _) = db.db.get_pinned(make_key(&fund_txid, 0)).unwrap().map(|b| (b.len(), 0i8)).unwrap();
        assert_eq!(v, 45);
        // Created outpoint in block 1 should be gone.
        let gone = db.db.get_pinned(make_key(&spend_txid, 0)).unwrap();
        assert!(gone.is_none());
        // Undo record must be consumed: disconnecting again is an error.
        assert!(matches!(db.disconnect_block(1), Err(UtxoError::NoUndo(1))));
        // But live-mode set_tip still works after disconnect.
        let t2 = db.tip().unwrap().unwrap();
        assert_eq!(t2.height, 1); // it's the caller's job to roll tip back

        // path / disk_size_bytes / flush / approx size (approx_size just queries a RocksDB prop).
        assert_eq!(db.path(), dir.as_path());
        db.flush().unwrap();
        let _ = db.approx_size_on_disk();
        let sz = db.disk_size_bytes();
        assert!(sz > 0, "expected non-zero on-disk size after apply+flush");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Missing (prev_txid, prev_vout) reference increments `missing` without
    /// failing the whole block.
    #[test]
    fn missing_utxo_counted_not_fatal() {
        let dir = tmp_path("miss");
        let db = UtxoStore::open(&dir).unwrap();
        let inputs = vec![inp_row(
            0, 0, 0, [0x33u8; 32], [0x44u8; 32], 0, false,
        )];
        let r = db
            .apply_block(0, &[tx_row(0, 0, [0x33u8; 32])], &inputs, &[])
            .unwrap();
        assert_eq!(r.missing, 1);
        assert!(r.spends.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Bulk mode (no undo): apply_block returns no undo log, disconnect fails
    /// with NoUndo, and all spend + fee paths still work.
    #[test]
    fn bulk_mode_no_undo_recorded() {
        let dir = tmp_path("bulk");
        let db = UtxoStore::open(&dir).unwrap();
        let out_txid = [0xAAu8; 32];
        db.apply_block(0, &[tx_row(0, 0, out_txid)], &[], &[out_row(0, 0, 0, out_txid, 100)])
            .unwrap();
        let inp = inp_row(1, 0, 0, [0xBBu8; 32], out_txid, 0, false);
        let r = db.apply_block(1, &[tx_row(1, 0, [0xBBu8; 32])], std::slice::from_ref(&inp), &[]).unwrap();
        assert_eq!(r.spends.len(), 1);
        assert_eq!(r.fees[0], 100);
        // No undo logged; disconnect must fail.
        assert!(matches!(db.disconnect_block(1), Err(UtxoError::NoUndo(1))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same-block spend: output created and spent within one block. The
    /// created outpoint must be removed from `undo.created` (otherwise
    /// disconnect would delete a key that re-exists in `spent`).
    #[test]
    fn same_block_spend_affects_undo_correctly() {
        let dir = tmp_path("same");
        let db = UtxoStore::open_live(&dir).unwrap();
        let txid = [0x99u8; 32];
        let out = out_row(0, 0, 0, txid, 1000);
        let inp = inp_row(0, 1, 0, txid, txid, 0, false); // spends own tx's vout=0 in same block
        let tx0 = tx_row(0, 0, txid);
        let tx1 = tx_row(0, 1, [0x88u8; 32]);
        let r = db
            .apply_block(0, &[tx0, tx1], std::slice::from_ref(&inp), std::slice::from_ref(&out))
            .unwrap();
        assert_eq!(r.spends.len(), 1);
        // Now disconnect: the undo should handle the same-block spend cleanly.
        db.disconnect_block(0).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// set_tip twice → latest wins; tip() with no prior write returns None.
    #[test]
    fn tip_set_get_overwrite_and_empty() {
        let dir = tmp_path("tip");
        let db = UtxoStore::open_live(&dir).unwrap();
        assert!(db.tip().unwrap().is_none());
        let a = TipState { height: 10, hash: Hash32::from_bytes([1; 32]) };
        let b = TipState { height: 11, hash: Hash32::from_bytes([2; 32]) };
        db.set_tip(&a).unwrap();
        assert_eq!(db.tip().unwrap().unwrap(), a);
        db.set_tip(&b).unwrap();
        assert_eq!(db.tip().unwrap().unwrap(), b);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
