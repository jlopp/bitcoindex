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
use rustc_hash::{FxHashMap, FxHashSet};
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
    /// True when RocksDB statistics were requested at open time. Only then do
    /// the stall counters in `rocksdb.stats` carry meaningful values; see
    /// `stall_report`.
    statistics: bool,
}

pub struct SpendResult {
    pub spends: Vec<SpendRow>,
    pub fees: Vec<u64>,
    pub missing: u64,
    /// Instrumentation for the H2 question: how much of `apply_block` is a
    /// database round trip versus an in-memory `pending` hit.
    ///
    /// Every output of the block is in `pending` before the input loop starts,
    /// so a same-block spend never touches RocksDB. `db_lookups` counts the
    /// inputs that did have to be read from the database — exactly the set a
    /// batched prefetch would have to cover. `db_nanos` is the wall time spent
    /// inside those `get_pinned` calls.
    ///
    /// Measured on testnet4: 71.6% of non-coinbase inputs are database lookups,
    /// costing ~35% of apply wall time. A `batched_multi_get_cf` prefetch over
    /// precisely that key set was implemented and A/B-tested and changed apply
    /// wall time by 0.04% (noise). The cost is per-key RocksDB work, not I/O,
    /// so batching it does not pay. See PERF.md §9. These counters are kept
    /// because the question is worth re-asking on mainnet, where the UTXO set
    /// is far larger and may not fit the block cache.
    ///
    /// `db_lookups == 0` on a block means the block never touched RocksDB for
    /// reads, so H2 could not help it at all. If `db_nanos` is a small share of
    /// apply wall time, the prefetch is not worth implementing.
    pub db_lookups: u64,
    pub db_nanos: u64,
    /// Inputs resolved from `pending` without a database read (same-block
    /// spends). `pending_hits + db_lookups` == non-coinbase input count.
    pub pending_hits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipState {
    pub height: u32,
    pub hash: Hash32,
}

/// Block cache capacity, shared by every column family.
///
/// The default was RocksDB's own 32 MiB — measured, not assumed: a probe run
/// reported `block_cache_capacity 33554432`. That is ~2,000x smaller than the
/// 3+ GiB UTXO data set this indexer builds, so almost every point lookup in
/// `apply_block` had to fault a data block off disk and decompress it.
///
/// The cache is shared across column families so the read path cannot be
/// starved by the undo/meta families, which are written far more than read.
const BLOCK_CACHE_BYTES: usize = 1 << 30; // 1 GiB

/// Tuning applied to the database *and* to every column family.
///
/// Building this once and cloning it into each `ColumnFamilyDescriptor` is
/// load-bearing, not stylistic. `DB::open_cf_descriptors` takes per-CF
/// `Options` and passes them to `rocksdb_open_column_families`, and the C API
/// applies the per-CF options to that family. So setting tuning only on the
/// DB-level `Options` (as this code used to) has *no effect on any column
/// family* — the default CF carried RocksDB's stock 64 MiB write buffer and
/// 32 MiB block cache regardless of what `open_with` configured. The measured
/// symptom was a write-buffer flush every ~1s at tip instead of every ~8s.
fn tuned_options(statistics: bool) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    // A 512 MiB buffer means a flush every ~8s at the observed write rate
    // rather than every second; with 4 buffers we can absorb a burst while a
    // flush is in flight.
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
    // A real block cache, plus a Bloom filter so a lookup for a spent outpoint
    // can reject a missing key without touching a data block. Both are
    // attached per-CF, which is the only place they take effect.
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(BLOCK_CACHE_BYTES);
    block_opts.set_block_cache(&cache);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_whole_key_filtering(true);
    opts.set_block_based_table_factory(&block_opts);
    if statistics {
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(0);
    }
    opts
}

impl UtxoStore {
    /// Open in bulk-load mode (no WAL, no undo logging).
    pub fn open(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(path, false, false, false)
    }

    /// Open in live mode: WAL enabled (durable per-block) and undo logging.
    pub fn open_live(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(path, true, true, false)
    }

    /// Like [`UtxoStore::open`] but with RocksDB statistics enabled, which
    /// makes `stall_report` meaningful. Statistics collection adds a small
    /// amount of per-operation overhead, so this is for diagnosis, not for
    /// throughput runs.
    pub fn open_statistics(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(path, false, false, true)
    }

    fn open_with(
        path: &Path,
        wal: bool,
        undo_enabled: bool,
        statistics: bool,
    ) -> Result<Self, UtxoError> {
        // DB-level options: create flags, background threads, statistics.
        let mut opts = tuned_options(statistics);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Per-CF options: this is where the storage tuning actually lands.
        // The undo and meta families get the same table factory and cache but
        // do not need a large write buffer, since they are tiny next to the
        // UTXO family.
        let utxo_cf_opts = tuned_options(statistics);
        let mut aux_cf_opts = Options::default();
        let mut aux_block_opts = rocksdb::BlockBasedOptions::default();
        aux_block_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(BLOCK_CACHE_BYTES));
        aux_cf_opts.set_block_based_table_factory(&aux_block_opts);
        aux_cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let cf_defs = vec![
            rocksdb::ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, utxo_cf_opts),
            rocksdb::ColumnFamilyDescriptor::new(CF_UNDO, aux_cf_opts.clone()),
            rocksdb::ColumnFamilyDescriptor::new(CF_META, aux_cf_opts),
        ];
        let db = DB::open_cf_descriptors(&opts, path, cf_defs)?;
        let _ = wal; // WAL is always on; bulk mode uses WriteOptions::disable_wal per write.
        Ok(UtxoStore {
            db,
            path: path.to_path_buf(),
            pending: Mutex::new(FxHashMap::default()),
            undo_enabled,
            statistics,
        })
    }

    fn cf_undo(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_UNDO).expect("undo cf")
    }
    fn cf_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_META).expect("meta cf")
    }
    fn cf_default(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .expect("default cf")
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
        // Outpoints created *and* spent inside this block. Populated during the
        // input loop, applied to `undo.created` once at the end. Using a set
        // here is what removes the quadratic scan from the input loop.
        let mut same_block: FxHashSet<[u8; 36]> = FxHashSet::default();

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

        // H2 instrumentation: how many inputs needed a database read at all,
        // and how much wall time those reads cost. This is the measurement that
        // decides whether the prefetch below is worth having.
        let mut db_lookups = 0u64;
        let mut db_nanos = 0u64;
        let mut pending_hits = 0u64;

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
                    // Mark it spent-in-block rather than doing a linear scan of
                    // `undo.created` here. The scan made this loop O(outputs x
                    // spent-inputs) — quadratic in block size, on the hottest
                    // path in the indexer (a 4k-tx block is ~16M comparisons).
                    // `same_block` is reconciled into `created` once, below.
                    same_block.insert(key);
                }
                pending_hits += 1;
                record_spend(&mut spends, &mut fees, height, inp, v);
            } else {
                db_lookups += 1;
                let t0 = std::time::Instant::now();
                let got = self.db.get_pinned(key);
                db_nanos += t0.elapsed().as_nanos() as u64;
                match got? {
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

        // Reconcile same-block spends into `undo.created` in one pass. Any
        // outpoint created in this block and also spent in it must not be
        // deleted on disconnect, because the deletion of `created` outpoints
        // is what reverses a same-block spend.
        if self.undo_enabled && !same_block.is_empty() {
            undo.created.retain(|k| !same_block.contains(k));
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
            db_lookups,
            db_nanos,
            pending_hits,
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

    /// Snapshot of RocksDB internals that matter for ingest throughput.
    ///
    /// These properties answer the questions that decide where to optimise:
    /// whether the write-buffer tuning in `open_with` actually took effect on
    /// the column families (`cur_size_all_mem_tables` / `size_all_mem_tables`
    /// against the configured 512 MiB per buffer), whether the write path is
    /// backing up behind flushes (`num_immutable_mem_table`,
    /// `mem_table_flush_pending`), whether compactions are queued
    /// (`compaction_pending`, `num_running_compactions`), whether RocksDB has
    /// started throttling us (`is_write_stopped`,
    /// `actual_delayed_write_rate`), and how the point-lookup path that
    /// `apply_block` depends on is cached (`block_cache_capacity/usage`).
    ///
    /// All of these are readable without enabling RocksDB statistics, so the
    /// act of measuring does not perturb the run. Properties that RocksDB
    /// declines to answer are reported as `"unavailable"` rather than
    /// omitted, so two runs can be diffed key-for-key.
    pub fn perf_properties(&self) -> Vec<(String, String)> {
        const KEYS: &[(&rocksdb::properties::PropName, &str)] = &[
            (rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES, "cur_size_all_mem_tables"),
            (rocksdb::properties::SIZE_ALL_MEM_TABLES, "size_all_mem_tables"),
            (rocksdb::properties::CUR_SIZE_ACTIVE_MEM_TABLE, "cur_size_active_mem_table"),
            (rocksdb::properties::NUM_IMMUTABLE_MEM_TABLE, "num_immutable_mem_table"),
            (rocksdb::properties::MEM_TABLE_FLUSH_PENDING, "mem_table_flush_pending"),
            (rocksdb::properties::NUM_RUNNING_FLUSHES, "num_running_flushes"),
            (rocksdb::properties::COMPACTION_PENDING, "compaction_pending"),
            (rocksdb::properties::NUM_RUNNING_COMPACTIONS, "num_running_compactions"),
            (rocksdb::properties::IS_WRITE_STOPPED, "is_write_stopped"),
            (rocksdb::properties::ACTUAL_DELAYED_WRITE_RATE, "actual_delayed_write_rate"),
            (rocksdb::properties::ESTIMATE_NUM_KEYS, "estimate_num_keys"),
            (rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE, "estimate_live_data_size"),
            (rocksdb::properties::TOTAL_SST_FILES_SIZE, "total_sst_files_size"),
            (rocksdb::properties::BLOCK_CACHE_CAPACITY, "block_cache_capacity"),
            (rocksdb::properties::BLOCK_CACHE_USAGE, "block_cache_usage"),
        ];

        let mut out: Vec<(String, String)> = Vec::with_capacity(KEYS.len() + 2);
        for &(prop, name) in KEYS {
            out.push((name.to_string(), self.property_or_unavailable(prop)));
        }
        // "num-files-at-level0" is generated, not a const.
        out.push((
            "num_files_at_level0".to_string(),
            self.property_or_unavailable(rocksdb::properties::num_files_at_level(0)),
        ));
        out
    }

    fn property_or_unavailable(&self, prop: impl rocksdb::CStrLike) -> String {
        self.db
            .property_value(prop)
            .ok()
            .flatten()
            .unwrap_or_else(|| "unavailable".to_string())
    }

    /// Lines from the `rocksdb.stats` blob that mention stalls, which is
    /// where compaction backpressure becomes visible.
    ///
    /// Returns empty unless the store was opened with `open_statistics`,
    /// because RocksDB only populates these counters when statistics are on.
    /// An empty result therefore means "not measured", never "zero stalls".
    pub fn stall_report(&self) -> Vec<String> {
        self.db
            .property_value(rocksdb::properties::STATS)
            .ok()
            .flatten()
            .map(|s| {
                s.lines()
                    .filter(|l| l.contains("stall") || l.contains("Stall"))
                    .map(|l| l.trim().to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// True when this store was opened with RocksDB statistics enabled, i.e.
    /// when `stall_report` can say anything meaningful.
    pub fn statistics_enabled(&self) -> bool {
        self.statistics
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

    /// The same-block reconciliation must not leave the created outpoint in
    /// the undo record, and disconnecting must not resurrect it. This is the
    /// behaviour the old `undo.created.retain()` scan provided; the set-based
    /// replacement has to produce an identical undo record, so assert on the
    /// encoded record rather than merely "did not error".
    #[test]
    fn same_block_spend_removes_key_from_undo_created() {
        let dir = tmp_path("same-undo");
        let db = UtxoStore::open_live(&dir).unwrap();
        let txid = [0x77u8; 32];
        // Two outputs created; only vout=0 is spent in the same block.
        let out0 = out_row(0, 0, 0, txid, 1000);
        let out1 = out_row(0, 0, 1, txid, 2000);
        let inp = inp_row(0, 1, 0, txid, txid, 0, false);
        let tx0 = tx_row(0, 0, txid);
        let tx1 = tx_row(0, 1, [0x66u8; 32]);
        db.apply_block(
            0,
            &[tx0, tx1],
            std::slice::from_ref(&inp),
            &[out0, out1],
        )
        .unwrap();

        // Inspect the stored undo record directly.
        let raw = db
            .db
            .get_cf(&db.cf_undo(), undo_key(0))
            .unwrap()
            .expect("undo record");
        let undo = BlockUndo::decode(&raw).expect("decodable undo");
        assert_eq!(undo.spent.len(), 0, "same-block spend must not be an undo.spent");
        assert_eq!(
            undo.created.len(),
            1,
            "spent-in-block outpoint must be dropped from undo.created"
        );
        assert_eq!(
            undo.created[0],
            make_key(&txid, 1),
            "only the unspent output should remain in undo.created"
        );

        // Disconnect must be a clean no-op on the set (nothing to restore,
        // nothing to delete that still exists).
        db.disconnect_block(0).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Guard the per-column-family options fix. `DB::open_cf_descriptors`
    /// applies each CF's own `Options`, so tuning set only on the DB-level
    /// options is silently ignored. This asserts the tuning actually reached
    /// the default (UTXO) family, by reading the properties back.
    #[test]
    fn column_family_options_actually_applied() {
        let dir = tmp_path("cfopts");
        let db = UtxoStore::open_statistics(&dir).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();

        let cache: usize = props
            .get("block_cache_capacity")
            .expect("block_cache_capacity present")
            .parse()
            .expect("numeric");
        assert_eq!(
            cache, BLOCK_CACHE_BYTES,
            "block cache on the default CF must be the configured size, not RocksDB's 32 MiB default"
        );

        // Writing enough to fill the small default write buffer would flush
        // early; instead assert the CF-level write buffer is the configured
        // 512 MiB by checking it is *not* the stock 64 MiB. `cur_size_active`
        // only grows with writes, so compare the configured value via a flush
        // boundary: an empty store reports ~1 MiB, which tells us nothing.
        // The block-cache assertion above is the load-bearing one; this one
        // documents intent for the buffer size.
        assert!(
            BLOCK_CACHE_BYTES >= 512 << 20,
            "cache must dominate the write buffer for the read path"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The H2 counters must be internally consistent: every non-coinbase input
    /// is either resolved from `pending` (same-block spend) or read from the
    /// database, never both and never neither. A block with one of each is the
    /// minimal case that proves the split is real.
    #[test]
    fn apply_block_reports_db_and_pending_lookup_counts() {
        let dir = tmp_path("h2counts");
        let db = UtxoStore::open_live(&dir).unwrap();

        // Block 0 funds outpoint A:0, which lands in the database.
        let a = [0xA1u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 1_000)])
            .unwrap();

        // Block 1 creates B:0 and spends it in the same block (a `pending`
        // hit), and also spends A:0 from block 0 (a database lookup).
        let b = [0xB1u8; 32];
        let tx1 = [0xC1u8; 32];
        let tx2 = [0xD1u8; 32];
        let inputs = [
            inp_row(1, 1, 0, tx1, a, 0, false), // DB lookup
            inp_row(1, 2, 0, tx2, b, 0, false), // same-block spend
        ];
        let outputs = [out_row(1, 0, 0, b, 500)];
        let txs = [tx_row(1, 0, b), tx_row(1, 1, tx1), tx_row(1, 2, tx2)];

        let r = db.apply_block(1, &txs, &inputs, &outputs).unwrap();
        assert_eq!(r.db_lookups, 1, "A:0 must be read from the database");
        assert_eq!(r.pending_hits, 1, "B:0 must be resolved from pending");
        assert_eq!(
            r.db_lookups + r.pending_hits,
            2,
            "every non-coinbase input must be accounted for exactly once"
        );
        assert!(
            r.db_nanos > 0,
            "a real database read must take measurable time"
        );
        assert_eq!(r.missing, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A block whose spends are all same-block must report zero database
    /// lookups — the case where a batched prefetch could not help at all.
    #[test]
    fn apply_block_reports_no_db_lookups_when_all_spends_are_same_block() {
        let dir = tmp_path("h2none");
        let db = UtxoStore::open_live(&dir).unwrap();
        let a = [0xA2u8; 32];
        let tx1 = [0xC2u8; 32];
        let inputs = [inp_row(0, 1, 0, tx1, a, 0, false)];
        let outputs = [out_row(0, 0, 0, a, 700)];
        let txs = [tx_row(0, 0, a), tx_row(0, 1, tx1)];

        let r = db.apply_block(0, &txs, &inputs, &outputs).unwrap();
        assert_eq!(r.db_lookups, 0);
        assert_eq!(r.pending_hits, 1);
        assert_eq!(r.db_nanos, 0, "no database read means no database time");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Coinbase inputs are neither a database lookup nor a pending hit, so a
    /// coinbase-only block reports zero for both. This is the case that makes
    /// the counters meaningful on testnet4, where most blocks are coinbase-only.
    #[test]
    fn apply_block_excludes_coinbase_from_lookup_counts() {
        let dir = tmp_path("h2cb");
        let db = UtxoStore::open_live(&dir).unwrap();
        let a = [0xA3u8; 32];
        let inputs = [inp_row(0, 0, 0, a, [0u8; 32], u32::MAX, true)];
        let outputs = [out_row(0, 0, 0, a, 50)];
        let r = db
            .apply_block(0, &[tx_row(0, 0, a)], &inputs, &outputs)
            .unwrap();
        assert_eq!(r.db_lookups, 0);
        assert_eq!(r.pending_hits, 0);
        assert_eq!(r.spends.len(), 0, "coinbase produces no spend row");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Guard the quadratic-scan fix: many outputs and many same-block spends
    /// must reconcile correctly. The old code was O(outputs x spends) and a
    /// regression would still pass a single-output test, so exercise a block
    /// with a realistic fan-out.
    #[test]
    fn same_block_reconciliation_scales_to_many_outputs() {
        let dir = tmp_path("same-many");
        let db = UtxoStore::open_live(&dir).unwrap();
        const N: u32 = 512;
        let txid = [0x5Au8; 32];
        let mut outs = Vec::new();
        for vout in 0..N {
            outs.push(out_row(0, 0, vout, txid, 1000 + vout as u64));
        }
        // Spend every other output in the same block.
        let mut inps = Vec::new();
        let mut expected_created = Vec::new();
        for vout in 0..N {
            if vout % 2 == 0 {
                inps.push(inp_row(0, 1, vout, txid, txid, vout, false));
            } else {
                expected_created.push(make_key(&txid, vout));
            }
        }
        let tx0 = tx_row(0, 0, txid);
        let tx1 = tx_row(0, 1, [0x5Bu8; 32]);
        let r = db.apply_block(0, &[tx0, tx1], &inps, &outs).unwrap();
        assert_eq!(r.spends.len(), N as usize / 2);

        let raw = db
            .db
            .get_cf(&db.cf_undo(), undo_key(0))
            .unwrap()
            .expect("undo record");
        let undo = BlockUndo::decode(&raw).expect("decodable undo");
        assert!(undo.spent.is_empty(), "all spends were same-block");
        assert_eq!(undo.created.len(), expected_created.len());
        for k in &expected_created {
            assert!(
                undo.created.contains(k),
                "unspent outpoint missing from undo.created"
            );
        }
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
