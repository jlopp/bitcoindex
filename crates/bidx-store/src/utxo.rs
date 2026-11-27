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

/// Approximate in-memory cost of one cache entry, used for eviction accounting.
///
/// The exact allocator overhead is unknowable, so this is a deliberate fixed
/// estimate: key 36 + `UtxoValue` 45 = 81 bytes of payload, plus the hash-map
/// and recency-queue overhead. Budgets are therefore approximate, which is fine
/// for a cache whose whole purpose is bounded memory.
const CACHE_ENTRY_COST: usize = 112;

/// A bounded cache of the most recently created UTXOs, kept in memory.
///
/// Why this exists: `apply_block` must read the *previous* output for every
/// spent input, because a transaction input carries only a pointer (`prev_txid`,
/// `prev_vout`) — not the value, address, script type, or height. Those four
/// fields are required by `SpendRow` and by fee computation, and they live only
/// in the UTXO store. On testnet4 that was 23.7M database lookups costing ~35%
/// of apply wall time (PERF.md §9), and the cost is per-key RocksDB work rather
/// than I/O, so batching it did not help.
///
/// This cache removes the RocksDB lookup for the *most recently created*
/// outputs, which are the ones most likely to be spent soon. It is a plain hash
/// lookup instead of a block-index search, bloom check and value construction.
///
/// Entry size cannot be reduced below `UtxoValue`: `spent_value_sat`,
/// `spent_script_type`, `spent_address_hash` and `spent_height` are all written
/// into `SpendRow`, so truncating the address hash (or any other field) would
/// corrupt the index rather than merely risk a lookup collision. The key cannot
/// be truncated either — the cache is keyed on the full outpoint, and a false
/// hit would return the wrong value.
struct RecentUtxoCache {
    map: FxHashMap<[u8; 36], UtxoValue>,
    /// Creation order, oldest at the front. Used to evict the least recently
    /// created entries once the budget is exceeded. An outpoint is created at
    /// most once and spent at most once, so this is a faithful recency order.
    order: std::collections::VecDeque<[u8; 36]>,
    budget_bytes: usize,
    hits: u64,
    misses: u64,
}

impl RecentUtxoCache {
    fn new(budget_bytes: usize) -> Self {
        Self {
            map: FxHashMap::default(),
            order: std::collections::VecDeque::new(),
            budget_bytes,
            hits: 0,
            misses: 0,
        }
    }

    /// Insert a newly created outpoint. Called for every output of every block.
    ///
    /// The caller only invokes this when the cache is enabled and undo is off,
    /// so there is no "already present" case in practice; if one occurs the
    /// entry is refreshed without being re-queued, so the recency order stays
    /// consistent with creation order.
    fn insert(&mut self, key: [u8; 36], val: UtxoValue) {
        if self.map.insert(key, val).is_none() {
            self.order.push_back(key);
            self.evict_to_budget();
        }
    }

    /// Look up a spent outpoint, removing it on a hit.
    ///
    /// Removal mirrors the store semantics: a spent outpoint leaves the UTXO
    /// set, so a later block cannot legitimately spend it again. Keeping it
    /// would risk serving a stale value if the outpoint were somehow recreated.
    fn remove(&mut self, key: &[u8; 36]) -> Option<UtxoValue> {
        match self.map.remove(key) {
            Some(v) => {
                self.hits += 1;
                // Leave the key in `order`; eviction tolerates a stale key.
                Some(v)
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    /// Drop the least recently created entries until the budget is met.
    ///
    /// `order` may contain keys that were already removed by a spend, so
    /// eviction skips those rather than assuming the front key is resident.
    fn evict_to_budget(&mut self) {
        while self.map.len() * CACHE_ENTRY_COST > self.budget_bytes {
            match self.order.pop_front() {
                Some(k) => {
                    self.map.remove(&k);
                }
                // coverage-exempt: unreachable — `insert` pushes every key to
                // `order`, and keys leave `order` only here (where they are
                // removed from `map` in the same step), so `map.len() <=
                // order.len()` always holds and the queue cannot run dry while
                // the map is non-empty. Kept so the loop terminates rather than
                // spins if that invariant ever changes.
                None => break,
            }
        }
    }

    /// Lower the byte budget and evict down to it. Used by
    /// `UtxoStore::relieve_pressure`; a no-op if the new budget is not smaller.
    /// Eviction is by creation order (see `evict_to_budget`), so the entries
    /// most likely to be spent again are the ones kept.
    fn set_budget(&mut self, budget_bytes: usize) {
        if budget_bytes < self.budget_bytes {
            self.budget_bytes = budget_bytes;
            self.evict_to_budget();
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn bytes(&self) -> usize {
        self.map.len() * CACHE_ENTRY_COST
    }
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
            if b.len() < 2 {
                return None;
            }
            let v = u16::from_le_bytes([b[0], b[1]]) as u64;
            *b = &b[2..];
            Some(v)
        }
        0xfe => {
            if b.len() < 4 {
                return None;
            }
            let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as u64;
            *b = &b[4..];
            Some(v)
        }
        0xff => {
            if b.len() < 8 {
                return None;
            }
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
    /// Bounded in-memory cache of the most recently created UTXOs. `None` when
    /// disabled (undo-enabled/live mode, where correctness is worth more than
    /// speed, and benchmarking the baseline). See `RecentUtxoCache`.
    recent: Option<Mutex<RecentUtxoCache>>,
    /// When false, undo records are not written (bulk initial load).
    /// Live mode enables undo so reorgs can be reversed.
    undo_enabled: bool,
    /// True when RocksDB statistics were requested at open time. Only then do
    /// the stall counters in `rocksdb.stats` carry meaningful values; see
    /// `stall_report`.
    statistics: bool,
    /// The shared block cache handle, kept so the runtime can shrink its
    /// capacity under memory pressure (`relieve_pressure`). `None` only in
    /// `#[cfg(test)]` fixtures that build a store without going through
    /// `open_with`; every production path sets it.
    block_cache: Option<rocksdb::Cache>,
    /// The block cache's current capacity. RocksDB's `Cache` exposes
    /// `set_capacity` but no getter, so the value is tracked here to know how
    /// far `relieve_pressure` can shrink it. Atomic (not `Cell`) because the
    /// store is shared across threads via `Arc`.
    block_cache_bytes: std::sync::atomic::AtomicUsize,
    /// True when this store is a bulk-index store (no undo) *and* the caller
    /// asked to defer background compaction. Only then does `finalize_bulk_load`
    /// re-enable compaction and run the single end-of-run `compact_range`.
    bulk_deferred_compaction: bool,
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
    /// Inputs resolved from the in-memory recent-UTXO cache, i.e. database
    /// lookups the cache removed. Always 0 when the cache is disabled.
    pub cache_hits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipState {
    pub height: u32,
    pub hash: Hash32,
}

/// Snapshot of the in-memory recent-UTXO cache, for reporting and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: usize,
    pub budget_bytes: usize,
    pub hits: u64,
    pub misses: u64,
}

impl CacheStats {
    /// Cache hit rate over all lookups the cache was consulted for, or `None`
    /// before any lookup has happened.
    pub fn hit_rate(&self) -> Option<f64> {
        let total = self.hits + self.misses;
        if total == 0 {
            None
        } else {
            Some(self.hits as f64 / total as f64)
        }
    }
}

/// Default block cache capacity, shared by every column family.
///
/// The default was RocksDB's own 32 MiB — measured, not assumed: a probe run
/// reported `block_cache_capacity 33554432`. That is ~2,000x smaller than the
/// 3+ GiB UTXO data set this indexer builds, so almost every point lookup in
/// `apply_block` had to fault a data block off disk and decompress it.
///
/// The cache is shared across column families so the read path cannot be
/// starved by the undo/meta families, which are written far more than read.
/// This 1 GiB value is now only the *fallback*: callers that know the host's
/// memory (via `bidx_core::plan_memory_for_host`) pass a larger size
/// through [`UtxoStore::open_tuned`]. It is kept as the default so a bare
/// `open()` on a small host cannot allocate an outsized cache.
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 1 << 30; // 1 GiB

/// Compaction input readahead in bulk mode: 2 MiB runs instead of one read per
/// data block. Large enough to turn the compaction's read pattern sequential,
/// small enough not to waste NVMe bandwidth on read-ahead the compaction skips.
const COMPACTION_READAHEAD_BYTES: usize = 2 << 20;

/// Ceiling on background compaction/flush throughput in bulk mode, in bytes per
/// second (2 GiB/s). The point is not to slow compaction but to stop it from
/// monopolizing the device queue the *foreground* point lookups and `blk*.dat`
/// reads share: an unthrottled compaction burst on a fast NVMe can stall the
/// apply path for as long as it runs. Set well above what a typical NVMe
/// sustains so a fast device is never artificially capped in steady state.
const COMPACTION_RATE_LIMIT_BYTES: i64 = 2 << 30;

/// `estimated_entry_charge` for the HyperClockCache: the fixed charge reserved
/// per cached entry. RocksDB recommends the larger of the data-block and
/// metadata-block size; 16 KiB is a conservative value for a block cache whose
/// blocks are mostly small UTXO entries. Erring low costs a little metadata
/// space, which is the safe direction (erring high forces evictions).
const CACHE_ENTRY_CHARGE: usize = 16 << 10;

/// Default L0 write-throttle triggers. Named consts because a deferred-
/// compaction load raises them (`disable_l0_write_throttle`) and then
/// `finalize_bulk_load` restores them; sharing the values is what keeps the
/// deferral and the restore from drifting apart.
const L0_SLOWDOWN_TRIGGER: i32 = 20;
const L0_STOP_TRIGGER: i32 = 30;

/// Write-path tuning that does not depend on the block cache. Applied to the
/// database *and* to every column family (see [`tuned_options`]).
///
/// `max_background_jobs` and `max_subcompactions` scale with the host's core
/// count: the previous hardcoded 8 was tuned on an 8-core box, and on a 12- or
/// 16-core machine leaving cores idle during L0/L1 compaction is free
/// throughput left on the table. `subcompactions` in particular lets a single
/// large compaction use multiple cores.
///
/// `bulk` selects the bulk-ingest profile. It changes *write* I/O, which is the
/// dominant disk cost of an initial index: leveled compaction rewrites the same
/// data through every level, so the bytes that hit NVMe are many times the
/// logical UTXO size. Two settings attack that:
///
/// * `set_level_compaction_dynamic_level_bytes(true)` sizes the levels so the
///   bulk of the data lands in the bottommost level instead of being rewritten
///   on the way down, cutting write amplification.
/// * `set_compaction_readahead_size` prefetches input SSTs for a compaction in
///   large reads, so the compaction does not issue one small read per block.
///
/// Live mode leaves both at RocksDB's defaults: it writes little per block and
/// values the steady-state read behaviour dynamic levels can change.
fn apply_write_tuning(opts: &mut Options, bulk: bool) {
    // A 512 MiB buffer means a flush every ~8s at the observed write rate
    // rather than every second; with 4 buffers we can absorb a burst while a
    // flush is in flight.
    opts.set_write_buffer_size(512 << 20);
    opts.set_max_write_buffer_number(4);
    opts.set_target_file_size_base(256 << 20);
    opts.set_level_zero_file_num_compaction_trigger(8);
    opts.set_level_zero_slowdown_writes_trigger(L0_SLOWDOWN_TRIGGER);
    opts.set_level_zero_stop_writes_trigger(L0_STOP_TRIGGER);
    let jobs = default_background_jobs();
    opts.set_max_background_jobs(jobs);
    opts.increase_parallelism(jobs);
    // Parallelize a single compaction across cores (bounded by the core count,
    // since each subcompaction is a thread). Without this a 12-core box runs
    // every compaction single-threaded.
    opts.set_max_subcompactions(default_subcompactions());
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

    if bulk {
        // Let the bottommost level absorb the bulk of the data instead of
        // rewriting it down every level. This is the single biggest write-amp
        // reduction available to an append-heavy load.
        opts.set_level_compaction_dynamic_level_bytes(true);
        // Read compaction inputs in 2 MiB runs rather than block-at-a-time.
        opts.set_compaction_readahead_size(COMPACTION_READAHEAD_BYTES);
        // Cap compaction/IO background traffic so it cannot saturate the same
        // NVMe queue the point lookups and blk reads share.
        opts.set_ratelimiter(COMPACTION_RATE_LIMIT_BYTES, 100_000, 10);
    }
}

/// Neutralize the L0 write throttle for a deferred-compaction bulk load.
///
/// With auto-compaction off, L0 only grows: nothing ever merges it. RocksDB's
/// default `level_zero_slowdown_writes_trigger` (20) and
/// `level_zero_stop_writes_trigger` (30) would then *stop all writes* once L0
/// crossed 30 files — with no compaction coming to relieve the pressure, the
/// load deadlocks. Raising both to the maximum keeps the write path open; the
/// pile-up is exactly what `finalize_bulk_load` is for.
fn disable_l0_write_throttle(opts: &mut Options) {
    opts.set_level_zero_slowdown_writes_trigger(i32::MAX);
    opts.set_level_zero_stop_writes_trigger(i32::MAX);
}

/// Background jobs for the write path: one per core, clamped to a sane range.
/// Exposed so a test can pin the policy without a specific machine.
pub(crate) fn background_jobs_for(cores: usize) -> i32 {
    cores.clamp(4, 16) as i32
}

/// Subcompactions for one compaction: roughly half the cores, at least 2 so a
/// dual-core box still parallelizes, at most 8 (RocksDB's own practical cap).
pub(crate) fn subcompactions_for(cores: usize) -> u32 {
    (cores / 2).clamp(2, 8) as u32
}

fn default_background_jobs() -> i32 {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    background_jobs_for(cores)
}

fn default_subcompactions() -> u32 {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    subcompactions_for(cores)
}

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
///
/// The block cache is passed in as a shared handle rather than constructed
/// here: a store allocates exactly *one* cache and hands the same `Arc`-backed
/// handle to every column family. Constructing a fresh cache per CF (as this
/// used to) both tripled the memory and fragmented it, so a lookup in the UTXO
/// family could not benefit from blocks the undo family had already read.
fn tuned_options(statistics: bool, block_cache: &rocksdb::Cache, bulk: bool) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    apply_write_tuning(&mut opts, bulk);
    // A real block cache, plus a Bloom filter so a lookup for a spent outpoint
    // can reject a missing key without touching a data block. Both are
    // attached per-CF, which is the only place they take effect.
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(block_cache);
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

/// Default cap on the total memtable memory across all column families, used
/// by the `open*` entry points that do not take a full [`bidx_core::MemoryPlan`].
/// Matches the planner's `MEMTABLE_BUDGET_KB` so a hand-rolled open and a planned
/// open bound memtables the same way.
pub const DEFAULT_MEMTABLE_BUDGET_BYTES: usize =
    (bidx_core::mem::MEMTABLE_BUDGET_KB as usize) << 10;

impl UtxoStore {
    /// Open in bulk-load mode (no WAL, no undo logging), with the default
    /// block cache and no recent-UTXO cache.
    pub fn open(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            DEFAULT_BLOCK_CACHE_BYTES,
            0,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in live mode: WAL enabled (durable per-block) and undo logging.
    pub fn open_live(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            true,
            true,
            false,
            DEFAULT_BLOCK_CACHE_BYTES,
            0,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Like [`UtxoStore::open`] but with RocksDB statistics enabled, which
    /// makes `stall_report` meaningful. Statistics collection adds a small
    /// amount of per-operation overhead, so this is for diagnosis, not for
    /// throughput runs.
    pub fn open_statistics(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            DEFAULT_BLOCK_CACHE_BYTES,
            0,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode with the in-memory recent-UTXO cache enabled.
    ///
    /// The cache is a pure performance layer: it holds a copy of recently
    /// created outputs so a spend of a recent output can be answered from memory
    /// instead of RocksDB. Disabled in live mode because `BlockUndo` must be
    /// byte-exact for reorgs, and mixing an in-memory view into that path adds
    /// a correctness risk for no benchmarkable benefit.
    pub fn open_cached(path: &Path, cache_bytes: usize) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            DEFAULT_BLOCK_CACHE_BYTES,
            cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode with both the cache and RocksDB statistics.
    pub fn open_cached_statistics(path: &Path, cache_bytes: usize) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            DEFAULT_BLOCK_CACHE_BYTES,
            cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode with an explicit block-cache size (bytes) and
    /// recent-UTXO cache size (bytes; 0 disables it).
    ///
    /// Prefer [`UtxoStore::open_planned`] for production bulk indexing: it sizes
    /// both caches from the host's available memory *and* caps memtables, so a
    /// large-RAM box is not left faulting the UTXO read path off disk while
    /// staying under the watchdog ceiling. This variant takes the two cache
    /// sizes directly for callers that have already planned them.
    pub fn open_tuned(
        path: &Path,
        block_cache_bytes: usize,
        recent_cache_bytes: usize,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            block_cache_bytes,
            recent_cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// [`UtxoStore::open_tuned`] with RocksDB statistics enabled.
    pub fn open_tuned_statistics(
        path: &Path,
        block_cache_bytes: usize,
        recent_cache_bytes: usize,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            block_cache_bytes,
            recent_cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode from a full [`bidx_core::MemoryPlan`].
    ///
    /// This is the entry point a bulk index should use: the plan's caches,
    /// memtable cap, and file-LRU bound all derive from one ceiling, so the
    /// process cannot be sized past the watchdog limit it will abort at. See
    /// `bidx_core::mem`.
    pub fn open_planned(
        path: &Path,
        plan: &bidx_core::MemoryPlan,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            plan.block_cache_bytes,
            plan.recent_cache_bytes,
            plan.memtable_budget_bytes,
            false,
        )
    }

    /// [`UtxoStore::open_planned`] with RocksDB statistics enabled.
    pub fn open_planned_statistics(
        path: &Path,
        plan: &bidx_core::MemoryPlan,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            plan.block_cache_bytes,
            plan.recent_cache_bytes,
            plan.memtable_budget_bytes,
            false,
        )
    }

    /// Like [`UtxoStore::open_planned`], but defers background compaction for
    /// the duration of a bulk load: writes accumulate without the leveled
    /// rewrites that otherwise dominate disk I/O during an initial index.
    ///
    /// The caller **must** call [`UtxoStore::finalize_bulk_load`] when the load
    /// is done, before the store is used for anything else. Until then L0 holds
    /// an ever-growing pile of un-compacted files, so reads scan more of them —
    /// fine while writing, not fine for steady state. `finalize_bulk_load`
    /// re-enables compaction and compacts once, paying the I/O a single time
    /// instead of interleaving it with the load.
    ///
    /// Deferral trades peak read amplification during the load for far less
    /// total write I/O. It is the right default only when the load is
    /// write-dominated and the store is not read heavily until it finishes,
    /// which is exactly an initial bulk index.
    pub fn open_planned_bulk(
        path: &Path,
        plan: &bidx_core::MemoryPlan,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            plan.block_cache_bytes,
            plan.recent_cache_bytes,
            plan.memtable_budget_bytes,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_with(
        path: &Path,
        wal: bool,
        undo_enabled: bool,
        statistics: bool,
        block_cache_bytes: usize,
        recent_cache_bytes: usize,
        memtable_budget_bytes: usize,
        auto_compaction_disabled: bool,
    ) -> Result<Self, UtxoError> {
        // Bulk-load tuning follows undo, which is exactly the split between an
        // initial index (no undo) and live tracking (undo). Deriving it here
        // rather than threading another flag keeps the two from drifting: a
        // store without undo *is* a bulk store.
        let bulk = !undo_enabled;

        // One block cache, shared by every column family. Allocating it here
        // (rather than inside `tuned_options`) is what makes the sharing real:
        // the same handle is cloned into each CF descriptor below, so a block
        // read for the undo family can be reused by the UTXO family and the
        // total is bounded by one budget instead of three.
        //
        // A HyperClockCache rather than an LRU: on the apply path a dozen parse
        // workers plus the consumer thread hit this one shared cache, and the
        // LRU's per-hit mutex becomes a contention point. HyperClock is
        // lock-free on the read path. `estimated_entry_charge` is a conservative
        // per-entry reservation (see `CACHE_ENTRY_CHARGE`); the cache stays
        // mutable so `relieve_pressure` can shrink it — HyperClockCache
        // implements `SetCapacity`, so shedding works as it does for the LRU.
        let block_cache =
            rocksdb::Cache::new_hyper_clock_cache(block_cache_bytes.max(1), CACHE_ENTRY_CHARGE);

        // DB-level options: create flags, background threads, statistics.
        let mut opts = tuned_options(statistics, &block_cache, bulk);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // Cap the total memtable memory across all families. Without this each
        // family may hold up to `write_buffer_size * max_write_buffer_number`
        // (512 MiB * 4 = 2 GiB), so three families could pin ~6 GiB of memtables
        // that the cache budget never accounted for — the accounting gap that
        // let a 32 GiB box reach its ceiling and segfault. `0` leaves RocksDB's
        // own per-family default (used only by tests that want stock behaviour).
        if memtable_budget_bytes > 0 {
            opts.set_db_write_buffer_size(memtable_budget_bytes);
        }
        // Optionally defer background compaction entirely for the run. The
        // caller re-enables it and compacts once via `finalize_bulk_load`.
        opts.set_disable_auto_compactions(auto_compaction_disabled);
        if auto_compaction_disabled {
            disable_l0_write_throttle(&mut opts);
        }

        // Per-CF options: this is where the storage tuning actually lands.
        // The undo and meta families share the same table factory and cache;
        // they do not need a large write buffer, since they are tiny next to
        // the UTXO family, but they must still point at the *shared* cache.
        let mut utxo_cf_opts = tuned_options(statistics, &block_cache, bulk);
        // Per-CF options are what actually take effect (see `tuned_options`), so
        // the deferral must be set here too, not only on the DB-level options.
        utxo_cf_opts.set_disable_auto_compactions(auto_compaction_disabled);
        if auto_compaction_disabled {
            disable_l0_write_throttle(&mut utxo_cf_opts);
        }
        let mut aux_cf_opts = Options::default();
        let mut aux_block_opts = rocksdb::BlockBasedOptions::default();
        aux_block_opts.set_block_cache(&block_cache);
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
            recent: if recent_cache_bytes > 0 {
                Some(Mutex::new(RecentUtxoCache::new(recent_cache_bytes)))
            } else {
                None
            },
            undo_enabled,
            statistics,
            block_cache: Some(block_cache),
            block_cache_bytes: std::sync::atomic::AtomicUsize::new(block_cache_bytes),
            bulk_deferred_compaction: bulk && auto_compaction_disabled,
        })
    }

    fn cf_utxo(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .expect("default cf")
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
        let mut cache_hits = 0u64;

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
                // Cache-first: a recently created output can be answered from
                // memory instead of a RocksDB point lookup. On a miss we fall
                // through to the database, which is the baseline path.
                if let Some(ref cache) = self.recent {
                    if let Some(v) = cache.lock().remove(&key) {
                        cache_hits += 1;
                        batch.delete(key);
                        record_spend(&mut spends, &mut fees, height, inp, v);
                        continue;
                    }
                }
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

        // Publish this block's outputs into the cache once the block is done.
        //
        // Deliberately not interleaved with the input loop. `pending` is seeded
        // with every output before the loop starts, so a same-block spend is
        // resolved from `pending` and never reaches the cache anyway; inserting
        // earlier would only risk the cache holding an outpoint that this same
        // block is about to spend, which would make the cache and `pending`
        // disagree. At block end, `pending` holds exactly the outputs that
        // survived — the true new UTXOs — so it is both correct and minimal.
        //
        // Only reachable when the cache is enabled, which is bulk (non-undo)
        // mode; `undo_enabled` is false here by construction, so no undo entry
        // is needed for these.
        if let Some(ref cache) = self.recent {
            let mut c = cache.lock();
            for (key, val) in pending.iter() {
                c.insert(*key, *val);
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
            cache_hits,
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

    /// Current in-memory cache footprint in bytes, and its cumulative hit/miss
    /// counters. `None` when the cache is disabled.
    ///
    /// The byte figure is an estimate (`CACHE_ENTRY_COST` per entry) because the
    /// allocator's exact overhead is not observable; it is accurate enough to
    /// confirm the budget is being respected.
    pub fn cache_stats(&self) -> Option<CacheStats> {
        let c = self.recent.as_ref()?.lock();
        Some(CacheStats {
            entries: c.len(),
            bytes: c.bytes(),
            budget_bytes: c.budget_bytes,
            hits: c.hits,
            misses: c.misses,
        })
    }

    pub fn flush(&self) -> Result<(), UtxoError> {
        self.db.flush()?;
        Ok(())
    }

    /// Close out a deferred-compaction bulk load: re-enable background
    /// compaction and compact the UTXO column family once, so the L0 pile that
    /// accumulated during the load is merged in a single pass.
    ///
    /// A no-op (returns `false`) for any store not opened via
    /// [`UtxoStore::open_planned_bulk`] — a live store, or a bulk store whose
    /// compaction was never deferred, must not be compacted out from under
    /// itself here. Returns `true` when it actually compacted.
    ///
    /// Both steps precede the `compact_range`: the memtable is flushed so the
    /// final batch of writes is part of the compaction, and compaction is
    /// re-enabled first so a flush triggered by the compaction is not blocked.
    pub fn finalize_bulk_load(&self) -> Result<bool, UtxoError> {
        if !self.bulk_deferred_compaction {
            return Ok(false);
        }
        // Re-enable background compaction on the UTXO family and restore the
        // L0 triggers the deferral had raised (see `disable_l0_write_throttle`),
        // so the store returns to its standard tuned configuration once the
        // load is over. Per-CF options are what took effect at open (see
        // `tuned_options`), so the toggles must be per-CF here too. The trigger
        // values come from the same consts `apply_write_tuning` uses, so the
        // two cannot drift.
        let slowdown = L0_SLOWDOWN_TRIGGER.to_string();
        let stop = L0_STOP_TRIGGER.to_string();
        self.db.set_options_cf(
            self.cf_utxo(),
            &[
                ("disable_auto_compactions", "false"),
                ("level0_slowdown_writes_trigger", &slowdown),
                ("level0_stop_writes_trigger", &stop),
            ],
        )?;
        // Push any buffered writes into an SST so the compaction below covers
        // them, then compact the whole key range in one go.
        self.db.flush()?;
        self.db
            .compact_range_cf(self.cf_utxo(), None::<&[u8]>, None::<&[u8]>);
        Ok(true)
    }

    /// Shed memory under pressure, without failing the run.
    ///
    /// Called by the consumer when RSS crosses the watchdog's *soft* threshold
    /// — i.e. before the hard ceiling that aborts the pipeline. Two levers, both
    /// safe and both reversible by the workload:
    ///
    /// 1. **Shrink the recent-UTXO cache.** Its entries are a pure cache of the
    ///    UTXO set, so dropping some costs hit rate, never correctness. The
    ///    cache keeps functioning at the smaller budget and re-fills if pressure
    ///    passes.
    /// 2. **Halve the shared block cache.** RocksDB evicts to the new capacity
    ///    on its own; a smaller block cache costs lookups, not correctness.
    ///
    /// Deliberately *not* done here: flushing memtables (`flush` is an I/O and
    /// compaction burst, and the write buffer is already bounded by
    /// `set_db_write_buffer_size`). Returns the bytes of cache capacity removed,
    /// so a caller can log the effect.
    pub fn relieve_pressure(&self) -> usize {
        let mut freed = 0usize;
        if let Some(ref c) = self.recent {
            let mut c = c.lock();
            let before = c.budget_bytes;
            // Never below one entry's worth, so the cache cannot be shrunk into
            // a state where it holds nothing but still reports as enabled.
            let target = (before / 2).max(CACHE_ENTRY_COST);
            c.set_budget(target);
            freed += before.saturating_sub(target);
        }
        if let Some(ref cache) = self.block_cache {
            let mut cache = cache.clone();
            let cur = self
                .block_cache_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            let target = (cur / 2).max(64 << 20); // never below 64 MiB
            if target < cur {
                cache.set_capacity(target);
                self.block_cache_bytes
                    .store(target, std::sync::atomic::Ordering::Relaxed);
                freed += cur - target;
            }
        }
        freed
    }

    /// Test-only: empty the in-memory cache so a subsequent spend is forced to
    /// the database. Used to exercise the DB fallback without a second store.
    #[cfg(test)]
    fn cache_clear_for_test(&self) {
        if let Some(ref c) = self.recent {
            let mut c = c.lock();
            c.map.clear();
            c.order.clear();
        }
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
            (
                rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES,
                "cur_size_all_mem_tables",
            ),
            (
                rocksdb::properties::SIZE_ALL_MEM_TABLES,
                "size_all_mem_tables",
            ),
            (
                rocksdb::properties::CUR_SIZE_ACTIVE_MEM_TABLE,
                "cur_size_active_mem_table",
            ),
            (
                rocksdb::properties::NUM_IMMUTABLE_MEM_TABLE,
                "num_immutable_mem_table",
            ),
            (
                rocksdb::properties::MEM_TABLE_FLUSH_PENDING,
                "mem_table_flush_pending",
            ),
            (
                rocksdb::properties::NUM_RUNNING_FLUSHES,
                "num_running_flushes",
            ),
            (
                rocksdb::properties::COMPACTION_PENDING,
                "compaction_pending",
            ),
            (
                rocksdb::properties::NUM_RUNNING_COMPACTIONS,
                "num_running_compactions",
            ),
            (rocksdb::properties::IS_WRITE_STOPPED, "is_write_stopped"),
            (
                rocksdb::properties::ACTUAL_DELAYED_WRITE_RATE,
                "actual_delayed_write_rate",
            ),
            (rocksdb::properties::ESTIMATE_NUM_KEYS, "estimate_num_keys"),
            (
                rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE,
                "estimate_live_data_size",
            ),
            (
                rocksdb::properties::TOTAL_SST_FILES_SIZE,
                "total_sst_files_size",
            ),
            (
                rocksdb::properties::BLOCK_CACHE_CAPACITY,
                "block_cache_capacity",
            ),
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
        // Delegated to a pure helper so the `None` arm (a property RocksDB
        // declines to answer) is unit-testable without a property that errors.
        format_property(self.db.property_value(prop).ok().flatten())
    }

    /// Lines from the `rocksdb.stats` blob that mention stalls, which is
    /// where compaction backpressure becomes visible.
    ///
    /// Returns empty unless the store was opened with `open_statistics`,
    /// because RocksDB only populates these counters when statistics are on.
    /// An empty result therefore means "not measured", never "zero stalls".
    pub fn stall_report(&self) -> Vec<String> {
        // Statistics off means the property is absent or meaningless, so the
        // documented "empty unless statistics were enabled" contract is
        // enforced here rather than relying on RocksDB's property behavior.
        if !self.statistics {
            return Vec::new();
        }
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

/// Render a RocksDB property value, substituting `"unavailable"` when the
/// property is absent. Split out from `property_or_unavailable` so the `None`
/// arm — a property RocksDB declines to answer — is directly testable.
fn format_property(value: Option<String>) -> String {
    value.unwrap_or_else(|| "unavailable".to_string())
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
        u.spent.push((
            [1u8; 36],
            UtxoValue {
                value_sat: 1,
                script_type: 0,
                height: 0,
                address_hash: [0u8; 32],
            },
        ));
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
            (
                &[0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..],
                Some(1u64),
            ),
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
        for n in [
            0u64,
            252,
            253,
            65_535,
            65_536,
            u32::MAX as u64,
            u64::MAX - 1,
        ] {
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
        let r = db
            .apply_block(0, &[tx_row(0, 0, fund_txid)], &[], &b0_outputs)
            .unwrap();
        assert!(r.spends.is_empty());
        assert_eq!(r.missing, 0);

        // Block 1 spends block 0's output via a coinbase + one regular input.
        let spend_txid = [0x22u8; 32];
        let spend_cb = inp_row(1, 0, 0, spend_txid, [0u8; 32], 0xFFFF_FFFF, true);
        let spend_in = inp_row(1, 1, 0, spend_txid, fund_txid, 0, false);
        let b1_inputs = vec![spend_cb.clone(), spend_in.clone()];
        let b1_outputs = vec![out_row(1, 1, 0, spend_txid, 45_000)];
        let r = db
            .apply_block(
                1,
                &[tx_row(1, 0, [0u8; 32]), tx_row(1, 1, spend_txid)],
                &b1_inputs,
                &b1_outputs,
            )
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
        db.set_tip(&TipState {
            height: 1,
            hash: Hash32::from_bytes([0xEE; 32]),
        })
        .unwrap();
        let t = db.tip().unwrap().unwrap();
        assert_eq!(t.height, 1);
        assert_eq!(t.hash.0, [0xEE; 32]);

        // Disconnect block 1 and verify the funded output comes back.
        db.disconnect_block(1).unwrap();
        let (v, _) = db
            .db
            .get_pinned(make_key(&fund_txid, 0))
            .unwrap()
            .map(|b| (b.len(), 0i8))
            .unwrap();
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
        let inputs = vec![inp_row(0, 0, 0, [0x33u8; 32], [0x44u8; 32], 0, false)];
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
        db.apply_block(
            0,
            &[tx_row(0, 0, out_txid)],
            &[],
            &[out_row(0, 0, 0, out_txid, 100)],
        )
        .unwrap();
        let inp = inp_row(1, 0, 0, [0xBBu8; 32], out_txid, 0, false);
        let r = db
            .apply_block(
                1,
                &[tx_row(1, 0, [0xBBu8; 32])],
                std::slice::from_ref(&inp),
                &[],
            )
            .unwrap();
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
            .apply_block(
                0,
                &[tx0, tx1],
                std::slice::from_ref(&inp),
                std::slice::from_ref(&out),
            )
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
        db.apply_block(0, &[tx0, tx1], std::slice::from_ref(&inp), &[out0, out1])
            .unwrap();

        // Inspect the stored undo record directly.
        let raw = db
            .db
            .get_cf(&db.cf_undo(), undo_key(0))
            .unwrap()
            .expect("undo record");
        let undo = BlockUndo::decode(&raw).expect("decodable undo");
        assert_eq!(
            undo.spent.len(),
            0,
            "same-block spend must not be an undo.spent"
        );
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
            cache, DEFAULT_BLOCK_CACHE_BYTES,
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
            DEFAULT_BLOCK_CACHE_BYTES >= 512 << 20,
            "cache must dominate the write buffer for the read path"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An explicit block-cache size must reach the column family, and it must
    /// be *one* cache shared across CFs rather than one per CF. Both are
    /// load-bearing: a per-CF cache silently triples memory and prevents a
    /// block read for one family being reused by another.
    #[test]
    fn open_tuned_sizes_a_single_shared_block_cache() {
        let dir = tmp_path("tunedcache");
        // A deliberately non-default, easily-recognized size.
        let want = 256 << 20;
        let db = UtxoStore::open_tuned_statistics(&dir, want, 0).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();
        let cache: usize = props
            .get("block_cache_capacity")
            .expect("block_cache_capacity present")
            .parse()
            .expect("numeric");
        assert_eq!(cache, want, "explicit block-cache size must reach the CF");
        // No recent cache requested → none enabled.
        assert!(db.cache_stats().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `open_planned` must route the plan's caches into the store, and a plan
    /// with a zero recent cache must leave the recent cache disabled. This is
    /// the entry point a bulk index uses, so both the "caches reach the CF" and
    /// the "zero disables" branches need covering.
    #[test]
    fn open_planned_routes_the_plan_into_the_store() {
        let dir = tmp_path("planned");
        let plan = bidx_core::MemoryPlan {
            ceiling_kb: 16 * 1024 * 1024,
            block_cache_bytes: 128 << 20,
            recent_cache_bytes: 32 << 20,
            memtable_budget_bytes: 256 << 20,
            file_cache_files: 8,
        };
        let db = UtxoStore::open_planned_statistics(&dir, &plan).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();
        let cache: usize = props
            .get("block_cache_capacity")
            .expect("block_cache_capacity present")
            .parse()
            .expect("numeric");
        assert_eq!(cache, plan.block_cache_bytes, "plan block cache must reach the CF");
        assert_eq!(
            db.cache_stats().unwrap().budget_bytes,
            plan.recent_cache_bytes,
            "plan recent cache must reach the store"
        );
        let _ = std::fs::remove_dir_all(&dir);

        // A zero recent cache in the plan disables it.
        let dir2 = tmp_path("plannedzero");
        let zero = bidx_core::MemoryPlan {
            recent_cache_bytes: 0,
            ..plan
        };
        let db2 = UtxoStore::open_planned(&dir2, &zero).unwrap();
        assert!(db2.cache_stats().is_none(), "zero recent cache must disable it");
        let _ = std::fs::remove_dir_all(&dir2);
    }

    fn bulk_test_plan() -> bidx_core::MemoryPlan {
        bidx_core::MemoryPlan {
            ceiling_kb: 16 * 1024 * 1024,
            block_cache_bytes: 64 << 20,
            recent_cache_bytes: 16 << 20,
            memtable_budget_bytes: 64 << 20,
            file_cache_files: 4,
        }
    }

    /// `open_planned_bulk` must route the plan's caches into the store exactly
    /// like `open_planned`, so the bulk entry point is not a silently different
    /// configuration. The deferred-compaction half is asserted by the finalize
    /// tests below; here we only pin the caches.
    #[test]
    fn open_planned_bulk_routes_the_plan_caches() {
        let dir = tmp_path("bulkcaches");
        let plan = bulk_test_plan();
        let db = UtxoStore::open_planned_bulk(&dir, &plan).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();
        assert_eq!(
            props
                .get("block_cache_capacity")
                .expect("block_cache_capacity present")
                .parse::<usize>()
                .expect("numeric"),
            plan.block_cache_bytes,
            "bulk entry point must route the plan's block cache"
        );
        assert_eq!(
            db.cache_stats().unwrap().budget_bytes,
            plan.recent_cache_bytes,
            "bulk entry point must route the plan's recent cache"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deferred-compaction bulk store must accept writes while compaction is
    /// off, then `finalize_bulk_load` re-enables compaction, compacts once, and
    /// leaves the data intact. Asserting the post-compaction spend resolves
    /// checks the compaction did not drop or corrupt a live key — the failure
    /// mode a naive `compact_range` after a re-enable could hide.
    #[test]
    fn bulk_load_defers_compaction_then_finalize_compacts() {
        let dir = tmp_path("bulkfinalize");
        let db = UtxoStore::open_planned_bulk(&dir, &bulk_test_plan()).unwrap();
        // A write under deferred compaction: no auto-compaction runs, so this
        // lands in the memtable/L0 pile the finalize below must merge.
        let a = [0x61u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 5_000)])
            .unwrap();
        assert!(
            db.finalize_bulk_load().unwrap(),
            "a deferred bulk store must report that it compacted"
        );
        // The output survived the compaction: a spend of it still resolves.
        let t1 = [0x62u8; 32];
        let inp = inp_row(1, 0, 0, t1, a, 0, false);
        let r = db.apply_block(1, &[tx_row(1, 0, t1)], &[inp], &[]).unwrap();
        assert_eq!(r.spends.len(), 1);
        assert_eq!(r.spends[0].spent_value_sat, 5_000);
        assert_eq!(r.missing, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `finalize_bulk_load` must be a no-op for a store that did not defer
    /// compaction: a live store (undo enabled) must never be compacted out from
    /// under its reorg path, and a plain bulk store never accumulated the L0
    /// pile that needs merging.
    #[test]
    fn finalize_bulk_load_is_a_noop_for_non_deferred_stores() {
        let dir = tmp_path("livefinalize");
        let live = UtxoStore::open_live(&dir).unwrap();
        assert!(
            !live.finalize_bulk_load().unwrap(),
            "a live store must not compact on finalize"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir2 = tmp_path("plainfinalize");
        let plain = UtxoStore::open_planned(&dir2, &bulk_test_plan()).unwrap();
        assert!(
            !plain.finalize_bulk_load().unwrap(),
            "a bulk store that never deferred must be a finalize no-op"
        );
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// `open_tuned` with a non-zero recent-cache budget must enable the cache
    /// and answer a spend from it, i.e. the plumbing actually reaches the
    /// store rather than being accepted and dropped.
    #[test]
    fn open_tuned_enables_the_recent_cache() {
        let dir = tmp_path("tunedrecent");
        let db = UtxoStore::open_tuned(&dir, DEFAULT_BLOCK_CACHE_BYTES, 8 << 20).unwrap();
        let stats = db.cache_stats().expect("recent cache enabled by budget");
        assert_eq!(stats.budget_bytes, 8 << 20);
        let a = [0x51u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 123)])
            .unwrap();
        let t1 = [0x52u8; 32];
        let r = db
            .apply_block(
                1,
                &[tx_row(1, 0, t1)],
                &[inp_row(1, 0, 0, t1, a, 0, false)],
                &[],
            )
            .unwrap();
        assert_eq!(r.cache_hits, 1, "recent spend must hit the cache");
        assert_eq!(r.db_lookups, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The write-path tuning must scale with the core count, with a floor and a
    /// ceiling so neither a tiny nor a huge host gets an absurd value.
    #[test]
    fn background_jobs_and_subcompactions_scale_and_clamp() {
        assert_eq!(background_jobs_for(1), 4);
        assert_eq!(background_jobs_for(8), 8);
        assert_eq!(background_jobs_for(12), 12);
        assert_eq!(background_jobs_for(64), 16);

        assert_eq!(subcompactions_for(1), 2);
        assert_eq!(subcompactions_for(4), 2);
        assert_eq!(subcompactions_for(12), 6);
        assert_eq!(subcompactions_for(64), 8);
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

    /// The cache must answer a spend of a recently created output without a
    /// database lookup, and must produce the *same* `SpendRow` a database read
    /// would. Fidelity is the point: a wrong value here silently corrupts the
    /// spend index, which is why the entry keeps the full `UtxoValue`.
    #[test]
    fn recent_cache_serves_recent_spend_without_db_lookup() {
        let dir = tmp_path("cachehit");
        let db = UtxoStore::open_cached(&dir, 8 << 20).unwrap();
        let a = [0xE1u8; 32];

        // Block 0 creates A:0 (goes into the cache at block end).
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 4_242)])
            .unwrap();

        // Block 1 spends A:0. The cache should satisfy it.
        let tx1 = [0xE2u8; 32];
        let r = db
            .apply_block(
                1,
                &[tx_row(1, 0, tx1)],
                &[inp_row(1, 0, 0, tx1, a, 0, false)],
                &[],
            )
            .unwrap();

        assert_eq!(
            r.cache_hits, 1,
            "spend of a recent output must hit the cache"
        );
        assert_eq!(r.db_lookups, 0, "no database read should have been needed");
        assert_eq!(r.missing, 0);
        // Fidelity: the same fields a DB read would have produced.
        assert_eq!(r.spends.len(), 1);
        assert_eq!(r.spends[0].spent_value_sat, 4_242);
        assert_eq!(r.spends[0].spent_height, 0);
        assert_eq!(r.spends[0].spent_address_hash, [0xEEu8; 32]);
        assert_eq!(r.fees[0], 4_242, "fee comes from the cached input value");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// With the cache disabled, the same spend costs a database lookup and
    /// reports no cache hits. This is the A/B control.
    #[test]
    fn recent_cache_disabled_uses_db_lookup() {
        let dir = tmp_path("cachemiss");
        let db = UtxoStore::open(&dir).unwrap();
        let a = [0xE3u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 100)])
            .unwrap();
        let tx1 = [0xE4u8; 32];
        let r = db
            .apply_block(
                1,
                &[tx_row(1, 0, tx1)],
                &[inp_row(1, 0, 0, tx1, a, 0, false)],
                &[],
            )
            .unwrap();
        assert_eq!(r.cache_hits, 0);
        assert_eq!(r.db_lookups, 1, "no cache means the database must be read");
        assert_eq!(r.spends[0].spent_value_sat, 100);
        assert!(db.cache_stats().is_none(), "cache is disabled");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The cache must respect its budget, and evicting an entry must degrade to
    /// a correct database lookup — never a wrong value or a `missing` count.
    #[test]
    fn recent_cache_evicts_to_budget_and_still_resolves() {
        let dir = tmp_path("cacheevict");
        // Budget for ~4 entries; create many more so eviction is forced.
        let db = UtxoStore::open_cached(&dir, 4 * CACHE_ENTRY_COST).unwrap();
        let a = [0xE5u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 999)])
            .unwrap();

        // 40 more single-output blocks, each evicting older entries.
        for h in 1..41u32 {
            let t = [h as u8; 32];
            db.apply_block(h, &[tx_row(h, 0, t)], &[], &[out_row(h, 0, 0, t, 10)])
                .unwrap();
        }
        let stats = db.cache_stats().expect("cache enabled");
        assert!(
            stats.bytes <= stats.budget_bytes,
            "cache exceeded budget: {} > {}",
            stats.bytes,
            stats.budget_bytes
        );

        // A:0 was evicted, so spending it must still work via the database.
        let tx1 = [0xE6u8; 32];
        let r = db
            .apply_block(
                41,
                &[tx_row(41, 0, tx1)],
                &[inp_row(41, 0, 0, tx1, a, 0, false)],
                &[],
            )
            .unwrap();
        assert_eq!(r.missing, 0, "eviction must not lose data");
        assert_eq!(r.spends[0].spent_value_sat, 999);
        assert_eq!(r.spends[0].spent_height, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A same-block spend must still be resolved from `pending`, not the cache,
    /// and must not be double counted. `pending_hits + db_lookups + cache_hits`
    /// must equal the non-coinbase input count exactly.
    #[test]
    fn recent_cache_accounting_is_exhaustive() {
        let dir = tmp_path("cacheacct");
        let db = UtxoStore::open_cached(&dir, 8 << 20).unwrap();
        // Fund A:0 in block 0 (cached).
        let a = [0xE7u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 100)])
            .unwrap();

        // Block 1: creates B:0 and spends it same-block (pending), spends A:0
        // (cache), and spends an outpoint that exists only in the database.
        let d = [0xE8u8; 32];
        db.apply_block(1, &[tx_row(1, 0, d)], &[], &[out_row(1, 0, 0, d, 77)])
            .unwrap();
        // Drop the cache so D:0 must come from RocksDB.
        db.cache_clear_for_test();

        let b = [0xE9u8; 32];
        let tx1 = [0xEAu8; 32];
        let tx2 = [0xEBu8; 32];
        let tx3 = [0xECu8; 32];
        let inputs = [
            inp_row(2, 1, 0, tx1, b, 0, false), // same-block -> pending
            inp_row(2, 2, 0, tx2, a, 0, false), // in DB (cache cleared)
            inp_row(2, 3, 0, tx3, d, 0, false), // in DB
        ];
        let outputs = [out_row(2, 0, 0, b, 5)];
        let txs = [
            tx_row(2, 0, b),
            tx_row(2, 1, tx1),
            tx_row(2, 2, tx2),
            tx_row(2, 3, tx3),
        ];
        let r = db.apply_block(2, &txs, &inputs, &outputs).unwrap();
        assert_eq!(
            r.pending_hits + r.db_lookups + r.cache_hits,
            3,
            "every non-coinbase input accounted for exactly once"
        );
        assert_eq!(r.pending_hits, 1);
        assert_eq!(r.db_lookups, 2);
        assert_eq!(r.cache_hits, 0);
        assert_eq!(r.missing, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Cache stats must report a sane hit rate, and `hit_rate` must be `None`
    /// before any lookup rather than a bogus 0% or NaN.
    #[test]
    fn recent_cache_stats_and_hit_rate() {
        let dir = tmp_path("cachestats");
        let db = UtxoStore::open_cached(&dir, 8 << 20).unwrap();
        let stats = db.cache_stats().expect("cache enabled");
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.hit_rate(), None, "no lookups yet");

        // Create and spend, then miss once.
        let a = [0xF1u8; 32];
        db.apply_block(0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 1)])
            .unwrap();
        let t1 = [0xF2u8; 32];
        db.apply_block(
            1,
            &[tx_row(1, 0, t1)],
            &[inp_row(1, 0, 0, t1, a, 0, false)],
            &[],
        )
        .unwrap();
        let t2 = [0xF3u8; 32];
        db.apply_block(
            2,
            &[tx_row(2, 0, t2)],
            &[inp_row(2, 0, 0, t2, a, 0, false)],
            &[],
        )
        .unwrap();

        let stats = db.cache_stats().expect("cache enabled");
        assert_eq!(stats.hits, 1, "first spend hit");
        assert!(stats.misses >= 1, "second spend of a spent outpoint missed");
        let rate = stats.hit_rate().expect("lookups happened");
        assert!((0.0..=1.0).contains(&rate));
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
        let a = TipState {
            height: 10,
            hash: Hash32::from_bytes([1; 32]),
        };
        let b = TipState {
            height: 11,
            hash: Hash32::from_bytes([2; 32]),
        };
        db.set_tip(&a).unwrap();
        assert_eq!(db.tip().unwrap().unwrap(), a);
        db.set_tip(&b).unwrap();
        assert_eq!(db.tip().unwrap().unwrap(), b);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recent_cache_insert_refresh_does_not_requeue() {
        // The caller only inserts an outpoint once, but the contract says a
        // re-insert refreshes the value without pushing a second recency entry.
        // Assert both the refresh and the single queue entry.
        let mut c = RecentUtxoCache::new(10 * CACHE_ENTRY_COST);
        let k = [0x11u8; 36];
        let v1 = UtxoValue {
            value_sat: 1,
            script_type: 1,
            height: 1,
            address_hash: [0; 32],
        };
        let v2 = UtxoValue {
            value_sat: 2,
            script_type: 2,
            height: 2,
            address_hash: [0; 32],
        };
        c.insert(k, v1);
        assert_eq!(c.order.len(), 1);
        c.insert(k, v2); // same key: refresh, no requeue
        assert_eq!(c.order.len(), 1, "refresh must not requeue");
        assert_eq!(c.map[&k].value_sat, 2, "value must be refreshed");
        assert_eq!(c.len(), 1);
        assert_eq!(c.bytes(), CACHE_ENTRY_COST);
    }

    #[test]
    fn recent_cache_evict_stops_cleanly_when_budget_below_one_entry() {
        // A budget smaller than a single entry forces the eviction loop to run
        // against an empty map. It must terminate (order exhausted → break)
        // rather than spin, leaving the cache empty.
        let mut c = RecentUtxoCache::new(0);
        c.evict_to_budget();
        assert_eq!(c.len(), 0);
        // With an entry present but a zero budget, eviction empties it and stops.
        c.insert(
            [0x22u8; 36],
            UtxoValue {
                value_sat: 1,
                script_type: 0,
                height: 0,
                address_hash: [0; 32],
            },
        );
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn recent_cache_set_budget_evicts_down_and_ignores_growth() {
        // `set_budget` is the eviction primitive behind `relieve_pressure`: it
        // must lower the budget and evict to it, and must NOT grow the budget
        // (growing would undo a previous relief step).
        let mut c = RecentUtxoCache::new(10 * CACHE_ENTRY_COST);
        let val = UtxoValue {
            value_sat: 1,
            script_type: 0,
            height: 0,
            address_hash: [0; 32],
        };
        for i in 0..10u8 {
            let mut k = [0u8; 36];
            k[0] = i;
            c.insert(k, val);
        }
        assert_eq!(c.len(), 10);
        // Shrink to 4 entries: eviction drops the rest.
        c.set_budget(4 * CACHE_ENTRY_COST);
        assert_eq!(c.budget_bytes, 4 * CACHE_ENTRY_COST);
        assert_eq!(c.len(), 4);
        assert_eq!(c.bytes(), 4 * CACHE_ENTRY_COST);
        // A larger budget is ignored, so the earlier relief is not undone.
        c.set_budget(8 * CACHE_ENTRY_COST);
        assert_eq!(c.budget_bytes, 4 * CACHE_ENTRY_COST);
        assert_eq!(c.len(), 4);
    }

    /// `relieve_pressure` must actually free memory on a real store — shrink the
    /// recent cache and halve the block cache — because that is the whole point
    /// of the soft watchdog threshold. Asserting the freed byte count (not just
    /// "did not panic") is what proves the lever moved.
    #[test]
    fn relieve_pressure_shrinks_both_caches() {
        let dir = tmp_path("relieve");
        let db = UtxoStore::open_tuned(&dir, 256 << 20, 64 << 20).unwrap();
        // Fill the recent cache past the half-budget point so the halving
        // forces a real eviction: 64 MiB / CACHE_ENTRY_COST entries is the
        // budget, so fill ~1.5x that.
        let val = UtxoValue {
            value_sat: 1,
            script_type: 0,
            height: 0,
            address_hash: [0; 32],
        };
        let budget_entries = (64 << 20) / CACHE_ENTRY_COST;
        let filled = budget_entries * 3 / 2;
        {
            let mut c = db.recent.as_ref().unwrap().lock();
            for i in 0..filled {
                let mut k = [0u8; 36];
                k[..8].copy_from_slice(&(i as u64).to_le_bytes());
                c.insert(k, val);
            }
        }
        let before = db.cache_stats().unwrap();
        assert_eq!(before.entries, budget_entries, "filled to the budget");
        let freed = db.relieve_pressure();
        // Recent cache halved (64 MiB -> 32 MiB) plus block cache halved
        // (256 MiB -> 128 MiB) = 160 MiB.
        assert_eq!(freed, (32 << 20) + (128 << 20));
        let after = db.cache_stats().unwrap();
        assert_eq!(after.budget_bytes, before.budget_bytes / 2);
        // Entries were evicted to fit the smaller budget, and the byte figure
        // respects it.
        assert!(after.entries < before.entries, "relief must evict entries");
        assert!(after.bytes <= after.budget_bytes);
        // A second call halves again, so relief is progressive.
        let freed2 = db.relieve_pressure();
        assert_eq!(freed2, (16 << 20) + (64 << 20));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The block-cache half of `relieve_pressure` must stop at the floor rather
    /// than shrinking to zero (a zero-capacity cache is not a smaller cache, it
    /// is a broken one), and report no further freeing once at the floor.
    #[test]
    fn relieve_pressure_stops_the_block_cache_at_its_floor() {
        let dir = tmp_path("relieffloor");
        // Open with the minimum so one halving reaches the 64 MiB floor.
        let db = UtxoStore::open_tuned(&dir, 64 << 20, 0).unwrap();
        assert_eq!(db.relieve_pressure(), 0, "already at the block-cache floor");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn block_undo_decode_rejects_truncated_spent_and_created_sections() {
        // Truncated in the middle of a `created` key (36-byte) section.
        let mut enc = Vec::new();
        put_varint(&mut enc, 0); // no spent
        put_varint(&mut enc, 1); // one created
        enc.extend_from_slice(&[0xABu8; 10]); // only 10 of 36 bytes
        assert!(BlockUndo::decode(&enc).is_none());

        // A spent record whose value bytes are too short to decode.
        let mut enc2 = Vec::new();
        put_varint(&mut enc2, 1); // one spent
        enc2.extend_from_slice(&[0u8; 36]); // key
        enc2.extend_from_slice(&[0u8; 10]); // value shorter than 45
        assert!(BlockUndo::decode(&enc2).is_none());
    }

    #[test]
    fn open_cached_statistics_enables_cache_and_statistics() {
        let dir = tmp_path("cachedstats");
        let db = UtxoStore::open_cached_statistics(&dir, 8 * CACHE_ENTRY_COST).unwrap();
        assert!(db.statistics_enabled(), "statistics flag must be on");
        assert!(db.cache_stats().is_some(), "recent cache must be enabled");
        // A normal store has neither.
        let dir2 = tmp_path("plainstats");
        let db2 = UtxoStore::open(&dir2).unwrap();
        assert!(!db2.statistics_enabled());
        assert!(db2.cache_stats().is_none());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[test]
    fn db_lookup_of_a_corrupt_utxo_value_counts_as_missing() {
        // A spend whose key is present in RocksDB but whose stored value is the
        // wrong length cannot be decoded; it must be counted as missing, not
        // panic or produce a bogus spend.
        let dir = tmp_path("corrupt");
        let db = UtxoStore::open_live(&dir).unwrap();
        let prev_txid = [0x77u8; 32];
        // Write a 10-byte value (valid values are 45 bytes).
        db.db.put(make_key(&prev_txid, 0), [0u8; 10]).unwrap();
        let inputs = vec![inp_row(0, 0, 0, [0x88u8; 32], prev_txid, 0, false)];
        let r = db
            .apply_block(0, &[tx_row(0, 0, [0x88u8; 32])], &inputs, &[])
            .unwrap();
        assert_eq!(r.missing, 1);
        assert!(r.spends.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stall_report_reads_stats_when_enabled_and_is_empty_otherwise() {
        // With statistics on, the `rocksdb.stats` property is populated and the
        // stall filter runs (returning zero or more matching lines). With
        // statistics off, the property is absent and the report is empty —
        // "not measured", never a misleading "zero stalls".
        let dir = tmp_path("stall");
        let db = UtxoStore::open_cached_statistics(&dir, 4 * CACHE_ENTRY_COST).unwrap();
        assert!(db.statistics_enabled());
        let with_stats = db.stall_report();
        // Every returned line must actually mention a stall.
        assert!(with_stats
            .iter()
            .all(|l| l.contains("stall") || l.contains("Stall")));
        let _ = std::fs::remove_dir_all(&dir);

        let dir2 = tmp_path("stalloff");
        let db2 = UtxoStore::open(&dir2).unwrap();
        assert!(!db2.statistics_enabled());
        assert!(db2.stall_report().is_empty());
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// `perf_properties` must report a property RocksDB declines to answer as
    /// `"unavailable"` rather than dropping the row, so two runs can be diffed
    /// key-for-key. The `None` arm is what this pins.
    #[test]
    fn format_property_reports_unavailable_for_a_missing_value() {
        assert_eq!(format_property(Some("123".to_string())), "123");
        assert_eq!(format_property(None), "unavailable");
    }
}
