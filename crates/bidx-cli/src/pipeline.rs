//! Parse pipeline orchestration.
//!
//! Architecture:
//!   - Height ranges are parsed in parallel by a Rayon pool. Each worker
//!     opens (lazily, cached per-thread) the blk files it needs, seeks
//!     directly to each block's recorded location, and fully parses it.
//!   - Parsed blocks stream through a bounded crossbeam channel that
//!     preserves height order via a reorder buffer (next_expected).
//!   - A single consumer applies UTXO changes (RocksDB) in strict height
//!     order — the only stage that must be sequential — then enriches
//!     transactions with fees, updates the block's total_fee, and pushes
//!     everything into the Parquet sink, rotating part files every
//!     `blocks_per_part` blocks.

use crate::diskguard::{DiskGuardConfig, DiskRecorder, DEFAULT_CHECKPOINT_FILE_NAME};
use anyhow::{Context, Result};
use bidx_parser::{
    parse_block_full, parse_block_full_opts, read_xor_key_from_dir, BlkFile, FullBlock,
};
use bidx_store::{ParquetSink, SinkConfig, UtxoStore};
use crossbeam_channel::{bounded, Receiver};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
/// The RSS ceiling `bidx` will abort a run at, based on memory that is actually
/// available, not merely installed.
///
/// This is now a thin view of [`bidx_core::plan_memory_for_host`]'s ceiling, so
/// there is exactly one place the number is derived. The old standalone form
/// computed the ceiling from `MemAvailable` while the cache budget was computed
/// from the same field *independently* — the two could sum past the machine, and
/// the ceiling landed exactly where a RocksDB allocation could fail (which is a
/// segfault, not an error). Deriving both from one plan is the fix; see
/// `bidx_core::mem`.
#[cfg(test)]
pub(crate) fn max_rss_kb() -> u64 {
    bidx_core::plan_memory_for_host().ceiling_kb
}

use std::sync::Arc;
use tracing::{info, warn};

/// Reorder-buffer watermark shared with workers. If the consumer falls
/// more than this many blocks behind, workers spin briefly to give it
/// time to drain — prevents pathological growth when an early height is
/// slow and the ordered consumer can't drain the buffer. With each parsed
/// `FullBlock` typically <10 MB, 32 blocks ≈ 320 MB worst-case headroom.
///
/// Public so `bidx bench` can sweep it without a rebuild.
pub const REORDER_HIGH: usize = 32;
/// Worker -> consumer channel depth. Bounded tightly (`2 * REORDER_HIGH`) so
/// parsed `FullBlock`s cannot pile up by the hundreds in the channel while the
/// ordered consumer is inside a slow `apply_block` or flush.
pub const CHANNEL_DEPTH: usize = 64;

/// Consumer -> sink channel depth. The sink thread overlaps Parquet I/O with
/// the UTXO apply, so a small bound is enough to keep both busy while capping
/// the number of fully-materialized blocks in flight (each holds all of a
/// block's rows, ~a few MB at the tip).
pub const SINK_CHANNEL_DEPTH: usize = 8;

/// Sink -> ClickHouse part-loader channel depth. Bounds the number of completed
/// Parquet parts waiting on disk when `--stream-clickhouse --unlink-loaded-parts`
/// is active so a slow ClickHouse server backpressures the sink cleanly.
pub const PART_CHANNEL_DEPTH: usize = 4;

/// Optional pipelined ClickHouse ingestion configuration for `bidx parse`.
pub struct StreamClickHouseConfig {
    pub loader: bidx_loader::ChLoader,
    pub mode: bidx_loader::LoadMode,
    pub unlink_loaded_parts: bool,
}

/// Resolve the two cache sizes from the optional CLI overrides and the
/// host-derived budget. A `Some(mb)` override wins; `None` takes the planned
/// value. Split out so both the override and auto-sized branches are unit
/// testable without a live host.
fn resolve_cache_bytes(
    block_cache_mb: Option<usize>,
    utxo_cache_mb: Option<usize>,
    budget: bidx_core::CacheBudget,
) -> (usize, usize) {
    let block = block_cache_mb
        .map(|mb| mb.saturating_mul(1 << 20))
        .unwrap_or(budget.block_cache_bytes);
    let recent = utxo_cache_mb
        .map(|mb| mb.saturating_mul(1 << 20))
        .unwrap_or(budget.recent_cache_bytes);
    (block, recent)
}

/// Configuration for a parse run.
///
/// Deliberately not `Clone`/`Debug`: it holds a boxed `dyn RpcTransport` (see
/// `rpc_transport`), which is neither.
pub struct ParseConfig {
    pub blocks_dir: PathBuf,
    pub out: PathBuf,
    pub utxo: PathBuf,
    pub start: u32,
    pub end: Option<u32>,
    pub blocks_per_part: u32,
    pub threads: Option<usize>,
    /// If set, run the disk-space pre-flight check before parsing: query the
    /// node's tip over this RPC URL and project the index's final size. See
    /// `diskguard` module. If `None`, skip the check.
    pub rpc_url: Option<String>,
    pub rpc_cookie: Option<PathBuf>,
    /// Allow the user to skip the disk-space guard explicitly.
    pub skip_disk_check: bool,
    /// Override the checkpoint-file path. Defaults to
    /// `<parent-of-utxo>/bidx-disk-checkpoints.txt`.
    pub checkpoint_file: Option<PathBuf>,
    /// Network — auto-detected from the first blk file if `None`.
    pub network: Option<bidx_core::Network>,
    /// Optional set of block hashes that are orphaned (not on the active
    /// chain). Blocks whose hash is in this set are skipped during parse
    /// (their data is not indexed and UTXO state is not mutated).
    pub orphan_blocks: Option<Arc<FxHashSet<bidx_core::Hash32>>>,
    /// Injected RPC transport for the disk pre-flight. Production leaves this
    /// `None`, so the pre-flight builds an HTTP client from `rpc_url`/
    /// `rpc_cookie`; tests set it to a scripted transport to exercise the
    /// pre-flight (projection, orphan filtering, and their log branches)
    /// without a live node.
    pub rpc_transport: Option<Box<dyn bidx_live::RpcTransport>>,
    /// Capture raw witness stack elements into the optional witnesses table.
    pub capture_witness: bool,
    /// RocksDB block-cache size in MiB. `None` (the default) sizes it from the
    /// host's available memory via `bidx_core::plan_memory_for_host`. A larger
    /// cache is the single biggest lever on the apply path, because every spent
    /// input is a point lookup that otherwise faults a data block off disk.
    /// An override does not change the watchdog ceiling; the soft threshold
    /// still sheds the cache if the override turns out too large for the host.
    pub block_cache_mb: Option<usize>,
    /// In-memory recent-UTXO cache size in MiB. `None` (the default) sizes it
    /// from the host's available memory. The cache answers a spend of a
    /// recently created output without a RocksDB lookup at all. Only used in
    /// bulk mode (no undo); live mode ignores it.
    pub utxo_cache_mb: Option<usize>,
    /// Optional pipelined ClickHouse part-file loader: drops materialized views
    /// up front, streams each completed Parquet part into ClickHouse while
    /// parsing continues, and rebuilds derived indexes + views at completion.
    pub stream_clickhouse: Option<StreamClickHouseConfig>,
}

/// A parse-stage message: either a fully-parsed block, or a "skipped"
/// marker for a height whose block hash is in the orphan set (we skip its
/// data but must still advance the ordered consumer past that height).
pub(crate) enum WorkItem {
    Block { height: u32, block: FullBlock },
    Skipped { height: u32 },
}

pub fn run_parse(mut cfg: ParseConfig) -> Result<()> {
    info!(?cfg.blocks_dir, "building chain index");
    let (index, stats) = bidx_parser::build_chain_index(&cfg.blocks_dir)?;
    info!(
        tip = stats.tip_height,
        orphans = stats.orphans,
        "chain index ready"
    );

    let tip = index.tip_height().context("empty chain")?;
    let start = cfg.start.min(tip);
    let end = cfg.end.unwrap_or(tip + 1).min(tip + 1);
    if start >= end {
        anyhow::bail!("empty range: start {start} >= end {end}");
    }
    let total = (end - start) as u64;
    info!(start, end, total, "starting parse");

    // ---- disk-space pre-flight (only meaningful with --rpc-url) ----
    let network = cfg
        .network
        .or_else(|| bidx_core::Network::detect_from_blocks_dir(&cfg.blocks_dir))
        .context("could not detect network from blocks dir; pass --network")?;
    let parent_of_utxo = cfg
        .utxo
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(".")); // coverage-exempt: utxo is a store dir, never "/"
    let checkpoint_file = cfg
        .checkpoint_file
        .clone()
        .unwrap_or_else(|| parent_of_utxo.join(DEFAULT_CHECKPOINT_FILE_NAME));
    let disk_cfg: DiskGuardConfig = DiskGuardConfig {
        checkpoint_file,
        network,
        index_root: parent_of_utxo.clone(),
        tracked_dirs: vec![cfg.utxo.clone(), cfg.out.clone()],
    };

    if !cfg.skip_disk_check {
        if let Some(rpc_url) = &cfg.rpc_url {
            let node = match cfg.rpc_transport.take() {
                // Test/alternate transport injected by the caller.
                Some(t) => bidx_live::NodeClient::with_transport(t),
                None => bidx_live::NodeClient::new(bidx_live::RpcConfig {
                    url: rpc_url.clone(),
                    cookie_path: cfg.rpc_cookie.clone(),
                    user: None,
                    password: None,
                })
                .context("init RPC client for disk pre-flight")?,
            };
            let node_tip = node.get_block_count().context("getblockcount")?;
            let local_tip =
                bidx_core::read_checkpoints(&disk_cfg.checkpoint_file, disk_cfg.network)
                    .last()
                    .map(|c| c.height)
                    .unwrap_or(0);
            if local_tip == 0 && start == 0 {
                tracing::info!(
                    network = network.name(),
                    "no disk checkpoints yet — running without growth projection until \
                     height 100,000 is reached"
                );
            }
            disk_cfg.run_pre_flight(node_tip, local_tip, bidx_core::MIN_FREE_BYTES)?;

            // Fetch the set of orphaned (stale) blocks so the parallel
            // parser can skip them. Failure is non-fatal: proceed without
            // filtering (we log a warning).
            match node.orphan_block_hashes() {
                Ok(set) if !set.is_empty() => {
                    warn!(
                        count = set.len(),
                        "fetched orphan block hashes; they will be skipped"
                    );
                    cfg.orphan_blocks = Some(Arc::new(set));
                }
                Ok(_) => tracing::info!("no orphaned blocks reported by node"),
                Err(e) => warn!("orphan-block fetch failed, continuing without filter: {e}"),
            }
        } else {
            tracing::info!(
                "no --rpc-url given; skipping disk-space projection (use --rpc-url to enable)"
            );
        }
    }

    // Group block locations by file so each worker opens each file once.
    // We keep per-thread file caches instead: simpler and lets height order
    // interleave across files naturally.
    let index = Arc::new(index);

    // Plan the whole memory layout from the host's memory unless the caller
    // pinned the caches. The apply path is one point lookup per spent input,
    // and on a mainnet-sized UTXO set most of those miss the block cache and
    // fault a data block off disk — which is the dominant cost of a bulk index.
    // A 32 GiB box otherwise leaves ~29 GiB idle while it thrashes NVMe.
    //
    // Crucially this is *one* plan: the caches, the memtable cap, the decoded-
    // file LRU, and the watchdog ceiling all derive from the same number, so
    // `caches + memtables + file LRU + fixed reserve` cannot exceed the ceiling
    // the watchdog aborts at. Sizing the caches and the ceiling independently
    // (the previous behaviour) let the total reach the ceiling, so the watchdog
    // tripped exactly where a RocksDB allocation could fail — and RocksDB
    // segfaults on allocation failure instead of returning an error.
    let mut planned = bidx_core::plan_memory_for_host();
    let (block_cache_bytes, recent_cache_bytes) =
        resolve_cache_bytes(cfg.block_cache_mb, cfg.utxo_cache_mb, planned.caches());
    // An explicit cache override replaces the plan's cache sizes but *not* its
    // ceiling: the ceiling stays `MemAvailable`-derived, so a too-large
    // `--block-cache-mb` cannot raise it past what the kernel will grant. The
    // overridden caches are the operator's choice; the plan's memtable cap and
    // file-LRU bound stay in force, and the soft watchdog still sheds the
    // overridden caches if they turn out to be too big for the host.
    planned.block_cache_bytes = block_cache_bytes;
    planned.recent_cache_bytes = recent_cache_bytes;
    info!(
        block_cache_mb = block_cache_bytes / (1 << 20),
        recent_cache_mb = recent_cache_bytes / (1 << 20),
        file_cache_files = planned.file_cache_files,
        ceiling_mb = planned.ceiling_kb / 1024,
        "opening UTXO store"
    );
    // If pipelined ClickHouse ingestion is enabled, drop materialized views
    // up front (failing fast before opening RocksDB if ClickHouse is down) and
    // spawn a background worker that loads each completed Parquet part file as
    // soon as the sink finishes writing it.
    let (part_tx, mut ch_handle) = if let Some(stream_ch) = cfg.stream_clickhouse.take() {
        stream_ch
            .loader
            .begin_streaming_load()
            .context("prepare ClickHouse for streaming load")?;
        let (ptx, prx) = bounded::<u32>(PART_CHANNEL_DEPTH);
        let ch_out = cfg.out.clone();
        let ch_wit = cfg.capture_witness;
        let handle = std::thread::spawn(move || -> Result<StreamClickHouseConfig> {
            while let Ok(done_part) = prx.recv() {
                stream_ch
                    .loader
                    .load_part(
                        &ch_out,
                        done_part,
                        ch_wit,
                        stream_ch.mode,
                        stream_ch.unlink_loaded_parts,
                    )
                    .with_context(|| format!("ClickHouse load of part {done_part:05} failed"))?;
            }
            Ok(stream_ch)
        });
        (Some(ptx), Some(handle))
    } else {
        (None, None)
    };

    // Defer background compaction for the duration of the load. An initial
    // index is write-dominated, and leveled compaction rewrites the same data
    // through every level; deferring turns that interleaved write I/O into a
    // single compaction in `finalize_bulk_load` after the last block. Writes
    // pile up in L0 meanwhile, which is fine because nothing reads the store
    // heavily until the load finishes. See `UtxoStore::open_planned_bulk`.
    let utxo = Arc::new(UtxoStore::open_planned_bulk(&cfg.utxo, &planned)?);

    // Channel carries parsed blocks from workers to the ordered consumer.
    // Bound it to keep memory in check if the consumer (RocksDB) lags.
    let (tx, rx) = bounded::<WorkItem>(CHANNEL_DEPTH);

    let progress = ProgressBar::new(total);
    progress.set_style(
        ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] {bar:40} {pos}/{len} blocks ({per_sec}, ETA {eta})",
        )
        .unwrap(),
    );

    // Stats
    let parsed = Arc::new(AtomicU64::new(0));
    let missing_utxos = Arc::new(AtomicU64::new(0));

    // Reorder-buffer watermark shared with workers. If the consumer falls
    // more than REORDER_HIGH blocks behind, workers spin briefly to give it
    // time to drain — prevents pathological growth when an early height is
    // slow and the ordered consumer can't drain the buffer.
    let reorder_depth = Arc::new(AtomicUsize::new(0));
    // Height the consumer is waiting for. Workers holding this height are
    // exempt from the watermark, which is what makes the spin loop safe: the
    // one worker that can unblock the consumer is never parked.
    let next_height = Arc::new(AtomicU32::new(start));

    let height_cursor = AtomicU32::new(start);
    let index_for_workers = Arc::clone(&index);
    let orphan_blocks = cfg.orphan_blocks.clone();
    let orphans_skipped = Arc::new(AtomicU64::new(0));
    let reorder_depth_w = Arc::clone(&reorder_depth);

    // Spawn the ordered consumer on its own thread.
    let out_dir = cfg.out.clone();
    let blocks_per_part = cfg.blocks_per_part;
    let capture_witness = cfg.capture_witness;
    let recorder = Arc::new(DiskRecorder::new(disk_cfg.clone()));
    // The decoded-file LRU is created here, before the consumer, so the
    // consumer can shrink it under memory pressure. It is cloned into each
    // worker below; every clone shares one inner, so a `shrink_to` from the
    // consumer lowers the bound for all of them.
    let file_cache = ThreadFileCache::with_capacity(&cfg.blocks_dir, planned.file_cache_files);
    // The watchdog must exist before the consumer is spawned so the consumer
    // can poll it. `Arc` because the consumer thread needs its own handle;
    // the original binding keeps it alive for the whole run. Its ceiling is the
    // one the memory plan was sized against, so "the plan fits under the
    // ceiling" is the same statement as "the watchdog will not trip on a run
    // that respects its budgets".
    let watchdog = Arc::new(crate::bench::OomWatchdog::start(planned.ceiling_kb));
    let consumer = std::thread::spawn({
        let progress = progress.clone();
        let missing_utxos = Arc::clone(&missing_utxos);
        let recorder = Arc::clone(&recorder);
        let reorder_depth = Arc::clone(&reorder_depth_w);
        let next_height = Arc::clone(&next_height);
        let watchdog = Arc::clone(&watchdog);
        let file_cache = file_cache.clone();
        // The consumer borrows the store; the original `utxo` binding is kept
        // for the post-run `finalize_bulk_load` call, so the thread gets a
        // clone rather than the only handle.
        let utxo = Arc::clone(&utxo);
        move || -> Result<()> {
            consume_ordered(
                rx,
                start,
                end,
                blocks_per_part,
                &out_dir,
                &utxo,
                &progress,
                &missing_utxos,
                recorder.as_ref(),
                &reorder_depth,
                &next_height,
                Some(watchdog.as_ref()),
                capture_witness,
                &file_cache,
                planned.file_cache_files,
                part_tx,
            )
        }
    });

    // Parallel parse stage. Heights are processed in parallel; each result
    // is sent into the channel tagged with its height for reordering.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.threads.unwrap_or(0))
        .build()?;

    let reorder_depth_w = Arc::clone(&reorder_depth);
    let next_height_w = Arc::clone(&next_height);
    let parse_opts = bidx_parser::ParseOptions {
        capture_witness: cfg.capture_witness,
    };
    // Workers need the watchdog too: when it trips, the consumer returns and
    // drops `rx`, so every worker's next `send` fails. Reporting that as
    // "consumer dropped" would hide the actual cause, so the send-failure path
    // checks the watchdog first.
    let watchdog_w = Arc::clone(&watchdog);

    let num_workers = pool.current_num_threads();
    let parse_result: Result<()> = pool.install(|| {
        // Workers claim heights in strict ascending order from `height_cursor`
        // rather than binary-splitting `(start..end)` via `par_iter()`. That
        // keeps every worker on the same 1-2 `blk*.dat` files (preventing LRU
        // thrashing across distant chain eras) and lets `wait_for_consumer`
        // park workers before they allocate `FullBlock`s far ahead of `next`.
        (0..num_workers)
            .into_par_iter()
            .try_for_each(|_| -> Result<()> {
                let cache = file_cache.clone();
                let tx = tx.clone();
                loop {
                    let height = height_cursor.fetch_add(1, Ordering::Relaxed);
                    if height >= end {
                        return Ok(());
                    }
                    wait_for_consumer(
                        height,
                        REORDER_HIGH,
                        &next_height_w,
                        &reorder_depth_w,
                        &watchdog_w,
                    )?;
                    let loc = &index_for_workers.by_height[height as usize];
                    let bytes = cache.block_bytes(loc)?;
                    let mut header_raw = [0u8; 80];
                    header_raw.copy_from_slice(&bytes[..80]);
                    let header = bidx_core::BlockHeader::parse(&header_raw);
                    let hash = bidx_core::dsha256(&header_raw);
                    // Skip orphaned (stale / side-chain) blocks. Their UTXOs
                    // were never part of the active chain; indexing them would
                    // poison the UTXO set and emit duplicate rows. Notify the
                    // ordered consumer so it advances past this height.
                    if let Some(orphans) = &orphan_blocks {
                        if orphans.contains(&hash) {
                            drop(bytes);
                            orphans_skipped.fetch_add(1, Ordering::Relaxed);
                            send_work_item(&tx, WorkItem::Skipped { height }, &watchdog_w)?;
                            continue;
                        }
                    }
                    let block = parse_block_full_opts(&bytes, height, hash, &header, parse_opts)?;
                    // Release the `Arc<BlkFile>` reference before blocking on
                    // `tx.send` so an evicted 128 MiB file buffer can be freed
                    // immediately rather than lingering across channel backpressure.
                    drop(bytes);
                    send_work_item(&tx, WorkItem::Block { height, block }, &watchdog_w)?;
                }
            })
    });
    // Close the channel so the consumer can terminate.
    drop(tx);
    parse_result?;

    let consumer_res = consumer
        .join()
        // coverage-exempt: consumer only returns Err by panicking (not in fixtures)
        .map_err(|_| anyhow::anyhow!("consumer panicked"))?;
    if let Err(consumer_err) = consumer_res {
        if let Some(h) = ch_handle.take() {
            if let Ok(Err(ch_err)) = h.join() {
                return Err(ch_err);
            }
        }
        return Err(consumer_err);
    }

    // The load is done: re-enable compaction and compact the UTXO family once,
    // paying the deferred write I/O in a single pass instead of interleaving it
    // with the parse. Runs concurrently with the ClickHouse worker loading the
    // final Parquet part.
    if utxo.finalize_bulk_load()? {
        info!("finalized bulk load: compacted UTXO store");
    }

    if let Some(h) = ch_handle.take() {
        let stream_ch = h
            .join()
            // coverage-exempt: ClickHouse worker only panics on a fatal runtime abort
            .map_err(|_| anyhow::anyhow!("ClickHouse streaming loader panicked"))??;
        stream_ch
            .loader
            .finish_streaming_load()
            .context("finalize ClickHouse derived indexes and views")?;
    }

    progress.finish_with_message("parse complete");
    println!("parsed {} blocks", parsed.load(Ordering::Relaxed));
    println!("missing utxos: {}", missing_utxos.load(Ordering::Relaxed));
    let skipped = orphans_skipped.load(Ordering::Relaxed);
    if skipped > 0 {
        println!("skipped {} orphaned (stale) blocks", skipped);
    }
    Ok(())
}

/// Default per-process bound on the number of decoded blk files held in the
/// shared LRU, used by the tests and as the fallback when no `MemoryPlan` is
/// supplied.
///
/// Each `BlkFile` is ~128 MiB in memory, so this caps the decoded-file heap at
/// ~2 GiB. Signet uses XOR-obfuscated blk files (Bitcoin Core v28), so each
/// open allocates a 128 MiB decoded `Vec<u8>`; without eviction, 12 workers
/// opening all 149 files share ~149 × 128 MiB ≈ 19 GiB. XOR decode (~150 ms per
/// 128 MiB) only runs on cache miss. Production scales this with the memory
/// ceiling (see `bidx_core::MemoryPlan::file_cache_files`), so the constant is
/// only referenced from tests.
#[cfg(test)]
pub(crate) const MAX_CACHED_FILES: usize = 16;

pub(crate) struct ThreadFileCache {
    blocks_dir: PathBuf,
    inner: Arc<CacheInner>,
}

/// Zero-copy view into a cached [`BlkFile`]'s payload for one block.
///
/// Holds the `Arc<BlkFile>` backing the `Mmap` or decoded `Vec<u8>` alongside
/// the validated `[start..end]` slice bounds, eliminating the per-block heap
/// allocation + multi-megabyte `memcpy` (`Arc::from(slice)`) across ~680 GiB of
/// mainnet block payloads.
pub(crate) struct CachedBlock {
    file: Arc<BlkFile>,
    start: usize,
    end: usize,
}

impl std::ops::Deref for CachedBlock {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.file.data()[self.start..self.end]
    }
}

type InflightLoad = Arc<std::sync::OnceLock<std::result::Result<Arc<BlkFile>, String>>>;

struct CacheInner {
    /// LRU list, oldest at front, most-recently-used at back.
    lru: parking_lot::Mutex<std::collections::VecDeque<u32>>,
    map: parking_lot::RwLock<FxHashMap<u32, Arc<BlkFile>>>,
    /// Per-`file_id` singleflight table so that when 12 workers simultaneously
    /// cross into a new `blkNNNNN.dat`, only one worker reads and XOR-decodes
    /// the 128 MiB file while the others wait on its `OnceLock`.
    inflight: parking_lot::Mutex<FxHashMap<u32, InflightLoad>>,
    /// Most-recently-used `file_id` (`u32::MAX` when empty). Lets ~99.5% of
    /// sequential block hits skip locking `lru` and scanning the `VecDeque`.
    mru_file_id: AtomicU32,
    /// Optional 8-byte `xor.dat` key resolved once for `blocks_dir`.
    xor_key: Option<[u8; 8]>,
    /// Maximum resident files. A shared `AtomicUsize` so `shrink_to` (called by
    /// the consumer under memory pressure) lowers the bound for every clone —
    /// each worker holds its own `ThreadFileCache` but they share this inner.
    capacity: AtomicUsize,
    #[cfg(test)]
    opens: AtomicUsize,
}

impl Clone for ThreadFileCache {
    fn clone(&self) -> Self {
        ThreadFileCache {
            blocks_dir: self.blocks_dir.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ThreadFileCache {
    /// Convenience constructor using [`MAX_CACHED_FILES`]. Test-only because
    /// production always passes the plan's `file_cache_files` to
    /// [`ThreadFileCache::with_capacity`].
    #[cfg(test)]
    pub(crate) fn new(blocks_dir: &Path) -> Self {
        Self::with_capacity(blocks_dir, MAX_CACHED_FILES)
    }

    /// Like [`ThreadFileCache::new`] but with an explicit resident-file bound,
    /// so a memory plan can scale the decoded-file heap with the ceiling.
    pub(crate) fn with_capacity(blocks_dir: &Path, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let xor_key = read_xor_key_from_dir(blocks_dir);
        ThreadFileCache {
            blocks_dir: blocks_dir.to_path_buf(),
            inner: Arc::new(CacheInner {
                lru: parking_lot::Mutex::new(std::collections::VecDeque::with_capacity(capacity)),
                map: parking_lot::RwLock::new(FxHashMap::default()),
                inflight: parking_lot::Mutex::new(FxHashMap::default()),
                mru_file_id: AtomicU32::new(u32::MAX),
                xor_key,
                capacity: AtomicUsize::new(capacity),
                #[cfg(test)]
                opens: AtomicUsize::new(0),
            }),
        }
    }

    /// Lower the resident-file bound and evict down to it. Called by the
    /// consumer under memory pressure. Idempotent and monotone-decreasing: a
    /// later call may shrink further but never grows it back (the workload does
    /// not re-request files it no longer needs, so re-growing only risks
    /// re-approaching the ceiling).
    pub(crate) fn shrink_to(&self, capacity: usize) {
        let capacity = capacity.max(1);
        let cur = self.inner.capacity.load(Ordering::Relaxed);
        if capacity >= cur {
            return;
        }
        self.inner.capacity.store(capacity, Ordering::Relaxed);
        let mut lru = self.inner.lru.lock();
        while lru.len() > capacity {
            if let Some(old) = lru.pop_front() {
                self.inner.map.write().remove(&old);
            }
        }
    }

    /// Number of `BlkFile`s currently resident in the shared LRU. Used by
    /// `bidx bench --trace-cache` to prove the decoded-file bound actually
    /// holds; a value above the capacity means eviction is broken.
    ///
    /// Exposed for tests: the whole point of the bound is that this never
    /// exceeds it, and an assertion on the count is the only way to catch an
    /// eviction regression that would otherwise only show up as a 19 GiB RSS
    /// spike on a long run.
    #[cfg(test)]
    pub(crate) fn resident(&self) -> usize {
        self.inner.map.read().len()
    }

    fn touch(&self, file_id: u32) {
        let capacity = self.inner.capacity.load(Ordering::Relaxed);
        let mut lru = self.inner.lru.lock();
        // Move `file_id` to the back (most recently used).
        if let Some(pos) = lru.iter().position(|&id| id == file_id) {
            lru.remove(pos);
        }
        lru.push_back(file_id);
        self.inner.mru_file_id.store(file_id, Ordering::Relaxed);
        // Evict oldest if over capacity.
        while lru.len() > capacity {
            if let Some(old) = lru.pop_front() {
                if old != file_id {
                    self.inner.map.write().remove(&old);
                }
            }
        }
    }

    fn get_file(&self, file_id: u32) -> Result<Arc<BlkFile>> {
        if let Some(bf) = self.inner.map.read().get(&file_id).cloned() {
            if self.inner.mru_file_id.load(Ordering::Relaxed) != file_id {
                self.touch(file_id);
            }
            return Ok(bf);
        }

        let slot = {
            let mut inflight = self.inner.inflight.lock();
            Arc::clone(inflight.entry(file_id).or_default())
        };
        let res = slot.get_or_init(|| {
            #[cfg(test)]
            self.inner.opens.fetch_add(1, Ordering::Relaxed);
            let path = self.blocks_dir.join(format!("blk{:05}.dat", file_id));
            let bf = Arc::new(
                BlkFile::open_with_key(&path, file_id, self.inner.xor_key)
                    .map_err(|e| e.to_string())?,
            );
            self.inner.map.write().insert(file_id, Arc::clone(&bf));
            self.touch(file_id);
            Ok(bf)
        });
        self.inner.inflight.lock().remove(&file_id);
        match res {
            Ok(bf) => Ok(Arc::clone(bf)),
            Err(msg) => anyhow::bail!("{msg}"),
        }
    }

    pub(crate) fn block_bytes(&self, loc: &bidx_core::BlockLocation) -> Result<CachedBlock> {
        let file = self.get_file(loc.file_id)?;
        let (start, end) = file.block_range(loc)?;
        Ok(CachedBlock { file, start, end })
    }
}

/// Park a parse worker when its assigned `height` runs more than
/// `reorder_high` blocks ahead of `next_height` (or when the consumer's
/// reorder buffer is already above `reorder_high`).
///
/// The worker holding `next_height` itself is always exempt (`height ==
/// next_height` short-circuits), keeping the pipeline deadlock-free while
/// bounding in-flight `FullBlock`s across workers, channel, and reorder buffer
/// combined.
pub(crate) fn wait_for_consumer(
    height: u32,
    reorder_high: usize,
    next_height: &AtomicU32,
    reorder_depth: &AtomicUsize,
    watchdog: &crate::bench::OomWatchdog,
) -> Result<()> {
    while (height.saturating_sub(next_height.load(Ordering::Relaxed)) as usize > reorder_high
        || reorder_depth.load(Ordering::Relaxed) > reorder_high)
        && next_height.load(Ordering::Relaxed) != height
    {
        if watchdog.is_tripped() {
            return Err(watchdog.tripped_error());
        }
        std::thread::sleep(std::time::Duration::from_micros(50));
    }
    Ok(())
}

/// Send a [`WorkItem`] to the ordered consumer, surfacing the RSS watchdog's
/// error rather than a generic `"consumer dropped"` if the consumer aborted on
/// an over-limit trip.
pub(crate) fn send_work_item(
    tx: &crossbeam_channel::Sender<WorkItem>,
    item: WorkItem,
    watchdog: &crate::bench::OomWatchdog,
) -> Result<()> {
    tx.send(item).map_err(|_| {
        if watchdog.is_tripped() {
            watchdog.tripped_error()
        } else {
            anyhow::anyhow!("consumer dropped")
        }
    })
}

/// Minimum interval between consecutive cache-shedding steps during a single
/// sustained pressure episode. Spans ten 200 ms watchdog polls so `VmRSS`
/// reflects the previous step's `mremap` / `malloc_trim` before deciding
/// whether a further step is required.
pub(crate) const PRESSURE_RELIEF_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(2);

/// Progressive memory-pressure controller shared by `consume_ordered` and
/// `bench::bench_consume`.
///
/// Unlike a pure single-shot rising-edge latch (which fired once and then
/// stayed inert forever if one halving brought RSS to e.g. 76% — above the
/// recovery threshold — before RSS climbed on to 100%), [`PressureState`]
/// sheds immediately on the rising edge *and* takes additional steps every
/// [`PRESSURE_RELIEF_COOLDOWN`] while pressure persists until either RSS
/// recovers or all three cache levers reach their floors.
pub(crate) struct PressureState {
    last_relief: Option<std::time::Instant>,
    pub(crate) file_cache_bound: usize,
}

impl PressureState {
    pub(crate) fn new(file_cache_start: usize) -> Self {
        Self {
            last_relief: None,
            file_cache_bound: file_cache_start,
        }
    }

    pub(crate) fn poll(
        &mut self,
        pressured: bool,
        now: std::time::Instant,
        utxo: Option<&UtxoStore>,
        file_cache: &ThreadFileCache,
    ) -> usize {
        if !pressured {
            self.last_relief = None;
            return 0;
        }
        if let Some(prev) = self.last_relief {
            if now.saturating_duration_since(prev) < PRESSURE_RELIEF_COOLDOWN {
                return 0;
            }
        }
        self.last_relief = Some(now);
        let mut freed = 0;
        if let Some(u) = utxo {
            freed += u.relieve_pressure();
        }
        let next_bound = (self.file_cache_bound / 2).max(1);
        let shrank_files = next_bound < self.file_cache_bound;
        if shrank_files {
            file_cache.shrink_to(next_bound);
            self.file_cache_bound = next_bound;
            bidx_core::trim_allocator();
        }
        if freed > 0 || shrank_files {
            // coverage-exempt: the `tracing::warn!` field expressions are only
            // evaluated when a subscriber is installed; the unit tests run
            // without one, so only the macro's target line is instrumented.
            tracing::warn!(
                freed_mb = freed / (1 << 20),
                file_cache_files = self.file_cache_bound,
                "watchdog: RSS over soft threshold; shedding cache memory"
            );
        }
        freed
    }
}

/// Receives out-of-order parsed blocks, buffers until the next expected
/// height arrives, applies UTXO changes in order, and hands each enriched block
/// to a dedicated sink thread that writes Parquet.
///
/// The sink runs on its own thread so its file I/O (and zstd compression)
/// overlaps the RocksDB apply on this thread — they are the two halves of the
/// consumer's cost and neither depends on the other's result.
#[allow(clippy::too_many_arguments)]
fn consume_ordered(
    rx: Receiver<WorkItem>,
    start: u32,
    end: u32,
    blocks_per_part: u32,
    out_dir: &Path,
    utxo: &UtxoStore,
    progress: &ProgressBar,
    missing_utxos: &AtomicU64,
    recorder: &DiskRecorder,
    reorder_depth: &AtomicUsize,
    // Height the consumer is waiting for; workers exempt themselves from
    // backpressure when their own height matches (see `run_parse`). Without
    // this the backpressure spin can deadlock: the worker holding `next`
    // parks, so nothing ever arrives to unblock the consumer.
    next_height: &AtomicU32,
    watchdog: Option<&crate::bench::OomWatchdog>,
    capture_witness: bool,
    // The shared decoded-file LRU, so the consumer can shrink it under pressure.
    file_cache: &ThreadFileCache,
    // The plan's starting file-LRU bound; each relief step halves from the last.
    file_cache_start: usize,
    part_tx: Option<crossbeam_channel::Sender<u32>>,
) -> Result<()> {
    let mut buffer: BTreeMap<u32, FullBlock> = BTreeMap::new();
    let mut skipped_queue: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    let mut next = start;
    next_height.store(next, Ordering::Relaxed);
    let mut pressure = PressureState::new(file_cache_start);

    let sink_cfg = SinkConfig {
        capture_witness,
        ..SinkConfig::default()
    };
    let start_part = start / blocks_per_part;

    // Dedicated sink thread. Bounded so a slow sink backpressures the consumer
    // instead of buffering unboundedly; the consumer simply blocks on `send`,
    // which stops it draining the reorder buffer, which stops the workers.
    let (sink_tx, sink_rx) = bounded::<SinkMsg>(SINK_CHANNEL_DEPTH);
    let sink_out = out_dir.to_path_buf();
    let sink_handle = std::thread::spawn(move || -> Result<()> {
        sink_thread_main(
            sink_rx,
            &sink_out,
            blocks_per_part,
            start_part,
            sink_cfg,
            part_tx,
        )
    });

    'consume: while let Ok(item) = rx.recv() {
        match item {
            WorkItem::Block { height, block } => {
                buffer.insert(height, block);
                reorder_depth.fetch_add(1, Ordering::Relaxed);
            }
            WorkItem::Skipped { height } => {
                skipped_queue.insert(height);
            }
        }

        // First, advance the consumer past any skipped heights that are
        // contiguous with `next`. This unlocks progress when an orphan
        // block's legitimate data never arrives.
        while skipped_queue.remove(&next) {
            tracing::info!(height = next, "skipping orphan block");
            progress.inc(1);
            let _ = recorder.on_block_applied(next)?;
            next += 1;
            next_height.store(next, Ordering::Relaxed);
            if next >= end {
                break 'consume;
            }
        }

        while let Some(block) = buffer.remove(&next) {
            // Poll the RSS watchdog once per block. It is a relaxed atomic
            // load, so the cost is nil next to apply_block.
            //
            // Two stages, and the order matters. First, on the *rising edge* of
            // the soft threshold, shed cache memory (shrink the recent-UTXO
            // cache and the block cache, and halve the decoded-file LRU) so the
            // hard ceiling is not reached. Only if RSS has *still* crossed the
            // hard ceiling do we abort. The soft stage is the whole point:
            // aborting at the hard limit is too late — by then an allocation
            // may already have failed, and RocksDB does not handle allocation
            // failure, it segfaults.
            //
            // Edge-triggered, not level-triggered: the watchdog flag is sticky
            // until RSS recovers, so shedding on every pressured block would
            // floor the caches and wreck throughput. One step per episode, and
            // the next episode can only start after RSS has fallen back.
            if let Some(w) = watchdog {
                pressure.poll(
                    w.is_pressured(),
                    std::time::Instant::now(),
                    Some(utxo),
                    file_cache,
                );
                if w.is_tripped() {
                    return Err(w.tripped_error());
                }
            }
            // Drained one from reorder buffer — signal workers that there
            // is space for more.
            reorder_depth.fetch_sub(1, Ordering::Relaxed);

            let mut block = block;
            let res = utxo.apply_block(next, &block.txs, &block.inputs, &block.outputs)?;
            missing_utxos.fetch_add(res.missing, Ordering::Relaxed);

            // Enrich txs with fees and block with total fee.
            //
            // By consensus only `tx_index == 0` can be a coinbase tx, so
            // checking `block.inputs.first()` is O(1) with zero per-block
            // `Vec<bool>` heap allocation or full-input scan.
            let first_is_coinbase = block
                .inputs
                .first()
                .is_some_and(|inp| inp.is_coinbase && inp.tx_index == 0);
            let mut total_fee = 0u64;
            for (i, tx) in block.txs.iter_mut().enumerate() {
                tx.fee = res.fees[i];
                if !(i == 0 && first_is_coinbase) {
                    total_fee = total_fee.saturating_add(res.fees[i]);
                }
            }
            block.block_row.total_fee = total_fee;

            // Hand the fully-enriched block to the sink thread. A send error
            // means the sink already failed; surface its error, not a generic
            // "channel closed".
            if sink_tx
                .send(SinkMsg::Block {
                    block,
                    spends: res.spends,
                })
                .is_err()
            {
                // coverage-exempt: only reached if the background Parquet sink
                // thread died mid-run on a filesystem fault.
                return Err(reap_dead_sink(sink_handle));
            }

            progress.inc(1);
            let _ = recorder.on_block_applied(next)?;
            next += 1;
            next_height.store(next, Ordering::Relaxed);
            if next >= end {
                break 'consume;
            }
        }
    }

    // Ask the sink to flush its last part and close, then reap it. Join before
    // the early-exit/truncation check so a sink failure is reported even on a
    // complete range.
    let _ = sink_tx.send(SinkMsg::Finish);
    drop(sink_tx);
    let sink_res = sink_handle
        .join()
        // coverage-exempt: sink only returns Err by panicking (not in fixtures)
        .unwrap_or_else(|_| Err(anyhow::anyhow!("sink thread panicked")));

    if next < end {
        sink_res?;
        anyhow::bail!("channel closed early: stopped at height {next}, expected {end}");
    }
    sink_res
}

// coverage-exempt: only reached if the background Parquet sink thread died mid-run on an I/O fault.
fn reap_dead_sink(sink_handle: std::thread::JoinHandle<Result<()>>) -> anyhow::Error {
    let e = sink_handle
        .join()
        .unwrap_or_else(|_| Err(anyhow::anyhow!("sink thread panicked")));
    e.unwrap_err().context("parquet sink thread failed")
}

/// A message from the ordered consumer to the sink thread.
enum SinkMsg {
    /// An enriched block (fees filled, total_fee set) plus its spend rows.
    Block {
        block: FullBlock,
        spends: Vec<bidx_core::SpendRow>,
    },
    /// Flush the open part and stop.
    Finish,
}

/// The sink thread body: owns the `ParquetSink`, rotates part files by height,
/// writes every row kind for each block via concrete SoA column builders, and
/// notifies the optional ClickHouse streaming loader as each part closes.
fn sink_thread_main(
    rx: Receiver<SinkMsg>,
    out_dir: &Path,
    blocks_per_part: u32,
    start_part: u32,
    sink_cfg: SinkConfig,
    part_tx: Option<crossbeam_channel::Sender<u32>>,
) -> Result<()> {
    let mut part_id = start_part;
    let mut sink = ParquetSink::create(out_dir, part_id, sink_cfg)?;
    while let Ok(msg) = rx.recv() {
        match msg {
            SinkMsg::Block { block, spends } => {
                // Rotate part file at boundaries.
                let wanted_part = block.block_row.height / blocks_per_part;
                if wanted_part != part_id {
                    sink.finish()?;
                    if let Some(ref tx) = part_tx {
                        tx.send(part_id).map_err(|_| {
                            anyhow::anyhow!("ClickHouse streaming loader stopped early")
                        })?;
                    }
                    part_id = wanted_part;
                    sink = ParquetSink::create(out_dir, part_id, sink_cfg)?;
                }
                sink.push_block_rows(
                    &block.block_row,
                    &block.txs,
                    &block.inputs,
                    &block.outputs,
                    &spends,
                    &block.witnesses,
                )?;
            }
            SinkMsg::Finish => break,
        }
    }
    sink.finish()?;
    if let Some(ref tx) = part_tx {
        tx.send(part_id)
            .map_err(|_| anyhow::anyhow!("ClickHouse streaming loader stopped early"))?;
    }
    Ok(())
}

/// Debug helper: parse a single block and print its rows as JSON.
pub fn inspect_block(blocks_dir: &Path, height: u32) -> Result<()> {
    let (index, _) = bidx_parser::build_chain_index(blocks_dir)?;
    let loc = index
        .by_height
        .get(height as usize)
        .context("height out of range")?;
    let path = blocks_dir.join(format!("blk{:05}.dat", loc.file_id));
    let bf = BlkFile::open(&path, loc.file_id)?;
    let bytes = bf.block_bytes(loc)?;
    let mut header_raw = [0u8; 80];
    header_raw.copy_from_slice(&bytes[..80]);
    let header = bidx_core::BlockHeader::parse(&header_raw);
    let hash = bidx_core::dsha256(&header_raw);
    let block = parse_block_full(bytes, height, hash, &header)?;

    println!("block {} hash {}", height, hash);
    println!(
        "  txs={} size={} weight={}",
        block.block_row.tx_count, block.block_row.size, block.block_row.weight
    );
    println!(
        "  inputs={} outputs={}",
        block.inputs.len(),
        block.outputs.len()
    );
    for tx in block.txs.iter().take(3) {
        let txid = bidx_core::Hash32::from_bytes(tx.txid);
        println!(
            "  tx[{}] {} in={} out={} witness={}",
            tx.tx_index, txid, tx.input_count, tx.output_count, tx.has_witness
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bidx_core::{BlockLocation, BLOCK_HEADER_LEN};
    use std::fs;

    /// Build a synthetic blocks dir with a single blk file containing N
    /// coinbase-only blocks. Returns (dir, [heights]).
    fn make_chain(n: u32) -> (PathBuf, Vec<bidx_core::Hash32>) {
        let tmp = std::env::temp_dir().join(format!(
            "bidx-pipe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        let blk_path = tmp.join("blk00000.dat");

        let mut data = Vec::new();
        let mut prev = bidx_core::Hash32::ZERO;
        let mut hashes = Vec::new();
        for i in 0..n {
            let mut hdr = [0u8; BLOCK_HEADER_LEN];
            hdr[0] = (1 + i) as u8; // version; distinct per height so hashes differ
            hdr[4..36].copy_from_slice(prev.as_bytes());
            for j in 0..32 {
                hdr[36 + j] = (i as u8).wrapping_add(j as u8);
            }
            hdr[68..72].copy_from_slice(&(1000u32 + i).to_le_bytes());
            hdr[72..76].copy_from_slice(&0x1d00_ffffu32.to_le_bytes());
            hdr[76..80].copy_from_slice(&i.to_le_bytes()); // nonce = height

            // One coinbase tx per block (same byte layout bidx-parser uses
            // for its own unit tests: version|1in{spk=0,0xffffffff,0} → 1out
            // (value only, empty spk) → locktime).
            let mut tx = Vec::new();
            tx.extend_from_slice(&1i32.to_le_bytes()); // version
            tx.push(1u8); // 1 input
            tx.extend_from_slice(&[0u8; 32]); // prev_txid = zero
            tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // prev_vout = max
            tx.push(0u8); // script_sig_len = 0
            tx.extend_from_slice(&0u32.to_le_bytes()); // sequence
            tx.push(1u8); // 1 output
            tx.extend_from_slice(&100u64.to_le_bytes()); // value
            tx.push(0u8); // spk len 0
                          // Non-zero locktime: `BlkFile::open` trims trailing zero padding,
                          // so a synthetic record ending in 0 would lose its final bytes.
            tx.extend_from_slice(&0x1122_3344u32.to_le_bytes()); // locktime

            let payload_len = (BLOCK_HEADER_LEN + 1 + tx.len()) as u32;
            data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
            data.extend_from_slice(&payload_len.to_le_bytes());
            data.extend_from_slice(&hdr);
            data.push(1u8); // tx_count
            data.extend_from_slice(&tx);
            prev = bidx_core::dsha256(&hdr);
            hashes.push(prev);
        }
        fs::write(&blk_path, &data).unwrap();
        (tmp, hashes)
    }

    /// Coinbase-only tx body paying `value` sat to a single output. Non-zero
    /// locktime so the trailing-zero trim cannot swallow the record.
    fn synth_coinbase(value: u64) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes()); // version
        tx.push(1u8); // 1 input
        tx.extend_from_slice(&[0u8; 32]); // prev_txid = zero (coinbase)
        tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // prev_vout = max
        tx.push(0u8); // script_sig_len
        tx.extend_from_slice(&0u32.to_le_bytes()); // sequence
        tx.push(1u8); // 1 output
        tx.extend_from_slice(&value.to_le_bytes());
        tx.push(0u8); // spk len 0
        tx.extend_from_slice(&0x1122_3344u32.to_le_bytes()); // non-zero locktime
        tx
    }

    /// A non-coinbase tx spending `(prev_txid, prev_vout)` and paying `value`
    /// sat to a single output, so the block carries a real fee.
    fn synth_spend(prev_txid: &bidx_core::Hash32, prev_vout: u32, value: u64) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes()); // version
        tx.push(1u8); // 1 input
        tx.extend_from_slice(prev_txid.as_bytes()); // real prev txid
        tx.extend_from_slice(&prev_vout.to_le_bytes());
        tx.push(0u8); // script_sig_len
        tx.extend_from_slice(&0u32.to_le_bytes()); // sequence
        tx.push(1u8); // 1 output
        tx.extend_from_slice(&value.to_le_bytes());
        tx.push(0u8); // spk len 0
        tx.extend_from_slice(&0x5566_7788u32.to_le_bytes()); // non-zero locktime
        tx
    }

    /// A segwit coinbase-shaped tx: marker/flag set and one witness item of
    /// two bytes on its single input, so `capture_witness` has a payload to
    /// record. Non-zero locktime keeps the synthetic record from being
    /// trimmed.
    fn synth_segwit_coinbase(value: u64) -> Vec<u8> {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes()); // version
        tx.push(0x00); // segwit marker
        tx.push(0x01); // segwit flag
        tx.push(1u8); // 1 input
        tx.extend_from_slice(&[0u8; 32]); // prev_txid = zero (coinbase)
        tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // prev_vout = max
        tx.push(0u8); // script_sig_len
        tx.extend_from_slice(&0u32.to_le_bytes()); // sequence
        tx.push(1u8); // 1 output
        tx.extend_from_slice(&value.to_le_bytes());
        tx.push(0u8); // spk len 0
                      // witness for the single input: 1 item, 2 bytes.
        tx.push(1u8);
        tx.push(2u8);
        tx.push(0xAA);
        tx.push(0xBB);
        tx.extend_from_slice(&0x1122_3344u32.to_le_bytes()); // non-zero locktime
        tx
    }

    /// Single-block chain whose coinbase is a segwit tx with a 2-byte witness
    /// item, for exercising witness capture through the whole pipeline.
    fn make_witness_chain() -> PathBuf {
        let tmp = std::env::temp_dir().join(format!(
            "bidx-wit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        let blk_path = tmp.join("blk00000.dat");

        let tx = synth_segwit_coinbase(100);
        let mut hdr = [0u8; BLOCK_HEADER_LEN];
        hdr[0] = 1;
        hdr[68..72].copy_from_slice(&1000u32.to_le_bytes());
        hdr[72..76].copy_from_slice(&0x1d00_ffffu32.to_le_bytes());
        hdr[79] = 0x01; // non-zero tail so the record survives the trim

        let mut data = Vec::new();
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&((BLOCK_HEADER_LEN + 1 + tx.len()) as u32).to_le_bytes());
        data.extend_from_slice(&hdr);
        data.push(1u8); // tx_count
        data.extend_from_slice(&tx);
        fs::write(&blk_path, &data).unwrap();
        tmp
    }

    /// Two blocks: block 0 is a coinbase paying 100 sat; block 1 spends that
    /// coinbase output and pays 60, so block 1 carries a 40-sat fee. Returns
    /// (dir, [hash0, hash1]).
    fn make_spend_chain() -> (PathBuf, Vec<bidx_core::Hash32>) {
        let tmp = std::env::temp_dir().join(format!(
            "bidx-spend-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        let blk_path = tmp.join("blk00000.dat");

        let cb0 = synth_coinbase(100);
        let txid0 = bidx_core::dsha256(&cb0);
        let mut hdr0 = [0u8; BLOCK_HEADER_LEN];
        hdr0[0] = 1;
        hdr0[68..72].copy_from_slice(&1000u32.to_le_bytes()); // time
        hdr0[72..76].copy_from_slice(&0x1d00_ffffu32.to_le_bytes()); // bits
        hdr0[79] = 0x01; // non-zero tail so the record survives the trim
        let hash0 = bidx_core::dsha256(&hdr0);

        let mut data = Vec::new();
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&((BLOCK_HEADER_LEN + 1 + cb0.len()) as u32).to_le_bytes());
        data.extend_from_slice(&hdr0);
        data.push(1u8); // tx_count
        data.extend_from_slice(&cb0);

        let cb1 = synth_coinbase(50);
        let spend = synth_spend(&txid0, 0, 60);
        let mut hdr1 = [0u8; BLOCK_HEADER_LEN];
        hdr1[0] = 2;
        hdr1[4..36].copy_from_slice(hash0.as_bytes());
        hdr1[68..72].copy_from_slice(&1001u32.to_le_bytes()); // time
        hdr1[72..76].copy_from_slice(&0x1d00_ffffu32.to_le_bytes()); // bits
        hdr1[79] = 0x02;
        let hash1 = bidx_core::dsha256(&hdr1);
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(
            &((BLOCK_HEADER_LEN + 1 + cb1.len() + spend.len()) as u32).to_le_bytes(),
        );
        data.extend_from_slice(&hdr1);
        data.push(2u8); // tx_count
        data.extend_from_slice(&cb1);
        data.extend_from_slice(&spend);

        fs::write(&blk_path, &data).unwrap();
        (tmp, vec![hash0, hash1])
    }

    #[test]
    fn resolve_cache_bytes_prefers_overrides_and_falls_back_to_budget() {
        let budget = bidx_core::CacheBudget {
            block_cache_bytes: 3 << 30,
            recent_cache_bytes: 5 << 30,
        };
        // No overrides: the planned values are used verbatim.
        assert_eq!(resolve_cache_bytes(None, None, budget), (3 << 30, 5 << 30));
        // A block-cache override wins; the recent cache still auto-sizes.
        assert_eq!(
            resolve_cache_bytes(Some(2), None, budget),
            (2 << 20, 5 << 30)
        );
        // A recent-cache override wins; the block cache still auto-sizes.
        assert_eq!(
            resolve_cache_bytes(None, Some(7), budget),
            (3 << 30, 7 << 20)
        );
        // An explicit 0 disables the recent cache rather than falling back.
        assert_eq!(resolve_cache_bytes(None, Some(0), budget).1, 0);
    }

    #[test]
    fn inspect_block_prints_rows_for_existing_height() {
        let (dir, _) = make_chain(4);
        // Should print the block without panicking. We can't easily assert
        // on stdout under `cargo test`, but a successful return + no panic
        // is what we verify.
        inspect_block(&dir, 0).expect("inspect 0");
        inspect_block(&dir, 2).expect("inspect 2");
        inspect_block(&dir, 3).expect("inspect 3");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn inspect_block_out_of_range_returns_err() {
        let (dir, _) = make_chain(2);
        let e = inspect_block(&dir, 99).err().expect("should error");
        assert!(format!("{:#}", e).contains("out of range"), "got {}", e);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_parse_writes_parquet_files_in_range() {
        let (blocks_dir, _) = make_chain(3);
        let out = std::env::temp_dir().join(format!(
            "bidx-pipe-out-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let utxo = std::env::temp_dir().join(format!(
            "bidx-pipe-utxo-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&utxo).unwrap();
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(3),
            blocks_per_part: 2, // forces rotation: part 0 = [0,1], part 1 = [2]
            threads: Some(2),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true, // no real disk-check against /tmp
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .expect("run_parse");
        // Layout: out/<entity>/part-NNNNN.parquet, one entity dir per kind.
        // With 3 blocks and blocks_per_part=2 we expect part-000000 (b0,b1)
        // and part-000001 (b2) to exist for each of the 5 kinds.
        let mut walk = Vec::new();
        for entity in ["blocks", "transactions", "inputs", "outputs", "spends"] {
            let dir = out.join(entity);
            if dir.exists() {
                for e in std::fs::read_dir(&dir).unwrap().flatten() {
                    walk.push(format!("{}/{}", entity, e.file_name().to_string_lossy()));
                }
            }
        }
        let has_part0 = walk
            .iter()
            .filter(|n| n.contains("part-00000.parquet"))
            .count();
        let has_part1 = walk
            .iter()
            .filter(|n| n.contains("part-00001.parquet"))
            .count();
        assert_eq!(
            has_part0, 5,
            "expected all 5 entites to have part-00000: {:?}",
            walk
        );
        // With blocks_per_part=2 the 3rd block (h=2) lands in part 1.
        assert_eq!(
            has_part1, 5,
            "expected all 5 entities to have part-00001: {:?}",
            walk
        );
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    #[test]
    fn run_parse_capture_witness_writes_payload_table() {
        let blocks_dir = make_witness_chain();
        let out = std::env::temp_dir().join(format!(
            "bidx-pipe-wit-out-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let utxo = std::env::temp_dir().join(format!(
            "bidx-pipe-wit-utxo-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&utxo).unwrap();
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(1),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: true,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .expect("run_parse with witness capture");

        // The optional witnesses table must be written and carry the 2-byte
        // item from the segwit coinbase.
        let part = out.join("witnesses/part-00000.parquet");
        assert!(part.exists(), "witnesses part must exist: {:?}", part);
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let f = std::fs::File::open(&part).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut rows = 0usize;
        for batch in reader {
            let batch = batch.unwrap();
            rows += batch.num_rows();
            let col = batch
                .column_by_name("item_bytes")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .unwrap();
            assert_eq!(col.value(0), &[0xAA, 0xBB]);
        }
        assert_eq!(rows, 1, "one witness stack item expected");

        // And with capture off, no witnesses table is produced.
        let out2 = out.with_extension("no-wit");
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out2.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(1),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts2.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .expect("run_parse without witness capture");
        assert!(!out2.join("witnesses").exists());

        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&out2);
        let _ = fs::remove_dir_all(&utxo);
    }

    #[test]
    fn run_parse_rejects_empty_range() {
        let (blocks_dir, _) = make_chain(3);
        let out = std::env::temp_dir().join(format!("bo-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bu-{}", std::process::id()));
        std::fs::create_dir_all(&utxo).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        // start=10 clamps to tip=2; end=0 stays. Therefore start(2) >= end(0).
        let e = run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 10,
            end: Some(0),
            blocks_per_part: 1000,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("c")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .err()
        .expect("should error");
        assert!(format!("{:#}", e).contains("empty range"));
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    /// Drive the sink thread directly: it must write every row kind for each
    /// block, rotate part files at the boundary, and stop on `Finish`.
    #[test]
    fn sink_thread_writes_rows_and_rotates_on_boundary() {
        use crossbeam_channel::bounded;
        let out = std::env::temp_dir().join(format!(
            "bidx-sink-thread-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (tx, rx) = bounded::<SinkMsg>(4);
        let out_for_thread = out.clone();
        let handle = std::thread::spawn(move || {
            sink_thread_main(rx, &out_for_thread, 2, 0, SinkConfig::default(), None)
        });

        // Height 0 and 1 -> part 0; height 2 -> part 1 (boundary at 2/2=1).
        for h in 0..3u32 {
            let mut block = synthetic_full_block(h);
            // Add a second tx + input + output + witness so every push path runs.
            block.txs.push(bidx_core::TxRow {
                height: h,
                tx_index: 1,
                txid: [0x22; 32],
                version: 1,
                locktime: 0,
                size: 0,
                weight: 0,
                fee: 0,
                input_count: 1,
                output_count: 1,
                has_witness: false,
            });
            block.inputs.push(bidx_core::InputRow {
                height: h,
                tx_index: 1,
                input_index: 0,
                txid: [0x22; 32],
                prev_txid: [0xEE; 32],
                prev_vout: 0,
                script_sig_len: 0,
                sequence: 0,
                witness_items: 0,
                witness_bytes: 0,
                is_coinbase: false,
            });
            block.outputs.push(bidx_core::OutputRow {
                height: h,
                tx_index: 1,
                output_index: 0,
                txid: [0x22; 32],
                value_sat: 10,
                script_pubkey_len: 0,
                script_type: 1,
                address_hash: [0xBB; 32],
            });
            let spends = vec![bidx_core::SpendRow {
                height: h,
                spending_tx_index: 1,
                spending_input_index: 0,
                spending_txid: [0x22; 32],
                spent_txid: [0xEE; 32],
                spent_vout: 0,
                spent_value_sat: 5,
                spent_script_type: 1,
                spent_address_hash: [0xCC; 32],
                spent_height: 0,
            }];
            tx.send(SinkMsg::Block { block, spends }).unwrap();
        }
        tx.send(SinkMsg::Finish).unwrap();
        drop(tx);
        handle.join().unwrap().unwrap();

        for part in ["part-00000.parquet", "part-00001.parquet"] {
            for entity in ["blocks", "transactions", "inputs", "outputs", "spends"] {
                let p = out.join(entity).join(part);
                assert!(p.exists(), "missing {:?}", p);
            }
        }
        let _ = fs::remove_dir_all(&out);
    }

    /// A small in-memory `FullBlock` with one coinbase tx/input/output.
    fn synthetic_full_block(height: u32) -> FullBlock {
        let txid = [0x11u8; 32];
        FullBlock {
            block_row: bidx_core::BlockRow {
                height,
                hash: [0xAA; 32],
                prev_hash: [0u8; 32],
                merkle_root: [0u8; 32],
                time: 0,
                bits: 0,
                nonce: 0,
                version: 1,
                tx_count: 1,
                size: 0,
                weight: 0,
                total_fee: 0,
                coinbase_value: 100,
            },
            txs: vec![bidx_core::TxRow {
                height,
                tx_index: 0,
                txid,
                version: 1,
                locktime: 0,
                size: 0,
                weight: 0,
                fee: 0,
                input_count: 1,
                output_count: 1,
                has_witness: false,
            }],
            inputs: vec![bidx_core::InputRow {
                height,
                tx_index: 0,
                input_index: 0,
                txid,
                prev_txid: [0u8; 32],
                prev_vout: 0xFFFF_FFFF,
                script_sig_len: 0,
                sequence: 0,
                witness_items: 0,
                witness_bytes: 0,
                is_coinbase: true,
            }],
            outputs: vec![bidx_core::OutputRow {
                height,
                tx_index: 0,
                output_index: 0,
                txid,
                value_sat: 100,
                script_pubkey_len: 0,
                script_type: 0,
                address_hash: [0u8; 32],
            }],
            witnesses: vec![],
        }
    }

    #[test]
    fn thread_file_cache_opens_and_caches() {
        let (blocks_dir, _hashes) = make_chain(2);
        // Build a location pointing into blk00000.dat.
        let loc = BlockLocation {
            file_id: 0,
            offset: 0,
            record_len: (BLOCK_HEADER_LEN + 1 + 47 + 8) as u32, // header + cnt + coinbase(50ish) + record prefix
        };
        let cache = ThreadFileCache::new(&blocks_dir);
        let b1 = cache.block_bytes(&loc).expect("first read");
        assert_eq!(b1.len() as u32, loc.record_len - 8);
        // Second call pulls from the per-thread cache.
        let b2 = cache.block_bytes(&loc).expect("second read");
        assert_eq!(b1.len(), b2.len());
        let _ = std::fs::remove_dir_all(&blocks_dir);
    }

    /// Build `n` single-record blk files, each with its own id, so we can drive
    /// the LRU past `MAX_CACHED_FILES` and prove eviction keeps the resident
    /// set bounded. A regression here is exactly what turns a long run into a
    /// multi-gigabyte RSS spike, so it must be asserted rather than assumed.
    fn make_many_blk_files(n: u32) -> PathBuf {
        let tmp = std::env::temp_dir().join(format!(
            "bidx-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        for file_id in 0..n {
            let mut hdr = [0u8; BLOCK_HEADER_LEN];
            hdr[0] = 1;
            hdr[76..80].copy_from_slice(&file_id.to_le_bytes());
            // Coinbase-only body ending in a non-zero locktime so the
            // trailing-zero trim in `BlkFile::open` cannot swallow it.
            let mut tx = Vec::new();
            tx.extend_from_slice(&1i32.to_le_bytes());
            tx.push(1u8);
            tx.extend_from_slice(&[0u8; 32]);
            tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
            tx.push(0u8);
            tx.extend_from_slice(&0u32.to_le_bytes());
            tx.push(1u8);
            tx.extend_from_slice(&100u64.to_le_bytes());
            tx.push(0u8);
            tx.extend_from_slice(&0x1122_3344u32.to_le_bytes());

            let payload_len = (BLOCK_HEADER_LEN + 1 + tx.len()) as u32;
            let mut data = Vec::new();
            data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
            data.extend_from_slice(&payload_len.to_le_bytes());
            data.extend_from_slice(&hdr);
            data.push(1u8);
            data.extend_from_slice(&tx);
            fs::write(tmp.join(format!("blk{:05}.dat", file_id)), &data).unwrap();
        }
        tmp
    }

    #[test]
    fn thread_file_cache_evicts_beyond_capacity() {
        // Open more distinct files than the LRU holds; the resident count must
        // never exceed the cap, or decoded 128 MiB buffers accumulate without
        // bound (the 19 GiB failure mode this bound exists to prevent).
        let n = (MAX_CACHED_FILES as u32) + 8;
        let dir = make_many_blk_files(n);
        let cache = ThreadFileCache::new(&dir);
        let loc = |file_id: u32| BlockLocation {
            file_id,
            offset: 0,
            record_len: (BLOCK_HEADER_LEN + 1 + 47 + 8) as u32,
        };
        for file_id in 0..n {
            cache
                .block_bytes(&loc(file_id))
                .unwrap_or_else(|e| panic!("open file {file_id}: {e}"));
            assert!(
                cache.resident() <= MAX_CACHED_FILES,
                "resident {} exceeded cap {} after opening file {}",
                cache.resident(),
                MAX_CACHED_FILES,
                file_id
            );
        }
        // After sweeping past the cap the map must be exactly at capacity,
        // i.e. eviction removed the overflow rather than merely not adding.
        assert_eq!(cache.resident(), MAX_CACHED_FILES);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn thread_file_cache_reuses_within_capacity() {
        // Below the cap nothing should be evicted: re-reading a cached file
        // must not change the resident set, which proves the LRU hit path is
        // taken rather than a reopen.
        let n = (MAX_CACHED_FILES as u32) - 2;
        let dir = make_many_blk_files(n);
        let cache = ThreadFileCache::new(&dir);
        let loc = |file_id: u32| BlockLocation {
            file_id,
            offset: 0,
            record_len: (BLOCK_HEADER_LEN + 1 + 47 + 8) as u32,
        };
        for file_id in 0..n {
            cache.block_bytes(&loc(file_id)).unwrap();
        }
        assert_eq!(cache.resident(), n as usize);
        // Re-read the oldest; it must still be resident.
        cache.block_bytes(&loc(0)).unwrap();
        assert_eq!(cache.resident(), n as usize);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn thread_file_cache_singleflight_mru_fast_path_and_error_cleanup() {
        let dir = make_many_blk_files(3);
        let xor_key = [0x33u8, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA];
        // Obfuscate all 3 files so singleflight also exercises cached xor.dat
        // decoding across concurrent threads.
        fs::write(dir.join("xor.dat"), xor_key).unwrap();
        for file_id in 0..3u32 {
            let p = dir.join(format!("blk{:05}.dat", file_id));
            let plain = fs::read(&p).unwrap();
            let obf: Vec<u8> = plain
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ xor_key[i & 7])
                .collect();
            fs::write(&p, obf).unwrap();
        }

        let cache = ThreadFileCache::with_capacity(&dir, 2);
        let loc = |file_id: u32| BlockLocation {
            file_id,
            offset: 0,
            record_len: (BLOCK_HEADER_LEN + 1 + 47 + 8) as u32,
        };

        // 8 threads simultaneously request file 0 on a cold cache: singleflight
        // must open & XOR-decode file 0 exactly once and return the same Arc.
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let files: Vec<Arc<BlkFile>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let c = cache.clone();
                    let b = Arc::clone(&barrier);
                    s.spawn(move || {
                        b.wait();
                        let blk = c.block_bytes(&loc(0)).unwrap();
                        assert_eq!((&*blk).len() as u32, loc(0).record_len - 8);
                        blk.file
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(
            cache.inner.opens.load(Ordering::Relaxed),
            1,
            "singleflight must open cold blk00000.dat exactly once across 8 threads"
        );
        for other in &files[1..] {
            assert!(
                Arc::ptr_eq(&files[0], other),
                "all concurrent callers must share the same Arc<BlkFile>"
            );
        }
        drop(files);

        // MRU fast path (`mru_file_id == 0`) skips `touch` and does not reopen.
        let b_mru = cache.block_bytes(&loc(0)).unwrap();
        assert_eq!(cache.inner.opens.load(Ordering::Relaxed), 1);
        drop(b_mru);

        // Open file 1 (`lru = [0, 1]`, `mru = 1`), then re-read file 0
        // (`mru != 0` triggers `touch`, rotating `lru` to `[1, 0]`, `mru = 0`),
        // then open file 2 at capacity 2 -> must evict file 1 (not file 0!).
        cache.block_bytes(&loc(1)).unwrap();
        cache.block_bytes(&loc(0)).unwrap();
        cache.block_bytes(&loc(2)).unwrap();
        assert_eq!(cache.resident(), 2);
        assert!(cache.inner.map.read().contains_key(&0));
        assert!(cache.inner.map.read().contains_key(&2));
        assert!(!cache.inner.map.read().contains_key(&1));

        // Missing file surfaces error and cleans up its `inflight` slot.
        let err = cache.block_bytes(&loc(9999));
        assert!(err.is_err());
        assert!(cache.inner.inflight.lock().is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pressure_state_sheds_on_rising_edge_and_progressively_after_cooldown() {
        // Relief must fire immediately on the rising edge, suppress per-block
        // shedding during the cooldown window, fire additional progressive
        // steps if RSS stays above the recovery band past cooldown, reset
        // immediately on recovery, and stop cleanly once all caches hit floor.
        let dir = tmp_utxo_dir("shed");
        let store = UtxoStore::open_tuned(&dir, 256 << 20, 64 << 20).unwrap();
        let file_dir = make_many_blk_files(8);
        let cache = ThreadFileCache::with_capacity(&file_dir, 8);
        let mut state = PressureState::new(8);
        let t0 = std::time::Instant::now();

        // No pressure: nothing shed, bound unchanged.
        assert_eq!(state.poll(false, t0, Some(&store), &cache), 0);
        assert_eq!(state.file_cache_bound, 8);

        // Rising edge: shed immediately. Recent halved (64->32 MiB) + block
        // cache halved (256->128 MiB) = 160 MiB; file bound halved to 4.
        assert_eq!(
            state.poll(true, t0, Some(&store), &cache),
            (32 << 20) + (128 << 20)
        );
        assert_eq!(state.file_cache_bound, 4);

        // Still pressured inside the 2s cooldown: no further shedding yet.
        assert_eq!(
            state.poll(
                true,
                t0 + std::time::Duration::from_millis(500),
                Some(&store),
                &cache,
            ),
            0
        );
        assert_eq!(state.file_cache_bound, 4);

        // Sustained pressure past cooldown (RSS never dipped below recovery):
        // takes a second progressive step rather than staying dead-latched!
        let t1 = t0 + PRESSURE_RELIEF_COOLDOWN;
        assert_eq!(
            state.poll(true, t1, Some(&store), &cache),
            (16 << 20) + (64 << 20)
        );
        assert_eq!(state.file_cache_bound, 2);

        // Recovery clears the cooldown so a fresh pressure episode fires right
        // away without waiting another 2s.
        assert_eq!(state.poll(false, t1, Some(&store), &cache), 0);
        assert_eq!(
            state.poll(
                true,
                t1 + std::time::Duration::from_millis(10),
                Some(&store),
                &cache,
            ),
            8 << 20 // block cache already at 64 MiB floor; recent 16->8 MiB
        );
        assert_eq!(state.file_cache_bound, 1);

        // With no store and file_cache_bound > 1, file-LRU lever still works;
        // once bound reaches 1, further polls at floor return 0 quietly.
        let mut s2 = PressureState::new(4);
        assert_eq!(s2.poll(true, t0, None, &cache), 0);
        assert_eq!(s2.file_cache_bound, 2);
        assert_eq!(
            s2.poll(true, t0 + PRESSURE_RELIEF_COOLDOWN, None, &cache),
            0
        );
        assert_eq!(s2.file_cache_bound, 1);
        assert_eq!(
            s2.poll(true, t0 + PRESSURE_RELIEF_COOLDOWN * 2, None, &cache),
            0
        );
        assert_eq!(s2.file_cache_bound, 1);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&file_dir);
    }

    #[test]
    fn wait_for_consumer_exempts_next_height_spins_until_caught_up_and_aborts_on_trip() {
        let next_h = AtomicU32::new(0);
        let reorder = AtomicUsize::new(0);
        let quiet_wd = crate::bench::OomWatchdog::start(u64::MAX);

        // Within `reorder_high` window: returns immediately.
        wait_for_consumer(2, 4, &next_h, &reorder, &quiet_wd).unwrap();

        // Even when `reorder > reorder_high`, the worker holding `next_h` is
        // exempt so the consumer can never deadlock.
        reorder.store(99, Ordering::Relaxed);
        wait_for_consumer(0, 4, &next_h, &reorder, &quiet_wd).unwrap();
        reorder.store(0, Ordering::Relaxed);

        // Ahead of `next_h + reorder_high`: spins briefly until consumer
        // advances `next_h`.
        let next_h_arc = Arc::new(AtomicU32::new(0));
        let next_h_bg = Arc::clone(&next_h_arc);
        let advancer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(2));
            next_h_bg.store(10, Ordering::Relaxed);
        });
        wait_for_consumer(10, 2, &next_h_arc, &reorder, &quiet_wd).unwrap();
        advancer.join().unwrap();

        // When the watchdog trips while parked, returns the watchdog error
        // rather than spinning forever.
        let tripped_wd = crate::bench::OomWatchdog::start(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !tripped_wd.is_tripped() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let err = wait_for_consumer(50, 2, &next_h, &reorder, &tripped_wd).unwrap_err();
        assert!(err.to_string().contains("watchdog"));
    }

    #[test]
    fn send_work_item_surfaces_watchdog_cause_when_consumer_drops() {
        let quiet_wd = crate::bench::OomWatchdog::start(u64::MAX);
        let (tx, rx) = bounded::<WorkItem>(2);
        send_work_item(&tx, WorkItem::Skipped { height: 0 }, &quiet_wd).unwrap();
        drop(rx);

        // Consumer dropped without a watchdog trip: reports "consumer dropped".
        let e = send_work_item(&tx, WorkItem::Skipped { height: 1 }, &quiet_wd).unwrap_err();
        assert!(e.to_string().contains("consumer dropped"), "got {e}");

        // Consumer dropped because watchdog tripped: surfaces watchdog error.
        let tripped_wd = crate::bench::OomWatchdog::start(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !tripped_wd.is_tripped() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let e2 = send_work_item(&tx, WorkItem::Skipped { height: 2 }, &tripped_wd).unwrap_err();
        assert!(e2.to_string().contains("watchdog"), "got {e2}");
    }

    #[test]
    fn consume_ordered_aborts_on_tripped_watchdog_and_early_channel_close() {
        let root = tmp_utxo_dir("consume-abort");
        let utxo_dir = root.join("utxo");
        let out_dir = root.join("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let store = UtxoStore::open(&utxo_dir).unwrap();
        let disk_cfg = DiskGuardConfig {
            checkpoint_file: root.join("checkpoints.txt"),
            network: bidx_core::Network::Regtest,
            index_root: root.clone(),
            tracked_dirs: vec![utxo_dir.clone(), out_dir.clone()],
        };
        let recorder = DiskRecorder::new(disk_cfg);
        let progress = ProgressBar::hidden();
        let missing = AtomicU64::new(0);
        let reorder = AtomicUsize::new(0);
        let next_h = AtomicU32::new(0);
        let file_cache = ThreadFileCache::with_capacity(&root, 2);

        // 1. Producer closes channel before delivering all heights: fails loudly.
        let (tx, rx) = bounded::<WorkItem>(2);
        drop(tx);
        let e1 = consume_ordered(
            rx,
            0,
            2,
            10,
            &out_dir,
            &store,
            &progress,
            &missing,
            &recorder,
            &reorder,
            &next_h,
            None,
            false,
            &file_cache,
            2,
            None,
        )
        .unwrap_err();
        assert!(e1.to_string().contains("channel closed early"), "got {e1}");

        // 2. Tripped watchdog aborts cleanly on the next drained block.
        let tripped_wd = crate::bench::OomWatchdog::start(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !tripped_wd.is_tripped() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let (tx2, rx2) = bounded::<WorkItem>(2);
        let dummy_block = FullBlock {
            block_row: bidx_core::BlockRow {
                height: 0,
                hash: [0u8; 32],
                prev_hash: [0u8; 32],
                merkle_root: [0u8; 32],
                time: 0,
                bits: 0,
                nonce: 0,
                version: 1,
                tx_count: 0,
                size: 80,
                weight: 320,
                total_fee: 0,
                coinbase_value: 0,
            },
            txs: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            witnesses: Vec::new(),
        };
        tx2.send(WorkItem::Block {
            height: 0,
            block: dummy_block,
        })
        .unwrap();
        drop(tx2);
        let e2 = consume_ordered(
            rx2,
            0,
            1,
            10,
            &out_dir,
            &store,
            &progress,
            &missing,
            &recorder,
            &reorder,
            &next_h,
            Some(&tripped_wd),
            false,
            &file_cache,
            2,
            None,
        )
        .unwrap_err();
        assert!(e2.to_string().contains("watchdog"), "got {e2}");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A unique temp dir for a UTXO store in a pipeline test.
    fn tmp_utxo_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "bidx-shed-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn thread_file_cache_shrink_to_lowers_the_bound_and_evicts() {
        // `shrink_to` is the relief lever: it must lower the resident bound and
        // evict down to it, and must never grow the bound back (a later, larger
        // request is ignored).
        let n = 8u32;
        let dir = make_many_blk_files(n);
        let cache = ThreadFileCache::with_capacity(&dir, 8);
        let loc = |file_id: u32| BlockLocation {
            file_id,
            offset: 0,
            record_len: (BLOCK_HEADER_LEN + 1 + 47 + 8) as u32,
        };
        for file_id in 0..n {
            cache.block_bytes(&loc(file_id)).unwrap();
        }
        assert_eq!(cache.resident(), 8);
        cache.shrink_to(3);
        assert!(cache.resident() <= 3, "shrink must evict to the new bound");
        // Opening more files now stays under the lowered bound.
        for file_id in 0..n {
            cache.block_bytes(&loc(file_id)).unwrap();
        }
        assert!(cache.resident() <= 3);
        // A grow request is a no-op, so relief is not silently undone.
        cache.shrink_to(16);
        cache.block_bytes(&loc(0)).unwrap();
        assert!(cache.resident() <= 3, "shrink_to must not grow the bound");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A transport that reports one stale `getchaintips` branch and serves the
    /// header chain for it, so `run_parse`'s disk pre-flight and orphan fetch
    /// run against scripted data instead of a live node.
    struct OrphanRpc {
        node_tip: u32,
        /// Stale tip hash (height node_tip); its parent is the next entry.
        branch: Vec<bidx_core::Hash32>,
    }

    impl bidx_live::RpcTransport for OrphanRpc {
        fn roundtrip(
            &self,
            body: &serde_json::Value,
        ) -> Result<serde_json::Value, bidx_live::RpcError> {
            let method = body["method"].as_str().unwrap_or("");
            let result = match method {
                "getblockcount" => serde_json::json!(self.node_tip),
                "getchaintips" => serde_json::json!([{
                    "height": self.node_tip,
                    "hash": self.branch[0].to_hex(),
                    // Only the stale tip is orphaned; its ancestors are shared
                    // with the active chain.
                    "branchlen": 1,
                    "status": "valid-fork",
                }]),
                "getblockheader" => {
                    // The requested hash is params[0]; find it in the branch and
                    // return its parent (or null previous for the last entry).
                    let want = body["params"][0].as_str().unwrap_or("");
                    let idx = self
                        .branch
                        .iter()
                        .position(|h| h.to_hex() == want)
                        .unwrap_or(0);
                    let prev = self.branch.get(idx + 1).map(|h| h.to_hex());
                    serde_json::json!({
                        "hash": want,
                        "height": self.node_tip.saturating_sub(idx as u32),
                        "confirmations": 0,
                        "previousblockhash": prev,
                    })
                }
                _ => serde_json::Value::Null,
            };
            Ok(serde_json::json!({ "result": result, "error": null }))
        }
    }

    #[test]
    fn run_parse_pre_flight_skips_orphan_blocks_end_to_end() {
        // Build a 4-block chain, report block 1 as a stale (orphan) branch tip,
        // and confirm run_parse still succeeds while block 1 is skipped.
        let (blocks_dir, hashes) = make_chain(4);
        let out = std::env::temp_dir().join(format!("bidx-pf-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-pf-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();

        // Stale branch: block 1 (tip). branchlen=1 means only block 1 is
        // orphaned; block 0 is shared with the active chain.
        let transport = OrphanRpc {
            node_tip: 3,
            branch: vec![hashes[1], hashes[0]],
        };

        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(4),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: Some("http://unused".into()),
            rpc_cookie: None,
            skip_disk_check: false,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: Some(Box::new(transport)),
        })
        .expect("run_parse with orphan skip");

        // The blocks table must contain heights 0, 2, 3 — never the orphan 1.
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let part = out.join("blocks/part-00000.parquet");
        let f = std::fs::File::open(&part).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut heights = Vec::new();
        for b in reader {
            let b = b.unwrap();
            let col = b
                .column_by_name("height")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            for i in 0..col.len() {
                heights.push(col.value(i));
            }
        }
        heights.sort_unstable();
        assert_eq!(heights, vec![0, 2, 3], "orphan height 1 must be skipped");

        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    #[test]
    fn run_parse_without_rpc_url_skips_pre_flight() {
        // The `else` branch (no rpc_url): the log line runs and parsing proceeds.
        // skip_disk_check=false forces entry into the block, with rpc_url None.
        let (blocks_dir, _) = make_chain(2);
        let out = std::env::temp_dir().join(format!("bidx-norpc-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-norpc-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(2),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: false,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .expect("run_parse without rpc");
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    #[test]
    fn run_parse_builds_an_http_client_when_no_transport_injected() {
        // With rpc_url set and rpc_transport None, run_parse must construct a
        // real NodeClient from the URL (the production path). Pointing at a
        // dead endpoint with no cookie makes `NodeClient::new` fail, and the
        // `?` must attach the "init RPC client" context.
        let (blocks_dir, _) = make_chain(2);
        let out = std::env::temp_dir().join(format!("bidx-http-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-http-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        let e = run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(2),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: Some("http://127.0.0.1:1".into()),
            rpc_cookie: None, // no credentials → NodeClient::new errors
            skip_disk_check: false,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .err()
        .expect("missing credentials must fail");
        assert!(format!("{:#}", e).contains("init RPC client"), "got: {e:#}");
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    /// Transport that answers `getblockcount` but fails `getchaintips`, so the
    /// pre-flight succeeds and the orphan fetch hits its non-fatal error arm.
    struct TipsFailRpc {
        node_tip: u32,
    }

    impl bidx_live::RpcTransport for TipsFailRpc {
        fn roundtrip(
            &self,
            body: &serde_json::Value,
        ) -> Result<serde_json::Value, bidx_live::RpcError> {
            match body["method"].as_str().unwrap_or("") {
                "getblockcount" => {
                    Ok(serde_json::json!({ "result": self.node_tip, "error": null }))
                }
                _ => Err(bidx_live::RpcError::BadResponse(
                    "getchaintips exploded".into(),
                )),
            }
        }
    }

    #[test]
    fn run_parse_continues_when_orphan_fetch_fails() {
        // The orphan fetch is best-effort: a failure logs a warning and parsing
        // proceeds with no orphan filter, so the run still writes its output.
        let (blocks_dir, _) = make_chain(2);
        let out = std::env::temp_dir().join(format!("bidx-tipfail-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-tipfail-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(2),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: Some("http://unused".into()),
            rpc_cookie: None,
            skip_disk_check: false,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: Some(Box::new(TipsFailRpc { node_tip: 1 })),
        })
        .expect("orphan fetch failure must be non-fatal");
        // Both heights were indexed despite the failed orphan fetch.
        assert!(out.join("blocks/part-00000.parquet").exists());
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    /// A transport that reports a tip but whose `getchaintips` returns an empty
    /// list, so the pre-flight runs and the orphan arm takes its `Ok(_)` /
    /// "no orphaned blocks" branch.
    struct NoTipsRpc;

    impl bidx_live::RpcTransport for NoTipsRpc {
        fn roundtrip(
            &self,
            body: &serde_json::Value,
        ) -> Result<serde_json::Value, bidx_live::RpcError> {
            let result = match body["method"].as_str().unwrap_or("") {
                "getblockcount" => serde_json::json!(1),
                "getchaintips" => serde_json::json!([]),
                _ => serde_json::Value::Null,
            };
            Ok(serde_json::json!({ "result": result, "error": null }))
        }
    }

    #[test]
    fn run_parse_auto_detects_network_and_defaults_checkpoint_path() {
        // Covers the three "not provided" fallbacks at the top of `run_parse`:
        //   * `network: None`  -> detect from the blk file's magic bytes;
        //   * `checkpoint_file: None` -> derive `<parent-of-utxo>/bidx-…txt`;
        //   * a checkpoint file that already holds an entry -> the
        //     `read_checkpoints(..).last().map(..)` arm is taken.
        // Also exercises the empty `getchaintips` (no-orphans) log branch.
        let (blocks_dir, _) = make_chain(2);
        // `make_chain` writes mainnet magic, so auto-detection must yield Mainnet.
        let out = std::env::temp_dir().join(format!("bidx-autonet-out-{}", std::process::id()));
        let utxo_parent =
            std::env::temp_dir().join(format!("bidx-autonet-parent-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo_parent);
        std::fs::create_dir_all(&utxo_parent).unwrap();
        let utxo = utxo_parent.join("utxo");
        std::fs::create_dir_all(&utxo).unwrap();
        // Pre-seed the *default* checkpoint file (derived from the utxo parent)
        // so `read_checkpoints` returns a non-empty history.
        let default_ckpt = utxo_parent.join(crate::diskguard::DEFAULT_CHECKPOINT_FILE_NAME);
        bidx_core::append_checkpoint(
            &default_ckpt,
            bidx_core::Network::Mainnet,
            bidx_core::DiskCheckpoint {
                height: 1,
                total_bytes: 1024,
            },
        )
        .unwrap();

        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(2),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: Some("http://unused".into()),
            rpc_cookie: None,
            skip_disk_check: false,
            network: None,         // exercise auto-detect
            checkpoint_file: None, // exercise the default-path fallback
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: Some(Box::new(NoTipsRpc)),
        })
        .expect("run_parse with auto-detected network and default checkpoint path");

        assert!(out.join("blocks/part-00000.parquet").exists());
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo_parent);
    }

    #[test]
    fn run_parse_orphan_at_final_height_finishes_cleanly() {
        // The orphan is the last height in range, so the ordered consumer's
        // skip loop reaches `next == end` and must finish the sink and return
        // Ok (rather than falling through to the "channel closed early" bail).
        let (blocks_dir, hashes) = make_chain(3);
        let out = std::env::temp_dir().join(format!("bidx-lastorph-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-lastorph-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        // branchlen=1: only height 2 (the tip) is orphaned.
        let transport = OrphanRpc {
            node_tip: 2,
            branch: vec![hashes[2]],
        };
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(3),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: Some("http://unused".into()),
            rpc_cookie: None,
            skip_disk_check: false,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: Some(Box::new(transport)),
        })
        .expect("orphan at final height must finish cleanly");

        // Heights 0 and 1 are present; the orphaned height 2 is absent.
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let part = out.join("blocks/part-00000.parquet");
        let f = std::fs::File::open(&part).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut heights = Vec::new();
        for b in reader {
            let b = b.unwrap();
            let col = b
                .column_by_name("height")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            for i in 0..col.len() {
                heights.push(col.value(i));
            }
        }
        heights.sort_unstable();
        assert_eq!(heights, vec![0, 1], "orphan height 2 must be skipped");
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    /// Transport whose `getchaintips` reports only an active tip, so
    /// `orphan_block_hashes` returns an empty set (the "no orphans" branch).
    struct NoOrphanRpc {
        node_tip: u32,
    }

    impl bidx_live::RpcTransport for NoOrphanRpc {
        fn roundtrip(
            &self,
            body: &serde_json::Value,
        ) -> Result<serde_json::Value, bidx_live::RpcError> {
            let result = match body["method"].as_str().unwrap_or("") {
                "getblockcount" => serde_json::json!(self.node_tip),
                "getchaintips" => serde_json::json!([{
                    "height": self.node_tip,
                    "hash": format!("{:064x}", self.node_tip),
                    "branchlen": 0u32,
                    "status": "active",
                }]),
                _ => serde_json::Value::Null,
            };
            Ok(serde_json::json!({ "result": result, "error": null }))
        }
    }

    #[test]
    fn run_parse_with_no_orphans_indexes_every_height() {
        // The node reports only an active tip, so the orphan set is empty and
        // every height in range is indexed (the "no orphaned blocks" branch).
        let (blocks_dir, _) = make_chain(3);
        let out = std::env::temp_dir().join(format!("bidx-noorph-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-noorph-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(3),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: Some("http://unused".into()),
            rpc_cookie: None,
            skip_disk_check: false,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: Some(Box::new(NoOrphanRpc { node_tip: 2 })),
        })
        .expect("run_parse with no orphans");

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let part = out.join("blocks/part-00000.parquet");
        let f = std::fs::File::open(&part).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut heights = Vec::new();
        for b in reader {
            let b = b.unwrap();
            let col = b
                .column_by_name("height")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            for i in 0..col.len() {
                heights.push(col.value(i));
            }
        }
        heights.sort_unstable();
        assert_eq!(
            heights,
            vec![0, 1, 2],
            "all heights indexed when no orphans"
        );
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    #[test]
    fn run_parse_accounts_fees_for_non_coinbase_spends() {
        // Block 1 spends block 0's 100-sat coinbase output for a 60-sat output,
        // so its total_fee is 40. This exercises the coinbase-flag scan and the
        // fee summation in the ordered consumer (all other fixtures are
        // coinbase-only, so the non-coinbase branch never runs).
        let (blocks_dir, _) = make_spend_chain();
        let out = std::env::temp_dir().join(format!("bidx-fee-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-fee-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(2),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .expect("run_parse with a spend");

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let part = out.join("blocks/part-00000.parquet");
        let f = std::fs::File::open(&part).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut fees = std::collections::HashMap::new();
        for b in reader {
            let b = b.unwrap();
            let heights = b
                .column_by_name("height")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            let tf = b
                .column_by_name("total_fee")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            for i in 0..heights.len() {
                fees.insert(heights.value(i), tf.value(i));
            }
        }
        assert_eq!(fees.get(&0), Some(&0), "coinbase-only block has zero fee");
        assert_eq!(
            fees.get(&1),
            Some(&40),
            "block 1 fee = 100 spent - 60 out; got {fees:?}"
        );
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
    }

    fn write_fake_ch_script(tag: &str, body: String) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!(
            "bidx-pipe-ch-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    #[test]
    fn run_parse_streams_parts_to_clickhouse_and_unlinks_when_configured() {
        let _g = crate::testlock::fork_guard();
        let (blocks_dir, _) = make_chain(3);
        let out = std::env::temp_dir().join(format!("bidx-streamch-out-{}", std::process::id()));
        let utxo = std::env::temp_dir().join(format!("bidx-streamch-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();

        let script = write_fake_ch_script(
            "ok",
            "#!/bin/sh\nq=''\nfor a in \"$@\"; do case \"$a\" in --query=*) q=\"$a\";; esac; done\n\
             cat >/dev/null\necho \"$q\" >> \"$0.log\"\nexit 0\n"
                .to_string(),
        );
        let ch_cfg = bidx_loader::ChConfig {
            database: "btc".into(),
            ..bidx_loader::ChConfig::default()
        };
        let loader = bidx_loader::ChLoader::with_client(ch_cfg, script.clone());

        run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out.clone(),
            utxo: utxo.clone(),
            start: 0,
            end: Some(3),
            blocks_per_part: 2, // rotates: part 0 = [0,1], part 1 = [2]
            threads: Some(2),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("ckpts.txt")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: Some(StreamClickHouseConfig {
                loader,
                mode: bidx_loader::LoadMode::Upload,
                unlink_loaded_parts: true,
            }),
            rpc_transport: None,
        })
        .expect("streaming parse to ClickHouse");

        let log = std::fs::read_to_string(format!("{}.log", script.display())).unwrap();
        assert!(log.contains("DROP VIEW IF EXISTS btc.outputs_by_address_mv"));
        for entity in bidx_loader::schema::ENTITIES {
            assert!(log.contains(&format!("INSERT INTO btc.{entity} FORMAT Parquet")));
            assert!(
                !out.join(entity).join("part-00000.parquet").exists(),
                "part 0 must be unlinked for {entity}"
            );
            assert!(
                !out.join(entity).join("part-00001.parquet").exists(),
                "part 1 must be unlinked for {entity}"
            );
        }
        assert!(log.contains("INSERT INTO btc.outputs_by_address "));
        assert!(log.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS btc.outputs_by_address_mv"));

        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
        let _ = fs::remove_file(format!("{}.log", script.display()));
        let _ = fs::remove_file(&script);
    }

    #[test]
    fn run_parse_surfaces_consumer_and_clickhouse_streaming_failures() {
        let _g = crate::testlock::fork_guard();
        let (blocks_dir, _) = make_chain(8);
        let utxo = std::env::temp_dir().join(format!("bidx-cherr-utxo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();

        // 1) Consumer fails without ClickHouse streaming (`out` is a regular file).
        let bad_out = std::env::temp_dir().join(format!("bidx-cherr-file-{}", std::process::id()));
        std::fs::write(&bad_out, "not-a-dir").unwrap();
        assert!(run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: bad_out.clone(),
            utxo: utxo.join("u1"),
            start: 0,
            end: Some(1),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("c1")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: None,
            rpc_transport: None,
        })
        .is_err());
        let _ = fs::remove_file(&bad_out);

        // 2) `begin_streaming_load` fails before RocksDB opens.
        let fail_all = write_fake_ch_script(
            "begin-fail",
            "#!/bin/sh\necho 'ch down' >&2\nexit 1\n".to_string(),
        );
        let out2 = std::env::temp_dir().join(format!("bidx-cherr-o2-{}", std::process::id()));
        let e2 = run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out2.clone(),
            utxo: utxo.join("u2"),
            start: 0,
            end: Some(2),
            blocks_per_part: 1,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("c2")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: Some(StreamClickHouseConfig {
                loader: bidx_loader::ChLoader::with_client(
                    bidx_loader::ChConfig::default(),
                    fail_all.clone(),
                ),
                mode: bidx_loader::LoadMode::ServerFile,
                unlink_loaded_parts: false,
            }),
            rpc_transport: None,
        })
        .unwrap_err();
        assert!(format!("{e2:#}").contains("prepare ClickHouse"), "{e2:#}");
        let _ = fs::remove_file(&fail_all);

        // 3) `load_part` fails across 8 parts (`blocks_per_part: 1`), forcing
        //    the sink to hit `part_tx.send` error and surfacing the underlying
        //    ClickHouse part-load error; also test 1-part failure where
        //    `consumer` finishes cleanly before `ch_handle.join()` surfaces it.
        let fail_insert = write_fake_ch_script(
            "part-fail",
            "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in --query=*INSERT*) echo 'part boom' >&2; exit 1;; esac; done\nexit 0\n"
                .to_string(),
        );
        for (end_h, tag) in [(8u32, "many"), (1u32, "one")] {
            let out_p =
                std::env::temp_dir().join(format!("bidx-cherr-{tag}-{}", std::process::id()));
            let ep = run_parse(ParseConfig {
                blocks_dir: blocks_dir.clone(),
                out: out_p.clone(),
                utxo: utxo.join(format!("u-{tag}")),
                start: 0,
                end: Some(end_h),
                blocks_per_part: 1,
                threads: Some(1),
                rpc_url: None,
                rpc_cookie: None,
                skip_disk_check: true,
                network: Some(bidx_core::Network::Mainnet),
                checkpoint_file: Some(utxo.join(format!("c-{tag}"))),
                orphan_blocks: None,
                capture_witness: false,
                block_cache_mb: None,
                utxo_cache_mb: None,
                stream_clickhouse: Some(StreamClickHouseConfig {
                    loader: bidx_loader::ChLoader::with_client(
                        bidx_loader::ChConfig::default(),
                        fail_insert.clone(),
                    ),
                    mode: bidx_loader::LoadMode::ServerFile,
                    unlink_loaded_parts: false,
                }),
                rpc_transport: None,
            })
            .unwrap_err();
            assert!(
                format!("{ep:#}").contains("ClickHouse load of part"),
                "{ep:#}"
            );
            let _ = fs::remove_dir_all(&out_p);
        }
        let _ = fs::remove_file(&fail_insert);

        // 4) `finish_streaming_load` fails on backfill (`TRUNCATE TABLE`).
        let fail_finish = write_fake_ch_script(
            "finish-fail",
            "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in --query=*TRUNCATE*) echo 'backfill boom' >&2; exit 1;; esac; done\nexit 0\n"
                .to_string(),
        );
        let out4 = std::env::temp_dir().join(format!("bidx-cherr-o4-{}", std::process::id()));
        let e4 = run_parse(ParseConfig {
            blocks_dir: blocks_dir.clone(),
            out: out4.clone(),
            utxo: utxo.join("u4"),
            start: 0,
            end: Some(1),
            blocks_per_part: 10,
            threads: Some(1),
            rpc_url: None,
            rpc_cookie: None,
            skip_disk_check: true,
            network: Some(bidx_core::Network::Mainnet),
            checkpoint_file: Some(utxo.join("c4")),
            orphan_blocks: None,
            capture_witness: false,
            block_cache_mb: None,
            utxo_cache_mb: None,
            stream_clickhouse: Some(StreamClickHouseConfig {
                loader: bidx_loader::ChLoader::with_client(
                    bidx_loader::ChConfig::default(),
                    fail_finish.clone(),
                ),
                mode: bidx_loader::LoadMode::ServerFile,
                unlink_loaded_parts: false,
            }),
            rpc_transport: None,
        })
        .unwrap_err();
        assert!(format!("{e4:#}").contains("finalize ClickHouse"), "{e4:#}");

        let _ = fs::remove_file(&fail_finish);
        let _ = fs::remove_dir_all(&out2);
        let _ = fs::remove_dir_all(&out4);
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&utxo);
    }

    #[test]
    fn sink_thread_surfaces_dropped_part_channel_on_rotate_and_finish() {
        let out_rot =
            std::env::temp_dir().join(format!("bidx-sink-pdrop-rot-{}", std::process::id()));
        let (tx, rx) = bounded::<SinkMsg>(4);
        let (ptx, prx) = bounded::<u32>(1);
        drop(prx);
        tx.send(SinkMsg::Block {
            block: synthetic_full_block(0),
            spends: Vec::new(),
        })
        .unwrap();
        tx.send(SinkMsg::Block {
            block: synthetic_full_block(2),
            spends: Vec::new(),
        })
        .unwrap();
        drop(tx);
        let e_rot =
            sink_thread_main(rx, &out_rot, 2, 0, SinkConfig::default(), Some(ptx)).unwrap_err();
        assert!(e_rot.to_string().contains("stopped early"));
        let _ = fs::remove_dir_all(&out_rot);

        let out_fin =
            std::env::temp_dir().join(format!("bidx-sink-pdrop-fin-{}", std::process::id()));
        let (tx2, rx2) = bounded::<SinkMsg>(4);
        let (ptx2, prx2) = bounded::<u32>(1);
        drop(prx2);
        tx2.send(SinkMsg::Finish).unwrap();
        let e_fin =
            sink_thread_main(rx2, &out_fin, 2, 0, SinkConfig::default(), Some(ptx2)).unwrap_err();
        assert!(e_fin.to_string().contains("stopped early"));
        let _ = fs::remove_dir_all(&out_fin);
    }
}
