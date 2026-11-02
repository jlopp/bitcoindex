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
use bidx_parser::{parse_block_full, BlkFile, FullBlock};
use bidx_store::{ParquetSink, SinkConfig, UtxoStore};
use crossbeam_channel::{bounded, Receiver};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
/// Get a sensible RSS ceiling for `bidx parse` based on memory that is
/// actually available, not merely installed.
///
/// Using `MemTotal` here is wrong: it ignores every other process on the box,
/// so on a busy machine the computed ceiling can sit *above* the point where
/// the kernel's OOM killer steps in, and the watchdog never fires. That is
/// exactly how a 30 GiB machine ends up killing bidx at 21 GiB RSS with no
/// diagnostic. `MemAvailable` is the kernel's own estimate of what a new
/// allocation can obtain without swapping, which is the quantity we want.
///
/// We still leave a reserve for the runtime and the page cache the parse stage
/// needs, and cap at 90% so a single runaway pipeline stage cannot consume the
/// whole box.
pub(crate) fn max_rss_kb() -> u64 {
    const RESERVED_FOR_RUNTIME_KB: u64 = 2 * 1024 * 1024; // 2 GiB
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let field_kb = |name: &str| -> Option<u64> {
        meminfo.as_ref()?.lines().find_map(|l| {
            let rest = l.strip_prefix(name)?;
            rest.split_whitespace().next()?.parse::<u64>().ok()
        })
    };
    let total_kb = field_kb("MemTotal:").unwrap_or(16 * 1024 * 1024);
    // Fall back to MemTotal when MemAvailable is missing (older kernels).
    let available_kb = field_kb("MemAvailable:").unwrap_or(total_kb);
    available_kb
        .saturating_sub(available_kb / 10)
        .saturating_sub(RESERVED_FOR_RUNTIME_KB)
        .max(256 * 1024) // never return something absurdly small
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
/// Worker -> consumer channel depth. Bounded on purpose: an unbounded
/// channel is a memory leak under backpressure.
pub const CHANNEL_DEPTH: usize = 512;

#[derive(Debug, Clone)]
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
}

/// A parse-stage message: either a fully-parsed block, or a "skipped"
/// marker for a height whose block hash is in the orphan set (we skip its
/// data but must still advance the ordered consumer past that height).
pub(crate) enum WorkItem {
    Block {
        height: u32,
        block: FullBlock,
    },
    Skipped {
        height: u32,
    },
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
        .unwrap_or_else(|| PathBuf::from("."));
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
            let node = bidx_live::NodeClient::new(bidx_live::RpcConfig {
                url: rpc_url.clone(),
                cookie_path: cfg.rpc_cookie.clone(),
                user: None,
                password: None,
            })
            .context("init RPC client for disk pre-flight")?;
            let node_tip = node.get_block_count().context("getblockcount")?;
            let local_tip = bidx_core::read_checkpoints(&disk_cfg.checkpoint_file, disk_cfg.network)
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
                    warn!(count = set.len(), "fetched orphan block hashes; they will be skipped");
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
    let utxo = Arc::new(UtxoStore::open(&cfg.utxo)?);

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

    let blocks_dir = cfg.blocks_dir.clone();
    let heights: Vec<u32> = (start..end).collect();
    let index_for_workers = Arc::clone(&index);
    let orphan_blocks = cfg.orphan_blocks.clone();
    let orphans_skipped = Arc::new(AtomicU64::new(0));
    let reorder_depth_w = Arc::clone(&reorder_depth);

    // Spawn the ordered consumer on its own thread.
    let out_dir = cfg.out.clone();
    let blocks_per_part = cfg.blocks_per_part;
    let recorder = Arc::new(DiskRecorder::new(disk_cfg.clone()));
    // The watchdog must exist before the consumer is spawned so the consumer
    // can poll it. `Arc` because the consumer thread needs its own handle;
    // the original binding keeps it alive for the whole run.
    let watchdog = Arc::new(crate::bench::OomWatchdog::start(max_rss_kb()));
    let consumer = std::thread::spawn({
        let progress = progress.clone();
        let missing_utxos = Arc::clone(&missing_utxos);
        let recorder = Arc::clone(&recorder);
        let reorder_depth = Arc::clone(&reorder_depth_w);
        let next_height = Arc::clone(&next_height);
        let watchdog = Arc::clone(&watchdog);
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
    let file_cache = ThreadFileCache::new(&blocks_dir);
    // Workers need the watchdog too: when it trips, the consumer returns and
    // drops `rx`, so every worker's next `send` fails. Reporting that as
    // "consumer dropped" would hide the actual cause, so the send-failure path
    // checks the watchdog first.
    let watchdog_w = Arc::clone(&watchdog);

    let parse_result: Result<()> = pool.install(|| {
        heights.par_iter().try_for_each_init(
            || {
                (
                    file_cache.clone(),
                    tx.clone(),
                    Arc::clone(&reorder_depth_w),
                    Arc::clone(&next_height_w),
                    Arc::clone(&watchdog_w),
                )
            },
            |(cache, tx, reorder, next_h, watchdog), &height| -> Result<()> {
                // Backpressure: spin/sleep until consumer drains the
                // reorder buffer below the watermark. This caps the
                // consumer's BTreeMap at ~REORDER_HIGH parsed blocks.
                //
                // Two guards make this safe. The watchdog check stops an
                // aborted consumer (RSS over limit) from turning the spin into
                // a hang. The `next_h` exemption stops a deadlock: the worker
                // holding the height the consumer is waiting for must run, or
                // no block ever arrives to let the consumer drain.
                while reorder.load(Ordering::Relaxed) > REORDER_HIGH
                    && next_h.load(Ordering::Relaxed) != height
                {
                    if watchdog.is_tripped() {
                        return Err(watchdog.tripped_error());
                    }
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                let loc = &index_for_workers.by_height[height as usize];
                let bytes = cache.block_bytes(loc)?;
                // We need the block's header to pass into parse; re-parse the
                // 80-byte header from the payload (cheap) and reuse the hash
                // from the chain index... but ChainIndex doesn't store hashes
                // per height in a Vec. Compute hash from header bytes here.
                let mut header_raw = [0u8; 80];
                header_raw.copy_from_slice(&bytes[..80]);
                let header = bidx_core::BlockHeader::parse(&header_raw);
                let hash = bidx_core::dsha256(&header_raw);
                // A send failure means the consumer is gone. That is either a
                // real bug or the RSS watchdog having aborted it; report the
                // actual cause rather than a generic "consumer dropped".
                let send_failed = |w: &crate::bench::OomWatchdog| -> anyhow::Error {
                    if w.is_tripped() {
                        w.tripped_error()
                    } else {
                        anyhow::anyhow!("consumer dropped")
                    }
                };
                // Skip orphaned (stale / side-chain) blocks. Their UTXOs were
                // never part of the active chain; indexing them would poison
                // the UTXO set and emit duplicate rows. We still must notify
                // the ordered consumer so it can advance past this height.
                if let Some(orphans) = &orphan_blocks {
                    if orphans.contains(&hash) {
                        orphans_skipped.fetch_add(1, Ordering::Relaxed);
                        tx.send(WorkItem::Skipped { height })
                            .map_err(|_| send_failed(watchdog))?;
                        return Ok(());
                    }
                }
                let block = parse_block_full(&bytes, height, hash, &header)?;
                tx.send(WorkItem::Block { height, block })
                    .map_err(|_| send_failed(watchdog))?;
                Ok(())
            },
        )
    });
    // Close the channel so the consumer can terminate.
    drop(tx);
    parse_result?;

    consumer
        .join()
        .map_err(|_| anyhow::anyhow!("consumer panicked"))??;

    progress.finish_with_message("parse complete");
    println!("parsed {} blocks", parsed.load(Ordering::Relaxed));
    println!("missing utxos: {}", missing_utxos.load(Ordering::Relaxed));
    let skipped = orphans_skipped.load(Ordering::Relaxed);
    if skipped > 0 {
        println!("skipped {} orphaned (stale) blocks", skipped);
    }
    Ok(())
}

/// Per-process bounded LRU cache of mmap'd blk files.
///
/// Each `BlkFile` is ~128 MiB in memory — we keep at most
/// `MAX_CACHED_FILES` decoded at once to bound RSS. Signet uses XOR-
/// obfuscated blk files (Bitcoin Core v28), so each open allocates a
/// 128 MiB decoded `Vec<u8>`; without eviction, 12 workers opening all
/// 149 files share ~149 × 128 MiB ≈ 19 GiB. With a 16-file LRU we cap
/// at ~2 GiB. XOR decode (~150 ms per 128 MiB) only runs on cache miss.
const MAX_CACHED_FILES: usize = 16;

pub(crate) struct ThreadFileCache {
    blocks_dir: PathBuf,
    inner: Arc<CacheInner>,
}

struct CacheInner {
    /// LRU list, oldest at front, most-recently-used at back.
    lru: parking_lot::Mutex<std::collections::VecDeque<u32>>,
    map: parking_lot::RwLock<FxHashMap<u32, Arc<BlkFile>>>,
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
    pub(crate) fn new(blocks_dir: &Path) -> Self {
        ThreadFileCache {
            blocks_dir: blocks_dir.to_path_buf(),
            inner: Arc::new(CacheInner {
                lru: parking_lot::Mutex::new(std::collections::VecDeque::with_capacity(MAX_CACHED_FILES)),
                map: parking_lot::RwLock::new(FxHashMap::default()),
            }),
        }
    }

    /// Number of `BlkFile`s currently resident in the shared LRU. Used by
    /// `bidx bench --trace-cache` to prove the decoded-file bound actually
    /// holds; a value above `MAX_CACHED_FILES` means eviction is broken.
    /// Number of `BlkFile`s currently resident in the shared LRU.
    ///
    /// Exposed for tests: the whole point of `MAX_CACHED_FILES` is that this
    /// never exceeds it, and an assertion on the count is the only way to
    /// catch an eviction regression that would otherwise only show up as a
    /// 19 GiB RSS spike on a long run.
    #[cfg(test)]
    pub(crate) fn resident(&self) -> usize {
        self.inner.map.read().len()
    }

    fn touch(&self, file_id: u32) {
        let mut lru = self.inner.lru.lock();
        // Move `file_id` to the back (most recently used).
        if let Some(pos) = lru.iter().position(|&id| id == file_id) {
            lru.remove(pos);
        }
        lru.push_back(file_id);
        // Evict oldest if over capacity.
        while lru.len() > MAX_CACHED_FILES {
            if let Some(old) = lru.pop_front() {
                if old != file_id {
                    self.inner.map.write().remove(&old);
                }
            }
        }
    }

    pub(crate) fn block_bytes(&self, loc: &bidx_core::BlockLocation) -> Result<std::sync::Arc<[u8]>> {
        let f = {
            let files = self.inner.map.read();
            files.get(&loc.file_id).cloned()
        };
        let f = match f {
            Some(bf) => {
                self.touch(loc.file_id);
                bf
            }
            None => {
                let path = self
                    .blocks_dir
                    .join(format!("blk{:05}.dat", loc.file_id));
                let bf = Arc::new(BlkFile::open(&path, loc.file_id)?);
                self.inner.map.write().insert(loc.file_id, Arc::clone(&bf));
                self.touch(loc.file_id);
                bf
            }
        };
        let slice = f.block_bytes(loc)?;
        Ok(Arc::from(slice))
    }
}

/// Receives out-of-order parsed blocks, buffers until the next expected
/// height arrives, applies UTXO changes in order, and writes to Parquet.
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
) -> Result<()> {
    let mut buffer: BTreeMap<u32, FullBlock> = BTreeMap::new();
    let mut skipped_queue: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    let mut next = start;
    next_height.store(next, Ordering::Relaxed);

    let mut part_id = start / blocks_per_part;
    let mut sink = ParquetSink::create(out_dir, part_id, SinkConfig::default())?;

    while let Ok(item) = rx.recv() {
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
                sink.finish()?;
                return Ok(());
            }
        }

        while let Some(block) = buffer.remove(&next) {
            // Poll the RSS watchdog once per block. It is a relaxed atomic
            // load, so the cost is nil next to apply_block.
            if let Some(w) = watchdog {
                if w.is_tripped() {
                    return Err(w.tripped_error());
                }
            }
            // Drained one from reorder buffer — signal workers that there
            // is space for more.
            reorder_depth.fetch_sub(1, Ordering::Relaxed);
            // Rotate part file at boundaries.
            let wanted_part = next / blocks_per_part;
            if wanted_part != part_id {
                sink.finish()?;
                part_id = wanted_part;
                sink = ParquetSink::create(out_dir, part_id, SinkConfig::default())?;
            }

            let mut block = block;
            let res = utxo.apply_block(next, &block.txs, &block.inputs, &block.outputs)?;
            missing_utxos.fetch_add(res.missing, Ordering::Relaxed);

            // Enrich txs with fees and block with total fee.
            let mut total_fee = 0u64;
            for (i, tx) in block.txs.iter_mut().enumerate() {
                tx.fee = res.fees[i];
                if !block.inputs.iter().any(|inp| inp.tx_index == i as u32 && inp.is_coinbase) {
                    total_fee = total_fee.saturating_add(res.fees[i]);
                }
            }
            block.block_row.total_fee = total_fee;

            sink.push_block(&block.block_row)?;
            for tx in &block.txs {
                sink.push_tx(tx)?;
            }
            for inp in &block.inputs {
                sink.push_input(inp)?;
            }
            for out in &block.outputs {
                sink.push_output(out)?;
            }
            for sp in &res.spends {
                sink.push_spend(sp)?;
            }

            progress.inc(1);
            let _ = recorder.on_block_applied(next)?;
            next += 1;
            next_height.store(next, Ordering::Relaxed);
            if next >= end {
                sink.finish()?;
                return Ok(());
            }
        }
    }

    // Channel closed before we saw every height: that's an error.
    sink.finish()?;
    if next < end {
        anyhow::bail!("channel closed early: stopped at height {next}, expected {end}");
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
    println!("  txs={} size={} weight={}", block.block_row.tx_count, block.block_row.size, block.block_row.weight);
    println!("  inputs={} outputs={}", block.inputs.len(), block.outputs.len());
    for tx in block.txs.iter().take(3) {
        let txid = bidx_core::Hash32::from_bytes(tx.txid);
        println!("  tx[{}] {} in={} out={} witness={}", tx.tx_index, txid, tx.input_count, tx.output_count, tx.has_witness);
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
        let has_part0 = walk.iter().filter(|n| n.contains("part-00000.parquet")).count();
        let has_part1 = walk.iter().filter(|n| n.contains("part-00001.parquet")).count();
        assert_eq!(has_part0, 5, "expected all 5 entites to have part-00000: {:?}", walk);
        // With blocks_per_part=2 the 3rd block (h=2) lands in part 1.
        assert_eq!(has_part1, 5, "expected all 5 entities to have part-00001: {:?}", walk);
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
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
        })
        .err()
        .expect("should error");
        assert!(format!("{:#}", e).contains("empty range"));
        let _ = fs::remove_dir_all(&blocks_dir);
        let _ = fs::remove_dir_all(&out);
        let _ = fs::remove_dir_all(&utxo);
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
}
