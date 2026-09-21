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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
/// Get a sensible RSS ceiling for `bidx parse` based on available RAM.
/// We leave ~2 GiB for OS + runtime + the page cache the bench tool
/// needs for parsing. Reads /proc/meminfo for `MemTotal`; falls
/// back to ~16 GiB if /proc is missing.
fn max_rss_kb() -> u64 {
    const RESERVED_FOR_RUNTIME_KB: u64 = 2 * 1024 * 1024; // 2 GiB
    let total_kb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .unwrap_or(16 * 1024 * 1024);
    // Cap at 90% of total to leave headroom.
    total_kb.saturating_sub(total_kb / 10).saturating_sub(RESERVED_FOR_RUNTIME_KB)
}

use std::sync::Arc;
use tracing::{info, warn};

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
    let (tx, rx) = bounded::<WorkItem>(512);

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
    // slow and the ordered consumer can't drain the buffer. With each parsed
    // FullBlock typically <10 MB, 32 blocks ≈ 320 MB worst-case headroom.
    const REORDER_HIGH: usize = 32;
    const REORDER_LOW: usize = 16;
    let reorder_depth = Arc::new(AtomicUsize::new(0));

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
    let consumer = std::thread::spawn({
        let progress = progress.clone();
        let missing_utxos = Arc::clone(&missing_utxos);
        let recorder = Arc::clone(&recorder);
        let reorder_depth = Arc::clone(&reorder_depth_w);
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
            )
        }
    });

    // Parallel parse stage. Heights are processed in parallel; each result
    // is sent into the channel tagged with its height for reordering.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.threads.unwrap_or(0))
        .build()?;

    let _watchdog = crate::bench::OomWatchdog::start(max_rss_kb());
    let reorder_depth_w = Arc::clone(&reorder_depth);
    let file_cache = ThreadFileCache::new(&blocks_dir);

    let parse_result: Result<()> = pool.install(|| {
        heights.par_iter().try_for_each_init(
            || (file_cache.clone(), tx.clone(), Arc::clone(&reorder_depth_w)),
            |(cache, tx, reorder), &height| -> Result<()> {
                // Backpressure: spin/sleep until consumer drains the
                // reorder buffer below the watermark. This caps the
                // consumer's BTreeMap at ~REORDER_HIGH parsed blocks.
                while reorder.load(Ordering::Relaxed) > REORDER_HIGH {
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
                // Skip orphaned (stale / side-chain) blocks. Their UTXOs were
                // never part of the active chain; indexing them would poison
                // the UTXO set and emit duplicate rows. We still must notify
                // the ordered consumer so it can advance past this height.
                if let Some(orphans) = &orphan_blocks {
                    if orphans.contains(&hash) {
                        orphans_skipped.fetch_add(1, Ordering::Relaxed);
                        tx.send(WorkItem::Skipped { height })
                            .map_err(|_| anyhow::anyhow!("consumer dropped"))?;
                        return Ok(());
                    }
                }
                let block = parse_block_full(&bytes, height, hash, &header)?;
                tx.send(WorkItem::Block { height, block })
                    .map_err(|_| anyhow::anyhow!("consumer dropped"))?;
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
) -> Result<()> {
    let mut buffer: BTreeMap<u32, FullBlock> = BTreeMap::new();
    let mut skipped_queue: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    let mut next = start;

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
            if next >= end {
                sink.finish()?;
                return Ok(());
            }
        }

        while let Some(block) = buffer.remove(&next) {
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
