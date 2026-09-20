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
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::info;

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
}

/// A parsed block in flight, tagged with its position.
pub(crate) struct WorkItem {
    pub(crate) height: u32,
    pub(crate) block: FullBlock,
}

pub fn run_parse(cfg: ParseConfig) -> Result<()> {
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

    let blocks_dir = cfg.blocks_dir.clone();
    let heights: Vec<u32> = (start..end).collect();
    let index_for_workers = Arc::clone(&index);

    // Spawn the ordered consumer on its own thread.
    let out_dir = cfg.out.clone();
    let blocks_per_part = cfg.blocks_per_part;
    let recorder = Arc::new(DiskRecorder::new(disk_cfg.clone()));
    let consumer = std::thread::spawn({
        let progress = progress.clone();
        let missing_utxos = Arc::clone(&missing_utxos);
        let recorder = Arc::clone(&recorder);
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
            )
        }
    });

    // Parallel parse stage. Heights are processed in parallel; each result
    // is sent into the channel tagged with its height for reordering.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.threads.unwrap_or(0))
        .build()?;

    let parse_result: Result<()> = pool.install(|| {
        heights.par_iter().try_for_each_init(
            || (ThreadFileCache::new(&blocks_dir), tx.clone()),
            |(cache, tx), &height| -> Result<()> {
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
                let block = parse_block_full(&bytes, height, hash, &header)?;
                tx.send(WorkItem { height, block })
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
    Ok(())
}

/// Per-thread cache of mmap'd blk files, keyed by file id.
pub(crate) struct ThreadFileCache {
    blocks_dir: PathBuf,
    files: RefCell<FxHashMap<u32, Arc<BlkFile>>>,
}

impl ThreadFileCache {
    pub(crate) fn new(blocks_dir: &Path) -> Self {
        ThreadFileCache {
            blocks_dir: blocks_dir.to_path_buf(),
            files: RefCell::new(FxHashMap::default()),
        }
    }

    pub(crate) fn block_bytes(&self, loc: &bidx_core::BlockLocation) -> Result<std::sync::Arc<[u8]>> {
        // We return an owned Arc<[u8]> copy to keep the API simple and safe;
        // the mmap page-cache copy is cheap relative to parse cost. If this
        // shows up in profiles, switch to returning a guard tied to the map.
        let mut files = self.files.borrow_mut();
        let f = match files.get(&loc.file_id) {
            Some(f) => f.clone(),
            None => {
                let path = self
                    .blocks_dir
                    .join(format!("blk{:05}.dat", loc.file_id));
                let bf = Arc::new(BlkFile::open(&path, loc.file_id)?);
                files.insert(loc.file_id, bf.clone());
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
) -> Result<()> {
    let mut buffer: BTreeMap<u32, FullBlock> = BTreeMap::new();
    let mut next = start;

    let mut part_id = start / blocks_per_part;
    let mut sink = ParquetSink::create(out_dir, part_id, SinkConfig::default())?;

    while let Ok(item) = rx.recv() {
        buffer.insert(item.height, item.block);

        while let Some(block) = buffer.remove(&next) {
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
