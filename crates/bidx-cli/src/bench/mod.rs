//! `bidx bench` — benchmark the ingest pipeline against a blocks directory.
//!
//! The bench reuses the *exact* pipeline stages from production — header
//! pass, parallel full parse, ordered UTXO apply, Parquet sink — and times
//! each stage with wall-clock, CPU-time, and RSS sampling so we can see
//! where the bottleneck is for a given dataset.
//!
//! Modes (orthogonal stage toggles — not a single linear benchmark):
//!   full   — header pass + parallel parse + ordered UTXO apply + Parquet sink.
//!   headers— header pass only (chain index construction).
//!   parse  — header pass + parallel parse only; sink and UTXO are skipped.
//!   drain  — header pass + parallel parse, but the consumer thread receives
//!            and drops every block without touching UTXO or Parquet. This is
//!            the *pure parse ceiling*: `parse` still runs a consumer that
//!            reorders and enriches, which hides backpressure effects.
//!   utxo   — full stages but Parquet sink writes to /dev/null equivalent
//!            (rows constructed but discarded) — isolates UTXO cost.
//!   sink   — full stages but UTXO apply skipped (no spend/fee enrichment) —
//!            isolates Parquet encoding + I/O cost.
//!
//! Comparing modes against a given dataset answers:
//!   "Is the bottleneck parallel parse, sequential UTXO apply, or sink I/O?"
//!
//! Watch `missing_utxos` in every trial: a nonzero count means `apply_block`
//! could not find a spent outpoint, so the run is measuring a *cold* UTXO
//! store. That is the cheapest possible version of the apply path and will
//! make `utxo_apply` look artificially fast. The bench fails a trial in
//! `full`/`utxo` mode when the count is nonzero unless `--allow-missing-utxos`
//! is passed.
//!
//! Note: bench runs deliberately do *not* write disk checkpoints — use
//! `bidx parse` for real runs that train the disk-space model.

pub mod metrics;

use anyhow::{Context, Result};
use bidx_store::{ParquetSink, SinkConfig, UtxoStore};
use indicatif::{ProgressBar, ProgressStyle};
use metrics::{peak_rss_kb, ResourceSample, StageTimer};
use rayon::prelude::*;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::pipeline::ThreadFileCache;
use crate::pipeline::WorkItem;

#[derive(Debug, Clone, clap::Args)]
pub struct BenchConfig {
    /// Bitcoin Core blocks directory.
    #[arg(long)]
    pub blocks_dir: PathBuf,

    /// Bench mode.
    #[arg(long, value_enum, default_value_t = BenchMode::Full)]
    pub mode: BenchMode,

    /// Start height (inclusive). Default 0.
    #[arg(long, default_value_t = 0)]
    pub start: u32,

    /// End height (exclusive). Default: chain tip.
    #[arg(long)]
    pub end: Option<u32>,

    /// Parser worker threads. Default: num_cpus.
    #[arg(long)]
    pub threads: Option<usize>,

    /// UTXO store path. Default: a tempdir under --utxo-dir, deleted after run.
    #[arg(long)]
    pub utxo: Option<PathBuf>,

    /// Directory to create the throwaway UTXO store in when `--utxo` is not
    /// given. Defaults to `./bidx-perf` in the current working directory.
    ///
    /// The CWD default matters: the previous default was the system temp dir,
    /// which on many machines is tmpfs (RAM-backed) and therefore flatters
    /// RocksDB's write path by hiding the stalls this benchmark exists to
    /// find. A project-local directory lands on the same filesystem as the
    /// data being indexed.
    #[arg(long)]
    pub utxo_dir: Option<PathBuf>,

    /// Directory for Parquet part files in sink/full/utxo modes. Defaults to
    /// `./bidx-perf` in the current working directory (see `--utxo-dir` for
    /// why the temp dir is a bad default). The directory is created if
    /// missing and its contents are left in place so they can be inspected;
    /// nothing is deleted unless it was auto-created.
    #[arg(long)]
    pub out_dir: Option<PathBuf>,

    /// Blocks per Parquet part file. Only relevant for full/sink modes.
    #[arg(long, default_value_t = 10_000)]
    pub blocks_per_part: u32,

    /// Rows per Parquet record batch. Default 250_000 (the production value).
    #[arg(long)]
    pub sink_batch_rows: Option<usize>,

    /// Reorder-buffer high watermark. Workers sleep while the consumer holds
    /// more than this many parsed blocks. Default 32 (production value).
    #[arg(long)]
    pub reorder_high: Option<usize>,

    /// Worker -> consumer channel depth. Default 512 (production value).
    #[arg(long)]
    pub channel_depth: Option<usize>,

    /// Number of trial runs to repeat. Results are reported per-trial and
    /// aggregated (min / median / max). Default 1.
    #[arg(long, default_value_t = 1)]
    pub repeat: u32,

    /// Fail a trial if `apply_block` could not resolve a spent outpoint.
    /// On by default; pass this flag to accept a cold/dirty UTXO store (for
    /// example when deliberately measuring a partial re-index).
    #[arg(long, default_value_t = false)]
    pub allow_missing_utxos: bool,

    /// Enable the in-memory recent-UTXO cache with this budget, in MiB. 0 (the
    /// default) disables it, which is the baseline. The cache keeps the most
    /// recently created outputs in memory so a spend of a recent output is
    /// answered without a RocksDB lookup. Only applies in bulk (non-undo) mode.
    #[arg(long, default_value_t = 0)]
    pub utxo_cache_mb: usize,

    /// Emit a RocksDB internal-properties snapshot per trial. Off by default
    /// because reading some properties can block briefly on a running DB.
    #[arg(long, default_value_t = false)]
    pub rocksdb_stats: bool,

    /// Write per-trial results as JSON to this path. Use "-" for stdout.
    #[arg(long)]
    pub csv: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchMode {
    Full,
    Headers,
    Parse,
    Drain,
    Utxo,
    Sink,
}

impl std::fmt::Display for BenchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

impl BenchMode {
    /// True when the ordered consumer should open a UTXO store and call
    /// `apply_block`. Single source of truth for the mode -> stage mask, so
    /// `run_trial`, `needs_utxo` and the tests cannot drift apart.
    fn needs_utxo(self) -> bool {
        matches!(self, BenchMode::Full | BenchMode::Utxo)
    }

    /// True when the ordered consumer should build Parquet rows.
    fn needs_sink(self) -> bool {
        matches!(self, BenchMode::Full | BenchMode::Sink)
    }

    /// True when the ordered consumer should enrich blocks with fees, which
    /// requires the UTXO result. `sink` mode writes rows without fees.
    fn needs_fee_enrichment(self) -> bool {
        self.needs_utxo()
    }
}

#[derive(Debug, Clone, Serialize)]
struct StageReport {
    name: &'static str,
    wall_secs: f64,
    cpu_util: f64,
    peak_rss_mb: f64,
}

/// Wall-clock latency distribution for a stage that runs once per block.
///
/// The aggregate stage timers above only sum durations, so a single
/// pathological block (a compaction stall, a reorder-buffer spin, an XOR
/// cache miss on a 128 MiB file) is invisible. These percentiles surface it.
#[derive(Debug, Clone, Default, Serialize)]
struct LatencyReport {
    /// Number of samples the percentiles were computed from.
    n: u64,
    p50_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    /// Total of all samples in seconds; should track the stage wall clock.
    sum_secs: f64,
}

impl LatencyReport {
    fn from_samples(mut samples: Vec<u64>) -> Self {
        if samples.is_empty() {
            return LatencyReport::default();
        }
        samples.sort_unstable();
        let n = samples.len() as u64;
        let pick = |q: f64| -> f64 {
            let idx = ((n as f64 - 1.0) * q).round() as usize;
            samples[idx] as f64 / 1e6
        };
        let sum: u64 = samples.iter().sum();
        LatencyReport {
            n,
            p50_ms: pick(0.50),
            p99_ms: pick(0.99),
            max_ms: samples[samples.len() - 1] as f64 / 1e6,
            sum_secs: sum as f64 / 1e9,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct TrialReport {
    trial: u32,
    mode: BenchMode,
    blocks: u64,
    /// Blocks per second across the pipeline stages only. Excludes the header
    /// pass, which is a fixed whole-corpus scan and would otherwise dominate
    /// short ranges.
    blocks_per_sec: f64,
    /// Blocks per second including the header pass. This is the number that
    /// corresponds to end-to-end wall clock, so it is the one to quote for a
    /// full-corpus run.
    blocks_per_sec_ex_header: f64,
    user_cpu_secs: f64,
    peak_rss_mb: f64,
    /// Non-coinbase inputs whose outpoint was absent from the UTXO set. Must
    /// be 0 for a trustworthy apply-path measurement.
    missing_utxos: u64,
    /// H2 instrumentation. `db_lookups` is the number of non-coinbase inputs
    /// that had to be read from RocksDB (i.e. were not resolved from the
    /// in-block `pending` map) — the exact set a batched prefetch would cover.
    /// `db_nanos` is the time spent inside those point lookups. If
    /// `db_lookups == 0`, no prefetch could help this run at all.
    db_lookups: u64,
    db_nanos: u64,
    pending_hits: u64,
    /// Inputs resolved from the in-memory recent-UTXO cache (0 when disabled).
    cache_hits: u64,
    stages: Vec<StageReport>,
    parse_latency: LatencyReport,
    utxo_latency: LatencyReport,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rocksdb: Vec<RocksDbProperty>,
}

#[derive(Debug, Clone, Serialize)]
struct RocksDbProperty {
    name: String,
    value: String,
}

pub fn run_bench(cfg: BenchConfig) -> Result<()> {
    if cfg.blocks_dir.read_dir()?.next().is_none() {
        anyhow::bail!("{} has no block files", cfg.blocks_dir.display());
    }

    // Default UTXO dir: under `--utxo-dir`, which itself defaults to the
    // project-local `./bidx-perf` root (see `default_perf_dir` for why not
    // temp). We can delete it on drop without the `tempfile` crate, which
    // currently can't be fetched offline.
    let mut utxo_cleanup: Option<PathBuf> = None;
    let utxo_path = match (&cfg.utxo, cfg.mode) {
        (Some(p), _) => p.clone(),
        (None, m) if m.needs_utxo() => {
            let root = cfg
                .utxo_dir
                .clone()
                .unwrap_or_else(default_perf_dir);
            // The root may not exist yet (first run in a fresh checkout), so
            // create it before the per-run subdirectory.
            std::fs::create_dir_all(&root)?;
            let p = root.join(format!(
                "bidx-bench-utxo-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            ));
            std::fs::create_dir_all(&p)?;
            utxo_cleanup = Some(p.clone());
            p
        }
        _ => PathBuf::from("/dev/null"),
    };

    let mut trials = Vec::with_capacity(cfg.repeat as usize);
    for trial in 0..cfg.repeat {
        let report = run_trial(&cfg, &utxo_path, trial)?;
        print_trial(&report);
        trials.push(report);
        // NOTE: between trials on the same data, RocksDB UTXO state carries
        // forward, so apply_block cost changes (fewer "missing" spends on
        // later trials). For pipelines == trial count > 1 we recommend
        // passing `--utxo` with a fresh dir per run, or accept the drift.
    }

    if cfg.repeat > 1 {
        print_aggregate(&trials);
    }

    if let Some(path) = &cfg.csv {
        let json = serde_json::to_string_pretty(&trials)?;
        if path.as_os_str() == "-" {
            println!("{}", json);
        } else {
            std::fs::write(path, json)?;
        }
    }

    if let Some(p) = utxo_cleanup {
        let _ = std::fs::remove_dir_all(&p);
    }

    Ok(())
}

/// Runtime-tunable copy of the production pipeline constants, so the bench can
/// sweep them without a rebuild. Defaults mirror `pipeline.rs`.
#[derive(Debug, Clone, Copy)]
struct Tuning {
    reorder_high: usize,
    channel_depth: usize,
    sink_batch_rows: usize,
}

impl Tuning {
    fn from_cfg(cfg: &BenchConfig) -> Self {
        Tuning {
            reorder_high: cfg.reorder_high.unwrap_or(crate::pipeline::REORDER_HIGH),
            channel_depth: cfg.channel_depth.unwrap_or(512),
            sink_batch_rows: cfg
                .sink_batch_rows
                .unwrap_or_else(|| SinkConfig::default().batch_size),
        }
    }

    fn sink_config(&self) -> SinkConfig {
        SinkConfig {
            batch_size: self.sink_batch_rows,
        }
    }
}

fn run_trial(cfg: &BenchConfig, utxo_path: &Path, trial: u32) -> Result<TrialReport> {
    let mut stages: Vec<StageReport> = Vec::new();

    // --- Stage 1: header pass ---
    let t = StageTimer::start();
    let (index, stats) = bidx_parser::build_chain_index(&cfg.blocks_dir)
        .context("header pass failed")?;
    let header_m = t.stop();
    stages.push(report_stage("header_pass", &header_m, cfg.threads));
    if cfg.mode == BenchMode::Headers {
        let tip = index.tip_height().unwrap_or(0);
        return Ok(TrialReport {
            trial,
            mode: cfg.mode,
            blocks: tip as u64 + 1,
            blocks_per_sec: (tip as f64 + 1.0) / header_m.wall_secs().max(1e-6),
            blocks_per_sec_ex_header: (tip as f64 + 1.0) / header_m.wall_secs().max(1e-6),
            user_cpu_secs: jiffies_to_secs(header_m.cpu_jiffies),
            peak_rss_mb: header_m.peak_rss_kb as f64 / 1024.0,
            missing_utxos: 0,
            db_lookups: 0,
            db_nanos: 0,
            pending_hits: 0,
            cache_hits: 0,
            stages,
            parse_latency: LatencyReport::default(),
            utxo_latency: LatencyReport::default(),
            rocksdb: Vec::new(),
        });
    }

    let tip = index.tip_height().context("empty chain")?;
    let start = cfg.start.min(tip);
    let end = cfg.end.unwrap_or(tip + 1).min(tip + 1);
    if start >= end {
        anyhow::bail!("empty range");
    }
    let total = (end - start) as u64;
    let tuning = Tuning::from_cfg(cfg);
    println!(
        "bench trial {}: {} blocks (heights {}..{}) mode={} threads={} reorder_high={} \
         channel_depth={} sink_batch_rows={}",
        trial,
        total,
        start,
        end,
        cfg.mode,
        cfg.threads.unwrap_or(num_cpus()),
        tuning.reorder_high,
        tuning.channel_depth,
        tuning.sink_batch_rows,
    );

    let index = Arc::new(index);
    let heights: Vec<u32> = (start..end).collect();
    let do_utxo = cfg.mode.needs_utxo();
    let do_sink = cfg.mode.needs_sink();
    // `drain` and `parse` both need the consumer thread to exist: drain's
    // whole purpose is to measure the parse stage with a consumer that does
    // no work, so skipping the consumer would measure nothing at all.
    let do_parse_stages =
        do_utxo || do_sink || matches!(cfg.mode, BenchMode::Parse | BenchMode::Drain);

    // Stage accumulators, shared across worker + consumer threads.
    let parse_wall = Arc::new(AtomicU64::new(0)); // ns
    let utxo_wall = Arc::new(AtomicU64::new(0));
    let sink_wall = Arc::new(AtomicU64::new(0));
    let blocks_done = Arc::new(AtomicU64::new(0));
    let missing_utxos = Arc::new(AtomicU64::new(0));
    let db_lookups = Arc::new(AtomicU64::new(0));
    let db_nanos = Arc::new(AtomicU64::new(0));
    let pending_hits = Arc::new(AtomicU64::new(0));
    let cache_hits = Arc::new(AtomicU64::new(0));
    // Per-block samples for the latency distribution. Only populated on the
    // consumer thread, so a plain Vec behind a mutex would do; we keep them
    // in the closure and return them with the join handle.
    let mut parse_samples: Vec<u64> = Vec::new();
    let utxo_samples: Vec<u64>;

    // UTXO store, opened before parse so workers don't wait on consumer.
    //
    // `--rocksdb-stats` implies a statistics-enabled open, because the
    // stall counters live in `rocksdb.stats` and are only populated when
    // statistics are on. That open has slightly higher per-op overhead, so
    // it is opt-in and the flag is reported alongside the numbers.
    let utxo = if do_utxo {
        let store = match (cfg.rocksdb_stats, cfg.utxo_cache_mb) {
            (true, 0) => UtxoStore::open_statistics(utxo_path),
            (false, 0) => UtxoStore::open(utxo_path),
            (true, mb) => UtxoStore::open_cached_statistics(utxo_path, mb << 20),
            (false, mb) => UtxoStore::open_cached(utxo_path, mb << 20),
        };
        Some(Arc::new(store.context(
            "open utxo (delete existing dir to reset between different start heights)",
        )?))    } else {
        None
    };

    let progress = ProgressBar::new(total);
    progress.set_style(
        ProgressStyle::with_template("{spinner} [{elapsed}] {pos}/{len} ({per_sec})")
            .unwrap(),
    );

    let pipeline_start = std::time::Instant::now();
    let pipeline_start_res = ResourceSample::now();
    // Shared reorder-buffer watermark, exactly as `pipeline::run_parse` uses
    // it. The bench consumer MUST enforce the same bound as production or the
    // BTreeMap grows without limit: on signet, blocks ≥200k arrive faster than
    // the consumer drains them, and an unbounded buffer held 28k+ parsed
    // blocks (~10 GiB) before the watchdog fired. That is a harness artifact,
    // not a property of the pipeline being measured.
    let reorder_depth = Arc::new(AtomicUsize::new(0));
    // The consumer publishes the height it is waiting for. Workers must never
    // park while holding that height: if they did, nobody would produce the
    // block that lets the consumer drain, and the pipeline would deadlock. So
    // the worker whose `height` equals this is exempt from backpressure.
    let next_height = Arc::new(AtomicU32::new(start));
    let watchdog = Arc::new(crate::bench::OomWatchdog::start(crate::pipeline::max_rss_kb()));
    let consumer_handle = if do_parse_stages {
        let (tx, rx) = crossbeam_channel::bounded::<WorkItem>(tuning.channel_depth);
        let parsed = Arc::clone(&blocks_done);
        let missing = Arc::clone(&missing_utxos);
        let db_lookups_c = Arc::clone(&db_lookups);
        let db_nanos_c = Arc::clone(&db_nanos);
        let pending_hits_c = Arc::clone(&pending_hits);
        let cache_hits_c = Arc::clone(&cache_hits);
        let sink_wall_c = Arc::clone(&sink_wall);
        let utxo_wall_c = Arc::clone(&utxo_wall);
        let utxo_c = utxo.clone();
        // Only allocate an output directory when a sink will actually write
        // into it. `parse`/`drain`/`utxo` never push rows, and creating a
        // throwaway directory for them would litter the perf root on every run.
        let out_dir = if do_sink {
            out_dir_for(cfg)?
        } else {
            PathBuf::new()
        };
        let blocks_per_part = cfg.blocks_per_part;
        let progress_c = progress.clone();
        let do_sink_c = do_sink;
        let do_fees_c = cfg.mode.needs_fee_enrichment();
        let do_utxo_c = do_utxo;
        let sink_cfg = tuning.sink_config();
        let watchdog_c = Arc::clone(&watchdog);
        let reorder_c = Arc::clone(&reorder_depth);
        let next_h_c = Arc::clone(&next_height);
        let consumer = std::thread::spawn(move || -> Result<Vec<u64>> {
            bench_consume(
                rx,
                start,
                end,
                blocks_per_part,
                do_sink_c,
                do_utxo_c,
                do_fees_c,
                sink_cfg,
                &out_dir,
                utxo_c.as_deref(),
                &parsed,
                &missing,
                &db_lookups_c,
                &db_nanos_c,
                &pending_hits_c,
                &cache_hits_c,
                &sink_wall_c,
                &utxo_wall_c,
                &progress_c,
                Some(&reorder_c),
                Some(&next_h_c),
                Some(watchdog_c.as_ref()),
            )
        });

        // Worker pool. `map_init` lets each worker accumulate per-block parse
        // durations in thread-local state and return them as items, so the
        // hot path stays lock-free (no shared sample buffer) while we still
        // get a per-block latency distribution.
        //
        // The file cache is created ONCE and cloned into each worker, exactly
        // as `pipeline::run_parse` does. Creating a fresh cache per worker
        // would give every thread its own 16-file LRU instead of one shared
        // 16-file LRU — i.e. 16x the decoded-file memory and none of the
        // cross-worker eviction contention that production actually has.
        let blocks_dir_w = cfg.blocks_dir.clone();
        let index_w = Arc::clone(&index);
        let parse_wall_w = Arc::clone(&parse_wall);
        let watchdog_w = Arc::clone(&watchdog);
        let reorder_w = Arc::clone(&reorder_depth);
        let next_h_w = Arc::clone(&next_height);
        let reorder_high_w = tuning.reorder_high;
        let file_cache = ThreadFileCache::new(&blocks_dir_w);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(cfg.threads.unwrap_or(0))
            .build()?;
        parse_samples = pool.install(|| {
            heights
                .par_iter()
                .map_init(
                    || {
                        (
                            file_cache.clone(),
                            tx.clone(),
                            Arc::clone(&parse_wall_w),
                            Arc::clone(&watchdog_w),
                            Arc::clone(&reorder_w),
                            Arc::clone(&next_h_w),
                        )
                    },
                    |(cache, tx, parse_wall, watchdog, reorder, next_h),
                     &height|
                     -> anyhow::Result<u64> {
                        // Backpressure, as in `pipeline::run_parse`. The
                        // exemption is load-bearing: a worker holding the
                        // height the consumer is waiting for must always be
                        // allowed to run, or no block can ever unblock the
                        // consumer and the whole pipeline deadlocks.
                        while reorder.load(Ordering::Relaxed) > reorder_high_w
                            && next_h.load(Ordering::Relaxed) != height
                        {
                            if watchdog.is_tripped() {
                                return Err(watchdog.tripped_error());
                            }
                            std::thread::sleep(std::time::Duration::from_micros(50));
                        }
                        let t = std::time::Instant::now();
                        let loc = &index_w.by_height[height as usize];
                        let bytes = cache.block_bytes(loc)?;
                        let mut header_raw = [0u8; 80];
                        header_raw.copy_from_slice(&bytes[..80]);
                        let header = bidx_core::BlockHeader::parse(&header_raw);
                        let hash = bidx_core::dsha256(&header_raw);
                        let block = bidx_parser::parse_block_full(&bytes, height, hash, &header)?;
                        let dt = t.elapsed().as_nanos() as u64;
                        parse_wall.fetch_add(dt, Ordering::Relaxed);
                        // As in `pipeline::run_parse`: a failed send means the
                        // consumer is gone, which is usually the RSS watchdog
                        // having aborted it. Surface the real cause.
                        tx.send(WorkItem::Block { height, block }).map_err(|_| {
                            if watchdog.is_tripped() {
                                watchdog.tripped_error()
                            } else {
                                anyhow::anyhow!("consumer dropped")
                            }
                        })?;
                        Ok(dt)
                    },
                )
                .collect::<anyhow::Result<Vec<u64>>>()
        })?;
        drop(tx);
        Some(consumer)
    } else {
        None
    };

    if let Some(h) = consumer_handle {
        let us = h.join().map_err(|_| anyhow::anyhow!("consumer panicked"))??;
        utxo_samples = us;
    } else {
        utxo_samples = Vec::new();
    }
    let pipeline_wall = pipeline_start.elapsed();
    let pipeline_end_res = ResourceSample::now();

    progress.finish_and_clear();

    let parse_ns = parse_wall.load(Ordering::Relaxed);
    let utxo_ns = utxo_wall.load(Ordering::Relaxed);
    let sink_ns = sink_wall.load(Ordering::Relaxed);
    let blocks = blocks_done.load(Ordering::Relaxed);
    let missing = missing_utxos.load(Ordering::Relaxed);

    // Parse runs on N workers in parallel and is interleaved with the
    // ordered consumer. We therefore report parse as the *sum of per-block
    // parse durations across all workers* (a CPU-effort proxy), and the
    // pipeline total as the wall-clock block. Sequential stages (UTXO apply,
    // sink) run on the consumer thread so their sums are true wall-time.
    stages.push(StageReport {
        name: "parse (Σ worker CPU-ish)",
        wall_secs: Duration::from_nanos(parse_ns).as_secs_f64(),
        cpu_util: 0.0,
        peak_rss_mb: 0.0,
    });
    if do_utxo {
        stages.push(StageReport {
            name: "utxo_apply (sequential)",
            wall_secs: Duration::from_nanos(utxo_ns).as_secs_f64(),
            cpu_util: 0.0,
            peak_rss_mb: 0.0,
        });
    }
    if do_sink {
        stages.push(StageReport {
            name: "parquet_sink",
            wall_secs: Duration::from_nanos(sink_ns).as_secs_f64(),
            cpu_util: 0.0,
            peak_rss_mb: 0.0,
        });
    }

    let pipeline_secs = pipeline_wall.as_secs_f64();
    let blocks_per_sec = blocks as f64 / pipeline_secs.max(1e-6);
    // The header pass is a fixed cost that runs before the pipeline proper.
    // It dominates short ranges (a full-corpus scan regardless of `--start`
    // /`--end`), so the throughput figure that compares cleanly across
    // ranges and modes is the one that excludes it.
    let header_secs = header_m.wall_secs();
    let blocks_per_sec_ex_header = blocks as f64 / (pipeline_secs + header_secs).max(1e-6);

    // Add a pipeline-level wall measurement.
    stages.push(StageReport {
        name: "pipeline_wall_total",
        wall_secs: pipeline_secs,
        cpu_util: jiffies_to_secs(pipeline_end_res.cpu_jiffies().saturating_sub(pipeline_start_res.cpu_jiffies()))
            / pipeline_secs.max(1e-6)
            / cfg.threads.unwrap_or(num_cpus()) as f64,
        // Process high-water mark, not the instantaneous sample: the pipeline
        // stage is exactly where a reorder buffer balloons and drains, so the
        // two-sample max would report a comfortable number while the box OOMs.
        peak_rss_mb: peak_rss_kb() as f64 / 1024.0,
    });

    // Silence unused-variable warnings for the header stats we don't use.
    let _ = stats.files_scanned;

    // A cold UTXO store makes apply_block look free, which is the single most
    // misleading result this tool can produce. Fail loudly instead.
    if do_utxo && missing > 0 && !cfg.allow_missing_utxos {
        anyhow::bail!(
            "trial {trial}: {missing} spent outpoints were missing from the UTXO store — \
             this run measured a cold store and its `utxo_apply` number is not meaningful. \
             Use a fresh `--utxo` dir seeded from genesis, or pass --allow-missing-utxos \
             to accept it."
        );
    }

    let rocksdb = if cfg.rocksdb_stats {
        utxo.as_ref()
            .map(|u| {
                let mut props: Vec<RocksDbProperty> = u
                    .perf_properties()
                    .into_iter()
                    .map(|(name, value)| RocksDbProperty { name, value })
                    .collect();
                // Statistics were enabled by open_statistics, so any stall
                // lines are real. Surface them as their own entry so the
                // report distinguishes "no stalls" from "not measured".
                let stalls = u.stall_report();
                props.push(RocksDbProperty {
                    name: "stall_lines".to_string(),
                    value: if stalls.is_empty() {
                        "none".to_string()
                    } else {
                        stalls.join(" | ")
                    },
                });
                props
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    Ok(TrialReport {
        trial,
        mode: cfg.mode,
        blocks,
        blocks_per_sec,
        blocks_per_sec_ex_header,
        user_cpu_secs: jiffies_to_secs(
            pipeline_end_res.cpu_jiffies().saturating_sub(pipeline_start_res.cpu_jiffies()),
        ),
        peak_rss_mb: peak_rss_kb() as f64 / 1024.0,
        missing_utxos: missing,
        db_lookups: db_lookups.load(Ordering::Relaxed),
        db_nanos: db_nanos.load(Ordering::Relaxed),
        pending_hits: pending_hits.load(Ordering::Relaxed),
        cache_hits: cache_hits.load(Ordering::Relaxed),
        stages,
        parse_latency: LatencyReport::from_samples(parse_samples),
        utxo_latency: LatencyReport::from_samples(utxo_samples),
        rocksdb,
    })
}

fn report_stage(name: &'static str, m: &metrics::StageMeasure, threads: Option<usize>) -> StageReport {
    StageReport {
        name,
        wall_secs: m.wall_secs(),
        cpu_util: m.cpu_utilization(threads.unwrap_or_else(num_cpus) as f64),
        peak_rss_mb: m.peak_rss_kb as f64 / 1024.0,
    }
}

fn jiffies_to_secs(jiffies: u64) -> f64 {
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    jiffies as f64 / hz
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
}

/// Default root for benchmark scratch output: `./bidx-perf` under the current
/// working directory.
///
/// Deliberately NOT `std::env::temp_dir()`. On many machines (including the
/// dev box this was written on) `/tmp` is tmpfs, i.e. RAM-backed, which
/// flatters RocksDB's write path by absorbing writes at memory speed and
/// hides exactly the compaction stalls the benchmark is meant to surface.
/// A CWD-relative directory also keeps results next to the project instead of
/// somewhere the user has to go hunting for them.
///
/// Falls back to `std::env::temp_dir()` only if the CWD cannot be determined,
/// which in practice means the process was started in a deleted directory.
pub(crate) fn default_perf_dir() -> PathBuf {
    match std::env::current_dir() {
        Ok(cwd) => cwd.join("bidx-perf"),
        // CWD is gone (deleted underneath us). Temp is the only sane fallback;
        // the caller still gets a usable path rather than a hard error.
        Err(_) => std::env::temp_dir().join("bidx-perf"),
    }
}

fn out_dir_for(cfg: &BenchConfig) -> Result<PathBuf> {
    // Explicit --out-dir is used as-is and left in place so the Parquet can
    // be inspected with parquet-tools / DuckDB.
    if let Some(d) = &cfg.out_dir {
        std::fs::create_dir_all(d)?;
        return Ok(d.clone());
    }
    // Sink modes: write Parquet to a per-process unique dir under the
    // project-local perf root (see `default_perf_dir` for why not temp).
    let p = default_perf_dir().join(format!(
        "bidx-bench-out-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));
    std::fs::create_dir_all(&p)?;
    Ok(p)
}

#[allow(clippy::too_many_arguments)]
/// Watchdog that trips when RSS grows past `max_rss_kb`.
///
/// Runs on a dedicated thread and only sets a flag; it cannot itself abort the
/// pipeline, because unwinding from an arbitrary thread would leave RocksDB
/// and the Parquet writers in an unknown state. Instead the two ordered
/// consumers (`pipeline::consume_ordered` and `bench::bench_consume`) poll
/// `is_tripped` once per block and return a clean error, which propagates out
/// as a normal failure with a diagnostic.
///
/// This is the user's safety net against one slow early height blowing up the
/// consumer's reorder buffer into GB-sized territory before our backpressure
/// counter kicks in — and against the kernel's OOM killer, which kills the
/// process with no explanation at all.
pub(crate) struct OomWatchdog {
    tripped: Arc<std::sync::atomic::AtomicBool>,
    max_rss_kb: u64,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl OomWatchdog {
    pub(crate) fn start(max_rss_kb: u64) -> Self {
        let tripped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let t2 = Arc::clone(&tripped);
        let handle = std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(200));
            let rss = ResourceSample::now().rss_kb;
            if rss > max_rss_kb {
                // Log at the moment of tripping so the message is present even
                // if the caller is slow to observe the flag.
                tracing::error!(
                    rss_mb = rss / 1024,
                    limit_mb = max_rss_kb / 1024,
                    "watchdog: RSS exceeded limit; aborting pipeline"
                );
                t2.store(true, Ordering::SeqCst);
                return;
            }
        });
        OomWatchdog {
            tripped,
            max_rss_kb,
            handle: Some(handle),
        }
    }

    pub(crate) fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// Error to return once tripped. Includes the limit so the operator knows
    /// which knob to turn (`--threads`, or `MAX_CACHED_FILES`).
    pub(crate) fn tripped_error(&self) -> anyhow::Error {
        anyhow::anyhow!(
            "watchdog: RSS exceeded {} MiB — reduce --threads or the decoded-file LRU \
             (MAX_CACHED_FILES) and re-run",
            self.max_rss_kb / 1024
        )
    }
}

impl Drop for OomWatchdog {
    fn drop(&mut self) {
        // Don't wait if watchdog is still alive — just detach.
        let _ = self.handle.take();
    }
}

/// The ordered consumer.
///
/// Returns the per-block `apply_block` durations in nanoseconds, so the
/// caller can report percentiles. `do_utxo` controls whether the store is
/// consulted; `do_fees` whether fees are written back into the rows (requires
/// `do_utxo`); `do_sink` whether Parquet rows are built. `reorder_depth` is
/// the shared watermark counter the workers spin on; the consumer owns the
/// increment/decrement side of that contract.
#[allow(clippy::too_many_arguments)]
fn bench_consume(
    rx: crossbeam_channel::Receiver<WorkItem>,
    start: u32,
    end: u32,
    blocks_per_part: u32,
    do_sink: bool,
    do_utxo: bool,
    do_fees: bool,
    sink_cfg: SinkConfig,
    out_dir: &Path,
    utxo: Option<&UtxoStore>,
    blocks_done: &AtomicU64,
    missing_utxos: &AtomicU64,
    db_lookups: &AtomicU64,
    db_nanos: &AtomicU64,
    pending_hits: &AtomicU64,
    cache_hits: &AtomicU64,
    sink_wall: &AtomicU64,
    utxo_wall: &AtomicU64,
    progress: &ProgressBar,
    // Shared reorder-buffer depth. The consumer increments it on insert and
    // decrements on drain; workers spin while it exceeds `reorder_high`. This
    // is the same backpressure contract as `pipeline::consume_ordered`, and
    // without it the buffer is unbounded.
    reorder_depth: Option<&AtomicUsize>,
    // Height the consumer is currently waiting for. Workers compare against
    // this to exempt themselves from backpressure (see the spin loop), which
    // is what keeps the pipeline deadlock-free.
    next_height: Option<&AtomicU32>,
    watchdog: Option<&crate::bench::OomWatchdog>,
) -> Result<Vec<u64>> {
    let mut buffer: std::collections::BTreeMap<u32, bidx_parser::FullBlock> =
        std::collections::BTreeMap::new();
    let mut next = start;
    if let Some(n) = next_height {
        n.store(next, Ordering::Relaxed);
    }
    let mut part_id = start / blocks_per_part;
    let mut sink: Option<ParquetSink> = if do_sink {
        Some(ParquetSink::create(out_dir, part_id, sink_cfg)?)
    } else {
        None
    };
    // Latency samples: one entry per block, nanoseconds. Pre-sized to the
    // block range so the consumer never reallocates on the hot path.
    let cap = (end.saturating_sub(start)) as usize;
    let mut utxo_samples: Vec<u64> = Vec::with_capacity(cap);

    while let Ok(item) = rx.recv() {
        match item {
            WorkItem::Block { height, block } => {
                buffer.insert(height, block);
                if let Some(d) = reorder_depth {
                    d.fetch_add(1, Ordering::Relaxed);
                }
            }
            WorkItem::Skipped { height } => {
                anyhow::bail!("bench did not expect a skipped height {height}");
            }
        }
        while let Some(block) = buffer.remove(&next) {
            // Drained one from the reorder buffer — release the watermark so a
            // worker blocked in its spin loop can produce the next block.
            if let Some(d) = reorder_depth {
                d.fetch_sub(1, Ordering::Relaxed);
            }
            // Poll the RSS watchdog once per block (a relaxed atomic load).
            if let Some(w) = watchdog {
                if w.is_tripped() {
                    return Err(w.tripped_error());
                }
            }
            if sink.is_some() {
                let wanted_part = next / blocks_per_part;
                if wanted_part != part_id {
                    let s = sink.take().expect("sink present");
                    s.finish()?;
                    part_id = wanted_part;
                    sink = Some(ParquetSink::create(out_dir, part_id, sink_cfg)?);
                }
            }

            let mut block = block;
            let res = if do_utxo {
                let t0 = std::time::Instant::now();
                let r = utxo
                    .map(|u| u.apply_block(next, &block.txs, &block.inputs, &block.outputs))
                    .transpose()?;
                let dt = t0.elapsed().as_nanos() as u64;
                utxo_wall.fetch_add(dt, Ordering::Relaxed);
                utxo_samples.push(dt);
                r
            } else {
                None
            };

            if let Some(ref res) = res {
                missing_utxos.fetch_add(res.missing, Ordering::Relaxed);
                db_lookups.fetch_add(res.db_lookups, Ordering::Relaxed);
                db_nanos.fetch_add(res.db_nanos, Ordering::Relaxed);
                pending_hits.fetch_add(res.pending_hits, Ordering::Relaxed);
                cache_hits.fetch_add(res.cache_hits, Ordering::Relaxed);
                if do_fees {
                    // Same O(txs x inputs) fix as `pipeline::consume_ordered`:
                    // derive the coinbase flags in one pass over inputs instead
                    // of re-scanning inputs for every tx.
                    let coinbase_tx: Vec<bool> = {
                        let mut flags = vec![false; block.txs.len()];
                        for inp in &block.inputs {
                            if let Some(f) = flags.get_mut(inp.tx_index as usize) {
                                *f |= inp.is_coinbase;
                            }
                        }
                        flags
                    };
                    let mut total_fee = 0u64;
                    for (i, tx) in block.txs.iter_mut().enumerate() {
                        tx.fee = res.fees[i];
                        if !coinbase_tx.get(i).copied().unwrap_or(false) {
                            total_fee = total_fee.saturating_add(res.fees[i]);
                        }
                    }
                    block.block_row.total_fee = total_fee;
                }
            }

            if let Some(sink) = sink.as_mut() {
                let t0 = std::time::Instant::now();
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
                if let Some(res) = &res {
                    for sp in &res.spends {
                        sink.push_spend(sp)?;
                    }
                }
                sink_wall.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }

            blocks_done.fetch_add(1, Ordering::Relaxed);
            progress.inc(1);
            next += 1;
            // Publish the new target so the worker that produces it stops
            // being parked (and any worker parked on the old target resumes
            // once the buffer drains below the watermark).
            if let Some(n) = next_height {
                n.store(next, Ordering::Relaxed);
            }
            if next >= end {
                if let Some(s) = sink.take() {
                    s.finish()?;
                }
                return Ok(utxo_samples);
            }
        }
    }

    if let Some(s) = sink.take() {
        s.finish()?;
    }
    if next < end {
        anyhow::bail!("channel closed early at height {next}");
    }
    Ok(utxo_samples)
}

fn print_trial(r: &TrialReport) {
    println!("\n=== trial {} ({}) ===", r.trial, r.mode);
    println!("blocks: {}", r.blocks);
    println!("blocks/s: {:.1}", r.blocks_per_sec);
    println!(
        "blocks/s incl. header pass: {:.1}",
        r.blocks_per_sec_ex_header
    );
    println!("user cpu: {:.1}s peak rss: {:.1} MB", r.user_cpu_secs, r.peak_rss_mb);
    println!("missing utxos: {}", r.missing_utxos);
    // H2 instrumentation. Read this before considering a batched prefetch:
    // `db_lookups` is the only set a prefetch could cover, and `db_nanos` is
    // the ceiling on what removing it could save. The share is against the
    // apply stage, which is where those lookups live.
    let apply_ns = r
        .stages
        .iter()
        .find(|s| s.name.starts_with("utxo_apply"))
        .map(|s| (s.wall_secs * 1e9) as u64)
        .unwrap_or(0);
    let share = if apply_ns > 0 {
        100.0 * r.db_nanos as f64 / apply_ns as f64
    } else {
        0.0
    };
    println!(
        "utxo lookups: {} db / {} pending ({}% db)  db_time: {:.3}s ({:.1}% of apply)",
        r.db_lookups,
        r.pending_hits,
        if r.db_lookups + r.pending_hits > 0 {
            100.0 * r.db_lookups as f64 / (r.db_lookups + r.pending_hits) as f64
        } else {
            0.0
        },
        r.db_nanos as f64 / 1e9,
        share,
    );
    if r.cache_hits > 0 {
        println!("utxo cache hits: {}", r.cache_hits);
    }
    println!("stages:");
    for s in &r.stages {
        println!(
            "  {:>30}  wall {:>7.3}s  cpu_util {:.0}%  peak_rss {:>8.0} MB",
            s.name,
            s.wall_secs,
            s.cpu_util * 100.0,
            s.peak_rss_mb
        );
    }
    if r.parse_latency.n > 0 {
        println!(
            "per-block parse   p50 {:>7.2}ms  p99 {:>8.2}ms  max {:>8.2}ms  (n={})",
            r.parse_latency.p50_ms, r.parse_latency.p99_ms, r.parse_latency.max_ms, r.parse_latency.n
        );
    }
    if r.utxo_latency.n > 0 {
        println!(
            "per-block apply   p50 {:>7.2}ms  p99 {:>8.2}ms  max {:>8.2}ms  (n={})",
            r.utxo_latency.p50_ms, r.utxo_latency.p99_ms, r.utxo_latency.max_ms, r.utxo_latency.n
        );
    }
    if !r.rocksdb.is_empty() {
        println!("rocksdb:");
        for p in &r.rocksdb {
            println!("  {:>28}  {}", p.name, p.value);
        }
    }
}

fn print_aggregate(trials: &[TrialReport]) {
    let mut by_stage: std::collections::BTreeMap<&'static str, Vec<f64>> =
        std::collections::BTreeMap::new();
    for t in trials {
        for s in &t.stages {
            by_stage.entry(s.name).or_default().push(s.wall_secs);
        }
    }
    println!("\n=== aggregate over {} trials ===", trials.len());
    for (name, mut v) in by_stage {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let (_, med, _) = median(&v);
        println!(
            "  {:>30}  min {:>7.3}s  med {:>7.3}s  max {:>7.3}s",
            name,
            v.first().unwrap(),
            med,
            v.last().unwrap()
        );
    }
}

fn median(v: &[f64]) -> (f64, f64, f64) {
    let mut v = v.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = v.len() / 2;
    let med = if v.len() % 2 == 0 {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    };
    (v[0], med, v[v.len() - 1])
}

/// Test-only alias so the mode->stage mask tests read as a table. The logic
/// lives on `BenchMode::needs_utxo`.
#[cfg(test)]
fn needs_utxo_alias(mode: BenchMode) -> bool {
    mode.needs_utxo()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg(mode: BenchMode) -> BenchConfig {
        BenchConfig {
            blocks_dir: PathBuf::from("/tmp"),
            mode,
            start: 0,
            end: None,
            threads: None,
            utxo: None,
            utxo_dir: None,
            out_dir: None,
            blocks_per_part: 1,
            sink_batch_rows: None,
            reorder_high: None,
            channel_depth: None,
            repeat: 1,
            allow_missing_utxos: false,
            utxo_cache_mb: 0,
            rocksdb_stats: false,
            csv: None,
        }
    }

    #[test]
    fn bench_mode_display_is_lowercase() {
        assert_eq!(BenchMode::Full.to_string(), "full");
        assert_eq!(BenchMode::Headers.to_string(), "headers");
        assert_eq!(BenchMode::Parse.to_string(), "parse");
        assert_eq!(BenchMode::Drain.to_string(), "drain");
        assert_eq!(BenchMode::Utxo.to_string(), "utxo");
        assert_eq!(BenchMode::Sink.to_string(), "sink");
    }

    #[test]
    fn needs_utxo_toggle_by_mode() {
        assert!(needs_utxo_alias(BenchMode::Full));
        assert!(needs_utxo_alias(BenchMode::Utxo));
        assert!(!needs_utxo_alias(BenchMode::Headers));
        assert!(!needs_utxo_alias(BenchMode::Parse));
        assert!(!needs_utxo_alias(BenchMode::Drain));
        assert!(!needs_utxo_alias(BenchMode::Sink));
    }

    #[test]
    fn needs_sink_toggle_by_mode() {
        assert!(BenchMode::Full.needs_sink());
        assert!(BenchMode::Sink.needs_sink());
        assert!(!BenchMode::Headers.needs_sink());
        assert!(!BenchMode::Parse.needs_sink());
        assert!(!BenchMode::Drain.needs_sink());
        assert!(!BenchMode::Utxo.needs_sink());
    }

    #[test]
    fn needs_fee_enrichment_follows_utxo() {
        // Fees come out of the apply result, so any mode that applies UTXO
        // must also enrich; any mode that doesn't must not touch fees.
        for m in [
            BenchMode::Full,
            BenchMode::Headers,
            BenchMode::Parse,
            BenchMode::Drain,
            BenchMode::Utxo,
            BenchMode::Sink,
        ] {
            assert_eq!(m.needs_fee_enrichment(), m.needs_utxo(), "mode {m}");
        }
    }

    #[test]
    fn tuning_defaults_match_production_pipeline() {
        // The bench must measure production defaults unless told otherwise.
        let t = Tuning::from_cfg(&test_cfg(BenchMode::Full));
        assert_eq!(t.reorder_high, crate::pipeline::REORDER_HIGH);
        assert_eq!(t.channel_depth, 512);
        assert_eq!(t.sink_batch_rows, SinkConfig::default().batch_size);
    }

    #[test]
    fn tuning_overrides_are_honoured() {
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.reorder_high = Some(4);
        cfg.channel_depth = Some(64);
        cfg.sink_batch_rows = Some(1_000);
        let t = Tuning::from_cfg(&cfg);
        assert_eq!(t.reorder_high, 4);
        assert_eq!(t.channel_depth, 64);
        assert_eq!(t.sink_config().batch_size, 1_000);
    }

    #[test]
    fn median_picks_center_and_averages_even_sizes() {
        let (lo, med, hi) = median(&[1.0, 2.0, 3.0]);
        assert_eq!((lo, med, hi), (1.0, 2.0, 3.0));
        let (_, med, _) = median(&[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(med, 2.5);
        let (_, med, _) = median(&[5.0]);
        assert_eq!(med, 5.0);
        // Unsorted input must be sorted first.
        let (_, med, _) = median(&[9.0, 1.0, 5.0, 2.0]);
        assert_eq!(med, (2.0 + 5.0) / 2.0);
    }

    #[test]
    fn latency_report_empty_is_all_zero() {
        let r = LatencyReport::from_samples(Vec::new());
        assert_eq!(r.n, 0);
        assert_eq!(r.p50_ms, 0.0);
        assert_eq!(r.p99_ms, 0.0);
        assert_eq!(r.max_ms, 0.0);
        assert_eq!(r.sum_secs, 0.0);
    }

    #[test]
    fn latency_report_percentiles_and_sum() {
        // 100 samples of 1 ms plus one 500 ms outlier: p50 must sit on the
        // bulk, p99 must stay off the outlier, max must catch it.
        let mut samples = vec![1_000_000u64; 100];
        samples.push(500_000_000);
        let r = LatencyReport::from_samples(samples);
        assert_eq!(r.n, 101);
        assert!((r.p50_ms - 1.0).abs() < 1e-9);
        assert!((r.p99_ms - 1.0).abs() < 1e-9, "p99 {} should be 1ms", r.p99_ms);
        assert!((r.max_ms - 500.0).abs() < 1e-9);
        // 100 * 1ms + 500ms = 0.6s
        assert!((r.sum_secs - 0.6).abs() < 1e-6, "sum {}", r.sum_secs);
    }

    #[test]
    fn latency_report_single_sample_is_all_that_sample() {
        let r = LatencyReport::from_samples(vec![2_500_000]);
        assert_eq!(r.n, 1);
        assert!((r.p50_ms - 2.5).abs() < 1e-9);
        assert!((r.p99_ms - 2.5).abs() < 1e-9);
        assert!((r.max_ms - 2.5).abs() < 1e-9);
    }

    #[test]
    fn out_dir_for_creates_unique_dir_under_perf_root() {
        let dir1 = out_dir_for(&test_cfg(BenchMode::Full)).unwrap();
        assert!(dir1.exists());
        // The default root is project-local, not the system temp dir: temp is
        // often tmpfs, which flatters RocksDB and hides write stalls.
        assert!(
            dir1.starts_with(default_perf_dir()),
            "generated dir {dir1:?} not under {:?}",
            default_perf_dir()
        );
        let _ = std::fs::remove_dir_all(&dir1);
    }

    #[test]
    fn default_perf_dir_is_bidx_perf_under_cwd() {
        // The whole point of the change: output lands in `./bidx-perf` in the
        // current working directory, not somewhere in $HOME or /tmp.
        let d = default_perf_dir();
        assert_eq!(d.file_name().unwrap(), "bidx-perf");
        assert_eq!(d.parent().unwrap(), std::env::current_dir().unwrap());
    }

    #[test]
    fn parse_mode_does_not_create_an_out_dir() {
        // Non-sink modes never write Parquet, so they must not litter the perf
        // root with throwaway directories on every run. We prove this by
        // pointing --out-dir at a path that does not exist and asserting it is
        // still absent afterwards.
        let (dir, _) = make_chain(3);
        let never = std::env::temp_dir().join(format!(
            "bidx-bench-nosink-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&never);
        let mut cfg = test_cfg(BenchMode::Parse);
        cfg.blocks_dir = dir.clone();
        cfg.out_dir = Some(never.clone());
        cfg.threads = Some(1);
        run_bench(cfg).unwrap();
        assert!(
            !never.exists(),
            "parse mode created an out dir it never writes to: {never:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn out_dir_for_honours_explicit_out_dir() {
        let explicit = std::env::temp_dir().join(format!("bidx-bench-explicit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&explicit);
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.out_dir = Some(explicit.clone());
        let got = out_dir_for(&cfg).unwrap();
        assert_eq!(got, explicit);
        assert!(got.exists());
        // Must not have been replaced by a generated name.
        assert!(!got
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("bidx-bench-out-"));
        let _ = std::fs::remove_dir_all(&explicit);
    }

    #[test]
    fn out_dir_for_reuses_existing_explicit_dir() {
        let explicit = std::env::temp_dir().join(format!("bidx-bench-reuse-{}", std::process::id()));
        std::fs::create_dir_all(&explicit).unwrap();
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.out_dir = Some(explicit.clone());
        assert_eq!(out_dir_for(&cfg).unwrap(), explicit);
        assert_eq!(out_dir_for(&cfg).unwrap(), explicit);
        let _ = std::fs::remove_dir_all(&explicit);
    }

    #[test]
    fn run_bench_rejects_empty_blocks_dir() {
        let empty = std::env::temp_dir().join(format!("bidx-bench-empty-blocks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&empty);
        std::fs::create_dir_all(&empty).unwrap();
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.blocks_dir = empty.clone();
        let e = run_bench(cfg).unwrap_err();
        assert!(e.to_string().contains("has no block files"), "got {e}");
        let _ = std::fs::remove_dir_all(&empty);
    }

    #[test]
    fn run_bench_rejects_missing_blocks_dir() {
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.blocks_dir = PathBuf::from("/nonexistent/bidx/blocks");
        assert!(run_bench(cfg).is_err());
    }

    #[test]
    fn run_bench_headers_mode_reports_tip_and_stages() {
        let (dir, _) = make_chain(3);
        let mut cfg = test_cfg(BenchMode::Headers);
        cfg.blocks_dir = dir.clone();
        run_bench(cfg).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_headers_mode_empty_chain_reports_error_not_panic() {
        // A blocks dir with a single empty blk file: the header pass finds no
        // records. That must surface as a clean error, never a panic on an
        // `expect("no blocks found")` inside the chain builder.
        let dir = std::env::temp_dir().join(format!("bidx-bench-hdrempty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("blk00000.dat"), b"").unwrap();
        let mut cfg = test_cfg(BenchMode::Headers);
        cfg.blocks_dir = dir.clone();
        let e = run_bench(cfg).unwrap_err();
        assert!(
            format!("{:#}", e).contains("no block records"),
            "expected a NoRecords error, got {e:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Build a synthetic blocks dir with a single blk file containing N
    /// coinbase-only blocks, mirroring the pipeline test helper.
    fn make_chain(n: u32) -> (PathBuf, Vec<bidx_core::Hash32>) {
        let tmp = std::env::temp_dir().join(format!(
            "bidx-bench-chain-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let blk_path = tmp.join("blk00000.dat");

        let mut data = Vec::new();
        let mut prev = bidx_core::Hash32::ZERO;
        let mut hashes = Vec::new();
        for i in 0..n {
            let mut hdr = [0u8; bidx_core::BLOCK_HEADER_LEN];
            hdr[0] = (1 + i) as u8;
            hdr[4..36].copy_from_slice(prev.as_bytes());
            for j in 0..32 {
                hdr[36 + j] = (i as u8).wrapping_add(j as u8);
            }
            hdr[68..72].copy_from_slice(&(1000u32 + i).to_le_bytes());
            hdr[72..76].copy_from_slice(&0x1d00_ffffu32.to_le_bytes());
            hdr[76..80].copy_from_slice(&i.to_le_bytes());

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
            // Non-zero locktime: `BlkFile::open` trims trailing zero padding,
            // so a synthetic record ending in 0 would lose its final bytes and
            // the header pass would then reject it as a bad record length.
            tx.extend_from_slice(&0x1122_3344u32.to_le_bytes());

            let payload_len = (bidx_core::BLOCK_HEADER_LEN + 1 + tx.len()) as u32;
            data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
            data.extend_from_slice(&payload_len.to_le_bytes());
            data.extend_from_slice(&hdr);
            data.push(1u8);
            data.extend_from_slice(&tx);
            prev = bidx_core::dsha256(&hdr);
            hashes.push(prev);
        }
        std::fs::write(&blk_path, &data).unwrap();
        (tmp, hashes)
    }

    /// Synthetic block with `tx_count` distinct non-coinbase-spending
    /// transactions, for exercising the fee-enrichment and missing-UTXO paths.
    fn synth_block(height: u32) -> bidx_parser::FullBlock {
        use bidx_core::types::{BlockRow, InputRow, OutputRow, TxRow};
        let h = [height as u8; 32];
        let txid = [(height as u8).wrapping_mul(7); 32];
        bidx_parser::FullBlock {
            block_row: BlockRow {
                height,
                hash: h,
                prev_hash: [0u8; 32],
                merkle_root: h,
                time: 1_609_459_200 + height,
                bits: 0x1d00_ffff,
                nonce: 0,
                version: 1,
                tx_count: 1,
                size: 100,
                weight: 400,
                total_fee: 0,
                coinbase_value: 5_000_000_000,
            },
            txs: vec![TxRow {
                height,
                tx_index: 0,
                txid,
                version: 1,
                locktime: 0,
                size: 10,
                weight: 40,
                fee: 0,
                input_count: 1,
                output_count: 1,
                has_witness: false,
            }],
            inputs: vec![InputRow {
                height,
                tx_index: 0,
                input_index: 0,
                txid,
                prev_txid: [0u8; 32],
                prev_vout: 0xffff_ffff,
                script_sig_len: 0,
                sequence: 0xffff_ffff,
                witness_items: 0,
                witness_bytes: 0,
                is_coinbase: true,
            }],
            outputs: vec![OutputRow {
                height,
                tx_index: 0,
                output_index: 0,
                txid,
                value_sat: 5_000_000_000,
                script_pubkey_len: 0,
                script_type: 0,
                address_hash: [0u8; 32],
            }],
        }
    }

    /// Drive `bench_consume` directly with synthetic blocks. Returns the
    /// counter atomics for assertions.
    struct ConsumeOutcome {
        blocks_done: u64,
        missing: u64,
        sink_wall: u64,
        utxo_wall: u64,
        utxo_samples: Vec<u64>,
    }

    fn drive_consume(
        blocks: Vec<(u32, bidx_parser::FullBlock)>,
        end: u32,
        blocks_per_part: u32,
        do_sink: bool,
        do_utxo: bool,
        do_fees: bool,
        utxo: Option<&UtxoStore>,
        out_dir: &Path,
    ) -> ConsumeOutcome {
        use crossbeam_channel::bounded;
        let (tx, rx) = bounded::<WorkItem>(4);
        for (h, b) in blocks {
            tx.send(WorkItem::Block { height: h, block: b }).unwrap();
        }
        drop(tx);

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let db_lookups = AtomicU64::new(0);
        let db_nanos = AtomicU64::new(0);
        let pending_hits = AtomicU64::new(0);
        let cache_hits = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);
        let progress = ProgressBar::new(end as u64);
        let us = bench_consume(
            rx,
            0,
            end,
            blocks_per_part,
            do_sink,
            do_utxo,
            do_fees,
            SinkConfig::default(),
            out_dir,
            utxo,
            &blocks_done,
            &missing,
            &db_lookups,
            &db_nanos,
            &pending_hits,
            &cache_hits,
            &sink_wall,
            &utxo_wall,
            &progress,
            None,
            None,
            None,
        )
        .unwrap();
        ConsumeOutcome {
            blocks_done: blocks_done.load(Ordering::Relaxed),
            missing: missing.load(Ordering::Relaxed),
            sink_wall: sink_wall.load(Ordering::Relaxed),
            utxo_wall: utxo_wall.load(Ordering::Relaxed),
            utxo_samples: us,
        }
    }

    fn consume_out_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("bidx-bench-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn bench_consume_thread_counts_blocks_and_rolls_part_files() {
        let out_dir = consume_out_dir("consume");
        let blocks: Vec<(u32, _)> = [0u32, 1].into_iter().map(|h| (h, synth_block(h))).collect();
        // blocks_per_part=1 forces Part rotation after the first block.
        let got = drive_consume(blocks, 2, 1, false, false, false, None, &out_dir);
        assert_eq!(got.blocks_done, 2);
        // No sink ⇒ sink_wall stays 0; no utxo ⇒ utxo_wall stays 0.
        assert_eq!(got.sink_wall, 0);
        assert_eq!(got.utxo_wall, 0);
        assert!(got.utxo_samples.is_empty());
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_tracks_reorder_depth_and_publishes_next_height() {
        // The consumer owns the increment/decrement side of the backpressure
        // contract: if it stops updating these two counters, the workers'
        // spin loop either never engages (unbounded buffer -> OOM) or never
        // releases (deadlock). Assert both explicitly.
        use crossbeam_channel::bounded;
        let out_dir = consume_out_dir("reorder");
        let (tx, rx) = bounded::<WorkItem>(4);
        for h in 0..3u32 {
            tx.send(WorkItem::Block { height: h, block: synth_block(h) }).unwrap();
        }
        drop(tx);

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let db_lookups = AtomicU64::new(0);
        let db_nanos = AtomicU64::new(0);
        let pending_hits = AtomicU64::new(0);
        let cache_hits = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);
        let progress = ProgressBar::new(3);
        let reorder = AtomicUsize::new(0);
        // Deliberately start "waiting" somewhere wrong to prove the consumer
        // publishes its real target.
        let next_h = AtomicU32::new(999);

        bench_consume(
            rx,
            0,
            3,
            1000,
            false,
            false,
            false,
            SinkConfig::default(),
            &out_dir,
            None,
            &blocks_done,
            &missing,
            &db_lookups,
            &db_nanos,
            &pending_hits,
            &cache_hits,
            &sink_wall,
            &utxo_wall,
            &progress,
            Some(&reorder),
            Some(&next_h),
            None,
        )
        .unwrap();

        assert_eq!(blocks_done.load(Ordering::Relaxed), 3);
        // Every inserted block was drained, so the counter must be back at 0.
        assert_eq!(
            reorder.load(Ordering::Relaxed),
            0,
            "reorder depth not decremented to zero"
        );
        // The consumer finished, so it is waiting for height 3 (== end).
        assert_eq!(next_h.load(Ordering::Relaxed), 3);
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_bails_when_channel_closes_early() {
        use crossbeam_channel::bounded;
        let (tx, rx) = bounded::<WorkItem>(4);
        drop(tx); // Close immediately.

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let db_lookups = AtomicU64::new(0);
        let db_nanos = AtomicU64::new(0);
        let pending_hits = AtomicU64::new(0);
        let cache_hits = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);
        let progress = ProgressBar::new(1);
        let out_dir = consume_out_dir("empty");

        let e = bench_consume(
            rx,
            0,
            5,
            1,
            false,
            false,
            false,
            SinkConfig::default(),
            &out_dir,
            None,
            &blocks_done,
            &missing,
            &db_lookups,
            &db_nanos,
            &pending_hits,
            &cache_hits,
            &sink_wall,
            &utxo_wall,
            &progress,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(e.to_string().contains("channel closed early"));
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_rejects_skipped_height() {
        use crossbeam_channel::bounded;
        let (tx, rx) = bounded::<WorkItem>(4);
        tx.send(WorkItem::Skipped { height: 0 }).unwrap();
        drop(tx);

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let db_lookups = AtomicU64::new(0);
        let db_nanos = AtomicU64::new(0);
        let pending_hits = AtomicU64::new(0);
        let cache_hits = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);
        let progress = ProgressBar::new(1);
        let out_dir = consume_out_dir("skipped");

        let e = bench_consume(
            rx,
            0,
            1,
            1,
            false,
            false,
            false,
            SinkConfig::default(),
            &out_dir,
            None,
            &blocks_done,
            &missing,
            &db_lookups,
            &db_nanos,
            &pending_hits,
            &cache_hits,
            &sink_wall,
            &utxo_wall,
            &progress,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(e.to_string().contains("did not expect a skipped height"));
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_utxo_mode_records_samples_and_missing() {
        // A block whose single input spends an outpoint that does not exist
        // must count as missing, and the apply latency must be sampled.
        let dir = std::env::temp_dir().join(format!("bidx-bench-utxodb-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = UtxoStore::open(&dir).unwrap();

        let mut b = synth_block(0);
        b.inputs[0].is_coinbase = false; // now a real spend of a missing outpoint
        let out_dir = consume_out_dir("utxomode");
        let got = drive_consume(vec![(0, b)], 1, 1, false, true, true, Some(&store), &out_dir);
        assert_eq!(got.blocks_done, 1);
        assert_eq!(got.missing, 1);
        assert_eq!(got.utxo_samples.len(), 1);
        assert!(got.utxo_wall > 0);

        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_sink_mode_writes_rows_without_fees() {
        // sink-only: no UTXO, so fees must stay zero even though the row is
        // written. Exercises the do_sink && !do_utxo branch.
        let out_dir = consume_out_dir("sinkmode");
        let got = drive_consume(
            vec![(0, synth_block(0))],
            1,
            1,
            true,
            false,
            false,
            None,
            &out_dir,
        );
        assert_eq!(got.blocks_done, 1);
        assert!(got.sink_wall > 0);
        assert_eq!(got.missing, 0);
        // All five entity files exist for part 0.
        for entity in ["blocks", "transactions", "inputs", "outputs", "spends"] {
            let p = out_dir.join(entity).join("part-00000.parquet");
            assert!(p.exists(), "missing {}", p.display());
        }
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_returns_samples_only_for_enabled_stages() {
        // UTXO on, sink off: utxo samples present, and blocks counted.
        let dir = std::env::temp_dir().join(format!("bidx-bench-samples-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = UtxoStore::open(&dir).unwrap();
        let out_dir = consume_out_dir("samples");

        let blocks: Vec<(u32, _)> = [0u32, 1, 2].into_iter().map(|h| (h, synth_block(h))).collect();
        let got = drive_consume(blocks, 3, 10, false, true, true, Some(&store), &out_dir);
        assert_eq!(got.blocks_done, 3);
        assert_eq!(got.utxo_samples.len(), 3);

        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn run_bench_full_mode_writes_parquet_and_fails_on_missing_utxo() {
        // Coinbase-only blocks: every input is coinbase, so apply_block
        // reports missing == 0 and the trial must succeed.
        let (dir, _) = make_chain(3);
        let utxo = std::env::temp_dir().join(format!("bidx-bench-full-utxo-{}", std::process::id()));
        let out = std::env::temp_dir().join(format!("bidx-bench-full-out-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&utxo);
        let _ = std::fs::remove_dir_all(&out);
        std::fs::create_dir_all(&utxo).unwrap();
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.blocks_dir = dir.clone();
        cfg.utxo = Some(utxo.clone());
        cfg.out_dir = Some(out.clone());
        cfg.threads = Some(2);
        run_bench(cfg).unwrap();
        assert!(out.join("blocks").join("part-00000.parquet").exists());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&utxo);
        let _ = std::fs::remove_dir_all(&out);
    }

    #[test]
    fn run_bench_parse_mode_skips_utxo_and_sink() {
        let (dir, _) = make_chain(4);
        let mut cfg = test_cfg(BenchMode::Parse);
        cfg.blocks_dir = dir.clone();
        cfg.threads = Some(2);
        run_bench(cfg).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_drain_mode_parses_and_discards() {
        // Drain must run the parse stages AND spawn a consumer that discards
        // results; without the consumer nothing would be received and the run
        // would report zero blocks.
        let (dir, _) = make_chain(4);
        let report = std::env::temp_dir().join(format!("bidx-bench-drain-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&report);
        let mut cfg = test_cfg(BenchMode::Drain);
        cfg.blocks_dir = dir.clone();
        cfg.threads = Some(2);
        cfg.csv = Some(report.clone());
        run_bench(cfg).unwrap();
        let body = std::fs::read_to_string(&report).unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v[0]["blocks"], 4, "drain must count every block: {body}");
        assert_eq!(v[0]["missing_utxos"], 0);
        // No UTXO store, so no apply samples.
        assert_eq!(v[0]["utxo_latency"]["n"], 0);
        let _ = std::fs::remove_file(&report);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_utxo_mode_without_out_dir_uses_temp() {
        let (dir, _) = make_chain(3);
        let utxo = std::env::temp_dir().join(format!("bidx-bench-utxo-only-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        let mut cfg = test_cfg(BenchMode::Utxo);
        cfg.blocks_dir = dir.clone();
        cfg.utxo = Some(utxo.clone());
        cfg.threads = Some(2);
        run_bench(cfg).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&utxo);
    }

    #[test]
    fn run_bench_rejects_empty_range() {
        let (dir, _) = make_chain(3);
        let mut cfg = test_cfg(BenchMode::Parse);
        cfg.blocks_dir = dir.clone();
        // start clamps to tip=2, end=0 stays → start(2) >= end(0).
        cfg.start = 10;
        cfg.end = Some(0);
        let e = run_bench(cfg).unwrap_err();
        assert!(e.to_string().contains("empty range"), "got {e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_repeat_aggregates_without_panicking() {
        let (dir, _) = make_chain(3);
        let mut cfg = test_cfg(BenchMode::Parse);
        cfg.blocks_dir = dir.clone();
        cfg.threads = Some(2);
        cfg.repeat = 2;
        run_bench(cfg).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn watchdog_trips_when_limit_is_below_current_rss() {
        // A limit of 1 KiB is certainly below this process's RSS, so the
        // watchdog must observe it on its next poll.
        let w = OomWatchdog::start(1);
        assert!(!w.is_tripped(), "must not be tripped before the poll runs");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !w.is_tripped() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(w.is_tripped(), "watchdog never tripped on a 1 KiB limit");
        let msg = w.tripped_error().to_string();
        assert!(msg.contains("watchdog"), "got {msg}");
        assert!(msg.contains("--threads"), "error must name the remedy: {msg}");
    }

    #[test]
    fn watchdog_stays_quiet_under_a_generous_limit() {
        // u64::MAX KiB is unreachable, so it must never trip.
        let w = OomWatchdog::start(u64::MAX);
        std::thread::sleep(std::time::Duration::from_millis(400));
        assert!(!w.is_tripped());
    }

    #[test]
    fn watchdog_drop_does_not_block() {
        // Drop must detach rather than join, so a live watchdog thread cannot
        // stall shutdown. If it joined, this test would block for ~200ms+.
        let w = OomWatchdog::start(u64::MAX);
        let t0 = std::time::Instant::now();
        drop(w);
        assert!(
            t0.elapsed() < std::time::Duration::from_millis(100),
            "drop blocked for {:?}",
            t0.elapsed()
        );
    }

    #[test]
    fn consume_ordered_aborts_when_watchdog_is_tripped() {
        // End-to-end: a pre-tripped watchdog must make the consumer return an
        // error rather than processing the whole range. We drive it with no
        // blocks at all, so the only way it can fail is via the watchdog poll
        // on the first drained block — hence we send one block.
        let out_dir = consume_out_dir("watchdog");
        let dir = std::env::temp_dir().join(format!("bidx-bench-wd-utxo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = UtxoStore::open(&dir).unwrap();

        let (tx, rx) = crossbeam_channel::bounded::<WorkItem>(4);
        tx.send(WorkItem::Block { height: 0, block: synth_block(0) }).unwrap();
        drop(tx);

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let db_lookups = AtomicU64::new(0);
        let db_nanos = AtomicU64::new(0);
        let pending_hits = AtomicU64::new(0);
        let cache_hits = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);
        let progress = ProgressBar::new(1);
        // Limit of 1 KiB guarantees it trips on its first poll.
        let watchdog = OomWatchdog::start(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !watchdog.is_tripped() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(watchdog.is_tripped());

        let e = bench_consume(
            rx,
            0,
            1,
            1,
            false,
            true,
            true,
            SinkConfig::default(),
            &out_dir,
            Some(&store),
            &blocks_done,
            &missing,
            &db_lookups,
            &db_nanos,
            &pending_hits,
            &cache_hits,
            &sink_wall,
            &utxo_wall,
            &progress,
            None,
            None,
            Some(&watchdog),
        )
        .unwrap_err();
        assert!(e.to_string().contains("watchdog"), "got {e}");
        assert_eq!(blocks_done.load(Ordering::Relaxed), 0, "must abort before applying");

        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn max_rss_kb_is_positive_and_below_total_memory() {
        // The ceiling must be usable (not zero) and must leave headroom under
        // MemTotal, since exceeding it is what gets the process OOM-killed.
        let ceiling = crate::pipeline::max_rss_kb();
        assert!(ceiling > 0);
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            let total_kb = meminfo
                .lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|n| n.parse::<u64>().ok());
            if let Some(total) = total_kb {
                assert!(
                    ceiling < total,
                    "ceiling {ceiling} KiB must be below MemTotal {total} KiB"
                );
            }
        }
    }

    #[test]
    fn run_bench_writes_json_report_and_stdout_variant() {
        let (dir, _) = make_chain(3);
        let report = std::env::temp_dir().join(format!("bidx-bench-report-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&report);
        let mut cfg = test_cfg(BenchMode::Headers);
        cfg.blocks_dir = dir.clone();
        cfg.csv = Some(report.clone());
        run_bench(cfg).unwrap();
        let body = std::fs::read_to_string(&report).unwrap();
        assert!(body.contains("\"mode\": \"headers\""), "got {body}");
        assert!(body.contains("\"missing_utxos\""));
        assert!(body.contains("\"blocks_per_sec_ex_header\""));

        // The "-" variant prints to stdout and writes no file.
        let mut cfg = test_cfg(BenchMode::Headers);
        cfg.blocks_dir = dir.clone();
        cfg.csv = Some(PathBuf::from("-"));
        run_bench(cfg).unwrap();

        let _ = std::fs::remove_file(&report);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_utxo_dir_override_controls_throwaway_location() {
        // With --utxo-dir set and no explicit --utxo, the throwaway store is
        // created under that dir and removed afterwards.
        let (dir, _) = make_chain(2);
        let root = std::env::temp_dir().join(format!("bidx-bench-utxoroot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let mut cfg = test_cfg(BenchMode::Utxo);
        cfg.blocks_dir = dir.clone();
        cfg.utxo_dir = Some(root.clone());
        cfg.threads = Some(1);
        run_bench(cfg).unwrap();
        // The generated subdir must be gone; the root itself remains.
        let leftovers: Vec<_> = std::fs::read_dir(&root).unwrap().flatten().collect();
        assert!(leftovers.is_empty(), "expected cleanup, found {:?}", leftovers);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_rocksdb_stats_flag_populates_properties() {
        let (dir, _) = make_chain(2);
        let utxo = std::env::temp_dir().join(format!("bidx-bench-rdbstats-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        let report = std::env::temp_dir().join(format!("bidx-bench-rdbstats-{}.json", std::process::id()));
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.blocks_dir = dir.clone();
        cfg.utxo = Some(utxo.clone());
        cfg.threads = Some(1);
        cfg.rocksdb_stats = true;
        cfg.csv = Some(report.clone());
        run_bench(cfg).unwrap();
        let body = std::fs::read_to_string(&report).unwrap();
        assert!(body.contains("stall_lines"), "got {body}");
        assert!(body.contains("cur_size_all_mem_tables"));
        assert!(body.contains("block_cache_capacity"));
        // With statistics enabled the stall section must be a real reading,
        // not the "not measured" placeholder.
        assert!(!body.contains("stall_lines\": \"none"), "got {body}");
        let _ = std::fs::remove_file(&report);
        let _ = std::fs::remove_dir_all(&utxo);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_bench_backpressure_and_batch_overrides_are_accepted() {
        let (dir, _) = make_chain(3);
        let utxo = std::env::temp_dir().join(format!("bidx-bench-tune-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&utxo);
        std::fs::create_dir_all(&utxo).unwrap();
        let mut cfg = test_cfg(BenchMode::Full);
        cfg.blocks_dir = dir.clone();
        cfg.utxo = Some(utxo.clone());
        cfg.threads = Some(2);
        cfg.reorder_high = Some(2);
        cfg.channel_depth = Some(16);
        cfg.sink_batch_rows = Some(1);
        run_bench(cfg).unwrap();
        let _ = std::fs::remove_dir_all(&utxo);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn print_trial_and_aggregate_do_not_panic() {
        // Build a two-trial aggregate with two stages, exercise the formatter.
        let t0 = TrialReport {
            trial: 0,
            mode: BenchMode::Full,
            blocks: 100,
            blocks_per_sec: 5.0,
            blocks_per_sec_ex_header: 4.5,
            user_cpu_secs: 1.5,
            peak_rss_mb: 128.0,
            missing_utxos: 0,
            db_lookups: 0,
            db_nanos: 0,
            pending_hits: 0,
            cache_hits: 0,
            stages: vec![
                StageReport { name: "header_pass", wall_secs: 0.5, cpu_util: 0.9, peak_rss_mb: 64.0 },
                StageReport { name: "pipeline_wall_total", wall_secs: 20.0, cpu_util: 0.8, peak_rss_mb: 128.0 },
            ],
            parse_latency: LatencyReport::from_samples(vec![1_000_000, 2_000_000]),
            utxo_latency: LatencyReport::from_samples(vec![3_000_000]),
            rocksdb: vec![RocksDbProperty { name: "stall_micros".into(), value: "0".into() }],
        };
        let mut t1 = t0.clone();
        t1.trial = 1;
        // Just call them; they print to stdout. We assert here that nothing
        // panics — printing correctness is verified visually in CI logs.
        print_trial(&t0);
        print_aggregate(&[t0.clone(), t1]);
        let _ = t0; // silence unused
    }

    #[test]
    fn print_trial_handles_report_with_no_latency_or_rocksdb() {
        // Headers-mode reports have empty latency and rocksdb vectors; the
        // formatter must skip those sections rather than print zeros.
        let t = TrialReport {
            trial: 0,
            mode: BenchMode::Headers,
            blocks: 10,
            blocks_per_sec: 1.0,
            blocks_per_sec_ex_header: 0.9,
            user_cpu_secs: 0.1,
            peak_rss_mb: 8.0,
            missing_utxos: 0,
            db_lookups: 0,
            db_nanos: 0,
            pending_hits: 0,
            cache_hits: 0,
            stages: vec![StageReport {
                name: "header_pass",
                wall_secs: 1.0,
                cpu_util: 0.5,
                peak_rss_mb: 8.0,
            }],
            parse_latency: LatencyReport::default(),
            utxo_latency: LatencyReport::default(),
            rocksdb: Vec::new(),
        };
        print_trial(&t);
    }
}
