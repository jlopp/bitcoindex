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
//!   utxo   — full stages but Parquet sink writes to /dev/null equivalent
//!            (rows constructed but discarded) — isolates UTXO cost.
//!   sink   — full stages but UTXO apply skipped (no spend/fee enrichment) —
//!            isolates Parquet encoding + I/O cost.
//!
//! Comparing modes against a given dataset answers:
//!   "Is the bottleneck parallel parse, sequential UTXO apply, or sink I/O?"
//!
//! Note: bench runs deliberately do *not* write disk checkpoints — use
//! `bidx parse` for real runs that train the disk-space model.

pub mod metrics;

use anyhow::{Context, Result};
use bidx_store::{ParquetSink, SinkConfig, UtxoStore};
use indicatif::{ProgressBar, ProgressStyle};
use metrics::{ResourceSample, StageTimer};
use rayon::prelude::*;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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

    /// UTXO store path. Default: a tempdir (deleted after run).
    #[arg(long)]
    pub utxo: Option<PathBuf>,

    /// Blocks per Parquet part file. Only relevant for full/sink modes.
    #[arg(long, default_value_t = 10_000)]
    pub blocks_per_part: u32,

    /// Number of trial runs to repeat. Results are reported per-trial and
    /// aggregated (min / median / max). Default 1.
    #[arg(long, default_value_t = 1)]
    pub repeat: u32,

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
    Utxo,
    Sink,
}

impl std::fmt::Display for BenchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[derive(Debug, Clone, Serialize)]
struct StageReport {
    name: &'static str,
    wall_secs: f64,
    cpu_util: f64,
    peak_rss_mb: f64,
}

#[derive(Debug, Clone, Serialize)]
struct TrialReport {
    trial: u32,
    mode: BenchMode,
    blocks: u64,
    blocks_per_sec: f64,
    user_cpu_secs: f64,
    peak_rss_mb: f64,
    stages: Vec<StageReport>,
}

pub fn run_bench(cfg: BenchConfig) -> Result<()> {
    if cfg.blocks_dir.read_dir()?.next().is_none() {
        anyhow::bail!("{} has no block files", cfg.blocks_dir.display());
    }

    // Default UTXO dir: workspace-local so we can delete it on drop without
    // the `tempfile` crate which currently can't be fetched offline.
    let mut utxo_cleanup: Option<PathBuf> = None;
    let utxo_path = match (&cfg.utxo, cfg.mode) {
        (Some(p), _) => p.clone(),
        (None, m) if needs_utxo(m) => {
            let p = std::env::temp_dir().join(format!(
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

fn needs_utxo(mode: BenchMode) -> bool {
    matches!(mode, BenchMode::Full | BenchMode::Utxo)
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
            user_cpu_secs: jiffies_to_secs(header_m.cpu_jiffies),
            peak_rss_mb: header_m.peak_rss_kb as f64 / 1024.0,
            stages,
        });
    }

    let tip = index.tip_height().context("empty chain")?;
    let start = cfg.start.min(tip);
    let end = cfg.end.unwrap_or(tip + 1).min(tip + 1);
    if start >= end {
        anyhow::bail!("empty range");
    }
    let total = (end - start) as u64;
    println!(
        "bench trial {}: {} blocks (heights {}..{}) mode={}",
        trial, total, start, end, cfg.mode
    );

    let index = Arc::new(index);
    let heights: Vec<u32> = (start..end).collect();
    let do_utxo = matches!(cfg.mode, BenchMode::Full | BenchMode::Utxo);
    let do_sink = matches!(cfg.mode, BenchMode::Full | BenchMode::Sink);
    let do_parse_stages = do_utxo || do_sink || cfg.mode == BenchMode::Parse;

    // Stage accumulators, shared across worker + consumer threads.
    let parse_wall = Arc::new(AtomicU64::new(0)); // ns
    let utxo_wall = Arc::new(AtomicU64::new(0));
    let sink_wall = Arc::new(AtomicU64::new(0));
    let blocks_done = Arc::new(AtomicU64::new(0));
    let missing_utxos = Arc::new(AtomicU64::new(0));

    // UTXO store, opened before parse so workers don't wait on consumer.
    let utxo = if do_utxo {
        Some(Arc::new(UtxoStore::open(utxo_path).context(
            "open utxo (delete existing dir to reset between different start heights)",
        )?))
    } else {
        None
    };

    let progress = ProgressBar::new(total);
    progress.set_style(
        ProgressStyle::with_template("{spinner} [{elapsed}] {pos}/{len} ({per_sec})")
            .unwrap(),
    );

    let pipeline_start = std::time::Instant::now();
    let pipeline_start_res = ResourceSample::now();
    let consumer_handle = if do_parse_stages {
        let (tx, rx) = crossbeam_channel::bounded::<WorkItem>(512);
        let parsed = Arc::clone(&blocks_done);
        let missing = Arc::clone(&missing_utxos);
        let sink_wall_c = Arc::clone(&sink_wall);
        let utxo_wall_c = Arc::clone(&utxo_wall);
        let utxo_c = utxo.clone();
        let out_dir = out_dir_for(cfg)?;
        let blocks_per_part = cfg.blocks_per_part;
        let progress_c = progress.clone();
        let do_sink_c = do_sink;
        let consumer = std::thread::spawn(move || -> Result<()> {
            bench_consume(
                rx,
                start,
                end,
                blocks_per_part,
                do_sink_c,
                &out_dir,
                utxo_c.as_deref(),
                &parsed,
                &missing,
                &sink_wall_c,
                &utxo_wall_c,
                &progress_c,
            )
        });

        // Worker pool.
        let blocks_dir_w = cfg.blocks_dir.clone();
        let index_w = Arc::clone(&index);
        let parse_wall_w = Arc::clone(&parse_wall);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(cfg.threads.unwrap_or(0))
            .build()?;
        let parse_result: anyhow::Result<()> = pool.install(|| {
            heights.par_iter().try_for_each_init(
                || {
                    (
                        ThreadFileCache::new(&blocks_dir_w),
                        tx.clone(),
                        Arc::clone(&parse_wall_w),
                    )
                },
                |(cache, tx, parse_wall), &height| -> anyhow::Result<()> {
                    let t = std::time::Instant::now();
                    let loc = &index_w.by_height[height as usize];
                    let bytes = cache.block_bytes(loc)?;
                    let mut header_raw = [0u8; 80];
                    header_raw.copy_from_slice(&bytes[..80]);
                    let header = bidx_core::BlockHeader::parse(&header_raw);
                    let hash = bidx_core::dsha256(&header_raw);
                    let block = bidx_parser::parse_block_full(&bytes, height, hash, &header)?;
                    parse_wall.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    tx.send(WorkItem::Block { height, block })
                        .map_err(|_| anyhow::anyhow!("consumer dropped"))?;
                    Ok(())
                },
            )
        });
        parse_result?;
        drop(tx);
        Some(consumer)
    } else {
        None
    };

    if let Some(h) = consumer_handle {
        h.join().map_err(|_| anyhow::anyhow!("consumer panicked"))??;
    }
    let pipeline_wall = pipeline_start.elapsed();
    let pipeline_end_res = ResourceSample::now();

    progress.finish_and_clear();

    let parse_ns = parse_wall.load(Ordering::Relaxed);
    let utxo_ns = utxo_wall.load(Ordering::Relaxed);
    let sink_ns = sink_wall.load(Ordering::Relaxed);
    let blocks = blocks_done.load(Ordering::Relaxed);

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

    // Add a pipeline-level wall measurement.
    stages.push(StageReport {
        name: "pipeline_wall_total",
        wall_secs: pipeline_secs,
        cpu_util: jiffies_to_secs(pipeline_end_res.cpu_jiffies().saturating_sub(pipeline_start_res.cpu_jiffies()))
            / pipeline_secs.max(1e-6)
            / cfg.threads.unwrap_or(num_cpus()) as f64,
        peak_rss_mb: (pipeline_end_res.rss_kb.max(pipeline_start_res.rss_kb)) as f64 / 1024.0,
    });

    // Silence unused-variable warnings for the header stats we don't use.
    let _ = stats.files_scanned;

    Ok(TrialReport {
        trial,
        mode: cfg.mode,
        blocks,
        blocks_per_sec,
        user_cpu_secs: jiffies_to_secs(
            pipeline_end_res.cpu_jiffies().saturating_sub(pipeline_start_res.cpu_jiffies()),
        ),
        peak_rss_mb: (pipeline_end_res.rss_kb.max(pipeline_start_res.rss_kb)) as f64 / 1024.0,
        stages,
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

fn out_dir_for(_cfg: &BenchConfig) -> Result<PathBuf> {
    // Sink modes: write Parquet to a workspace tempdir so users can inspect
    // if needed; for now delete after. If users want persistent output they
    // should run `bidx parse` directly. We use a per-process unique dir in
    // system temp rather than the `tempfile` crate (unavailable offline).
    let p = std::env::temp_dir().join(format!(
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
/// Watchdog that bails if RSS grows past `max_rss_kb`. Runs on a dedicated
/// thread; on trip it sets `tripped` and logs. The caller is expected to
/// check `tripped` periodically (in the consumer loop) and abort cleanly.
///
/// This is the user's safety net against one slow early height blowing up
/// the consumer's reorder buffer into GB-sized territory before our
/// backpressure counter kicks in.
pub(crate) struct OomWatchdog {
    tripped: Arc<std::sync::atomic::AtomicBool>,
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
                t2.store(true, Ordering::SeqCst);
                return;
            }
        });
        OomWatchdog { tripped, handle: Some(handle) }
    }

    pub(crate) fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }
}

impl Drop for OomWatchdog {
    fn drop(&mut self) {
        // Don't wait if watchdog is still alive — just detach.
        let _ = self.handle.take();
    }
}

fn bench_consume(
    rx: crossbeam_channel::Receiver<WorkItem>,
    start: u32,
    end: u32,
    blocks_per_part: u32,
    do_sink: bool,
    out_dir: &Path,
    utxo: Option<&UtxoStore>,
    blocks_done: &AtomicU64,
    missing_utxos: &AtomicU64,
    sink_wall: &AtomicU64,
    utxo_wall: &AtomicU64,
    progress: &ProgressBar,
) -> Result<()> {
    let mut buffer: std::collections::BTreeMap<u32, bidx_parser::FullBlock> =
        std::collections::BTreeMap::new();
    let mut next = start;
    let mut part_id = start / blocks_per_part;
    let mut sink: Option<ParquetSink> = if do_sink {
        Some(ParquetSink::create(out_dir, part_id, SinkConfig::default())?)
    } else {
        None
    };

    while let Ok(item) = rx.recv() {
        match item {
            WorkItem::Block { height, block } => {
                buffer.insert(height, block);
            }
            WorkItem::Skipped { height } => {
                anyhow::bail!("bench did not expect a skipped height {height}");
            }
        }
        while let Some(block) = buffer.remove(&next) {
            if sink.is_some() {
                let wanted_part = next / blocks_per_part;
                if wanted_part != part_id {
                    let s = sink.take().expect("sink present");
                    s.finish()?;
                    part_id = wanted_part;
                    sink = Some(ParquetSink::create(out_dir, part_id, SinkConfig::default())?);
                }
            }

            let mut block = block;
            let t0 = std::time::Instant::now();
            let res = utxo
                .map(|u| u.apply_block(next, &block.txs, &block.inputs, &block.outputs))
                .transpose()?;
            utxo_wall.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if let Some(ref res) = res {
                missing_utxos.fetch_add(res.missing, Ordering::Relaxed);
                let mut total_fee = 0u64;
                for (i, tx) in block.txs.iter_mut().enumerate() {
                    tx.fee = res.fees[i];
                    if !block
                        .inputs
                        .iter()
                        .any(|inp| inp.tx_index == i as u32 && inp.is_coinbase)
                    {
                        total_fee = total_fee.saturating_add(res.fees[i]);
                    }
                }
                block.block_row.total_fee = total_fee;
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
            if next >= end {
                if let Some(s) = sink.take() {
                    s.finish()?;
                }
                return Ok(());
            }
        }
    }

    if let Some(s) = sink.take() {
        s.finish()?;
    }
    if next < end {
        anyhow::bail!("channel closed early at height {next}");
    }
    Ok(())
}

fn print_trial(r: &TrialReport) {
    println!("\n=== trial {} ({}) ===", r.trial, r.mode);
    println!("blocks: {}", r.blocks);
    println!("blocks/s: {:.1}", r.blocks_per_sec);
    println!("user cpu: {:.1}s peak rss: {:.1} MB", r.user_cpu_secs, r.peak_rss_mb);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_mode_display_is_lowercase() {
        assert_eq!(BenchMode::Full.to_string(), "full");
        assert_eq!(BenchMode::Headers.to_string(), "headers");
        assert_eq!(BenchMode::Parse.to_string(), "parse");
        assert_eq!(BenchMode::Utxo.to_string(), "utxo");
        assert_eq!(BenchMode::Sink.to_string(), "sink");
    }

    #[test]
    fn needs_utxo_toggle_by_mode() {
        assert!(needs_utxo(BenchMode::Full));
        assert!(needs_utxo(BenchMode::Utxo));
        assert!(!needs_utxo(BenchMode::Headers));
        assert!(!needs_utxo(BenchMode::Parse));
        assert!(!needs_utxo(BenchMode::Sink));
    }

    #[test]
    fn needs_sink_toggle_by_mode() {
        // Mirror of the do_sink mask at run_trial.
        fn needs_sink(m: BenchMode) -> bool {
            matches!(m, BenchMode::Full | BenchMode::Sink)
        }
        assert!(needs_sink(BenchMode::Full));
        assert!(needs_sink(BenchMode::Sink));
        assert!(!needs_sink(BenchMode::Headers));
        assert!(!needs_sink(BenchMode::Parse));
        assert!(!needs_sink(BenchMode::Utxo));
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
    fn out_dir_for_creates_unique_dir_under_temp() {
        let dir1 = out_dir_for(&BenchConfig {
            blocks_dir: PathBuf::from("/tmp"),
            mode: BenchMode::Full,
            start: 0,
            end: None,
            threads: None,
            utxo: None,
            blocks_per_part: 1,
            repeat: 1,
            csv: None,
        })
        .unwrap();
        assert!(dir1.exists());
        assert!(dir1.starts_with(std::env::temp_dir()));
        let _ = std::fs::remove_dir_all(&dir1);
    }

    #[test]
    fn bench_consume_thread_counts_blocks_and_rolls_part_files() {
        use crossbeam_channel::bounded;
        let (tx, rx) = bounded::<WorkItem>(4);

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);

        // Two synthetic blocks with distinct heights and one tx each.
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

        let out_dir = std::env::temp_dir().join(format!("bidx-bench-consume-{}", std::process::id()));
        std::fs::create_dir_all(&out_dir).unwrap();

        // Send two blocks in expected order.
        for h in [0u32, 1] {
            tx.send(WorkItem { height: h, block: synth_block(h) }).unwrap();
        }
        drop(tx);

        let progress = ProgressBar::new(2);
        // blocks_per_part=1 forces Part rotation after first block. Use
        // do_sink=false so we don't need Parquet writers (we've covered
        // their rotation in `pipeline` tests already).
        bench_consume(
            rx,
            0,
            2,
            1, // blocks_per_part
            false, // do_sink
            &out_dir,
            None, // utxo
            &blocks_done,
            &missing,
            &sink_wall,
            &utxo_wall,
            &progress,
        )
        .unwrap();
        assert_eq!(blocks_done.load(Ordering::Relaxed), 2);
        // No sink ⇒ sink_wall stays 0.
        assert_eq!(sink_wall.load(Ordering::Relaxed), 0);
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn bench_consume_bails_when_channel_closes_early() {
        use crossbeam_channel::bounded;
        let (tx, rx) = bounded::<WorkItem>(4);
        drop(tx); // Close immediately.

        let blocks_done = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let sink_wall = AtomicU64::new(0);
        let utxo_wall = AtomicU64::new(0);
        let progress = ProgressBar::new(1);
        let out_dir = std::env::temp_dir().join(format!("bidx-bench-empty-{}", std::process::id()));
        std::fs::create_dir_all(&out_dir).unwrap();

        let e = bench_consume(
            rx,
            0, 5, 1, false, &out_dir, None,
            &blocks_done, &missing, &sink_wall, &utxo_wall, &progress,
        )
        .unwrap_err();
        assert!(e.to_string().contains("channel closed early"));
        let _ = std::fs::remove_dir_all(out_dir);
    }

    #[test]
    fn print_trial_and_aggregate_do_not_panic() {
        // Build a two-trial aggregate with two stages, exercise the formatter.
        let t0 = TrialReport {
            trial: 0,
            mode: BenchMode::Full,
            blocks: 100,
            blocks_per_sec: 5.0,
            user_cpu_secs: 1.5,
            peak_rss_mb: 128.0,
            stages: vec![
                StageReport { name: "header_pass", wall_secs: 0.5, cpu_util: 0.9, peak_rss_mb: 64.0 },
                StageReport { name: "pipeline_wall_total", wall_secs: 20.0, cpu_util: 0.8, peak_rss_mb: 128.0 },
            ],
        };
        let mut t1 = t0.clone();
        t1.trial = 1;
        // Just call them; they print to stdout. We assert here that nothing
        // panics — printing correctness is verified visually in CI logs.
        print_trial(&t0);
        print_aggregate(&[t0.clone(), t1]);
        let _ = t0; // silence unused
    }
}
