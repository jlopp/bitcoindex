//! Disk-space tracking + startup guard.
//!
//! Two responsibilities, both driven from the same handle:
//!
//! 1. **Recorder** (`DiskRecorder`): while we index, every 100,000-height
//!    boundary we flush RocksDB, sum the UTXO db dir size plus all Parquet
//!    out dirs, and append `(height, total_bytes)` under the current network
//!    into `<checkpoint_file>`.
//!
//! 2. **Startup guard** (`DiskGuardConfig::run_pre_flight`): before doing
//!    any indexing work we
//!       - read the local indexed tip from the checkpoint file,
//!       - ask the node for its current tip height (RPC `getblockcount`),
//!       - project the index's final size by fitting a linear regression
//!         through the recorded checkpoints,
//!       - compare that projected growth against the free bytes on the
//!         filesystem holding the index. If we'd end up with <10 GiB free,
//!         exit with a message saying how many more GiB are needed.
//!
//! If the network has no checkpoints yet, we can't estimate growth — we
//! print a warning and proceed. The first `--start 0` mainnet run on a new
//! disk will therefore proceed even on too-small disks; the checkpoints
//! written during that run prevent that from repeating on subsequent runs.

use anyhow::{Context, Result};
use bidx_core::{
    append_checkpoint, dir_size_bytes, free_space_bytes, projected_total_bytes,
    read_checkpoints, DiskCheckpoint, Network,
};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::info;

/// Default location of the checkpoint history file (relative to the UTXO
/// store). Subdirectories per network are not used — sections within one
/// file are.
pub const DEFAULT_CHECKPOINT_FILE_NAME: &str = "bidx-disk-checkpoints.txt";

#[derive(Debug, Clone)]
pub struct DiskGuardConfig {
    /// Where to read/write the checkpoint history.
    pub checkpoint_file: PathBuf,
    /// Chain network (drives which section we write under).
    pub network: Network,
    /// Root path containing the index (used for both projected-size and
    /// free-space queries). Usually the parent of the UTXO dir.
    pub index_root: PathBuf,
    /// Directories whose contents sum to the index's total disk footprint.
    /// Normally: the UTXO db dir + each Parquet out dir.
    pub tracked_dirs: Vec<PathBuf>,
}

impl DiskGuardConfig {
    /// Project the index's final size at `target_height` using the recorded
    /// checkpoints. `extra_dirs_in_tracking_file`: currently assumed to be
    /// parallel to `self.tracked_dirs`; the free-space stat still uses
    /// `index_root`. Returns the raw linear-regression estimate.
    pub fn projected_total_at(&self, target_height: u32) -> (u64, Vec<DiskCheckpoint>) {
        let cps = read_checkpoints(&self.checkpoint_file, self.network);
        let est = projected_total_bytes(&cps, target_height);
        (est, cps)
    }

    /// Current total disk footprint of the tracked dirs, in bytes.
    pub fn current_total_bytes(&self) -> u64 {
        self.tracked_dirs
            .iter()
            .map(|d| dir_size_bytes(d))
            .sum::<u64>()
    }

    /// Startup check: project the needed disk for finishing the chain and
    /// refuse to start if we'd leave <10 GiB free.
    ///
    /// - `node_tip_height`: the chain's current tip height (already fetched
    ///   by the caller). Used only to know where indexing will finish.
    /// - `local_tip_height`: the highest height we've already recorded a
    ///   checkpoint for; indexing continues from here (used only to log).
    /// - `min_free_bytes`: refuse if projected free falls below this.
    pub fn run_pre_flight(
        &self,
        node_tip_height: u32,
        local_tip_height: u32,
        min_free_bytes: u64,
    ) -> Result<PreflightReport> {
        let current = self.current_total_bytes();
        let (projected_total, checkpoints) = self.projected_total_at(node_tip_height);
        // With zero checkpoints we have no history to extrapolate from; the
        // report logs that and skips the bail decision entirely — we cannot
        // say anything meaningful about growth yet.
        let projection_available = !checkpoints.is_empty();
        let additional_needed = if projection_available {
            projected_total.saturating_sub(current)
        } else {
            0
        };

        let free_now = free_space_bytes(&self.index_root)
            .context("could not read filesystem free space (statfs)")?;
        let free_at_end = free_now.saturating_sub(additional_needed);

        let report = PreflightReport {
            network: self.network,
            indexed_tip: local_tip_height,
            node_tip: node_tip_height,
            checkpoints_used: checkpoints.len(),
            current_total_bytes: current,
            projected_total_bytes: projected_total,
            additional_needed_bytes: additional_needed,
            free_bytes_now: free_now,
            free_bytes_at_completion: free_at_end,
            min_free_bytes,
        };
        report.log();

        if projection_available && free_at_end < min_free_bytes {
            let deficit_bytes = min_free_bytes.saturating_sub(free_at_end);
            let need_gib = (deficit_bytes + (1 << 30) - 1) / (1 << 30);
            anyhow::bail!(
                "insufficient disk space: projecting the {} index to height {} \
                 needs ~{:.1} GiB more disk than will remain free (projected free at \
                 completion {:.1} GiB, require ≥ {:.1} GiB). Free up at least \
                 {} GiB and try again.",
                self.network.name(),
                node_tip_height,
                additional_needed as f64 / (1u64 << 30) as f64,
                free_at_end as f64 / (1u64 << 30) as f64,
                min_free_bytes as f64 / (1u64 << 30) as f64,
                need_gib,
            );
        }

        info!(
            free_gib = format_args!("{:.1}", free_now as f64 / (1u64 << 30) as f64),
            "startup disk check OK"
        );
        Ok(report)
    }
}

#[derive(Debug)]
#[allow(dead_code)] // field `min_free_bytes` kept for future callers/tests
pub struct PreflightReport {
    pub network: Network,
    pub indexed_tip: u32,
    pub node_tip: u32,
    pub checkpoints_used: usize,
    pub current_total_bytes: u64,
    pub projected_total_bytes: u64,
    pub additional_needed_bytes: u64,
    pub free_bytes_now: u64,
    pub free_bytes_at_completion: u64,
    pub min_free_bytes: u64,
}

impl PreflightReport {
    fn log(&self) {
        let gib = |b: u64| b as f64 / (1u64 << 30) as f64;
        if self.checkpoints_used == 0 {
            info!(
                network = self.network.name(),
                "no prior disk checkpoints for this network yet — growth estimate \
                 unavailable until 100k blocks have been indexed on-disk; skipping \
                 projection"
            );
            return;
        }
        info!(
            network = self.network.name(),
            indexed_tip = self.indexed_tip,
            node_tip = self.node_tip,
            checkpoints = self.checkpoints_used,
            current_gib = format_args!("{:.2}", gib(self.current_total_bytes)),
            projected_gib = format_args!("{:.2}", gib(self.projected_total_bytes)),
            additional_gib = format_args!("{:.2}", gib(self.additional_needed_bytes)),
            free_now_gib = format_args!("{:.1}", gib(self.free_bytes_now)),
            free_end_gib = format_args!("{:.1}", gib(self.free_bytes_at_completion)),
            "disk-space projection"
        );
    }
}

/// Append-only checkpoint recorder. Drop into a pipeline / live loop and call
/// `on_block_applied(height)` after each successfully applied block; it only
/// does work when `height % 100_000 == 0`.
pub struct DiskRecorder {
    cfg: DiskGuardConfig,
    state: Mutex<Option<u32>>, // last recorded checkpoint
}

impl DiskRecorder {
    pub fn new(cfg: DiskGuardConfig) -> Self {
        // Seed last-checkpoint from the file so a re-run doesn't duplicate.
        let last = read_checkpoints(&cfg.checkpoint_file, cfg.network)
            .last()
            .map(|c| c.height);
        DiskRecorder {
            cfg,
            state: Mutex::new(last),
        }
    }

    /// Nothing happens unless `height` is a fresh checkpoint boundary (env
    /// override for tests). Returns `true` if a checkpoint was written this
    /// call.
    pub fn on_block_applied(&self, height: u32) -> Result<bool> {
        let step = bidx_core::disk::checkpoint_step();
        if height == 0 || height % step != 0 {
            return Ok(false);
        }
        let mut g = self.state.lock().unwrap();
        if g.map_or(false, |lh| height <= lh) {
            return Ok(false);
        }
        let total = self.cfg.current_total_bytes();
        let cp = DiskCheckpoint {
            height,
            total_bytes: total,
        };
        append_checkpoint(&self.cfg.checkpoint_file, self.cfg.network, cp)
            .with_context(|| {
                format!(
                    "write checkpoint to {}",
                    self.cfg.checkpoint_file.display()
                )
            })?;
        info!(
            height,
            total_gib = format_args!("{:.2}", total as f64 / (1u64 << 30) as f64),
            "recorded disk checkpoint"
        );
        *g = Some(height);
        Ok(true)
    }
}

/// Convenience: build a `DiskGuardConfig` for the common layout
/// `<index_root>/{utxo-db,out[,*]}`.
#[allow(dead_code)]
pub fn default_layout(
    index_root: &Path,
    network: Network,
    utxo_dir: &Path,
    out_dirs: &[PathBuf],
) -> DiskGuardConfig {
    let mut tracked = vec![utxo_dir.to_path_buf()];
    tracked.extend(out_dirs.iter().cloned());
    DiskGuardConfig {
        checkpoint_file: index_root.join(DEFAULT_CHECKPOINT_FILE_NAME),
        network,
        index_root: index_root.to_path_buf(),
        tracked_dirs: tracked,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Serializes tests that mutate BIDX_DISK_CHECKPOINT_STEP so their env
    /// mutations don't leak into each other (tests run threads in parallel
    /// by default).
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static G: OnceLock<Mutex<()>> = OnceLock::new();
        G.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn tmp_cfg(id: &str) -> (DiskGuardConfig, PathBuf) {
        let tmp = std::env::temp_dir().join(format!("bidx-guard-test-{}-{}", std::process::id(), id));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let ck = tmp.join("ckpt.txt");
        let cfg = DiskGuardConfig {
            checkpoint_file: ck.clone(),
            network: Network::Mainnet,
            index_root: std::env::temp_dir(),
            tracked_dirs: vec![std::env::temp_dir()],
        };
        (cfg, ck)
    }

    #[test]
    fn pre_flight_bails_on_insufficient_disk() {
        let _g = env_lock();
        std::env::set_var("BIDX_DISK_CHECKPOINT_STEP", "1");
        let (cfg, ck) = tmp_cfg("bails");
        // One 100k checkpoint of 1 GiB → growth rate 0.01 B/blk ⇒ at tip
        // 1e9 the projection is ~10 TiB; guaranteed to exceed real disks.
        append_checkpoint(
            &ck,
            Network::Mainnet,
            DiskCheckpoint { height: 100_000, total_bytes: 1 << 30 },
        )
        .unwrap();
        let r = cfg.run_pre_flight(1_000_000_000, 100_000, bidx_core::MIN_FREE_BYTES);
        let e = r.err().expect("should bail");
        let s = format!("{:#}", e);
        assert!(s.contains("insufficient disk space"), "got: {}", s);
        assert!(s.contains("GiB"), "need GiB units, got: {}", s);
        std::env::remove_var("BIDX_DISK_CHECKPOINT_STEP");
    }

    #[test]
    fn pre_flight_succeeds_with_no_checkpoints() {
        let (cfg, _ck) = tmp_cfg("nockpt");
        // No checkpoints: projection unavailable, but must not crash.
        let r = cfg.run_pre_flight(900_000, 0, bidx_core::MIN_FREE_BYTES).unwrap();
        assert_eq!(r.checkpoints_used, 0);
        // Fields are populated sensibly.
        assert_eq!(r.network, Network::Mainnet);
        assert_eq!(r.node_tip, 900_000);
        assert_eq!(r.additional_needed_bytes, 0);
        assert!(r.free_bytes_now > 0);
        assert_eq!(r.free_bytes_at_completion, r.free_bytes_now);
    }

    #[test]
    fn pre_flight_projection_positive_when_checkpoints_show_growth() {
        let (mut cfg, ck) = tmp_cfg("slope");
        // Use a minimal, controlled tracked dir so current_total_bytes is tiny
        // (not the whole system /tmp).
        let tiny = std::env::temp_dir().join(format!("bidx-tiny-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tiny);
        std::fs::create_dir_all(&tiny).unwrap();
        std::fs::write(tiny.join("only"), [0u8; 64]).unwrap();
        cfg.tracked_dirs = vec![tiny.clone()];
        // Historical growth: 0.5 GiB per 100k blocks — 0.005 B/blk.
        for (h, bytes) in [(100_000u32, 1u64 << 29), (200_000u32, 1u64 << 30)] {
            append_checkpoint(&ck, Network::Mainnet, DiskCheckpoint { height: h, total_bytes: bytes })
                .unwrap();
        }
        // Currently indexed 200k, total 1 GiB on disk; project another 200k blocks.
        let r = cfg.run_pre_flight(400_000, 200_000, 0).unwrap();
        assert_eq!(r.checkpoints_used, 2);
        // Fit through (100k,0.5G) (200k,1G) projects 2G at 400k — much more
        // than the 64-byte current dir we just wrote.
        assert!(r.projected_total_bytes > r.current_total_bytes);
        assert!(r.additional_needed_bytes > 0);
        let _ = std::fs::remove_dir_all(&tiny);
    }

    #[test]
    fn recorder_on_block_applied_respects_step_and_dedup() {
        let _g = env_lock();
        std::env::set_var("BIDX_DISK_CHECKPOINT_STEP", "5");
        let (cfg, ck) = tmp_cfg("recorder");
        let rec = DiskRecorder::new(cfg.clone());
        // h=0 never records.
        assert!(!rec.on_block_applied(0).unwrap());
        // h=1,2,3,4,6 not multiple of 5 → no write.
        for h in [1, 2, 3, 4, 6] {
            assert!(!rec.on_block_applied(h).unwrap());
        }
        // h=5 and h=10 write.
        assert!(rec.on_block_applied(5).unwrap());
        assert!(rec.on_block_applied(10).unwrap());
        // Re-playing h=5 ≤ last_dur must NOT write (dedup guard).
        assert!(!rec.on_block_applied(5).unwrap());
        let read = read_checkpoints(&ck, Network::Mainnet);
        assert_eq!(read.len(), 2, "expected 2 checkpoints, got {:?}", read);
        assert_eq!(read[0].height, 5);
        assert_eq!(read[1].height, 10);
        std::env::remove_var("BIDX_DISK_CHECKPOINT_STEP");
    }

    #[test]
    fn current_total_bytes_sums_tracked_dirs() {
        let (mut cfg, _ck) = tmp_cfg("total");
        let tmp = std::env::temp_dir().join(format!("bidx-guard-total-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(tmp.join("a")).unwrap();
        std::fs::create_dir_all(tmp.join("b")).unwrap();
        std::fs::write(tmp.join("a/x"), [1u8; 16]).unwrap();
        std::fs::write(tmp.join("b/y"), [1u8; 32]).unwrap();
        cfg.tracked_dirs = vec![tmp.join("a"), tmp.join("b")];
        let t = cfg.current_total_bytes();
        assert!(t >= 48, "got {}", t);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn default_layout_builds_expected_paths() {
        let root = std::path::PathBuf::from("/tmp/bidx-root");
        let utxo = std::path::PathBuf::from("/tmp/bidx-root/utxo");
        let out = std::path::PathBuf::from("/tmp/bidx-root/out");
        let cfg = default_layout(&root, Network::Testnet4, &utxo, std::slice::from_ref(&out));
        assert_eq!(cfg.network, Network::Testnet4);
        assert_eq!(cfg.index_root, root);
        assert_eq!(cfg.checkpoint_file, root.join("bidx-disk-checkpoints.txt"));
        assert_eq!(cfg.tracked_dirs, vec![utxo, out]);
    }

    #[test]
    fn recorder_boots_up_last_known_checkpoint_skips_rewrites() {
        let _g = env_lock();
        std::env::set_var("BIDX_DISK_CHECKPOINT_STEP", "10");
        let (cfg, ck) = tmp_cfg("boots");
        append_checkpoint(&ck, Network::Mainnet, DiskCheckpoint { height: 20, total_bytes: 1 }).unwrap();
        let rec = DiskRecorder::new(cfg);
        // Re-seeded from existing file: below-max h is rejected.
        assert!(!rec.on_block_applied(10).unwrap());
        // New, higher boundary is accepted.
        assert!(rec.on_block_applied(30).unwrap());
        let read = read_checkpoints(&ck, Network::Mainnet);
        assert_eq!(read.len(), 2);
        assert_eq!(read[1].height, 30);
        std::env::remove_var("BIDX_DISK_CHECKPOINT_STEP");
    }
}
