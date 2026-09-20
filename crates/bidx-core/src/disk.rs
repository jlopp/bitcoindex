//! Disk-space accounting for the indexer's startup disk-space guard.
//!
//! Two jobs:
//!   1. Record how much disk the indexer consumes at each 100,000-height
//!      boundary, per network, so we can estimate how large the DB will be
//!      when indexing completes.
//!   2. On startup, project the final size against available free space and
//!      refuse to start if we'd run out (<10 GB free by completion).
//!
//! The checkpoint history is a plain append-only text file. The format is
//! line-based for easy inspection:
//!
//! ```text
//! # bidx disk checkpoints v1
//! network       mainnet
//! 100000        318705664
//! 200000        673185792
//! ```
//!
//! Heights are absolute block heights; the second column is the total size
//! in bytes of the indexer's disk footprint at that height (sum of the
//! directory sizes for the UTXO store + each Parquet out directory). A
//! linear regression through these checkpoints gives us the growth rate in
//! bytes/block, which is what the startup projection uses.

use std::fs;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use crate::{BLOCK_MAGIC, BLOCK_MAGIC_REGTEST, BLOCK_MAGIC_SIGNET, BLOCK_MAGIC_TESTNET3, BLOCK_MAGIC_TESTNET4};

/// Height bucket for checkpoint recording — every 100,000 blocks.
pub const CHECKPOINT_HEIGHT_STEP: u32 = 100_000;

/// Effective checkpoint step. Reads `BIDX_DISK_CHECKPOINT_STEP` if set (used
/// only in tests to avoid having to simulate 100k blocks).
pub fn checkpoint_step() -> u32 {
    std::env::var("BIDX_DISK_CHECKPOINT_STEP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(CHECKPOINT_HEIGHT_STEP)
}
/// Refuse startup if projected free space at completion is below this.
pub const MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet3,
    Testnet4,
    Signet,
    Regtest,
}

impl Network {
    pub fn name(&self) -> &'static str {
        match self {
            Network::Mainnet => "mainnet",
            Network::Testnet3 => "testnet3",
            Network::Testnet4 => "testnet4",
            Network::Signet => "signet",
            Network::Regtest => "regtest",
        }
    }

    pub fn from_magic(magic: u32) -> Option<Self> {
        match magic {
            BLOCK_MAGIC => Some(Network::Mainnet),
            BLOCK_MAGIC_TESTNET3 => Some(Network::Testnet3),
            BLOCK_MAGIC_SIGNET => Some(Network::Signet),
            BLOCK_MAGIC_TESTNET4 => Some(Network::Testnet4),
            BLOCK_MAGIC_REGTEST => Some(Network::Regtest),
            _ => None,
        }
    }

    /// Detect the network a `blocks_dir` belongs to by reading the first
    /// blk record's magic. Returns `None` if no blk file can be read.
    pub fn detect_from_blocks_dir(blocks_dir: &Path) -> Option<Self> {
        // Find the first blk*.dat file (sorted for determinism).
        let mut entries: Vec<PathBuf> = fs::read_dir(blocks_dir)
            .ok()?
            .filter_map(|e| {
                let p = e.ok()?.path();
                let name = p.file_name()?.to_str()?;
                if name.starts_with("blk") && name.ends_with(".dat") {
                    Some(p.to_path_buf())
                } else {
                    None
                }
            })
            .collect();
        entries.sort();
        let first = entries.into_iter().next()?;
        let data = fs::read(&first).ok()?;
        if data.len() < 4 {
            return None;
        }
        let magic = u32::from_le_bytes(data[..4].try_into().ok()?);
        Self::from_magic(magic)
    }
}

/// Size of a directory tree in bytes, recursive. Best-effort: ignores
/// symlinks (doesn't follow them) and any file that errors.
pub fn dir_size_bytes(path: &Path) -> u64 {
    fn inner(path: &Path) -> u64 {
        let mut total = 0u64;
        let Ok(rd) = fs::read_dir(path) else { return 0 };
        for entry in rd.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_symlink() {
                continue;
            }
            let p = entry.path();
            if ft.is_dir() {
                total = total.saturating_add(inner(&p));
            } else if let Ok(m) = entry.metadata() {
                total = total.saturating_add(m.len());
            }
        }
        total
    }
    if !path.exists() {
        return 0;
    }
    inner(path)
}

/// Free bytes available to an unprivileged user on the filesystem containing `path`.
pub fn free_space_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    // statfs fails on non-existent paths; walk up to an existing parent.
    let mut p: &Path = path;
    loop {
        let s = CString::new(p.as_os_str().as_bytes()).ok()?;
        let mut b: libc::statfs = unsafe { std::mem::zeroed() };
        let r = unsafe { libc::statfs(s.as_ptr(), &mut b) };
        if r == 0 {
            let avail = b.f_bavail as u64;
            let size = b.f_bsize as u64;
            return Some(avail.saturating_mul(size));
        }
        match p.parent() {
            Some(parent) => p = parent,
            None => return None,
        }
    }
}

/// A single recorded (height, total_bytes) checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskCheckpoint {
    pub height: u32,
    pub total_bytes: u64,
}

/// Read the checkpoint history for one network. Returns an empty vec if the
/// file doesn't exist yet or the network isn't recorded in it.
///
/// The file may contain checkpoints for multiple networks — only the section
/// under `network <name>` matching `net` is returned.
pub fn read_checkpoints(path: &Path, net: Network) -> Vec<DiskCheckpoint> {
    let Ok(f) = fs::File::open(path) else { return Vec::new() };
    let r = std::io::BufReader::new(f);
    let mut out = Vec::new();
    let mut cur_net: Option<String> = None;
    for line in r.lines().map_while(Result::ok) {
        let line = line.trim().to_string();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts[0] == "network" && parts.len() == 2 {
            cur_net = Some(parts[1].to_string());
            continue;
        }
        if parts.len() == 2 {
            if cur_net.as_deref() == Some(net.name()) {
                if let (Ok(h), Ok(b)) = (parts[0].parse::<u32>(), parts[1].parse::<u64>()) {
                    out.push(DiskCheckpoint { height: h, total_bytes: b });
                }
            }
        }
    }
    out.sort_by_key(|c| c.height);
    out
}

/// Append a checkpoint under the section `network <name>`. Creates the file
/// (with a header comment) if it doesn't exist; appends a new `network` block
/// if the network isn't recorded yet.
pub fn append_checkpoint(path: &Path, net: Network, cp: DiskCheckpoint) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let existing = read_checkpoints(path, net);
    // Don't record the same height twice.
    if existing.iter().any(|c| c.height == cp.height) {
        return Ok(());
    }
    let has_section = existing.iter().next().is_some()
        || fs::read_to_string(path)
            .map(|s| s.lines().any(|l| l.trim() == format!("network {}", net.name())))
            .unwrap_or(false);

    let need_header = !path.exists();
    let mut f = fs::OpenOptions::new().create(true).append(true).open(path)?;
    if need_header {
        writeln!(f, "# bidx disk checkpoints v1")?;
        writeln!(f, "# one 'network <name>' section per chain; lines are '<height> <total_bytes>'")?;
    }
    if !has_section {
        writeln!(f, "network {}", net.name())?;
    }
    writeln!(f, "{} {}", cp.height, cp.total_bytes)?;
    Ok(())
}

/// Fit a simple linear regression `bytes = a + b * height` through the
/// checkpoints and return the slope (bytes per block) + intercept.
///
/// Falls back to a coarse single-segment slope when <2 points, then 0 when
/// no data. The projection is honest: it only needs to be a rough upper
/// bound to catch the "incomplete mainnet on a small disk" case early.
fn linreg(pts: &[DiskCheckpoint]) -> (f64, f64) {
    if pts.is_empty() {
        return (0.0, 0.0);
    }
    if pts.len() == 1 {
        return (0.0, pts[0].total_bytes as f64);
    }
    let n = pts.len() as f64;
    let mut sx = 0.0f64;
    let mut sy = 0.0f64;
    let mut sxy = 0.0f64;
    let mut sxx = 0.0f64;
    for p in pts {
        let x = p.height as f64;
        let y = p.total_bytes as f64;
        sx += x;
        sy += y;
        sxy += x * y;
        sxx += x * x;
    }
    let denom = n * sxx - sx * sx;
    if denom.abs() < f64::EPSILON {
        return (0.0, sy / n);
    }
    let b = (n * sxy - sx * sy) / denom;
    let a = (sy - b * sx) / n;
    (a, b.max(0.0))
}

/// Project the total index size (bytes) at `target_height` using the
/// recorded checkpoint history.
pub fn projected_total_bytes(pts: &[DiskCheckpoint], target_height: u32) -> u64 {
    let (a, b) = linreg(pts);
    (a + b * target_height as f64).max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn network_detection_mainnet() {
        // 16-byte buffer beginning with the mainnet magic.
        let mut v = vec![0u8; 16];
        v[..4].copy_from_slice(&BLOCK_MAGIC.to_le_bytes());
        let tmp = std::env::temp_dir().join(format!("bidx-disk-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("blk00000.dat"), &v).unwrap();
        assert_eq!(Network::detect_from_blocks_dir(&tmp), Some(Network::Mainnet));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn checkpoints_roundtrip_and_growth() {
        let tmp = std::env::temp_dir().join(format!("bidx-disk-ckpt-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        append_checkpoint(&tmp, Network::Mainnet, DiskCheckpoint { height: 100_000, total_bytes: 1_000 }).unwrap();
        append_checkpoint(&tmp, Network::Mainnet, DiskCheckpoint { height: 200_000, total_bytes: 2_000 }).unwrap();
        append_checkpoint(&tmp, Network::Mainnet, DiskCheckpoint { height: 100_000, total_bytes: 1_000 }).unwrap();
        let v = read_checkpoints(&tmp, Network::Mainnet);
        assert_eq!(v.len(), 2, "duplicate height not re-recorded");
        // Slope between the two points: 1000 bytes / 100_000 blocks = 0.01 B/blk.
        let proj = projected_total_bytes(&v, 300_000);
        assert_eq!(proj, 3_000);
        let _ = std::fs::remove_file(&tmp);
    }
}
