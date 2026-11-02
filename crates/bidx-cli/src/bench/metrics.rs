//! Lightweight per-stage metric collection for benchmarking.
//!
//! Each stage reports a wall-clock duration plus a resource snapshot so we
//! can reason about CPU vs I/O vs memory. Encodings are JSON-friendly.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default)]
pub struct ResourceSample {
    /// Resident set size, KiB.
    pub rss_kb: u64,
    /// User CPU jiffies consumed by the process so far.
    pub user_jiffies: u64,
    /// System CPU jiffies consumed by the process so far.
    pub sys_jiffies: u64,
}

impl ResourceSample {
    pub fn now() -> Self {
        let mut out = ResourceSample::default();
        if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
            // Fields after comm (in parens, may contain spaces) start at field 3.
            // utime=14, stime=15, rss=24 (1-indexed from start of line).
            if let Some(rparen) = stat.rfind(')') {
                let rest = &stat[rparen + 2..]; // state char + space
                let f: Vec<&str> = rest.split_whitespace().collect();
                // rest[0]=state(3), rest[11]=utime(14), rest[12]=stime(15), rest[21]=rss(24)
                if f.len() > 21 {
                    out.user_jiffies = f[11].parse().unwrap_or(0);
                    out.sys_jiffies = f[12].parse().unwrap_or(0);
                    let pages: u64 = f[21].parse().unwrap_or(0);
                    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
                    out.rss_kb = pages * page_size / 1024;
                }
            }
        }
        out
    }

    pub fn cpu_jiffies(&self) -> u64 {
        self.user_jiffies + self.sys_jiffies
    }
}

/// Process lifetime peak resident set size, in KiB.
///
/// `ResourceSample::now()` returns the *instantaneous* RSS, so taking the max
/// of two samples (start and end of a stage) silently under-reports any peak
/// that happens in between — and a reorder buffer that balloons and drains
/// within one stage is exactly that shape. This reads the kernel's own
/// high-water mark (`VmHWM`), which is monotonic and therefore immune to
/// sampling luck.
///
/// Falls back to `getrusage(2)`'s `ru_maxrss` (also KiB on Linux) when
/// `/proc` is unavailable, and to 0 if neither works.
pub fn peak_rss_kb() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(v) = status.lines().find_map(|l| {
            let rest = l.strip_prefix("VmHWM:")?;
            rest.split_whitespace().next()?.parse::<u64>().ok()
        }) {
            return v;
        }
    }
    // SAFETY: `getrusage` only writes into the provided struct and cannot fail
    // for RUSAGE_SELF; we still check the return value.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc == 0 {
        // Linux reports ru_maxrss in KiB; other platforms use bytes, but this
        // indexer is Linux-only in practice.
        return usage.ru_maxrss as u64;
    }
    0
}

/// Subtract-able snapshot pair for one stage.
#[derive(Debug, Clone, Copy)]
pub struct StageTimer {
    start: Instant,
    start_res: ResourceSample,
}

impl StageTimer {
    pub fn start() -> Self {
        StageTimer {
            start: Instant::now(),
            start_res: ResourceSample::now(),
        }
    }

    pub fn stop(self) -> StageMeasure {
        let wall = self.start.elapsed();
        let end_res = ResourceSample::now();
        StageMeasure {
            wall,
            cpu_jiffies: end_res.cpu_jiffies() - self.start_res.cpu_jiffies(),
            // Process-lifetime high-water mark, monotonic across stages. Using
            // the max of the two instantaneous samples would miss a buffer that
            // balloons and drains inside a single stage, which is precisely the
            // failure mode this harness exists to catch.
            peak_rss_kb: peak_rss_kb(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StageMeasure {
    pub wall: Duration,
    pub cpu_jiffies: u64,
    pub peak_rss_kb: u64,
}

impl StageMeasure {
    pub fn wall_secs(&self) -> f64 {
        self.wall.as_secs_f64()
    }

    /// Jiffies per second of wall time — >1 means multiple cores busy.
    pub fn cpu_utilization(&self, thread_count_hint: f64) -> f64 {
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
        let secs = self.wall.as_secs_f64().max(1e-6);
        let cpu_secs = self.cpu_jiffies as f64 / hz;
        (cpu_secs / secs / thread_count_hint).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_sample_reads_proc() {
        let a = ResourceSample::now();
        // Do some work.
        let mut acc = 0u64;
        for i in 0..1_000_000u64 {
            acc = acc.wrapping_add(i * i);
        }
        let b = ResourceSample::now();
        assert!(b.cpu_jiffies() >= a.cpu_jiffies());
        assert!(b.rss_kb > 0);
        std::hint::black_box(acc);
    }

    #[test]
    fn peak_rss_kb_is_positive_and_monotonic() {
        // A live process always has a non-zero high-water mark, and it can
        // never decrease: it is the kernel's monotonic VmHWM.
        let a = peak_rss_kb();
        assert!(a > 0, "peak_rss_kb returned 0");
        // Force some allocation so the high-water mark has a chance to move,
        // then confirm the second reading is not below the first.
        let ballast: Vec<u8> = vec![7u8; 8 * 1024 * 1024];
        let b = peak_rss_kb();
        assert!(b >= a, "peak RSS went backwards: {a} -> {b}");
        std::hint::black_box(&ballast);
    }

    #[test]
    fn peak_rss_kb_is_at_least_instantaneous_rss() {
        // VmHWM is by definition >= current VmRSS, so the harness's headline
        // number can never under-report what the process is actually using.
        let inst = ResourceSample::now().rss_kb;
        let peak = peak_rss_kb();
        assert!(
            peak >= inst,
            "peak {peak} KiB below instantaneous {inst} KiB"
        );
    }

    #[test]
    fn stage_measure_reports_peak_high_water_mark() {
        // A stage that allocates and frees within its own body must still
        // report the transient peak: this is the regression that let a
        // reorder buffer balloon and drain unnoticed.
        let t = StageTimer::start();
        {
            let ballast: Vec<u8> = vec![3u8; 32 * 1024 * 1024];
            std::hint::black_box(&ballast);
        }
        let m = t.stop();
        assert!(
            m.peak_rss_kb >= 32 * 1024,
            "stage peak {} KiB did not capture the transient allocation",
            m.peak_rss_kb
        );
    }
}
