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
            peak_rss_kb: end_res.rss_kb.max(self.start_res.rss_kb),
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
}
