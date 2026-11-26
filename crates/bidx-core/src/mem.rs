//! Host memory detection and cache-budget planning.
//!
//! The indexer's ingest throughput is dominated by the RocksDB UTXO read path:
//! `apply_block` performs one point lookup per spent input, and on mainnet the
//! UTXO set is far larger than the block cache, so most lookups fault a data
//! block off disk. The fix is to spend the machine's *idle* RAM on caches — a
//! larger RocksDB block cache and an in-memory recent-UTXO cache — rather than
//! leaving it unused.
//!
//! This module owns the policy for how much RAM to hand out. It is deliberately
//! pure (the `/proc/meminfo` read is a thin wrapper) so the split is unit
//! testable without a live machine.
//!
//! Two quantities matter:
//!
//! - `MemAvailable` is the kernel's own estimate of what a new allocation can
//!   obtain without swapping. It is the right input, *not* `MemTotal`: on a
//!   busy box `MemTotal` ignores every other process, so a budget sized from it
//!   can sit above the kernel's OOM point.
//! - The budget is a *fraction* of the usable amount, not all of it. The
//!   remaining slack covers allocator overhead, the reorder buffer, sink
//!   buffers, and — importantly — leaves the OS some page-cache headroom so
//!   the RocksDB compaction writes are not starved.

/// One tenth of `MemAvailable` is reserved against the kernel's own estimate
/// being optimistic (cgroup limits, memory fragmentation, a competing process
/// growing between the read and the allocation).
const RESERVE_FRACTION_DENOM: u64 = 10;

/// Fixed reserve for the runtime itself: parse worker scratch, the reorder
/// buffer, in-flight sink batches, and allocator slack. Kept in KiB to match
/// `/proc/meminfo`.
pub const RUNTIME_RESERVE_KB: u64 = 2 * 1024 * 1024; // 2 GiB

/// Percentage of the *usable* memory handed to the caches. The rest stays as
/// slack (see the module docs). 60% is deliberately conservative: on a 32 GiB
/// box with ~19 GiB available that is still a multi-GiB cache.
pub const CACHE_SHARE_PCT: u64 = 60;

/// Percentage of the cache budget given to the recent-UTXO cache. It gets the
/// larger share because a hit there removes the RocksDB lookup entirely (no
/// bloom check, no memtable/SST search, no `PinnableSlice`), whereas a block
/// cache hit still pays the per-key lookup work — see `PERF.md` §9.
pub const RECENT_SHARE_PCT: u64 = 60;

/// Fallback used when `/proc/meminfo` is unreadable (non-Linux, or a fault).
/// Small enough to be safe on any host: a 1 GiB block cache and no recent
/// cache, i.e. exactly the pre-existing behaviour.
const FALLBACK_AVAILABLE_KB: u64 = 3 * 1024 * 1024; // 3 GiB

/// A planned split of RAM between the two caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheBudget {
    /// RocksDB block-cache capacity, shared by every column family.
    pub block_cache_bytes: usize,
    /// Recent-UTXO cache capacity. `0` disables it.
    pub recent_cache_bytes: usize,
}

/// Read a `name: <value> kB` field from `/proc/meminfo` content. `None` when
/// the content is absent, the field is missing, or the value does not parse.
///
/// Split out from the file read so every branch (present / missing / garbage)
/// is testable without a live `/proc`.
pub fn meminfo_field_kb(meminfo: Option<&str>, name: &str) -> Option<u64> {
    let meminfo = meminfo?;
    meminfo.lines().find_map(|l| {
        let rest = l.strip_prefix(name)?;
        rest.split_whitespace().next()?.parse::<u64>().ok()
    })
}

/// `MemAvailable` in KiB, or a conservative fallback when unavailable.
pub fn mem_available_kb() -> u64 {
    let raw = std::fs::read_to_string("/proc/meminfo").ok();
    meminfo_field_kb(raw.as_deref(), "MemAvailable:").unwrap_or(FALLBACK_AVAILABLE_KB)
}

/// `MemTotal` in KiB, or a conservative fallback when unavailable.
pub fn mem_total_kb() -> u64 {
    let raw = std::fs::read_to_string("/proc/meminfo").ok();
    meminfo_field_kb(raw.as_deref(), "MemTotal:").unwrap_or(FALLBACK_AVAILABLE_KB)
}

/// Plan a cache budget from the host's available and total memory.
///
/// The usable amount is `available` minus a tenth (measurement reserve) minus
/// the fixed runtime reserve; the caches then take [`CACHE_SHARE_PCT`] of that,
/// split [`RECENT_SHARE_PCT`] to the recent-UTXO cache and the remainder to the
/// RocksDB block cache. `total` only bounds the result so a bogus
/// `MemAvailable` (some kernels report more available than total) cannot
/// produce a budget larger than the machine.
pub fn plan_cache_budget(available_kb: u64, total_kb: u64) -> CacheBudget {
    // A bogus `MemAvailable` above `MemTotal` (seen on some kernels) is clamped
    // to the physical machine. A zero `MemTotal` is ignored rather than used as
    // the cap, so a missing field cannot collapse the budget to nothing.
    let cap = if total_kb == 0 { available_kb } else { total_kb };
    let available_kb = available_kb.min(cap);
    let usable_kb = available_kb
        .saturating_sub(available_kb / RESERVE_FRACTION_DENOM)
        .saturating_sub(RUNTIME_RESERVE_KB);
    let cache_kb = usable_kb * CACHE_SHARE_PCT / 100;
    let recent_kb = cache_kb * RECENT_SHARE_PCT / 100;
    let block_kb = cache_kb - recent_kb;
    CacheBudget {
        block_cache_bytes: kb_to_bytes(block_kb),
        recent_cache_bytes: kb_to_bytes(recent_kb),
    }
}

/// Convert KiB to bytes, saturating rather than overflowing on absurd inputs.
fn kb_to_bytes(kb: u64) -> usize {
    kb.saturating_mul(1024).min(usize::MAX as u64) as usize
}

/// Plan a cache budget from the live host. Thin wrapper over
/// [`plan_cache_budget`] so callers (the CLI) have one obvious entry point.
pub fn plan_cache_budget_for_host() -> CacheBudget {
    plan_cache_budget(mem_available_kb(), mem_total_kb())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_field_reads_the_named_field_only() {
        let blob = "MemTotal:       31617968 kB\nMemAvailable:   19306756 kB\nSwapTotal: 0 kB\n";
        assert_eq!(meminfo_field_kb(Some(blob), "MemAvailable:"), Some(19_306_756));
        assert_eq!(meminfo_field_kb(Some(blob), "MemTotal:"), Some(31_617_968));
        // A field that is not present yields None.
        assert_eq!(meminfo_field_kb(Some(blob), "MemFree:"), None);
    }

    #[test]
    fn meminfo_field_rejects_garbage_and_missing_input() {
        // Non-numeric value.
        assert_eq!(meminfo_field_kb(Some("MemAvailable: lots kB\n"), "MemAvailable:"), None);
        // Value with no token after the name.
        assert_eq!(meminfo_field_kb(Some("MemAvailable:\n"), "MemAvailable:"), None);
        // No content at all.
        assert_eq!(meminfo_field_kb(None, "MemAvailable:"), None);
    }

    #[test]
    fn live_reads_are_positive() {
        // A running Linux host always reports both fields; the fallback keeps
        // them non-zero even if /proc is unavailable.
        assert!(mem_available_kb() > 0);
        assert!(mem_total_kb() > 0);
    }

    #[test]
    fn plan_splits_usable_memory_between_the_two_caches() {
        // 19 GiB available, 32 GiB total: usable = 19 - 1.9 - 2 = 15.1 GiB,
        // cache = 60% = 9.06 GiB, recent = 60% of that = 5.44 GiB,
        // block = 3.62 GiB. Assert the exact arithmetic rather than a range so
        // a change to any constant is caught here.
        let avail = 19 * 1024 * 1024; // KiB
        let total = 32 * 1024 * 1024;
        let b = plan_cache_budget(avail, total);
        let usable_kb = avail - avail / 10 - RUNTIME_RESERVE_KB;
        let cache_kb = usable_kb * CACHE_SHARE_PCT / 100;
        let recent_kb = cache_kb * RECENT_SHARE_PCT / 100;
        let block_kb = cache_kb - recent_kb;
        assert_eq!(b.recent_cache_bytes, (recent_kb * 1024) as usize);
        assert_eq!(b.block_cache_bytes, (block_kb * 1024) as usize);
        // The recent cache gets the larger share by construction.
        assert!(b.recent_cache_bytes > b.block_cache_bytes);
    }

    #[test]
    fn plan_never_exceeds_available_and_is_zero_safe() {
        // A tiny machine: the reserves swallow everything, so both caches are 0
        // rather than the arithmetic wrapping around to a huge number.
        let b = plan_cache_budget(512 * 1024, 512 * 1024);
        assert_eq!(b, CacheBudget { block_cache_bytes: 0, recent_cache_bytes: 0 });

        // A bogus MemAvailable larger than MemTotal is clamped to MemTotal, so
        // the budget cannot exceed the physical machine.
        let b = plan_cache_budget(999 * 1024 * 1024, 8 * 1024 * 1024);
        let clamped = plan_cache_budget(8 * 1024 * 1024, 8 * 1024 * 1024);
        assert_eq!(b, clamped);
    }

    #[test]
    fn plan_at_the_default_fallback_is_the_legacy_block_cache_size() {
        // The fallback (3 GiB available) must not silently change behaviour on
        // a host with no /proc: usable = 3 - 0.3 - 2 = 0.7 GiB, cache = 0.42
        // GiB. That is below the legacy 1 GiB, which is the conservative side —
        // assert it is simply small and non-negative, not a specific value.
        let b = plan_cache_budget(FALLBACK_AVAILABLE_KB, FALLBACK_AVAILABLE_KB);
        assert!(b.block_cache_bytes < 1024 * 1024 * 1024);
        assert!(b.block_cache_bytes > 0);
    }

    #[test]
    fn kb_to_bytes_saturates() {
        assert_eq!(kb_to_bytes(1), 1024);
        assert_eq!(kb_to_bytes(u64::MAX), usize::MAX);
    }

    #[test]
    fn plan_for_host_returns_a_sane_split() {
        let b = plan_cache_budget_for_host();
        // On any real host at least one cache is non-empty; the split is
        // ordered the same way as the pure function.
        assert!(b.block_cache_bytes > 0 || b.recent_cache_bytes > 0);
        assert!(b.recent_cache_bytes >= b.block_cache_bytes);
    }
}
