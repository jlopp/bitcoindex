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
//! # One plan, not two budgets
//!
//! There used to be *two* independent numbers derived from `/proc/meminfo`: the
//! cache budget (a fraction of available) and the watchdog ceiling (available
//! minus a reserve). Neither knew about the other, and neither accounted for the
//! heap that is *not* a cache but is still ours: RocksDB memtables, the decoded
//! blk-file LRU, the reorder buffer, the sink buffers, and RocksDB's own
//! index/filter/iterator allocations. The result was that `caches + overhead`
//! could reach the ceiling, so the watchdog tripped exactly at the point where a
//! RocksDB allocation could fail — and RocksDB does not handle allocation
//! failure, it segfaults.
//!
//! [`plan_memory`] fixes that by deriving everything from one number and
//! enforcing the invariant:
//!
//! ```text
//! block_cache + recent_cache + memtables + file_lru + fixed_runtime
//!     <= ceiling * (1 - spike_margin)
//! ```
//!
//! where `ceiling` is the same watchdog limit the runtime aborts at. The caches
//! are sized *last*, as whatever is left over, so they can never push the total
//! past the ceiling.

/// One eighth of `MemAvailable` is reserved for the kernel page cache (which
/// backs `blk*.dat`, SST, and Parquet I/O) and against `MemAvailable` being
/// optimistic (cgroup limits, memory fragmentation, or competing host
/// processes).
const RESERVE_FRACTION_DENOM: u64 = 8;

/// Fixed reserve for the runtime itself: parse worker scratch, the reorder
/// buffer, in-flight sink batches, and allocator slack. Kept in KiB to match
/// `/proc/meminfo`.
pub const RUNTIME_RESERVE_KB: u64 = 2 * 1024 * 1024; // 2 GiB

/// RocksDB memtable budget: `write_buffer_size (512 MiB) *
/// max_write_buffer_number (4)` for the UTXO family, plus the two small aux
/// families. Capped across all families via `set_db_write_buffer_size` so this
/// is a hard ceiling, not a per-family one.
pub const MEMTABLE_BUDGET_KB: u64 = 2 * 1024 * 1024; // 2 GiB

/// Heap that is not a cache and does not scale with the block cache: the
/// `ChainIndex` built by the header pass, the reorder/channel buffers, the
/// `pending` map, `ParquetSink`'s column builders and Zstd encoders, RocksDB's
/// background compaction buffers and `TableReader` handles, and multi-thread
/// allocator arena slack.
pub const FIXED_RUNTIME_RESERVE_KB: u64 = 2 * 1024 * 1024; // 2 GiB

/// Decoded size of one blk file held by the file LRU. Bitcoin Core v28 stores
/// blk files XOR-obfuscated, so opening one allocates a full decoded buffer of
/// the file's length (~128 MiB for mainnet).
pub const DECODED_FILE_KB: u64 = 128 * 1024; // 128 MiB

/// Upper bound on resident files in the decoded-blk LRU. Because parse workers
/// pull heights from a sequential cursor, all workers stay within a small
/// window of heights (typically 1-2 `blk*.dat` files, occasionally 3-4 when
/// Bitcoin Core wrote blocks slightly out of order), so 8 files (1 GiB) keeps
/// hit rates near 100% without pinning 2 GiB.
pub const MAX_FILE_CACHE_FILES: usize = 8;

/// Fraction of the post-overhead ceiling the caches may occupy. The remaining
/// 25% is the spike margin for RocksDB compaction bursts, file-backed `mmap`
/// pages counted in `VmRSS`, and allocator arena retention.
pub const CACHE_FRACTION_PCT: u64 = 75;

/// Soft-threshold percentage of `ceiling_kb`: cache-shedding starts here,
/// leaving a 20% runway before the hard ceiling for progressive relief steps
/// to take effect.
pub const SOFT_PCT: u64 = 80;

/// Hysteresis recovery percentage of `ceiling_kb`: pressure clears once RSS
/// drops back below this band.
pub const RECOVER_PCT: u64 = 72;

/// Percentage of the *cache* budget given to the recent-UTXO cache. It gets the
/// larger share because a hit there removes the RocksDB lookup entirely (no
/// bloom check, no memtable/SST search, no `PinnableSlice`), whereas a block
/// cache hit still pays the per-key lookup work — see `PERF.md` §9.
pub const RECENT_SHARE_PCT: u64 = 60;

/// Smallest ceiling we will ever hand out. Below this the process cannot make
/// useful progress, so clamping here (rather than returning a near-zero limit
/// that trips instantly) keeps the failure mode a clear error, not a hang.
pub const MIN_CEILING_KB: u64 = 512 * 1024; // 512 MiB

/// Fallback used when `/proc/meminfo` is unreadable (non-Linux, or a fault).
/// Small enough to be safe on any host: a small block cache and no recent
/// cache, i.e. exactly the pre-existing behaviour.
const FALLBACK_AVAILABLE_KB: u64 = 3 * 1024 * 1024; // 3 GiB

/// A planned split of RAM between the two caches. Kept as the public cache-only
/// view for callers that only care about the caches (e.g. the bench harness);
/// [`MemoryPlan`] is the full picture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheBudget {
    /// RocksDB block-cache capacity, shared by every column family.
    pub block_cache_bytes: usize,
    /// Recent-UTXO cache capacity. `0` disables it.
    pub recent_cache_bytes: usize,
}

/// The complete memory plan for a bulk parse run: the watchdog ceiling plus
/// every budget that must fit beneath it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryPlan {
    /// Hard RSS ceiling in KiB. The runtime aborts when RSS exceeds this; the
    /// soft threshold (a fraction of it) triggers relief first.
    pub ceiling_kb: u64,
    /// RocksDB block-cache capacity, shared by every column family.
    pub block_cache_bytes: usize,
    /// Recent-UTXO cache capacity. `0` disables it.
    pub recent_cache_bytes: usize,
    /// Cap passed to `set_db_write_buffer_size`, bounding all memtables.
    pub memtable_budget_bytes: usize,
    /// Maximum number of decoded blk files the shared LRU may hold.
    pub file_cache_files: usize,
}

impl MemoryPlan {
    /// The cache-only view, for callers that do not care about the rest.
    pub fn caches(&self) -> CacheBudget {
        CacheBudget {
            block_cache_bytes: self.block_cache_bytes,
            recent_cache_bytes: self.recent_cache_bytes,
        }
    }

    /// RSS (KiB) at which the consumer should start shedding memory rather than
    /// waiting for the hard ceiling. [`SOFT_PCT`]% leaves a 20% runway for
    /// progressive relief steps and compaction bursts before the hard limit.
    pub fn soft_kb(&self) -> u64 {
        self.ceiling_kb / 100 * SOFT_PCT
    }
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

/// Plan the whole memory layout from the host's available and total memory.
///
/// The ceiling is `available - 10% - runtime_reserve` (the same number the
/// watchdog aborts at). Beneath it we account, in order, for the memtable
/// budget, the decoded-file LRU (scaled to ~10% of the ceiling), and a runtime
/// reserve; the caches then take [`CACHE_FRACTION_PCT`] of what remains, so the
/// spike margin is guaranteed rather than hoped for.
///
/// Every overhead is *capped by a fraction of the ceiling*, not just by its own
/// absolute constant. On a big box the caps never bind and the overheads are the
/// full `MEMTABLE_BUDGET_KB` / `FIXED_RUNTIME_RESERVE_KB`; on a small box they
/// scale down so the fixed reserves cannot themselves exceed the ceiling (which
/// is what the old fixed constants did — on a 4 GiB host the 2 GiB memtable
/// reserve alone was larger than the whole ceiling). `total` only bounds the
/// result so a bogus `MemAvailable` (some kernels report more available than
/// total) cannot produce a budget larger than the machine.
pub fn plan_memory(available_kb: u64, total_kb: u64) -> MemoryPlan {
    // A bogus `MemAvailable` above `MemTotal` (seen on some kernels) is clamped
    // to the physical machine. A zero `MemTotal` is ignored rather than used as
    // the cap, so a missing field cannot collapse the budget to nothing.
    let cap = if total_kb == 0 { available_kb } else { total_kb };
    let available_kb = available_kb.min(cap);

    let ceiling_kb = available_kb
        .saturating_sub(available_kb / RESERVE_FRACTION_DENOM)
        .saturating_sub(RUNTIME_RESERVE_KB)
        .max(MIN_CEILING_KB);

    // The file LRU is capped at ~1/16th of the ceiling and clamped to
    // `[1, MAX_FILE_CACHE_FILES]` so a tiny host still caches at least one file
    // and a large one does not pin gigabytes of decoded blk data.
    let file_cache_files =
        ((ceiling_kb / 16) / DECODED_FILE_KB).clamp(1, MAX_FILE_CACHE_FILES as u64) as usize;
    let file_cache_kb = file_cache_files as u64 * DECODED_FILE_KB;

    // Overheads: the absolute budget, but never more than a fraction of the
    // ceiling (see the doc comment). The fractions are chosen so the sum stays
    // below `ceiling - cache_margin` on every host size.
    let memtable_kb = MEMTABLE_BUDGET_KB.min(ceiling_kb / 4);
    let fixed_kb = FIXED_RUNTIME_RESERVE_KB.min(ceiling_kb / 8);
    let overhead_kb = memtable_kb + fixed_kb + file_cache_kb;

    let cache_kb = ceiling_kb.saturating_sub(overhead_kb) * CACHE_FRACTION_PCT / 100;
    let recent_kb = cache_kb * RECENT_SHARE_PCT / 100;
    let block_kb = cache_kb - recent_kb;

    MemoryPlan {
        ceiling_kb,
        block_cache_bytes: kb_to_bytes(block_kb),
        recent_cache_bytes: kb_to_bytes(recent_kb),
        memtable_budget_bytes: kb_to_bytes(memtable_kb),
        file_cache_files,
    }
}

/// Plan the cache split from the host's available and total memory.
///
/// Thin wrapper over [`plan_memory`] kept for callers that only need the two
/// cache sizes; it returns exactly the caches `plan_memory` would.
pub fn plan_cache_budget(available_kb: u64, total_kb: u64) -> CacheBudget {
    plan_memory(available_kb, total_kb).caches()
}

/// Ask the system allocator to return unused heap pages to the OS.
///
/// Under multi-threaded workloads (Rayon parse workers + RocksDB background
/// flush/compaction threads), glibc's `ptmalloc2` retains freed chunks in
/// per-thread arenas rather than unmapping them, so `VmRSS` can stay gigabytes
/// above live Rust/C++ allocations even after caches evict or temporary
/// header-pass maps drop. Calling `malloc_trim(0)` at coarse lifecycle
/// boundaries (before planning memory after the header pass, between bench
/// trials, and when the watchdog sheds cache memory) forces `madvise` on free
/// arena pages so the kernel — and the RSS watchdog — see reclaimed RAM.
pub fn trim_allocator() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: `malloc_trim(0)` is thread-safe in glibc and only releases
    // completely free pages from allocator arenas back to the kernel.
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Plan the whole memory layout from the live host. Thin wrapper over
/// [`plan_memory`] so callers (the CLI) have one obvious entry point.
///
/// Trims allocator arenas first so any slack left behind by the preceding
/// header pass (or an earlier benchmark trial) is returned to `MemAvailable`
/// before the snapshot is read.
pub fn plan_memory_for_host() -> MemoryPlan {
    trim_allocator();
    plan_memory(mem_available_kb(), mem_total_kb())
}

/// Convert KiB to bytes, saturating rather than overflowing on absurd inputs.
fn kb_to_bytes(kb: u64) -> usize {
    kb.saturating_mul(1024).min(usize::MAX as u64) as usize
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
        // 32 GiB available, 32 GiB total: ceiling = 32 - 4 - 2 = 26 GiB,
        // file LRU = clamp(26/16 GiB / 128 MiB, 1, 8) = 8 files = 1 GiB,
        // overhead = 2 + 2 + 1 = 5 GiB, cache = (26 - 5) * 75% = 15.75 GiB,
        // recent = 60% = 9.45 GiB, block = 6.30 GiB. Assert the exact
        // arithmetic rather than a range so a change to any constant is caught.
        let avail = 32 * 1024 * 1024; // KiB
        let total = 32 * 1024 * 1024;
        let p = plan_memory(avail, total);
        let ceiling_kb = avail - avail / RESERVE_FRACTION_DENOM - RUNTIME_RESERVE_KB;
        assert_eq!(p.ceiling_kb, ceiling_kb);
        let file_kb = p.file_cache_files as u64 * DECODED_FILE_KB;
        // On a 32 GiB ceiling neither overhead cap binds, so the overheads are
        // the full constants.
        let memtable_kb = MEMTABLE_BUDGET_KB.min(ceiling_kb / 4);
        let fixed_kb = FIXED_RUNTIME_RESERVE_KB.min(ceiling_kb / 8);
        assert_eq!(memtable_kb, MEMTABLE_BUDGET_KB);
        assert_eq!(fixed_kb, FIXED_RUNTIME_RESERVE_KB);
        let cache_kb =
            (ceiling_kb - (memtable_kb + fixed_kb + file_kb)) * CACHE_FRACTION_PCT / 100;
        let recent_kb = cache_kb * RECENT_SHARE_PCT / 100;
        assert_eq!(p.recent_cache_bytes, (recent_kb * 1024) as usize);
        assert_eq!(p.block_cache_bytes, ((cache_kb - recent_kb) * 1024) as usize);
        // The recent cache gets the larger share by construction.
        assert!(p.recent_cache_bytes > p.block_cache_bytes);
        assert_eq!(p.memtable_budget_bytes, (MEMTABLE_BUDGET_KB * 1024) as usize);
        assert_eq!(p.file_cache_files, MAX_FILE_CACHE_FILES);
    }

    /// The load-bearing invariant: everything the plan budgets for must fit
    /// under the ceiling with the spike margin intact. This is the property
    /// whose absence let the watchdog trip at the allocation-failure point and
    /// segfault RocksDB.
    #[test]
    fn plan_fits_under_the_ceiling_with_a_spike_margin() {
        for avail_gib in [4u64, 8, 16, 24, 32, 64, 128] {
            let avail = avail_gib * 1024 * 1024;
            let p = plan_memory(avail, avail);
            let file_kb = p.file_cache_files as u64 * DECODED_FILE_KB;
            // Mirror the plan's own capped runtime reserve (it scales down on a
            // small host); the memtable cap is already in `memtable_budget_bytes`.
            let fixed_kb = FIXED_RUNTIME_RESERVE_KB.min(p.ceiling_kb / 8);
            let budgeted_kb = (p.block_cache_bytes as u64
                + p.recent_cache_bytes as u64
                + p.memtable_budget_bytes as u64)
                / 1024
                + file_kb
                + fixed_kb;
            assert!(
                budgeted_kb <= p.ceiling_kb,
                "avail={avail_gib}GiB: budgeted {budgeted_kb} KiB exceeds ceiling {} KiB",
                p.ceiling_kb
            );
            // And the margin is real, not a rounding artefact: at least 5% of
            // the ceiling stays unallocated.
            assert!(
                budgeted_kb <= p.ceiling_kb * 95 / 100,
                "avail={avail_gib}GiB: no spike margin"
            );
        }
    }

    #[test]
    fn plan_never_exceeds_available_and_is_zero_safe() {
        // A tiny machine: the ceiling clamps to the floor and the overheads
        // scale down with it, so the caches stay small rather than wrapping to a
        // huge number or the fixed reserves exceeding the ceiling.
        let p = plan_memory(256 * 1024, 256 * 1024);
        assert_eq!(p.ceiling_kb, MIN_CEILING_KB);
        assert!(p.block_cache_bytes < 256 * 1024 * 1024);
        assert!(p.recent_cache_bytes < 256 * 1024 * 1024);
        assert_eq!(p.file_cache_files, 1);

        // A bogus MemAvailable larger than MemTotal is clamped to MemTotal, so
        // the plan cannot exceed the physical machine.
        let bogus = plan_memory(999 * 1024 * 1024, 8 * 1024 * 1024);
        let clamped = plan_memory(8 * 1024 * 1024, 8 * 1024 * 1024);
        assert_eq!(bogus, clamped);
    }

    #[test]
    fn plan_scales_the_file_lru_with_the_ceiling() {
        // A small ceiling keeps a single decoded file...
        assert_eq!(plan_memory(4 * 1024 * 1024, 4 * 1024 * 1024).file_cache_files, 1);
        // ...a medium ceiling scales proportionally...
        assert_eq!(plan_memory(16 * 1024 * 1024, 16 * 1024 * 1024).file_cache_files, 6);
        // ...and a large one caps at MAX_FILE_CACHE_FILES rather than pinning gigabytes.
        assert_eq!(
            plan_memory(256 * 1024 * 1024, 256 * 1024 * 1024).file_cache_files,
            MAX_FILE_CACHE_FILES
        );
    }

    #[test]
    fn plan_at_the_default_fallback_is_conservative() {
        // The fallback (3 GiB available) must stay small: ceiling = 3 - 0.375 - 2
        // = 0.625 GiB, so the caches are well under the legacy 1 GiB block cache.
        let p = plan_memory(FALLBACK_AVAILABLE_KB, FALLBACK_AVAILABLE_KB);
        assert!(p.block_cache_bytes < 1024 * 1024 * 1024);
    }

    #[test]
    fn kb_to_bytes_saturates() {
        assert_eq!(kb_to_bytes(1), 1024);
        assert_eq!(kb_to_bytes(u64::MAX), usize::MAX);
    }

    #[test]
    fn soft_threshold_sits_below_the_ceiling() {
        let p = plan_memory(32 * 1024 * 1024, 32 * 1024 * 1024);
        assert!(p.soft_kb() < p.ceiling_kb);
        assert_eq!(p.soft_kb(), p.ceiling_kb / 100 * SOFT_PCT);
        const { assert!(RECOVER_PCT < SOFT_PCT) };
    }

    #[test]
    fn trim_allocator_preserves_live_heap_allocations() {
        let live: Vec<u64> = (0..16_384).collect();
        let transient: Vec<u8> = vec![0xA5; 4 << 20];
        drop(transient);
        trim_allocator();
        assert_eq!(live.len(), 16_384);
        assert_eq!(live[0], 0);
        assert_eq!(live[16_383], 16_383);
    }

    #[test]
    fn cache_budget_wrapper_matches_the_plan() {
        let p = plan_memory(16 * 1024 * 1024, 16 * 1024 * 1024);
        assert_eq!(plan_cache_budget(16 * 1024 * 1024, 16 * 1024 * 1024), p.caches());
    }

    #[test]
    fn plan_for_host_returns_a_sane_split() {
        let p = plan_memory_for_host();
        // On any real host the ceiling is above the floor and the split is
        // ordered the same way as the pure function.
        assert!(p.ceiling_kb >= MIN_CEILING_KB);
        assert!(p.recent_cache_bytes >= p.block_cache_bytes);
        assert!(p.file_cache_files >= 1);
    }
}
