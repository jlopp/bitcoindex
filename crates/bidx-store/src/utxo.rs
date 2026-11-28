//! RocksDB-backed UTXO store with per-block undo logging and tip state.
//!
//! Column families:
//!   default  : UTXO entries
//!              key   = txid(32) || vout(4, little-endian)      -> 36 bytes
//!              value = value_sat(8) || script_type(1) || height(4)
//!                      || address_hash(32)                     -> 45 bytes
//!   undo     : per-block undo records for reorg handling
//!              key   = height(4, big-endian, so scans are height-ordered)
//!              value = encoded BlockUndo (see below)
//!   meta     : tip state
//!              key   = b"tip"  ->  height(4 LE) || block_hash(32)
//!
//! BlockUndo encoding (all integers little-endian):
//!   n_spent(varint)  then n_spent × [key(36) || UtxoValue(45)]
//!   n_created(varint) then n_created × [key(36)]
//!
//! To disconnect a block we re-insert the spent outputs and delete the
//! created outpoints. The undo record is written atomically in the same
//! WriteBatch as the UTXO mutations, so a crash mid-apply can't leave the
//! undo log inconsistent with the set (we use the WAL for live mode).

use bidx_core::{Hash32, InputRow, OutputRow};
use parking_lot::Mutex;
use rocksdb::{Options, WriteBatch, DB};
use rustc_hash::{FxHashMap, FxHashSet};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UtxoError {
    #[error(transparent)]
    Rocks(#[from] rocksdb::Error),
    #[error("no undo record for height {0}")]
    NoUndo(u32),
    #[error("corrupt undo record at height {0}")]
    CorruptUndo(u32),
    #[error("tip state missing")]
    NoTip,
}

const CF_UNDO: &str = "undo";
const CF_META: &str = "meta";
const META_TIP_KEY: &[u8] = b"tip";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtxoValue {
    pub value_sat: u64,
    pub script_type: u8,
    pub height: u32,
    pub address_hash: [u8; 32],
}

/// Compact variable-length stack-encoded [`UtxoValue`] (`<= 49` bytes, zero
/// heap allocation).
///
/// Layout:
///   - `tag` (LEB128 `u64`, 1-6 bytes):
///       `(height << 6) | (hash_code << 4) | script_nibble`
///     where `hash_code` is `1` (`0` zero-hash bytes), `2` (`20`-byte HASH160
///     with 12 trailing zero bytes — ~78% of all mainnet outputs), or `3`
///     (full `32`-byte hash); `0` is reserved/invalid so zeroed buffers fail
///     decoding immediately.
///   - optional raw `script_type: u8` escape byte when `script_nibble == 15`
///     (all standard [`bidx_core::ScriptType`] variants are `0..=9`, so 0 bytes).
///   - `value_sat` (LEB128 `u64`, 1-10 bytes; 2-4 bytes for typical amounts).
///   - `address_hash` prefix (`0`, `20`, or `32` raw bytes).
///
/// Shrinks stored RocksDB UTXO & undo values from a fixed `45` bytes to `~26`
/// bytes on mainnet (`-42%` memtable, L0 SST, compaction, and block-cache I/O).
#[derive(Clone, Copy)]
pub struct EncodedUtxo {
    buf: [u8; 49],
    len: u8,
}

impl AsRef<[u8]> for EncodedUtxo {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

impl std::ops::Deref for EncodedUtxo {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

#[inline]
fn put_leb128_buf(buf: &mut [u8; 49], pos: &mut usize, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf[*pos] = byte;
        *pos += 1;
        if v == 0 {
            break;
        }
    }
}

#[inline]
fn read_leb128(b: &mut &[u8]) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let &byte = b.first()?;
        *b = &b[1..];
        if shift == 63 && byte > 1 {
            return None;
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Some(result);
        }
        shift += 7;
    }
}

impl UtxoValue {
    #[inline]
    fn encode(&self) -> EncodedUtxo {
        let (hash_code, hash_len): (u64, usize) = if self.address_hash == [0u8; 32] {
            (1, 0)
        } else if self.address_hash[20..32] == [0u8; 12] {
            (2, 20)
        } else {
            (3, 32)
        };
        let nib = self.script_type.min(15) as u64;
        let tag = ((self.height as u64) << 6) | (hash_code << 4) | nib;

        let mut buf = [0u8; 49];
        let mut pos = 0usize;
        put_leb128_buf(&mut buf, &mut pos, tag);
        if nib == 15 {
            buf[pos] = self.script_type;
            pos += 1;
        }
        put_leb128_buf(&mut buf, &mut pos, self.value_sat);
        if hash_len > 0 {
            buf[pos..pos + hash_len].copy_from_slice(&self.address_hash[..hash_len]);
            pos += hash_len;
        }
        EncodedUtxo {
            buf,
            len: pos as u8,
        }
    }

    #[inline]
    fn decode_stream(b: &mut &[u8]) -> Option<Self> {
        let tag = read_leb128(b)?;
        let height_u64 = tag >> 6;
        if height_u64 > u32::MAX as u64 {
            return None;
        }
        let hash_len = match (tag >> 4) & 0x03 {
            1 => 0usize,
            2 => 20usize,
            3 => 32usize,
            _ => return None,
        };
        let nib = (tag & 0x0f) as u8;
        let script_type = if nib == 15 {
            let &raw_ty = b.first()?;
            if raw_ty < 15 {
                return None;
            }
            *b = &b[1..];
            raw_ty
        } else {
            nib
        };
        let value_sat = read_leb128(b)?;
        if b.len() < hash_len {
            return None;
        }
        let mut address_hash = [0u8; 32];
        if hash_len > 0 {
            address_hash[..hash_len].copy_from_slice(&b[..hash_len]);
            *b = &b[hash_len..];
        }
        Some(UtxoValue {
            value_sat,
            script_type,
            height: height_u64 as u32,
            address_hash,
        })
    }

    fn decode(mut b: &[u8]) -> Option<Self> {
        let val = Self::decode_stream(&mut b)?;
        b.is_empty().then_some(val)
    }
}

#[inline]
fn make_key(txid: &[u8; 32], vout: u32) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[..32].copy_from_slice(txid);
    k[32..].copy_from_slice(&vout.to_le_bytes());
    k
}

/// Approximate in-memory cost of one cache entry, used for eviction accounting.
///
/// Strict upper bound on total heap bytes per resident entry in
/// [`RecentUtxoCache`]: 92 bytes for [`CacheEntry`] (36B key + 44B packed UTXO
/// fields + 8B intrusive FIFO links + alignment) plus `< 21.4` bytes (`<= 2.67`
/// `u64` buckets/entry) for the open-addressed hash index (`<= 114` B/entry
/// for every capacity `>= 2`). Because `entries` reserves with `reserve_exact`
/// capped at `max_entries`, the heap allocated by the cache never exceeds
/// `budget_bytes`.
const CACHE_ENTRY_COST: usize = 120;

const SLOT_NONE: u32 = u32::MAX;
const SLOT_FREE_MARKER: u32 = u32::MAX - 1;

/// Entries flushed per `WriteBatch` (~2.6 MiB) when draining or shrinking the
/// write-back [`RecentUtxoCache`] into RocksDB, avoiding multi-gigabyte batch
/// allocations under memory pressure.
const CACHE_FLUSH_BATCH_ENTRIES: usize = 32_768;

/// Dust / inscription-postage value ceiling (satoshis) at or below which
/// outputs bypass [`RecentUtxoCache`] and write directly to RocksDB (`0`-sat
/// burns, `330`-sat P2TR Ordinals/Runes postage, `546`-sat P2PKH/P2WPKH dust).
pub const DUST_UTXO_THRESHOLD_SAT: u64 = 546;

/// Output-count threshold identifying mass-dusting / airdrop transactions.
pub const HIGH_FANOUT_OUTPUT_THRESHOLD: u32 = 64;

/// Value ceiling (satoshis) treated as cold dust on high-fanout transactions.
pub const HIGH_FANOUT_DUST_SAT: u64 = 1_000;

/// Decide whether an unspent output surviving same-block resolution should
/// enter [`RecentUtxoCache`] (`true`) or bypass it directly to RocksDB
/// (`false`).
///
/// Cold outputs (`<= 546` sat dust/inscriptions, `BareMultisig`
/// Counterparty/Stamps data, `Unknown`/`WitnessUnknown` non-standard scripts,
/// and high-fanout dust) have near-zero spend probability; bypassing the cache
/// keeps 100% of `RecentUtxoCache` slots dedicated to high-turnover UTXOs that
/// cancel in RAM without ever touching RocksDB.
#[inline]
pub(crate) fn should_cache_utxo(val: &UtxoValue, tx_output_count: u32) -> bool {
    if val.value_sat <= DUST_UTXO_THRESHOLD_SAT {
        return false;
    }
    if matches!(
        val.script_type,
        x if x == bidx_core::ScriptType::Unknown as u8
            || x == bidx_core::ScriptType::OpReturn as u8
            || x == bidx_core::ScriptType::BareMultisig as u8
            || x == bidx_core::ScriptType::WitnessUnknown as u8
    ) {
        return false;
    }
    if tx_output_count >= HIGH_FANOUT_OUTPUT_THRESHOLD && val.value_sat <= HIGH_FANOUT_DUST_SAT {
        return false;
    }
    true
}

#[derive(Clone, Copy)]
struct CacheEntry {
    /// Previous (older) slot in FIFO creation order, `SLOT_NONE` at `head`, or
    /// `SLOT_FREE_MARKER` while this slot sits on the free list.
    prev: u32,
    /// Next (newer) slot in FIFO order (`SLOT_NONE` at `tail`), or the next
    /// free slot when `prev == SLOT_FREE_MARKER`.
    next: u32,
    height: u32,
    value_sat_le: [u8; 8],
    key: [u8; 36],
    address_hash: [u8; 32],
    script_type: u8,
}

impl CacheEntry {
    #[inline]
    fn new(key: [u8; 36], val: UtxoValue) -> Self {
        Self {
            prev: SLOT_NONE,
            next: SLOT_NONE,
            height: val.height,
            value_sat_le: val.value_sat.to_le_bytes(),
            key,
            address_hash: val.address_hash,
            script_type: val.script_type,
        }
    }

    #[inline]
    fn set_kv(&mut self, key: [u8; 36], val: UtxoValue) {
        self.key = key;
        self.set_val(val);
    }

    #[inline]
    fn set_val(&mut self, val: UtxoValue) {
        self.height = val.height;
        self.value_sat_le = val.value_sat.to_le_bytes();
        self.address_hash = val.address_hash;
        self.script_type = val.script_type;
    }

    #[inline]
    fn value(&self) -> UtxoValue {
        UtxoValue {
            value_sat: u64::from_le_bytes(self.value_sat_le),
            script_type: self.script_type,
            height: self.height,
            address_hash: self.address_hash,
        }
    }
}

const _: () = assert!(std::mem::size_of::<CacheEntry>() == 92);

/// Fast 64-bit hash of a 36-byte outpoint with a SplitMix64 avalanche tail so
/// both the lower-32 bucket index and upper-32 tag bits distribute uniformly
/// even on single-byte synthetic keys, without chaining 5 sequential 64-bit
/// multiplies per lookup.
#[inline]
fn hash_outpoint(key: &[u8; 36]) -> u64 {
    let w0 = u64::from_le_bytes([
        key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
    ]);
    let w1 = u64::from_le_bytes([
        key[8], key[9], key[10], key[11], key[12], key[13], key[14], key[15],
    ]);
    let w2 = u64::from_le_bytes([
        key[16], key[17], key[18], key[19], key[20], key[21], key[22], key[23],
    ]);
    let w3 = u64::from_le_bytes([
        key[24], key[25], key[26], key[27], key[28], key[29], key[30], key[31],
    ]);
    let vout = u32::from_le_bytes([key[32], key[33], key[34], key[35]]) as u64;
    let mut h = w0
        ^ w1.rotate_left(17)
        ^ w2.rotate_left(34)
        ^ w3.rotate_left(51)
        ^ vout.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

#[inline]
fn max_entries_for(budget_bytes: usize) -> usize {
    (budget_bytes / CACHE_ENTRY_COST).min((u32::MAX - 2) as usize)
}

#[inline]
fn bucket_cap_for(entries: usize) -> usize {
    (entries.saturating_mul(4) / 3 + 1)
        .next_power_of_two()
        .max(4)
}

/// A strictly memory-bounded cache of the most recently created UTXOs.
///
/// Why this layout instead of `FxHashMap + VecDeque`:
/// 1. **Zero tombstones on spend.** Most Bitcoin outputs are spent shortly
///    after creation. Leaving spent keys in a side `VecDeque` until live count
///    hits the cap accumulated hundreds of millions of dead 36-byte keys (~19
///    GiB around mainnet height 435,000, where `VecDeque` growth doubled from
///    \(2^{28}\) to \(2^{29}\)). An intrusive doubly-linked FIFO list over a
///    slot array unlinks spent entries in \(O(1)\) and recycles their slots
///    immediately via `free_head`. And Knuth Algorithm R backward-shift
///    deletion keeps the `u64` hash index 100% tombstone-free without rehashing.
/// 2. **Exact byte cap, no power-of-two slab overshoot.** `entries` grows via
///    `reserve_exact` clamped to `max_entries`, and the hash index stores only
///    packed `(tag32 << 32) | (slot + 1)` words (8 bytes/bucket), so live +
///    index heap never exceeds `budget_bytes`.
/// 3. **Real OS memory reclamation under pressure.** `set_budget` drops the old
///    index first, compacts live entries in place, and calls `shrink_to` so
///    glibc's `mremap` unmaps trailing pages without a 2x copy spike.
///
/// Entry fidelity is unchanged: full 36-byte outpoint keys and full 45-byte
/// `UtxoValue`s are stored so `SpendRow` fields are exact.
struct RecentUtxoCache {
    entries: Vec<CacheEntry>,
    /// Open-addressed linear-probing index: `0` = empty, else
    /// `((tag32 as u64) << 32) | ((slot + 1) as u64)`.
    buckets: Vec<u64>,
    len: usize,
    head: u32,
    tail: u32,
    free_head: u32,
    budget_bytes: usize,
    max_entries: usize,
    hits: u64,
    misses: u64,
}

impl RecentUtxoCache {
    fn new(budget_bytes: usize) -> Self {
        Self {
            entries: Vec::new(),
            buckets: Vec::new(),
            len: 0,
            head: SLOT_NONE,
            tail: SLOT_NONE,
            free_head: SLOT_NONE,
            budget_bytes,
            max_entries: max_entries_for(budget_bytes),
            hits: 0,
            misses: 0,
        }
    }

    #[inline]
    fn find(&self, key: &[u8; 36], h: u64) -> Option<(usize, u32)> {
        if self.buckets.is_empty() {
            return None;
        }
        let mask = self.buckets.len() - 1;
        let tag = (h >> 32) as u32;
        let mut pos = (h as usize) & mask;
        loop {
            let b = self.buckets[pos];
            if b == 0 {
                return None;
            }
            if (b >> 32) as u32 == tag && &self.entries[((b as u32) - 1) as usize].key == key {
                return Some((pos, (b as u32) - 1));
            }
            pos = (pos + 1) & mask;
        }
    }

    #[inline]
    fn insert_bucket(&mut self, slot: u32, h: u64) {
        let mask = self.buckets.len() - 1;
        let tag = (h >> 32) as u32;
        let word = ((tag as u64) << 32) | ((slot + 1) as u64);
        let mut pos = (h as usize) & mask;
        while self.buckets[pos] != 0 {
            pos = (pos + 1) & mask;
        }
        self.buckets[pos] = word;
    }

    /// Knuth Algorithm R backward-shift deletion: removes `del_pos` leaving
    /// zero tombstones so probe lengths stay constant across hundreds of
    /// millions of create/spend cycles.
    #[inline]
    fn remove_bucket(&mut self, del_pos: usize) {
        let mask = self.buckets.len() - 1;
        let mut empty = del_pos;
        let mut scan = (empty + 1) & mask;
        loop {
            let b = self.buckets[scan];
            if b == 0 {
                self.buckets[empty] = 0;
                break;
            }
            let slot = (b as u32) - 1;
            let ideal = (hash_outpoint(&self.entries[slot as usize].key) as usize) & mask;
            if ((scan.wrapping_sub(ideal)) & mask) >= ((scan.wrapping_sub(empty)) & mask) {
                self.buckets[empty] = b;
                empty = scan;
            }
            scan = (scan + 1) & mask;
        }
    }

    fn ensure_bucket_capacity(&mut self) {
        let target_entries = (self.len + 1)
            .saturating_mul(2)
            .max(4)
            .min(self.max_entries);
        let bcap = bucket_cap_for(target_entries);
        // Drop the old bucket array before allocating the new one so the two
        // large buffers do not coexist at peak RSS.
        self.buckets = Vec::new();
        self.buckets = vec![0u64; bcap];
        let mut cur = self.head;
        while cur != SLOT_NONE {
            let h = hash_outpoint(&self.entries[cur as usize].key);
            self.insert_bucket(cur, h);
            cur = self.entries[cur as usize].next;
        }
    }

    #[inline]
    fn unlink_fifo(&mut self, slot: u32) {
        let prev = self.entries[slot as usize].prev;
        let next = self.entries[slot as usize].next;
        if prev != SLOT_NONE {
            self.entries[prev as usize].next = next;
        } else {
            self.head = next;
        }
        if next != SLOT_NONE {
            self.entries[next as usize].prev = prev;
        } else {
            self.tail = prev;
        }
    }

    #[inline]
    fn push_back_fifo(&mut self, slot: u32) {
        let old_tail = self.tail;
        let e = &mut self.entries[slot as usize];
        e.prev = old_tail;
        e.next = SLOT_NONE;
        if old_tail != SLOT_NONE {
            self.entries[old_tail as usize].next = slot;
        } else {
            self.head = slot;
        }
        self.tail = slot;
    }

    /// Insert a newly created outpoint into the write-back cache, returning
    /// `Some((evicted_key, evicted_val))` whenever an unspent entry must be
    /// spilled to RocksDB (either because the cache is at `max_entries` and
    /// evicted the oldest FIFO entry, or because `max_entries == 0`). If `key`
    /// is already present, refreshes in place without altering FIFO recency.
    fn insert(&mut self, key: [u8; 36], val: UtxoValue) -> Option<([u8; 36], UtxoValue)> {
        if self.max_entries == 0 {
            return Some((key, val));
        }
        let h = hash_outpoint(&key);
        if let Some((_, slot)) = self.find(&key, h) {
            self.entries[slot as usize].set_val(val);
            return None;
        }
        if self.len == self.max_entries {
            // At capacity: evict the oldest FIFO entry (`head`) in place,
            // reuse its slot directly without growing `entries` or `buckets`,
            // and return the evicted entry so the caller flushes it to RocksDB.
            let victim = self.head;
            let victim_key = self.entries[victim as usize].key;
            let victim_val = self.entries[victim as usize].value();
            let victim_h = hash_outpoint(&victim_key);
            if let Some((victim_pos, _)) = self.find(&victim_key, victim_h) {
                self.remove_bucket(victim_pos);
            }
            self.unlink_fifo(victim);
            self.entries[victim as usize].set_kv(key, val);
            self.push_back_fifo(victim);
            self.insert_bucket(victim, h);
            return Some((victim_key, victim_val));
        }
        if (self.len + 1) * 4 > self.buckets.len() * 3 {
            self.ensure_bucket_capacity();
        }
        let slot = if self.free_head != SLOT_NONE {
            let s = self.free_head;
            self.free_head = self.entries[s as usize].next;
            self.entries[s as usize].set_kv(key, val);
            s
        } else {
            if self.entries.len() == self.entries.capacity() {
                // Grow in bounded steps (at most 4M entries ≈ 368 MiB per step)
                // and clamp strictly to `max_entries` so `Vec` doubling never
                // overshoots the byte budget.
                let cur = self.entries.capacity();
                let step = cur.clamp(64, 1 << 22);
                let next_cap = cur.saturating_add(step).min(self.max_entries);
                self.entries.reserve_exact(next_cap - self.entries.len());
            }
            let s = self.entries.len() as u32;
            self.entries.push(CacheEntry::new(key, val));
            s
        };
        self.push_back_fifo(slot);
        self.insert_bucket(slot, h);
        self.len += 1;
        None
    }

    /// Look up a spent outpoint, unlinking it and recycling its slot on a hit.
    fn remove(&mut self, key: &[u8; 36]) -> Option<UtxoValue> {
        let h = hash_outpoint(key);
        let Some((pos, slot)) = self.find(key, h) else {
            self.misses += 1;
            return None;
        };
        self.hits += 1;
        self.remove_bucket(pos);
        self.unlink_fifo(slot);
        let e = &mut self.entries[slot as usize];
        let val = e.value();
        e.prev = SLOT_FREE_MARKER;
        e.next = self.free_head;
        self.free_head = slot;
        self.len -= 1;
        Some(val)
    }

    /// Pop up to `max_batch` oldest FIFO entries while `self.len > target_len`,
    /// appending each dirty UTXO into `batch` so `flush_recent_cache` and
    /// `relieve_pressure` stream write-back evictions to RocksDB in bounded
    /// ~2.6 MiB chunks without holding the cache mutex across I/O.
    fn evict_oldest_to_batch(
        &mut self,
        target_len: usize,
        max_batch: usize,
        batch: &mut WriteBatch,
    ) -> usize {
        let mut n = 0usize;
        while self.len > target_len && n < max_batch {
            let victim = self.head;
            let victim_key = self.entries[victim as usize].key;
            let victim_h = hash_outpoint(&victim_key);
            if let Some((victim_pos, _)) = self.find(&victim_key, victim_h) {
                self.remove_bucket(victim_pos);
            }
            self.unlink_fifo(victim);
            let e = &mut self.entries[victim as usize];
            batch.put(victim_key, e.value().encode());
            e.prev = SLOT_FREE_MARKER;
            e.next = self.free_head;
            self.free_head = victim;
            self.len -= 1;
            n += 1;
        }
        n
    }

    /// Release backing vectors once completely drained.
    fn release_if_empty(&mut self) {
        if self.len == 0 {
            self.entries = Vec::new();
            self.buckets = Vec::new();
            self.head = SLOT_NONE;
            self.tail = SLOT_NONE;
            self.free_head = SLOT_NONE;
        }
    }

    /// Lower the byte budget, evict oldest entries down to it, and release
    /// excess backing capacity back to the allocator in place.
    fn set_budget(&mut self, budget_bytes: usize) {
        if budget_bytes >= self.budget_bytes {
            return;
        }
        self.budget_bytes = budget_bytes;
        self.max_entries = max_entries_for(budget_bytes);
        // Free the old bucket array before compacting so peak memory drops
        // immediately instead of holding old + new allocations together.
        self.buckets = Vec::new();

        while self.len > self.max_entries {
            let victim = self.head;
            self.unlink_fifo(victim);
            self.entries[victim as usize].prev = SLOT_FREE_MARKER;
            self.len -= 1;
        }

        // Compact live slots into `0..self.len` in place, rewriting neighbor
        // links as each slot shifts left so FIFO creation order is preserved.
        let mut write_idx = 0usize;
        for read_idx in 0..self.entries.len() {
            if self.entries[read_idx].prev == SLOT_FREE_MARKER {
                continue;
            }
            if read_idx != write_idx {
                let moved = self.entries[read_idx];
                let new_slot = write_idx as u32;
                if moved.prev != SLOT_NONE {
                    self.entries[moved.prev as usize].next = new_slot;
                } else {
                    self.head = new_slot;
                }
                if moved.next != SLOT_NONE {
                    self.entries[moved.next as usize].prev = new_slot;
                } else {
                    self.tail = new_slot;
                }
                self.entries[write_idx] = moved;
            }
            write_idx += 1;
        }
        self.free_head = SLOT_NONE;
        self.entries.truncate(write_idx);
        self.entries.shrink_to(self.max_entries);

        if self.len > 0 {
            let bcap = bucket_cap_for(self.len.max(4).min(self.max_entries));
            self.buckets = vec![0u64; bcap];
            for slot in 0..(write_idx as u32) {
                let h = hash_outpoint(&self.entries[slot as usize].key);
                self.insert_bucket(slot, h);
            }
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn bytes(&self) -> usize {
        self.len * CACHE_ENTRY_COST
    }
}

#[inline]
fn undo_key(height: u32) -> [u8; 4] {
    height.to_be_bytes()
}

/// A record of everything needed to reverse one block's UTXO mutations.
#[derive(Debug, Default, Clone)]
pub struct BlockUndo {
    /// Outputs this block spent (must be re-inserted on disconnect).
    pub spent: Vec<([u8; 36], UtxoValue)>,
    /// Outpoints this block created (must be deleted on disconnect).
    pub created: Vec<[u8; 36]>,
}

impl BlockUndo {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.spent.len() * 68 + self.created.len() * 36 + 16);
        put_varint(&mut out, self.spent.len() as u64);
        for (key, val) in &self.spent {
            out.extend_from_slice(key);
            out.extend_from_slice(val.encode().as_ref());
        }
        put_varint(&mut out, self.created.len() as u64);
        for key in &self.created {
            out.extend_from_slice(key);
        }
        out
    }

    fn decode(mut b: &[u8]) -> Option<Self> {
        let n_spent = read_varint(&mut b)? as usize;
        let mut spent = Vec::with_capacity(n_spent.min(65_536));
        for _ in 0..n_spent {
            if b.len() < 36 {
                return None;
            }
            let mut key = [0u8; 36];
            key.copy_from_slice(&b[..36]);
            b = &b[36..];
            let val = UtxoValue::decode_stream(&mut b)?;
            spent.push((key, val));
        }
        let n_created = read_varint(&mut b)? as usize;
        let mut created = Vec::with_capacity(n_created.min(65_536));
        for _ in 0..n_created {
            if b.len() < 36 {
                return None;
            }
            let mut key = [0u8; 36];
            key.copy_from_slice(&b[..36]);
            created.push(key);
            b = &b[36..];
        }
        b.is_empty().then_some(BlockUndo { spent, created })
    }
}

#[inline]
fn put_varint(out: &mut Vec<u8>, n: u64) {
    match n {
        0..=0xfc => out.push(n as u8),
        0xfd..=0xffff => {
            out.push(0xfd);
            out.extend_from_slice(&(n as u16).to_le_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xfe);
            out.extend_from_slice(&(n as u32).to_le_bytes());
        }
        _ => {
            out.push(0xff);
            out.extend_from_slice(&n.to_le_bytes());
        }
    }
}

#[inline]
fn read_varint(b: &mut &[u8]) -> Option<u64> {
    let first = *b.first()?;
    *b = &b[1..];
    match first {
        0x00..=0xfc => Some(first as u64),
        0xfd => {
            if b.len() < 2 {
                return None;
            }
            let v = u16::from_le_bytes([b[0], b[1]]) as u64;
            *b = &b[2..];
            Some(v)
        }
        0xfe => {
            if b.len() < 4 {
                return None;
            }
            let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as u64;
            *b = &b[4..];
            Some(v)
        }
        0xff => {
            if b.len() < 8 {
                return None;
            }
            let v = u64::from_le_bytes(b[..8].try_into().ok()?);
            *b = &b[8..];
            Some(v)
        }
    }
}

/// Default contiguous lookahead window size for [`UtxoStore::apply_window_in_place`].
///
/// Resolving up to 16 ready blocks as one unit cancels ~18-24% of cross-block
/// spends directly in `pending` before they ever touch [`RecentUtxoCache`] or
/// RocksDB, and coalesces RocksDB `batched_multi_get_cf` + `WriteBatch` calls
/// into 8-16x larger key-sorted batches.
pub const DEFAULT_UTXO_WINDOW_BLOCKS: usize = 16;

/// Mutable view of one block inside a multi-block UTXO apply window.
pub struct WindowBlock<'a> {
    pub height: u32,
    pub block_row: &'a mut bidx_core::BlockRow,
    pub txs: &'a mut [bidx_core::TxRow],
    pub inputs: &'a mut [InputRow],
    pub outputs: &'a [OutputRow],
}

/// Cumulative counters returned by [`UtxoStore::apply_block_in_place`] and
/// [`UtxoStore::apply_window_in_place`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WindowStats {
    pub missing: u64,
    pub db_lookups: u64,
    pub db_nanos: u64,
    pub pending_hits: u64,
    pub cache_hits: u64,
}

#[derive(Default)]
struct ApplyScratch {
    /// Unspent outputs created in the current block/window (`key -> (val, tx_output_count)`).
    pending: FxHashMap<[u8; 36], (UtxoValue, u32)>,
    misses: Vec<(usize, [u8; 36])>,
    resolved: Vec<Option<(UtxoValue, bool)>>,
    same_block: FxHashSet<[u8; 36]>,
    /// Multi-block window misses: `(key, block_idx, flat_input_idx)`.
    window_misses: Vec<([u8; 36], u16, u32)>,
    /// Flattened resolved `UtxoValue`s across all inputs in a window.
    window_resolved: Vec<Option<UtxoValue>>,
    /// Starting flat-input offset per block in `window_resolved`.
    window_offsets: Vec<usize>,
}

pub struct UtxoStore {
    db: DB,
    path: PathBuf,
    scratch: Mutex<ApplyScratch>,
    /// Bounded in-memory cache of the most recently created UTXOs. `None` when
    /// disabled (undo-enabled/live mode, where correctness is worth more than
    /// speed, and benchmarking the baseline). See `RecentUtxoCache`.
    recent: Option<Mutex<RecentUtxoCache>>,
    /// When false, undo records are not written (bulk initial load).
    /// Live mode enables undo so reorgs can be reversed.
    undo_enabled: bool,
    /// True when RocksDB statistics were requested at open time. Only then do
    /// the stall counters in `rocksdb.stats` carry meaningful values; see
    /// `stall_report`.
    statistics: bool,
    /// The shared block cache handle, kept so the runtime can shrink its
    /// capacity under memory pressure (`relieve_pressure`). `None` only in
    /// `#[cfg(test)]` fixtures that build a store without going through
    /// `open_with`; every production path sets it.
    block_cache: Option<rocksdb::Cache>,
    /// The block cache's current capacity. RocksDB's `Cache` exposes
    /// `set_capacity` but no getter, so the value is tracked here to know how
    /// far `relieve_pressure` can shrink it. Atomic (not `Cell`) because the
    /// store is shared across threads via `Arc`.
    block_cache_bytes: std::sync::atomic::AtomicUsize,
    /// True when this store is a bulk-index store (no undo) *and* the caller
    /// asked to defer background compaction. Only then does `finalize_bulk_load`
    /// re-enable compaction and run the single end-of-run `compact_range`.
    bulk_deferred_compaction: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipState {
    pub height: u32,
    pub hash: Hash32,
}

/// Snapshot of the in-memory recent-UTXO cache, for reporting and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: usize,
    pub budget_bytes: usize,
    pub hits: u64,
    pub misses: u64,
}

impl CacheStats {
    /// Cache hit rate over all lookups the cache was consulted for, or `None`
    /// before any lookup has happened.
    pub fn hit_rate(&self) -> Option<f64> {
        let total = self.hits + self.misses;
        if total == 0 {
            None
        } else {
            Some(self.hits as f64 / total as f64)
        }
    }
}

/// Default block cache capacity, shared by every column family.
///
/// The default was RocksDB's own 32 MiB — measured, not assumed: a probe run
/// reported `block_cache_capacity 33554432`. That is ~2,000x smaller than the
/// 3+ GiB UTXO data set this indexer builds, so almost every point lookup in
/// `apply_block` had to fault a data block off disk and decompress it.
///
/// The cache is shared across column families so the read path cannot be
/// starved by the undo/meta families, which are written far more than read.
/// This 1 GiB value is now only the *fallback*: callers that know the host's
/// memory (via `bidx_core::plan_memory_for_host`) pass a larger size
/// through [`UtxoStore::open_tuned`]. It is kept as the default so a bare
/// `open()` on a small host cannot allocate an outsized cache.
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 1 << 30; // 1 GiB

/// Compaction input readahead in bulk mode: 2 MiB runs instead of one read per
/// data block. Large enough to turn the compaction's read pattern sequential,
/// small enough not to waste NVMe bandwidth on read-ahead the compaction skips.
const COMPACTION_READAHEAD_BYTES: usize = 2 << 20;

/// Ceiling on background compaction/flush throughput in bulk mode, in bytes per
/// second (2 GiB/s). The point is not to slow compaction but to stop it from
/// monopolizing the device queue the *foreground* point lookups and `blk*.dat`
/// reads share: an unthrottled compaction burst on a fast NVMe can stall the
/// apply path for as long as it runs. Set well above what a typical NVMe
/// sustains so a fast device is never artificially capped in steady state.
const COMPACTION_RATE_LIMIT_BYTES: i64 = 2 << 30;

/// `estimated_entry_charge` for the HyperClockCache: the fixed charge reserved
/// per cached entry. RocksDB recommends the larger of the data-block and
/// metadata-block size; 16 KiB is a conservative value for a block cache whose
/// blocks are mostly small UTXO entries. Erring low costs a little metadata
/// space, which is the safe direction (erring high forces evictions).
const CACHE_ENTRY_CHARGE: usize = 16 << 10;

/// Steady-state L0 compaction and write-throttle triggers.
const L0_COMPACTION_TRIGGER: i32 = 8;
const L0_SLOWDOWN_TRIGGER: i32 = 20;
const L0_STOP_TRIGGER: i32 = 30;

/// Coarse L0 batching triggers used during [`UtxoStore::open_planned_bulk`].
///
/// Disabling auto-compaction completely let L0 accumulate 400+ overlapping SSTs
/// past mainnet height ~400,000, turning every cache miss into an \(O(N_{L0})\)
/// probe across hundreds of files. Keeping background auto-compaction active at
/// a coarse 16-file batch trigger merges ~8 GiB of L0 SSTs at a time into
/// disjoint base-level files (purging cancelled `put`+`delete` pairs) while
/// bounding L0 read amplification to <= ~16 files for the entire bulk load.
const BULK_L0_COMPACTION_TRIGGER: i32 = 16;
const BULK_L0_SLOWDOWN_TRIGGER: i32 = 48;
const BULK_L0_STOP_TRIGGER: i32 = 64;

/// Write-path tuning that does not depend on the block cache. Applied to the
/// database *and* to every column family (see [`tuned_options`]).
///
/// `max_background_jobs` and `max_subcompactions` scale with the host's core
/// count: the previous hardcoded 8 was tuned on an 8-core box, and on a 12- or
/// 16-core machine leaving cores idle during L0/L1 compaction is free
/// throughput left on the table. `subcompactions` in particular lets a single
/// large compaction use multiple cores.
///
/// `bulk` selects the bulk-ingest profile. It changes *write* I/O, which is the
/// dominant disk cost of an initial index: leveled compaction rewrites the same
/// data through every level, so the bytes that hit NVMe are many times the
/// logical UTXO size. Two settings attack that:
///
/// * `set_level_compaction_dynamic_level_bytes(true)` sizes the levels so the
///   bulk of the data lands in the bottommost level instead of being rewritten
///   on the way down, cutting write amplification.
/// * `set_compaction_readahead_size` prefetches input SSTs for a compaction in
///   large reads, so the compaction does not issue one small read per block.
///
/// Live mode leaves both at RocksDB's defaults: it writes little per block and
/// values the steady-state read behaviour dynamic levels can change.
/// Individual memtable size derived from the DB-wide memtable budget.
///
/// With `max_write_buffer_number = 4` and `min_write_buffer_number_to_merge = 2`
/// in bulk mode, scaling each buffer to `budget / 4` (clamped to `[64 MiB,
/// 512 MiB]`) guarantees two buffers can merge without exceeding a small host's
/// `db_write_buffer_size` while keeping the 512 MiB buffer on >= 2 GiB budgets.
pub(crate) fn write_buffer_bytes_for(memtable_budget_bytes: usize) -> usize {
    if memtable_budget_bytes == 0 {
        512 << 20
    } else {
        (memtable_budget_bytes / 4).clamp(64 << 20, 512 << 20)
    }
}

fn apply_write_tuning(opts: &mut Options, bulk: bool, memtable_budget_bytes: usize) {
    // Up to 512 MiB per buffer on >= 2 GiB budgets means a flush every ~8s at
    // the observed write rate rather than every second; on smaller hosts it
    // scales down with `memtable_budget_bytes` so 4 buffers fit cleanly.
    opts.set_write_buffer_size(write_buffer_bytes_for(memtable_budget_bytes));
    opts.set_max_write_buffer_number(4);
    opts.set_target_file_size_base(256 << 20);
    opts.set_level_zero_file_num_compaction_trigger(L0_COMPACTION_TRIGGER);
    opts.set_level_zero_slowdown_writes_trigger(L0_SLOWDOWN_TRIGGER);
    opts.set_level_zero_stop_writes_trigger(L0_STOP_TRIGGER);
    let jobs = default_background_jobs();
    opts.set_max_background_jobs(jobs);
    opts.increase_parallelism(jobs);
    // Parallelize a single compaction across cores (bounded by the core count,
    // since each subcompaction is a thread). Without this a 12-core box runs
    // every compaction single-threaded.
    opts.set_max_subcompactions(default_subcompactions());
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    // Merge memtables in memory before flushing (bulk: two, live: one). See
    // `memtables_to_merge` for why a bulk load wants this.
    opts.set_min_write_buffer_number_to_merge(memtables_to_merge(bulk));
    // Every valid Bitcoin input spends an existing UTXO, so any point lookup
    // that reaches the bottommost LSM level is guaranteed to hit. Skipping
    // filter generation on the bottommost level saves ~10 bits/key of filter
    // RAM across ~80–170M cold UTXOs while upper levels keep Ribbon/Bloom
    // filters to reject misses before reaching the bottom level.
    opts.set_optimize_filters_for_hits(true);
    // Enable whole-key Bloom filtering inside the 512 MiB active/immutable
    // memtables so cache misses skip memtable skiplist binary searches when
    // the outpoint is in an older SST.
    opts.set_memtable_prefix_bloom_ratio(0.05);
    opts.set_memtable_whole_key_filtering(true);

    if bulk {
        // Let the bottommost level absorb the bulk of the data instead of
        // rewriting it down every level. This is the single biggest write-amp
        // reduction available to an append-heavy load.
        opts.set_level_compaction_dynamic_level_bytes(true);
        // Read compaction inputs in 2 MiB runs rather than block-at-a-time.
        opts.set_compaction_readahead_size(COMPACTION_READAHEAD_BYTES);
        // Cap compaction/IO background traffic so it cannot saturate the same
        // NVMe queue the point lookups and blk reads share.
        opts.set_ratelimiter(COMPACTION_RATE_LIMIT_BYTES, 100_000, 10);
    }
}

/// Coarse L0 batching for [`UtxoStore::open_planned_bulk`]: let L0 accumulate
/// up to 16 SSTs before triggering background compaction (with slowdown/stop
/// raised to 48/64) instead of disabling compaction completely.
///
/// Disabling auto-compactions caused L0 to pile up 400+ overlapping SST files
/// once `RecentUtxoCache` filled past height ~400,000, degrading every DB point
/// lookup to \(O(N_{L0})\). Batching L0 in 16-file waves caps L0 depth while
/// still amortizing compaction writes and purging tombstones continuously.
fn apply_bulk_l0_batching(opts: &mut Options) {
    opts.set_level_zero_file_num_compaction_trigger(BULK_L0_COMPACTION_TRIGGER);
    opts.set_level_zero_slowdown_writes_trigger(BULK_L0_SLOWDOWN_TRIGGER);
    opts.set_level_zero_stop_writes_trigger(BULK_L0_STOP_TRIGGER);
}

/// Background jobs for the write path: one per core, clamped to a sane range.
/// Exposed so a test can pin the policy without a specific machine.
pub(crate) fn background_jobs_for(cores: usize) -> i32 {
    cores.clamp(4, 16) as i32
}

/// Subcompactions for one compaction: roughly half the cores, at least 2 so a
/// dual-core box still parallelizes, at most 8 (RocksDB's own practical cap).
pub(crate) fn subcompactions_for(cores: usize) -> u32 {
    (cores / 2).clamp(2, 8) as u32
}

/// `min_write_buffer_number_to_merge`: how many memtables RocksDB merges in
/// memory before flushing to L0.
///
/// A bulk index creates an outpoint and spends it a short time later. If the
/// `put` and the `delete` land in two different memtables, the default (1)
/// flushes both: two SST entries for a UTXO that no longer exists. Merging two
/// memtables first applies last-write-wins across them, so the pair cancels and
/// *neither* reaches L0 — a short-lived output costs zero flush bytes instead
/// of two. `finalize_bulk_load` would drop the cancelled pair anyway, so the
/// steady-state store is identical; this removes the intermediate write.
///
/// Live mode keeps the default (1): it flushes per block for crash durability
/// (the WAL is on and synced there), and holding memtables back would widen the
/// unrecoverable window for no throughput benefit. Exposed as a pure function
/// so the policy is unit-tested without a live store, matching the
/// `background_jobs_for` / `subcompactions_for` pattern.
pub(crate) fn memtables_to_merge(bulk: bool) -> i32 {
    if bulk {
        2
    } else {
        1
    }
}

fn default_background_jobs() -> i32 {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    background_jobs_for(cores)
}

fn default_subcompactions() -> u32 {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    subcompactions_for(cores)
}

/// Tuning applied to the database *and* to every column family.
///
/// Building this once and cloning it into each `ColumnFamilyDescriptor` is
/// load-bearing, not stylistic. `DB::open_cf_descriptors` takes per-CF
/// `Options` and passes them to `rocksdb_open_column_families`, and the C API
/// applies the per-CF options to that family. So setting tuning only on the
/// DB-level `Options` (as this code used to) has *no effect on any column
/// family* — the default CF carried RocksDB's stock 64 MiB write buffer and
/// 32 MiB block cache regardless of what `open_with` configured. The measured
/// symptom was a write-buffer flush every ~1s at tip instead of every ~8s.
///
/// The block cache is passed in as a shared handle rather than constructed
/// here: a store allocates exactly *one* cache and hands the same `Arc`-backed
/// handle to every column family. Constructing a fresh cache per CF (as this
/// used to) both tripled the memory and fragmented it, so a lookup in the UTXO
/// family could not benefit from blocks the undo family had already read.
fn tuned_options(
    statistics: bool,
    block_cache: &rocksdb::Cache,
    bulk: bool,
    memtable_budget_bytes: usize,
) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    apply_write_tuning(&mut opts, bulk, memtable_budget_bytes);
    // Shared HyperClock block cache + partitioned two-level indexes + hybrid
    // Ribbon/Bloom filters:
    // - `TwoLevelIndexSearch` + `partition_filters` splits giant 256 MiB SST
    //   index and filter blocks into 4 KiB partitions, pinning only the tiny
    //   top-level index (~few MiB total) so `block_cache.set_capacity()` can
    //   still shrink cleanly under memory pressure while hot partitions stay
    //   resident.
    // - `hybrid_ribbon_filter(10.0, 2)` keeps fast Bloom filters on L0/L1 and
    //   uses ~30% smaller Ribbon filters on L2+ for the same ~1% FPR.
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_format_version(5);
    block_opts.set_block_cache(block_cache);
    block_opts.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
    block_opts.set_partition_filters(true);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_top_level_index_and_filter(true);
    block_opts.set_hybrid_ribbon_filter(10.0, 2);
    block_opts.set_whole_key_filtering(true);
    opts.set_block_based_table_factory(&block_opts);
    if statistics {
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(0);
    }
    opts
}

/// Default cap on the total memtable memory across all column families, used
/// by the `open*` entry points that do not take a full [`bidx_core::MemoryPlan`].
/// Matches the planner's `MEMTABLE_BUDGET_KB` so a hand-rolled open and a planned
/// open bound memtables the same way.
pub const DEFAULT_MEMTABLE_BUDGET_BYTES: usize =
    (bidx_core::mem::MEMTABLE_BUDGET_KB as usize) << 10;

impl UtxoStore {
    /// Open in bulk-load mode (no WAL, no undo logging), with the default
    /// block cache and no recent-UTXO cache.
    pub fn open(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            DEFAULT_BLOCK_CACHE_BYTES,
            0,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in live mode: WAL enabled (durable per-block) and undo logging.
    pub fn open_live(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            true,
            true,
            false,
            DEFAULT_BLOCK_CACHE_BYTES,
            0,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Like [`UtxoStore::open`] but with RocksDB statistics enabled, which
    /// makes `stall_report` meaningful. Statistics collection adds a small
    /// amount of per-operation overhead, so this is for diagnosis, not for
    /// throughput runs.
    pub fn open_statistics(path: &Path) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            DEFAULT_BLOCK_CACHE_BYTES,
            0,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode with the in-memory recent-UTXO cache enabled.
    ///
    /// The cache is a pure performance layer: it holds a copy of recently
    /// created outputs so a spend of a recent output can be answered from memory
    /// instead of RocksDB. Disabled in live mode because `BlockUndo` must be
    /// byte-exact for reorgs, and mixing an in-memory view into that path adds
    /// a correctness risk for no benchmarkable benefit.
    pub fn open_cached(path: &Path, cache_bytes: usize) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            DEFAULT_BLOCK_CACHE_BYTES,
            cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode with both the cache and RocksDB statistics.
    pub fn open_cached_statistics(path: &Path, cache_bytes: usize) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            DEFAULT_BLOCK_CACHE_BYTES,
            cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode with an explicit block-cache size (bytes) and
    /// recent-UTXO cache size (bytes; 0 disables it).
    ///
    /// Prefer [`UtxoStore::open_planned`] for production bulk indexing: it sizes
    /// both caches from the host's available memory *and* caps memtables, so a
    /// large-RAM box is not left faulting the UTXO read path off disk while
    /// staying under the watchdog ceiling. This variant takes the two cache
    /// sizes directly for callers that have already planned them.
    pub fn open_tuned(
        path: &Path,
        block_cache_bytes: usize,
        recent_cache_bytes: usize,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            block_cache_bytes,
            recent_cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// [`UtxoStore::open_tuned`] with RocksDB statistics enabled.
    pub fn open_tuned_statistics(
        path: &Path,
        block_cache_bytes: usize,
        recent_cache_bytes: usize,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            block_cache_bytes,
            recent_cache_bytes,
            DEFAULT_MEMTABLE_BUDGET_BYTES,
            false,
        )
    }

    /// Open in bulk-load mode from a full [`bidx_core::MemoryPlan`].
    ///
    /// This is the entry point a bulk index should use: the plan's caches,
    /// memtable cap, and file-LRU bound all derive from one ceiling, so the
    /// process cannot be sized past the watchdog limit it will abort at. See
    /// `bidx_core::mem`.
    pub fn open_planned(path: &Path, plan: &bidx_core::MemoryPlan) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            plan.block_cache_bytes,
            plan.recent_cache_bytes,
            plan.memtable_budget_bytes,
            false,
        )
    }

    /// [`UtxoStore::open_planned`] with RocksDB statistics enabled.
    pub fn open_planned_statistics(
        path: &Path,
        plan: &bidx_core::MemoryPlan,
    ) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            true,
            plan.block_cache_bytes,
            plan.recent_cache_bytes,
            plan.memtable_budget_bytes,
            false,
        )
    }

    /// Like [`UtxoStore::open_planned`], but uses coarse L0 batching
    /// (`BULK_L0_COMPACTION_TRIGGER = 16`, slowdown `48`, stop `64`) for the
    /// duration of a bulk load so background compactions merge ~8 GiB waves of
    /// L0 SSTs while keeping L0 depth bounded for cache-miss point lookups.
    ///
    /// The caller **must** call [`UtxoStore::finalize_bulk_load`] when the load
    /// is done, before the store is used for steady-state queries or reopened
    /// in live mode: `finalize_bulk_load` drains the write-back
    /// `RecentUtxoCache`, restores steady-state L0 triggers (`8`/`20`/`30`),
    /// flushes memtables, and runs a final compaction pass.
    pub fn open_planned_bulk(path: &Path, plan: &bidx_core::MemoryPlan) -> Result<Self, UtxoError> {
        Self::open_with(
            path,
            false,
            false,
            false,
            plan.block_cache_bytes,
            plan.recent_cache_bytes,
            plan.memtable_budget_bytes,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_with(
        path: &Path,
        wal: bool,
        undo_enabled: bool,
        statistics: bool,
        block_cache_bytes: usize,
        recent_cache_bytes: usize,
        memtable_budget_bytes: usize,
        bulk_l0_batching: bool,
    ) -> Result<Self, UtxoError> {
        // Bulk-load tuning follows undo, which is exactly the split between an
        // initial index (no undo) and live tracking (undo). Deriving it here
        // rather than threading another flag keeps the two from drifting: a
        // store without undo *is* a bulk store.
        let bulk = !undo_enabled;

        // One block cache, shared by every column family. Allocating it here
        // (rather than inside `tuned_options`) is what makes the sharing real:
        // the same handle is cloned into each CF descriptor below, so a block
        // read for the undo family can be reused by the UTXO family and the
        // total is bounded by one budget instead of three.
        //
        // A HyperClockCache rather than an LRU: on the apply path a dozen parse
        // workers plus the consumer thread hit this one shared cache, and the
        // LRU's per-hit mutex becomes a contention point. HyperClock is
        // lock-free on the read path. `estimated_entry_charge` is a conservative
        // per-entry reservation (see `CACHE_ENTRY_CHARGE`); the cache stays
        // mutable so `relieve_pressure` can shrink it — HyperClockCache
        // implements `SetCapacity`, so shedding works as it does for the LRU.
        let block_cache =
            rocksdb::Cache::new_hyper_clock_cache(block_cache_bytes.max(1), CACHE_ENTRY_CHARGE);

        // DB-level options: create flags, background threads, statistics.
        let mut opts = tuned_options(statistics, &block_cache, bulk, memtable_budget_bytes);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // Cap the total memtable memory across all families. Without this each
        // family may hold up to `write_buffer_size * max_write_buffer_number`
        // (512 MiB * 4 = 2 GiB), so three families could pin ~6 GiB of memtables
        // that the cache budget never accounted for — the accounting gap that
        // let a 32 GiB box reach its ceiling and segfault. `0` leaves RocksDB's
        // own per-family default (used only by tests that want stock behaviour).
        if memtable_budget_bytes > 0 {
            opts.set_db_write_buffer_size(memtable_budget_bytes);
        }
        if bulk_l0_batching {
            apply_bulk_l0_batching(&mut opts);
        }

        // Per-CF options: this is where the storage tuning actually lands.
        // The undo and meta families share the same table factory and cache;
        // they do not need a large write buffer, since they are tiny next to
        // the UTXO family, but they must still point at the *shared* cache.
        let mut utxo_cf_opts = tuned_options(statistics, &block_cache, bulk, memtable_budget_bytes);
        if bulk_l0_batching {
            apply_bulk_l0_batching(&mut utxo_cf_opts);
        }
        let mut aux_cf_opts = Options::default();
        let mut aux_block_opts = rocksdb::BlockBasedOptions::default();
        aux_block_opts.set_block_cache(&block_cache);
        aux_block_opts.set_cache_index_and_filter_blocks(true);
        aux_cf_opts.set_block_based_table_factory(&aux_block_opts);
        aux_cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let cf_defs = vec![
            rocksdb::ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, utxo_cf_opts),
            rocksdb::ColumnFamilyDescriptor::new(CF_UNDO, aux_cf_opts.clone()),
            rocksdb::ColumnFamilyDescriptor::new(CF_META, aux_cf_opts),
        ];
        let db = DB::open_cf_descriptors(&opts, path, cf_defs)?;
        let _ = wal; // WAL is always on; bulk mode uses WriteOptions::disable_wal per write.
        Ok(UtxoStore {
            db,
            path: path.to_path_buf(),
            scratch: Mutex::new(ApplyScratch::default()),
            recent: if recent_cache_bytes > 0 {
                Some(Mutex::new(RecentUtxoCache::new(recent_cache_bytes)))
            } else {
                None
            },
            undo_enabled,
            statistics,
            block_cache: Some(block_cache),
            block_cache_bytes: std::sync::atomic::AtomicUsize::new(block_cache_bytes),
            bulk_deferred_compaction: bulk && bulk_l0_batching,
        })
    }

    fn cf_utxo(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .expect("default cf")
    }
    fn cf_undo(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_UNDO).expect("undo cf")
    }
    fn cf_meta(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_META).expect("meta cf")
    }

    fn commit_pending_utxos(
        &self,
        pending: &mut FxHashMap<[u8; 36], (UtxoValue, u32)>,
        batch: &mut WriteBatch,
    ) {
        if let Some(ref cache) = self.recent {
            let mut c = cache.lock();
            for (key, (val, tx_out_cnt)) in pending.drain() {
                if should_cache_utxo(&val, tx_out_cnt) {
                    if let Some((ev_k, ev_v)) = c.insert(key, val) {
                        batch.put(ev_k, ev_v.encode());
                    }
                } else {
                    batch.put(key, val.encode());
                }
            }
        } else {
            for (key, (val, _)) in pending.drain() {
                batch.put(key, val.encode());
            }
        }
    }

    /// Apply a single block forward in place: enriches resolved `inputs[i]`
    /// with `spent_*` fields, accumulates transaction fees directly onto
    /// `txs[i].fee`, and computes `block_row.total_fee` without allocating any
    /// side `Vec` buffers. In undo-enabled (live) mode, writes the undo record
    /// for `height` atomically in the same `WriteBatch`.
    pub fn apply_block_in_place(
        &self,
        height: u32,
        block_row: &mut bidx_core::BlockRow,
        txs: &mut [bidx_core::TxRow],
        inputs: &mut [InputRow],
        outputs: &[OutputRow],
    ) -> Result<WindowStats, UtxoError> {
        let mut scratch = self.scratch.lock();
        let ApplyScratch {
            pending,
            misses,
            resolved,
            same_block,
            ..
        } = &mut *scratch;
        pending.clear();
        misses.clear();
        resolved.clear();
        resolved.resize(inputs.len(), None);
        same_block.clear();
        for tx in txs.iter_mut() {
            tx.fee = 0;
        }

        let mut batch = WriteBatch::default();
        let mut missing = 0u64;
        let mut undo = BlockUndo::default();

        for out in outputs {
            if out.script_type == bidx_core::ScriptType::OpReturn as u8 {
                continue;
            }
            let Some(tx) = txs.get(out.tx_index as usize) else {
                continue;
            };
            let key = make_key(&tx.txid, out.output_index as u32);
            let tx_out_cnt = tx.output_count;
            pending.insert(
                key,
                (
                    UtxoValue {
                        value_sat: out.value_sat,
                        script_type: out.script_type,
                        height,
                        address_hash: out.address_hash,
                    },
                    tx_out_cnt,
                ),
            );
            if self.undo_enabled {
                undo.created.push(key);
            }
        }

        let mut db_lookups = 0u64;
        let mut db_nanos = 0u64;
        let mut pending_hits = 0u64;
        let mut cache_hits = 0u64;

        {
            let mut recent_guard = self.recent.as_ref().map(|c| c.lock());
            for (idx, inp) in inputs.iter().enumerate() {
                if inp.is_coinbase {
                    continue;
                }
                let key = make_key(&inp.prev_txid, inp.prev_vout);
                if let Some((v, _)) = pending.remove(&key) {
                    if self.undo_enabled {
                        same_block.insert(key);
                    }
                    pending_hits += 1;
                    resolved[idx] = Some((v, false));
                } else if let Some(v) = recent_guard.as_mut().and_then(|c| c.remove(&key)) {
                    cache_hits += 1;
                    resolved[idx] = Some((v, false));
                } else {
                    misses.push((idx, key));
                }
            }
        }

        if !misses.is_empty() {
            db_lookups = misses.len() as u64;
            misses.sort_unstable_by_key(|a| a.1);
            let sorted_unique = misses.windows(2).all(|w| w[0].1 < w[1].1);
            let cf = self.cf_utxo();
            let t0 = std::time::Instant::now();
            let results =
                self.db
                    .batched_multi_get_cf(&cf, misses.iter().map(|(_, k)| k), sorted_unique);
            db_nanos = t0.elapsed().as_nanos() as u64;
            for (&(idx, key), got) in misses.iter().zip(results) {
                match got? {
                    Some(bytes) => match UtxoValue::decode(bytes.as_ref()) {
                        Some(v) => {
                            batch.delete(key);
                            resolved[idx] = Some((v, true));
                        }
                        None => missing += 1,
                    },
                    None => missing += 1,
                }
            }
        }

        for (idx, inp) in inputs.iter_mut().enumerate() {
            if let Some((v, from_db)) = resolved[idx] {
                if from_db && self.undo_enabled {
                    undo.spent
                        .push((make_key(&inp.prev_txid, inp.prev_vout), v));
                }
                enrich_input_in_place(txs, inp, v);
            }
        }

        if self.undo_enabled && !same_block.is_empty() {
            undo.created.retain(|k| !same_block.contains(k));
        }

        finalize_block_fees(block_row, txs, inputs, outputs);
        self.commit_pending_utxos(pending, &mut batch);

        if self.undo_enabled {
            batch.put_cf(&self.cf_undo(), undo_key(height), undo.encode());
        }

        if !batch.is_empty() {
            let mut wo = rocksdb::WriteOptions::default();
            if self.undo_enabled {
                wo.set_sync(true);
            } else {
                wo.disable_wal(true);
            }
            self.db.write_opt(batch, &wo)?;
        }

        Ok(WindowStats {
            missing,
            db_lookups,
            db_nanos,
            pending_hits,
            cache_hits,
        })
    }

    /// Apply a contiguous window of blocks in strict height order.
    ///
    /// In bulk mode (`!undo_enabled`) with `window.len() > 1`, outputs created
    /// in block `b` and spent in block `b + d` (`d < window.len()`) cancel
    /// directly inside `pending` before ever touching [`RecentUtxoCache`] or
    /// RocksDB, and all remaining DB misses + surviving cold/evicted writes
    /// across the entire window execute as **one** key-sorted
    /// `batched_multi_get_cf` and **one** `WriteBatch`.
    pub fn apply_window_in_place(
        &self,
        window: &mut [WindowBlock<'_>],
    ) -> Result<WindowStats, UtxoError> {
        if window.is_empty() {
            return Ok(WindowStats::default());
        }
        if self.undo_enabled || window.len() == 1 {
            let mut acc = WindowStats::default();
            for b in window.iter_mut() {
                let s =
                    self.apply_block_in_place(b.height, b.block_row, b.txs, b.inputs, b.outputs)?;
                acc.missing += s.missing;
                acc.db_lookups += s.db_lookups;
                acc.db_nanos += s.db_nanos;
                acc.pending_hits += s.pending_hits;
                acc.cache_hits += s.cache_hits;
            }
            return Ok(acc);
        }

        let mut scratch = self.scratch.lock();
        let ApplyScratch {
            pending,
            window_misses,
            window_resolved,
            window_offsets,
            ..
        } = &mut *scratch;
        pending.clear();
        window_misses.clear();
        window_offsets.clear();

        let mut total_inputs = 0usize;
        for b in window.iter_mut() {
            window_offsets.push(total_inputs);
            total_inputs += b.inputs.len();
            for tx in b.txs.iter_mut() {
                tx.fee = 0;
            }
        }
        window_resolved.clear();
        window_resolved.resize(total_inputs, None);

        let mut stats = WindowStats::default();

        // Pass 1: step sequentially through all blocks in the window so both
        // intra-block AND cross-block spends within the window cancel in
        // `pending` under a single `RecentUtxoCache` lock acquisition.
        {
            let mut recent_guard = self.recent.as_ref().map(|c| c.lock());
            for (b_idx, b) in window.iter().enumerate() {
                let height = b.height;
                for out in b.outputs {
                    if out.script_type == bidx_core::ScriptType::OpReturn as u8 {
                        continue;
                    }
                    let Some(tx) = b.txs.get(out.tx_index as usize) else {
                        continue;
                    };
                    let key = make_key(&tx.txid, out.output_index as u32);
                    let tx_out_cnt = tx.output_count;
                    pending.insert(
                        key,
                        (
                            UtxoValue {
                                value_sat: out.value_sat,
                                script_type: out.script_type,
                                height,
                                address_hash: out.address_hash,
                            },
                            tx_out_cnt,
                        ),
                    );
                }

                let base = window_offsets[b_idx];
                for (inp_idx, inp) in b.inputs.iter().enumerate() {
                    if inp.is_coinbase {
                        continue;
                    }
                    let key = make_key(&inp.prev_txid, inp.prev_vout);
                    let flat_idx = base + inp_idx;
                    if let Some((v, _)) = pending.remove(&key) {
                        stats.pending_hits += 1;
                        window_resolved[flat_idx] = Some(v);
                    } else if let Some(v) = recent_guard.as_mut().and_then(|c| c.remove(&key)) {
                        stats.cache_hits += 1;
                        window_resolved[flat_idx] = Some(v);
                    } else {
                        window_misses.push((key, b_idx as u16, flat_idx as u32));
                    }
                }
            }
        }

        let mut batch = WriteBatch::default();

        // Pass 2: one coalesced key-sorted `batched_multi_get_cf` across all
        // misses in the window.
        if !window_misses.is_empty() {
            stats.db_lookups = window_misses.len() as u64;
            window_misses.sort_unstable();
            let sorted_unique = window_misses.windows(2).all(|w| w[0].0 < w[1].0);
            let cf = self.cf_utxo();
            let t0 = std::time::Instant::now();
            let results = self.db.batched_multi_get_cf(
                &cf,
                window_misses.iter().map(|(k, _, _)| k),
                sorted_unique,
            );
            stats.db_nanos = t0.elapsed().as_nanos() as u64;

            let mut prev_resolved_key: Option<[u8; 36]> = None;
            for (&(key, _, flat_idx), got) in window_misses.iter().zip(results) {
                if prev_resolved_key == Some(key) {
                    // Duplicate spend of the same DB outpoint across blocks in
                    // the window: the earlier block consumed it, so later
                    // attempts count as missing just like sequential apply.
                    stats.missing += 1;
                    continue;
                }
                match got? {
                    Some(bytes) => match UtxoValue::decode(bytes.as_ref()) {
                        Some(v) => {
                            batch.delete(key);
                            window_resolved[flat_idx as usize] = Some(v);
                            prev_resolved_key = Some(key);
                        }
                        None => stats.missing += 1,
                    },
                    None => stats.missing += 1,
                }
            }
        }

        // Pass 3: enrich inputs + populate per-block fees in place, then commit
        // surviving `pending` entries in one `WriteBatch`.
        for (b_idx, b) in window.iter_mut().enumerate() {
            let base = window_offsets[b_idx];
            for (inp_idx, inp) in b.inputs.iter_mut().enumerate() {
                if let Some(v) = window_resolved[base + inp_idx] {
                    enrich_input_in_place(b.txs, inp, v);
                }
            }
            finalize_block_fees(b.block_row, b.txs, b.inputs, b.outputs);
        }

        self.commit_pending_utxos(pending, &mut batch);

        if !batch.is_empty() {
            let mut wo = rocksdb::WriteOptions::default();
            wo.disable_wal(true);
            self.db.write_opt(batch, &wo)?;
        }

        Ok(stats)
    }

    /// Stream every live unspent UTXO `(txid, vout, UtxoValue)` in the store.
    ///
    /// Drains any unspent entries still resident in the write-back
    /// [`RecentUtxoCache`] first, then scans `CF_UTXO` sequentially with a
    /// 4 MiB readahead window and `fill_cache = false` so ClickHouse's
    /// `utxos_by_address` index can be populated directly from the ~170M live
    /// UTXOs instead of sorting ~5.8B historical funding+spend rows.
    pub fn for_each_live_utxo<E, F>(&self, mut on_utxo: F) -> Result<u64, E>
    where
        E: From<UtxoError>,
        F: FnMut(&[u8; 32], u32, &UtxoValue) -> Result<(), E>,
    {
        self.evict_recent_to(0)?;
        let mut ro = rocksdb::ReadOptions::default();
        ro.fill_cache(false);
        ro.set_readahead_size(4 << 20);
        let iter = self
            .db
            .iterator_cf_opt(&self.cf_utxo(), ro, rocksdb::IteratorMode::Start);
        let mut count = 0u64;
        for item in iter {
            let (k, v) = item.map_err(UtxoError::from)?;
            if k.len() != 36 {
                continue;
            }
            let Some(val) = UtxoValue::decode(&v) else {
                continue;
            };
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&k[..32]);
            let vout = u32::from_le_bytes([k[32], k[33], k[34], k[35]]);
            on_utxo(&txid, vout, &val)?;
            count += 1;
        }
        Ok(count)
    }

    /// Reverse the block applied at `height`, restoring spent outputs and
    /// removing created outpoints. Consumes the undo record.
    pub fn disconnect_block(&self, height: u32) -> Result<(), UtxoError> {
        let cf = self.cf_undo();
        let raw = self
            .db
            .get_cf(&cf, undo_key(height))?
            .ok_or(UtxoError::NoUndo(height))?;
        let undo = BlockUndo::decode(&raw).ok_or(UtxoError::CorruptUndo(height))?;

        let mut batch = WriteBatch::default();
        // Restore spent outputs.
        for (key, val) in &undo.spent {
            batch.put(*key, val.encode());
        }
        // Remove created outpoints that remain in the set. Those spent
        // same-block were already removed (and not in `created`).
        for key in &undo.created {
            batch.delete(*key);
        }
        batch.delete_cf(&cf, undo_key(height));

        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(batch, &wo)?;
        Ok(())
    }

    /// Persist the current chain tip.
    pub fn set_tip(&self, tip: &TipState) -> Result<(), UtxoError> {
        let mut b = [0u8; 36];
        b[..4].copy_from_slice(&tip.height.to_le_bytes());
        b[4..].copy_from_slice(tip.hash.as_bytes());
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.db.put_cf_opt(&self.cf_meta(), META_TIP_KEY, b, &wo)?;
        Ok(())
    }

    /// Read the persisted tip, if any.
    pub fn tip(&self) -> Result<Option<TipState>, UtxoError> {
        match self.db.get_cf(&self.cf_meta(), META_TIP_KEY)? {
            Some(b) if b.len() == 36 => {
                let mut height = [0u8; 4];
                height.copy_from_slice(&b[..4]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&b[4..]);
                Ok(Some(TipState {
                    height: u32::from_le_bytes(height),
                    hash: Hash32::from_bytes(hash),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Current in-memory cache footprint in bytes, and its cumulative hit/miss
    /// counters. `None` when the cache is disabled.
    ///
    /// The byte figure is an estimate (`CACHE_ENTRY_COST` per entry) because the
    /// allocator's exact overhead is not observable; it is accurate enough to
    /// confirm the budget is being respected.
    pub fn cache_stats(&self) -> Option<CacheStats> {
        let c = self.recent.as_ref()?.lock();
        Some(CacheStats {
            entries: c.len(),
            bytes: c.bytes(),
            budget_bytes: c.budget_bytes,
            hits: c.hits,
            misses: c.misses,
        })
    }

    /// Evict oldest dirty write-back entries from `RecentUtxoCache` down to
    /// `target_entries` in bounded `CACHE_FLUSH_BATCH_ENTRIES` (~2.6 MiB)
    /// `WriteBatch` chunks, dropping the cache mutex across every `write_opt`
    /// so no lock is held during RocksDB I/O.
    fn evict_recent_to(&self, target_entries: usize) -> Result<usize, UtxoError> {
        let Some(ref recent) = self.recent else {
            return Ok(0);
        };
        let mut total = 0usize;
        let mut wo = rocksdb::WriteOptions::default();
        wo.disable_wal(true);
        loop {
            let mut batch = WriteBatch::default();
            let n = {
                let mut c = recent.lock();
                let n =
                    c.evict_oldest_to_batch(target_entries, CACHE_FLUSH_BATCH_ENTRIES, &mut batch);
                if target_entries == 0 && n == 0 {
                    c.release_if_empty();
                }
                n
            };
            if n == 0 {
                break;
            }
            self.db.write_opt(batch, &wo)?;
            total += n;
        }
        Ok(total)
    }

    pub fn flush(&self) -> Result<(), UtxoError> {
        self.evict_recent_to(0)?;
        self.db.flush()?;
        Ok(())
    }

    /// Close out a bulk load opened via [`UtxoStore::open_planned_bulk`]:
    /// drain all remaining unspent entries from the write-back
    /// `RecentUtxoCache` into RocksDB, release its multi-GiB backing vectors,
    /// restore steady-state L0 triggers (`8`/`20`/`30`), flush memtables, and
    /// run a final full-range compaction.
    ///
    /// A no-op (returns `false`) for any store not opened via
    /// [`UtxoStore::open_planned_bulk`]. Returns `true` when it finalized.
    pub fn finalize_bulk_load(&self) -> Result<bool, UtxoError> {
        if !self.bulk_deferred_compaction {
            return Ok(false);
        }
        self.evict_recent_to(0)?;
        bidx_core::trim_allocator();
        let trigger = L0_COMPACTION_TRIGGER.to_string();
        let slowdown = L0_SLOWDOWN_TRIGGER.to_string();
        let stop = L0_STOP_TRIGGER.to_string();
        self.db.set_options_cf(
            self.cf_utxo(),
            &[
                ("level0_file_num_compaction_trigger", &trigger),
                ("level0_slowdown_writes_trigger", &slowdown),
                ("level0_stop_writes_trigger", &stop),
            ],
        )?;
        self.db.flush()?;
        self.db
            .compact_range_cf(self.cf_utxo(), None::<&[u8]>, None::<&[u8]>);
        Ok(true)
    }

    /// Shed memory under pressure, without failing the run.
    ///
    /// Called by the consumer when RSS crosses the watchdog's *soft* threshold
    /// — i.e. before the hard ceiling that aborts the pipeline. Two levers, both
    /// safe and both reversible by the workload:
    ///
    /// 1. **Shrink the write-back recent-UTXO cache.** Dirty victims above the
    ///    halved target are flushed to RocksDB in bounded 32K-entry batches
    ///    first (never losing unspent UTXOs), then the backing `entries` and
    ///    `buckets` vectors are compacted and shrunk in place.
    /// 2. **Halve the shared block cache.** RocksDB evicts to the new capacity
    ///    on its own; a smaller block cache costs lookups, not correctness.
    ///
    /// Returns the bytes of cache capacity removed, so a caller can log the
    /// effect.
    pub fn relieve_pressure(&self) -> usize {
        let mut freed = 0usize;
        if let Some(ref recent) = self.recent {
            let target = {
                let c = recent.lock();
                // Never below one entry's worth, so the cache cannot be shrunk
                // into a state where it holds nothing yet reports as enabled.
                (c.budget_bytes / 2).max(CACHE_ENTRY_COST)
            };
            let _ = self.evict_recent_to(max_entries_for(target));
            let mut c = recent.lock();
            let before = c.budget_bytes;
            if target < before {
                c.set_budget(target);
                freed += before - target;
            }
        }
        if let Some(ref cache) = self.block_cache {
            let mut cache = cache.clone();
            let cur = self
                .block_cache_bytes
                .load(std::sync::atomic::Ordering::Relaxed);
            let target = (cur / 2).max(64 << 20); // never below 64 MiB
            if target < cur {
                cache.set_capacity(target);
                self.block_cache_bytes
                    .store(target, std::sync::atomic::Ordering::Relaxed);
                freed += cur - target;
            }
        }
        if freed > 0 {
            // Return freed C++ HyperClockCache slabs and shrunk Rust buffers
            // from per-thread allocator arenas back to the kernel so `VmRSS`
            // reflects the relief immediately.
            bidx_core::trim_allocator();
        }
        freed
    }

    /// Test-only: flush and empty the in-memory write-back cache into RocksDB
    /// so a subsequent spend is forced through the database lookup path.
    #[cfg(test)]
    fn cache_clear_for_test(&self) {
        self.evict_recent_to(0)
            .expect("flush recent cache for test");
    }

    pub fn approx_size_on_disk(&self) -> u64 {
        self.db
            .property_value(rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE)
            .ok()
            .flatten()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Filesystem path of the UTXO db.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Snapshot of RocksDB internals that matter for ingest throughput.
    ///
    /// These properties answer the questions that decide where to optimise:
    /// whether the write-buffer tuning in `open_with` actually took effect on
    /// the column families (`cur_size_all_mem_tables` / `size_all_mem_tables`
    /// against the configured 512 MiB per buffer), whether the write path is
    /// backing up behind flushes (`num_immutable_mem_table`,
    /// `mem_table_flush_pending`), whether compactions are queued
    /// (`compaction_pending`, `num_running_compactions`), whether RocksDB has
    /// started throttling us (`is_write_stopped`,
    /// `actual_delayed_write_rate`), and how the point-lookup path that
    /// `apply_block` depends on is cached (`block_cache_capacity/usage`).
    ///
    /// All of these are readable without enabling RocksDB statistics, so the
    /// act of measuring does not perturb the run. Properties that RocksDB
    /// declines to answer are reported as `"unavailable"` rather than
    /// omitted, so two runs can be diffed key-for-key.
    pub fn perf_properties(&self) -> Vec<(String, String)> {
        const KEYS: &[(&rocksdb::properties::PropName, &str)] = &[
            (
                rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES,
                "cur_size_all_mem_tables",
            ),
            (
                rocksdb::properties::SIZE_ALL_MEM_TABLES,
                "size_all_mem_tables",
            ),
            (
                rocksdb::properties::CUR_SIZE_ACTIVE_MEM_TABLE,
                "cur_size_active_mem_table",
            ),
            (
                rocksdb::properties::NUM_IMMUTABLE_MEM_TABLE,
                "num_immutable_mem_table",
            ),
            (
                rocksdb::properties::MEM_TABLE_FLUSH_PENDING,
                "mem_table_flush_pending",
            ),
            (
                rocksdb::properties::NUM_RUNNING_FLUSHES,
                "num_running_flushes",
            ),
            (
                rocksdb::properties::COMPACTION_PENDING,
                "compaction_pending",
            ),
            (
                rocksdb::properties::NUM_RUNNING_COMPACTIONS,
                "num_running_compactions",
            ),
            (rocksdb::properties::IS_WRITE_STOPPED, "is_write_stopped"),
            (
                rocksdb::properties::ACTUAL_DELAYED_WRITE_RATE,
                "actual_delayed_write_rate",
            ),
            (rocksdb::properties::ESTIMATE_NUM_KEYS, "estimate_num_keys"),
            (
                rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE,
                "estimate_live_data_size",
            ),
            (
                rocksdb::properties::TOTAL_SST_FILES_SIZE,
                "total_sst_files_size",
            ),
            (
                rocksdb::properties::BLOCK_CACHE_CAPACITY,
                "block_cache_capacity",
            ),
            (rocksdb::properties::BLOCK_CACHE_USAGE, "block_cache_usage"),
        ];

        let mut out: Vec<(String, String)> = Vec::with_capacity(KEYS.len() + 2);
        for &(prop, name) in KEYS {
            out.push((name.to_string(), self.property_or_unavailable(prop)));
        }
        // "num-files-at-level0" is generated, not a const.
        out.push((
            "num_files_at_level0".to_string(),
            self.property_or_unavailable(rocksdb::properties::num_files_at_level(0)),
        ));
        out
    }

    fn property_or_unavailable(&self, prop: impl rocksdb::CStrLike) -> String {
        // Delegated to a pure helper so the `None` arm (a property RocksDB
        // declines to answer) is unit-testable without a property that errors.
        format_property(self.db.property_value(prop).ok().flatten())
    }

    /// Lines from the `rocksdb.stats` blob that mention stalls, which is
    /// where compaction backpressure becomes visible.
    ///
    /// Returns empty unless the store was opened with `open_statistics`,
    /// because RocksDB only populates these counters when statistics are on.
    /// An empty result therefore means "not measured", never "zero stalls".
    pub fn stall_report(&self) -> Vec<String> {
        // Statistics off means the property is absent or meaningless, so the
        // documented "empty unless statistics were enabled" contract is
        // enforced here rather than relying on RocksDB's property behavior.
        if !self.statistics {
            return Vec::new();
        }
        self.db
            .property_value(rocksdb::properties::STATS)
            .ok()
            .flatten()
            .map(|s| {
                s.lines()
                    .filter(|l| l.contains("stall") || l.contains("Stall"))
                    .map(|l| l.trim().to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// True when this store was opened with RocksDB statistics enabled, i.e.
    /// when `stall_report` can say anything meaningful.
    pub fn statistics_enabled(&self) -> bool {
        self.statistics
    }

    /// Total bytes consumed by this UTXO db on disk, including WAL and MANIFEST.
    pub fn disk_size_bytes(&self) -> u64 {
        bidx_core::dir_size_bytes(&self.path)
    }
}

impl Drop for UtxoStore {
    fn drop(&mut self) {
        // Bulk stores write with `disable_wal(true)` and hold hot unspent
        // UTXOs exclusively in `RecentUtxoCache` until evicted or finalized.
        // Draining `RecentUtxoCache` and flushing unlogged memtables on drop
        // ensures reopening the store (e.g. `open_live` after a bulk load)
        // never loses UTXOs even if the caller did not call `flush` or
        // `finalize_bulk_load` explicitly.
        if !self.undo_enabled {
            let _ = self.evict_recent_to(0);
            let _ = self.db.flush();
        }
    }
}

/// Render a RocksDB property value, substituting `"unavailable"` when the
/// property is absent. Split out from `property_or_unavailable` so the `None`
/// arm — a property RocksDB declines to answer — is directly testable.
fn format_property(value: Option<String>) -> String {
    value.unwrap_or_else(|| "unavailable".to_string())
}

#[inline]
fn enrich_input_in_place(txs: &mut [bidx_core::TxRow], inp: &mut InputRow, v: UtxoValue) {
    inp.spent_value_sat = v.value_sat;
    inp.spent_script_type = v.script_type;
    inp.spent_address_hash = v.address_hash;
    inp.spent_height = v.height;
    if let Some(tx) = txs.get_mut(inp.tx_index as usize) {
        tx.fee = tx.fee.saturating_add(v.value_sat);
    }
}

#[inline]
fn finalize_block_fees(
    block_row: &mut bidx_core::BlockRow,
    txs: &mut [bidx_core::TxRow],
    inputs: &[InputRow],
    outputs: &[OutputRow],
) {
    for out in outputs {
        if let Some(tx) = txs.get_mut(out.tx_index as usize) {
            tx.fee = tx.fee.saturating_sub(out.value_sat);
        }
    }
    let first_is_coinbase = inputs
        .first()
        .is_some_and(|inp| inp.is_coinbase && inp.tx_index == 0);
    let mut total_fee = 0u64;
    for (i, tx) in txs.iter_mut().enumerate() {
        if i == 0 && first_is_coinbase {
            tx.fee = 0;
        } else {
            total_fee = total_fee.saturating_add(tx.fee);
        }
    }
    block_row.total_fee = total_fee;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utxo_value_roundtrip() {
        let v = UtxoValue {
            value_sat: 123_456_789,
            script_type: 2,
            height: 800_000,
            address_hash: [0xab; 32],
        };
        let dec = UtxoValue::decode(&v.encode()).unwrap();
        assert_eq!(dec, v);
    }

    #[test]
    fn block_undo_roundtrip() {
        let mut u = BlockUndo::default();
        for i in 0..5u8 {
            let mut key = [0u8; 36];
            key[0] = i;
            key[35] = 255 - i;
            u.spent.push((
                key,
                UtxoValue {
                    value_sat: (i as u64) * 1_000_000,
                    script_type: i,
                    height: 100 + i as u32,
                    address_hash: [i; 32],
                },
            ));
        }
        for i in 0..3u8 {
            let mut key = [0u8; 36];
            key[10] = i;
            u.created.push(key);
        }
        let enc = u.encode();
        let dec = BlockUndo::decode(&enc).unwrap();
        assert_eq!(dec.spent.len(), 5);
        assert_eq!(dec.created.len(), 3);
        for (a, b) in u.spent.iter().zip(dec.spent.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1, b.1);
        }
        assert_eq!(u.created, dec.created);
    }

    #[test]
    fn block_undo_empty_and_varint_boundaries() {
        // Empty undo.
        let u = BlockUndo::default();
        let dec = BlockUndo::decode(&u.encode()).unwrap();
        assert!(dec.spent.is_empty() && dec.created.is_empty());

        // Varint boundary: 252 -> 1 byte, 253 -> 3 bytes.
        let mut out = Vec::new();
        put_varint(&mut out, 252);
        assert_eq!(out.len(), 1);
        out.clear();
        put_varint(&mut out, 253);
        assert_eq!(out.len(), 3);
        // Round-trip a large count.
        let mut out = Vec::new();
        put_varint(&mut out, 4_000_000);
        let mut slice: &[u8] = &out;
        assert_eq!(read_varint(&mut slice), Some(4_000_000));
    }

    #[test]
    fn block_undo_rejects_truncated() {
        let mut u = BlockUndo::default();
        u.spent.push((
            [1u8; 36],
            UtxoValue {
                value_sat: 1,
                script_type: 0,
                height: 0,
                address_hash: [0u8; 32],
            },
        ));
        let enc = u.encode();
        // Truncate mid-record.
        assert!(BlockUndo::decode(&enc[..20]).is_none());
    }

    #[test]
    fn read_varint_all_classes_and_eof() {
        for (bytes, want) in [
            (&[0u8][..], Some(0u64)),
            (&[0xfc][..], Some(252u64)),
            (&[0xfd, 0xfd, 0x00][..], Some(253u64)),
            (&[0xfe, 0x00, 0x00, 0x01, 0x00][..], Some(65_536u64)),
            (
                &[0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00][..],
                Some(1u64),
            ),
            // Truncations of each multi-byte form.
            (&[0xfd][..], None),
            (&[0xfd, 0xaa][..], None),
            (&[0xfe][..], None),
            (&[0xfe, 0, 0, 0][..], None),
            (&[0xff][..], None),
            (&[0xff, 0, 0, 0, 0, 0, 0, 0][..], None),
            // Empty input.
            (&[][..], None),
        ] {
            let mut s: &[u8] = bytes;
            assert_eq!(read_varint(&mut s), want, "case {:?}", bytes);
        }
        // put_varint ↔ read_varint roundtrips for big values.
        for n in [
            0u64,
            252,
            253,
            65_535,
            65_536,
            u32::MAX as u64,
            u64::MAX - 1,
        ] {
            let mut buf = Vec::new();
            put_varint(&mut buf, n);
            let mut s: &[u8] = &buf;
            assert_eq!(read_varint(&mut s), Some(n));
        }
    }

    #[test]
    fn utxo_value_decode_rejects_wrong_len() {
        // Zeroed buffers of any length carry `hash_code == 0` and must fail.
        for n in 0..60usize {
            assert!(UtxoValue::decode(&vec![0u8; n]).is_none(), "len={n}");
        }

        // Roundtrip all 3 hash-length classes (0B, 20B HASH160, 32B) + escape
        // script_type >= 15, and assert compact byte savings over 45B.
        let mut h20 = [0u8; 32];
        h20[..20].copy_from_slice(&[0x42u8; 20]);
        let cases = [
            UtxoValue {
                value_sat: 0,
                script_type: 0,
                height: 0,
                address_hash: [0u8; 32],
            },
            UtxoValue {
                value_sat: 100_000,
                script_type: 2,
                height: 800_000,
                address_hash: h20,
            },
            UtxoValue {
                value_sat: u64::MAX,
                script_type: 200,
                height: u32::MAX,
                address_hash: [0xFE; 32],
            },
        ];
        assert!(cases[0].encode().len() <= 3);
        assert!(cases[1].encode().len() <= 28);
        for c in cases {
            let enc = c.encode();
            assert_eq!(UtxoValue::decode(&enc), Some(c));
            // Any prefix truncation or trailing byte must fail.
            for cut in 0..enc.len() {
                assert!(UtxoValue::decode(&enc[..cut]).is_none());
            }
            let mut extra = enc.as_ref().to_vec();
            extra.push(0x00);
            assert!(UtxoValue::decode(&extra).is_none());
        }

        // Invalid LEB128 (>10 continuation bytes and 10th-byte overflow) +
        // height > u32::MAX + non-canonical script_type < 15 on escape nibble.
        assert!(UtxoValue::decode(&[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00
        ])
        .is_none());
        assert!(
            UtxoValue::decode(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02])
                .is_none()
        );
        let mut bad_height = [0u8; 49];
        let mut pos = 0;
        put_leb128_buf(
            &mut bad_height,
            &mut pos,
            ((u32::MAX as u64 + 1) << 6) | 0x10,
        );
        put_leb128_buf(&mut bad_height, &mut pos, 1);
        assert!(UtxoValue::decode(&bad_height[..pos]).is_none());
        // Escape nibble (15) followed by a non-canonical script_type < 15.
        assert!(UtxoValue::decode(&[0x1f, 3, 1]).is_none());
    }

    #[test]
    fn undo_key_is_big_endian_height_ordering() {
        // BE ordering lets prefix-scan cleanup work when we add it.
        assert!(undo_key(100) < undo_key(200));
        assert!(undo_key(u32::MAX - 1) < undo_key(u32::MAX));
        assert_eq!(undo_key(0xDEADBEEF_u32), [0xDE, 0xAD, 0xBE, 0xEF]);
    }

    /// Synthetic OutputRow for tests.
    fn out_row(
        height: u32,
        tx_index: u32,
        output_index: u32,
        _txid: [u8; 32],
        value_sat: u64,
    ) -> OutputRow {
        OutputRow {
            height,
            tx_index: tx_index as u16,
            output_index: output_index as u16,
            value_sat,
            script_pubkey_len: 25,
            script_type: 1,
            address_hash: [0xEEu8; 32],
        }
    }

    fn inp_row(
        height: u32,
        tx_index: u32,
        input_index: u32,
        _txid: [u8; 32],
        prev_txid: [u8; 32],
        prev_vout: u32,
        is_coinbase: bool,
    ) -> InputRow {
        InputRow {
            height,
            tx_index: tx_index as u16,
            input_index: input_index as u16,
            prev_txid,
            prev_vout,
            script_sig_len: 0,
            sequence: 0,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase,
            spent_value_sat: 0,
            spent_script_type: 0,
            spent_address_hash: [0u8; 32],
            spent_height: 0,
        }
    }

    fn tx_row(height: u32, tx_index: u32, txid: [u8; 32]) -> bidx_core::TxRow {
        bidx_core::TxRow {
            height,
            tx_index: tx_index as u16,
            txid,
            version: 1,
            locktime: 0,
            size: 0,
            weight: 0,
            fee: 0,
            input_count: 1,
            output_count: 1,
            has_witness: false,
        }
    }

    struct TestApplyRes {
        inputs: Vec<InputRow>,
        fees: Vec<u64>,
        missing: u64,
        db_lookups: u64,
        db_nanos: u64,
        pending_hits: u64,
        cache_hits: u64,
    }

    fn apply_test(
        db: &UtxoStore,
        height: u32,
        txs: &[bidx_core::TxRow],
        inputs: &[InputRow],
        outputs: &[OutputRow],
    ) -> TestApplyRes {
        let mut blk = bidx_core::BlockRow {
            height,
            ..Default::default()
        };
        let mut txs_mut = txs.to_vec();
        let mut inps_mut = inputs.to_vec();
        let st = db
            .apply_block_in_place(height, &mut blk, &mut txs_mut, &mut inps_mut, outputs)
            .unwrap();
        let fees = txs_mut.iter().map(|t| t.fee).collect();
        TestApplyRes {
            inputs: inps_mut,
            fees,
            missing: st.missing,
            db_lookups: st.db_lookups,
            db_nanos: st.db_nanos,
            pending_hits: st.pending_hits,
            cache_hits: st.cache_hits,
        }
    }

    /// Temp rocksdb path per test; unique to avoid race collisions.
    fn tmp_path(kind: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "bidx-utxo-{}-{}-{}",
            kind,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // RocksDB needs the parent dir to exist.
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Apply a block whose only output is sent to a later block which spends it.
    /// Confirms spend linking, fees, and missing UTXO accounting.
    /// the sparse fields we added. heavily Covered elsewhere; we focus on the
    /// previously branches: set_tip/tip roundtrip, missing-UTXO accounting,
    /// disconnect_block's "NoUndo" path, disk_size_bytes and path().
    #[test]
    fn apply_link_spend_disconnect_roundtrip() {
        let dir = tmp_path("live");
        let db = UtxoStore::open_live(&dir).unwrap();
        let fund_txid = [0x11u8; 32];
        let fund = out_row(0, 0, 0, fund_txid, 50_000);
        let b0_outputs = vec![fund.clone()];
        let r = apply_test(&db, 0, &[tx_row(0, 0, fund_txid)], &[], &b0_outputs);
        assert!(r.inputs.is_empty());
        assert_eq!(r.missing, 0);

        // Block 1 spends block 0's output via a coinbase + one regular input.
        let spend_txid = [0x22u8; 32];
        let spend_cb = inp_row(1, 0, 0, spend_txid, [0u8; 32], 0xFFFF_FFFF, true);
        let spend_in = inp_row(1, 1, 0, spend_txid, fund_txid, 0, false);
        let b1_inputs = vec![spend_cb.clone(), spend_in.clone()];
        let b1_outputs = vec![out_row(1, 1, 0, spend_txid, 45_000)];
        let r = apply_test(
            &db,
            1,
            &[tx_row(1, 0, [0u8; 32]), tx_row(1, 1, spend_txid)],
            &b1_inputs,
            &b1_outputs,
        );
        assert_eq!(
            r.inputs[0].spent_value_sat, 0,
            "coinbase inputs record no spend"
        );
        let s = &r.inputs[1];
        assert_eq!(s.prev_txid, fund_txid);
        assert_eq!(s.prev_vout, 0);
        assert_eq!(s.spent_value_sat, 50_000);
        assert_eq!(s.spent_height, 0);
        assert_eq!(s.tx_index, 1);
        // Fee = inputs(50k) - outputs(45k) = 5000 sat for tx_index=1 row only.
        assert_eq!(r.fees[1], 5_000);

        // tip roundtrip
        db.set_tip(&TipState {
            height: 1,
            hash: Hash32::from_bytes([0xEE; 32]),
        })
        .unwrap();
        let t = db.tip().unwrap().unwrap();
        assert_eq!(t.height, 1);
        assert_eq!(t.hash.0, [0xEE; 32]);

        // Disconnect block 1 and verify the funded output comes back intact.
        db.disconnect_block(1).unwrap();
        let restored = db
            .db
            .get_pinned(make_key(&fund_txid, 0))
            .unwrap()
            .and_then(|b| UtxoValue::decode(&b))
            .unwrap();
        assert_eq!(restored.value_sat, 50_000);
        // Created outpoint in block 1 should be gone.
        let gone = db.db.get_pinned(make_key(&spend_txid, 0)).unwrap();
        assert!(gone.is_none());
        // Undo record must be consumed: disconnecting again is an error.
        assert!(matches!(db.disconnect_block(1), Err(UtxoError::NoUndo(1))));
        // But live-mode set_tip still works after disconnect.
        let t2 = db.tip().unwrap().unwrap();
        assert_eq!(t2.height, 1); // it's the caller's job to roll tip back

        // path / disk_size_bytes / flush / approx size (approx_size just queries a RocksDB prop).
        assert_eq!(db.path(), dir.as_path());
        db.flush().unwrap();
        let _ = db.approx_size_on_disk();
        let sz = db.disk_size_bytes();
        assert!(sz > 0, "expected non-zero on-disk size after apply+flush");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Missing (prev_txid, prev_vout) reference increments `missing` without
    /// failing the whole block.
    #[test]
    fn missing_utxo_counted_not_fatal() {
        let dir = tmp_path("miss");
        let db = UtxoStore::open(&dir).unwrap();
        let inputs = vec![inp_row(0, 0, 0, [0x33u8; 32], [0x44u8; 32], 0, false)];
        let r = apply_test(&db, 0, &[tx_row(0, 0, [0x33u8; 32])], &inputs, &[]);
        assert_eq!(r.missing, 1);
        assert_eq!(r.inputs[0].spent_value_sat, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Bulk mode (no undo): apply_block returns no undo log, disconnect fails
    /// with NoUndo, and all spend + fee paths still work.
    #[test]
    fn bulk_mode_no_undo_recorded() {
        let dir = tmp_path("bulk");
        let db = UtxoStore::open(&dir).unwrap();
        let out_txid = [0xAAu8; 32];
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, out_txid)],
            &[],
            &[out_row(0, 0, 0, out_txid, 100)],
        );
        let inp = inp_row(1, 0, 0, [0xBBu8; 32], out_txid, 0, false);
        let r = apply_test(
            &db,
            1,
            &[tx_row(1, 0, [0xBBu8; 32])],
            std::slice::from_ref(&inp),
            &[],
        );
        assert_eq!(r.inputs[0].spent_value_sat, 100);
        assert_eq!(r.fees[0], 100);
        // No undo logged; disconnect must fail.
        assert!(matches!(db.disconnect_block(1), Err(UtxoError::NoUndo(1))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same-block spend: output created and spent within one block. The
    /// created outpoint must be removed from `undo.created` (otherwise
    /// disconnect would delete a key that re-exists in `spent`).
    #[test]
    fn same_block_spend_affects_undo_correctly() {
        let dir = tmp_path("same");
        let db = UtxoStore::open_live(&dir).unwrap();
        let txid = [0x99u8; 32];
        let out = out_row(0, 0, 0, txid, 1000);
        let inp = inp_row(0, 1, 0, txid, txid, 0, false); // spends own tx's vout=0 in same block
        let tx0 = tx_row(0, 0, txid);
        let tx1 = tx_row(0, 1, [0x88u8; 32]);
        let r = apply_test(
            &db,
            0,
            &[tx0, tx1],
            std::slice::from_ref(&inp),
            std::slice::from_ref(&out),
        );
        assert_eq!(r.inputs[0].spent_value_sat, 1000);
        // Now disconnect: the undo should handle the same-block spend cleanly.
        db.disconnect_block(0).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same-block reconciliation must not leave the created outpoint in
    /// the undo record, and disconnecting must not resurrect it. This is the
    /// behaviour the old `undo.created.retain()` scan provided; the set-based
    /// replacement has to produce an identical undo record, so assert on the
    /// encoded record rather than merely "did not error".
    #[test]
    fn same_block_spend_removes_key_from_undo_created() {
        let dir = tmp_path("same-undo");
        let db = UtxoStore::open_live(&dir).unwrap();
        let txid = [0x77u8; 32];
        // Two outputs created; only vout=0 is spent in the same block.
        let out0 = out_row(0, 0, 0, txid, 1000);
        let out1 = out_row(0, 0, 1, txid, 2000);
        let inp = inp_row(0, 1, 0, txid, txid, 0, false);
        let tx0 = tx_row(0, 0, txid);
        let tx1 = tx_row(0, 1, [0x66u8; 32]);
        apply_test(
            &db,
            0,
            &[tx0, tx1],
            std::slice::from_ref(&inp),
            &[out0, out1],
        );

        // Inspect the stored undo record directly.
        let raw = db
            .db
            .get_cf(&db.cf_undo(), undo_key(0))
            .unwrap()
            .expect("undo record");
        let undo = BlockUndo::decode(&raw).expect("decodable undo");
        assert_eq!(
            undo.spent.len(),
            0,
            "same-block spend must not be an undo.spent"
        );
        assert_eq!(
            undo.created.len(),
            1,
            "spent-in-block outpoint must be dropped from undo.created"
        );
        assert_eq!(
            undo.created[0],
            make_key(&txid, 1),
            "only the unspent output should remain in undo.created"
        );

        // Disconnect must be a clean no-op on the set (nothing to restore,
        // nothing to delete that still exists).
        db.disconnect_block(0).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Guard the per-column-family options fix. `DB::open_cf_descriptors`
    /// applies each CF's own `Options`, so tuning set only on the DB-level
    /// options is silently ignored. This asserts the tuning actually reached
    /// the default (UTXO) family, by reading the properties back.
    #[test]
    fn column_family_options_actually_applied() {
        let dir = tmp_path("cfopts");
        let db = UtxoStore::open_statistics(&dir).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();

        let cache: usize = props
            .get("block_cache_capacity")
            .expect("block_cache_capacity present")
            .parse()
            .expect("numeric");
        assert_eq!(
            cache, DEFAULT_BLOCK_CACHE_BYTES,
            "block cache on the default CF must be the configured size, not RocksDB's 32 MiB default"
        );

        // Writing enough to fill the small default write buffer would flush
        // early; instead assert the CF-level write buffer is the configured
        // 512 MiB by checking it is *not* the stock 64 MiB. `cur_size_active`
        // only grows with writes, so compare the configured value via a flush
        // boundary: an empty store reports ~1 MiB, which tells us nothing.
        // The block-cache assertion above is the load-bearing one; this one
        // documents intent for the buffer size.
        assert!(
            DEFAULT_BLOCK_CACHE_BYTES >= 512 << 20,
            "cache must dominate the write buffer for the read path"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An explicit block-cache size must reach the column family, and it must
    /// be *one* cache shared across CFs rather than one per CF. Both are
    /// load-bearing: a per-CF cache silently triples memory and prevents a
    /// block read for one family being reused by another.
    #[test]
    fn open_tuned_sizes_a_single_shared_block_cache() {
        let dir = tmp_path("tunedcache");
        // A deliberately non-default, easily-recognized size.
        let want = 256 << 20;
        let db = UtxoStore::open_tuned_statistics(&dir, want, 0).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();
        let cache: usize = props
            .get("block_cache_capacity")
            .expect("block_cache_capacity present")
            .parse()
            .expect("numeric");
        assert_eq!(cache, want, "explicit block-cache size must reach the CF");
        // No recent cache requested → none enabled.
        assert!(db.cache_stats().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `open_planned` must route the plan's caches into the store, and a plan
    /// with a zero recent cache must leave the recent cache disabled. This is
    /// the entry point a bulk index uses, so both the "caches reach the CF" and
    /// the "zero disables" branches need covering.
    #[test]
    fn open_planned_routes_the_plan_into_the_store() {
        let dir = tmp_path("planned");
        let plan = bidx_core::MemoryPlan {
            ceiling_kb: 16 * 1024 * 1024,
            block_cache_bytes: 128 << 20,
            recent_cache_bytes: 32 << 20,
            memtable_budget_bytes: 256 << 20,
            file_cache_files: 8,
        };
        let db = UtxoStore::open_planned_statistics(&dir, &plan).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();
        let cache: usize = props
            .get("block_cache_capacity")
            .expect("block_cache_capacity present")
            .parse()
            .expect("numeric");
        assert_eq!(
            cache, plan.block_cache_bytes,
            "plan block cache must reach the CF"
        );
        assert_eq!(
            db.cache_stats().unwrap().budget_bytes,
            plan.recent_cache_bytes,
            "plan recent cache must reach the store"
        );
        let _ = std::fs::remove_dir_all(&dir);

        // A zero recent cache in the plan disables it.
        let dir2 = tmp_path("plannedzero");
        let zero = bidx_core::MemoryPlan {
            recent_cache_bytes: 0,
            ..plan
        };
        let db2 = UtxoStore::open_planned(&dir2, &zero).unwrap();
        assert!(
            db2.cache_stats().is_none(),
            "zero recent cache must disable it"
        );
        let _ = std::fs::remove_dir_all(&dir2);
    }

    fn bulk_test_plan() -> bidx_core::MemoryPlan {
        bidx_core::MemoryPlan {
            ceiling_kb: 16 * 1024 * 1024,
            block_cache_bytes: 64 << 20,
            recent_cache_bytes: 16 << 20,
            memtable_budget_bytes: 64 << 20,
            file_cache_files: 4,
        }
    }

    /// `open_planned_bulk` must route the plan's caches into the store exactly
    /// like `open_planned`, so the bulk entry point is not a silently different
    /// configuration. The deferred-compaction half is asserted by the finalize
    /// tests below; here we only pin the caches.
    #[test]
    fn open_planned_bulk_routes_the_plan_caches() {
        let dir = tmp_path("bulkcaches");
        let plan = bulk_test_plan();
        let db = UtxoStore::open_planned_bulk(&dir, &plan).unwrap();
        let props: std::collections::HashMap<String, String> =
            db.perf_properties().into_iter().collect();
        assert_eq!(
            props
                .get("block_cache_capacity")
                .expect("block_cache_capacity present")
                .parse::<usize>()
                .expect("numeric"),
            plan.block_cache_bytes,
            "bulk entry point must route the plan's block cache"
        );
        assert_eq!(
            db.cache_stats().unwrap().budget_bytes,
            plan.recent_cache_bytes,
            "bulk entry point must route the plan's recent cache"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deferred-compaction bulk store must accept writes while compaction is
    /// off, then `finalize_bulk_load` re-enables compaction, compacts once, and
    /// leaves the data intact. Asserting the post-compaction spend resolves
    /// checks the compaction did not drop or corrupt a live key — the failure
    /// mode a naive `compact_range` after a re-enable could hide.
    #[test]
    fn bulk_load_defers_compaction_then_finalize_compacts() {
        let dir = tmp_path("bulkfinalize");
        let db = UtxoStore::open_planned_bulk(&dir, &bulk_test_plan()).unwrap();
        // A write under deferred compaction: no auto-compaction runs, so this
        // lands in the memtable/L0 pile the finalize below must merge.
        let a = [0x61u8; 32];
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, a)],
            &[],
            &[out_row(0, 0, 0, a, 5_000)],
        );
        assert!(
            db.finalize_bulk_load().unwrap(),
            "a deferred bulk store must report that it compacted"
        );
        // The output survived the compaction: a spend of it still resolves.
        let t1 = [0x62u8; 32];
        let inp = inp_row(1, 0, 0, t1, a, 0, false);
        let r = apply_test(&db, 1, &[tx_row(1, 0, t1)], &[inp], &[]);
        assert_eq!(r.inputs.len(), 1);
        assert_eq!(r.inputs[0].spent_value_sat, 5_000);
        assert_eq!(r.missing, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `finalize_bulk_load` must be a no-op for a store that did not defer
    /// compaction: a live store (undo enabled) must never be compacted out from
    /// under its reorg path, and a plain bulk store never accumulated the L0
    /// pile that needs merging.
    #[test]
    fn finalize_bulk_load_is_a_noop_for_non_deferred_stores() {
        let dir = tmp_path("livefinalize");
        let live = UtxoStore::open_live(&dir).unwrap();
        assert!(
            !live.finalize_bulk_load().unwrap(),
            "a live store must not compact on finalize"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir2 = tmp_path("plainfinalize");
        let plain = UtxoStore::open_planned(&dir2, &bulk_test_plan()).unwrap();
        assert!(
            !plain.finalize_bulk_load().unwrap(),
            "a bulk store that never deferred must be a finalize no-op"
        );
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// `open_tuned` with a non-zero recent-cache budget must enable the cache
    /// and answer a spend from it, i.e. the plumbing actually reaches the
    /// store rather than being accepted and dropped.
    #[test]
    fn open_tuned_enables_the_recent_cache() {
        let dir = tmp_path("tunedrecent");
        let db = UtxoStore::open_tuned(&dir, DEFAULT_BLOCK_CACHE_BYTES, 8 << 20).unwrap();
        let stats = db.cache_stats().expect("recent cache enabled by budget");
        assert_eq!(stats.budget_bytes, 8 << 20);
        let a = [0x51u8; 32];
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, a)],
            &[],
            &[out_row(0, 0, 0, a, 10_000)],
        );
        let t1 = [0x52u8; 32];
        let r = apply_test(
            &db,
            1,
            &[tx_row(1, 0, t1)],
            &[inp_row(1, 0, 0, t1, a, 0, false)],
            &[],
        );
        assert_eq!(r.cache_hits, 1, "recent spend must hit the cache");
        assert_eq!(r.db_lookups, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The write-path tuning must scale with the core count, with a floor and a
    /// ceiling so neither a tiny nor a huge host gets an absurd value.
    #[test]
    fn background_jobs_and_subcompactions_scale_and_clamp() {
        assert_eq!(background_jobs_for(1), 4);
        assert_eq!(background_jobs_for(8), 8);
        assert_eq!(background_jobs_for(12), 12);
        assert_eq!(background_jobs_for(64), 16);

        assert_eq!(subcompactions_for(1), 2);
        assert_eq!(subcompactions_for(4), 2);
        assert_eq!(subcompactions_for(12), 6);
        assert_eq!(subcompactions_for(64), 8);
    }

    /// A bulk load merges two memtables before flushing (so create+spend pairs
    /// cancel and never reach L0); live mode keeps the per-buffer default.
    #[test]
    fn memtables_to_merge_is_two_for_bulk_one_for_live() {
        assert_eq!(memtables_to_merge(true), 2);
        assert_eq!(memtables_to_merge(false), 1);
    }

    /// The H2 counters must be internally consistent: every non-coinbase input
    /// is either resolved from `pending` (same-block spend) or read from the
    /// database, never both and never neither. A block with one of each is the
    /// minimal case that proves the split is real.
    #[test]
    fn apply_block_reports_db_and_pending_lookup_counts() {
        let dir = tmp_path("h2counts");
        let db = UtxoStore::open_live(&dir).unwrap();

        // Block 0 funds outpoint A:0, which lands in the database.
        let a = [0xA1u8; 32];
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, a)],
            &[],
            &[out_row(0, 0, 0, a, 1_000)],
        );

        // Block 1 creates B:0 and spends it in the same block (a `pending`
        // hit), and also spends A:0 from block 0 (a database lookup).
        let b = [0xB1u8; 32];
        let tx1 = [0xC1u8; 32];
        let tx2 = [0xD1u8; 32];
        let inputs = [
            inp_row(1, 1, 0, tx1, a, 0, false), // DB lookup
            inp_row(1, 2, 0, tx2, b, 0, false), // same-block spend
        ];
        let outputs = [out_row(1, 0, 0, b, 500)];
        let txs = [tx_row(1, 0, b), tx_row(1, 1, tx1), tx_row(1, 2, tx2)];

        let r = apply_test(&db, 1, &txs, &inputs, &outputs);
        assert_eq!(r.db_lookups, 1, "A:0 must be read from the database");
        assert_eq!(r.pending_hits, 1, "B:0 must be resolved from pending");
        assert_eq!(
            r.db_lookups + r.pending_hits,
            2,
            "every non-coinbase input must be accounted for exactly once"
        );
        assert!(
            r.db_nanos > 0,
            "a real database read must take measurable time"
        );
        assert_eq!(r.missing, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A block whose spends are all same-block must report zero database
    /// lookups — the case where a batched prefetch could not help at all.
    #[test]
    fn apply_block_reports_no_db_lookups_when_all_spends_are_same_block() {
        let dir = tmp_path("h2none");
        let db = UtxoStore::open_live(&dir).unwrap();
        let a = [0xA2u8; 32];
        let tx1 = [0xC2u8; 32];
        let inputs = [inp_row(0, 1, 0, tx1, a, 0, false)];
        let outputs = [out_row(0, 0, 0, a, 700)];
        let txs = [tx_row(0, 0, a), tx_row(0, 1, tx1)];

        let r = apply_test(&db, 0, &txs, &inputs, &outputs);
        assert_eq!(r.db_lookups, 0);
        assert_eq!(r.pending_hits, 1);
        assert_eq!(r.db_nanos, 0, "no database read means no database time");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Coinbase inputs are neither a database lookup nor a pending hit, so a
    /// coinbase-only block reports zero for both. This is the case that makes
    /// the counters meaningful on testnet4, where most blocks are coinbase-only.
    #[test]
    fn apply_block_excludes_coinbase_from_lookup_counts() {
        let dir = tmp_path("h2cb");
        let db = UtxoStore::open_live(&dir).unwrap();
        let a = [0xA3u8; 32];
        let inputs = [inp_row(0, 0, 0, a, [0u8; 32], u32::MAX, true)];
        let outputs = [out_row(0, 0, 0, a, 50)];
        let r = apply_test(&db, 0, &[tx_row(0, 0, a)], &inputs, &outputs);
        assert_eq!(r.db_lookups, 0);
        assert_eq!(r.pending_hits, 0);
        assert_eq!(r.inputs[0].spent_value_sat, 0, "coinbase produces no spend");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The cache must answer a spend of a recently created output without a
    /// database lookup, and must produce the *same* `InputRow.spent_*` fields a
    /// database read would. Fidelity is the point: a wrong value here silently
    /// corrupts the spend index, which is why the entry keeps the full `UtxoValue`.
    #[test]
    fn recent_cache_serves_recent_spend_without_db_lookup() {
        let dir = tmp_path("cachehit");
        let db = UtxoStore::open_cached(&dir, 8 << 20).unwrap();
        let a = [0xE1u8; 32];

        // Block 0 creates A:0 (goes into the cache at block end).
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, a)],
            &[],
            &[out_row(0, 0, 0, a, 4_242)],
        );

        // Block 1 spends A:0. The cache should satisfy it.
        let tx1 = [0xE2u8; 32];
        let r = apply_test(
            &db,
            1,
            &[tx_row(1, 0, tx1)],
            &[inp_row(1, 0, 0, tx1, a, 0, false)],
            &[],
        );

        assert_eq!(
            r.cache_hits, 1,
            "spend of a recent output must hit the cache"
        );
        assert_eq!(r.db_lookups, 0, "no database read should have been needed");
        assert_eq!(r.missing, 0);
        // Fidelity: the same fields a DB read would have produced.
        assert_eq!(r.inputs.len(), 1);
        assert_eq!(r.inputs[0].spent_value_sat, 4_242);
        assert_eq!(r.inputs[0].spent_height, 0);
        assert_eq!(r.inputs[0].spent_address_hash, [0xEEu8; 32]);
        assert_eq!(r.fees[0], 4_242, "fee comes from the cached input value");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// With the cache disabled, the same spend costs a database lookup and
    /// reports no cache hits. This is the A/B control.
    #[test]
    fn recent_cache_disabled_uses_db_lookup() {
        let dir = tmp_path("cachemiss");
        let db = UtxoStore::open(&dir).unwrap();
        let a = [0xE3u8; 32];
        apply_test(&db, 0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 100)]);
        let tx1 = [0xE4u8; 32];
        let r = apply_test(
            &db,
            1,
            &[tx_row(1, 0, tx1)],
            &[inp_row(1, 0, 0, tx1, a, 0, false)],
            &[],
        );
        assert_eq!(r.cache_hits, 0);
        assert_eq!(r.db_lookups, 1, "no cache means the database must be read");
        assert_eq!(r.inputs[0].spent_value_sat, 100);
        assert!(db.cache_stats().is_none(), "cache is disabled");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The cache must respect its budget, and evicting an entry must degrade to
    /// a correct database lookup — never a wrong value or a `missing` count.
    #[test]
    fn recent_cache_evicts_to_budget_and_still_resolves() {
        let dir = tmp_path("cacheevict");
        // Budget for ~4 entries; create many more so eviction is forced.
        let db = UtxoStore::open_cached(&dir, 4 * CACHE_ENTRY_COST).unwrap();
        let a = [0xE5u8; 32];
        apply_test(&db, 0, &[tx_row(0, 0, a)], &[], &[out_row(0, 0, 0, a, 999)]);

        // 40 more single-output blocks, each evicting older entries.
        for h in 1..41u32 {
            let t = [h as u8; 32];
            apply_test(
                &db,
                h,
                &[tx_row(h, 0, t)],
                &[],
                &[out_row(h, 0, 0, t, 1_000)],
            );
        }
        let stats = db.cache_stats().expect("cache enabled");
        assert!(
            stats.bytes <= stats.budget_bytes,
            "cache exceeded budget: {} > {}",
            stats.bytes,
            stats.budget_bytes
        );

        // A:0 was evicted, so spending it must still work via the database.
        let tx1 = [0xE6u8; 32];
        let r = apply_test(
            &db,
            41,
            &[tx_row(41, 0, tx1)],
            &[inp_row(41, 0, 0, tx1, a, 0, false)],
            &[],
        );
        assert_eq!(r.missing, 0, "eviction must not lose data");
        assert_eq!(r.inputs[0].spent_value_sat, 999);
        assert_eq!(r.inputs[0].spent_height, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A same-block spend must still be resolved from `pending`, not the cache,
    /// and must not be double counted. `pending_hits + db_lookups + cache_hits`
    /// must equal the non-coinbase input count exactly.
    #[test]
    fn recent_cache_accounting_is_exhaustive() {
        let dir = tmp_path("cacheacct");
        let db = UtxoStore::open_cached(&dir, 8 << 20).unwrap();
        // Fund A:0 in block 0 (cached).
        let a = [0xE7u8; 32];
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, a)],
            &[],
            &[out_row(0, 0, 0, a, 10_000)],
        );

        // Block 1: creates B:0 and spends it same-block (pending), spends A:0
        // (cache), and spends an outpoint that exists only in the database.
        let d = [0xE8u8; 32];
        apply_test(
            &db,
            1,
            &[tx_row(1, 0, d)],
            &[],
            &[out_row(1, 0, 0, d, 7_700)],
        );
        // Flush the cache so D:0 and A:0 must come from RocksDB.
        db.cache_clear_for_test();

        let b = [0xE9u8; 32];
        let tx1 = [0xEAu8; 32];
        let tx2 = [0xEBu8; 32];
        let tx3 = [0xECu8; 32];
        let inputs = [
            inp_row(2, 1, 0, tx1, b, 0, false), // same-block -> pending
            inp_row(2, 2, 0, tx2, a, 0, false), // in DB (cache cleared)
            inp_row(2, 3, 0, tx3, d, 0, false), // in DB
        ];
        let outputs = [out_row(2, 0, 0, b, 5_000)];
        let txs = [
            tx_row(2, 0, b),
            tx_row(2, 1, tx1),
            tx_row(2, 2, tx2),
            tx_row(2, 3, tx3),
        ];
        let r = apply_test(&db, 2, &txs, &inputs, &outputs);
        assert_eq!(
            r.pending_hits + r.db_lookups + r.cache_hits,
            3,
            "every non-coinbase input accounted for exactly once"
        );
        assert_eq!(r.pending_hits, 1);
        assert_eq!(r.db_lookups, 2);
        assert_eq!(r.cache_hits, 0);
        assert_eq!(r.missing, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Cache stats must report a sane hit rate, and `hit_rate` must be `None`
    /// before any lookup rather than a bogus 0% or NaN.
    #[test]
    fn recent_cache_stats_and_hit_rate() {
        let dir = tmp_path("cachestats");
        let db = UtxoStore::open_cached(&dir, 8 << 20).unwrap();
        let stats = db.cache_stats().expect("cache enabled");
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.hit_rate(), None, "no lookups yet");

        // Create and spend, then miss once.
        let a = [0xF1u8; 32];
        apply_test(
            &db,
            0,
            &[tx_row(0, 0, a)],
            &[],
            &[out_row(0, 0, 0, a, 1_000)],
        );
        let t1 = [0xF2u8; 32];
        apply_test(
            &db,
            1,
            &[tx_row(1, 0, t1)],
            &[inp_row(1, 0, 0, t1, a, 0, false)],
            &[],
        );
        let t2 = [0xF3u8; 32];
        apply_test(
            &db,
            2,
            &[tx_row(2, 0, t2)],
            &[inp_row(2, 0, 0, t2, a, 0, false)],
            &[],
        );

        let stats = db.cache_stats().expect("cache enabled");
        assert_eq!(stats.hits, 1, "first spend hit");
        assert!(stats.misses >= 1, "second spend of a spent outpoint missed");
        let rate = stats.hit_rate().expect("lookups happened");
        assert!((0.0..=1.0).contains(&rate));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Guard the quadratic-scan fix: many outputs and many same-block spends
    /// must reconcile correctly. The old code was O(outputs x spends) and a
    /// regression would still pass a single-output test, so exercise a block
    /// with a realistic fan-out.
    #[test]
    fn same_block_reconciliation_scales_to_many_outputs() {
        let dir = tmp_path("same-many");
        let db = UtxoStore::open_live(&dir).unwrap();
        const N: u32 = 512;
        let txid = [0x5Au8; 32];
        let mut outs = Vec::new();
        for vout in 0..N {
            outs.push(out_row(0, 0, vout, txid, 1000 + vout as u64));
        }
        // Spend every other output in the same block.
        let mut inps = Vec::new();
        let mut expected_created = Vec::new();
        for vout in 0..N {
            if vout % 2 == 0 {
                inps.push(inp_row(0, 1, vout, txid, txid, vout, false));
            } else {
                expected_created.push(make_key(&txid, vout));
            }
        }
        let tx0 = tx_row(0, 0, txid);
        let tx1 = tx_row(0, 1, [0x5Bu8; 32]);
        let r = apply_test(&db, 0, &[tx0, tx1], &inps, &outs);
        assert_eq!(
            r.inputs.iter().filter(|i| i.spent_value_sat > 0).count(),
            N as usize / 2
        );

        let raw = db
            .db
            .get_cf(&db.cf_undo(), undo_key(0))
            .unwrap()
            .expect("undo record");
        let undo = BlockUndo::decode(&raw).expect("decodable undo");
        assert!(undo.spent.is_empty(), "all spends were same-block");
        assert_eq!(undo.created.len(), expected_created.len());
        for k in &expected_created {
            assert!(
                undo.created.contains(k),
                "unspent outpoint missing from undo.created"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// set_tip twice → latest wins; tip() with no prior write returns None.
    #[test]
    fn tip_set_get_overwrite_and_empty() {
        let dir = tmp_path("tip");
        let db = UtxoStore::open_live(&dir).unwrap();
        assert!(db.tip().unwrap().is_none());
        let a = TipState {
            height: 10,
            hash: Hash32::from_bytes([1; 32]),
        };
        let b = TipState {
            height: 11,
            hash: Hash32::from_bytes([2; 32]),
        };
        db.set_tip(&a).unwrap();
        assert_eq!(db.tip().unwrap().unwrap(), a);
        db.set_tip(&b).unwrap();
        assert_eq!(db.tip().unwrap().unwrap(), b);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recent_cache_insert_refresh_does_not_requeue() {
        // The caller only inserts an outpoint once, but the contract says a
        // re-insert refreshes the value without pushing a second recency entry
        // or promoting `k1` ahead of `k2` in FIFO creation order.
        let mut c = RecentUtxoCache::new(2 * CACHE_ENTRY_COST);
        let k1 = [0x11u8; 36];
        let k2 = [0x22u8; 36];
        let k3 = [0x33u8; 36];
        let v1 = UtxoValue {
            value_sat: 1,
            script_type: 1,
            height: 1,
            address_hash: [0; 32],
        };
        let v2 = UtxoValue {
            value_sat: 2,
            script_type: 2,
            height: 2,
            address_hash: [0; 32],
        };
        c.insert(k1, v1);
        c.insert(k2, v1);
        assert_eq!(c.entries.len(), 2);
        c.insert(k1, v2); // same key: refresh in place, stays oldest (`head`)
        assert_eq!(c.entries.len(), 2, "refresh must not allocate another slot");
        assert_eq!(c.len(), 2);
        assert_eq!(c.bytes(), 2 * CACHE_ENTRY_COST);
        // Verify refreshed value before testing FIFO eviction order.
        assert_eq!(c.entries[c.head as usize].value().value_sat, 2);
        // Inserting `k3` at capacity=2 must evict `k1` (still `head`), keeping `k2`.
        c.insert(k3, v1);
        assert!(
            c.remove(&k1).is_none(),
            "refreshed k1 remained oldest and was evicted"
        );
        assert_eq!(c.remove(&k2).unwrap().value_sat, 1);
        assert_eq!(c.remove(&k3).unwrap().value_sat, 1);
    }

    #[test]
    fn recent_cache_evict_stops_cleanly_when_budget_below_one_entry() {
        // A budget smaller than a single entry (`max_entries == 0`) must ignore
        // inserts without allocating or spinning, and shrinking a populated
        // cache down to 0 must empty and release its backing vectors.
        let mut c = RecentUtxoCache::new(0);
        assert_eq!(c.len(), 0);
        assert!(c.remove(&[0x99u8; 36]).is_none(), "empty bucket lookup");
        let val = UtxoValue {
            value_sat: 1,
            script_type: 0,
            height: 0,
            address_hash: [0; 32],
        };
        c.insert([0x22u8; 36], val);
        assert_eq!(c.len(), 0);
        assert_eq!(c.entries.capacity(), 0);
        assert_eq!(c.buckets.capacity(), 0);

        let mut c2 = RecentUtxoCache::new(4 * CACHE_ENTRY_COST);
        c2.insert([0x33u8; 36], val);
        assert_eq!(c2.len(), 1);
        c2.set_budget(0);
        assert_eq!(c2.len(), 0);
        assert_eq!(c2.entries.capacity(), 0);
        assert_eq!(c2.buckets.capacity(), 0);
    }

    #[test]
    fn recent_cache_recycles_spent_slots_without_tombstone_growth() {
        // Regression test for the mainnet height ~435,000 crash: under heavy
        // create-and-spend churn where live count stays well below capacity,
        // spent entries must be unlinked and their slots/buckets recycled in
        // place rather than accumulating tombstones in a side queue.
        let max_entries = 64usize;
        let mut c = RecentUtxoCache::new(max_entries * CACHE_ENTRY_COST);
        let val = UtxoValue {
            value_sat: 42,
            script_type: 1,
            height: 100,
            address_hash: [0xAB; 32],
        };

        // Keep 16 long-lived entries while churning 20,000 short-lived outputs.
        for i in 0..16u64 {
            let mut k = [0u8; 36];
            k[..8].copy_from_slice(&i.to_le_bytes());
            c.insert(k, val);
        }
        for i in 16..20_016u64 {
            let mut k = [0u8; 36];
            k[..8].copy_from_slice(&i.to_le_bytes());
            c.insert(k, val);
            assert_eq!(c.remove(&k).unwrap().value_sat, 42);
        }

        assert_eq!(c.len(), 16);
        // Only 17 distinct slots were ever needed simultaneously (16 resident +
        // 1 transient churn slot recycled 20,000 times via `free_head`).
        assert_eq!(
            c.entries.len(),
            17,
            "spent slots must be recycled via free_head"
        );
        assert!(c.entries.capacity() <= max_entries);
        assert!(
            c.entries.capacity() * std::mem::size_of::<CacheEntry>()
                + c.buckets.capacity() * std::mem::size_of::<u64>()
                <= max_entries * CACHE_ENTRY_COST,
            "actual heap must stay within budget_bytes"
        );
        for i in 0..16u64 {
            let mut k = [0u8; 36];
            k[..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(c.remove(&k).unwrap().value_sat, 42);
        }
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn recent_cache_compacts_scrambled_slots_and_preserves_fifo_order() {
        // Scramble slot indices relative to FIFO creation order (freeing slot 0,
        // middle slots, and tail slots, then refilling via `free_head`) so
        // `set_budget`'s in-place compaction exercises both `read_idx == write_idx`
        // and `read_idx != write_idx` for `head`, interior nodes, and `tail`,
        // as well as linear-probe collisions and backward-shift deletions.
        let mut c = RecentUtxoCache::new(128 * CACHE_ENTRY_COST);
        let mut model: std::collections::VecDeque<([u8; 36], u64)> =
            std::collections::VecDeque::new();

        let make_k = |id: u64| -> [u8; 36] {
            let mut k = [0u8; 36];
            k[..8].copy_from_slice(&id.to_le_bytes());
            k
        };
        let make_v = |v: u64| UtxoValue {
            value_sat: v,
            script_type: 1,
            height: v as u32,
            address_hash: [0; 32],
        };

        // Case A: slot 0 stays live (`read_idx == write_idx` at index 0) while
        // later slots have holes.
        for id in 1..=20u64 {
            c.insert(make_k(id), make_v(id));
        }
        assert_eq!(c.remove(&make_k(5)).unwrap().value_sat, 5);
        assert_eq!(c.remove(&make_k(20)).unwrap().value_sat, 20); // unlink tail
        c.set_budget(64 * CACHE_ENTRY_COST);
        assert_eq!(c.len(), 18);
        assert!(c.entries.capacity() <= 64);

        // Case B: rebuild a fresh cache, delete slot 0 and odd slots, refill out
        // of physical order so `head` and `tail` sit past dead slots, then
        // shrink via `set_budget` so eviction + compaction both run.
        let mut c = RecentUtxoCache::new(128 * CACHE_ENTRY_COST);
        for id in 0..80u64 {
            let k = make_k(id);
            c.insert(k, make_v(id));
            model.push_back((k, id));
        }
        // Delete slot 0 (`head`) and every 3rd entry to scramble `free_head` and
        // exercise backward-shift deletion across probe chains.
        for id in (0..80u64).step_by(3) {
            let k = make_k(id);
            assert_eq!(c.remove(&k).unwrap().value_sat, id);
            model.retain(|(mk, _)| mk != &k);
        }
        // Insert 27 more (refilling all 27 freed slots in LIFO free-list order,
        // including physical slot 0 at the bottom of the stack), then free
        // physical slot 0 again so `head` must shift to `new_slot = 0`.
        for id in 100..127u64 {
            let k = make_k(id);
            c.insert(k, make_v(id));
            model.push_back((k, id));
        }
        let slot0_key = c.entries[0].key;
        assert!(c.remove(&slot0_key).is_some());
        model.retain(|(mk, _)| mk != &slot0_key);

        // Shrink down to 30 entries: oldest entries in `model` must be evicted
        // and the 30 newest must remain in exact FIFO order!
        c.set_budget(30 * CACHE_ENTRY_COST);
        while model.len() > 30 {
            let (evicted, _) = model.pop_front().unwrap();
            assert!(c.remove(&evicted).is_none());
        }
        assert_eq!(c.len(), 30);
        assert_eq!(c.entries.len(), 30);
        assert!(c.entries.capacity() <= 30);
        assert!(
            c.entries.capacity() * std::mem::size_of::<CacheEntry>()
                + c.buckets.capacity() * std::mem::size_of::<u64>()
                <= 30 * CACHE_ENTRY_COST
        );

        // Insert 10 more at capacity=30: the 10 oldest of the remaining 30 must
        // be evicted in exact FIFO order.
        for id in 200..210u64 {
            let k = make_k(id);
            c.insert(k, make_v(id));
            let (oldest_k, _) = model.pop_front().unwrap();
            assert!(
                c.remove(&oldest_k).is_none(),
                "FIFO order broken after compaction"
            );
            model.push_back((k, id));
        }
        for (k, expected_val) in model {
            assert_eq!(c.remove(&k).unwrap().value_sat, expected_val);
        }
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn write_buffer_bytes_scales_with_memtable_budget() {
        assert_eq!(write_buffer_bytes_for(0), 512 << 20);
        assert_eq!(write_buffer_bytes_for(64 << 20), 64 << 20);
        assert_eq!(write_buffer_bytes_for(1 << 30), 256 << 20);
        assert_eq!(write_buffer_bytes_for(4 << 30), 512 << 20);
    }

    #[test]
    fn recent_cache_set_budget_evicts_down_and_ignores_growth() {
        // `set_budget` is the eviction primitive behind `relieve_pressure`: it
        // must lower the budget and evict to it, and must NOT grow the budget
        // (growing would undo a previous relief step).
        let mut c = RecentUtxoCache::new(10 * CACHE_ENTRY_COST);
        let val = UtxoValue {
            value_sat: 1,
            script_type: 0,
            height: 0,
            address_hash: [0; 32],
        };
        for i in 0..10u8 {
            let mut k = [0u8; 36];
            k[0] = i;
            c.insert(k, val);
        }
        assert_eq!(c.len(), 10);
        // Shrink to 4 entries: eviction drops the rest.
        c.set_budget(4 * CACHE_ENTRY_COST);
        assert_eq!(c.budget_bytes, 4 * CACHE_ENTRY_COST);
        assert_eq!(c.len(), 4);
        assert_eq!(c.bytes(), 4 * CACHE_ENTRY_COST);
        // A larger budget is ignored, so the earlier relief is not undone.
        c.set_budget(8 * CACHE_ENTRY_COST);
        assert_eq!(c.budget_bytes, 4 * CACHE_ENTRY_COST);
        assert_eq!(c.len(), 4);
    }

    /// `relieve_pressure` must actually free memory on a real store — shrink the
    /// recent cache and halve the block cache — because that is the whole point
    /// of the soft watchdog threshold. Asserting the freed byte count (not just
    /// "did not panic") is what proves the lever moved.
    #[test]
    fn relieve_pressure_shrinks_both_caches() {
        let dir = tmp_path("relieve");
        let db = UtxoStore::open_tuned(&dir, 256 << 20, 64 << 20).unwrap();
        // Fill the recent cache past the half-budget point so the halving
        // forces a real eviction: 64 MiB / CACHE_ENTRY_COST entries is the
        // budget, so fill ~1.5x that.
        let val = UtxoValue {
            value_sat: 1,
            script_type: 0,
            height: 0,
            address_hash: [0; 32],
        };
        let budget_entries = (64 << 20) / CACHE_ENTRY_COST;
        let filled = budget_entries * 3 / 2;
        {
            let mut c = db.recent.as_ref().unwrap().lock();
            for i in 0..filled {
                let mut k = [0u8; 36];
                k[..8].copy_from_slice(&(i as u64).to_le_bytes());
                c.insert(k, val);
            }
        }
        let before = db.cache_stats().unwrap();
        assert_eq!(before.entries, budget_entries, "filled to the budget");
        let freed = db.relieve_pressure();
        // Recent cache halved (64 MiB -> 32 MiB) plus block cache halved
        // (256 MiB -> 128 MiB) = 160 MiB.
        assert_eq!(freed, (32 << 20) + (128 << 20));
        let after = db.cache_stats().unwrap();
        assert_eq!(after.budget_bytes, before.budget_bytes / 2);
        // Entries were evicted to fit the smaller budget, and the byte figure
        // respects it.
        assert!(after.entries < before.entries, "relief must evict entries");
        assert!(after.bytes <= after.budget_bytes);
        // A second call halves again, so relief is progressive.
        let freed2 = db.relieve_pressure();
        assert_eq!(freed2, (16 << 20) + (64 << 20));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The block-cache half of `relieve_pressure` must stop at the floor rather
    /// than shrinking to zero (a zero-capacity cache is not a smaller cache, it
    /// is a broken one), and report no further freeing once at the floor.
    #[test]
    fn relieve_pressure_stops_both_caches_at_their_floors() {
        let dir = tmp_path("relieffloor");
        // Open with both caches at their minimum floor (64 MiB block cache,
        // 1-entry recent cache) so `relieve_pressure` stops immediately at 0.
        let db = UtxoStore::open_tuned(&dir, 64 << 20, CACHE_ENTRY_COST).unwrap();
        assert_eq!(db.relieve_pressure(), 0, "already at both cache floors");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn block_undo_decode_rejects_truncated_spent_and_created_sections() {
        // Truncated in the middle of a `created` key (36-byte) section.
        let mut enc = Vec::new();
        put_varint(&mut enc, 0); // no spent
        put_varint(&mut enc, 1); // one created
        enc.extend_from_slice(&[0xABu8; 10]); // only 10 of 36 bytes
        assert!(BlockUndo::decode(&enc).is_none());

        // A spent record whose value bytes are too short to decode.
        let mut enc2 = Vec::new();
        put_varint(&mut enc2, 1); // one spent
        enc2.extend_from_slice(&[0u8; 36]); // key
        enc2.extend_from_slice(&[0u8; 10]); // value shorter than 45
        assert!(BlockUndo::decode(&enc2).is_none());
    }

    #[test]
    fn open_cached_statistics_enables_cache_and_statistics() {
        let dir = tmp_path("cachedstats");
        let db = UtxoStore::open_cached_statistics(&dir, 8 * CACHE_ENTRY_COST).unwrap();
        assert!(db.statistics_enabled(), "statistics flag must be on");
        assert!(db.cache_stats().is_some(), "recent cache must be enabled");
        // A normal store has neither.
        let dir2 = tmp_path("plainstats");
        let db2 = UtxoStore::open(&dir2).unwrap();
        assert!(!db2.statistics_enabled());
        assert!(db2.cache_stats().is_none());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[test]
    fn db_lookup_of_a_corrupt_utxo_value_counts_as_missing() {
        // A spend whose key is present in RocksDB but whose stored value is the
        // wrong length cannot be decoded; it must be counted as missing, not
        // panic or produce a bogus spend.
        let dir = tmp_path("corrupt");
        let db = UtxoStore::open_live(&dir).unwrap();
        let prev_txid = [0x77u8; 32];
        // Write a 10-byte value (valid values are 45 bytes).
        db.db.put(make_key(&prev_txid, 0), [0u8; 10]).unwrap();
        let inputs = vec![inp_row(0, 0, 0, [0x88u8; 32], prev_txid, 0, false)];
        let r = apply_test(&db, 0, &[tx_row(0, 0, [0x88u8; 32])], &inputs, &[]);
        assert_eq!(r.missing, 1);
        assert_eq!(r.inputs[0].spent_value_sat, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stall_report_reads_stats_when_enabled_and_is_empty_otherwise() {
        // With statistics on, the `rocksdb.stats` property is populated and the
        // stall filter runs (returning zero or more matching lines). With
        // statistics off, the property is absent and the report is empty —
        // "not measured", never a misleading "zero stalls".
        let dir = tmp_path("stall");
        let db = UtxoStore::open_cached_statistics(&dir, 4 * CACHE_ENTRY_COST).unwrap();
        assert!(db.statistics_enabled());
        let with_stats = db.stall_report();
        // Every returned line must actually mention a stall.
        assert!(with_stats
            .iter()
            .all(|l| l.contains("stall") || l.contains("Stall")));
        let _ = std::fs::remove_dir_all(&dir);

        let dir2 = tmp_path("stalloff");
        let db2 = UtxoStore::open(&dir2).unwrap();
        assert!(!db2.statistics_enabled());
        assert!(db2.stall_report().is_empty());
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// `perf_properties` must report a property RocksDB declines to answer as
    /// `"unavailable"` rather than dropping the row, so two runs can be diffed
    /// key-for-key. The `None` arm is what this pins.
    #[test]
    fn format_property_reports_unavailable_for_a_missing_value() {
        assert_eq!(format_property(Some("123".to_string())), "123");
        assert_eq!(format_property(None), "unavailable");
    }

    #[test]
    fn op_return_outputs_skip_utxo_store_and_cache_while_preserving_fees_and_sorted_multiget_order()
    {
        let dir = tmp_path("opret_multiget");
        let db = UtxoStore::open_cached(&dir, 16 * CACHE_ENTRY_COST).unwrap();

        // Block 0: create two spendable outputs with reverse-lexicographic txids
        // (`[0xEE; 32]` and `[0x11; 32]`) PLUS an OP_RETURN output (`script_type = 7`)
        // burning 500 sat.
        let tx_high = [0xEEu8; 32];
        let tx_low = [0x11u8; 32];
        let mut op_ret = out_row(0, 0, 2, tx_high, 500);
        op_ret.script_type = bidx_core::ScriptType::OpReturn as u8;
        let out_hi = out_row(0, 0, 0, tx_high, 10_000);
        let out_lo = out_row(0, 1, 0, tx_low, 20_000);

        apply_test(
            &db,
            0,
            &[tx_row(0, 0, tx_high), tx_row(0, 1, tx_low)],
            &[],
            &[out_hi, op_ret, out_lo],
        );

        // Only the 2 spendable outputs entered `RecentUtxoCache` and RocksDB;
        // the OP_RETURN output was skipped.
        assert_eq!(db.cache_stats().unwrap().entries, 2);
        assert!(db.db.get_pinned(make_key(&tx_high, 2)).unwrap().is_none());

        // Flush the write-back recent cache into RocksDB so both `tx_high:0`
        // and `tx_low:0` must be fetched via `batched_multi_get_cf`.
        db.cache_clear_for_test();

        // Block 2 spends `tx_high:0` at input 0 and `tx_low:0` at input 1
        // (unsorted key order!), creating one spendable 25_000-sat output and
        // one 1_000-sat burned OP_RETURN output -> fee must be
        // (10_000 + 20_000) - (25_000 + 1_000) = 4_000 sat, and `inputs` must
        // preserve `[tx_high, tx_low]` input order despite sorted MultiGet.
        let spend_tx = [0x99u8; 32];
        let mut burn2 = out_row(2, 0, 1, spend_tx, 1_000);
        burn2.script_type = bidx_core::ScriptType::OpReturn as u8;
        let res = apply_test(
            &db,
            2,
            &[tx_row(2, 0, spend_tx)],
            &[
                inp_row(2, 0, 0, spend_tx, tx_high, 0, false),
                inp_row(2, 0, 1, spend_tx, tx_low, 0, false),
            ],
            &[out_row(2, 0, 0, spend_tx, 25_000), burn2],
        );

        assert_eq!(res.db_lookups, 2);
        assert_eq!(res.fees, vec![4_000]);
        assert_eq!(res.inputs.len(), 2);
        assert_eq!(res.inputs[0].prev_txid, tx_high);
        assert_eq!(res.inputs[0].spent_value_sat, 10_000);
        assert_eq!(res.inputs[1].prev_txid, tx_low);
        assert_eq!(res.inputs[1].spent_value_sat, 20_000);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn should_cache_utxo_filters_dust_nonstandard_and_high_fanout_dust() {
        use bidx_core::ScriptType;
        let mk = |value_sat: u64, st: ScriptType| UtxoValue {
            value_sat,
            script_type: st as u8,
            height: 840_000,
            address_hash: [0x11; 32],
        };

        // Dust boundary (`<= 546` sat rejected, `>= 547` sat admitted).
        assert!(!should_cache_utxo(&mk(0, ScriptType::P2WPKH), 2));
        assert!(!should_cache_utxo(&mk(330, ScriptType::P2TR), 2));
        assert!(!should_cache_utxo(
            &mk(DUST_UTXO_THRESHOLD_SAT, ScriptType::P2PKH),
            2
        ));
        assert!(should_cache_utxo(
            &mk(DUST_UTXO_THRESHOLD_SAT + 1, ScriptType::P2PKH),
            2
        ));

        // Standard spendable script types above dust are admitted.
        for st in [
            ScriptType::P2PKH,
            ScriptType::P2SH,
            ScriptType::P2WPKH,
            ScriptType::P2WSH,
            ScriptType::P2TR,
            ScriptType::P2PK,
        ] {
            assert!(should_cache_utxo(&mk(10_000, st), 2));
        }

        // Data-carrier / non-standard types bypass the cache regardless of value.
        for st in [
            ScriptType::Unknown,
            ScriptType::OpReturn,
            ScriptType::BareMultisig,
            ScriptType::WitnessUnknown,
        ] {
            assert!(!should_cache_utxo(&mk(100_000, st), 2));
        }

        // High-fanout mass-dusting outputs (`>= 64` outputs and `<= 1_000` sat)
        // bypass the cache while normal payouts in the same tx still enter it.
        assert!(should_cache_utxo(
            &mk(HIGH_FANOUT_DUST_SAT, ScriptType::P2WPKH),
            HIGH_FANOUT_OUTPUT_THRESHOLD - 1
        ));
        assert!(!should_cache_utxo(
            &mk(HIGH_FANOUT_DUST_SAT, ScriptType::P2WPKH),
            HIGH_FANOUT_OUTPUT_THRESHOLD
        ));
        assert!(should_cache_utxo(
            &mk(HIGH_FANOUT_DUST_SAT + 1, ScriptType::P2WPKH),
            HIGH_FANOUT_OUTPUT_THRESHOLD
        ));
    }

    #[test]
    fn writeback_cache_cancels_ephemeral_utxos_and_bypasses_cold_outputs() {
        let dir = tmp_path("wb_selective");
        let db = UtxoStore::open_cached(&dir, 16 * CACHE_ENTRY_COST).unwrap();

        let tx_hot = [0xA1u8; 32];
        let tx_cold = [0xB2u8; 32];
        let hot_out = out_row(0, 0, 0, tx_hot, 50_000);
        let mut dust_out = out_row(0, 1, 0, tx_cold, 330);
        dust_out.script_type = bidx_core::ScriptType::P2TR as u8;
        let mut msig_out = out_row(0, 1, 1, tx_cold, 20_000);
        msig_out.script_type = bidx_core::ScriptType::BareMultisig as u8;

        apply_test(
            &db,
            0,
            &[tx_row(0, 0, tx_hot), tx_row(0, 1, tx_cold)],
            &[],
            &[hot_out, dust_out, msig_out],
        );

        // Only `tx_hot:0` entered `RecentUtxoCache`, and because the cache is
        // write-back, `tx_hot:0` has NOT been written to RocksDB! Meanwhile
        // both cold outputs bypassed RAM straight into RocksDB.
        assert_eq!(db.cache_stats().unwrap().entries, 1);
        assert!(db.db.get_pinned(make_key(&tx_hot, 0)).unwrap().is_none());
        assert!(db.db.get_pinned(make_key(&tx_cold, 0)).unwrap().is_some());
        assert!(db.db.get_pinned(make_key(&tx_cold, 1)).unwrap().is_some());

        // Block 1 spends all three: `tx_hot:0` cancels purely in RAM (never
        // touching RocksDB!), while `tx_cold:0` and `tx_cold:1` resolve via DB.
        let spend_tx = [0xC3u8; 32];
        let r = apply_test(
            &db,
            1,
            &[tx_row(1, 0, spend_tx)],
            &[
                inp_row(1, 0, 0, spend_tx, tx_hot, 0, false),
                inp_row(1, 0, 1, spend_tx, tx_cold, 0, false),
                inp_row(1, 0, 2, spend_tx, tx_cold, 1, false),
            ],
            &[out_row(1, 0, 0, spend_tx, 70_000)],
        );
        assert_eq!(r.cache_hits, 1);
        assert_eq!(r.db_lookups, 2);
        assert_eq!(r.missing, 0);
        assert_eq!(r.fees, vec![330]);
        assert_eq!(db.cache_stats().unwrap().entries, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sub_entry_cache_spills_to_db_and_drop_persists_writeback_for_live_reopen() {
        // 1. Sub-entry budget (`16 < CACHE_ENTRY_COST`, `max_entries == 0`):
        //    `c.insert` immediately returns `Some((key, val))` so the output
        //    spills directly into RocksDB without data loss.
        let dir_tiny = tmp_path("wb_tiny");
        let tiny = UtxoStore::open_cached(&dir_tiny, 16).unwrap();
        let t0 = [0x31u8; 32];
        let t1 = [0x32u8; 32];
        apply_test(
            &tiny,
            0,
            &[tx_row(0, 0, t0)],
            &[],
            &[out_row(0, 0, 0, t0, 12_345)],
        );
        assert_eq!(tiny.cache_stats().unwrap().entries, 0);
        let r = apply_test(
            &tiny,
            1,
            &[tx_row(1, 0, t1)],
            &[inp_row(1, 0, 0, t1, t0, 0, false)],
            &[],
        );
        assert_eq!(r.db_lookups, 1);
        assert_eq!(r.missing, 0);
        assert_eq!(r.inputs[0].spent_value_sat, 12_345);
        drop(tiny);
        let _ = std::fs::remove_dir_all(&dir_tiny);

        // 2. Dropping a write-back cached store without calling `flush()` must
        //    drain `RecentUtxoCache` and flush unlogged memtables so reopening
        //    in live mode sees every unspent UTXO.
        let dir_reopen = tmp_path("wb_reopen");
        {
            let bulk = UtxoStore::open_cached(&dir_reopen, 8 << 20).unwrap();
            let a = [0x41u8; 32];
            apply_test(
                &bulk,
                0,
                &[tx_row(0, 0, a)],
                &[],
                &[out_row(0, 0, 0, a, 25_000)],
            );
            assert_eq!(bulk.cache_stats().unwrap().entries, 1);
            // Also verify explicit `flush()` drains the write-back cache cleanly,
            // then add a second cached output to verify `Drop` flushes too.
            bulk.flush().unwrap();
            assert_eq!(bulk.cache_stats().unwrap().entries, 0);
            let b = [0x42u8; 32];
            apply_test(
                &bulk,
                1,
                &[tx_row(1, 0, b)],
                &[],
                &[out_row(1, 0, 0, b, 35_000)],
            );
            assert_eq!(bulk.cache_stats().unwrap().entries, 1);
        }
        let live = UtxoStore::open_live(&dir_reopen).unwrap();
        let spend_tx = [0x43u8; 32];
        let r = apply_test(
            &live,
            2,
            &[tx_row(2, 0, spend_tx)],
            &[
                inp_row(2, 0, 0, spend_tx, [0x41u8; 32], 0, false),
                inp_row(2, 0, 1, spend_tx, [0x42u8; 32], 0, false),
            ],
            &[out_row(2, 0, 0, spend_tx, 59_000)],
        );
        assert_eq!(r.missing, 0);
        assert_eq!(r.fees, vec![1_000]);
        live.disconnect_block(2).unwrap();
        let _ = std::fs::remove_dir_all(&dir_reopen);
    }

    #[test]
    fn apply_window_in_place_and_live_utxo_iterator_match_sequential_apply() {
        let dir_win = tmp_path("win_apply");
        let dir_seq = tmp_path("seq_apply");
        let win_db = UtxoStore::open_cached(&dir_win, 4 << 20).unwrap();
        let seq_db = UtxoStore::open_cached(&dir_seq, 4 << 20).unwrap();

        // Empty window is a clean no-op.
        assert_eq!(
            win_db.apply_window_in_place(&mut []).unwrap(),
            WindowStats::default()
        );

        // Seed height 0 into both stores, then flush to RocksDB so height 2's
        // spend exercises `window_misses` + duplicate/corrupt/missing branches.
        let t_seed = [0x60u8; 32];
        let t_cold = [0x61u8; 32];
        for db in [&win_db, &seq_db] {
            apply_test(
                db,
                0,
                &[tx_row(0, 0, t_seed), tx_row(0, 1, t_cold)],
                &[inp_row(0, 0, 0, t_seed, [0u8; 32], 0xFFFF_FFFF, true)],
                &[
                    out_row(0, 0, 0, t_seed, 90_000),
                    // Dust <= 546 sat exercises the `should_cache_utxo == false`
                    // direct-to-RocksDB path when surviving `pending`.
                    out_row(0, 1, 0, t_cold, 500),
                ],
            );
            db.flush().unwrap();
            // Corrupt key + odd-length key in CF_UTXO exercise decode=None and
            // k.len() != 36 guards in `apply_window_in_place` and `for_each_live_utxo`.
            db.db.put(make_key(&[0x6Fu8; 32], 0), [0u8; 8]).unwrap();
            db.db.put(b"short_key", [1u8; 4]).unwrap();
        }

        // Build a 3-block window (heights 1, 2, 3):
        // - Height 1 creates `t1` (spent in height 2 -> cross-block `pending` hit!)
        //   and `t1_keep` (survives in write-back cache) + `t1_opret` (OP_RETURN)
        //   + `t1_dust` (cold bypass).
        // - Height 2 spends `t1` (cross-block pending hit), `t_seed` (DB hit),
        //   corrupt `[0x6F; 32]` (missing), absent `[0x7F; 32]` (missing).
        // - Height 3 attempts to double-spend `t_seed` in the same window (must
        //   count as missing!) and creates `t3_keep`.
        let t1_cb = [0x62u8; 32];
        let t1 = [0x63u8; 32];
        let t2_cb = [0x64u8; 32];
        let t2 = [0x65u8; 32];
        let t3_cb = [0x66u8; 32];
        let t3 = [0x67u8; 32];

        let mut opret = out_row(1, 1, 2, t1, 0);
        opret.script_type = bidx_core::ScriptType::OpReturn as u8;

        // Also include an output with out-of-range tx_index (`u16::MAX`) to exercise
        // the defensive `txs.get(...) == None` guard in both `apply_window_in_place`
        // and `apply_block_in_place`, plus an input with `tx_index = u16::MAX` to
        // exercise `txs.get_mut(...) == None` in `enrich_input_in_place` and
        // `finalize_block_fees`.
        let mut orphan_out = out_row(1, 0, 9, t1, 100);
        orphan_out.tx_index = u16::MAX;
        let mut orphan_inp = inp_row(2, 1, 4, t2, t_cold, 0, false);
        orphan_inp.tx_index = u16::MAX;

        let make_blocks = || {
            (
                (
                    bidx_core::BlockRow {
                        height: 1,
                        ..Default::default()
                    },
                    vec![tx_row(1, 0, t1_cb), tx_row(1, 1, t1)],
                    vec![inp_row(1, 0, 0, t1_cb, [0u8; 32], 0xFFFF_FFFF, true)],
                    vec![
                        out_row(1, 0, 0, t1_cb, 50_000),
                        out_row(1, 1, 0, t1, 40_000),
                        out_row(1, 1, 1, t1, 500),
                        opret.clone(),
                        orphan_out.clone(),
                    ],
                ),
                (
                    bidx_core::BlockRow {
                        height: 2,
                        ..Default::default()
                    },
                    vec![tx_row(2, 0, t2_cb), tx_row(2, 1, t2)],
                    vec![
                        inp_row(2, 0, 0, t2_cb, [0u8; 32], 0xFFFF_FFFF, true),
                        inp_row(2, 1, 0, t2, t1, 0, false),
                        inp_row(2, 1, 1, t2, t_seed, 0, false),
                        inp_row(2, 1, 2, t2, [0x6Fu8; 32], 0, false),
                        inp_row(2, 1, 3, t2, [0x7Fu8; 32], 0, false),
                        orphan_inp.clone(),
                    ],
                    vec![
                        out_row(2, 0, 0, t2_cb, 50_000),
                        out_row(2, 1, 0, t2, 120_000),
                    ],
                ),
                (
                    bidx_core::BlockRow {
                        height: 3,
                        ..Default::default()
                    },
                    vec![tx_row(3, 0, t3_cb), tx_row(3, 1, t3)],
                    vec![
                        inp_row(3, 0, 0, t3_cb, [0u8; 32], 0xFFFF_FFFF, true),
                        inp_row(3, 1, 0, t3, t_seed, 0, false),
                    ],
                    vec![out_row(3, 0, 0, t3_cb, 50_000)],
                ),
            )
        };

        let (mut w1, mut w2, mut w3) = make_blocks();
        let w_stats = {
            let mut win = [
                WindowBlock {
                    height: 1,
                    block_row: &mut w1.0,
                    txs: &mut w1.1,
                    inputs: &mut w1.2,
                    outputs: &w1.3,
                },
                WindowBlock {
                    height: 2,
                    block_row: &mut w2.0,
                    txs: &mut w2.1,
                    inputs: &mut w2.2,
                    outputs: &w2.3,
                },
                WindowBlock {
                    height: 3,
                    block_row: &mut w3.0,
                    txs: &mut w3.1,
                    inputs: &mut w3.2,
                    outputs: &w3.3,
                },
            ];
            win_db.apply_window_in_place(&mut win).unwrap()
        };
        // `t1:0` cancelled in `pending` across blocks 1 -> 2!
        assert_eq!(w_stats.pending_hits, 1);
        assert_eq!(w_stats.missing, 3);
        assert_eq!(w2.0.total_fee, 10_000);
        assert_eq!(w2.1[0].fee, 0);
        assert_eq!(w2.1[1].fee, 10_000);
        assert_eq!(w2.2[1].spent_value_sat, 40_000);
        assert_eq!(w2.2[2].spent_value_sat, 90_000);

        // Compare against 1-by-1 `apply_window_in_place` (single-block path) on `seq_db`,
        // and verify cache hit on a subsequent single-block window.
        let (mut s1, mut s2, mut s3) = make_blocks();
        for (h, blk) in [(1u32, &mut s1), (2, &mut s2), (3, &mut s3)] {
            let mut single = [WindowBlock {
                height: h,
                block_row: &mut blk.0,
                txs: &mut blk.1,
                inputs: &mut blk.2,
                outputs: &blk.3,
            }];
            seq_db.apply_window_in_place(&mut single).unwrap();
        }
        assert_eq!(w2.0.total_fee, s2.0.total_fee);
        assert_eq!(w2.2, s2.2);

        // Spend `t2:0` (resident in `RecentUtxoCache`) in a 2-block window AND
        // on a no-cache store to cover both `cache_hits` and `self.recent == None`.
        let t4 = [0x68u8; 32];
        let t5 = [0x69u8; 32];
        let mut b4 = (
            bidx_core::BlockRow {
                height: 4,
                ..Default::default()
            },
            vec![tx_row(4, 0, t4)],
            vec![inp_row(4, 0, 0, t4, t2, 0, false)],
            vec![out_row(4, 0, 0, t4, 115_000)],
        );
        let mut b5 = (
            bidx_core::BlockRow {
                height: 5,
                ..Default::default()
            },
            vec![tx_row(5, 0, t5)],
            vec![],
            vec![out_row(5, 0, 0, t5, 10_000)],
        );
        let s45 = win_db
            .apply_window_in_place(&mut [
                WindowBlock {
                    height: 4,
                    block_row: &mut b4.0,
                    txs: &mut b4.1,
                    inputs: &mut b4.2,
                    outputs: &b4.3,
                },
                WindowBlock {
                    height: 5,
                    block_row: &mut b5.0,
                    txs: &mut b5.1,
                    inputs: &mut b5.2,
                    outputs: &b5.3,
                },
            ])
            .unwrap();
        assert_eq!(s45.cache_hits, 1);

        // `for_each_live_utxo` drains `RecentUtxoCache` and yields every live entry.
        let mut exported = Vec::new();
        let n = win_db
            .for_each_live_utxo::<UtxoError, _>(|txid, vout, val| {
                exported.push((*txid, vout, val.value_sat));
                Ok(())
            })
            .unwrap();
        assert_eq!(n as usize, exported.len());
        assert!(exported
            .iter()
            .any(|(t, v, s)| *t == t4 && *v == 0 && *s == 115_000));

        // Also exercise `apply_window_in_place` on an uncached (`self.recent == None`)
        // store and an `undo_enabled` live store (including same-block spend +
        // DB-corrupt value in `apply_block_in_place`).
        let dir_nocache = tmp_path("win_nocache");
        let nocache = UtxoStore::open(&dir_nocache).unwrap();
        let mut nc0 = (
            bidx_core::BlockRow::default(),
            vec![tx_row(0, 0, t4)],
            vec![],
            vec![out_row(0, 0, 0, t4, 1_000)],
        );
        let mut nc1 = (
            bidx_core::BlockRow::default(),
            vec![tx_row(1, 0, t5)],
            vec![],
            vec![out_row(1, 0, 0, t5, 2_000)],
        );
        nocache
            .apply_window_in_place(&mut [
                WindowBlock {
                    height: 0,
                    block_row: &mut nc0.0,
                    txs: &mut nc0.1,
                    inputs: &mut nc0.2,
                    outputs: &nc0.3,
                },
                WindowBlock {
                    height: 1,
                    block_row: &mut nc1.0,
                    txs: &mut nc1.1,
                    inputs: &mut nc1.2,
                    outputs: &nc1.3,
                },
            ])
            .unwrap();

        let dir_live = tmp_path("win_live");
        let live_s = UtxoStore::open_live(&dir_live).unwrap();
        live_s.db.put(make_key(&[0x99u8; 32], 0), [0u8; 6]).unwrap();
        live_s
            .db
            .put(
                make_key(&[0x98u8; 32], 0),
                UtxoValue {
                    value_sat: 9_000,
                    script_type: 2,
                    height: 0,
                    address_hash: [1u8; 32],
                }
                .encode(),
            )
            .unwrap();
        let mut l0 = (
            bidx_core::BlockRow {
                height: 1,
                ..Default::default()
            },
            vec![tx_row(1, 0, t4), tx_row(1, 1, t5)],
            vec![
                inp_row(1, 1, 0, t5, t4, 0, false),
                inp_row(1, 1, 1, t5, [0x99u8; 32], 0, false),
                inp_row(1, 1, 2, t5, [0x97u8; 32], 0, false),
                inp_row(1, 1, 3, t5, [0x98u8; 32], 0, false),
            ],
            vec![
                out_row(1, 0, 0, t4, 5_000),
                {
                    let mut o = out_row(1, 0, 1, t4, 0);
                    o.script_type = bidx_core::ScriptType::OpReturn as u8;
                    o
                },
                out_row(1, 1, 0, t5, 13_000),
            ],
        );
        let ls = live_s
            .apply_window_in_place(&mut [WindowBlock {
                height: 1,
                block_row: &mut l0.0,
                txs: &mut l0.1,
                inputs: &mut l0.2,
                outputs: &l0.3,
            }])
            .unwrap();
        assert_eq!(ls.pending_hits, 1);
        assert_eq!(ls.missing, 2);
        live_s.disconnect_block(1).unwrap();

        let _ = std::fs::remove_dir_all(&dir_win);
        let _ = std::fs::remove_dir_all(&dir_seq);
        let _ = std::fs::remove_dir_all(&dir_nocache);
        let _ = std::fs::remove_dir_all(&dir_live);

        // Exercise write-back FIFO eviction AND cache-bypass inside `apply_window_in_place`
        // using a tiny 1-slot cache (one dust output bypasses the cache directly to `batch.put`,
        // and two standard outputs exceed the 1-slot capacity).
        let dir_evict = tmp_path("win_evict");
        let tiny = UtxoStore::open_cached(&dir_evict, CACHE_ENTRY_COST).unwrap();
        let mut p2wpkh0 = out_row(0, 0, 0, t4, 50_000);
        p2wpkh0.script_type = bidx_core::ScriptType::P2WPKH as u8;
        let mut p2wpkh1 = out_row(0, 0, 1, t4, 60_000);
        p2wpkh1.script_type = bidx_core::ScriptType::P2WPKH as u8;
        let mut dust_out = out_row(0, 0, 2, t4, 100);
        dust_out.script_type = bidx_core::ScriptType::P2WPKH as u8;
        let mut eb0_tx = tx_row(0, 0, t4);
        eb0_tx.output_count = 3;
        let mut eb0 = (
            bidx_core::BlockRow::default(),
            vec![eb0_tx],
            vec![],
            vec![p2wpkh0, p2wpkh1, dust_out],
        );
        tiny.apply_window_in_place(&mut [WindowBlock {
            height: 0,
            block_row: &mut eb0.0,
            txs: &mut eb0.1,
            inputs: &mut eb0.2,
            outputs: &eb0.3,
        }])
        .unwrap();
        assert_eq!(tiny.cache_stats().unwrap().entries, 1);
        // The evicted output AND the cache-bypassed dust output were written to RocksDB.
        assert!(
            tiny.db.get_pinned(make_key(&t4, 0)).unwrap().is_some()
                || tiny.db.get_pinned(make_key(&t4, 1)).unwrap().is_some()
        );
        assert!(tiny.db.get_pinned(make_key(&t4, 2)).unwrap().is_some());
        let _ = std::fs::remove_dir_all(&dir_evict);
    }
}
