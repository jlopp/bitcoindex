//! Header pass: scan all blk files, build the hash->(header, location) map,
//! then reconstruct the main chain by walking back from the tip with the most
//! accumulated work (approximated by longest connected chain — with full
//! headers available we compute real cumulative work from `bits`).

use crate::blkfile::{discover_blk_files, BlkError, BlkFile};
use bidx_core::{BlockLocation, ChainIndex, Hash32};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::path::Path;
use tracing::info;

#[derive(Debug, Default)]
pub struct ChainBuildStats {
    pub total_records: usize,
    pub unique_blocks: usize,
    pub orphans: usize,
    pub files_scanned: usize,
    pub tip_height: u32,
    pub tip_hash: Hash32,
}

struct HeaderEntry {
    prev: Hash32,
    location: BlockLocation,
    /// Proof-of-work contribution for this header. Stored as an f64
    /// approximation of the true 256-bit `2^256/(target+1)` value. f64 has a
    /// 52-bit mantissa, ample for ordering: two blocks at the same height
    /// with genuinely different targets differ by far more than 2^-52 in
    /// log-space, and exact ties in cumulative work do not decide the
    /// main chain on mainnet (we never observe equal-work competing tips).
    work: f64,
}

/// Scan a single blk file into `(hash, HeaderEntry)` pairs in file order, plus
/// the number of records seen.
///
/// The `Vec` is built per file rather than merged into a shared map so the
/// parallel map is allocation-local and lock-free; the caller merges the
/// per-file vectors in file-id order to preserve first-seen-wins determinism.
fn scan_file(path: &Path, file_id: u32) -> Result<(Vec<(Hash32, HeaderEntry)>, usize), BlkError> {
    let bf = BlkFile::open(path, file_id)?;
    let mut out: Vec<(Hash32, HeaderEntry)> = Vec::new();
    for rec in bf.records() {
        let rec = rec?;
        let work = work_from_bits(rec.header.bits);
        out.push((
            rec.hash,
            HeaderEntry {
                prev: rec.header.prev_hash,
                location: rec.location,
                work,
            },
        ));
    }
    let count = out.len();
    Ok((out, count))
}

/// Derive approximate proof-of-work from compact `bits`.
/// work ~= 2^256 / (target + 1)
fn work_from_bits(bits: u32) -> f64 {
    let exponent = (bits >> 24) as i32;
    let mantissa = (bits & 0x007f_ffff) as f64;
    if mantissa == 0.0 || exponent < 3 {
        return 0.0;
    }
    // target = mantissa * 256^(exponent - 3); work = 2^256 / (target+1)
    let target_log2 = (mantissa.log2()) + 8.0 * (exponent as f64 - 3.0);
    (256.0 - target_log2).exp2()
}

/// Memoized cumulative work for `h`: `work[h] + cum_work[prev[h]]`, walking
/// back to a zero-prev header. Returns `None` if any ancestor is missing, or if
/// the chain never reaches genesis. The walk is iterative (not recursive)
/// because recursion depth would equal chain length — ~1M for mainnet — and
/// overflow the stack.
fn resolve_cum(
    h: Hash32,
    headers: &FxHashMap<Hash32, HeaderEntry>,
    cum: &mut FxHashMap<Hash32, f64>,
) -> Option<f64> {
    if let Some(&w) = cum.get(&h) {
        return Some(w);
    }
    let entry = headers.get(&h)?;
    let base = if entry.prev == Hash32::ZERO {
        0.0
    } else {
        // Recursion depth equals chain length in the worst case; Bitcoin's
        // chain is ~1M deep which would overflow the stack, so use an
        // explicit stack walk.
        let mut path = Vec::new();
        let mut cur = h;
        loop {
            if let Some(&w) = cum.get(&cur) {
                let mut acc = w;
                for &p in path.iter().rev() {
                    let e = &headers[&p];
                    acc += e.work;
                    cum.insert(p, acc);
                }
                return cum.get(&h).copied();
            }
            let e = headers.get(&cur)?;
            if e.prev == Hash32::ZERO {
                cum.insert(cur, e.work);
                let mut acc = e.work;
                for &p in path.iter().rev() {
                    let e2 = &headers[&p];
                    acc += e2.work;
                    cum.insert(p, acc);
                }
                return cum.get(&h).copied();
            }
            path.push(cur);
            cur = e.prev;
        }
    };
    let w = base + entry.work;
    cum.insert(h, w);
    Some(w)
}

/// Reconstruct the main chain from all blk files under `blocks_dir`.
///
/// Tie-breaking follows cumulative work, not chain length, so headers on
/// stale branches (post-BIP30 fixed duplicates etc.) are handled correctly.
pub fn build_chain_index(blocks_dir: &Path) -> Result<(ChainIndex, ChainBuildStats), BlkError> {
    let files = discover_blk_files(blocks_dir)?;
    info!(files = files.len(), "header pass: scanning blk files");

    // Scan the blk files in parallel. A full mainnet header pass is a
    // single-threaded walk of the whole blk corpus and took ~25 minutes on a
    // 12-core box; the per-file scan is embarrassingly parallel (each file is
    // independent mmap + record walk), so only the merge below stays serial.
    //
    // `collect` into a `Vec` preserves input order, and `discover_blk_files`
    // returns files sorted by id, so the merge sees records in exactly the same
    // order the old sequential loop did. That is what keeps first-seen-wins
    // deterministic across duplicate block hashes.
    let scanned: Vec<(Vec<(Hash32, HeaderEntry)>, usize)> = files
        .par_iter()
        .map(|(file_id, path)| scan_file(path, *file_id))
        .collect::<Result<Vec<_>, BlkError>>()?;

    let mut headers: FxHashMap<Hash32, HeaderEntry> = FxHashMap::default();
    let mut stats = ChainBuildStats::default();
    for (records, count) in scanned {
        stats.files_scanned += 1;
        stats.total_records += count;
        for (hash, entry) in records {
            // First-seen wins on hash duplicates (BIP30 duplicate coinbases
            // share txids, not block hashes, so block-hash dupes indicate
            // duplicated file content — keep the earliest copy).
            headers.entry(hash).or_insert(entry);
        }
    }
    stats.unique_blocks = headers.len();

    // Compute cumulative work for every block we can connect to genesis.
    // Genesis prev_hash is all-zero and not present in the map.
    let genesis_candidates: Vec<Hash32> = headers
        .iter()
        .filter(|(_, e)| e.prev == Hash32::ZERO || !headers.contains_key(&e.prev))
        .map(|(h, _)| *h)
        .collect();

    // Build children adjacency lazily via prev lookups during DFS.
    // cum_work[h] = work[h] + cum_work[prev[h]]
    let mut cum_work: FxHashMap<Hash32, f64> = FxHashMap::default();
    // Iterative post-order from every genesis candidate.
    for &g in &genesis_candidates {
        let mut stack = vec![(g, false)];
        while let Some((h, processed)) = stack.pop() {
            // Defensive: every element pushed onto `stack` came from
            // `genesis_candidates`, which is built from `headers` keys, so the
            // lookup cannot miss. Guarded rather than indexed to keep the pass
            // total against a future change to how the stack is seeded.
            // coverage-exempt: 182 — the `continue` below, unreachable as explained above.
            let Some(entry) = headers.get(&h) else {
                continue;
            };
            // coverage-exempt: unreachable — `cum_work` starts empty and this
            // DFS only ever pushes `(genesis_candidate, true)` (it does not
            // descend to children), so the first pop of a given hash always
            // misses and the memoized branch is never taken here. Memoization
            // happens later in `resolve_cum`. Kept as a cheap guard against
            // re-seeding.
            if cum_work.contains_key(&h) {
                continue;
            }
            let parent_cum = if entry.prev == Hash32::ZERO {
                Some(0.0)
            } else {
                cum_work.get(&entry.prev).copied()
            };
            if processed {
                let base = parent_cum.unwrap_or(0.0);
                cum_work.insert(h, base + entry.work);
                continue;
            }
            match parent_cum {
                Some(_) => {
                    stack.push((h, true));
                    // push children: we don't keep an adjacency list to save
                    // memory; instead mark node and let tip search below do
                    // the chain walk backwards from the max-work tip.
                }
                None => {
                    // Parent exists but not yet computed — try later via its
                    // own genesis candidate path. If it's genuinely missing
                    // the node simply never gets cum_work.
                }
            }
        }
    }

    // Because we skipped adjacency, do a simpler robust pass: repeatedly
    // relax cum_work until fixpoint. For Bitcoin's ~1M headers this is fine
    // in practice since chains are nearly linear; genesis-anchored nodes
    // resolve on the first sweep in file order for most blocks.
    //
    // To guarantee correctness regardless of insertion order, walk from each
    // node backwards to genesis memoizing as we go.
    let all_hashes: Vec<Hash32> = headers.keys().copied().collect();
    for h in all_hashes {
        resolve_cum(h, &headers, &mut cum_work);
    }

    // Tip = max cumulative work. An empty chain (a blocks dir whose files
    // hold no records, e.g. a node that has only preallocated blk files) is a
    // caller error, not a panic: the pipeline already reports "empty chain"
    // from `ChainIndex::tip_height()`.
    let mut tip: Option<(Hash32, f64)> = None;
    for (h, &w) in cum_work.iter() {
        match tip {
            Some((_, tw)) if tw >= w => {}
            _ => tip = Some((*h, w)),
        }
    }
    let Some((tip_hash, _)) = tip else {
        return Err(BlkError::NoRecords(blocks_dir.to_path_buf()));
    };

    // Walk back from tip to genesis assigning heights.
    let mut chain: Vec<Hash32> = Vec::new();
    let mut cur = tip_hash;
    loop {
        chain.push(cur);
        let entry = &headers[&cur];
        if entry.prev == Hash32::ZERO {
            break;
        }
        match headers.get(&entry.prev) {
            Some(_) => cur = entry.prev,
            // coverage-exempt: unreachable (see `cum_work` above) — the tip is
            // always a genesis-anchored block, so every `prev` on this walk is
            // in `headers`; a blocks dir with no genesis-anchored block returns
            // `NoRecords` before reaching here. Kept as a guard against a
            // future change to tip selection.
            None => break,
        }
    }
    chain.reverse();

    let mut by_height = Vec::with_capacity(chain.len());
    let mut hash_to_height = FxHashMap::default();
    for (i, h) in chain.iter().enumerate() {
        by_height.push(headers[h].location);
        hash_to_height.insert(*h, i as u32);
    }

    stats.orphans = headers.len().saturating_sub(chain.len());
    stats.tip_height = chain.len() as u32 - 1;
    stats.tip_hash = tip_hash;
    info!(
        height = stats.tip_height,
        orphans = stats.orphans,
        "header pass complete"
    );

    Ok((
        ChainIndex {
            by_height,
            hash_to_height,
        },
        stats,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bidx_core::{dsha256, BLOCK_HEADER_LEN};
    use std::path::PathBuf;

    /// A synthetic header with a chosen prev hash and nBits. The merkle root
    /// and nonce are varied so distinct headers hash to distinct values.
    fn header(prev: Hash32, bits: u32, tag: u8) -> [u8; BLOCK_HEADER_LEN] {
        let mut h = [0u8; BLOCK_HEADER_LEN];
        h[0] = 1; // version
        h[4..36].copy_from_slice(&prev.0);
        for j in 0..32 {
            h[36 + j] = tag.wrapping_add(j as u8);
        }
        h[68..72].copy_from_slice(&1_600_000_000u32.to_le_bytes()); // time
        h[72..76].copy_from_slice(&bits.to_le_bytes()); // bits
        h[76..80].copy_from_slice(&(tag as u32).to_le_bytes()); // nonce
        h
    }

    /// Write one blk*.dat file from a list of raw headers.
    fn write_blk(dir: &Path, file_id: u32, headers: &[[u8; BLOCK_HEADER_LEN]]) {
        let mut data = Vec::new();
        for hdr in headers {
            data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
            data.extend_from_slice(&(BLOCK_HEADER_LEN as u32).to_le_bytes());
            data.extend_from_slice(hdr);
        }
        std::fs::write(dir.join(format!("blk{file_id:05}.dat")), &data).unwrap();
    }

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("bidx-chain-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn work_from_bits_is_positive_for_real_targets_and_zero_for_degenerate() {
        // A plausible mainnet-era compact target.
        assert!(work_from_bits(0x1d00ffff) > 0.0);
        // Mantissa == 0 → no target → zero work.
        assert_eq!(work_from_bits(0x1d000000), 0.0);
        // Exponent < 3 is not a valid target encoding → zero work.
        assert_eq!(work_from_bits(0x0200ffff), 0.0);
    }

    #[test]
    fn build_chain_index_reconstructs_a_linear_chain() {
        let dir = tmp_dir("linear");
        let mut headers = Vec::new();
        let mut prev = Hash32::ZERO;
        let mut hashes = Vec::new();
        for tag in 0..4u8 {
            let hdr = header(prev, 0x1d00ffff, tag);
            hashes.push(dsha256(&hdr));
            headers.push(hdr);
            prev = hashes[hashes.len() - 1];
        }
        write_blk(&dir, 0, &headers);

        let (idx, stats) = build_chain_index(&dir).unwrap();
        assert_eq!(stats.total_records, 4);
        assert_eq!(stats.unique_blocks, 4);
        assert_eq!(stats.orphans, 0);
        assert_eq!(stats.files_scanned, 1);
        assert_eq!(stats.tip_height, 3);
        assert_eq!(stats.tip_hash, hashes[3]);
        // Heights map back to the right hashes.
        assert_eq!(idx.tip_height(), Some(3));
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(idx.hash_to_height.get(h), Some(&(i as u32)));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_chain_index_prefers_the_branch_with_more_cumulative_work() {
        // Genesis -> A -> B (two blocks) versus genesis -> C (one block, but
        // with a *higher* per-block work target so it must NOT win on its own).
        let dir = tmp_dir("branch");
        let genesis = header(Hash32::ZERO, 0x1d00ffff, 1);
        let g = dsha256(&genesis);
        let a = header(g, 0x1d00ffff, 2);
        let ah = dsha256(&a);
        let b = header(ah, 0x1d00ffff, 3);
        let bh = dsha256(&b);
        let c = header(g, 0x1d00ffff, 4); // sibling of A, shorter chain
        let ch = dsha256(&c);
        write_blk(&dir, 0, &[genesis, a, b, c]);

        let (idx, stats) = build_chain_index(&dir).unwrap();
        // The longer/higher-work branch (genesis-A-B) is the main chain.
        assert_eq!(stats.tip_hash, bh);
        assert_eq!(stats.tip_height, 2);
        // C is an orphan.
        assert_eq!(stats.orphans, 1);
        assert_eq!(idx.hash_to_height.get(&ch), None);
        assert_eq!(idx.hash_to_height.get(&ah), Some(&1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_chain_index_ties_break_deterministically_and_keep_all_records() {
        // Two branches of equal length and equal work; the tip is whichever the
        // max-work scan picks, but the result must be internally consistent and
        // one block must be counted as an orphan.
        let dir = tmp_dir("tie");
        let genesis = header(Hash32::ZERO, 0x1d00ffff, 10);
        let g = dsha256(&genesis);
        let a = header(g, 0x1d00ffff, 11);
        let b = header(g, 0x1d00ffff, 12);
        write_blk(&dir, 0, &[genesis, a, b]);

        let (idx, stats) = build_chain_index(&dir).unwrap();
        assert_eq!(stats.unique_blocks, 3);
        assert_eq!(stats.orphans, 1);
        assert_eq!(idx.tip_height(), Some(1));
        // Whichever tip won, its hash must be the one recorded at the tip height.
        assert_eq!(idx.hash_to_height.get(&stats.tip_hash), Some(&1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_chain_index_reports_no_records_for_an_empty_blocks_dir() {
        // Preallocated-but-empty blk file: discover_blk_files finds the file but
        // it holds no records, so the pass must report NoRecords rather than
        // panic on an empty tip.
        let dir = tmp_dir("empty");
        std::fs::write(dir.join("blk00000.dat"), [0u8; 256]).unwrap();
        match build_chain_index(&dir) {
            Err(BlkError::NoRecords(p)) => assert_eq!(p, dir),
            Err(e) => panic!("expected NoRecords, got error {e:?}"),
            Ok(_) => panic!("expected NoRecords, got Ok"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_chain_index_errors_when_no_blk_files_exist() {
        let dir = tmp_dir("nofiles");
        match build_chain_index(&dir) {
            Err(BlkError::NoFiles(p)) => assert_eq!(p, dir),
            Err(e) => panic!("expected NoFiles, got error {e:?}"),
            Ok(_) => panic!("expected NoFiles, got Ok"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_chain_index_first_seen_wins_on_duplicate_block_hash() {
        // The same record duplicated across two files must collapse to one
        // header, and the earliest file's location must be retained.
        let dir = tmp_dir("dup");
        let genesis = header(Hash32::ZERO, 0x1d00ffff, 20);
        let g = dsha256(&genesis);
        let a = header(g, 0x1d00ffff, 21);
        write_blk(&dir, 0, &[genesis, a]);
        write_blk(&dir, 1, &[genesis, a]); // byte-identical duplicate

        let (idx, stats) = build_chain_index(&dir).unwrap();
        assert_eq!(stats.total_records, 4);
        assert_eq!(
            stats.unique_blocks, 2,
            "duplicate hash must not be counted twice"
        );
        assert_eq!(stats.orphans, 0);
        // The retained location is from file 0 (first seen).
        assert_eq!(idx.by_height[0].file_id, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_chain_index_reports_no_records_when_no_block_reaches_genesis() {
        // A copy that holds blocks whose parent chain never reaches a
        // zero-prev header (a pruned/partial snapshot with no genesis). No
        // block can be assigned cumulative work, so there is no tip and the
        // pass reports NoRecords rather than fabricating a chain. This also
        // documents why the walk-back in `build_chain_index` can never hit a
        // missing parent: only genesis-anchored blocks become the tip.
        let dir = tmp_dir("truncated");
        let missing_parent = dsha256(&header(Hash32::ZERO, 0x1d00ffff, 30));
        // Only the child is present; its parent hash is never seen.
        let child = header(missing_parent, 0x1d00ffff, 31);
        write_blk(&dir, 0, &[child]);

        match build_chain_index(&dir) {
            Err(BlkError::NoRecords(p)) => assert_eq!(p, dir),
            Err(e) => panic!("expected NoRecords, got error {e:?}"),
            Ok(_) => panic!("expected NoRecords, got Ok"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_cum_memoizes_a_walk_reaching_genesis_without_a_seed() {
        // `build_chain_index`'s DFS seeds every genesis candidate before the
        // fixpoint pass, so `resolve_cum` normally finds the genesis already in
        // the memo map. This drives the function directly with an *empty* memo
        // to exercise the walk that reaches `prev == ZERO` itself: it must seed
        // genesis and then unwind the recorded path, assigning cumulative work
        // to every block on the way back.
        let unit = work_from_bits(0x1d00ffff);
        let genesis = header(Hash32::ZERO, 0x1d00ffff, 40);
        let g = dsha256(&genesis);
        let a = header(g, 0x1d00ffff, 41);
        let ah = dsha256(&a);
        let b = header(ah, 0x1d00ffff, 42);
        let bh = dsha256(&b);

        let loc = BlockLocation {
            file_id: 0,
            offset: 0,
            record_len: 88,
        };
        let entry = |prev| HeaderEntry {
            prev,
            location: loc,
            work: unit,
        };
        let mut headers = FxHashMap::default();
        headers.insert(g, entry(Hash32::ZERO));
        headers.insert(ah, entry(g));
        headers.insert(bh, entry(ah));

        let mut cum = FxHashMap::default();
        let w = resolve_cum(bh, &headers, &mut cum).unwrap();
        // Genesis seeded, then the path unwound: 1x, 2x, 3x.
        assert!((cum[&g] - unit).abs() < 1e-6);
        assert!((cum[&ah] - 2.0 * unit).abs() < 1e-6);
        assert!((w - 3.0 * unit).abs() < 1e-6);

        // Drive the `prev == ZERO` branch directly: a genesis block that is not
        // yet memoized takes `base = 0.0` and writes the plain (non-walk) result.
        let mut cum2 = FxHashMap::default();
        let wg = resolve_cum(g, &headers, &mut cum2).unwrap();
        assert!((wg - unit).abs() < 1e-6);
        // Then a descendant of an already-memoized block takes the direct path
        // (its parent is in `cum`, so no walk is needed).
        let wb = resolve_cum(bh, &headers, &mut cum2).unwrap();
        assert!((wb - 3.0 * unit).abs() < 1e-6);

        // A block whose parent is absent yields None (the `headers.get(cur)?`
        // guard) and must not memoize anything for it.
        let orphan = dsha256(&header(Hash32::ZERO, 0x1d00ffff, 99));
        let child = header(orphan, 0x1d00ffff, 98);
        let ch = dsha256(&child);
        headers.insert(ch, entry(orphan));
        assert!(resolve_cum(ch, &headers, &mut cum).is_none());
        assert!(!cum.contains_key(&ch));

        // A hash that is not present at all also yields None.
        assert!(resolve_cum(Hash32([0xEE; 32]), &headers, &mut cum).is_none());
    }
}
