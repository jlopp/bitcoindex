//! Header pass: scan all blk files, build the hash->(header, location) map,
//! then reconstruct the main chain by walking back from the tip with the most
//! accumulated work (approximated by longest connected chain — with full
//! headers available we compute real cumulative work from `bits`).

use crate::blkfile::{discover_blk_files, BlkError, BlkFile};
use bidx_core::{BlockLocation, ChainIndex, Hash32};
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

/// Reconstruct the main chain from all blk files under `blocks_dir`.
///
/// Tie-breaking follows cumulative work, not chain length, so headers on
/// stale branches (post-BIP30 fixed duplicates etc.) are handled correctly.
pub fn build_chain_index(blocks_dir: &Path) -> Result<(ChainIndex, ChainBuildStats), BlkError> {
    let files = discover_blk_files(blocks_dir)?;
    info!(files = files.len(), "header pass: scanning blk files");

    let mut headers: FxHashMap<Hash32, HeaderEntry> = FxHashMap::default();
    let mut stats = ChainBuildStats::default();

    for (file_id, path) in &files {
        let bf = BlkFile::open(path, *file_id)?;
        stats.files_scanned += 1;
        for rec in bf.records() {
            let rec = rec?;
            stats.total_records += 1;
            let work = work_from_bits(rec.header.bits);
            // First-seen wins on hash duplicates (BIP30 duplicate coinbases
            // share txids, not block hashes, so block-hash dupes indicate
            // duplicated file content — keep the earliest copy).
            headers.entry(rec.hash).or_insert(HeaderEntry {
                prev: rec.header.prev_hash,
                location: rec.location,
                work,
            });
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
            let Some(entry) = headers.get(&h) else { continue };
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

    // Tip = max cumulative work.
    let mut tip: Option<(Hash32, f64)> = None;
    for (h, &w) in cum_work.iter() {
        match tip {
            Some((_, tw)) if tw >= w => {}
            _ => tip = Some((*h, w)),
        }
    }
    let (tip_hash, _) = tip.expect("no blocks found");

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
            None => break, // truncated history (pruned or partial copy)
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
