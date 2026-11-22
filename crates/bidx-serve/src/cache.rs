//! In-process response cache.
//!
//! Hot read traffic on a block explorer is extremely skewed (the tip, recent
//! blocks, popular addresses), so a small cache in front of ClickHouse
//! absorbs most of it. Keyed by the raw request target; bodies are
//! `Arc<str>` so a hit is a refcount bump, not a copy.
//!
//! Eviction is two-generation per shard: inserts go to `young`; when `young`
//! fills, it becomes `old` and the previous `old` is dropped. A hit in `old`
//! is promoted. This approximates LRU with O(1) work and no per-entry
//! bookkeeping.

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: usize = 32;

/// Map a 64-bit hash onto `n` shards. FxHash's low bits are weak for
/// structured keys (sequential IPs, similar paths), so fold the high bits in
/// with a Fibonacci multiply first.
pub(crate) fn spread(hash: u64, n: usize) -> usize {
    ((hash.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize) % n
}

fn shard_index(key: &str) -> usize {
    let mut h = rustc_hash::FxHasher::default();
    key.hash(&mut h);
    spread(h.finish(), SHARDS)
}

#[derive(Debug, Clone)]
pub struct Cached {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Arc<str>,
    pub max_age_secs: u64,
    expires: Instant,
}

impl Cached {
    pub fn new(
        status: u16,
        content_type: &'static str,
        body: Arc<str>,
        ttl: Duration,
        max_age_secs: u64,
        now: Instant,
    ) -> Self {
        Cached {
            status,
            content_type,
            body,
            max_age_secs,
            expires: now + ttl,
        }
    }
}

#[derive(Default)]
struct Shard {
    young: FxHashMap<Box<str>, Cached>,
    old: FxHashMap<Box<str>, Cached>,
}

pub struct ResponseCache {
    shards: Vec<Mutex<Shard>>,
    per_shard: usize,
}

impl ResponseCache {
    pub fn new(max_entries: usize) -> Self {
        // Two generations per shard, so each holds half the shard budget.
        let per_shard = (max_entries / SHARDS / 2).max(1);
        ResponseCache {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            per_shard,
        }
    }

    fn shard(&self, key: &str) -> &Mutex<Shard> {
        &self.shards[shard_index(key)]
    }

    pub fn get(&self, key: &str, now: Instant) -> Option<Cached> {
        let mut s = self.shard(key).lock();
        if let Some(c) = s.young.get(key) {
            if c.expires > now {
                return Some(c.clone());
            }
            s.young.remove(key);
            return None;
        }
        let (k, c) = s.old.remove_entry(key)?;
        if c.expires <= now {
            return None;
        }
        let out = c.clone();
        Self::insert_young(&mut s, self.per_shard, k, c);
        Some(out)
    }

    /// Store `entry` unless it is already expired (a zero TTL).
    pub fn put(&self, key: &str, entry: Cached, now: Instant) {
        if entry.expires <= now {
            return;
        }
        let mut s = self.shard(key).lock();
        s.old.remove(key);
        Self::insert_young(&mut s, self.per_shard, key.into(), entry);
    }

    fn insert_young(s: &mut Shard, cap: usize, key: Box<str>, entry: Cached) {
        if s.young.len() >= cap && !s.young.contains_key(&key) {
            s.old = std::mem::take(&mut s.young);
        }
        s.young.insert(key, entry);
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| {
                let s = s.lock();
                s.young.len() + s.old.len()
            })
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    #[test]
    fn hit_before_expiry_miss_after() {
        let c = ResponseCache::new(1000);
        let t0 = Instant::now();
        assert!(c.is_empty());
        c.put(
            "/a",
            Cached::new(
                200,
                "application/json",
                body("x"),
                Duration::from_secs(1),
                1,
                t0,
            ),
            t0,
        );
        let hit = c.get("/a", t0 + Duration::from_millis(999)).unwrap();
        assert_eq!(
            (hit.status, hit.content_type, &*hit.body, hit.max_age_secs),
            (200, "application/json", "x", 1)
        );
        assert!(c.get("/a", t0 + Duration::from_secs(1)).is_none());
        // Expired entry was dropped.
        assert!(c.is_empty());
        assert!(c.get("/missing", t0).is_none());
    }

    #[test]
    fn zero_ttl_is_not_stored() {
        let c = ResponseCache::new(1000);
        c.put(
            "/a",
            Cached::new(200, "t", body("x"), Duration::ZERO, 0, Instant::now()),
            Instant::now(),
        );
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn hits_share_the_body_allocation() {
        let c = ResponseCache::new(1000);
        let now = Instant::now();
        let b = body("payload");
        c.put(
            "/a",
            Cached::new(200, "t", b.clone(), Duration::from_secs(5), 5, now),
            now,
        );
        let hit = c.get("/a", now).unwrap();
        assert!(Arc::ptr_eq(&hit.body, &b));
    }

    #[test]
    fn generations_bound_size_and_promote_old_hits() {
        // per_shard = 1: every shard holds at most one young + one old entry.
        let c = ResponseCache::new(SHARDS * 2);
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        for i in 0..1000 {
            c.put(
                &format!("/k{i}"),
                Cached::new(200, "t", body("v"), ttl, 60, now),
                now,
            );
        }
        assert!(c.len() <= SHARDS * 2, "len {}", c.len());

        // Find two keys in the same shard to exercise rotation precisely.
        let c = ResponseCache::new(SHARDS * 2);
        let shard_of = shard_index;
        let keys: Vec<String> = (0..10_000).map(|i| format!("/s{i}")).collect();
        let s0 = shard_of(&keys[0]);
        let same: Vec<&String> = keys.iter().filter(|k| shard_of(k) == s0).take(3).collect();
        let (a, b, d) = (same[0].as_str(), same[1].as_str(), same[2].as_str());
        c.put(a, Cached::new(200, "t", body("A"), ttl, 60, now), now);
        c.put(b, Cached::new(200, "t", body("B"), ttl, 60, now), now); // a → old
                                                                       // Hit on `a` in old promotes it to young (b → old).
        assert_eq!(&*c.get(a, now).unwrap().body, "A");
        c.put(d, Cached::new(200, "t", body("D"), ttl, 60, now), now); // a → old, b dropped
        assert!(c.get(b, now).is_none());
        assert_eq!(&*c.get(a, now).unwrap().body, "A");
        // Re-putting a key already in young overwrites without rotating.
        let young_key = a; // promoted again by the get above
        c.put(
            young_key,
            Cached::new(200, "t", body("A2"), ttl, 60, now),
            now,
        );
        assert_eq!(&*c.get(young_key, now).unwrap().body, "A2");
        assert_eq!(&*c.get(d, now).unwrap().body, "D");
    }

    #[test]
    fn expired_old_generation_entry_is_a_miss() {
        let c = ResponseCache::new(SHARDS * 2);
        let shard_of = shard_index;
        let keys: Vec<String> = (0..10_000).map(|i| format!("/e{i}")).collect();
        let s0 = shard_of(&keys[0]);
        let same: Vec<&String> = keys.iter().filter(|k| shard_of(k) == s0).take(2).collect();
        let t0 = Instant::now();
        c.put(
            same[0],
            Cached::new(200, "t", body("x"), Duration::from_millis(10), 0, t0),
            t0,
        );
        c.put(
            same[1],
            Cached::new(200, "t", body("y"), Duration::from_secs(60), 60, t0),
            t0,
        ); // same[0] → old
        assert!(c.get(same[0], t0 + Duration::from_secs(1)).is_none());
        assert_eq!(c.len(), 1);
    }
}
