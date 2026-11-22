//! API-key authentication and token-bucket rate limiting.
//!
//! Keys are looked up by the SHA-256 of the presented token, so plaintext
//! tokens never need to live in config. Each key has its own bucket; anonymous
//! traffic (when allowed) shares per-client-IP buckets. Ops endpoints bypass
//! all of this in the service so probes and scrapes are never throttled.

use crate::config::{AuthConfig, RateLimitConfig};
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::time::Instant;

const IP_SHARDS: usize = 16;
/// Per-shard IP-bucket count above which idle (fully refilled) buckets are
/// evicted. Evicting a full bucket is lossless: a fresh one is identical.
const IP_SHARD_SOFT_CAP: usize = 4096;
/// Per-shard count at which the shard is reset outright, bounding memory
/// under a flood of distinct source addresses.
const IP_SHARD_HARD_CAP: usize = 4 * IP_SHARD_SOFT_CAP;
/// Minimum spacing between idle sweeps of one shard, so a flood of new IPs
/// cannot turn every insert into a full scan.
const IP_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

struct IpShard {
    map: FxHashMap<IpAddr, Bucket>,
    last_sweep: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn full(burst: f64, now: Instant) -> Self {
        Bucket {
            tokens: burst,
            last: now,
        }
    }

    /// Refill, then take one token. `Err(secs)` = retry after.
    fn take(&mut self, rate: f64, burst: f64, now: Instant) -> Result<(), u64> {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + dt * rate).min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err(((1.0 - self.tokens) / rate).ceil().max(1.0) as u64)
        }
    }

    fn is_full_at(&self, rate: f64, burst: f64, now: Instant) -> bool {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens + dt * rate >= burst
    }
}

struct Tenant {
    name: String,
    rate: f64,
    burst: f64,
    bucket: Mutex<Bucket>,
}

/// Outcome of [`Limiter::check`]. `tenant` indexes [`Limiter::tenant_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow {
        tenant: Option<usize>,
    },
    Unauthorized,
    Limited {
        tenant: Option<usize>,
        retry_after_secs: u64,
    },
}

pub struct Limiter {
    required: bool,
    by_hash: FxHashMap<[u8; 32], usize>,
    tenants: Vec<Tenant>,
    ip_rate: f64,
    ip_burst: f64,
    ip_shards: Vec<Mutex<IpShard>>,
    soft_cap: usize,
    hard_cap: usize,
}

impl Limiter {
    pub fn new(auth: &AuthConfig, rl: &RateLimitConfig) -> Self {
        let now = Instant::now();
        let tenants: Vec<Tenant> = auth
            .keys
            .iter()
            .map(|k| Tenant {
                name: k.name.clone(),
                rate: k.rate_per_sec,
                burst: k.burst,
                bucket: Mutex::new(Bucket::full(k.burst, now)),
            })
            .collect();
        let by_hash = auth
            .keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.token_sha256, i))
            .collect();
        Limiter {
            required: auth.required,
            by_hash,
            tenants,
            ip_rate: rl.per_ip_rps,
            ip_burst: rl.per_ip_burst,
            ip_shards: (0..IP_SHARDS)
                .map(|_| {
                    Mutex::new(IpShard {
                        map: FxHashMap::default(),
                        last_sweep: None,
                    })
                })
                .collect(),
            soft_cap: IP_SHARD_SOFT_CAP,
            hard_cap: IP_SHARD_HARD_CAP,
        }
    }

    /// True when there is nothing to check (no keys, no per-IP limit), so
    /// the service can skip header parsing entirely.
    pub fn is_noop(&self) -> bool {
        self.tenants.is_empty() && !self.required && self.ip_rate <= 0.0
    }

    pub fn tenant_names(&self) -> Vec<&str> {
        self.tenants.iter().map(|t| t.name.as_str()).collect()
    }

    pub fn tenant_name(&self, idx: usize) -> Option<&str> {
        self.tenants.get(idx).map(|t| t.name.as_str())
    }

    pub fn check(&self, token: Option<&str>, ip: Option<IpAddr>, now: Instant) -> Decision {
        if let Some(token) = token {
            let hash = crate::config::ApiKey::hash_token(token);
            let Some(&idx) = self.by_hash.get(&hash) else {
                // A wrong key is rejected even when anonymous access is
                // allowed: silently downgrading would hide misconfiguration.
                return Decision::Unauthorized;
            };
            let t = &self.tenants[idx];
            if t.rate <= 0.0 {
                return Decision::Allow { tenant: Some(idx) };
            }
            return match t.bucket.lock().take(t.rate, t.burst, now) {
                Ok(()) => Decision::Allow { tenant: Some(idx) },
                Err(retry_after_secs) => Decision::Limited {
                    tenant: Some(idx),
                    retry_after_secs,
                },
            };
        }
        if self.required {
            return Decision::Unauthorized;
        }
        match ip {
            Some(ip) if self.ip_rate > 0.0 => match self.take_ip(ip, now) {
                Ok(()) => Decision::Allow { tenant: None },
                Err(retry_after_secs) => Decision::Limited {
                    tenant: None,
                    retry_after_secs,
                },
            },
            _ => Decision::Allow { tenant: None },
        }
    }

    fn shard_index(&self, ip: &IpAddr) -> usize {
        let mut h = rustc_hash::FxHasher::default();
        ip.hash(&mut h);
        crate::cache::spread(h.finish(), IP_SHARDS)
    }

    fn take_ip(&self, ip: IpAddr, now: Instant) -> Result<(), u64> {
        let shard = &self.ip_shards[self.shard_index(&ip)];
        let mut shard = shard.lock();
        let len = shard.map.len();
        if len >= self.soft_cap && !shard.map.contains_key(&ip) {
            if len >= self.hard_cap {
                shard.map.clear();
            } else if shard
                .last_sweep
                .is_none_or(|t| now.saturating_duration_since(t) >= IP_SWEEP_INTERVAL)
            {
                let (rate, burst) = (self.ip_rate, self.ip_burst);
                shard.map.retain(|_, b| !b.is_full_at(rate, burst, now));
                shard.last_sweep = Some(now);
            }
        }
        shard
            .map
            .entry(ip)
            .or_insert_with(|| Bucket::full(self.ip_burst, now))
            .take(self.ip_rate, self.ip_burst, now)
    }

    #[cfg(test)]
    fn tracked_ips(&self) -> usize {
        self.ip_shards.iter().map(|s| s.lock().map.len()).sum()
    }
}

/// The presented API key: `Authorization: Bearer <t>` or `X-API-Key: <t>`.
pub fn extract_token<'a>(
    authorization: Option<&'a str>,
    api_key: Option<&'a str>,
) -> Option<&'a str> {
    if let Some(v) = authorization {
        let v = v.trim();
        if v.len() > 7 && v[..7].eq_ignore_ascii_case("bearer ") {
            let t = v[7..].trim();
            if !t.is_empty() {
                return Some(t);
            }
        }
    }
    api_key.map(str::trim).filter(|t| !t.is_empty())
}

/// Client IP: the peer, or — behind a trusted proxy — the rightmost
/// parseable `X-Forwarded-For` entry (the one our proxy appended).
pub fn client_ip(peer: Option<IpAddr>, forwarded_for: Option<&str>, trust: bool) -> Option<IpAddr> {
    if trust {
        if let Some(ip) = forwarded_for
            .and_then(|v| v.rsplit(',').next())
            .and_then(|s| s.trim().parse().ok())
        {
            return Some(ip);
        }
    }
    peer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ApiKey;
    use std::time::Duration;

    fn key(name: &str, tok: &str, rate: f64, burst: f64) -> ApiKey {
        ApiKey {
            name: name.into(),
            token_sha256: ApiKey::hash_token(tok),
            rate_per_sec: rate,
            burst,
        }
    }

    fn ip(s: &str) -> Option<IpAddr> {
        Some(s.parse().unwrap())
    }

    #[test]
    fn no_keys_and_no_ip_limit_is_a_noop_that_allows_everything() {
        let l = Limiter::new(&AuthConfig::default(), &RateLimitConfig::default());
        assert!(l.is_noop());
        let now = Instant::now();
        for _ in 0..100 {
            assert_eq!(
                l.check(None, ip("1.2.3.4"), now),
                Decision::Allow { tenant: None }
            );
        }
        // An unknown key is still rejected.
        assert_eq!(l.check(Some("nope"), None, now), Decision::Unauthorized);
        assert!(l.tenant_names().is_empty());
    }

    #[test]
    fn required_auth_rejects_anonymous_and_unknown_keys() {
        let auth = AuthConfig {
            required: true,
            keys: vec![key("acme", "s3cret", 0.0, 1.0)],
        };
        let l = Limiter::new(&auth, &RateLimitConfig::default());
        assert!(!l.is_noop());
        let now = Instant::now();
        assert_eq!(l.check(None, ip("1.1.1.1"), now), Decision::Unauthorized);
        assert_eq!(l.check(Some("wrong"), None, now), Decision::Unauthorized);
        // rate 0 = unlimited.
        for _ in 0..1000 {
            assert_eq!(
                l.check(Some("s3cret"), None, now),
                Decision::Allow { tenant: Some(0) }
            );
        }
        assert_eq!(l.tenant_name(0), Some("acme"));
        assert_eq!(l.tenant_name(1), None);
        assert_eq!(l.tenant_names(), vec!["acme"]);
    }

    #[test]
    fn per_key_bucket_allows_burst_then_limits_then_refills() {
        let auth = AuthConfig {
            required: false,
            keys: vec![key("a", "t", 2.0, 3.0)],
        };
        let l = Limiter::new(&auth, &RateLimitConfig::default());
        let t0 = Instant::now();
        for _ in 0..3 {
            assert_eq!(
                l.check(Some("t"), None, t0),
                Decision::Allow { tenant: Some(0) }
            );
        }
        assert_eq!(
            l.check(Some("t"), None, t0),
            Decision::Limited {
                tenant: Some(0),
                retry_after_secs: 1
            }
        );
        // 0.5 s at 2 rps refills exactly one token.
        let t1 = t0 + Duration::from_millis(500);
        assert_eq!(
            l.check(Some("t"), None, t1),
            Decision::Allow { tenant: Some(0) }
        );
        assert!(matches!(
            l.check(Some("t"), None, t1),
            Decision::Limited { .. }
        ));
        // A long idle period caps at burst, not beyond.
        let t2 = t1 + Duration::from_secs(3600);
        for _ in 0..3 {
            assert!(matches!(
                l.check(Some("t"), None, t2),
                Decision::Allow { .. }
            ));
        }
        assert!(matches!(
            l.check(Some("t"), None, t2),
            Decision::Limited { .. }
        ));
    }

    #[test]
    fn retry_after_reflects_slow_rates() {
        let auth = AuthConfig {
            required: false,
            keys: vec![key("slow", "t", 0.1, 1.0)],
        };
        let l = Limiter::new(&auth, &RateLimitConfig::default());
        let now = Instant::now();
        assert!(matches!(
            l.check(Some("t"), None, now),
            Decision::Allow { .. }
        ));
        assert_eq!(
            l.check(Some("t"), None, now),
            Decision::Limited {
                tenant: Some(0),
                retry_after_secs: 10
            }
        );
    }

    #[test]
    fn per_ip_limit_is_independent_per_address_and_keys_bypass_it() {
        let auth = AuthConfig {
            required: false,
            keys: vec![key("k", "t", 0.0, 1.0)],
        };
        let rl = RateLimitConfig {
            per_ip_rps: 1.0,
            per_ip_burst: 2.0,
        };
        let l = Limiter::new(&auth, &rl);
        let now = Instant::now();
        let a = ip("10.0.0.1");
        let b = ip("2001:db8::1");
        assert!(matches!(
            l.check(None, a, now),
            Decision::Allow { tenant: None }
        ));
        assert!(matches!(l.check(None, a, now), Decision::Allow { .. }));
        assert_eq!(
            l.check(None, a, now),
            Decision::Limited {
                tenant: None,
                retry_after_secs: 1
            }
        );
        assert!(matches!(l.check(None, b, now), Decision::Allow { .. }));
        // Keyed traffic from a throttled IP is governed by the key instead.
        assert!(matches!(
            l.check(Some("t"), a, now),
            Decision::Allow { tenant: Some(0) }
        ));
        // Unknown client IP (e.g. unix socket in tests) is not limited.
        for _ in 0..10 {
            assert!(matches!(l.check(None, None, now), Decision::Allow { .. }));
        }
    }

    #[test]
    fn sequential_ips_spread_across_every_shard() {
        let l = Limiter::new(&AuthConfig::default(), &RateLimitConfig::default());
        let mut counts = [0usize; IP_SHARDS];
        for i in 0..1024u32 {
            counts[l.shard_index(&IpAddr::from(std::net::Ipv4Addr::from(0x0a00_0000 + i)))] += 1;
        }
        // 64 expected per shard; allow generous skew but no empty shards.
        assert!(
            counts.iter().all(|&c| (16..=160).contains(&c)),
            "{counts:?}"
        );
    }

    fn v4(n: u32) -> Option<IpAddr> {
        Some(IpAddr::from(std::net::Ipv4Addr::from(n)))
    }

    #[test]
    fn idle_ip_buckets_are_swept_at_most_once_per_interval() {
        let rl = RateLimitConfig {
            per_ip_rps: 1.0,
            per_ip_burst: 1.0,
        };
        let mut l = Limiter::new(&AuthConfig::default(), &rl);
        // One logical shard's worth of caps, tiny so the test is fast.
        l.soft_cap = 4;
        l.hard_cap = 1_000_000;
        let t0 = Instant::now();
        let n = 4 * IP_SHARDS as u32 * 4;
        for i in 0..n {
            let _ = l.check(None, v4(0x0a00_0000 + i), t0);
        }
        // Every bucket was just drained, so sweeps evicted nothing.
        assert_eq!(l.tracked_ips(), n as usize);
        // Much later all are full again: the next newcomer per shard sweeps.
        let t1 = t0 + Duration::from_secs(10);
        for i in 0..(IP_SHARDS as u32 * 8) {
            let _ = l.check(None, v4(0x0b00_0000 + i), t1);
        }
        // Every old bucket was evicted; only the newcomers remain.
        assert_eq!(l.tracked_ips(), IP_SHARDS * 8);
        // A returning (evicted) client starts with a full bucket.
        assert!(matches!(
            l.check(None, v4(0x0a00_0000), t1),
            Decision::Allow { .. }
        ));
        // Within the sweep interval a full shard just grows (no rescan).
        let before = l.tracked_ips();
        let t2 = t1 + Duration::from_millis(10);
        for i in 0..(IP_SHARDS as u32 * 8) {
            let _ = l.check(None, v4(0x0c00_0000 + i), t2);
        }
        assert_eq!(l.tracked_ips(), before + IP_SHARDS * 8);
    }

    #[test]
    fn a_shard_at_the_hard_cap_is_reset() {
        let rl = RateLimitConfig {
            per_ip_rps: 1.0,
            per_ip_burst: 1.0,
        };
        let mut l = Limiter::new(&AuthConfig::default(), &rl);
        l.soft_cap = 2;
        l.hard_cap = 3;
        let t0 = Instant::now();
        for i in 0..(IP_SHARDS as u32 * 32) {
            let _ = l.check(None, v4(0x0a00_0000 + i), t0);
        }
        // No shard can exceed hard_cap + 1 entries.
        let max = l
            .ip_shards
            .iter()
            .map(|s| s.lock().map.len())
            .max()
            .unwrap();
        assert!(max <= 3, "max shard {max}");
    }

    #[test]
    fn extract_token_prefers_bearer_and_falls_back_to_api_key_header() {
        assert_eq!(extract_token(Some("Bearer abc"), Some("zzz")), Some("abc"));
        assert_eq!(extract_token(Some("  bearer   abc  "), None), Some("abc"));
        assert_eq!(extract_token(Some("Basic Zm9v"), Some(" k ")), Some("k"));
        assert_eq!(extract_token(Some("Bearer    "), None), None);
        assert_eq!(extract_token(Some("Bearer"), None), None);
        assert_eq!(extract_token(None, Some("   ")), None);
        assert_eq!(extract_token(None, None), None);
    }

    #[test]
    fn client_ip_uses_forwarded_for_only_when_trusted() {
        let peer = ip("10.0.0.9");
        let xff = Some("203.0.113.5, 198.51.100.7");
        assert_eq!(client_ip(peer, xff, false), peer);
        assert_eq!(client_ip(peer, xff, true), ip("198.51.100.7"));
        assert_eq!(client_ip(peer, Some("garbage"), true), peer);
        assert_eq!(client_ip(peer, None, true), peer);
        assert_eq!(
            client_ip(None, Some("2001:db8::2"), true),
            ip("2001:db8::2")
        );
    }
}
