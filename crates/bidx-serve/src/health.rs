//! Liveness and readiness state for the serving API.
//!
//! `/healthz` answers "is the process alive" and is always 200 once the
//! server is up. `/readyz` answers "should I send it traffic" and returns 503
//! until the ClickHouse connection has been verified and the index has a
//! non-zero tip (a fresh, empty database is not ready to serve).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Shared readiness state, updated by the serving loop.
#[derive(Debug, Default)]
pub struct Health {
    ch_ok: AtomicBool,
    has_tip: AtomicBool,
    indexed_height: AtomicU32,
    node_height: AtomicU32,
}

impl Health {
    pub fn new() -> Self {
        Health::default()
    }

    /// Mark whether the ClickHouse connection is verified.
    pub fn set_clickhouse_ok(&self, ok: bool) {
        self.ch_ok.store(ok, Ordering::Relaxed);
    }

    /// Record the index tip height. A height of `None` (no tip yet) clears the
    /// has-tip flag so readiness reports not-ready on an empty database.
    pub fn set_indexed_height(&self, height: Option<u32>) {
        match height {
            Some(h) => {
                self.indexed_height.store(h, Ordering::Relaxed);
                self.has_tip.store(true, Ordering::Relaxed);
            }
            None => {
                self.indexed_height.store(0, Ordering::Relaxed);
                self.has_tip.store(false, Ordering::Relaxed);
            }
        }
    }

    pub fn set_node_height(&self, height: u32) {
        self.node_height.store(height, Ordering::Relaxed);
    }

    /// Liveness: the process is up and serving.
    pub fn live(&self) -> bool {
        true
    }

    /// Readiness: ClickHouse reachable *and* the index has a tip.
    pub fn ready(&self) -> bool {
        self.ch_ok.load(Ordering::Relaxed) && self.has_tip.load(Ordering::Relaxed)
    }

    pub fn indexed_height(&self) -> u32 {
        self.indexed_height.load(Ordering::Relaxed)
    }

    pub fn node_height(&self) -> u32 {
        self.node_height.load(Ordering::Relaxed)
    }

    /// Blocks behind the node tip (0 when caught up or when the node height is
    /// unknown).
    pub fn lag(&self) -> u32 {
        let node = self.node_height.load(Ordering::Relaxed);
        let idx = self.indexed_height.load(Ordering::Relaxed);
        node.saturating_sub(idx)
    }

    /// JSON body for `/readyz`, with per-check detail.
    pub fn ready_json(&self) -> String {
        let ready = self.ready();
        format!(
            "{{\"ready\":{ready},\"checks\":{{\"clickhouse\":{},\"index_has_tip\":{},\"indexed_height\":{},\"node_height\":{},\"lag_blocks\":{}}}}}",
            self.ch_ok.load(Ordering::Relaxed),
            self.has_tip.load(Ordering::Relaxed),
            self.indexed_height(),
            self.node_height(),
            self.lag(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_not_ready_and_becomes_ready() {
        let h = Health::new();
        assert!(h.live());
        assert!(!h.ready(), "no ClickHouse, no tip → not ready");
        h.set_clickhouse_ok(true);
        assert!(!h.ready(), "still no tip");
        h.set_indexed_height(Some(1));
        assert!(h.ready());
    }

    #[test]
    fn empty_database_is_not_ready() {
        let h = Health::new();
        h.set_clickhouse_ok(true);
        h.set_indexed_height(None);
        assert!(!h.ready(), "empty index must not report ready");
        assert_eq!(h.indexed_height(), 0);
    }

    #[test]
    fn losing_clickhouse_makes_it_unready_again() {
        let h = Health::new();
        h.set_clickhouse_ok(true);
        h.set_indexed_height(Some(100));
        assert!(h.ready());
        h.set_clickhouse_ok(false);
        assert!(!h.ready());
    }

    #[test]
    fn lag_is_saturating_and_tracks_both_heights() {
        let h = Health::new();
        h.set_indexed_height(Some(100));
        h.set_node_height(130);
        assert_eq!(h.lag(), 30);
        // Indexed ahead of node (or node unknown) → 0, never underflow.
        h.set_node_height(50);
        assert_eq!(h.lag(), 0);
    }

    #[test]
    fn ready_json_reports_each_check() {
        let h = Health::new();
        h.set_clickhouse_ok(true);
        h.set_indexed_height(Some(840_000));
        h.set_node_height(840_005);
        let json = h.ready_json();
        assert!(json.contains("\"ready\":true"));
        assert!(json.contains("\"clickhouse\":true"));
        assert!(json.contains("\"index_has_tip\":true"));
        assert!(json.contains("\"indexed_height\":840000"));
        assert!(json.contains("\"node_height\":840005"));
        assert!(json.contains("\"lag_blocks\":5"));
        // Must be valid JSON.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["ready"], true);
    }
}
