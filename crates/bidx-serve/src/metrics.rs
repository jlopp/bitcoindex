//! Prometheus text-exposition metrics.
//!
//! A tiny, allocation-light registry: counters and gauges keyed by name plus
//! a fixed label set, rendered in the Prometheus 0.0.4 text format. No
//! background collection — the serving loop updates the gauges it already
//! computes (tip height, query counts) and `/metrics` renders on scrape.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonic counter (never decreases).
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    /// Absolute set. Used for gauge-typed samples; a counter should only ever
    /// be `inc`/`add`ed, but the storage is shared so the method lives here.
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
    /// Saturating decrement, for gauge-typed samples (in-flight requests).
    pub fn dec(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }
}

/// Upper bounds (seconds) of the request-latency histogram buckets.
const LATENCY_BUCKETS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0,
];

/// A fixed-bucket Prometheus histogram. Lock-free: one atomic per bucket.
#[derive(Debug, Default)]
pub struct Histogram {
    /// Non-cumulative per-bucket counts; the last slot is `+Inf`.
    buckets: [AtomicU64; LATENCY_BUCKETS.len() + 1],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn observe_micros(&self, micros: u64) {
        let secs = micros as f64 / 1e6;
        let idx = LATENCY_BUCKETS
            .iter()
            .position(|&b| secs <= b)
            .unwrap_or(LATENCY_BUCKETS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn render(&self, name: &str, help: &str, out: &mut String) {
        use std::fmt::Write as _;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0;
        for (i, b) in self.buckets.iter().enumerate() {
            cumulative += b.load(Ordering::Relaxed);
            match LATENCY_BUCKETS.get(i) {
                Some(le) => {
                    let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
                }
                None => {
                    let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
                }
            }
        }
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {}", self.count());
    }
}

/// A gauge that can go up or down.
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A metric family with a fixed label set, rendered as one line per sample.
struct Family {
    name: String,
    help: String,
    kind: &'static str,
    /// label-set → value
    samples: Vec<(Vec<(String, String)>, Counter)>,
}

/// Registry of counters/gauges that `/metrics` renders.
#[derive(Default)]
pub struct Registry {
    families: Vec<Family>,
    /// Simple single-sample families keyed by name for cheap update.
    singles: BTreeMap<String, (String, &'static str, Counter)>,
}

impl Registry {
    pub fn new() -> Self {
        Registry::default()
    }

    /// Register a single-sample counter (no labels).
    pub fn register_counter(&mut self, name: &str, help: &str) {
        self.singles.insert(
            name.to_string(),
            (help.to_string(), "counter", Counter::default()),
        );
    }

    /// Register a single-sample gauge (no labels).
    pub fn register_gauge(&mut self, name: &str, help: &str) {
        self.singles.insert(
            name.to_string(),
            (help.to_string(), "gauge", Counter::default()),
        );
    }

    pub fn counter(&self, name: &str) -> Option<&Counter> {
        self.singles.get(name).map(|(_, _, c)| c)
    }

    /// Register a labeled family (e.g. per-route request counts).
    pub fn register_labeled(
        &mut self,
        name: &str,
        help: &str,
        kind: &'static str,
        label_sets: Vec<Vec<(String, String)>>,
    ) {
        let samples = label_sets
            .into_iter()
            .map(|labels| (labels, Counter::default()))
            .collect();
        self.families.push(Family {
            name: name.to_string(),
            help: help.to_string(),
            kind,
            samples,
        });
    }

    /// Increment the sample in a labeled family whose labels match.
    pub fn labeled_inc(&self, name: &str, labels: &[(&str, &str)]) {
        if let Some(fam) = self.families.iter().find(|f| f.name == name) {
            for (lset, ctr) in &fam.samples {
                if lset.len() == labels.len()
                    && lset
                        .iter()
                        .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
                {
                    ctr.inc();
                    return;
                }
            }
        }
    }

    /// Render the whole registry in Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (name, (help, kind, ctr)) in &self.singles {
            out.push_str(&format!("# HELP {name} {help}\n"));
            out.push_str(&format!("# TYPE {name} {kind}\n"));
            out.push_str(&format!("{name} {}\n", ctr.get()));
        }
        for fam in &self.families {
            out.push_str(&format!("# HELP {} {}\n", fam.name, fam.help));
            out.push_str(&format!("# TYPE {} {}\n", fam.name, fam.kind));
            for (labels, ctr) in &fam.samples {
                out.push_str(&fam.name);
                if !labels.is_empty() {
                    out.push('{');
                    for (i, (k, v)) in labels.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push_str(&format!("{k}=\"{}\"", escape_label(v)));
                    }
                    out.push('}');
                }
                out.push_str(&format!(" {}\n", ctr.get()));
            }
        }
        out
    }
}

/// Escape a label value per the Prometheus exposition format.
fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The service's named metrics, wired once at startup.
pub struct Metrics {
    pub registry: Registry,
    pub latency: Histogram,
}

const LATENCY_NAME: &str = "bidx_request_duration_seconds";
const LATENCY_HELP: &str = "Time from request parsed to response built";

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::new();
        registry.register_gauge("bidx_indexed_height", "Highest height present in the index");
        registry.register_gauge("bidx_node_height", "Node tip height as last observed");
        registry.register_gauge(
            "bidx_indexer_lag_blocks",
            "Node height minus indexed height (0 when caught up)",
        );
        registry.register_counter("bidx_queries_total", "Total served queries");
        registry.register_counter(
            "bidx_query_errors_total",
            "Total queries that returned an error",
        );
        registry.register_counter(
            "bidx_address_not_found_total",
            "Address queries that matched no outputs",
        );
        registry.register_counter(
            "bidx_unauthorized_total",
            "Requests rejected for a missing or unknown API key",
        );
        registry.register_counter(
            "bidx_rate_limited_total",
            "Requests rejected by a per-key or per-IP rate limit",
        );
        registry.register_counter(
            "bidx_shed_total",
            "Connections refused with 503 because the worker queue was full",
        );
        registry.register_counter(
            "bidx_cache_hits_total",
            "Responses served from the in-process cache",
        );
        registry.register_counter(
            "bidx_cache_misses_total",
            "Cacheable requests that had to query ClickHouse",
        );
        registry.register_gauge("bidx_inflight_requests", "Requests currently being handled");
        registry.register_gauge(
            "bidx_draining",
            "1 after SIGTERM while the server drains, else 0",
        );
        registry.register_labeled(
            "bidx_requests_by_surface_total",
            "Requests by serving surface",
            "counter",
            vec![
                vec![("surface".to_string(), "esplora".to_string())],
                vec![("surface".to_string(), "electrum".to_string())],
                vec![("surface".to_string(), "ops".to_string())],
            ],
        );
        registry.register_labeled(
            "bidx_responses_by_status_total",
            "Responses by HTTP status class",
            "counter",
            vec![
                vec![("status".to_string(), "2xx".to_string())],
                vec![("status".to_string(), "4xx".to_string())],
                vec![("status".to_string(), "5xx".to_string())],
            ],
        );
        Metrics {
            registry,
            latency: Histogram::default(),
        }
    }

    /// Like [`new`](Self::new), plus per-tenant request and rate-limit
    /// families labeled with each API key's name.
    pub fn with_tenants(names: &[&str]) -> Self {
        let mut m = Metrics::new();
        let sets: Vec<Vec<(String, String)>> = names
            .iter()
            .map(|n| vec![("tenant".to_string(), (*n).to_string())])
            .collect();
        m.registry.register_labeled(
            "bidx_tenant_requests_total",
            "Requests by API-key tenant",
            "counter",
            sets.clone(),
        );
        m.registry.register_labeled(
            "bidx_tenant_rate_limited_total",
            "Rate-limited requests by API-key tenant",
            "counter",
            sets,
        );
        m
    }

    /// Full exposition: the registry plus the latency histogram.
    pub fn render(&self) -> String {
        let mut out = self.registry.render();
        self.latency.render(LATENCY_NAME, LATENCY_HELP, &mut out);
        out
    }

    fn single(&self, name: &str) {
        if let Some(c) = self.registry.counter(name) {
            c.inc();
        }
    }
    pub fn inc_unauthorized(&self) {
        self.single("bidx_unauthorized_total");
    }
    pub fn inc_rate_limited(&self, tenant: Option<&str>) {
        self.single("bidx_rate_limited_total");
        if let Some(t) = tenant {
            self.registry
                .labeled_inc("bidx_tenant_rate_limited_total", &[("tenant", t)]);
        }
    }
    pub fn inc_tenant(&self, tenant: &str) {
        self.registry
            .labeled_inc("bidx_tenant_requests_total", &[("tenant", tenant)]);
    }
    pub fn inc_shed(&self) {
        self.single("bidx_shed_total");
    }
    pub fn inc_cache_hit(&self) {
        self.single("bidx_cache_hits_total");
    }
    pub fn inc_cache_miss(&self) {
        self.single("bidx_cache_misses_total");
    }
    pub fn inflight_inc(&self) {
        self.single("bidx_inflight_requests");
    }
    pub fn inflight_dec(&self) {
        if let Some(c) = self.registry.counter("bidx_inflight_requests") {
            c.dec();
        }
    }
    pub fn set_draining(&self, draining: bool) {
        if let Some(c) = self.registry.counter("bidx_draining") {
            c.set(draining as u64);
        }
    }

    pub fn set_indexed_height(&self, h: u32) {
        if let Some(c) = self.registry.counter("bidx_indexed_height") {
            c.set(h as u64);
        }
    }
    pub fn set_node_height(&self, h: u32) {
        if let Some(c) = self.registry.counter("bidx_node_height") {
            c.set(h as u64);
        }
    }
    pub fn set_lag(&self, lag: u32) {
        if let Some(c) = self.registry.counter("bidx_indexer_lag_blocks") {
            c.set(lag as u64);
        }
    }
    pub fn inc_queries(&self) {
        if let Some(c) = self.registry.counter("bidx_queries_total") {
            c.inc();
        }
    }
    pub fn inc_query_errors(&self) {
        if let Some(c) = self.registry.counter("bidx_query_errors_total") {
            c.inc();
        }
    }
    pub fn inc_address_not_found(&self) {
        if let Some(c) = self.registry.counter("bidx_address_not_found_total") {
            c.inc();
        }
    }
    pub fn inc_surface(&self, surface: &str) {
        self.registry
            .labeled_inc("bidx_requests_by_surface_total", &[("surface", surface)]);
    }
    pub fn inc_status(&self, status: &str) {
        self.registry
            .labeled_inc("bidx_responses_by_status_total", &[("status", status)]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_render_with_help_and_type() {
        let m = Metrics::new();
        m.inc_queries();
        m.inc_queries();
        m.inc_query_errors();
        m.set_indexed_height(840_000);
        m.set_node_height(840_010);
        m.set_lag(10);
        let text = m.registry.render();
        assert!(text.contains("# TYPE bidx_queries_total counter"));
        assert!(text.contains("bidx_queries_total 2\n"));
        assert!(text.contains("bidx_query_errors_total 1\n"));
        assert!(text.contains("# TYPE bidx_indexed_height gauge"));
        assert!(text.contains("bidx_indexed_height 840000\n"));
        assert!(text.contains("bidx_indexer_lag_blocks 10\n"));
    }

    #[test]
    fn labeled_families_render_and_increment() {
        let m = Metrics::new();
        m.inc_surface("esplora");
        m.inc_surface("esplora");
        m.inc_surface("electrum");
        m.inc_status("2xx");
        m.inc_status("4xx");
        m.inc_status("4xx");
        let text = m.registry.render();
        assert!(text.contains("bidx_requests_by_surface_total{surface=\"esplora\"} 2\n"));
        assert!(text.contains("bidx_requests_by_surface_total{surface=\"electrum\"} 1\n"));
        assert!(text.contains("bidx_responses_by_status_total{status=\"2xx\"} 1\n"));
        assert!(text.contains("bidx_responses_by_status_total{status=\"4xx\"} 2\n"));
        assert!(text.contains("bidx_responses_by_status_total{status=\"5xx\"} 0\n"));
    }

    #[test]
    fn unknown_metric_and_labels_are_ignored_not_panicked() {
        let m = Metrics::new();
        // Unknown surface: must not panic and must not increment anything.
        m.inc_surface("bogus");
        // Unknown label key.
        m.registry
            .labeled_inc("bidx_requests_by_surface_total", &[("wrong", "x")]);
        // Unknown family name.
        m.registry.labeled_inc("does_not_exist", &[]);
        assert!(m.registry.counter("does_not_exist").is_none());
        let text = m.registry.render();
        assert!(text.contains("bidx_requests_by_surface_total{surface=\"esplora\"} 0\n"));
    }

    #[test]
    fn label_values_are_escaped() {
        let mut r = Registry::new();
        r.register_labeled(
            "test_metric",
            "h",
            "counter",
            vec![vec![("path".to_string(), "a\"b\\c\nd".to_string())]],
        );
        let text = r.render();
        assert!(text.contains(r#"path="a\"b\\c\nd""#), "got: {text}");
    }

    #[test]
    fn multi_label_samples_join_with_commas() {
        let mut r = Registry::new();
        r.register_labeled(
            "two_label",
            "h",
            "counter",
            vec![vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]],
        );
        r.labeled_inc("two_label", &[("a", "1"), ("b", "2")]);
        let text = r.render();
        assert!(
            text.contains("two_label{a=\"1\",b=\"2\"} 1\n"),
            "got: {text}"
        );
    }

    #[test]
    fn labeled_inc_requires_exact_label_match() {
        let mut r = Registry::new();
        r.register_labeled(
            "fam",
            "h",
            "counter",
            vec![vec![("a".to_string(), "1".to_string())]],
        );
        // Right key, wrong value → no sample matches → nothing incremented.
        r.labeled_inc("fam", &[("a", "2")]);
        assert!(r.render().contains("fam{a=\"1\"} 0\n"));
    }

    #[test]
    fn default_matches_new() {
        let a = Metrics::default();
        let b = Metrics::new();
        assert_eq!(a.registry.render(), b.registry.render());
    }

    #[test]
    fn address_not_found_metric_increments() {
        let m = Metrics::new();
        m.inc_address_not_found();
        m.inc_address_not_found();
        let text = m.registry.render();
        assert!(text.contains("bidx_address_not_found_total 2\n"));
    }

    #[test]
    fn counter_and_gauge_getters() {
        let c = Counter::default();
        assert_eq!(c.get(), 0);
        c.inc();
        c.add(4);
        assert_eq!(c.get(), 5);
        let g = Gauge::default();
        g.set(7);
        assert_eq!(g.get(), 7);
        g.set(3);
        assert_eq!(g.get(), 3);
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_sum_is_in_seconds() {
        let m = Metrics::new();
        m.latency.observe_micros(500); // 0.0005s → le=0.001
        m.latency.observe_micros(1_000); // exactly 0.001 → le=0.001
        m.latency.observe_micros(30_000); // → le=0.05
        m.latency.observe_micros(60_000_000); // 60s → +Inf only
        let text = m.render();
        assert!(text.contains("# TYPE bidx_request_duration_seconds histogram\n"));
        assert!(
            text.contains("bidx_request_duration_seconds_bucket{le=\"0.001\"} 2\n"),
            "{text}"
        );
        assert!(
            text.contains("bidx_request_duration_seconds_bucket{le=\"0.025\"} 2\n"),
            "{text}"
        );
        assert!(
            text.contains("bidx_request_duration_seconds_bucket{le=\"0.05\"} 3\n"),
            "{text}"
        );
        assert!(
            text.contains("bidx_request_duration_seconds_bucket{le=\"10\"} 3\n"),
            "{text}"
        );
        assert!(
            text.contains("bidx_request_duration_seconds_bucket{le=\"+Inf\"} 4\n"),
            "{text}"
        );
        assert!(
            text.contains("bidx_request_duration_seconds_sum 60.0315\n"),
            "{text}"
        );
        assert!(
            text.contains("bidx_request_duration_seconds_count 4\n"),
            "{text}"
        );
        assert_eq!(m.latency.count(), 4);
    }

    #[test]
    fn operational_counters_and_inflight_gauge() {
        let m = Metrics::with_tenants(&["acme", "globex"]);
        m.inc_unauthorized();
        m.inc_rate_limited(Some("acme"));
        m.inc_rate_limited(None);
        m.inc_tenant("globex");
        m.inc_tenant("globex");
        m.inc_shed();
        m.inc_cache_hit();
        m.inc_cache_miss();
        m.inc_cache_miss();
        m.inflight_inc();
        m.inflight_inc();
        m.inflight_dec();
        m.set_draining(true);
        let t = m.render();
        for line in [
            "bidx_unauthorized_total 1\n",
            "bidx_rate_limited_total 2\n",
            "bidx_tenant_rate_limited_total{tenant=\"acme\"} 1\n",
            "bidx_tenant_rate_limited_total{tenant=\"globex\"} 0\n",
            "bidx_tenant_requests_total{tenant=\"globex\"} 2\n",
            "bidx_shed_total 1\n",
            "bidx_cache_hits_total 1\n",
            "bidx_cache_misses_total 2\n",
            "bidx_inflight_requests 1\n",
            "bidx_draining 1\n",
        ] {
            assert!(t.contains(line), "missing {line:?} in {t}");
        }
        // Decrement saturates at zero.
        m.inflight_dec();
        m.inflight_dec();
        m.set_draining(false);
        let t = m.render();
        assert!(t.contains("bidx_inflight_requests 0\n") && t.contains("bidx_draining 0\n"));
        // Without tenants the per-tenant families are absent but calls are harmless.
        let plain = Metrics::new();
        plain.inc_tenant("acme");
        assert!(!plain.render().contains("bidx_tenant_requests_total"));
    }

    #[test]
    fn empty_registry_renders_empty_string() {
        let r = Registry::new();
        assert_eq!(r.render(), "");
    }
}
