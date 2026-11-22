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
    pub fn register_labeled(&mut self, name: &str, help: &str, kind: &'static str, label_sets: Vec<Vec<(String, String)>>) {
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
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// The service's named metrics, wired once at startup.
pub struct Metrics {
    pub registry: Registry,
}

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
        Metrics { registry }
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
        self.registry.labeled_inc("bidx_requests_by_surface_total", &[("surface", surface)]);
    }
    pub fn inc_status(&self, status: &str) {
        self.registry.labeled_inc("bidx_responses_by_status_total", &[("status", status)]);
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
        m.registry.labeled_inc("bidx_requests_by_surface_total", &[("wrong", "x")]);
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
        assert!(text.contains("two_label{a=\"1\",b=\"2\"} 1\n"), "got: {text}");
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
    fn empty_registry_renders_empty_string() {
        let r = Registry::new();
        assert_eq!(r.render(), "");
    }
}
