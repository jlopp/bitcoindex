//! The serving HTTP surface: routing, JSON-RPC dispatch, metrics and health.
//!
//! Requests are parsed (by [`crate::server`]) into a [`Request`], checked
//! against API-key auth and rate limits, answered from the in-process cache
//! when possible, and otherwise routed by [`Service::handle`] to the Esplora
//! REST handlers, the Electrum JSON-RPC dispatcher, or the ops endpoints.
//! `handle` does no socket I/O (only the store does), so it is directly
//! unit-testable.

use crate::cache::{Cached, ResponseCache};
use crate::config::Config;
use crate::electrum::Electrum;
use crate::esplora::{self, Esplora};
use crate::health::Health;
use crate::limits::{client_ip, extract_token, Decision, Limiter};
use crate::metrics::Metrics;
use crate::store::Store;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A parsed HTTP request.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: String,
    /// The raw request target (`/path?query`), used as the cache key.
    pub target: String,
    /// Header names are lowercased.
    pub headers: Vec<(String, String)>,
    pub peer: Option<IpAddr>,
    /// Whether the client allows the connection to be reused.
    pub keep_alive: bool,
}

impl Request {
    pub fn new(method: &str, target: &str, body: &str) -> Self {
        let (path, query) = split_query(target);
        Request {
            method: method.to_string(),
            path,
            query,
            body: body.to_string(),
            target: target.to_string(),
            headers: Vec::new(),
            peer: None,
            keep_alive: false,
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .push((name.to_ascii_lowercase(), value.to_string()));
        self
    }

    pub fn with_peer(mut self, peer: IpAddr) -> Self {
        self.peer = Some(peer);
        self
    }

    pub fn query_param(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// First header with this (lowercase) name.
    pub fn header(&self, lower_name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == lower_name)
            .map(|(_, v)| v.as_str())
    }
}

/// A shared, immutable response body. Cloning is a refcount bump, so cache
/// hits never copy the payload.
#[derive(Clone, PartialEq, Eq)]
pub struct Body(Arc<str>);

impl Body {
    pub fn as_arc(&self) -> &Arc<str> {
        &self.0
    }
}

impl std::ops::Deref for Body {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.0, f)
    }
}

impl std::fmt::Display for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for Body {
    fn from(s: String) -> Self {
        Body(Arc::from(s))
    }
}

impl From<Arc<str>> for Body {
    fn from(s: Arc<str>) -> Self {
        Body(s)
    }
}

impl PartialEq<str> for Body {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for Body {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl PartialEq<String> for Body {
    fn eq(&self, other: &String) -> bool {
        *self.0 == **other
    }
}

/// `Cache-Control` policy for a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheControl {
    NoStore,
    /// Shared caches (CDNs) may store it for this many seconds.
    Public(u64),
    /// Only the client may store it — used when auth is required, so a CDN
    /// never serves an authenticated response to an anonymous caller.
    Private(u64),
}

impl std::fmt::Display for CacheControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheControl::NoStore => f.write_str("no-store"),
            CacheControl::Public(0) | CacheControl::Private(0) => f.write_str("no-cache"),
            CacheControl::Public(s) => write!(f, "public, max-age={s}"),
            CacheControl::Private(s) => write!(f, "private, max-age={s}"),
        }
    }
}

/// An HTTP response with a rendered body.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Body,
    pub cache_control: CacheControl,
    pub retry_after: Option<u64>,
}

impl HttpResponse {
    pub fn json(status: u16, body: String) -> Self {
        HttpResponse {
            status,
            content_type: "application/json",
            body: body.into(),
            cache_control: CacheControl::NoStore,
            retry_after: None,
        }
    }
    pub fn text(status: u16, body: impl Into<String>) -> Self {
        HttpResponse {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into().into(),
            cache_control: CacheControl::NoStore,
            retry_after: None,
        }
    }

    pub fn reason(&self) -> &'static str {
        match self.status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            404 => "Not Found",
            405 => "Method Not Allowed",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
    }
}

/// How long a successful Esplora response may be cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Freshness {
    NoCache,
    /// Changes every block (tip routes, unconfirmed-depth data).
    Tip,
    /// Changes whenever a matching output is created or spent.
    Address,
    /// Data anchored at a block height; immutable once buried deep enough.
    AtHeight(u32),
}

/// Split `/path?a=1&b=2` into `("/path", [("a","1"),("b","2")])`.
fn split_query(target: &str) -> (String, Vec<(String, String)>) {
    match target.split_once('?') {
        None => (target.to_string(), Vec::new()),
        Some((p, q)) => {
            let pairs = q
                .split('&')
                .filter(|s| !s.is_empty())
                .map(|kv| match kv.split_once('=') {
                    Some((k, v)) => (k.to_string(), v.to_string()),
                    None => (kv.to_string(), String::new()),
                })
                .collect();
            (p.to_string(), pairs)
        }
    }
}

/// The composed serving service.
pub struct Service {
    pub store: Arc<Store>,
    pub cfg: Arc<Config>,
    pub metrics: Arc<Metrics>,
    pub health: Arc<Health>,
    limiter: Limiter,
    cache: Option<ResponseCache>,
}

impl Service {
    pub fn new(
        store: Arc<Store>,
        cfg: Arc<Config>,
        metrics: Arc<Metrics>,
        health: Arc<Health>,
    ) -> Self {
        let limiter = Limiter::new(&cfg.auth, &cfg.rate_limit);
        let cache = cfg
            .cache
            .enabled
            .then(|| ResponseCache::new(cfg.cache.max_entries));
        Service {
            store,
            cfg,
            metrics,
            health,
            limiter,
            cache,
        }
    }

    /// Refresh health/metrics from the store (and, if configured, the node).
    /// Called periodically by the serve loop and once at startup.
    pub fn refresh_health(&self) {
        match self.store.tip_height() {
            Ok(h) => {
                self.health.set_clickhouse_ok(true);
                self.health.set_indexed_height(h);
                if let Some(h) = h {
                    self.metrics.set_indexed_height(h);
                }
            }
            Err(_) => {
                self.health.set_clickhouse_ok(false);
                self.health.set_indexed_height(None);
            }
        }
        if let Some(node) = &self.cfg.node {
            if let Some(h) = fetch_node_height(node) {
                self.health.set_node_height(h);
                self.metrics.set_node_height(h);
                self.metrics.set_lag(self.health.lag());
            }
        }
    }

    /// Route and answer one request. Pure w.r.t. sockets (only the store does
    /// I/O), so it is directly unit-testable.
    pub fn handle(&self, req: &Request) -> HttpResponse {
        let start = Instant::now();
        self.metrics.inflight_inc();
        let (resp, surface) = self.route(req, start);
        self.metrics.inflight_dec();
        self.metrics
            .latency
            .observe_micros(start.elapsed().as_micros() as u64);
        self.metrics.inc_queries();
        self.metrics.inc_surface(surface);
        self.metrics.inc_status(status_class(resp.status));
        if resp.status >= 500 {
            self.metrics.inc_query_errors();
        }
        resp
    }

    fn route(&self, req: &Request, now: Instant) -> (HttpResponse, &'static str) {
        // Ops endpoints are always available and never throttled.
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/metrics") => {
                return (HttpResponse::text(200, self.metrics.render()), "ops");
            }
            ("GET", "/healthz") => return (HttpResponse::text(200, "ok"), "ops"),
            ("GET", "/readyz") => {
                let ready = self.health.ready();
                let status = if ready { 200 } else { 503 };
                return (HttpResponse::json(status, self.health.ready_json()), "ops");
            }
            _ => {}
        }

        let electrum = req.method == "POST" && (req.path == "/" || req.path == "/electrum");
        let surface = if electrum { "electrum" } else { "esplora" };
        if let Some(rejected) = self.admit(req, now) {
            return (rejected, surface);
        }

        // Electrum JSON-RPC: POST to the root or /electrum.
        if electrum {
            if !self.cfg.electrum_enabled {
                return (
                    HttpResponse::json(404, err_body("electrum disabled")),
                    "ops",
                );
            }
            return (self.electrum(req), "electrum");
        }

        // Everything else is the Esplora REST surface.
        if !self.cfg.esplora_enabled {
            return (HttpResponse::json(404, err_body("esplora disabled")), "ops");
        }
        if req.method != "GET" {
            return (
                HttpResponse::json(405, err_body("method not allowed")),
                "esplora",
            );
        }
        (self.esplora_cached(req, now), "esplora")
    }

    /// Auth + rate limit. `Some(response)` when the request is rejected.
    fn admit(&self, req: &Request, now: Instant) -> Option<HttpResponse> {
        if self.limiter.is_noop() {
            return None;
        }
        let token = extract_token(req.header("authorization"), req.header("x-api-key"));
        let ip = client_ip(
            req.peer,
            req.header("x-forwarded-for"),
            self.cfg.server.trust_forwarded_for,
        );
        match self.limiter.check(token, ip, now) {
            Decision::Allow { tenant } => {
                if let Some(name) = tenant.and_then(|t| self.limiter.tenant_name(t)) {
                    self.metrics.inc_tenant(name);
                }
                None
            }
            Decision::Unauthorized => {
                self.metrics.inc_unauthorized();
                Some(HttpResponse::json(
                    401,
                    err_body("missing or invalid API key"),
                ))
            }
            Decision::Limited {
                tenant,
                retry_after_secs,
            } => {
                self.metrics
                    .inc_rate_limited(tenant.and_then(|t| self.limiter.tenant_name(t)));
                let mut r = HttpResponse::json(429, err_body("rate limit exceeded"));
                r.retry_after = Some(retry_after_secs);
                Some(r)
            }
        }
    }

    fn esplora_cached(&self, req: &Request, now: Instant) -> HttpResponse {
        if let Some(cache) = &self.cache {
            if let Some(hit) = cache.get(&req.target, now) {
                self.metrics.inc_cache_hit();
                return HttpResponse {
                    status: hit.status,
                    content_type: hit.content_type,
                    body: hit.body.into(),
                    cache_control: self.cache_control(hit.max_age_secs),
                    retry_after: None,
                };
            }
            self.metrics.inc_cache_miss();
        }
        let (mut resp, freshness) = self.esplora_route(req);
        let Some((ttl, max_age)) = self.ttl_for(resp.status, freshness) else {
            return resp;
        };
        resp.cache_control = self.cache_control(max_age);
        if let Some(cache) = &self.cache {
            let entry = Cached::new(
                resp.status,
                resp.content_type,
                resp.body.as_arc().clone(),
                ttl,
                max_age,
                now,
            );
            cache.put(&req.target, entry, now);
        }
        resp
    }

    fn cache_control(&self, max_age: u64) -> CacheControl {
        if self.cfg.auth.required {
            CacheControl::Private(max_age)
        } else {
            CacheControl::Public(max_age)
        }
    }

    /// `(in-process TTL, Cache-Control max-age secs)` for a response, or
    /// `None` if it must not be cached (errors and not-found answers can
    /// change at the next block).
    fn ttl_for(&self, status: u16, f: Freshness) -> Option<(Duration, u64)> {
        if status != 200 {
            return None;
        }
        let c = &self.cfg.cache;
        let tip = Some((Duration::from_millis(c.tip_ttl_ms), c.tip_ttl_ms / 1000));
        match f {
            Freshness::NoCache => None,
            Freshness::Tip => tip,
            Freshness::Address => Some((
                Duration::from_millis(c.address_ttl_ms),
                c.address_ttl_ms / 1000,
            )),
            Freshness::AtHeight(h) => {
                let indexed = self.health.indexed_height();
                let buried = indexed >= h && indexed - h + 1 >= c.confirmations.max(1);
                if buried {
                    Some((
                        Duration::from_secs(c.immutable_ttl_secs),
                        c.immutable_ttl_secs,
                    ))
                } else {
                    tip
                }
            }
        }
    }

    fn esplora_route(&self, req: &Request) -> (HttpResponse, Freshness) {
        let e = Esplora::with_metrics(&self.store, &self.cfg, &self.metrics);
        let segs: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
        let limit = req
            .query_param("limit")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(100)
            .min(1000);
        let (r, f) = match segs.as_slice() {
            ["blocks", "tip", "height"] => (e.tip_height(), Freshness::Tip),
            ["blocks", "tip", "hash"] => (e.tip_hash(), Freshness::Tip),
            ["blocks", h] => match h.parse::<u32>() {
                Ok(n) => (e.block(Some(n), None), Freshness::AtHeight(n)),
                Err(_) => (
                    esplora::Response::bad_request("invalid height"),
                    Freshness::NoCache,
                ),
            },
            ["block", hash] => {
                let r = e.block(None, Some(hash));
                let f = height_freshness(r.body.get("height"));
                (r, f)
            }
            ["block-height", h] => match h.parse::<u32>() {
                Ok(n) => (e.block_hash_at_height(n), Freshness::AtHeight(n)),
                Err(_) => (
                    esplora::Response::bad_request("invalid height"),
                    Freshness::NoCache,
                ),
            },
            ["address", seg] => (e.address(seg), Freshness::Address),
            ["address", seg, "utxo"] => (e.address_utxo(seg, limit), Freshness::Address),
            ["address", seg, "txs"] => (e.address_txs(seg, limit), Freshness::Address),
            ["tx", txid] => {
                let r = e.tx(txid);
                let f = height_freshness(r.body.get("status").and_then(|s| s.get("block_height")));
                (r, f)
            }
            // Outspends change when a previously unspent output is spent.
            ["tx", txid, "outspends"] => (e.tx_outspends(txid), Freshness::Address),
            ["address-prefix", prefix] => (e.address_prefix(prefix), Freshness::Address),
            _ => (
                esplora::Response::not_found("unknown route"),
                Freshness::NoCache,
            ),
        };
        (render_esplora(r), f)
    }

    fn electrum(&self, req: &Request) -> HttpResponse {
        let parsed: serde_json::Value = match serde_json::from_str(&req.body) {
            Ok(v) => v,
            Err(e) => {
                return HttpResponse::json(
                    200,
                    json_rpc_error(
                        serde_json::Value::Null,
                        -32700,
                        &format!("parse error: {e}"),
                    ),
                )
            }
        };
        let el = Electrum::new(&self.store, &self.cfg);
        let id = parsed.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let method = match parsed.get("method").and_then(|m| m.as_str()) {
            Some(m) => m,
            None => {
                return HttpResponse::json(
                    200,
                    json_rpc_error(id, -32600, "invalid request: missing method"),
                )
            }
        };
        let empty = serde_json::Value::Null;
        let params = parsed.get("params").unwrap_or(&empty);
        match el.handle(method, params) {
            Ok(result) => HttpResponse::json(200, json_rpc_result(id, result)),
            Err(e) => HttpResponse::json(200, json_rpc_error(id, e.code, &e.message)),
        }
    }
}

fn render_esplora(r: esplora::Response) -> HttpResponse {
    let body = serde_json::to_string(&r.body).unwrap_or_else(|_| "{}".to_string());
    HttpResponse::json(r.status, body)
}

fn height_freshness(v: Option<&serde_json::Value>) -> Freshness {
    match v
        .and_then(|h| h.as_u64())
        .and_then(|h| u32::try_from(h).ok())
    {
        Some(h) => Freshness::AtHeight(h),
        None => Freshness::NoCache,
    }
}

fn err_body(msg: &str) -> String {
    format!("{{\"error\":\"{msg}\"}}")
}

fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

fn json_rpc_result(id: serde_json::Value, result: serde_json::Value) -> String {
    // Serializing a `serde_json::Value` is infallible (the type system admits
    // no non-serializable variant), so the fallback is defensive only.
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0", "id": id, "result": result
    }))
    .unwrap_or_else(|_| "{}".to_string())
}

fn json_rpc_error(id: serde_json::Value, code: i32, message: &str) -> String {
    // See json_rpc_result: Value serialization cannot fail.
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message }
    }))
    .unwrap_or_else(|_| "{}".to_string())
}

/// Best-effort node tip height via `getblockcount`. Returns `None` on any
/// error so a node outage never breaks serving.
fn fetch_node_height(node: &crate::config::NodeConfig) -> Option<u32> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let body = serde_json::json!({
        "jsonrpc": "1.0", "id": "bidx", "method": "getblockcount", "params": []
    });
    let mut req = client.post(&node.rpc_url).json(&body);
    if let Some(user) = &node.user {
        req = req.basic_auth(user, node.password.as_deref());
    }
    let resp = req.send().ok()?;
    let v: serde_json::Value = resp.json().ok()?;
    v.get("result")?.as_u64().map(|n| n as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{hex64, store as mock_store, wire};
    use std::net::TcpListener;

    fn service_with(routes: &[(&str, &str)]) -> Arc<Service> {
        let (s, _) = mock_store(routes, r#"{"data":[],"rows":0}"#);
        Arc::new(Service::new(
            Arc::new(s),
            Arc::new(Config::default()),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ))
    }

    #[test]
    fn split_query_and_request_params() {
        let (p, q) = split_query("/a/b?x=1&y=2");
        assert_eq!(p, "/a/b");
        assert_eq!(q, vec![("x".into(), "1".into()), ("y".into(), "2".into())]);
        let (p, q) = split_query("/a/b");
        assert_eq!(p, "/a/b");
        assert!(q.is_empty());
        // Flag-style param with no '='.
        let (_, q) = split_query("/a?flag");
        assert_eq!(q, vec![("flag".into(), String::new())]);
        let r = Request::new("GET", "/a?limit=5", "");
        assert_eq!(r.query_param("limit"), Some("5"));
        assert_eq!(r.query_param("nope"), None);
    }

    #[test]
    fn ops_endpoints() {
        let svc = service_with(&[]);
        let m = svc.handle(&Request::new("GET", "/metrics", ""));
        assert_eq!(m.status, 200);
        assert!(m.body.contains("bidx_queries_total"));

        let h = svc.handle(&Request::new("GET", "/healthz", ""));
        assert_eq!(h.status, 200);
        assert_eq!(h.body, "ok");

        // Not ready (no clickhouse/tip).
        let r = svc.handle(&Request::new("GET", "/readyz", ""));
        assert_eq!(r.status, 503);
        assert!(r.body.contains("\"ready\":false"));
    }

    #[test]
    fn readyz_becomes_ready_when_health_is_set() {
        let svc = service_with(&[]);
        svc.health.set_clickhouse_ok(true);
        svc.health.set_indexed_height(Some(10));
        let r = svc.handle(&Request::new("GET", "/readyz", ""));
        assert_eq!(r.status, 200);
        assert!(r.body.contains("\"ready\":true"));
    }

    #[test]
    fn esplora_routes_dispatch() {
        let disp = hex64(0xaa);
        let block = format!(
            r#"{{"data":[{{"height":5,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}}],"rows":1}}"#,
            wire(&disp),
            wire(&hex64(0)),
            wire(&hex64(0))
        );
        let svc = service_with(&[
            ("max(height)", r#"{"data":[{"height":5}],"rows":1}"#),
            ("FROM bitcoin.blocks", &block),
        ]);

        assert_eq!(
            svc.handle(&Request::new("GET", "/blocks/tip/height", ""))
                .body,
            "5"
        );
        assert_eq!(
            svc.handle(&Request::new("GET", "/blocks/tip/hash", ""))
                .body,
            format!("\"{disp}\"")
        );
        let b = svc.handle(&Request::new("GET", "/blocks/5", ""));
        assert_eq!(b.status, 200);
        assert!(b.body.contains("\"height\":5"));
        let bh = svc.handle(&Request::new("GET", "/block-height/5", ""));
        assert_eq!(bh.body, format!("\"{disp}\""));
        // Bad height.
        assert_eq!(
            svc.handle(&Request::new("GET", "/blocks/notanumber", ""))
                .status,
            400
        );
        // Unknown route.
        assert_eq!(svc.handle(&Request::new("GET", "/nope", "")).status, 404);
    }

    #[test]
    fn esplora_address_and_tx_routes_with_limit() {
        let stats = r#"{"data":[{"funded_count":1,"funded_sum":9,"spent_count":0,"spent_sum":0,"history_count":1}],"rows":1}"#;
        let svc = service_with(&[("FROM bitcoin.address_stats", stats)]);
        let r = svc.handle(&Request::new(
            "GET",
            "/address/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            "",
        ));
        assert_eq!(r.status, 200);
        assert!(r.body.contains("\"balance\":9"));

        // utxo + txs routes (empty mocks → empty arrays).
        let r = svc.handle(&Request::new(
            "GET",
            "/address/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa/utxo?limit=5",
            "",
        ));
        assert_eq!(r.body, "[]");
        let r = svc.handle(&Request::new(
            "GET",
            "/address/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa/txs",
            "",
        ));
        assert_eq!(r.body, "[]");

        // address-prefix.
        let svc2 = service_with(&[(
            "address_prefix",
            r#"{"data":[{"address":"bc1q","script_type":4}],"rows":1}"#,
        )]);
        let r = svc2.handle(&Request::new("GET", "/address-prefix/bc1q", ""));
        assert_eq!(r.body, "[\"bc1q\"]");

        // tx not found.
        let r = svc.handle(&Request::new(
            "GET",
            &format!("/tx/{disp}", disp = hex64(0xbb)),
            "",
        ));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn method_not_allowed_and_disabled_surfaces() {
        let svc = service_with(&[]);
        assert_eq!(
            svc.handle(&Request::new("POST", "/blocks/tip/height", ""))
                .status,
            405
        );

        // Esplora disabled.
        let (s, _) = mock_store(&[], r#"{"data":[],"rows":0}"#);
        let mut cfg = Config::default();
        cfg.esplora_enabled = false;
        let svc = Arc::new(Service::new(
            Arc::new(s),
            Arc::new(cfg),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ));
        assert_eq!(
            svc.handle(&Request::new("GET", "/blocks/tip/height", ""))
                .status,
            404
        );

        // Electrum disabled.
        let (s, _) = mock_store(&[], r#"{"data":[],"rows":0}"#);
        let mut cfg = Config::default();
        cfg.electrum_enabled = false;
        let svc = Arc::new(Service::new(
            Arc::new(s),
            Arc::new(cfg),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ));
        assert_eq!(svc.handle(&Request::new("POST", "/", "{}")).status, 404);
    }

    #[test]
    fn electrum_json_rpc_success_and_errors() {
        let svc = service_with(&[]);
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"server.version","params":[]}"#;
        let r = svc.handle(&Request::new("POST", "/", body));
        assert_eq!(r.status, 200);
        assert!(r.body.contains("\"result\""));
        assert!(r.body.contains("\"id\":1"));

        // Parse error → -32700.
        let r = svc.handle(&Request::new("POST", "/", "not json"));
        assert!(r.body.contains("-32700"));

        // Missing method → -32600.
        let r = svc.handle(&Request::new("POST", "/", r#"{"id":2}"#));
        assert!(r.body.contains("-32600"));

        // Unknown method → -32601.
        let r = svc.handle(&Request::new(
            "POST",
            "/electrum",
            r#"{"id":3,"method":"nope"}"#,
        ));
        assert!(r.body.contains("-32601"));
        // params absent → null, handled.
        let r = svc.handle(&Request::new(
            "POST",
            "/",
            r#"{"id":4,"method":"server.ping"}"#,
        ));
        assert!(r.body.contains("\"result\":null"));
    }

    #[test]
    fn metrics_are_incremented_by_traffic() {
        let svc = service_with(&[]);
        svc.handle(&Request::new("GET", "/healthz", ""));
        svc.handle(&Request::new("GET", "/nope", "")); // 404 → 4xx
        svc.handle(&Request::new("POST", "/", r#"{"method":"server.ping"}"#));
        let text = svc.metrics.registry.render();
        assert!(text.contains("bidx_queries_total 3\n"), "metrics: {text}");
        assert!(
            text.contains("bidx_responses_by_status_total{status=\"2xx\"} 2\n"),
            "metrics: {text}"
        );
        assert!(
            text.contains("bidx_responses_by_status_total{status=\"4xx\"} 1\n"),
            "metrics: {text}"
        );
        assert!(
            text.contains("bidx_requests_by_surface_total{surface=\"ops\"} 1\n"),
            "metrics: {text}"
        );
        assert!(
            text.contains("bidx_requests_by_surface_total{surface=\"electrum\"} 1\n"),
            "metrics: {text}"
        );
    }

    #[test]
    fn server_error_increments_error_counter() {
        // A transport failure inside the store → 500.
        struct Failing;
        impl crate::store::Transport for Failing {
            fn query(&self, _: &str) -> Result<String, crate::store::StoreError> {
                Err(crate::store::StoreError::Transport("down".into()))
            }
        }
        let s = Arc::new(Store::new(Box::new(Failing), "bitcoin"));
        let svc = Arc::new(Service::new(
            s,
            Arc::new(Config::default()),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ));
        let r = svc.handle(&Request::new("GET", "/blocks/tip/height", ""));
        assert_eq!(r.status, 500);
        let text = svc.metrics.registry.render();
        assert!(text.contains("bidx_query_errors_total 1\n"));
        assert!(text.contains("bidx_responses_by_status_total{status=\"5xx\"} 1\n"));
    }

    #[test]
    fn refresh_health_sets_ready_and_metrics() {
        let svc = service_with(&[("max(height)", r#"{"data":[{"height":42}],"rows":1}"#)]);
        svc.refresh_health();
        assert!(svc.health.ready());
        let text = svc.metrics.registry.render();
        assert!(text.contains("bidx_indexed_height 42\n"));
    }

    #[test]
    fn refresh_health_marks_unready_on_transport_failure() {
        struct Failing;
        impl crate::store::Transport for Failing {
            fn query(&self, _: &str) -> Result<String, crate::store::StoreError> {
                Err(crate::store::StoreError::Transport("down".into()))
            }
        }
        let s = Arc::new(Store::new(Box::new(Failing), "bitcoin"));
        let svc = Arc::new(Service::new(
            s,
            Arc::new(Config::default()),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ));
        svc.refresh_health();
        assert!(!svc.health.ready());
    }

    #[test]
    fn refresh_health_queries_node_when_configured() {
        // With a node configured, refresh_health must attempt the RPC. The
        // node here is unreachable, so the height stays unknown (lag 0) but
        // the attempt is exercised.
        let (s, _) = mock_store(
            &[("max(height)", r#"{"data":[{"height":10}],"rows":1}"#)],
            r#"{"data":[],"rows":0}"#,
        );
        let mut cfg = Config::default();
        cfg.node = Some(crate::config::NodeConfig {
            rpc_url: "http://127.0.0.1:1".into(),
            cookie: None,
            user: Some("u".into()),
            password: Some("p".into()),
        });
        let svc = Arc::new(Service::new(
            Arc::new(s),
            Arc::new(cfg),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ));
        svc.refresh_health();
        assert!(svc.health.ready());
        // Node unreachable → node height 0, lag 0 (saturating).
        assert_eq!(svc.health.lag(), 0);
    }

    /// A one-shot JSON-RPC node: answers the first request with `{"result":h}`.
    fn mock_node(height: u32) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = std::io::Read::read(&mut s, &mut buf);
                let body = format!("{{\"result\":{height},\"error\":null,\"id\":\"bidx\"}}");
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = std::io::Write::write_all(&mut s, resp.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    #[test]
    fn refresh_health_records_node_height_and_lag() {
        let url = mock_node(120);
        let (s, _) = mock_store(
            &[("max(height)", r#"{"data":[{"height":100}],"rows":1}"#)],
            r#"{"data":[],"rows":0}"#,
        );
        let mut cfg = Config::default();
        cfg.node = Some(crate::config::NodeConfig {
            rpc_url: url,
            cookie: None,
            user: None,
            password: None,
        });
        let metrics = Arc::new(Metrics::new());
        let svc = Arc::new(Service::new(
            Arc::new(s),
            Arc::new(cfg),
            metrics.clone(),
            Arc::new(Health::new()),
        ));
        svc.refresh_health();
        assert_eq!(svc.health.node_height(), 120);
        assert_eq!(svc.health.lag(), 20);
        let text = metrics.registry.render();
        assert!(text.contains("bidx_node_height 120\n"), "metrics: {text}");
        assert!(
            text.contains("bidx_indexer_lag_blocks 20\n"),
            "metrics: {text}"
        );
    }

    #[test]
    fn block_not_found_returns_404() {
        let svc = service_with(&[]);
        let r = svc.handle(&Request::new("GET", "/blocks/999", ""));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn block_by_hash_and_bad_height_routes() {
        let svc = service_with(&[]);
        // /block/:hash with a well-formed hash (no row) → 404.
        let r = svc.handle(&Request::new("GET", &format!("/block/{}", hex64(0xaa)), ""));
        assert_eq!(r.status, 404);
        // /block-height/:h with a non-numeric height → 400.
        let r = svc.handle(&Request::new("GET", "/block-height/nope", ""));
        assert_eq!(r.status, 400);
    }

    #[test]
    fn tx_routes_dispatch_through_the_router() {
        let svc = service_with(&[]);
        // /tx/:txid and /tx/:txid/outspends both reach the handler (404 here
        // because the mock store has no rows).
        assert_eq!(
            svc.handle(&Request::new("GET", &format!("/tx/{}", hex64(0xaa)), ""))
                .status,
            404
        );
        assert_eq!(
            svc.handle(&Request::new(
                "GET",
                &format!("/tx/{}/outspends", hex64(0xaa)),
                ""
            ))
            .status,
            404
        );
    }

    #[test]
    fn render_esplora_serializes_status_and_body() {
        let r = esplora::Response::ok(serde_json::json!({"ok": true}));
        let http = render_esplora(r);
        assert_eq!(http.status, 200);
        assert_eq!(http.body, "{\"ok\":true}");
    }

    #[test]
    fn json_rpc_helpers_are_valid_json() {
        let ok = json_rpc_result(serde_json::json!(1), serde_json::json!(["a"]));
        let v: serde_json::Value = serde_json::from_str(&ok).unwrap();
        assert_eq!(v["result"][0], "a");
        let err = json_rpc_error(serde_json::Value::Null, -32601, "nope");
        let v: serde_json::Value = serde_json::from_str(&err).unwrap();
        assert_eq!(v["error"]["code"], -32601);
    }

    #[test]
    fn response_reasons() {
        assert_eq!(HttpResponse::text(200, "x").reason(), "OK");
        assert_eq!(HttpResponse::text(404, "x").reason(), "Not Found");
        assert_eq!(HttpResponse::text(405, "x").reason(), "Method Not Allowed");
        assert_eq!(HttpResponse::text(503, "x").reason(), "Service Unavailable");
        assert_eq!(
            HttpResponse::text(500, "x").reason(),
            "Internal Server Error"
        );
        assert_eq!(HttpResponse::text(400, "x").reason(), "Bad Request");
        assert_eq!(HttpResponse::text(401, "x").reason(), "Unauthorized");
        assert_eq!(HttpResponse::text(429, "x").reason(), "Too Many Requests");
        assert_eq!(HttpResponse::text(418, "x").reason(), "Unknown");
    }

    fn block_row(height: u32) -> String {
        format!(
            r#"{{"data":[{{"height":{height},"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}}],"rows":1}}"#,
            wire(&hex64(0xaa)),
            wire(&hex64(0)),
            wire(&hex64(0))
        )
    }

    fn svc_cfg(
        routes: &[(&str, &str)],
        cfg: Config,
    ) -> (Arc<Service>, Arc<crate::testutil::RouteMock>) {
        let (s, mock) = mock_store(routes, r#"{"data":[],"rows":0}"#);
        let names: Vec<&str> = cfg.auth.keys.iter().map(|k| k.name.as_str()).collect();
        let metrics = Arc::new(Metrics::with_tenants(&names));
        let svc = Service::new(
            Arc::new(s),
            Arc::new(cfg.clone()),
            metrics,
            Arc::new(Health::new()),
        );
        (Arc::new(svc), mock)
    }

    fn api_key(name: &str, token: &str, rate: f64, burst: f64) -> crate::config::ApiKey {
        crate::config::ApiKey {
            name: name.into(),
            token_sha256: crate::config::ApiKey::hash_token(token),
            rate_per_sec: rate,
            burst,
        }
    }

    #[test]
    fn auth_required_gates_the_api_but_never_ops_endpoints() {
        let mut cfg = Config::default();
        cfg.auth.required = true;
        cfg.auth.keys = vec![api_key("acme", "tok", 0.0, 1.0)];
        let (svc, _) = svc_cfg(
            &[("max(height)", r#"{"data":[{"height":7}],"rows":1}"#)],
            cfg,
        );

        let r = svc.handle(&Request::new("GET", "/blocks/tip/height", ""));
        assert_eq!(r.status, 401);
        assert_eq!(r.reason(), "Unauthorized");
        assert!(r.body.contains("API key"));
        // Electrum is gated too, and counted under its surface.
        let r = svc.handle(&Request::new("POST", "/", r#"{"method":"server.ping"}"#));
        assert_eq!(r.status, 401);
        let r = svc.handle(
            &Request::new("GET", "/blocks/tip/height", "")
                .with_header("Authorization", "Bearer nope"),
        );
        assert_eq!(r.status, 401);
        // Probes and scrapes need no key.
        assert_eq!(svc.handle(&Request::new("GET", "/healthz", "")).status, 200);

        let ok = svc.handle(
            &Request::new("GET", "/blocks/tip/height", "")
                .with_header("Authorization", "Bearer tok"),
        );
        assert_eq!((ok.status, &*ok.body), (200, "7"));
        // Authenticated responses must not be cached by shared caches.
        assert_eq!(ok.cache_control, CacheControl::Private(1));
        let ok = svc
            .handle(&Request::new("GET", "/blocks/tip/height", "").with_header("X-API-Key", "tok"));
        assert_eq!(ok.status, 200);

        let m = svc.metrics.render();
        assert!(m.contains("bidx_unauthorized_total 3\n"), "{m}");
        assert!(
            m.contains("bidx_tenant_requests_total{tenant=\"acme\"} 2\n"),
            "{m}"
        );
        assert!(
            m.contains("bidx_requests_by_surface_total{surface=\"electrum\"} 1\n"),
            "{m}"
        );
        assert!(
            m.contains("bidx_responses_by_status_total{status=\"4xx\"} 3\n"),
            "{m}"
        );
    }

    #[test]
    fn per_key_quota_returns_429_with_retry_after() {
        let mut cfg = Config::default();
        cfg.auth.keys = vec![api_key("small", "s", 0.5, 1.0)];
        let (svc, _) = svc_cfg(&[], cfg);
        let req = Request::new("GET", "/healthz-not", "").with_header("x-api-key", "s");
        assert_eq!(svc.handle(&req).status, 404);
        let r = svc.handle(&req);
        assert_eq!(r.status, 429);
        assert_eq!(r.reason(), "Too Many Requests");
        assert_eq!(r.retry_after, Some(2));
        // Anonymous access still works (auth not required, no IP limit).
        assert_eq!(svc.handle(&Request::new("GET", "/nope", "")).status, 404);
        let m = svc.metrics.render();
        assert!(m.contains("bidx_rate_limited_total 1\n"), "{m}");
        assert!(
            m.contains("bidx_tenant_rate_limited_total{tenant=\"small\"} 1\n"),
            "{m}"
        );
    }

    #[test]
    fn per_ip_limit_applies_to_anonymous_traffic() {
        let mut cfg = Config::default();
        cfg.rate_limit.per_ip_rps = 0.001;
        cfg.rate_limit.per_ip_burst = 1.0;
        let (svc, _) = svc_cfg(&[], cfg.clone());
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let req = Request::new("GET", "/nope", "").with_peer(peer);
        assert_eq!(svc.handle(&req).status, 404);
        assert_eq!(svc.handle(&req).status, 429);
        // X-Forwarded-For is ignored unless trusted...
        let spoof = req.clone().with_header("X-Forwarded-For", "1.2.3.4");
        assert_eq!(svc.handle(&spoof).status, 429);
        // ...and honored when it is.
        cfg.server.trust_forwarded_for = true;
        let (svc, _) = svc_cfg(&[], cfg);
        assert_eq!(svc.handle(&req).status, 404);
        assert_eq!(svc.handle(&spoof).status, 404, "distinct forwarded client");
        assert_eq!(svc.handle(&spoof).status, 429);
    }

    #[test]
    fn cache_serves_repeats_without_querying_clickhouse() {
        let (svc, mock) = svc_cfg(
            &[("max(height)", r#"{"data":[{"height":9}],"rows":1}"#)],
            Config::default(),
        );
        let a = svc.handle(&Request::new("GET", "/blocks/tip/height", ""));
        let queries = mock.calls().len();
        let b = svc.handle(&Request::new("GET", "/blocks/tip/height", ""));
        assert_eq!(mock.calls().len(), queries, "second request hit the cache");
        assert_eq!(a, b);
        assert!(
            Arc::ptr_eq(a.body.as_arc(), b.body.as_arc()),
            "hit shares the body"
        );
        // A different query string is a different key.
        svc.handle(&Request::new("GET", "/blocks/tip/height?x=1", ""));
        assert!(mock.calls().len() > queries);
        let m = svc.metrics.render();
        assert!(m.contains("bidx_cache_hits_total 1\n"), "{m}");
        assert!(m.contains("bidx_cache_misses_total 2\n"), "{m}");
    }

    #[test]
    fn errors_and_not_found_are_never_cached() {
        let (svc, mock) = svc_cfg(&[], Config::default());
        let r = svc.handle(&Request::new("GET", "/blocks/999", ""));
        assert_eq!((r.status, r.cache_control), (404, CacheControl::NoStore));
        let n = mock.calls().len();
        svc.handle(&Request::new("GET", "/blocks/999", ""));
        assert!(mock.calls().len() > n, "404 re-queried");
        let r = svc.handle(&Request::new("GET", "/blocks/abc", ""));
        assert_eq!((r.status, r.cache_control), (400, CacheControl::NoStore));
    }

    #[test]
    fn deep_blocks_are_immutable_and_shallow_ones_use_the_tip_ttl() {
        let block = block_row(95);
        let (svc, _) = svc_cfg(&[("FROM bitcoin.blocks", &block)], Config::default());
        svc.health.set_indexed_height(Some(100));
        // depth = 100 - 95 + 1 = 6 = confirmations → immutable.
        let r = svc.handle(&Request::new("GET", "/blocks/95", ""));
        assert_eq!(r.cache_control, CacheControl::Public(300));
        let r = svc.handle(&Request::new("GET", "/block-height/95", ""));
        assert_eq!(r.cache_control, CacheControl::Public(300));
        // depth 5 → still reorgable.
        let r = svc.handle(&Request::new("GET", "/blocks/96", ""));
        assert_eq!(r.cache_control, CacheControl::Public(1));
        // Above the indexed tip (stale health) → tip TTL, never immutable.
        let r = svc.handle(&Request::new("GET", "/blocks/150", ""));
        assert_eq!(r.cache_control, CacheControl::Public(1));
        // By hash: the height comes from the body (95 → immutable).
        let r = svc.handle(&Request::new("GET", &format!("/block/{}", hex64(0xaa)), ""));
        assert_eq!(
            (r.status, r.cache_control),
            (200, CacheControl::Public(300))
        );
    }

    #[test]
    fn address_routes_use_the_address_ttl_and_ops_are_no_store() {
        let stats = r#"{"data":[{"funded_count":1,"funded_sum":9,"spent_count":0,"spent_sum":0,"history_count":1}],"rows":1}"#;
        let mut cfg = Config::default();
        cfg.cache.address_ttl_ms = 4_000;
        let (svc, _) = svc_cfg(&[("FROM bitcoin.address_stats", stats)], cfg);
        let r = svc.handle(&Request::new(
            "GET",
            "/address/1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            "",
        ));
        assert_eq!((r.status, r.cache_control), (200, CacheControl::Public(4)));
        let r = svc.handle(&Request::new("GET", "/metrics", ""));
        assert_eq!(r.cache_control, CacheControl::NoStore);
        let r = svc.handle(&Request::new("POST", "/", r#"{"method":"server.ping"}"#));
        assert_eq!(r.cache_control, CacheControl::NoStore);
    }

    #[test]
    fn disabled_cache_still_emits_cache_control_for_cdns() {
        let mut cfg = Config::default();
        cfg.cache.enabled = false;
        let (svc, mock) = svc_cfg(
            &[("max(height)", r#"{"data":[{"height":3}],"rows":1}"#)],
            cfg,
        );
        let r = svc.handle(&Request::new("GET", "/blocks/tip/height", ""));
        assert_eq!(r.cache_control, CacheControl::Public(1));
        let n = mock.calls().len();
        svc.handle(&Request::new("GET", "/blocks/tip/height", ""));
        assert!(mock.calls().len() > n);
        assert!(svc.metrics.render().contains("bidx_cache_misses_total 0\n"));
    }

    #[test]
    fn ttl_for_skips_uncacheable_and_non_200_responses() {
        let (svc, _) = svc_cfg(&[], Config::default());
        assert_eq!(svc.ttl_for(200, Freshness::NoCache), None);
        assert_eq!(svc.ttl_for(404, Freshness::Tip), None);
        assert_eq!(
            svc.ttl_for(200, Freshness::Tip),
            Some((Duration::from_secs(1), 1))
        );
        // confirmations = 0 is treated as 1: the tip block itself is immutable.
        let mut cfg = Config::default();
        cfg.cache.confirmations = 0;
        let (svc, _) = svc_cfg(&[], cfg);
        svc.health.set_indexed_height(Some(10));
        assert_eq!(
            svc.ttl_for(200, Freshness::AtHeight(10)),
            Some((Duration::from_secs(300), 300))
        );
    }

    #[test]
    fn height_freshness_reads_numeric_heights_only() {
        assert_eq!(
            height_freshness(Some(&serde_json::json!(7))),
            Freshness::AtHeight(7)
        );
        assert_eq!(
            height_freshness(Some(&serde_json::json!("7"))),
            Freshness::NoCache
        );
        assert_eq!(
            height_freshness(Some(&serde_json::json!(u64::MAX))),
            Freshness::NoCache
        );
        assert_eq!(height_freshness(None), Freshness::NoCache);
    }

    #[test]
    fn cache_control_and_body_render() {
        assert_eq!(CacheControl::NoStore.to_string(), "no-store");
        assert_eq!(CacheControl::Public(0).to_string(), "no-cache");
        assert_eq!(CacheControl::Private(0).to_string(), "no-cache");
        assert_eq!(CacheControl::Public(30).to_string(), "public, max-age=30");
        assert_eq!(CacheControl::Private(30).to_string(), "private, max-age=30");
        let b: Body = Arc::<str>::from("hi").into();
        assert_eq!(format!("{b}|{b:?}"), "hi|\"hi\"");
        let owned = String::from("hi");
        assert!(b == *"hi" && b == "hi" && b == owned);
    }

    #[test]
    fn request_builders_set_headers_lowercased_and_peer() {
        let peer: IpAddr = "::1".parse().unwrap();
        let r = Request::new("GET", "/x?a=1", "")
            .with_header("X-Thing", "v")
            .with_peer(peer);
        assert_eq!(r.header("x-thing"), Some("v"));
        assert_eq!(r.header("missing"), None);
        assert_eq!(r.peer, Some(peer));
        assert_eq!(r.target, "/x?a=1");
        assert!(!r.keep_alive);
    }

    #[test]
    fn status_class_mapping() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
        assert_eq!(status_class(503), "5xx");
    }
}
