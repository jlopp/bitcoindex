//! The serving HTTP surface: routing, JSON-RPC dispatch, metrics and health.
//!
//! Deliberately dependency-free (std `TcpListener` only). Requests are parsed
//! into a [`Request`], routed by [`Service::handle`] to the Esplora REST
//! handlers, the Electrum JSON-RPC dispatcher, or the ops endpoints, and the
//! result rendered as an [`HttpResponse`]. `handle` is pure with respect to
//! I/O so it is unit-testable; [`serve_listener`] adds the socket loop.
//!
//! One request per connection, `Connection: close`. Thread-per-connection
//! keeps a slow ClickHouse query from stalling the accept loop.

use crate::config::Config;
use crate::electrum::Electrum;
use crate::esplora::{self, Esplora};
use crate::health::Health;
use crate::metrics::Metrics;
use crate::store::Store;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

/// Maximum request body we will read (JSON-RPC batches are tiny).
const MAX_BODY: usize = 1 << 20;
/// Per-connection read/write timeout.
const IO_TIMEOUT: Duration = Duration::from_secs(30);

/// A parsed HTTP request.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: String,
}

impl Request {
    pub fn new(method: &str, path: &str, body: &str) -> Self {
        let (path, query) = split_query(path);
        Request {
            method: method.to_string(),
            path,
            query,
            body: body.to_string(),
        }
    }

    pub fn query_param(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// An HTTP response with a rendered body.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

impl HttpResponse {
    fn json(status: u16, body: String) -> Self {
        HttpResponse {
            status,
            content_type: "application/json",
            body,
        }
    }
    fn text(status: u16, body: impl Into<String>) -> Self {
        HttpResponse {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
        }
    }

    pub fn reason(&self) -> &'static str {
        match self.status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
    }
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
}

impl Service {
    pub fn new(
        store: Arc<Store>,
        cfg: Arc<Config>,
        metrics: Arc<Metrics>,
        health: Arc<Health>,
    ) -> Self {
        Service {
            store,
            cfg,
            metrics,
            health,
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
        let (resp, surface) = self.route(req);
        // Instrumentation.
        self.metrics.inc_queries();
        self.metrics.inc_surface(surface);
        self.metrics.inc_status(status_class(resp.status));
        if resp.status >= 500 {
            self.metrics.inc_query_errors();
        }
        resp
    }

    fn route(&self, req: &Request) -> (HttpResponse, &'static str) {
        // Ops endpoints are always available.
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/metrics") => {
                return (HttpResponse::text(200, self.metrics.registry.render()), "ops");
            }
            ("GET", "/healthz") => return (HttpResponse::text(200, "ok"), "ops"),
            ("GET", "/readyz") => {
                let ready = self.health.ready();
                let status = if ready { 200 } else { 503 };
                return (HttpResponse::json(status, self.health.ready_json()), "ops");
            }
            _ => {}
        }

        // Electrum JSON-RPC: POST to the root or /electrum.
        if req.method == "POST" && (req.path == "/" || req.path == "/electrum") {
            if !self.cfg.electrum_enabled {
                return (HttpResponse::json(404, err_body("electrum disabled")), "ops");
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
        (self.esplora_route(req), "esplora")
    }

    fn esplora_route(&self, req: &Request) -> HttpResponse {
        let e = Esplora::with_metrics(&self.store, &self.cfg, &self.metrics);
        let segs: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
        let limit = req
            .query_param("limit")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(100)
            .min(1000);
        let r = match segs.as_slice() {
            ["blocks", "tip", "height"] => e.tip_height(),
            ["blocks", "tip", "hash"] => e.tip_hash(),
            ["blocks", h] => match h.parse::<u32>() {
                Ok(n) => e.block(Some(n), None),
                Err(_) => esplora::Response::bad_request("invalid height"),
            },
            ["block", hash] => e.block(None, Some(hash)),
            ["block-height", h] => match h.parse::<u32>() {
                Ok(n) => e.block_hash_at_height(n),
                Err(_) => esplora::Response::bad_request("invalid height"),
            },
            ["address", seg] => e.address(seg),
            ["address", seg, "utxo"] => e.address_utxo(seg, limit),
            ["address", seg, "txs"] => e.address_txs(seg, limit),
            ["tx", txid] => e.tx(txid),
            ["tx", txid, "outspends"] => e.tx_outspends(txid),
            ["address-prefix", prefix] => e.address_prefix(prefix),
            _ => esplora::Response::not_found("unknown route"),
        };
        render_esplora(r)
    }

    fn electrum(&self, req: &Request) -> HttpResponse {
        let parsed: serde_json::Value = match serde_json::from_str(&req.body) {
            Ok(v) => v,
            Err(e) => {
                return HttpResponse::json(
                    200,
                    json_rpc_error(serde_json::Value::Null, -32700, &format!("parse error: {e}")),
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

/// Parse one HTTP/1.1 request from a stream.
fn read_request(stream: &mut TcpStream) -> std::io::Result<Request> {
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.trim_end().split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    if method.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty request line",
        ));
    }
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0).min(MAX_BODY);
            }
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }
    let body = String::from_utf8_lossy(&body).to_string();
    Ok(Request::new(&method, &target, &body))
}

/// Write one response and close.
fn write_response(stream: &mut TcpStream, resp: &HttpResponse) -> std::io::Result<()> {
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        resp.status,
        resp.reason(),
        resp.content_type,
        resp.body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(resp.body.as_bytes())?;
    stream.flush()
}

/// Serve on an already-bound listener. Each connection is handled on its own
/// thread; a malformed request is answered 400 rather than dropping the loop.
pub fn serve_listener(listener: TcpListener, service: Arc<Service>) {
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let service = service.clone();
        std::thread::spawn(move || {
            let resp = match read_request(&mut stream) {
                Ok(req) => service.handle(&req),
                Err(_) => HttpResponse::json(400, err_body("bad request")),
            };
            let _ = write_response(&mut stream, &resp);
        });
    }
}

/// Bind and serve on `addr`.
pub fn serve(addr: &str, service: Arc<Service>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    serve_bound(addr, listener, service);
    Ok(())
}

/// Log and serve on an already-bound listener. Split from [`serve`] so the
/// bind (fallible) and the accept loop (blocks forever) can be exercised
/// independently.
pub fn serve_bound(addr: &str, listener: TcpListener, service: Arc<Service>) {
    tracing::info!("bidx serve listening on {addr}");
    serve_listener(listener, service);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{hex64, store as mock_store, wire};

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

        assert_eq!(svc.handle(&Request::new("GET", "/blocks/tip/height", "")).body, "5");
        assert_eq!(
            svc.handle(&Request::new("GET", "/blocks/tip/hash", "")).body,
            format!("\"{disp}\"")
        );
        let b = svc.handle(&Request::new("GET", "/blocks/5", ""));
        assert_eq!(b.status, 200);
        assert!(b.body.contains("\"height\":5"));
        let bh = svc.handle(&Request::new("GET", "/block-height/5", ""));
        assert_eq!(bh.body, format!("\"{disp}\""));
        // Bad height.
        assert_eq!(
            svc.handle(&Request::new("GET", "/blocks/notanumber", "")).status,
            400
        );
        // Unknown route.
        assert_eq!(
            svc.handle(&Request::new("GET", "/nope", "")).status,
            404
        );
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
        let svc2 = service_with(&[("address_prefix", r#"{"data":[{"address":"bc1q","script_type":4}],"rows":1}"#)]);
        let r = svc2.handle(&Request::new("GET", "/address-prefix/bc1q", ""));
        assert_eq!(r.body, "[\"bc1q\"]");

        // tx not found.
        let r = svc.handle(&Request::new("GET", &format!("/tx/{disp}", disp = hex64(0xbb)), ""));
        assert_eq!(r.status, 404);
    }

    #[test]
    fn method_not_allowed_and_disabled_surfaces() {
        let svc = service_with(&[]);
        assert_eq!(svc.handle(&Request::new("POST", "/blocks/tip/height", "")).status, 405);

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
        assert_eq!(svc.handle(&Request::new("GET", "/blocks/tip/height", "")).status, 404);

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
        let r = svc.handle(&Request::new("POST", "/", r#"{"id":4,"method":"server.ping"}"#));
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
        let (s, _) = mock_store(&[("max(height)", r#"{"data":[{"height":10}],"rows":1}"#)], r#"{"data":[],"rows":0}"#);
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
        let (s, _) = mock_store(&[("max(height)", r#"{"data":[{"height":100}],"rows":1}"#)], r#"{"data":[],"rows":0}"#);
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
        assert!(text.contains("bidx_indexer_lag_blocks 20\n"), "metrics: {text}");
    }

    #[test]
    fn read_request_errors_on_truncated_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream).is_err()
        });
        let mut c = TcpStream::connect(addr).unwrap();
        // Declares 100 bytes of body but sends none, then closes.
        c.write_all(b"POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\n").unwrap();
        drop(c);
        assert!(handle.join().unwrap());
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
            svc.handle(&Request::new("GET", &format!("/tx/{}", hex64(0xaa)), "")).status,
            404
        );
        assert_eq!(
            svc.handle(&Request::new("GET", &format!("/tx/{}/outspends", hex64(0xaa)), "")).status,
            404
        );
    }

    #[test]
    fn serve_bound_logs_and_accepts_connections() {
        let svc = service_with(&[]);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let svc2 = svc.clone();
        std::thread::spawn(move || serve_bound("test", listener, svc2));
        // A request answered proves the accept loop started.
        let mut c = TcpStream::connect(addr).unwrap();
        c.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let mut resp = String::new();
        c.read_to_string(&mut resp).unwrap();
        assert!(resp.contains("200 OK"), "got: {resp}");
    }

    #[test]
    fn serve_binds_and_serves() {
        let svc = service_with(&[]);
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe); // free the port for `serve`
        let svc2 = svc.clone();
        std::thread::spawn(move || {
            let _ = serve(&addr.to_string(), svc2);
        });
        // Retry the connect briefly to avoid a race with bind().
        let mut last = String::new();
        for _ in 0..50 {
            if let Ok(mut c) = TcpStream::connect(addr) {
                c.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
                let _ = c.read_to_string(&mut last);
                if last.contains("200 OK") {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(last.contains("200 OK"), "got: {last}");
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
    fn read_request_parses_method_target_and_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Server side: read one request and echo the parsed fields as JSON.
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let req = read_request(&mut stream).unwrap();
            serde_json::json!({
                "method": req.method, "path": req.path, "body": req.body,
                "limit": req.query_param("limit"),
            })
        });
        let mut c = TcpStream::connect(addr).unwrap();
        let body = "{\"x\":1}";
        let raw = format!(
            "POST /tx/abc?limit=5 HTTP/1.1\r\nHost: h\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        c.write_all(raw.as_bytes()).unwrap();
        let v = handle.join().unwrap();
        assert_eq!(v["method"], "POST");
        assert_eq!(v["path"], "/tx/abc");
        assert_eq!(v["body"], body);
        assert_eq!(v["limit"], "5");
    }

    #[test]
    fn read_request_rejects_empty_request_line() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream).is_err()
        });
        let mut c = TcpStream::connect(addr).unwrap();
        // A blank first line yields an empty method → InvalidData.
        c.write_all(b"\r\n\r\n").unwrap();
        assert!(handle.join().unwrap());
    }

    #[test]
    fn response_reasons() {
        assert_eq!(HttpResponse::text(200, "x").reason(), "OK");
        assert_eq!(HttpResponse::text(404, "x").reason(), "Not Found");
        assert_eq!(HttpResponse::text(405, "x").reason(), "Method Not Allowed");
        assert_eq!(HttpResponse::text(503, "x").reason(), "Service Unavailable");
        assert_eq!(HttpResponse::text(500, "x").reason(), "Internal Server Error");
        assert_eq!(HttpResponse::text(400, "x").reason(), "Bad Request");
        assert_eq!(HttpResponse::text(418, "x").reason(), "Unknown");
    }

    #[test]
    fn status_class_mapping() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
        assert_eq!(status_class(503), "5xx");
    }

    #[test]
    fn end_to_end_over_a_real_socket() {
        let svc = service_with(&[("max(height)", r#"{"data":[{"height":123}],"rows":1}"#)]);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve_listener(listener, svc));

        // GET request.
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .write_all(b"GET /blocks/tip/height HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
        assert!(resp.contains("Content-Type: application/json"));
        assert!(resp.trim_end().ends_with("123"));

        // POST JSON-RPC request.
        let body = r#"{"id":1,"method":"server.ping"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(resp.contains("\"result\":null"), "got: {resp}");

        // Malformed request line → 400.
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"\r\n\r\n").unwrap();
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        assert!(resp.starts_with("HTTP/1.1 400"), "got: {resp}");
    }
}
