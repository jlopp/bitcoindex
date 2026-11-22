//! HTTP/1.1 connection handling: a fixed worker pool fed by a bounded queue,
//! persistent connections, load shedding, and graceful shutdown.
//!
//! * The accept thread hands sockets to `workers` threads over a bounded
//!   channel. When the queue is full the connection gets an immediate
//!   `503 Retry-After: 1` instead of waiting — under overload it is better to
//!   fail fast and let the client (or the load balancer) retry elsewhere.
//! * Connections are kept alive (HTTP/1.1 default) up to
//!   `max_requests_per_conn`, then closed so L4 balancers rebalance.
//! * On [`Shutdown::trigger`] the accept loop stops, queued connections are
//!   still served, and workers close each connection after its current
//!   request. [`serve_until`] returns once workers finish or
//!   `shutdown_timeout` passes.

use crate::config::ServerConfig;
use crate::service::{HttpResponse, Request, Service};
use crate::shutdown::Shutdown;
use crossbeam_channel::{RecvTimeoutError, TrySendError};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

/// Maximum request body we will read (JSON-RPC batches are tiny).
const MAX_BODY: usize = 1 << 20;
/// Maximum length of the request line or of one header line.
const MAX_LINE: u64 = 8 * 1024;
const MAX_HEADERS: usize = 100;
/// Read/write timeout for the first request on a connection.
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// Write timeout for the 503 sent when shedding, so a slow client cannot
/// stall the accept thread.
const SHED_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

/// Sizing and lifecycle knobs for one listener.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    pub workers: usize,
    pub queue: usize,
    /// `None` disables keep-alive.
    pub keep_alive: Option<Duration>,
    pub max_requests_per_conn: u32,
    pub shutdown_timeout: Duration,
}

impl From<&ServerConfig> for ServerOptions {
    fn from(c: &ServerConfig) -> Self {
        ServerOptions {
            workers: c.workers.max(1),
            queue: c.queue.max(1),
            keep_alive: (c.keep_alive_secs > 0).then(|| Duration::from_secs(c.keep_alive_secs)),
            max_requests_per_conn: c.max_requests_per_conn.max(1),
            shutdown_timeout: Duration::from_secs(c.shutdown_timeout_secs),
        }
    }
}

impl ServerOptions {
    /// A small pool for the ops listener (metrics, health probes).
    pub fn ops() -> Self {
        ServerOptions {
            workers: 4,
            queue: 64,
            keep_alive: Some(Duration::from_secs(5)),
            max_requests_per_conn: 100,
            shutdown_timeout: Duration::from_secs(1),
        }
    }
}

fn invalid(msg: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

/// Read one line of at most `MAX_LINE` bytes. `Ok(None)` on clean EOF.
fn read_line<R: BufRead>(r: &mut R, line: &mut String) -> std::io::Result<Option<()>> {
    line.clear();
    let n = r.by_ref().take(MAX_LINE).read_line(line)?;
    if n == 0 {
        return Ok(None);
    }
    if !line.ends_with('\n') && n as u64 >= MAX_LINE {
        return Err(invalid("line too long"));
    }
    Ok(Some(()))
}

/// Parse one HTTP/1.x request. `Ok(None)` means the client closed (or went
/// idle past the read timeout) before sending anything — not an error.
pub fn read_request<R: BufRead>(
    r: &mut R,
    peer: Option<IpAddr>,
) -> std::io::Result<Option<Request>> {
    use std::io::ErrorKind::{TimedOut, WouldBlock};
    let mut line = String::new();
    match read_line(r, &mut line) {
        Ok(None) => return Ok(None),
        Ok(Some(())) => {}
        Err(e) if line.is_empty() && matches!(e.kind(), WouldBlock | TimedOut) => return Ok(None),
        Err(e) => return Err(e),
    }
    let mut parts = line.trim_end().split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let http11 = parts.next() == Some("HTTP/1.1");
    if method.is_empty() {
        return Err(invalid("empty request line"));
    }

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    let mut connection: Option<String> = None;
    loop {
        if read_line(r, &mut line)?.is_none() || line == "\r\n" || line == "\n" {
            break;
        }
        if headers.len() >= MAX_HEADERS {
            return Err(invalid("too many headers"));
        }
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let k = k.trim().to_ascii_lowercase();
        let v = v.trim();
        match k.as_str() {
            "content-length" => {
                content_length = v.parse().map_err(|_| invalid("bad content-length"))?;
                if content_length > MAX_BODY {
                    return Err(invalid("body too large"));
                }
            }
            // Chunked bodies are not supported; rejecting them (rather than
            // guessing) keeps a persistent connection from desynchronizing.
            "transfer-encoding" => return Err(invalid("transfer-encoding not supported")),
            "connection" => connection = Some(v.to_ascii_lowercase()),
            _ => {}
        }
        headers.push((k, v.to_string()));
    }
    let mut body = vec![0u8; content_length];
    r.read_exact(&mut body)?;

    let has = |tok: &str| {
        connection
            .as_deref()
            .is_some_and(|c| c.split(',').any(|t| t.trim() == tok))
    };
    let mut req = Request::new(&method, &target, &String::from_utf8_lossy(&body));
    req.headers = headers;
    req.peer = peer;
    req.keep_alive = if http11 {
        !has("close")
    } else {
        has("keep-alive")
    };
    Ok(Some(req))
}

/// Serialize one response. Buffered: headers and body go out in one write.
pub fn write_response<W: Write>(
    w: &mut W,
    resp: &HttpResponse,
    keep_alive: bool,
) -> std::io::Result<()> {
    let (status, reason, ct, len, cc) = (
        resp.status,
        resp.reason(),
        resp.content_type,
        resp.body.len(),
        resp.cache_control,
    );
    write!(w, "HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {len}\r\nCache-Control: {cc}\r\n")?;
    if let Some(s) = resp.retry_after {
        write!(w, "Retry-After: {s}\r\n")?;
    }
    if resp.status == 401 {
        w.write_all(b"WWW-Authenticate: Bearer\r\n")?;
    }
    w.write_all(if keep_alive {
        b"Connection: keep-alive\r\n\r\n"
    } else {
        b"Connection: close\r\n\r\n"
    })?;
    w.write_all(resp.body.as_bytes())?;
    w.flush()
}

fn handle_connection(stream: TcpStream, svc: &Service, opts: &ServerOptions, stop: &Shutdown) {
    let peer = stream.peer_addr().ok().map(|a| a.ip());
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::with_capacity(16 * 1024, &stream);
    let mut served = 0u32;
    loop {
        let timeout = match (served, opts.keep_alive) {
            (0, _) | (_, None) => IO_TIMEOUT,
            (_, Some(ka)) => ka,
        };
        if stream.set_read_timeout(Some(timeout)).is_err() {
            return;
        }
        let req = match read_request(&mut reader, peer) {
            Ok(Some(req)) => req,
            Ok(None) => return,
            Err(_) => {
                let bad = HttpResponse::json(400, "{\"error\":\"bad request\"}".to_string());
                let _ = write_response(&mut writer, &bad, false);
                return;
            }
        };
        served += 1;
        let resp = svc.handle(&req);
        let keep = req.keep_alive
            && opts.keep_alive.is_some()
            && served < opts.max_requests_per_conn
            && !stop.is_stopping();
        if write_response(&mut writer, &resp, keep).is_err() || !keep {
            return;
        }
    }
}

fn shed(mut stream: TcpStream) {
    let _ = stream.set_write_timeout(Some(SHED_WRITE_TIMEOUT));
    let _ = stream.write_all(
        b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nContent-Type: application/json\r\nContent-Length: 27\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{\"error\":\"server overload\"}",
    );
}

/// The accept loop: hand each connection to the pool, shed when the queue is
/// full, stop once `stop` is set (checked after each accept, which
/// [`Shutdown::trigger`] wakes). Generic over the connection source so
/// accept errors can be exercised.
fn dispatch<I>(
    incoming: I,
    tx: &crossbeam_channel::Sender<TcpStream>,
    metrics: &crate::metrics::Metrics,
    stop: &Shutdown,
) where
    I: Iterator<Item = std::io::Result<TcpStream>>,
{
    for stream in incoming {
        if stop.is_stopping() {
            break;
        }
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                // EMFILE and friends: back off instead of spinning.
                tracing::warn!(error = %e, "accept failed");
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        match tx.try_send(stream) {
            Ok(()) => {}
            Err(TrySendError::Full(s)) => {
                metrics.inc_shed();
                shed(s);
            }
            Err(TrySendError::Disconnected(_)) => break,
        }
    }
}

/// Serve `listener` until `stop` is triggered, then drain. Returns once the
/// workers have finished (or `opts.shutdown_timeout` elapsed).
pub fn serve_until(
    listener: TcpListener,
    service: Arc<Service>,
    opts: ServerOptions,
    stop: Arc<Shutdown>,
) -> std::io::Result<()> {
    stop.register(listener.local_addr()?);
    let (tx, rx) = crossbeam_channel::bounded::<TcpStream>(opts.queue);
    // Each worker holds a clone of `done_tx`; the channel disconnects when
    // the last worker exits.
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(0);
    let opts = Arc::new(opts);
    for i in 0..opts.workers {
        let rx = rx.clone();
        let svc = service.clone();
        let opts = opts.clone();
        let stop = stop.clone();
        let done = done_tx.clone();
        std::thread::Builder::new()
            .name(format!("bidx-http-{i}"))
            .spawn(move || {
                for stream in rx.iter() {
                    handle_connection(stream, &svc, &opts, &stop);
                }
                drop(done);
            })?;
    }
    drop(done_tx);
    drop(rx);

    dispatch(listener.incoming(), &tx, &service.metrics, &stop);
    drop(tx);
    match done_rx.recv_timeout(opts.shutdown_timeout) {
        Err(RecvTimeoutError::Timeout) => {
            tracing::warn!("shutdown timeout reached with requests still in flight");
        }
        // Nothing sends on `done`; workers signal completion by disconnecting.
        Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
    }
    Ok(())
}

/// Serve forever on an already-bound listener with the service's
/// `[server]` settings.
pub fn serve_listener(listener: TcpListener, service: Arc<Service>) -> std::io::Result<()> {
    let opts = ServerOptions::from(&service.cfg.server);
    serve_until(listener, service, opts, Arc::new(Shutdown::new()))
}

/// Bind `addr` and serve until `stop` triggers.
pub fn serve(addr: &str, service: Arc<Service>, stop: Arc<Shutdown>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    tracing::info!("bidx serve listening on {addr}");
    let opts = ServerOptions::from(&service.cfg.server);
    serve_until(listener, service, opts, stop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::health::Health;
    use crate::metrics::Metrics;
    use crate::testutil::store as mock_store;
    use std::io::Cursor;

    fn service_with_cfg(routes: &[(&str, &str)], cfg: Config) -> Arc<Service> {
        let (s, _) = mock_store(routes, r#"{"data":[],"rows":0}"#);
        Arc::new(Service::new(
            Arc::new(s),
            Arc::new(cfg),
            Arc::new(Metrics::new()),
            Arc::new(Health::new()),
        ))
    }

    fn service() -> Arc<Service> {
        service_with_cfg(
            &[("max(height)", r#"{"data":[{"height":123}],"rows":1}"#)],
            Config::default(),
        )
    }

    fn parse(raw: &str) -> std::io::Result<Option<Request>> {
        read_request(&mut Cursor::new(raw.as_bytes().to_vec()), None)
    }

    fn opts(workers: usize, queue: usize) -> ServerOptions {
        ServerOptions {
            workers,
            queue,
            keep_alive: Some(Duration::from_secs(5)),
            max_requests_per_conn: 1000,
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    /// Start a server on an ephemeral port; returns its address and stopper.
    fn start(
        svc: Arc<Service>,
        o: ServerOptions,
    ) -> (
        std::net::SocketAddr,
        Arc<Shutdown>,
        std::thread::JoinHandle<std::io::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(Shutdown::new());
        let s2 = stop.clone();
        let h = std::thread::spawn(move || serve_until(listener, svc, o, s2));
        (addr, stop, h)
    }

    /// Read exactly one response (head + Content-Length body) off a stream.
    fn read_response(r: &mut BufReader<TcpStream>) -> (String, String) {
        let mut head = String::new();
        let mut len = 0usize;
        loop {
            let mut line = String::new();
            assert!(
                r.read_line(&mut line).unwrap() > 0,
                "connection closed mid-head"
            );
            if line == "\r\n" {
                break;
            }
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                len = v.trim().parse().unwrap();
            }
            head.push_str(&line);
        }
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).unwrap();
        (head, String::from_utf8(body).unwrap())
    }

    #[test]
    fn read_request_parses_method_target_headers_and_body() {
        let raw = "POST /tx/abc?limit=5 HTTP/1.1\r\nHost: h\r\nX-API-Key:  k1 \r\nno-colon-line\r\nContent-Length: 7\r\n\r\n{\"x\":1}";
        let req = parse(raw).unwrap().unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/tx/abc");
        assert_eq!(req.target, "/tx/abc?limit=5");
        assert_eq!(req.body, "{\"x\":1}");
        assert_eq!(req.query_param("limit"), Some("5"));
        assert_eq!(req.header("x-api-key"), Some("k1"));
        assert_eq!(req.header("host"), Some("h"));
        assert!(req.keep_alive, "HTTP/1.1 defaults to keep-alive");
        // Bare-LF line endings are tolerated.
        let req = parse("GET / HTTP/1.1\nHost: h\n\n").unwrap().unwrap();
        assert_eq!(req.header("host"), Some("h"));
    }

    #[test]
    fn read_request_keep_alive_rules_per_http_version() {
        let ka = |raw: &str| parse(raw).unwrap().unwrap().keep_alive;
        assert!(!ka("GET / HTTP/1.1\r\nConnection: close\r\n\r\n"));
        assert!(!ka("GET / HTTP/1.1\r\nConnection: Upgrade, Close\r\n\r\n"));
        assert!(!ka("GET / HTTP/1.0\r\n\r\n"));
        assert!(ka("GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n"));
        // Missing version → treated as 1.0.
        assert!(!ka("GET /\r\n\r\n"));
    }

    #[test]
    fn read_request_eof_and_idle_are_not_errors() {
        assert!(parse("").unwrap().is_none());
        struct Idle;
        impl Read for Idle {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "idle"))
            }
        }
        assert!(read_request(&mut BufReader::new(Idle), None)
            .unwrap()
            .is_none());
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "rst",
                ))
            }
        }
        assert!(read_request(&mut BufReader::new(Broken), None).is_err());
    }

    #[test]
    fn read_request_rejects_malformed_and_oversized_input() {
        let err = |raw: &str| parse(raw).unwrap_err().to_string();
        assert_eq!(err("\r\n\r\n"), "empty request line");
        assert_eq!(
            err("GET / HTTP/1.1\r\nContent-Length: nope\r\n\r\n"),
            "bad content-length"
        );
        assert_eq!(
            err(&format!(
                "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
                MAX_BODY + 1
            )),
            "body too large"
        );
        assert_eq!(
            err("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
            "transfer-encoding not supported"
        );
        let long = format!("GET /{} HTTP/1.1\r\n\r\n", "a".repeat(MAX_LINE as usize));
        assert_eq!(err(&long), "line too long");
        let many: String = (0..=MAX_HEADERS).map(|i| format!("X-{i}: v\r\n")).collect();
        assert_eq!(
            err(&format!("GET / HTTP/1.1\r\n{many}\r\n")),
            "too many headers"
        );
        // Declared body longer than what was sent.
        assert!(parse("POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\nshort").is_err());
        // EOF in the middle of headers ends the header block.
        let req = parse("GET / HTTP/1.1\r\nHost: h\r\n").unwrap().unwrap();
        assert_eq!(req.header("host"), Some("h"));
    }

    #[test]
    fn write_response_emits_cache_retry_auth_and_connection_headers() {
        let mut out = Vec::new();
        let mut r = HttpResponse::json(429, "{}".into());
        r.retry_after = Some(3);
        write_response(&mut out, &r, false).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("HTTP/1.1 429 Too Many Requests\r\n"), "{s}");
        assert!(s.contains("Retry-After: 3\r\n"));
        assert!(s.contains("Cache-Control: no-store\r\n"));
        assert!(s.contains("Connection: close\r\n\r\n{}"));

        let mut out = Vec::new();
        let mut r = HttpResponse::json(401, "{}".into());
        r.cache_control = crate::service::CacheControl::Public(60);
        write_response(&mut out, &r, true).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("WWW-Authenticate: Bearer\r\n"));
        assert!(s.contains("Cache-Control: public, max-age=60\r\n"));
        assert!(s.contains("Connection: keep-alive\r\n"));
        assert!(!s.contains("Retry-After"));
    }

    #[test]
    fn options_from_config_clamp_and_map_keep_alive() {
        let mut c = ServerConfig::default();
        let o = ServerOptions::from(&c);
        assert_eq!(
            (o.workers, o.queue, o.max_requests_per_conn),
            (256, 1024, 1000)
        );
        assert_eq!(o.keep_alive, Some(Duration::from_secs(5)));
        assert_eq!(o.shutdown_timeout, Duration::from_secs(25));
        c.keep_alive_secs = 0;
        c.workers = 0;
        c.queue = 0;
        c.max_requests_per_conn = 0;
        let o = ServerOptions::from(&c);
        assert_eq!(o.keep_alive, None);
        assert_eq!((o.workers, o.queue, o.max_requests_per_conn), (1, 1, 1));
        let ops = ServerOptions::ops();
        assert!(ops.workers < 16 && ops.keep_alive.is_some());
    }

    #[test]
    fn keep_alive_serves_many_requests_on_one_connection() {
        let (addr, stop, h) = start(service(), opts(2, 8));
        let c = TcpStream::connect(addr).unwrap();
        let mut w = c.try_clone().unwrap();
        let mut r = BufReader::new(c);
        for _ in 0..5 {
            w.write_all(b"GET /blocks/tip/height HTTP/1.1\r\nHost: x\r\n\r\n")
                .unwrap();
            let (head, body) = read_response(&mut r);
            assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
            assert!(head.contains("Connection: keep-alive"), "{head}");
            assert!(head.contains("Cache-Control: public, max-age=1"), "{head}");
            assert_eq!(body, "123");
        }
        // Client asks to close: server answers then closes.
        w.write_all(b"GET /healthz HTTP/1.1\r\nConnection: close\r\n\r\n")
            .unwrap();
        let (head, body) = read_response(&mut r);
        assert!(head.contains("Connection: close") && body == "ok");
        let mut rest = Vec::new();
        assert_eq!(r.read_to_end(&mut rest).unwrap(), 0);
        stop.trigger();
        h.join().unwrap().unwrap();
    }

    #[test]
    fn max_requests_per_conn_closes_the_connection() {
        let mut o = opts(1, 8);
        o.max_requests_per_conn = 2;
        let (addr, stop, h) = start(service(), o);
        let c = TcpStream::connect(addr).unwrap();
        let mut w = c.try_clone().unwrap();
        let mut r = BufReader::new(c);
        w.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        assert!(read_response(&mut r).0.contains("keep-alive"));
        w.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        assert!(read_response(&mut r).0.contains("Connection: close"));
        stop.trigger();
        h.join().unwrap().unwrap();
    }

    #[test]
    fn keep_alive_disabled_closes_after_each_response() {
        let mut o = opts(1, 8);
        o.keep_alive = None;
        let (addr, stop, h) = start(service(), o);
        let mut c = TcpStream::connect(addr).unwrap();
        c.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        let mut s = String::new();
        c.read_to_string(&mut s).unwrap();
        assert!(s.contains("Connection: close") && s.ends_with("ok"), "{s}");
        stop.trigger();
        h.join().unwrap().unwrap();
    }

    #[test]
    fn idle_keep_alive_connection_is_closed_after_the_timeout() {
        let mut o = opts(1, 8);
        o.keep_alive = Some(Duration::from_millis(100));
        let (addr, stop, h) = start(service(), o);
        let c = TcpStream::connect(addr).unwrap();
        let mut w = c.try_clone().unwrap();
        let mut r = BufReader::new(c);
        w.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        read_response(&mut r);
        // No second request: the server hangs up silently (no 400).
        let mut rest = Vec::new();
        assert_eq!(r.read_to_end(&mut rest).unwrap(), 0);
        stop.trigger();
        h.join().unwrap().unwrap();
    }

    #[test]
    fn malformed_request_gets_400_and_close() {
        let (addr, stop, h) = start(service(), opts(1, 8));
        let mut c = TcpStream::connect(addr).unwrap();
        c.write_all(b"\r\n\r\n").unwrap();
        let mut s = String::new();
        let _ = c.read_to_string(&mut s);
        assert!(s.starts_with("HTTP/1.1 400"), "{s}");
        // A connection closed without sending anything is simply dropped.
        drop(TcpStream::connect(addr).unwrap());
        stop.trigger();
        h.join().unwrap().unwrap();
    }

    #[test]
    fn full_queue_sheds_with_503_and_counts_it() {
        // One worker, queue of one: hold the worker with an open connection
        // and fill the queue, then the next connection is shed.
        let svc = service();
        let mut o = opts(1, 1);
        o.shutdown_timeout = Duration::from_millis(200);
        let (addr, stop, h) = start(svc.clone(), o);
        let busy = TcpStream::connect(addr).unwrap(); // occupies the worker
        std::thread::sleep(Duration::from_millis(100));
        let queued = TcpStream::connect(addr).unwrap(); // sits in the queue
        std::thread::sleep(Duration::from_millis(100));
        let mut shed_conn = TcpStream::connect(addr).unwrap();
        let mut s = String::new();
        shed_conn.read_to_string(&mut s).unwrap();
        assert!(s.starts_with("HTTP/1.1 503 Service Unavailable\r\n"), "{s}");
        assert!(s.contains("Retry-After: 1\r\n"));
        assert!(s.ends_with("{\"error\":\"server overload\"}"));
        assert!(svc.metrics.render().contains("bidx_shed_total 1\n"));
        stop.trigger();
        // The busy worker is still blocked reading `busy` (30 s timeout), so
        // the shutdown timeout path is taken and serve_until still returns.
        h.join().unwrap().unwrap();
        drop((busy, queued));
    }

    #[test]
    fn stop_closes_kept_alive_connections_after_the_current_request() {
        let (addr, stop, h) = start(service(), opts(2, 8));
        let c = TcpStream::connect(addr).unwrap();
        let mut w = c.try_clone().unwrap();
        let mut r = BufReader::new(c);
        w.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        assert!(read_response(&mut r).0.contains("keep-alive"));
        stop.trigger();
        // Next request on the existing connection is answered, then closed.
        w.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        let (head, _) = read_response(&mut r);
        assert!(head.contains("Connection: close"), "{head}");
        h.join().unwrap().unwrap();
        // Listener is gone (or no longer accepting into workers).
        let mut late = Vec::new();
        if let Ok(mut c) = TcpStream::connect(addr) {
            let _ = c.write_all(b"GET /healthz HTTP/1.1\r\n\r\n");
            let _ = c.read_to_end(&mut late);
        }
        assert!(late.is_empty());
    }

    #[test]
    fn serve_binds_serves_and_stops() {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let mut cfg = Config::default();
        cfg.server.workers = 2;
        cfg.server.shutdown_timeout_secs = 1;
        let svc = service_with_cfg(&[], cfg);
        let stop = Arc::new(Shutdown::new());
        let (s2, svc2) = (stop.clone(), svc.clone());
        let h = std::thread::spawn(move || serve(&addr.to_string(), svc2, s2));
        let mut last = String::new();
        for _ in 0..100 {
            if let Ok(mut c) = TcpStream::connect(addr) {
                c.write_all(b"GET /healthz HTTP/1.1\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let _ = c.read_to_string(&mut last);
                if last.contains("200 OK") {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(last.contains("200 OK"), "got: {last}");
        stop.trigger();
        h.join().unwrap().unwrap();
        // Bind failure surfaces as an error.
        assert!(serve("not-an-addr:99999", svc, Arc::new(Shutdown::new())).is_err());
    }

    #[test]
    fn dispatch_backs_off_on_accept_errors_and_stops_when_the_pool_is_gone() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let (accepted, _) = l.accept().unwrap();
        let (tx, rx) = crossbeam_channel::bounded::<TcpStream>(1);
        drop(rx);
        let metrics = Metrics::new();
        let stop = Shutdown::new();
        let items: Vec<std::io::Result<TcpStream>> = vec![
            Err(std::io::Error::other("too many open files")),
            Ok(accepted),
            Err(std::io::Error::other("never reached")),
        ];
        let mut it = items.into_iter();
        dispatch(&mut it, &tx, &metrics, &stop);
        // The error was skipped, the stream hit a disconnected pool → stop.
        assert_eq!(it.count(), 1, "loop ended at the disconnected send");
        assert!(metrics.render().contains("bidx_shed_total 0\n"));
    }

    #[test]
    fn a_zero_keep_alive_timeout_closes_after_the_first_response() {
        // set_read_timeout(Some(0)) is rejected by the OS layer; the worker
        // must close the connection instead of spinning.
        let mut o = opts(1, 8);
        o.keep_alive = Some(Duration::ZERO);
        let (addr, stop, h) = start(service(), o);
        let c = TcpStream::connect(addr).unwrap();
        let mut w = c.try_clone().unwrap();
        let mut r = BufReader::new(c);
        w.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        assert!(read_response(&mut r).0.contains("keep-alive"));
        let mut rest = Vec::new();
        assert_eq!(r.read_to_end(&mut rest).unwrap(), 0);
        stop.trigger();
        h.join().unwrap().unwrap();
    }

    #[test]
    fn serve_listener_uses_the_service_config() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let svc = service();
        std::thread::spawn(move || serve_listener(listener, svc));
        let mut c = TcpStream::connect(addr).unwrap();
        c.write_all(b"GET /healthz HTTP/1.0\r\n\r\n").unwrap();
        let mut s = String::new();
        c.read_to_string(&mut s).unwrap();
        assert!(
            s.starts_with("HTTP/1.1 200 OK") && s.contains("Connection: close"),
            "{s}"
        );
    }

    #[test]
    fn peer_ip_reaches_the_rate_limiter() {
        let mut cfg = Config::default();
        cfg.rate_limit.per_ip_rps = 0.001;
        cfg.rate_limit.per_ip_burst = 1.0;
        let svc = service_with_cfg(
            &[("max(height)", r#"{"data":[{"height":1}],"rows":1}"#)],
            cfg,
        );
        let (addr, stop, h) = start(svc, opts(1, 8));
        let c = TcpStream::connect(addr).unwrap();
        let mut w = c.try_clone().unwrap();
        let mut r = BufReader::new(c);
        w.write_all(b"GET /blocks/tip/height HTTP/1.1\r\n\r\n")
            .unwrap();
        assert!(read_response(&mut r).0.starts_with("HTTP/1.1 200"));
        w.write_all(b"GET /blocks/tip/height HTTP/1.1\r\n\r\n")
            .unwrap();
        let (head, _) = read_response(&mut r);
        assert!(head.starts_with("HTTP/1.1 429"), "{head}");
        assert!(head.contains("Retry-After: "), "{head}");
        stop.trigger();
        h.join().unwrap().unwrap();
    }
}
