//! Service configuration: TOML file plus `BIDX_*` environment overrides.
//!
//! Precedence is defaults < file < environment, so an operator can ship a
//! baseline `bidx.toml` and override per-deployment secrets/hosts via env
//! (which is what container orchestrators expect). Env keys use a `__`
//! separator for nesting: `BIDX_CLICKHOUSE__HOST=clickhouse`.

use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Top-level service configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub network: NetworkName,
    /// Bind address for the REST + Electrum JSON-RPC surface.
    pub api_addr: String,
    /// Bind address for `/metrics`, `/healthz`, `/readyz`.
    pub metrics_addr: String,
    /// Enable the Esplora-compatible REST routes.
    pub esplora_enabled: bool,
    /// Enable the Electrum JSON-RPC surface.
    pub electrum_enabled: bool,
    /// ClickHouse connection.
    pub clickhouse: ClickHouseConfig,
    /// Optional Bitcoin Core RPC, used for tip comparison in readiness and
    /// `bidx verify`.
    pub node: Option<NodeConfig>,
    pub log: String,
    pub server: ServerConfig,
    pub cache: CacheConfig,
    pub auth: AuthConfig,
    pub rate_limit: RateLimitConfig,
}

/// HTTP server sizing, keep-alive, and shutdown behavior.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// Fixed worker threads serving connections.
    pub workers: usize,
    /// Accepted connections waiting for a worker; beyond this new
    /// connections get an immediate `503` (load shedding).
    pub queue: usize,
    /// Idle keep-alive timeout; 0 disables keep-alive.
    pub keep_alive_secs: u64,
    /// Requests served on one connection before it is closed, so load
    /// rebalances across replicas behind an L4 balancer.
    pub max_requests_per_conn: u32,
    /// After SIGTERM: report not-ready for this long (so the load balancer
    /// deregisters us) before refusing new connections.
    pub drain_delay_secs: u64,
    /// After the drain delay: wait up to this long for in-flight requests.
    pub shutdown_timeout_secs: u64,
    /// Use the rightmost `X-Forwarded-For` entry as the client IP (only
    /// safe behind a proxy that sets it).
    pub trust_forwarded_for: bool,
}

/// In-process response cache.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_entries: usize,
    /// TTL for tip-dependent responses (`/blocks/tip/*`, `/fee-estimates`).
    pub tip_ttl_ms: u64,
    /// TTL for address/scripthash responses (change with every block).
    pub address_ttl_ms: u64,
    /// TTL (and `Cache-Control: max-age`) for responses about data buried
    /// at least `confirmations` deep.
    pub immutable_ttl_secs: u64,
    pub confirmations: u32,
}

/// Optional API-key authentication with per-key quotas.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AuthConfig {
    /// Reject requests without a valid key. When false, keys are still
    /// recognized (for per-tenant quotas/metrics) but anonymous access is
    /// allowed and governed by the per-IP limit.
    pub required: bool,
    pub keys: Vec<ApiKey>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApiKey {
    /// Tenant name, used as the metrics label.
    pub name: String,
    /// SHA-256 of the bearer token; plaintext tokens are never kept.
    pub token_sha256: [u8; 32],
    /// Sustained requests/second; 0 = unlimited.
    pub rate_per_sec: f64,
    pub burst: f64,
}

/// Per-client-IP token bucket for anonymous traffic.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RateLimitConfig {
    /// 0 = disabled.
    pub per_ip_rps: f64,
    pub per_ip_burst: f64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            workers: 256,
            queue: 1024,
            keep_alive_secs: 5,
            max_requests_per_conn: 1000,
            drain_delay_secs: 5,
            shutdown_timeout_secs: 25,
            trust_forwarded_for: false,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            enabled: true,
            max_entries: 100_000,
            tip_ttl_ms: 1_000,
            address_ttl_ms: 2_000,
            immutable_ttl_secs: 300,
            confirmations: 6,
        }
    }
}

impl ApiKey {
    pub fn hash_token(token: &str) -> [u8; 32] {
        use sha2::Digest;
        sha2::Sha256::digest(token.as_bytes()).into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkName {
    Mainnet,
    Testnet3,
    Testnet4,
    Signet,
    Regtest,
}

impl NetworkName {
    pub fn as_str(&self) -> &'static str {
        match self {
            NetworkName::Mainnet => "mainnet",
            NetworkName::Testnet3 => "testnet3",
            NetworkName::Testnet4 => "testnet4",
            NetworkName::Signet => "signet",
            NetworkName::Regtest => "regtest",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "mainnet" | "main" | "bitcoin" => Some(NetworkName::Mainnet),
            "testnet3" | "testnet" => Some(NetworkName::Testnet3),
            "testnet4" => Some(NetworkName::Testnet4),
            "signet" => Some(NetworkName::Signet),
            "regtest" => Some(NetworkName::Regtest),
            _ => None,
        }
    }

    /// Map onto the core `Network` used by the address codec.
    pub fn to_core(self) -> bidx_core::Network {
        match self {
            NetworkName::Mainnet => bidx_core::Network::Mainnet,
            NetworkName::Testnet3 => bidx_core::Network::Testnet3,
            NetworkName::Testnet4 => bidx_core::Network::Testnet4,
            NetworkName::Signet => bidx_core::Network::Signet,
            NetworkName::Regtest => bidx_core::Network::Regtest,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseConfig {
    /// One host, or a comma-separated list of replicas (`host` or
    /// `host:port`) the serve layer rotates across with failover.
    pub host: String,
    /// `http` or `https` for the serve layer's HTTP interface.
    pub scheme: String,
    /// Native protocol port, used by `bidx init-db` / `load` via
    /// `clickhouse-client` (9000 by default).
    pub port: u16,
    /// HTTP interface port, used by the serving layer (8123 by default).
    /// ClickHouse exposes HTTP on a different port than the native protocol;
    /// the serve layer speaks HTTP, so it must not reuse `port`.
    pub http_port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    /// Query timeout in seconds.
    pub timeout_secs: u64,
}

impl ClickHouseConfig {
    /// The configured host list, trimmed, empty entries dropped.
    pub fn hosts(&self) -> Vec<&str> {
        self.host
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    pub rpc_url: String,
    pub cookie: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            network: NetworkName::Mainnet,
            api_addr: "0.0.0.0:8432".into(),
            metrics_addr: "0.0.0.0:9101".into(),
            esplora_enabled: true,
            electrum_enabled: true,
            clickhouse: ClickHouseConfig {
                host: "localhost".into(),
                scheme: "http".into(),
                port: 9000,
                http_port: 8123,
                user: "default".into(),
                password: None,
                database: "bitcoin".into(),
                timeout_secs: 30,
            },
            node: None,
            log: "info".into(),
            server: ServerConfig::default(),
            cache: CacheConfig::default(),
            auth: AuthConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from an optional TOML file, then apply `BIDX_*`
    /// environment overrides, then validate.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut cfg = match path {
            Some(p) => Self::from_toml_file(p)?,
            None => Config::default(),
        };
        cfg.apply_env(|k| std::env::var(k).ok())?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn from_toml_file(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_toml_str(&text).map_err(|source| ConfigError::Parse {
            path: path.display().to_string(),
            source,
        })
    }

    pub fn from_toml_str(text: &str) -> Result<Self, toml::de::Error> {
        let file: FileConfig = toml::from_str(text)?;
        let mut cfg = Config::default();
        file.apply_to(&mut cfg)?;
        Ok(cfg)
    }

    /// Apply `BIDX_*` env overrides. `get` is injected so tests can supply a
    /// fake environment without mutating the process.
    pub fn apply_env<F>(&mut self, get: F) -> Result<(), ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(v) = get("BIDX_NETWORK") {
            self.network = NetworkName::parse(&v)
                .ok_or_else(|| ConfigError::Invalid(format!("unknown network '{v}'")))?;
        }
        if let Some(v) = get("BIDX_API_ADDR") {
            self.api_addr = v;
        }
        if let Some(v) = get("BIDX_METRICS_ADDR") {
            self.metrics_addr = v;
        }
        if let Some(v) = get("BIDX_ESPLORA_ENABLED") {
            self.esplora_enabled = parse_bool(&v)?;
        }
        if let Some(v) = get("BIDX_ELECTRUM_ENABLED") {
            self.electrum_enabled = parse_bool(&v)?;
        }
        if let Some(v) = get("BIDX_RUST_LOG") {
            self.log = v;
        }
        // Nested ClickHouse settings use the `__` separator.
        if let Some(v) = get("BIDX_CLICKHOUSE__HOST") {
            self.clickhouse.host = v;
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__PORT") {
            self.clickhouse.port = v
                .parse()
                .map_err(|_| ConfigError::Invalid(format!("bad port '{v}'")))?;
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__HTTP_PORT") {
            self.clickhouse.http_port = v
                .parse()
                .map_err(|_| ConfigError::Invalid(format!("bad http_port '{v}'")))?;
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__USER") {
            self.clickhouse.user = v;
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__PASSWORD") {
            self.clickhouse.password = Some(v);
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__DATABASE") {
            self.clickhouse.database = v;
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__TIMEOUT_SECS") {
            self.clickhouse.timeout_secs = v
                .parse()
                .map_err(|_| ConfigError::Invalid(format!("bad timeout '{v}'")))?;
        }
        if let Some(v) = get("BIDX_NODE__RPC_URL") {
            let node = self.node.get_or_insert_with(|| NodeConfig {
                rpc_url: String::new(),
                cookie: None,
                user: None,
                password: None,
            });
            node.rpc_url = v;
        }
        if let Some(v) = get("BIDX_NODE__COOKIE") {
            if let Some(n) = self.node.as_mut() {
                n.cookie = Some(v);
            }
        }
        if let Some(v) = get("BIDX_CLICKHOUSE__SCHEME") {
            self.clickhouse.scheme = v.to_ascii_lowercase();
        }
        let s = &mut self.server;
        env_num(&get, "BIDX_SERVER__WORKERS", &mut s.workers)?;
        env_num(&get, "BIDX_SERVER__QUEUE", &mut s.queue)?;
        env_num(&get, "BIDX_SERVER__KEEP_ALIVE_SECS", &mut s.keep_alive_secs)?;
        env_num(
            &get,
            "BIDX_SERVER__MAX_REQUESTS_PER_CONN",
            &mut s.max_requests_per_conn,
        )?;
        env_num(
            &get,
            "BIDX_SERVER__DRAIN_DELAY_SECS",
            &mut s.drain_delay_secs,
        )?;
        env_num(
            &get,
            "BIDX_SERVER__SHUTDOWN_TIMEOUT_SECS",
            &mut s.shutdown_timeout_secs,
        )?;
        env_bool(
            &get,
            "BIDX_SERVER__TRUST_FORWARDED_FOR",
            &mut s.trust_forwarded_for,
        )?;
        let c = &mut self.cache;
        env_bool(&get, "BIDX_CACHE__ENABLED", &mut c.enabled)?;
        env_num(&get, "BIDX_CACHE__MAX_ENTRIES", &mut c.max_entries)?;
        env_num(&get, "BIDX_CACHE__TIP_TTL_MS", &mut c.tip_ttl_ms)?;
        env_num(&get, "BIDX_CACHE__ADDRESS_TTL_MS", &mut c.address_ttl_ms)?;
        env_num(
            &get,
            "BIDX_CACHE__IMMUTABLE_TTL_SECS",
            &mut c.immutable_ttl_secs,
        )?;
        env_num(&get, "BIDX_CACHE__CONFIRMATIONS", &mut c.confirmations)?;
        env_bool(&get, "BIDX_AUTH__REQUIRED", &mut self.auth.required)?;
        if let Some(v) = get("BIDX_AUTH__KEYS") {
            self.auth.keys = parse_env_keys(&v)?;
        }
        env_num(
            &get,
            "BIDX_RATE_LIMIT__PER_IP_RPS",
            &mut self.rate_limit.per_ip_rps,
        )?;
        env_num(
            &get,
            "BIDX_RATE_LIMIT__PER_IP_BURST",
            &mut self.rate_limit.per_ip_burst,
        )?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.api_addr == self.metrics_addr {
            return Err(ConfigError::Invalid(
                "api_addr and metrics_addr must differ".into(),
            ));
        }
        if self.clickhouse.database.is_empty() {
            return Err(ConfigError::Invalid("clickhouse.database is empty".into()));
        }
        if self.clickhouse.timeout_secs == 0 {
            return Err(ConfigError::Invalid(
                "clickhouse.timeout_secs must be > 0".into(),
            ));
        }
        if self.clickhouse.http_port == 0 {
            return Err(ConfigError::Invalid(
                "clickhouse.http_port must be > 0".into(),
            ));
        }
        if !self.esplora_enabled && !self.electrum_enabled {
            return Err(ConfigError::Invalid(
                "at least one of esplora_enabled / electrum_enabled must be true".into(),
            ));
        }
        let invalid = |m: &str| Err(ConfigError::Invalid(m.to_string()));
        if self.clickhouse.scheme != "http" && self.clickhouse.scheme != "https" {
            return invalid("clickhouse.scheme must be 'http' or 'https'");
        }
        if self.clickhouse.hosts().is_empty() {
            return invalid("clickhouse.host is empty");
        }
        if self.server.workers == 0 || self.server.queue == 0 {
            return invalid("server.workers and server.queue must be > 0");
        }
        if self.server.max_requests_per_conn == 0 {
            return invalid("server.max_requests_per_conn must be > 0");
        }
        if self.cache.enabled && self.cache.max_entries == 0 {
            return invalid("cache.max_entries must be > 0 when the cache is enabled");
        }
        if self.auth.required && self.auth.keys.is_empty() {
            return invalid("auth.required is set but no auth.keys are configured");
        }
        let mut names = rustc_hash::FxHashSet::default();
        let mut tokens = rustc_hash::FxHashSet::default();
        for k in &self.auth.keys {
            if k.name.is_empty() || !names.insert(k.name.as_str()) {
                return invalid("auth.keys names must be non-empty and unique");
            }
            if !tokens.insert(k.token_sha256) {
                return invalid("auth.keys tokens must be unique");
            }
            check_bucket("auth.keys", k.rate_per_sec, k.burst)?;
        }
        check_bucket(
            "rate_limit",
            self.rate_limit.per_ip_rps,
            self.rate_limit.per_ip_burst,
        )?;
        Ok(())
    }
}

fn check_bucket(what: &str, rate: f64, burst: f64) -> Result<(), ConfigError> {
    if !rate.is_finite() || rate < 0.0 || (rate > 0.0 && !(burst.is_finite() && burst >= 1.0)) {
        return Err(ConfigError::Invalid(format!(
            "{what}: rate must be >= 0 and burst >= 1 when rate is set"
        )));
    }
    Ok(())
}

fn parse_bool(v: &str) -> Result<bool, ConfigError> {
    match v.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Invalid(format!("bad boolean '{v}'"))),
    }
}

fn env_num<F, T>(get: &F, key: &str, slot: &mut T) -> Result<(), ConfigError>
where
    F: Fn(&str) -> Option<String>,
    T: std::str::FromStr,
{
    if let Some(v) = get(key) {
        *slot = v
            .trim()
            .parse()
            .map_err(|_| ConfigError::Invalid(format!("bad value for {key}: '{v}'")))?;
    }
    Ok(())
}

fn env_bool<F>(get: &F, key: &str, slot: &mut bool) -> Result<(), ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(v) = get(key) {
        *slot = parse_bool(&v)?;
    }
    Ok(())
}

/// `BIDX_AUTH__KEYS=name:token[:rps[:burst]],...`. A missing burst
/// defaults to `max(rps, 1)`.
fn parse_env_keys(v: &str) -> Result<Vec<ApiKey>, ConfigError> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let mut parts = entry.split(':');
            let bad = || {
                ConfigError::Invalid("BIDX_AUTH__KEYS entries are name:token[:rps[:burst]]".into())
            };
            let name = parts.next().filter(|s| !s.is_empty()).ok_or_else(bad)?;
            let token = parts.next().filter(|s| !s.is_empty()).ok_or_else(bad)?;
            let rate: f64 = match parts.next() {
                Some(r) => r.parse().map_err(|_| bad())?,
                None => 0.0,
            };
            let burst: f64 = match parts.next() {
                Some(b) => b.parse().map_err(|_| bad())?,
                None => rate.max(1.0),
            };
            if parts.next().is_some() {
                return Err(bad());
            }
            Ok(ApiKey {
                name: name.to_string(),
                token_sha256: ApiKey::hash_token(token),
                rate_per_sec: rate,
                burst,
            })
        })
        .collect()
}

fn decode_hex32(s: &str) -> Option<[u8; 32]> {
    let s = s.trim().as_bytes();
    if s.len() != 64 {
        return None;
    }
    let nib = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = [0u8; 32];
    for (i, pair) in s.chunks_exact(2).enumerate() {
        out[i] = (nib(pair[0])? << 4) | nib(pair[1])?;
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// TOML shape — mirrors Config but with every field optional.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    network: Option<String>,
    api_addr: Option<String>,
    metrics_addr: Option<String>,
    esplora_enabled: Option<bool>,
    electrum_enabled: Option<bool>,
    log: Option<String>,
    clickhouse: Option<FileClickHouse>,
    node: Option<FileNode>,
    server: Option<FileServer>,
    cache: Option<FileCache>,
    auth: Option<FileAuth>,
    rate_limit: Option<FileRateLimit>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileServer {
    workers: Option<usize>,
    queue: Option<usize>,
    keep_alive_secs: Option<u64>,
    max_requests_per_conn: Option<u32>,
    drain_delay_secs: Option<u64>,
    shutdown_timeout_secs: Option<u64>,
    trust_forwarded_for: Option<bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileCache {
    enabled: Option<bool>,
    max_entries: Option<usize>,
    tip_ttl_ms: Option<u64>,
    address_ttl_ms: Option<u64>,
    immutable_ttl_secs: Option<u64>,
    confirmations: Option<u32>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAuth {
    required: Option<bool>,
    #[serde(default)]
    keys: Vec<FileApiKey>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileApiKey {
    name: String,
    token: Option<String>,
    token_sha256: Option<String>,
    #[serde(default)]
    rate_per_sec: f64,
    burst: Option<f64>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRateLimit {
    per_ip_rps: Option<f64>,
    per_ip_burst: Option<f64>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileClickHouse {
    host: Option<String>,
    scheme: Option<String>,
    port: Option<u16>,
    http_port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileNode {
    rpc_url: Option<String>,
    cookie: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

fn set<T>(slot: &mut T, v: Option<T>) {
    if let Some(v) = v {
        *slot = v;
    }
}

impl FileApiKey {
    fn into_key(self) -> Result<ApiKey, toml::de::Error> {
        use serde::de::Error as _;
        let token_sha256 = match (self.token, self.token_sha256) {
            (Some(t), None) => ApiKey::hash_token(&t),
            (None, Some(h)) => decode_hex32(&h).ok_or_else(|| {
                toml::de::Error::custom(format!(
                    "auth key '{}': token_sha256 must be 64 hex chars",
                    self.name
                ))
            })?,
            _ => {
                return Err(toml::de::Error::custom(format!(
                    "auth key '{}': set exactly one of token / token_sha256",
                    self.name
                )))
            }
        };
        Ok(ApiKey {
            name: self.name,
            token_sha256,
            rate_per_sec: self.rate_per_sec,
            burst: self.burst.unwrap_or(self.rate_per_sec.max(1.0)),
        })
    }
}

impl FileConfig {
    fn apply_to(self, cfg: &mut Config) -> Result<(), toml::de::Error> {
        if let Some(s) = self.server {
            let d = &mut cfg.server;
            set(&mut d.workers, s.workers);
            set(&mut d.queue, s.queue);
            set(&mut d.keep_alive_secs, s.keep_alive_secs);
            set(&mut d.max_requests_per_conn, s.max_requests_per_conn);
            set(&mut d.drain_delay_secs, s.drain_delay_secs);
            set(&mut d.shutdown_timeout_secs, s.shutdown_timeout_secs);
            set(&mut d.trust_forwarded_for, s.trust_forwarded_for);
        }
        if let Some(c) = self.cache {
            let d = &mut cfg.cache;
            set(&mut d.enabled, c.enabled);
            set(&mut d.max_entries, c.max_entries);
            set(&mut d.tip_ttl_ms, c.tip_ttl_ms);
            set(&mut d.address_ttl_ms, c.address_ttl_ms);
            set(&mut d.immutable_ttl_secs, c.immutable_ttl_secs);
            set(&mut d.confirmations, c.confirmations);
        }
        if let Some(a) = self.auth {
            set(&mut cfg.auth.required, a.required);
            cfg.auth.keys = a
                .keys
                .into_iter()
                .map(FileApiKey::into_key)
                .collect::<Result<_, _>>()?;
        }
        if let Some(r) = self.rate_limit {
            set(&mut cfg.rate_limit.per_ip_rps, r.per_ip_rps);
            set(&mut cfg.rate_limit.per_ip_burst, r.per_ip_burst);
        }
        if let Some(n) = self.network.as_deref().and_then(NetworkName::parse) {
            cfg.network = n;
        }
        if let Some(v) = self.api_addr {
            cfg.api_addr = v;
        }
        if let Some(v) = self.metrics_addr {
            cfg.metrics_addr = v;
        }
        if let Some(v) = self.esplora_enabled {
            cfg.esplora_enabled = v;
        }
        if let Some(v) = self.electrum_enabled {
            cfg.electrum_enabled = v;
        }
        if let Some(v) = self.log {
            cfg.log = v;
        }
        if let Some(ch) = self.clickhouse {
            if let Some(v) = ch.host {
                cfg.clickhouse.host = v;
            }
            if let Some(v) = ch.scheme {
                cfg.clickhouse.scheme = v.to_ascii_lowercase();
            }
            if let Some(v) = ch.port {
                cfg.clickhouse.port = v;
            }
            if let Some(v) = ch.http_port {
                cfg.clickhouse.http_port = v;
            }
            if let Some(v) = ch.user {
                cfg.clickhouse.user = v;
            }
            if let Some(v) = ch.password {
                cfg.clickhouse.password = Some(v);
            }
            if let Some(v) = ch.database {
                cfg.clickhouse.database = v;
            }
            if let Some(v) = ch.timeout_secs {
                cfg.clickhouse.timeout_secs = v;
            }
        }
        if let Some(n) = self.node {
            cfg.node = Some(NodeConfig {
                rpc_url: n.rpc_url.unwrap_or_default(),
                cookie: n.cookie,
                user: n.user,
                password: n.password,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_from<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| (*v).to_string())
        }
    }

    #[test]
    fn defaults_are_valid() {
        let c = Config::default();
        c.validate().unwrap();
        assert_eq!(c.network, NetworkName::Mainnet);
        assert_eq!(c.clickhouse.port, 9000);
        assert_eq!(c.clickhouse.http_port, 8123);
        assert!(c.esplora_enabled && c.electrum_enabled);
    }

    #[test]
    fn toml_file_overrides_defaults() {
        let text = r#"
            network = "testnet4"
            api_addr = "127.0.0.1:9000"
            log = "debug"

            [clickhouse]
            host = "ch.internal"
            port = 9001
            database = "btc"
            timeout_secs = 5

            [node]
            rpc_url = "http://127.0.0.1:48332"
        "#;
        let c = Config::from_toml_str(text).unwrap();
        assert_eq!(c.network, NetworkName::Testnet4);
        assert_eq!(c.api_addr, "127.0.0.1:9000");
        assert_eq!(c.log, "debug");
        assert_eq!(c.clickhouse.host, "ch.internal");
        assert_eq!(c.clickhouse.port, 9001);
        assert_eq!(c.clickhouse.database, "btc");
        assert_eq!(c.clickhouse.timeout_secs, 5);
        assert_eq!(c.node.as_ref().unwrap().rpc_url, "http://127.0.0.1:48332");
        c.validate().unwrap();
    }

    #[test]
    fn env_overrides_file() {
        let mut c =
            Config::from_toml_str("network = \"testnet3\"\n[clickhouse]\nhost = \"file-host\"")
                .unwrap();
        let env = env_from(&[
            ("BIDX_NETWORK", "signet"),
            ("BIDX_CLICKHOUSE__HOST", "env-host"),
            ("BIDX_CLICKHOUSE__PORT", "9440"),
            ("BIDX_CLICKHOUSE__PASSWORD", "sekret"),
            ("BIDX_API_ADDR", "0.0.0.0:1234"),
            ("BIDX_ELECTRUM_ENABLED", "false"),
            ("BIDX_NODE__RPC_URL", "http://node:8332"),
        ]);
        c.apply_env(env).unwrap();
        assert_eq!(c.network, NetworkName::Signet);
        assert_eq!(c.clickhouse.host, "env-host");
        assert_eq!(c.clickhouse.port, 9440);
        assert_eq!(c.clickhouse.password.as_deref(), Some("sekret"));
        assert_eq!(c.api_addr, "0.0.0.0:1234");
        assert!(!c.electrum_enabled);
        assert_eq!(c.node.as_ref().unwrap().rpc_url, "http://node:8332");
    }

    #[test]
    fn env_covers_every_remaining_key() {
        let mut c = Config::default();
        c.apply_env(env_from(&[
            ("BIDX_METRICS_ADDR", "0.0.0.0:9999"),
            ("BIDX_ESPLORA_ENABLED", "off"),
            ("BIDX_RUST_LOG", "bidx=debug"),
            ("BIDX_CLICKHOUSE__USER", "alice"),
            ("BIDX_CLICKHOUSE__DATABASE", "chain"),
            ("BIDX_CLICKHOUSE__TIMEOUT_SECS", "7"),
        ]))
        .unwrap();
        assert_eq!(c.metrics_addr, "0.0.0.0:9999");
        assert!(!c.esplora_enabled);
        assert_eq!(c.log, "bidx=debug");
        assert_eq!(c.clickhouse.user, "alice");
        assert_eq!(c.clickhouse.database, "chain");
        assert_eq!(c.clickhouse.timeout_secs, 7);
    }

    #[test]
    fn env_node_cookie_requires_existing_node() {
        // BIDX_NODE__COOKIE alone (no RPC URL) is ignored, not a panic.
        let mut c = Config::default();
        c.apply_env(env_from(&[("BIDX_NODE__COOKIE", "/x/.cookie")]))
            .unwrap();
        assert!(c.node.is_none());
        // With an RPC URL the cookie is stored.
        let mut c = Config::default();
        c.apply_env(env_from(&[
            ("BIDX_NODE__RPC_URL", "http://n:8332"),
            ("BIDX_NODE__COOKIE", "/x/.cookie"),
        ]))
        .unwrap();
        assert_eq!(
            c.node.as_ref().unwrap().cookie.as_deref(),
            Some("/x/.cookie")
        );
    }

    #[test]
    fn env_rejects_bad_timeout() {
        let mut c = Config::default();
        assert!(c
            .apply_env(env_from(&[("BIDX_CLICKHOUSE__TIMEOUT_SECS", "soon")]))
            .is_err());
    }

    #[test]
    fn toml_covers_all_sections_including_node_credentials() {
        let text = r#"
            metrics_addr = "0.0.0.0:9200"
            esplora_enabled = false
            electrum_enabled = true
            [clickhouse]
            user = "bob"
            password = "pw"
            [node]
            rpc_url = "http://n:8332"
            cookie = "/c"
            user = "u"
            password = "p"
        "#;
        let c = Config::from_toml_str(text).unwrap();
        assert_eq!(c.metrics_addr, "0.0.0.0:9200");
        assert!(!c.esplora_enabled);
        assert!(c.electrum_enabled);
        assert_eq!(c.clickhouse.user, "bob");
        assert_eq!(c.clickhouse.password.as_deref(), Some("pw"));
        let node = c.node.unwrap();
        assert_eq!(node.rpc_url, "http://n:8332");
        assert_eq!(node.cookie.as_deref(), Some("/c"));
        assert_eq!(node.user.as_deref(), Some("u"));
        assert_eq!(node.password.as_deref(), Some("p"));
    }

    #[test]
    fn env_rejects_bad_values() {
        let mut c = Config::default();
        let e = c
            .apply_env(env_from(&[("BIDX_NETWORK", "moonnet")]))
            .unwrap_err();
        assert!(matches!(e, ConfigError::Invalid(_)));
        let mut c = Config::default();
        assert!(c
            .apply_env(env_from(&[("BIDX_CLICKHOUSE__PORT", "not-a-port")]))
            .is_err());
        let mut c = Config::default();
        assert!(c
            .apply_env(env_from(&[("BIDX_ESPLORA_ENABLED", "maybe")]))
            .is_err());
    }

    #[test]
    fn validate_rejects_conflicting_and_empty() {
        let mut c = Config::default();
        c.metrics_addr = c.api_addr.clone();
        assert!(matches!(c.validate(), Err(ConfigError::Invalid(_))));

        let mut c = Config::default();
        c.clickhouse.database = String::new();
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.clickhouse.timeout_secs = 0;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.clickhouse.http_port = 0;
        assert!(c.validate().is_err());

        let mut c = Config::default();
        c.esplora_enabled = false;
        c.electrum_enabled = false;
        assert!(c.validate().is_err());
    }

    #[test]
    fn http_port_comes_from_file_and_env() {
        // File.
        let c = Config::from_toml_str("[clickhouse]\nhttp_port = 8124").unwrap();
        assert_eq!(c.clickhouse.http_port, 8124);
        // Env overrides file.
        let mut c = Config::from_toml_str("[clickhouse]\nhttp_port = 8124").unwrap();
        c.apply_env(env_from(&[("BIDX_CLICKHOUSE__HTTP_PORT", "8125")]))
            .unwrap();
        assert_eq!(c.clickhouse.http_port, 8125);
        // Bad value rejected.
        let mut c = Config::default();
        assert!(c
            .apply_env(env_from(&[("BIDX_CLICKHOUSE__HTTP_PORT", "http")]))
            .is_err());
    }

    #[test]
    fn network_names_parse_and_map() {
        assert_eq!(NetworkName::parse("MAINNET"), Some(NetworkName::Mainnet));
        assert_eq!(NetworkName::parse("bitcoin"), Some(NetworkName::Mainnet));
        assert_eq!(NetworkName::parse("testnet"), Some(NetworkName::Testnet3));
        assert_eq!(NetworkName::parse("nope"), None);
        assert_eq!(NetworkName::Mainnet.as_str(), "mainnet");
        assert_eq!(NetworkName::Testnet3.as_str(), "testnet3");
        assert_eq!(NetworkName::Testnet4.as_str(), "testnet4");
        assert_eq!(NetworkName::Signet.as_str(), "signet");
        assert_eq!(NetworkName::Regtest.as_str(), "regtest");
        assert_eq!(NetworkName::Signet.to_core(), bidx_core::Network::Signet);
        assert_eq!(
            NetworkName::Testnet4.to_core(),
            bidx_core::Network::Testnet4
        );
        assert_eq!(
            NetworkName::Testnet3.to_core(),
            bidx_core::Network::Testnet3
        );
        assert_eq!(NetworkName::Mainnet.to_core(), bidx_core::Network::Mainnet);
        assert_eq!(NetworkName::Regtest.to_core(), bidx_core::Network::Regtest);
    }

    #[test]
    fn load_from_missing_file_errors_and_env_only_succeeds() {
        let missing = Path::new("/definitely/not/here/bidx.toml");
        match Config::load(Some(missing)) {
            Err(ConfigError::Io { path, .. }) => assert!(path.contains("bidx.toml")),
            other => panic!("expected Io error, got {other:?}"),
        }
        // No file path → defaults + env, no error.
        let c = Config::load(None).unwrap();
        assert_eq!(c.network, NetworkName::Mainnet);
    }

    #[test]
    fn load_reads_real_file_and_reports_parse_errors() {
        let dir = std::env::temp_dir().join(format!("bidx-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("good.toml");
        std::fs::write(&good, "network = \"regtest\"\n").unwrap();
        let c = Config::load(Some(&good)).unwrap();
        assert_eq!(c.network, NetworkName::Regtest);

        let bad = dir.join("bad.toml");
        std::fs::write(&bad, "network = = =").unwrap();
        match Config::load(Some(&bad)) {
            Err(ConfigError::Parse { path, .. }) => assert!(path.contains("bad.toml")),
            other => panic!("expected Parse error, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn toml_parse_error_is_reported_with_path() {
        match Config::load(Some(Path::new("/definitely/not/here/bidx.toml"))) {
            Err(ConfigError::Io { .. }) => {}
            other => panic!("expected Io error, got {other:?}"),
        }
        // from_toml_str surfaces the raw toml error, which the file path is
        // attached to by from_toml_file.
        assert!(Config::from_toml_str("network = = =").is_err());
    }

    #[test]
    fn scheme_and_host_list_from_file_and_env() {
        let c = Config::from_toml_str("[clickhouse]\nhost = \" a:8443, b ,,\"\nscheme = \"HTTPS\"")
            .unwrap();
        assert_eq!(c.clickhouse.hosts(), vec!["a:8443", "b"]);
        assert_eq!(c.clickhouse.scheme, "https");
        c.validate().unwrap();
        let mut c = Config::default();
        c.apply_env(env_from(&[("BIDX_CLICKHOUSE__SCHEME", "Https")]))
            .unwrap();
        assert_eq!(c.clickhouse.scheme, "https");

        let mut c = Config::default();
        c.clickhouse.scheme = "ftp".into();
        assert!(c.validate().unwrap_err().to_string().contains("scheme"));
        let mut c = Config::default();
        c.clickhouse.host = " , ".into();
        assert!(c.validate().unwrap_err().to_string().contains("host"));
    }

    #[test]
    fn server_cache_and_rate_limit_sections_from_file() {
        let c = Config::from_toml_str(
            r#"
            [server]
            workers = 8
            queue = 16
            keep_alive_secs = 0
            max_requests_per_conn = 50
            drain_delay_secs = 1
            shutdown_timeout_secs = 2
            trust_forwarded_for = true
            [cache]
            enabled = false
            max_entries = 10
            tip_ttl_ms = 11
            address_ttl_ms = 12
            immutable_ttl_secs = 13
            confirmations = 3
            [rate_limit]
            per_ip_rps = 5.0
            per_ip_burst = 10.0
            "#,
        )
        .unwrap();
        assert_eq!(
            c.server,
            ServerConfig {
                workers: 8,
                queue: 16,
                keep_alive_secs: 0,
                max_requests_per_conn: 50,
                drain_delay_secs: 1,
                shutdown_timeout_secs: 2,
                trust_forwarded_for: true,
            }
        );
        assert_eq!(
            c.cache,
            CacheConfig {
                enabled: false,
                max_entries: 10,
                tip_ttl_ms: 11,
                address_ttl_ms: 12,
                immutable_ttl_secs: 13,
                confirmations: 3,
            }
        );
        assert_eq!(
            c.rate_limit,
            RateLimitConfig {
                per_ip_rps: 5.0,
                per_ip_burst: 10.0
            }
        );
        c.validate().unwrap();
    }

    #[test]
    fn server_cache_and_rate_limit_sections_from_env() {
        let mut c = Config::default();
        c.apply_env(env_from(&[
            ("BIDX_SERVER__WORKERS", "4"),
            ("BIDX_SERVER__QUEUE", "9"),
            ("BIDX_SERVER__KEEP_ALIVE_SECS", "7"),
            ("BIDX_SERVER__MAX_REQUESTS_PER_CONN", "3"),
            ("BIDX_SERVER__DRAIN_DELAY_SECS", "0"),
            ("BIDX_SERVER__SHUTDOWN_TIMEOUT_SECS", "1"),
            ("BIDX_SERVER__TRUST_FORWARDED_FOR", "yes"),
            ("BIDX_CACHE__ENABLED", "0"),
            ("BIDX_CACHE__MAX_ENTRIES", "5"),
            ("BIDX_CACHE__TIP_TTL_MS", "6"),
            ("BIDX_CACHE__ADDRESS_TTL_MS", "7"),
            ("BIDX_CACHE__IMMUTABLE_TTL_SECS", "8"),
            ("BIDX_CACHE__CONFIRMATIONS", "9"),
            ("BIDX_RATE_LIMIT__PER_IP_RPS", " 2.5 "),
            ("BIDX_RATE_LIMIT__PER_IP_BURST", "4"),
        ]))
        .unwrap();
        assert_eq!(
            (c.server.workers, c.server.queue, c.server.keep_alive_secs),
            (4, 9, 7)
        );
        assert_eq!(c.server.max_requests_per_conn, 3);
        assert_eq!(
            (c.server.drain_delay_secs, c.server.shutdown_timeout_secs),
            (0, 1)
        );
        assert!(c.server.trust_forwarded_for);
        assert!(!c.cache.enabled);
        assert_eq!(
            (
                c.cache.max_entries,
                c.cache.tip_ttl_ms,
                c.cache.address_ttl_ms
            ),
            (5, 6, 7)
        );
        assert_eq!((c.cache.immutable_ttl_secs, c.cache.confirmations), (8, 9));
        assert_eq!(
            c.rate_limit,
            RateLimitConfig {
                per_ip_rps: 2.5,
                per_ip_burst: 4.0
            }
        );

        let mut c = Config::default();
        let e = c
            .apply_env(env_from(&[("BIDX_SERVER__WORKERS", "many")]))
            .unwrap_err();
        assert!(e.to_string().contains("BIDX_SERVER__WORKERS"));
        assert!(Config::default()
            .apply_env(env_from(&[("BIDX_CACHE__ENABLED", "perhaps")]))
            .is_err());
    }

    #[test]
    fn auth_keys_from_file_hash_tokens_and_reject_ambiguity() {
        let digest = ApiKey::hash_token("t2");
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        let c = Config::from_toml_str(&format!(
            r#"
            [auth]
            required = true
            [[auth.keys]]
            name = "acme"
            token = "t1"
            rate_per_sec = 50.0
            [[auth.keys]]
            name = "globex"
            token_sha256 = "{}"
            rate_per_sec = 0.5
            burst = 3.0
            "#,
            hex.to_ascii_uppercase()
        ))
        .unwrap();
        assert!(c.auth.required);
        assert_eq!(c.auth.keys.len(), 2);
        assert_eq!(c.auth.keys[0].token_sha256, ApiKey::hash_token("t1"));
        // Burst defaults to max(rate, 1).
        assert_eq!(c.auth.keys[0].burst, 50.0);
        assert_eq!(c.auth.keys[1].token_sha256, digest);
        assert_eq!(c.auth.keys[1].burst, 3.0);
        c.validate().unwrap();

        let both = "[[auth.keys]]\nname = \"x\"\ntoken = \"a\"\ntoken_sha256 = \"b\"";
        assert!(Config::from_toml_str(both)
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
        let neither = "[[auth.keys]]\nname = \"x\"";
        assert!(Config::from_toml_str(neither).is_err());
        let bad_hex = "[[auth.keys]]\nname = \"x\"\ntoken_sha256 = \"zz\"";
        assert!(Config::from_toml_str(bad_hex)
            .unwrap_err()
            .to_string()
            .contains("64 hex"));
        let bad_nibble = format!(
            "[[auth.keys]]\nname = \"x\"\ntoken_sha256 = \"{}\"",
            "g".repeat(64)
        );
        assert!(Config::from_toml_str(&bad_nibble).is_err());
    }

    #[test]
    fn auth_keys_from_env() {
        let mut c = Config::default();
        c.apply_env(env_from(&[
            ("BIDX_AUTH__REQUIRED", "true"),
            ("BIDX_AUTH__KEYS", "a:tok-a, b:tok-b:10 ,c:tok-c:2:8,"),
        ]))
        .unwrap();
        assert!(c.auth.required);
        let k = &c.auth.keys;
        assert_eq!(k.len(), 3);
        assert_eq!(
            (k[0].name.as_str(), k[0].rate_per_sec, k[0].burst),
            ("a", 0.0, 1.0)
        );
        assert_eq!((k[1].rate_per_sec, k[1].burst), (10.0, 10.0));
        assert_eq!((k[2].rate_per_sec, k[2].burst), (2.0, 8.0));
        assert_eq!(k[2].token_sha256, ApiKey::hash_token("tok-c"));
        c.validate().unwrap();

        for bad in ["noname", ":tok", "a:", "a:t:x", "a:t:1:y", "a:t:1:2:3"] {
            let mut c = Config::default();
            assert!(
                c.apply_env(env_from(&[("BIDX_AUTH__KEYS", bad)])).is_err(),
                "accepted {bad}"
            );
        }
    }

    #[test]
    fn validate_rejects_bad_server_cache_auth_and_bucket_settings() {
        let check = |f: &dyn Fn(&mut Config), needle: &str| {
            let mut c = Config::default();
            f(&mut c);
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains(needle), "{e} should mention {needle}");
        };
        let key = |name: &str, tok: &str, rate: f64, burst: f64| ApiKey {
            name: name.into(),
            token_sha256: ApiKey::hash_token(tok),
            rate_per_sec: rate,
            burst,
        };
        check(&|c| c.server.workers = 0, "workers");
        check(&|c| c.server.queue = 0, "queue");
        check(
            &|c| c.server.max_requests_per_conn = 0,
            "max_requests_per_conn",
        );
        check(&|c| c.cache.max_entries = 0, "max_entries");
        check(&|c| c.auth.required = true, "no auth.keys");
        check(&|c| c.auth.keys = vec![key("", "t", 0.0, 1.0)], "names");
        check(
            &|c| c.auth.keys = vec![key("a", "t", 0.0, 1.0), key("a", "u", 0.0, 1.0)],
            "names",
        );
        check(
            &|c| c.auth.keys = vec![key("a", "t", 0.0, 1.0), key("b", "t", 0.0, 1.0)],
            "tokens",
        );
        check(
            &|c| c.auth.keys = vec![key("a", "t", -1.0, 1.0)],
            "auth.keys",
        );
        check(
            &|c| c.auth.keys = vec![key("a", "t", f64::NAN, 1.0)],
            "auth.keys",
        );
        check(
            &|c| c.auth.keys = vec![key("a", "t", 5.0, 0.5)],
            "auth.keys",
        );
        check(
            &|c| c.auth.keys = vec![key("a", "t", 5.0, f64::INFINITY)],
            "auth.keys",
        );
        check(&|c| c.rate_limit.per_ip_rps = 5.0, "rate_limit");
        // Disabled cache tolerates max_entries 0; a zero rate needs no burst.
        let mut c = Config::default();
        c.cache.enabled = false;
        c.cache.max_entries = 0;
        c.auth.keys = vec![key("a", "t", 0.0, 0.0)];
        c.validate().unwrap();
    }

    #[test]
    fn unknown_keys_are_rejected_so_typos_fail_loudly() {
        let e = Config::from_toml_str("[clickhouse]\nhots = \"x\"").unwrap_err();
        assert!(e.to_string().contains("hots"), "{e}");
        assert!(Config::from_toml_str("api_adr = \"x\"").is_err());
        assert!(
            Config::from_toml_str("[[auth.keys]]\nname = \"a\"\ntoken = \"t\"\nrps = 1").is_err()
        );
    }

    #[test]
    fn shipped_example_config_parses_and_validates() {
        let text = include_str!("../../../bidx.toml.example");
        let c = Config::from_toml_str(text).unwrap();
        c.validate().unwrap();
    }

    #[test]
    fn kubernetes_configmap_config_parses_and_validates() {
        // Extract the `bidx.toml: |` block literal from the manifest.
        let manifest = include_str!("../../../deploy/kubernetes/configmap.yaml");
        let start = manifest.find("bidx.toml: |").unwrap();
        let toml: String = manifest[start..]
            .lines()
            .skip(1)
            .take_while(|l| l.is_empty() || l.starts_with("    "))
            .map(|l| l.strip_prefix("    ").unwrap_or(l))
            .collect::<Vec<_>>()
            .join("\n");
        let c = Config::from_toml_str(&toml).unwrap();
        c.validate().unwrap();
        assert!(c.server.trust_forwarded_for);
        assert_eq!(c.cache.max_entries, 50_000);
    }

    #[test]
    fn error_display_is_actionable() {
        let e = ConfigError::Invalid("nope".into());
        assert_eq!(e.to_string(), "invalid config: nope");
        let e = ConfigError::Io {
            path: "/x".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "nope"),
        };
        assert!(e.to_string().contains("/x"));
    }
}
