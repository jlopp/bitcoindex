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
    pub host: String,
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
                port: 9000,
                http_port: 8123,
                user: "default".into(),
                password: None,
                database: "bitcoin".into(),
                timeout_secs: 30,
            },
            node: None,
            log: "info".into(),
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
        file.apply_to(&mut cfg);
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
        Ok(())
    }
}

fn parse_bool(v: &str) -> Result<bool, ConfigError> {
    match v.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Invalid(format!("bad boolean '{v}'"))),
    }
}

// ---------------------------------------------------------------------------
// TOML shape — mirrors Config but with every field optional.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, serde::Deserialize)]
struct FileConfig {
    network: Option<String>,
    api_addr: Option<String>,
    metrics_addr: Option<String>,
    esplora_enabled: Option<bool>,
    electrum_enabled: Option<bool>,
    log: Option<String>,
    clickhouse: Option<FileClickHouse>,
    node: Option<FileNode>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct FileClickHouse {
    host: Option<String>,
    port: Option<u16>,
    http_port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct FileNode {
    rpc_url: Option<String>,
    cookie: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl FileConfig {
    fn apply_to(self, cfg: &mut Config) {
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
        let mut c = Config::from_toml_str("network = \"testnet3\"\n[clickhouse]\nhost = \"file-host\"").unwrap();
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
        c.apply_env(env_from(&[("BIDX_NODE__COOKIE", "/x/.cookie")])).unwrap();
        assert!(c.node.is_none());
        // With an RPC URL the cookie is stored.
        let mut c = Config::default();
        c.apply_env(env_from(&[
            ("BIDX_NODE__RPC_URL", "http://n:8332"),
            ("BIDX_NODE__COOKIE", "/x/.cookie"),
        ]))
        .unwrap();
        assert_eq!(c.node.as_ref().unwrap().cookie.as_deref(), Some("/x/.cookie"));
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
        let e = c.apply_env(env_from(&[("BIDX_NETWORK", "moonnet")])).unwrap_err();
        assert!(matches!(e, ConfigError::Invalid(_)));
        let mut c = Config::default();
        assert!(c.apply_env(env_from(&[("BIDX_CLICKHOUSE__PORT", "not-a-port")])).is_err());
        let mut c = Config::default();
        assert!(c.apply_env(env_from(&[("BIDX_ESPLORA_ENABLED", "maybe")])).is_err());
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
        assert_eq!(NetworkName::Testnet4.to_core(), bidx_core::Network::Testnet4);
        assert_eq!(NetworkName::Testnet3.to_core(), bidx_core::Network::Testnet3);
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
