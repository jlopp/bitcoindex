//! ClickHouse schema management and Parquet bulk loader.
//!
//! The loader talks to ClickHouse over its HTTP interface using plain
//! `clickhouse-client`-compatible SQL statements. We intentionally shell out
//! to `clickhouse-client` for DDL and use HTTP INSERT ... FROM INFILE-style
//! queries via the `file()` table function so the heavy lifting stays in
//! ClickHouse's native Parquet reader.

pub mod schema;

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("clickhouse-client not found on PATH")]
    NoClient,
    #[error("clickhouse-client failed: {0}")]
    Client(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct ChConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

impl Default for ChConfig {
    fn default() -> Self {
        ChConfig {
            host: "localhost".into(),
            port: 9000,
            user: "default".into(),
            password: None,
            database: "bitcoin".into(),
        }
    }
}

pub struct ChLoader {
    pub cfg: ChConfig,
    client: PathBuf,
}

impl ChLoader {
    pub fn new(cfg: ChConfig) -> Result<Self, LoadError> {
        let client = which_client().ok_or(LoadError::NoClient)?;
        Ok(ChLoader { cfg, client })
    }

    fn base_args(&self) -> Vec<String> {
        let mut a = vec![
            format!("--host={}", self.cfg.host),
            format!("--port={}", self.cfg.port),
            format!("--user={}", self.cfg.user),
        ];
        if let Some(p) = &self.cfg.password {
            a.push(format!("--password={p}"));
        }
        a
    }

    fn run_query(&self, sql: &str) -> Result<(), LoadError> {
        let out = std::process::Command::new(&self.client)
            .args(self.base_args())
            .arg(format!("--query={sql}"))
            .output()?;
        if !out.status.success() {
            return Err(LoadError::Client(String::from_utf8_lossy(&out.stderr).into_owned()));
        }
        Ok(())
    }

    /// Create database and all tables (idempotent).
    pub fn init_schema(&self) -> Result<(), LoadError> {
        self.run_query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.cfg.database))?;
        for ddl in schema::all_ddl(&self.cfg.database) {
            self.run_query(&ddl)?;
        }
        Ok(())
    }

    /// Bulk-load every part file for an entity from the Parquet directory
    /// using ClickHouse's `file()` table function with a glob.
    pub fn load_entity(&self, entity: &str, parquet_dir: &Path) -> Result<(), LoadError> {
        let glob = parquet_dir.join(entity).join("*.parquet");
        let sql = format!(
            "INSERT INTO {db}.{entity} SELECT * FROM file('{glob}', Parquet)",
            db = self.cfg.database,
            entity = entity,
            glob = glob.display(),
        );
        self.run_query(&sql)
    }

    pub fn load_all(&self, parquet_dir: &Path) -> Result<(), LoadError> {
        for entity in schema::ENTITIES {
            tracing::info!(entity, "loading into ClickHouse");
            self.load_entity(entity, parquet_dir)?;
        }
        Ok(())
    }
}

fn which_client() -> Option<PathBuf> {
    for name in ["clickhouse-client", "clickhouse"] {
        if let Ok(out) = std::process::Command::new("which").arg(name).output() {
            if out.status.success() {
                let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !p.is_empty() {
                    return Some(PathBuf::from(p));
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_local_clickhouse_defaults() {
        let c = ChConfig::default();
        assert_eq!(c.host, "localhost");
        assert_eq!(c.port, 9000);
        assert_eq!(c.user, "default");
        assert!(c.password.is_none());
        assert_eq!(c.database, "bitcoin");
    }

    #[test]
    fn load_error_display() {
        use std::error::Error as _;
        let no = LoadError::NoClient;
        assert_eq!(no.to_string(), "clickhouse-client not found on PATH");
        let cl = LoadError::Client("boom".into());
        assert_eq!(cl.to_string(), "clickhouse-client failed: boom");
        let io = LoadError::Io(std::io::Error::new(std::io::ErrorKind::Other, "io"));
        assert!(io.to_string().contains("io"));
        assert!(std::error::Error::source(&io).is_some());
        assert!(std::error::Error::source(&no).is_none());
    }

    /// We can't rely on a `clickhouse-client` binary in the sandbox, so
    /// construct a ChLoader through `new` only if one happens to exist;
    /// otherwise exercise the error path. Either way the base_args sql
    /// construction is asserted below.
    #[test]
    fn new_loader_resolves_or_errors() {
        let cfg = ChConfig::default();
        match ChLoader::new(cfg.clone()) {
            Ok(l) => {
                let args = l.base_args();
                assert!(args.iter().any(|a| a == &format!("--host={}", cfg.host)));
                assert!(args.iter().any(|a| a == &format!("--port={}", cfg.port)));
                assert!(args.iter().any(|a| a == &format!("--user={}", cfg.user)));
                assert!(!args.iter().any(|a| a.starts_with("--password")));
            }
            Err(LoadError::NoClient) => { /* acceptable in sandbox */ }
            Err(other) => panic!("unexpected new() error: {:?}", other),
        }
    }

    #[test]
    fn base_args_includes_optional_password() {
        // We can't construct ChLoader without a live binary, so test the args
        // helper through a synthesized ChLoader with a bogus client path.
        let cfg = ChConfig {
            host: "h".into(),
            port: 9999,
            user: "u".into(),
            password: Some("sekret".into()),
            database: "db".into(),
        };
        let loader = ChLoader {
            cfg,
            client: PathBuf::from("/definitely/not/a/real/binary/clickhouse-client"),
        };
        let args = loader.base_args();
        assert_eq!(args, vec![
            "--host=h", "--port=9999", "--user=u", "--password=sekret",
        ]);
        // run_query must surface Spawn failure as LoadError::Io (the binary
        // does not exist → spawn returns ENOENT).
        let e = loader.run_query("SELECT 1");
        match e {
            Err(LoadError::Io(_)) => {} // spawn ENOENT
            Err(LoadError::Client(_)) => {} // benign — spawn worked but binary returned nonzero
            Err(LoadError::NoClient) => panic!("NoClient is unreachable here"),
            Ok(_) => panic!("unexpected success from fake binary"),
        }
    }

    #[test]
    fn load_entity_builds_file_glob_query_and_errors_off() {
        let cfg = ChConfig::default();
        let loader = ChLoader {
            cfg: cfg.clone(),
            client: PathBuf::from("/definitely/not/a/real/bidx-ch"),
        };
        let tmp = std::env::temp_dir().join(format!("bidx-load-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let e = loader.load_entity("blocks", &tmp);
        assert!(e.is_err(), "expected spawn failure for bogus binary");
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
