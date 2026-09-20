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
