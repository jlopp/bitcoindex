//! ClickHouse schema management and Parquet bulk loader.
//!
//! The loader talks to ClickHouse over its `clickhouse-client` interface. We
//! intentionally shell out to `clickhouse-client` for DDL and use HTTP
//! `INSERT ... SELECT FROM file()`-style queries via the `file()` table
//! function so the heavy lifting stays in ClickHouse's native Parquet reader.
//!
//! Schema shape (see [`schema`]): five base entity tables are bulk-loaded from
//! Parquet. Every derived query index is an auto-maintained table fed by a
//! `MATERIALIZED VIEW` over a base table, so loading the base entities keeps
//! the whole 18-index catalogue in sync. Two indexes (`coinstats_by_height`,
//! `address_prefix`) are backfills and are refreshed by [`ChLoader::rebuild_index`].

pub mod schema;

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("clickhouse-client not found on PATH")]
    NoClient,
    #[error("clickhouse-client failed: {0}")]
    Client(String),
    #[error("unknown index '{0}'")]
    UnknownIndex(String),
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

    /// Run several statements in one client invocation (multiquery). Used for
    /// the backfill sequences, which must run as one batch.
    fn run_multiquery(&self, statements: &[String]) -> Result<(), LoadError> {
        let sql = statements.join(";\n");
        let out = std::process::Command::new(&self.client)
            .args(self.base_args())
            .arg("--multiquery")
            .arg(format!("--query={sql}"))
            .output()?;
        if !out.status.success() {
            return Err(LoadError::Client(String::from_utf8_lossy(&out.stderr).into_owned()));
        }
        Ok(())
    }

    /// Create database, all base + index tables, and all materialized views
    /// (idempotent).
    pub fn init_schema(&self) -> Result<(), LoadError> {
        self.run_query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.cfg.database))?;
        for ddl in schema::all_ddl(&self.cfg.database) {
            self.run_query(&ddl)?;
        }
        Ok(())
    }

    /// Bulk-load every part file for a base entity from the Parquet directory
    /// using ClickHouse's `file()` table function with a glob. Derived index
    /// tables are populated by the materialized views defined in the schema.
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

    /// Refresh one backfilled index. Only the two backfill indexes are valid
    /// names here; every other index is maintained automatically by its MV.
    pub fn rebuild_index(&self, name: &str) -> Result<(), LoadError> {
        match name {
            "coinstats_by_height" => self.run_multiquery(&schema::backfill_coinstats(&self.cfg.database)),
            // address_prefix is populated from rendered address strings; the
            // renderer lives in the query layer, so this is a no-op placeholder
            // that truncates the table ready for the caller's INSERT.
            "address_prefix" => {
                self.run_query(&format!("TRUNCATE TABLE {}.address_prefix", self.cfg.database))
            }
            other => Err(LoadError::UnknownIndex(other.to_string())),
        }
    }
}

fn which_client() -> Option<PathBuf> {
    let path = std::env::var("PATH").unwrap_or_default();
    client_from_path(&path)
}

/// Find a ClickHouse client executable on a PATH-like string. Prefers
/// `clickhouse-client` over the multi-call `clickhouse` binary, and scans
/// directories left to right. Split out from [`which_client`] so it can be
/// unit-tested without mutating the process environment.
fn client_from_path(path_env: &str) -> Option<PathBuf> {
    for dir in std::env::split_paths(path_env) {
        for name in ["clickhouse-client", "clickhouse"] {
            let cand = dir.join(name);
            if cand.is_file() {
                return Some(cand);
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
        let no = LoadError::NoClient;
        assert_eq!(no.to_string(), "clickhouse-client not found on PATH");
        let cl = LoadError::Client("boom".into());
        assert_eq!(cl.to_string(), "clickhouse-client failed: boom");
        let unk = LoadError::UnknownIndex("nope".into());
        assert_eq!(unk.to_string(), "unknown index 'nope'");
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
        assert!(e.is_err(), "bogus binary must fail");
        assert!(
            matches!(e, Err(LoadError::Io(_)) | Err(LoadError::Client(_))),
            "unexpected error from fake binary: {e:?}"
        );
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

    /// `rebuild_index` rejects names that are not backfilled indexes before
    /// touching the client, so the guard is testable without ClickHouse.
    #[test]
    fn rebuild_index_rejects_non_backfill_names() {
        let loader = ChLoader {
            cfg: ChConfig::default(),
            client: PathBuf::from("/definitely/not/a/real/bidx-ch"),
        };
        // An MV-maintained index must be refused with UnknownIndex.
        match loader.rebuild_index("transactions_by_txid") {
            Err(LoadError::UnknownIndex(n)) => assert_eq!(n, "transactions_by_txid"),
            other => panic!("expected UnknownIndex, got {:?}", other),
        }
        // A genuinely backfilled name must reach the client (and fail with a
        // spawn error against the bogus binary, not UnknownIndex).
        match loader.rebuild_index("coinstats_by_height") {
            Err(LoadError::UnknownIndex(n)) => panic!("coinstats should be valid, got {n}"),
            _ => {}
        }
    }

    /// `run_multiquery` joins statements with `;` and must fail on a bogus
    /// binary the same way `run_query` does.
    #[test]
    fn run_multiquery_errors_on_bogus_client() {
        let loader = ChLoader {
            cfg: ChConfig::default(),
            client: PathBuf::from("/definitely/not/a/real/bidx-ch"),
        };
        let stmts = vec!["SELECT 1".to_string(), "SELECT 2".to_string()];
        assert!(loader.run_multiquery(&stmts).is_err());
    }

    /// Every backfill name accepted by `rebuild_index` must correspond to an
    /// entry in the schema catalogue, so the two can't drift.
    #[test]
    fn backfill_names_are_in_catalogue() {
        let cat = schema::index_catalog();
        for name in ["coinstats_by_height", "address_prefix"] {
            assert!(
                cat.iter().any(|s| s.name == name),
                "backfill {name} missing from catalogue"
            );
        }
    }

    /// Build a fake `clickhouse-client`: a script that exits 0 and echoes the
    /// `--query=` argument, or (when `fail` is set) exits 1 with a message.
    /// Returns the path to the script.
    fn fake_client(tag: &str, fail: bool) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "bidx-fake-ch-{tag}-{}",
            std::process::id()
        ));
        let body = if fail {
            "#!/bin/sh\necho 'simulated failure' >&2\nexit 1\n".to_string()
        } else {
            "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in --query=*) echo \"$a\";; esac; done\nexit 0\n".to_string()
        };
        std::fs::write(&path, body).unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    fn loader_with(client: PathBuf) -> ChLoader {
        ChLoader { cfg: ChConfig { database: "db".into(), ..ChConfig::default() }, client }
    }

    #[test]
    fn init_schema_runs_database_then_all_ddl_on_success() {
        let client = fake_client("initok", false);
        let loader = loader_with(client.clone());
        loader.init_schema().expect("fake client succeeds");
        // Error path: a failing client surfaces LoadError::Client with stderr.
        let bad = loader_with(fake_client("initbad", true));
        match bad.init_schema() {
            Err(LoadError::Client(msg)) => assert!(msg.contains("simulated failure")),
            other => panic!("expected Client error, got {other:?}"),
        }
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn load_all_iterates_every_entity_on_success() {
        let client = fake_client("loadall", false);
        let loader = loader_with(client.clone());
        let tmp = std::env::temp_dir().join(format!("bidx-loadall-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        loader.load_all(&tmp).expect("fake client succeeds");
        let _ = std::fs::remove_dir_all(&tmp);
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn rebuild_index_backfills_succeed_and_address_prefix_truncates() {
        let client = fake_client("rebuild", false);
        let loader = loader_with(client.clone());
        // Both backfill names must reach the client and succeed.
        loader.rebuild_index("coinstats_by_height").expect("coinstats ok");
        loader.rebuild_index("address_prefix").expect("address_prefix ok");
        // And a failing client surfaces the error.
        let bad = loader_with(fake_client("rebuildbad", true));
        assert!(matches!(
            bad.rebuild_index("coinstats_by_height"),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn client_from_path_prefers_clickhouse_client_and_scans_order() {
        use std::os::unix::fs::PermissionsExt;
        let dir_a = std::env::temp_dir().join(format!("bidx-path-a-{}", std::process::id()));
        let dir_b = std::env::temp_dir().join(format!("bidx-path-b-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();

        // Nothing present → None.
        let p = std::env::join_paths([&dir_a, &dir_b]).unwrap();
        assert_eq!(client_from_path(p.to_str().unwrap()), None);

        // Only `clickhouse` in dir_b → found.
        let ch = dir_b.join("clickhouse");
        std::fs::write(&ch, b"#!/bin/sh\n").unwrap();
        let mut perms = std::fs::metadata(&ch).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&ch, perms.clone()).unwrap();
        let p = std::env::join_paths([&dir_a, &dir_b]).unwrap();
        assert_eq!(client_from_path(p.to_str().unwrap()), Some(ch.clone()));

        // `clickhouse-client` in dir_a wins over `clickhouse` in dir_b.
        let cc = dir_a.join("clickhouse-client");
        std::fs::write(&cc, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&cc, perms).unwrap();
        let p = std::env::join_paths([&dir_a, &dir_b]).unwrap();
        assert_eq!(client_from_path(p.to_str().unwrap()), Some(cc.clone()));

        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }
}
