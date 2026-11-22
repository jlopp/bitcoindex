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
//! the whole 19-index catalogue in sync. Three indexes are backfills:
//! `coinstats_by_height` and `scripthash_map` are pure SQL and refreshed by
//! [`ChLoader::rebuild_index`]; `address_prefix` needs a Rust-side base58/bech32
//! renderer and is refreshed by [`ChLoader::rebuild_address_prefix`].

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

    /// Construct with an explicit client path. Used by tests and by callers
    /// that ship a ClickHouse client alongside the binary rather than relying
    /// on it being on `PATH`.
    pub fn with_client(cfg: ChConfig, client: PathBuf) -> Self {
        ChLoader { cfg, client }
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
    /// (idempotent), then record the current schema version.
    pub fn init_schema(&self) -> Result<(), LoadError> {
        self.run_query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.cfg.database))?;
        for ddl in schema::all_ddl(&self.cfg.database) {
            self.run_query(&ddl)?;
        }
        self.run_query(&schema::meta_schema(&self.cfg.database))?;
        // A freshly-initialized database is at the current version by
        // construction (`all_ddl` already reflects it).
        self.run_query(&schema::record_schema_version(
            &self.cfg.database,
            schema::SCHEMA_VERSION,
        ))?;
        Ok(())
    }

    /// Run a `SELECT` and return the client's stdout (used for verification
    /// queries that must read values back out of ClickHouse).
    pub fn query_scalar(&self, sql: &str) -> Result<String, LoadError> {
        let out = std::process::Command::new(&self.client)
            .args(self.base_args())
            .arg(format!("--query={sql}"))
            .output()?;
        if !out.status.success() {
            return Err(LoadError::Client(String::from_utf8_lossy(&out.stderr).into_owned()));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// The highest applied schema version (0 when the table is absent).
    pub fn schema_version(&self) -> Result<u32, LoadError> {
        let out = self.query_scalar(&schema::read_schema_version(&self.cfg.database))?;
        Ok(out.trim().parse().unwrap_or(0))
    }

    /// Apply any migrations newer than the recorded schema version, in order,
    /// then record the new version. Idempotent: re-running with the version
    /// already current applies nothing.
    pub fn migrate(&self) -> Result<Vec<u32>, LoadError> {
        let current = self.schema_version()?;
        let migrations = schema::materialize_migrations(&self.cfg.database);
        self.apply_migrations(current, &migrations)
    }

    /// Apply a supplied migration list against a known current version. Split
    /// from [`migrate`] so the apply loop can be tested with synthetic
    /// migrations (the real list is empty until the first schema change).
    fn apply_migrations(
        &self,
        current: u32,
        migrations: &[(u32, Vec<String>)],
    ) -> Result<Vec<u32>, LoadError> {
        let mut applied = Vec::new();
        for (version, statements) in migrations {
            if *version > current {
                self.run_multiquery(statements)?;
                self.run_query(&schema::record_schema_version(&self.cfg.database, *version))?;
                applied.push(*version);
            }
        }
        Ok(applied)
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

    /// Refresh one backfilled index. Only the *SQL-only* backfill indexes are
    /// valid names here; every other index is maintained automatically by its
    /// MV, and `address_prefix` is handled by [`rebuild_address_prefix`]
    /// because it needs a Rust-side address renderer.
    pub fn rebuild_index(&self, name: &str) -> Result<(), LoadError> {
        match name {
            "coinstats_by_height" => self.run_multiquery(&schema::backfill_coinstats(&self.cfg.database)),
            "scripthash_map" => {
                self.run_multiquery(&schema::backfill_scripthash_map(&self.cfg.database))
            }
            // `address_prefix` needs base58/bech32 rendering, which ClickHouse
            // cannot do; see `rebuild_address_prefix`. Refuse it here rather
            // than silently truncating the table.
            other => Err(LoadError::UnknownIndex(other.to_string())),
        }
    }

    /// Rows per `INSERT` when rebuilding `address_prefix`. Each rendered row is
    /// ~130 bytes of SQL, and the statement is passed to `clickhouse-client` as
    /// a single `--query=` argv element, which Linux caps at 128 KiB
    /// (`MAX_ARG_STRLEN`). 500 keeps the statement well under that while still
    /// batching enough for ClickHouse to write efficiently.
    pub const ADDRESS_PREFIX_BATCH: usize = 500;

    /// Rebuild the `address_prefix` index. ClickHouse cannot encode
    /// base58/bech32 addresses, so this streams the distinct
    /// `(script_type, address_hash)` pairs out of `outputs`, renders each with
    /// `render` (the serving layer's codec), and inserts the results back in
    /// batches. `render` returns `None` for a pair with no address form, which
    /// is skipped.
    ///
    /// Returns the number of addresses inserted. Idempotent: the table is
    /// truncated first, so a re-run replaces its contents.
    pub fn rebuild_address_prefix<F>(&self, render: F) -> Result<usize, LoadError>
    where
        F: Fn(u8, &[u8; 32]) -> Option<String>,
    {
        let db = self.cfg.database.clone();
        self.run_query(&format!("TRUNCATE TABLE {db}.address_prefix"))?;
        let rows = self.query_scalar(&schema::address_prefix_source(&db))?;

        let mut batch: Vec<(String, u8, String)> = Vec::with_capacity(Self::ADDRESS_PREFIX_BATCH);
        let mut inserted = 0usize;
        for line in rows.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (ty_str, hash_hex) = line.split_once('\t').ok_or_else(|| {
                LoadError::Client(format!("malformed address_prefix row: {line:?}"))
            })?;
            let ty: u8 = ty_str.trim().parse().map_err(|_| {
                LoadError::Client(format!("bad script_type in address_prefix row: {line:?}"))
            })?;
            let hash = parse_hash32(hash_hex.trim())?;
            let Some(addr) = render(ty, &hash) else { continue };
            batch.push((addr, ty, hash_hex.trim().to_ascii_lowercase()));
            if batch.len() >= Self::ADDRESS_PREFIX_BATCH {
                self.run_query(&schema::insert_address_prefix(&db, &batch))?;
                inserted += batch.len();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.run_query(&schema::insert_address_prefix(&db, &batch))?;
            inserted += batch.len();
        }
        Ok(inserted)
    }
}

/// Parse a 64-char hex string into 32 bytes. The `outputs` source query emits
/// `address_hash` as `hex(...)`, so every pair comes back in this form.
fn parse_hash32(s: &str) -> Result<[u8; 32], LoadError> {
    if s.len() != 64 {
        return Err(LoadError::Client(format!(
            "expected 64 hex chars for address_hash, got {}: {s:?}",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)
            .map_err(|_| LoadError::Client(format!("bad hex in address_hash: {s:?}")))?;
    }
    Ok(out)
}

fn which_client() -> Option<PathBuf> {
    // An explicit override lets an operator ship a pinned client binary next
    // to the indexer instead of depending on whatever is on PATH.
    client_from_env(
        std::env::var_os("BIDX_CLICKHOUSE_CLIENT"),
        &std::env::var("PATH").unwrap_or_default(),
    )
}

/// Resolve the client binary from an optional explicit override plus a
/// PATH-like string. Pure, so it is unit-tested without touching the process
/// environment (which would race with other tests).
fn client_from_env(override_path: Option<std::ffi::OsString>, path_env: &str) -> Option<PathBuf> {
    if let Some(p) = override_path {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    client_from_path(path_env)
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
        let _g = fork_guard();
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
        let _g = fork_guard();
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
        let _g = fork_guard();
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
        let _g = fork_guard();
        let loader = ChLoader {
            cfg: ChConfig::default(),
            client: PathBuf::from("/definitely/not/a/real/bidx-ch"),
        };
        let stmts = vec!["SELECT 1".to_string(), "SELECT 2".to_string()];
        assert!(loader.run_multiquery(&stmts).is_err());
    }

    /// Every backfill name must correspond to an entry in the schema
    /// catalogue, so the two can't drift. `coinstats_by_height` and
    /// `scripthash_map` are SQL backfills; `address_prefix` is the Rust-rendered
    /// one.
    #[test]
    fn backfill_names_are_in_catalogue() {
        let cat = schema::index_catalog();
        for name in ["coinstats_by_height", "address_prefix", "scripthash_map"] {
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
        use std::os::unix::fs::PermissionsExt;
        // Write to a private temp file, set the mode, then rename into place.
        // The script's inode is therefore never open for writing at its final
        // path, so a concurrently-forked test that inherits the write fd cannot
        // make `execve` fail with ETXTBSY.
        let tmp = std::env::temp_dir().join(format!(
            "bidx-fake-ch-{tag}-{}-tmp",
            std::process::id()
        ));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    fn loader_with(client: PathBuf) -> ChLoader {
        ChLoader { cfg: ChConfig { database: "db".into(), ..ChConfig::default() }, client }
    }

    /// Tests that spawn the fake client must not run concurrently: a fork
    /// snapshots every open fd, so a sibling thread mid-`fs::write` would leave
    /// its write fd inherited by the child, and `execve` of that inode then
    /// fails with ETXTBSY. Holding this guard for the whole test body removes
    /// the overlap.
    static FORK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn fork_guard() -> std::sync::MutexGuard<'static, ()> {
        FORK_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn with_client_uses_the_supplied_path_verbatim() {
        let p = PathBuf::from("/opt/bidx/clickhouse-client");
        let l = ChLoader::with_client(ChConfig::default(), p.clone());
        assert_eq!(l.client, p);
    }

    #[test]
    fn which_client_honours_explicit_override_when_present() {
        let _g = fork_guard();
        // A real file at BIDX_CLICKHOUSE_CLIENT wins over PATH lookup. Exercised
        // through the pure resolver so the process env is never mutated.
        let client = fake_client("override", false);
        let resolved = client_from_env(Some(client.clone().into_os_string()), "/nonexistent");
        assert_eq!(resolved, Some(client.clone()));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn which_client_ignores_override_pointing_at_a_missing_file() {
        // An override that doesn't exist falls through to the PATH scan.
        let resolved = client_from_env(
            Some("/definitely/not/here/clickhouse-client".into()),
            "/definitely/empty-dir",
        );
        assert_eq!(resolved, None);
    }

    #[test]
    fn init_schema_runs_database_then_all_ddl_on_success() {
        let _g = fork_guard();
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
    fn schema_version_reads_scalar_and_migrate_is_forward_only() {
        let _g = fork_guard();
        let client = fake_client("migrate", false);
        let loader = loader_with(client.clone());
        // The fake client echoes its --query= argument; read_schema_version's
        // query has no leading digits, so parsing yields 0 → every migration
        // (currently just v2, the address_stats rebuild) is applied.
        let applied = loader.migrate().expect("fake client succeeds");
        assert_eq!(applied, vec![2], "v2 address_stats rebuild is pending at v0");
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn apply_migrations_skips_applied_and_runs_pending_in_order() {
        let _g = fork_guard();
        let client = fake_client("applymig", false);
        let loader = loader_with(client.clone());
        let migrations = vec![
            (1u32, vec!["SELECT 1".to_string()]),
            (2, vec!["SELECT 2".to_string()]),
            (3, vec!["SELECT 3".to_string()]),
        ];
        // Already at version 1: only 2 and 3 are applied, in order.
        let applied = loader.apply_migrations(1, &migrations).unwrap();
        assert_eq!(applied, vec![2, 3]);
        // Already at the newest: nothing applied.
        let applied = loader.apply_migrations(3, &migrations).unwrap();
        assert!(applied.is_empty());
        // From scratch: all three.
        let applied = loader.apply_migrations(0, &migrations).unwrap();
        assert_eq!(applied, vec![1, 2, 3]);
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn query_scalar_surfaces_client_failure() {
        let _g = fork_guard();
        let loader = loader_with(fake_client("qscalarbad", true));
        match loader.query_scalar("SELECT 1") {
            Err(LoadError::Client(m)) => assert!(m.contains("simulated failure")),
            other => panic!("expected Client error, got {other:?}"),
        }
    }

    #[test]
    fn schema_version_and_meta_ddl_are_wellformed() {
        // The version query must select a scalar we can parse.
        let q = schema::read_schema_version("db");
        assert!(q.contains("max(version)"));
        assert!(q.contains("db.meta_schema"));
        // The meta table and the record statement are idempotent / simple.
        assert!(schema::meta_schema("db").contains("CREATE TABLE IF NOT EXISTS db.meta_schema"));
        assert_eq!(
            schema::record_schema_version("db", 3),
            "INSERT INTO db.meta_schema (version) VALUES (3)"
        );
        // Baseline version is at least 1 and no migration targets a version
        // above it (a migration for a future version would be unreachable).
        assert!(schema::SCHEMA_VERSION >= 1);
        for (v, stmts) in schema::migrations() {
            assert!(v > 0 && v <= schema::SCHEMA_VERSION, "migration {v} out of range");
            assert!(!stmts.is_empty(), "migration {v} has no statements");
        }
    }

    #[test]
    fn load_all_iterates_every_entity_on_success() {
        let _g = fork_guard();
        let client = fake_client("loadall", false);
        let loader = loader_with(client.clone());
        let tmp = std::env::temp_dir().join(format!("bidx-loadall-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        loader.load_all(&tmp).expect("fake client succeeds");
        let _ = std::fs::remove_dir_all(&tmp);
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn rebuild_index_backfills_succeed_and_address_prefix_is_refused() {
        let _g = fork_guard();
        let client = fake_client("rebuild", false);
        let loader = loader_with(client.clone());
        // The SQL-only backfill names reach the client and succeed.
        loader.rebuild_index("coinstats_by_height").expect("coinstats ok");
        loader.rebuild_index("scripthash_map").expect("scripthash_map ok");
        // `address_prefix` needs a Rust renderer, so it is refused here rather
        // than silently truncating the table (use `rebuild_address_prefix`).
        match loader.rebuild_index("address_prefix") {
            Err(LoadError::UnknownIndex(n)) => assert_eq!(n, "address_prefix"),
            other => panic!("expected UnknownIndex for address_prefix, got {other:?}"),
        }
        // And a failing client surfaces the error.
        let bad = loader_with(fake_client("rebuildbad", true));
        assert!(matches!(
            bad.rebuild_index("coinstats_by_height"),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&client);
    }

    /// A fake client that answers the address_prefix source query with a fixed
    /// TSV body (so `rebuild_address_prefix` has rows to render) and echoes any
    /// other query. `fail` makes it exit 1.
    fn fake_prefix_client(tag: &str, rows: &str, fail: bool) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "bidx-fake-prefix-{tag}-{}",
            std::process::id()
        ));
        let body = if fail {
            "#!/bin/sh\necho 'simulated failure' >&2\nexit 1\n".to_string()
        } else {
            // Emit the fixed rows for the source SELECT; echo the query for
            // anything else (so TRUNCATE/INSERT calls succeed).
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do case \"$a\" in --query=*DISTINCT*script_type*) printf '%s' '{rows}'; exit 0;; esac; done\n\
                 for a in \"$@\"; do case \"$a\" in --query=*) echo \"$a\";; esac; done\n\
                 exit 0\n"
            )
        };
        use std::os::unix::fs::PermissionsExt;
        let tmp = std::env::temp_dir().join(format!(
            "bidx-fake-prefix-{tag}-{}-tmp",
            std::process::id()
        ));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    #[test]
    fn rebuild_address_prefix_renders_and_inserts_in_batches() {
        let _g = fork_guard();
        // Two renderable rows and one the renderer declines (OP-return-ish).
        let rows = format!(
            "2\t{}\n4\t{}\n7\t{}\n",
            "ab".repeat(32),
            "cd".repeat(32),
            "00".repeat(32)
        );
        let client = fake_prefix_client("ok", &rows, false);
        let loader = loader_with(client.clone());
        // The renderer receives the decoded type/hash and returns an address.
        let n = loader
            .rebuild_address_prefix(|ty, hash| match ty {
                2 => Some(format!("1main{}", hash[0])),
                4 => Some(format!("bc1q{}", hash[0])),
                _ => None,
            })
            .expect("backfill ok");
        // Only the two renderable rows are inserted.
        assert_eq!(n, 2);
        let _ = std::fs::remove_file(&client);

        // A failing client surfaces the error.
        let bad = loader_with(fake_prefix_client("bad", &rows, true));
        assert!(matches!(
            bad.rebuild_address_prefix(|_, _| Some("x".into())),
            Err(LoadError::Client(_))
        ));
    }

    #[test]
    fn rebuild_address_prefix_rejects_malformed_rows() {
        let _g = fork_guard();
        // A row with no tab separator.
        let client = fake_prefix_client("notab", "garbage-no-tab\n", false);
        let loader = loader_with(client.clone());
        assert!(matches!(
            loader.rebuild_address_prefix(|_, _| Some("x".into())),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&client);

        // A non-numeric script_type.
        let client = fake_prefix_client("badty", &format!("xx\t{}\n", "ab".repeat(32)), false);
        let loader = loader_with(client.clone());
        assert!(matches!(
            loader.rebuild_address_prefix(|_, _| Some("x".into())),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&client);

        // A short address_hash.
        let client = fake_prefix_client("shorthash", "2\tabcd\n", false);
        let loader = loader_with(client.clone());
        assert!(matches!(
            loader.rebuild_address_prefix(|_, _| Some("x".into())),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&client);

        // A non-hex address_hash of the right length.
        let client = fake_prefix_client("badhex", &format!("2\t{}\n", "zz".repeat(32)), false);
        let loader = loader_with(client.clone());
        assert!(matches!(
            loader.rebuild_address_prefix(|_, _| Some("x".into())),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn rebuild_address_prefix_skips_blank_lines_and_inserts_nothing_when_empty() {
        let _g = fork_guard();
        // No rows at all: `query_scalar` trims, so this is the empty string.
        let client = fake_prefix_client("empty", "\n", false);
        let loader = loader_with(client.clone());
        assert_eq!(loader.rebuild_address_prefix(|_, _| Some("x".into())).unwrap(), 0);
        let _ = std::fs::remove_file(&client);

        // An internal blank line (survives the outer trim) is skipped.
        let body = format!("2\t{}\n\n4\t{}\n", "ab".repeat(32), "cd".repeat(32));
        let client = fake_prefix_client("blank", &body, false);
        let loader = loader_with(client.clone());
        assert_eq!(loader.rebuild_address_prefix(|_, _| Some("x".into())).unwrap(), 2);
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn rebuild_address_prefix_flushes_full_batches() {
        let _g = fork_guard();
        // One row past the batch size forces the in-loop flush, then a final
        // partial flush for the remainder.
        let row = format!("2\t{}\n", "ab".repeat(32));
        let body = row.repeat(ChLoader::ADDRESS_PREFIX_BATCH + 1);
        let client = fake_prefix_client("batch", &body, false);
        let loader = loader_with(client.clone());
        let n = loader.rebuild_address_prefix(|_, _| Some("1x".into())).unwrap();
        assert_eq!(n, ChLoader::ADDRESS_PREFIX_BATCH + 1);
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn parse_hash32_validates_length_and_hex() {
        assert_eq!(parse_hash32(&"0f".repeat(32)).unwrap()[0], 0x0f);
        assert!(matches!(parse_hash32("abcd"), Err(LoadError::Client(_))));
        assert!(matches!(parse_hash32(&"zz".repeat(32)), Err(LoadError::Client(_))));
    }

    #[test]
    fn client_from_path_prefers_clickhouse_client_and_scans_order() {
        let _g = fork_guard();
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
