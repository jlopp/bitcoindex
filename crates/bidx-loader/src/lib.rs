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

pub mod live_insert;
pub mod rowbinary;
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
    /// Connect over TLS (`clickhouse-client --secure`). Required by ClickHouse
    /// Cloud and any server that only exposes the secure native port (9440).
    pub secure: bool,
}

impl Default for ChConfig {
    fn default() -> Self {
        ChConfig {
            host: "localhost".into(),
            port: 9000,
            user: "default".into(),
            password: None,
            database: "bitcoin".into(),
            secure: false,
        }
    }
}

impl ChConfig {
    /// Connection arguments for `clickhouse-client`, shared by every caller
    /// that spawns the client so TLS and credentials are applied uniformly.
    pub fn client_args(&self) -> Vec<String> {
        let mut a = vec![
            format!("--host={}", self.host),
            format!("--port={}", self.port),
            format!("--user={}", self.user),
        ];
        if let Some(p) = &self.password {
            a.push(format!("--password={p}"));
        }
        if self.secure {
            a.push("--secure".to_string());
        }
        a
    }
}

/// How `load_all_with` moves Parquet into ClickHouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    /// `INSERT ... SELECT FROM file()` — the server reads the files from its
    /// own `user_files` directory. Fastest, but the Parquet must be on the
    /// ClickHouse host.
    ServerFile,
    /// Stream each file from this machine over the client connection. Works
    /// against any reachable server, including ClickHouse Cloud.
    Upload,
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

    /// The resolved `clickhouse-client` binary path. Exposed so the live
    /// direct-insert path can open its own `RowBinary` pipes without resolving
    /// `PATH` a second time.
    pub fn client_path(&self) -> PathBuf {
        self.client.clone()
    }

    fn base_args(&self) -> Vec<String> {
        self.cfg.client_args()
    }

    fn run_query(&self, sql: &str) -> Result<(), LoadError> {
        let out = std::process::Command::new(&self.client)
            .args(self.base_args())
            .arg(format!("--query={sql}"))
            .output()?;
        if !out.status.success() {
            return Err(LoadError::Client(
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ));
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
            return Err(LoadError::Client(
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ));
        }
        Ok(())
    }

    /// Create database, all base + index tables, and all materialized views
    /// (idempotent), then record the current schema version.
    pub fn init_schema(&self) -> Result<(), LoadError> {
        self.run_query(&format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            self.cfg.database
        ))?;
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
            return Err(LoadError::Client(
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Retract every base-table row at `height` and above, so a live reorg can
    /// be repaired by reloading the canonical range. Live mode appends each
    /// applied block straight to ClickHouse, so a disconnected height leaves
    /// rows behind; this is the inverse operation.
    ///
    /// The base tables are partitioned by `intDiv(height, 10000)`, so the
    /// affected partition is dropped when `height` sits exactly on a 10,000
    /// boundary and row-deleted otherwise. A `DELETE` is used rather than
    /// `ALTER ... DROP PARTITION` for a mid-partition height because dropping
    /// would discard canonical rows below `height` too. `DELETE` is a mutation,
    /// so it is heavier than a partition drop — acceptable at the tip, where a
    /// reorg touches only the newest blocks.
    pub fn retract_from(&self, height: u32) -> Result<(), LoadError> {
        let db = &self.cfg.database;
        for table in schema::ENTITIES {
            if height % 10_000 == 0 {
                // Partition-aligned: drop the whole (>= height) partition range
                // cheaply. `PARTITION` takes the partition id (height / 10000).
                let part = height / 10_000;
                self.run_query(&format!("ALTER TABLE {db}.{table} DROP PARTITION {part}"))?;
            } else {
                self.run_query(&format!(
                    "DELETE FROM {db}.{table} WHERE height >= {height}"
                ))?;
            }
        }
        // The witness payload table is optional; retract it too when present.
        if self
            .query_scalar(&schema::table_exists(db, schema::WITNESS_ENTITY))?
            .trim()
            == "1"
        {
            self.run_query(&format!(
                "DELETE FROM {db}.{} WHERE height >= {height}",
                schema::WITNESS_ENTITY
            ))?;
        }
        Ok(())
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

    /// Maximum concurrent `clickhouse-client` streams when uploading Parquet
    /// parts or backfilling independent derived index tables.
    pub const UPLOAD_CONCURRENCY: usize = 4;
    pub const BACKFILL_CONCURRENCY: usize = 4;

    /// Upload a single Parquet file to `entity` over `clickhouse-client` stdin.
    pub fn upload_parquet_file(&self, entity: &str, path: &Path) -> Result<(), LoadError> {
        let sql = format!("INSERT INTO {}.{entity} FORMAT Parquet", self.cfg.database);
        let input = std::fs::File::open(path)?;
        let out = std::process::Command::new(&self.client)
            .args(self.base_args())
            .arg(format!("--query={sql}"))
            .stdin(input)
            .output()?;
        if !out.status.success() {
            return Err(LoadError::Client(format!(
                "upload of {} failed: {}",
                path.display(),
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }

    fn collect_parquet_files(dir: &Path) -> Result<Vec<PathBuf>, LoadError> {
        let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        files.sort_unstable();
        Ok(files)
    }

    /// Upload every part file for an entity from the *client* side: each file
    /// is streamed to `INSERT INTO ... FORMAT Parquet` over the client's
    /// stdin. Unlike [`load_entity`](Self::load_entity) this needs no access to
    /// the server's filesystem, so it works against ClickHouse Cloud or any
    /// remote cluster. Files are sent in name order (part files are named by
    /// block range). Returns the number of files uploaded.
    pub fn upload_entity(&self, entity: &str, parquet_dir: &Path) -> Result<usize, LoadError> {
        let files = Self::collect_parquet_files(&parquet_dir.join(entity))?;
        for path in &files {
            self.upload_parquet_file(entity, path)?;
        }
        Ok(files.len())
    }

    pub fn load_all(&self, parquet_dir: &Path) -> Result<(), LoadError> {
        self.load_all_with(parquet_dir, LoadMode::ServerFile)
    }

    /// Prepare ClickHouse for a bulk or pipelined-streaming load by dropping
    /// all materialized views in one batch so base-table inserts do not fan out
    /// into derived-table writes on every row.
    pub fn begin_streaming_load(&self) -> Result<(), LoadError> {
        tracing::info!("dropping materialized views for a deferred derived-index build");
        self.run_multiquery(&schema::drop_materialized_views(&self.cfg.database))
    }

    /// Load a single completed part (`part-{part_id:05}.parquet`) across all
    /// base entities (plus `witnesses` when `capture_witness` is set) in
    /// parallel, optionally unlinking each part file once ClickHouse confirms
    /// ingestion so `bidx parse --stream-clickhouse --unlink-loaded-parts`
    /// overlaps ClickHouse ingestion with RocksDB indexing and halves disk use.
    pub fn load_part(
        &self,
        parquet_dir: &Path,
        part_id: u32,
        capture_witness: bool,
        mode: LoadMode,
        unlink_after: bool,
    ) -> Result<(), LoadError> {
        let mut entities: Vec<&'static str> = schema::ENTITIES.to_vec();
        if capture_witness {
            entities.push(schema::WITNESS_ENTITY);
        }
        let file_name = format!("part-{part_id:05}.parquet");
        run_parallel(entities, 6, |entity| {
            let path = parquet_dir.join(entity).join(&file_name);
            match mode {
                LoadMode::ServerFile => {
                    let sql = format!(
                        "INSERT INTO {db}.{entity} SELECT * FROM file('{p}', Parquet)",
                        db = self.cfg.database,
                        p = path.display(),
                    );
                    self.run_query(&sql)?;
                }
                LoadMode::Upload => {
                    self.upload_parquet_file(entity, &path)?;
                }
            }
            if unlink_after {
                std::fs::remove_file(&path)?;
            }
            Ok(())
        })
    }

    /// Finalize a bulk or pipelined-streaming load by building all independent
    /// derived index tables in parallel from the base tables, then recreating
    /// the materialized views for subsequent live tracking.
    pub fn finish_streaming_load(&self) -> Result<(), LoadError> {
        tracing::info!("building derived index tables in parallel from the base tables");
        let groups = schema::backfill_derived_index_groups(&self.cfg.database);
        run_parallel(groups, Self::BACKFILL_CONCURRENCY, |grp| {
            self.run_multiquery(&grp)
        })?;
        tracing::info!("recreating materialized views for live tracking");
        self.run_multiquery(&schema::materialized_views(&self.cfg.database))
    }

    /// Load every base entity (plus the optional witness table) using `mode`,
    /// parallelizing base-table loads/uploads and derived-index backfills across
    /// concurrent ClickHouse connections while materialized views are dropped.
    pub fn load_all_with(&self, parquet_dir: &Path, mode: LoadMode) -> Result<(), LoadError> {
        self.begin_streaming_load()?;

        let mut entities: Vec<&'static str> = schema::ENTITIES.to_vec();
        if parquet_dir.join(schema::WITNESS_ENTITY).is_dir() {
            tracing::info!("loading optional witness payload table");
            entities.push(schema::WITNESS_ENTITY);
        } else {
            tracing::info!("no witness payload output present; skipping witnesses table");
        }

        match mode {
            LoadMode::ServerFile => {
                run_parallel(entities, 6, |entity| {
                    tracing::info!(entity, "loading into ClickHouse");
                    self.load_entity(entity, parquet_dir)
                })?;
            }
            LoadMode::Upload => {
                // Flatten `(entity, part_path)` across all entities so a pool
                // of `UPLOAD_CONCURRENCY` streams stays saturated on the large
                // `inputs`/`outputs`/`spends` tables instead of serializing
                // one entity or one part file at a time.
                let mut tasks: Vec<(&'static str, PathBuf)> = Vec::new();
                for entity in entities {
                    for path in Self::collect_parquet_files(&parquet_dir.join(entity))? {
                        tasks.push((entity, path));
                    }
                }
                run_parallel(tasks, Self::UPLOAD_CONCURRENCY, |(entity, path)| {
                    self.upload_parquet_file(entity, &path)
                })?;
            }
        }

        self.finish_streaming_load()
    }

    /// Refresh one backfilled index. Only the *SQL-only* backfill indexes are
    /// valid names here; every other index is maintained automatically by its
    /// MV, and `address_prefix` is handled by [`rebuild_address_prefix`]
    /// because it needs a Rust-side address renderer.
    pub fn rebuild_index(&self, name: &str) -> Result<(), LoadError> {
        match name {
            "coinstats_by_height" => {
                self.run_multiquery(&schema::backfill_coinstats(&self.cfg.database))
            }
            "scripthash_map" => {
                self.run_multiquery(&schema::backfill_scripthash_map(&self.cfg.database))
            }
            // `address_prefix` needs base58/bech32 rendering, which ClickHouse
            // cannot do; see `rebuild_address_prefix`. Refuse it here rather
            // than silently truncating the table.
            other => Err(LoadError::UnknownIndex(other.to_string())),
        }
    }

    /// Rows buffered per streaming `RowBinary` flush when rebuilding
    /// `address_prefix`.
    pub const ADDRESS_PREFIX_BATCH: usize = 500;

    /// Rebuild the `address_prefix` index by streaming distinct
    /// `(script_type, address_hash)` rows line-by-line out of `outputs` in
    /// bounded \(O(1)\) memory and writing rendered rows in `RowBinary` format
    /// into a single persistent `clickhouse-client` insert pipe (avoiding both
    /// buffering ~68 GiB of TSV in RAM and spawning millions of client
    /// processes).
    pub fn rebuild_address_prefix<F>(&self, render: F) -> Result<usize, LoadError>
    where
        F: Fn(u8, &[u8; 32]) -> Option<String>,
    {
        self.rebuild_address_prefix_chunked(|chunk, rb_buf| {
            let mut n = 0usize;
            for &(ty, ref hash) in chunk {
                if let Some(addr) = render(ty, hash) {
                    rowbinary::address_prefix(&addr, ty, hash, rb_buf);
                    n += 1;
                }
            }
            n
        })
    }

    /// Like [`ChLoader::rebuild_address_prefix`], but renders each chunk of
    /// base58/bech32 addresses in parallel across Rayon worker threads.
    pub fn rebuild_address_prefix_par<F>(&self, render: F) -> Result<usize, LoadError>
    where
        F: Fn(u8, &[u8; 32]) -> Option<String> + Sync,
    {
        use rayon::prelude::*;
        let map_fn = |(ty, hash): &(u8, [u8; 32])| render(*ty, hash);
        let mut rendered: Vec<Option<String>> = Vec::with_capacity(Self::ADDRESS_PREFIX_BATCH);
        self.rebuild_address_prefix_chunked(|chunk, rb_buf| {
            chunk.par_iter().map(map_fn).collect_into_vec(&mut rendered);
            let mut n = 0usize;
            for ((ty, hash), maybe_addr) in chunk.iter().zip(rendered.drain(..)) {
                if let Some(addr) = maybe_addr {
                    rowbinary::address_prefix(&addr, *ty, hash, rb_buf);
                    n += 1;
                }
            }
            n
        })
    }

    fn spawn_address_prefix_sink(
        &self,
        db: &str,
    ) -> Result<(std::process::Child, std::process::ChildStdin), LoadError> {
        use std::process::{Command, Stdio};
        let mut args = self.base_args();
        args.push(format!(
            "--query=INSERT INTO {db}.address_prefix ({}) FORMAT RowBinary",
            rowbinary::ADDRESS_PREFIX_COLS
        ));
        let mut child = Command::new(&self.client)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        // coverage-exempt — `child` is spawned with `Stdio::piped()` for stdin, so `take()` always returns `Some`.
        let Some(stdin) = child.stdin.take() else {
            return Err(LoadError::Client(
                "could not open stdin for address_prefix insert".into(),
            ));
        };
        Ok((child, stdin))
    }

    fn flush_address_prefix_chunk<C>(
        &self,
        db: &str,
        chunk: &mut Vec<(u8, [u8; 32])>,
        rb_buf: &mut Vec<u8>,
        sink: &mut Option<(std::process::Child, std::process::ChildStdin)>,
        encode_chunk: &mut C,
    ) -> Result<usize, LoadError>
    where
        C: FnMut(&[(u8, [u8; 32])], &mut Vec<u8>) -> usize,
    {
        use std::io::Write;
        if chunk.is_empty() {
            return Ok(0);
        }
        rb_buf.clear();
        let count = encode_chunk(chunk, rb_buf);
        chunk.clear();
        if count == 0 {
            return Ok(0);
        }
        if sink.is_none() {
            *sink = Some(self.spawn_address_prefix_sink(db)?);
        }
        if let Some((_, ref mut stdin)) = sink {
            stdin
                .write_all(rb_buf)
                .map_err(|e| LoadError::Client(format!("write to address_prefix failed: {e}")))?;
        }
        Ok(count)
    }

    fn rebuild_address_prefix_chunked<C>(&self, mut encode_chunk: C) -> Result<usize, LoadError>
    where
        C: FnMut(&[(u8, [u8; 32])], &mut Vec<u8>) -> usize,
    {
        use std::io::{BufRead, BufReader};
        use std::process::{Child, ChildStdin, Command, Stdio};

        let db = self.cfg.database.clone();
        self.run_query(&format!("TRUNCATE TABLE {db}.address_prefix"))?;

        let mut src_args = self.base_args();
        src_args.push(format!("--query={}", schema::address_prefix_source(&db)));
        let mut src_child = Command::new(&self.client)
            .args(src_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        // coverage-exempt — `src_child` is spawned with `Stdio::piped()` for stdout, so `take()` always returns `Some`.
        let Some(stdout) = src_child.stdout.take() else {
            return Err(LoadError::Client(
                "could not open stdout for address_prefix source".into(),
            ));
        };

        let mut sink: Option<(Child, ChildStdin)> = None;
        let mut chunk: Vec<(u8, [u8; 32])> = Vec::with_capacity(Self::ADDRESS_PREFIX_BATCH);
        let mut rb_buf: Vec<u8> = Vec::with_capacity(Self::ADDRESS_PREFIX_BATCH * 80);
        let mut inserted = 0usize;

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let stream_res: Result<(), LoadError> = (|| {
            while reader.read_line(&mut line)? != 0 {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    let (ty_str, hash_hex) = trimmed.split_once('\t').ok_or_else(|| {
                        LoadError::Client(format!("malformed address_prefix row: {trimmed:?}"))
                    })?;
                    let ty: u8 = ty_str.trim().parse().map_err(|_| {
                        LoadError::Client(format!(
                            "bad script_type in address_prefix row: {trimmed:?}"
                        ))
                    })?;
                    let hash = parse_hash32(hash_hex.trim())?;
                    chunk.push((ty, hash));
                    if chunk.len() >= Self::ADDRESS_PREFIX_BATCH {
                        inserted += self.flush_address_prefix_chunk(
                            &db,
                            &mut chunk,
                            &mut rb_buf,
                            &mut sink,
                            &mut encode_chunk,
                        )?;
                    }
                }
                line.clear();
            }
            inserted += self.flush_address_prefix_chunk(
                &db,
                &mut chunk,
                &mut rb_buf,
                &mut sink,
                &mut encode_chunk,
            )?;
            Ok(())
        })();

        drop(reader);
        if let Err(err) = stream_res {
            let _ = src_child.kill();
            let _ = src_child.wait();
            if let Some((mut child, stdin)) = sink.take() {
                drop(stdin);
                let _ = child.kill();
                let _ = child.wait();
            }
            return Err(err);
        }

        let src_out = src_child.wait_with_output()?;
        if !src_out.status.success() {
            if let Some((mut child, stdin)) = sink.take() {
                drop(stdin);
                let _ = child.kill();
                let _ = child.wait();
            }
            return Err(LoadError::Client(
                String::from_utf8_lossy(&src_out.stderr).into_owned(),
            ));
        }

        if let Some((child, stdin)) = sink.take() {
            drop(stdin);
            let sink_out = child.wait_with_output()?;
            if !sink_out.status.success() {
                return Err(LoadError::Client(
                    String::from_utf8_lossy(&sink_out.stderr).into_owned(),
                ));
            }
        }
        Ok(inserted)
    }
}

/// Run `items` across up to `concurrency` scoped worker threads, stopping early
/// and returning the first error if any task fails.
fn run_parallel<T, F>(items: Vec<T>, concurrency: usize, f: F) -> Result<(), LoadError>
where
    T: Send,
    F: Fn(T) -> Result<(), LoadError> + Sync,
{
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    if items.is_empty() {
        return Ok(());
    }
    let workers = concurrency.max(1).min(items.len());
    let queue: Mutex<std::collections::VecDeque<T>> = Mutex::new(items.into());
    let failed = AtomicBool::new(false);
    let first_err: Mutex<Option<LoadError>> = Mutex::new(None);

    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| {
                while !failed.load(Ordering::Relaxed) {
                    let next = queue.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
                    let Some(item) = next else {
                        return;
                    };
                    if let Err(e) = f(item) {
                        failed.store(true, Ordering::Relaxed);
                        let mut slot = first_err.lock().unwrap_or_else(|p| p.into_inner());
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        return;
                    }
                }
            });
        }
    });

    match first_err.into_inner().unwrap_or_else(|p| p.into_inner()) {
        Some(e) => Err(e),
        None => Ok(()),
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

    /// `ChLoader::new` must resolve a real client and build the loader when one
    /// is available. The sandbox has no `clickhouse-client` on `PATH`, so point
    /// `BIDX_CLICKHOUSE_CLIENT` at a fake one and assert the success arm. Holds
    /// the fork guard because it mutates the process environment and spawns a
    /// child.
    #[test]
    fn new_loader_succeeds_with_a_resolvable_client() {
        let _g = fork_guard();
        let client = fake_client("new-ok", false);
        let saved = std::env::var_os("BIDX_CLICKHOUSE_CLIENT");
        std::env::set_var("BIDX_CLICKHOUSE_CLIENT", &client);
        let result = ChLoader::new(ChConfig::default());
        match saved {
            Some(v) => std::env::set_var("BIDX_CLICKHOUSE_CLIENT", v),
            None => std::env::remove_var("BIDX_CLICKHOUSE_CLIENT"),
        }
        let loader = result.expect("a resolvable client must construct the loader");
        assert_eq!(loader.client_path(), client);
        let _ = std::fs::remove_file(&client);
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
            secure: false,
        };
        let loader = ChLoader {
            cfg,
            client: PathBuf::from("/definitely/not/a/real/binary/clickhouse-client"),
        };
        let args = loader.base_args();
        assert_eq!(
            args,
            vec!["--host=h", "--port=9999", "--user=u", "--password=sekret",]
        );
        // TLS appends `--secure`; no password means no `--password` arg.
        let tls = ChConfig {
            secure: true,
            password: None,
            ..loader.cfg.clone()
        };
        assert_eq!(
            tls.client_args(),
            vec!["--host=h", "--port=9999", "--user=u", "--secure"]
        );
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
        let path = std::env::temp_dir().join(format!("bidx-fake-ch-{tag}-{}", std::process::id()));
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
        let tmp =
            std::env::temp_dir().join(format!("bidx-fake-ch-{tag}-{}-tmp", std::process::id()));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    fn loader_with(client: PathBuf) -> ChLoader {
        ChLoader {
            cfg: ChConfig {
                database: "db".into(),
                ..ChConfig::default()
            },
            client,
        }
    }

    /// Tests that spawn the fake client must not run concurrently: a fork
    /// snapshots every open fd, so a sibling thread mid-`fs::write` would leave
    /// its write fd inherited by the child, and `execve` of that inode then
    /// fails with ETXTBSY. Holding this guard for the whole test body removes
    /// the overlap.
    static FORK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    pub(crate) fn fork_guard() -> std::sync::MutexGuard<'static, ()> {
        FORK_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn with_client_uses_the_supplied_path_verbatim() {
        let p = PathBuf::from("/opt/bidx/clickhouse-client");
        let l = ChLoader::with_client(ChConfig::default(), p.clone());
        assert_eq!(l.client, p);
    }

    /// A fake client that logs every `--query=` argument to `<script>.log` and
    /// answers the witness-table probe with `1`. Used to assert the exact SQL a
    /// `retract_from` emits.
    fn logging_client(tag: &str, witness_exists: bool) -> PathBuf {
        let path = std::env::temp_dir().join(format!("bidx-log-ch-{tag}-{}", std::process::id()));
        let probe = if witness_exists { "1" } else { "0" };
        let body = format!(
            "#!/bin/sh\nq=''\nfor a in \"$@\"; do case \"$a\" in --query=*) q=\"$a\";; esac; done\n\
             echo \"$q\" >> \"$0.log\"\n\
             case \"$q\" in *system.tables*) echo '{probe}';; esac\nexit 0\n"
        );
        use std::os::unix::fs::PermissionsExt;
        let tmp =
            std::env::temp_dir().join(format!("bidx-log-ch-{tag}-{}-tmp", std::process::id()));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    fn read_log(script: &Path) -> String {
        std::fs::read_to_string(format!("{}.log", script.display())).unwrap_or_default()
    }

    /// A client that appends `<query>|<stdin bytes>` per invocation to
    /// `$0.log`, so an upload test can assert which file went to which table
    /// and in what order. `fail` makes every invocation exit 1.
    fn upload_client(tag: &str, fail: bool) -> PathBuf {
        let path = std::env::temp_dir().join(format!("bidx-up-ch-{tag}-{}", std::process::id()));
        let tail = if fail {
            "echo 'upload refused' >&2\nexit 1\n"
        } else {
            "exit 0\n"
        };
        let body = format!(
            "#!/bin/sh\nq=''\nfor a in \"$@\"; do case \"$a\" in --query=*) q=\"$a\";; esac; done\n\
             printf '%s|%s\\n' \"$q\" \"$(cat)\" >> \"$0.log\"\n{tail}"
        );
        use std::os::unix::fs::PermissionsExt;
        let tmp = std::env::temp_dir().join(format!("bidx-up-ch-{tag}-{}-tmp", std::process::id()));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    fn parquet_tree(tag: &str, with_witness: bool) -> PathBuf {
        let root = std::env::temp_dir().join(format!("bidx-up-tree-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        for entity in schema::ENTITIES {
            let d = root.join(entity);
            std::fs::create_dir_all(&d).unwrap();
            // Written out of order to prove the upload sorts by name.
            std::fs::write(d.join("part-00001.parquet"), format!("{entity}-1")).unwrap();
            std::fs::write(d.join("part-00000.parquet"), format!("{entity}-0")).unwrap();
            std::fs::write(d.join("README.txt"), "not parquet").unwrap();
        }
        if with_witness {
            let d = root.join(schema::WITNESS_ENTITY);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("part-00000.parquet"), "wit-0").unwrap();
        }
        root
    }

    #[test]
    fn upload_entity_streams_each_parquet_file_in_name_order() {
        let _g = fork_guard();
        let client = upload_client("order", false);
        let loader = loader_with(client.clone());
        let root = parquet_tree("order", false);
        assert_eq!(loader.upload_entity("blocks", &root).unwrap(), 2);
        let log = read_log(&client);
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(
            lines,
            vec![
                "--query=INSERT INTO db.blocks FORMAT Parquet|blocks-0",
                "--query=INSERT INTO db.blocks FORMAT Parquet|blocks-1",
            ],
            "non-parquet files must be skipped and parts sent in order"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn upload_entity_handles_missing_dir_non_dir_and_client_failure() {
        let _g = fork_guard();
        let client = upload_client("edge", false);
        let loader = loader_with(client.clone());
        let root = std::env::temp_dir().join(format!("bidx-up-edge-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // Missing entity dir → nothing to upload, not an error.
        assert_eq!(loader.upload_entity("blocks", &root).unwrap(), 0);
        assert!(
            read_log(&client).is_empty(),
            "no client call for an absent dir"
        );
        // A *file* where the entity dir should be is a real I/O error.
        std::fs::write(root.join("blocks"), "x").unwrap();
        assert!(matches!(
            loader.upload_entity("blocks", &root),
            Err(LoadError::Io(_))
        ));
        let _ = std::fs::remove_file(&client);

        // A refusing server surfaces the file path and stderr.
        let bad = upload_client("refuse", true);
        let loader = loader_with(bad.clone());
        let tree = parquet_tree("refuse", false);
        match loader.upload_entity("outputs", &tree) {
            Err(LoadError::Client(m)) => {
                assert!(
                    m.contains("part-00000.parquet") && m.contains("upload refused"),
                    "{m}"
                )
            }
            other => panic!("expected Client error, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&tree);
        let _ = std::fs::remove_file(format!("{}.log", bad.display()));
        let _ = std::fs::remove_file(&bad);
    }

    #[test]
    fn load_all_with_upload_sends_every_entity_and_the_witness_table() {
        let _g = fork_guard();
        let client = upload_client("all", false);
        let loader = loader_with(client.clone());
        let root = parquet_tree("all", true);
        loader.load_all_with(&root, LoadMode::Upload).unwrap();
        let log = read_log(&client);
        for entity in schema::ENTITIES {
            assert!(
                log.contains(&format!(
                    "INSERT INTO db.{entity} FORMAT Parquet|{entity}-0"
                )),
                "{log}"
            );
        }
        assert!(
            log.contains("INSERT INTO db.witnesses FORMAT Parquet|wit-0"),
            "{log}"
        );
        // The deferred derived-index build brackets the load: views dropped
        // first, recreated last.
        assert!(
            log.contains("DROP VIEW IF EXISTS db.outputs_by_address_mv"),
            "{log}"
        );
        assert!(
            log.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS db.outputs_by_address_mv"),
            "{log}"
        );
        // ServerFile never streams stdin: every base entity is loaded via a
        // file() table function and nothing is sent as a Parquet upload.
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        loader.load_all_with(&root, LoadMode::ServerFile).unwrap();
        let log = read_log(&client);
        for entity in schema::ENTITIES {
            assert!(
                log.contains(&format!("INSERT INTO db.{entity} SELECT * FROM file(")),
                "ServerFile must load {entity} via file(): {log}"
            );
        }
        assert!(
            !log.contains("FORMAT Parquet"),
            "ServerFile must not stream Parquet over stdin: {log}"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    /// The deferred build must bracket the base load in the right order:
    /// drop views → load base entities → backfill derived → recreate views.
    /// A load with the views still live would fan every row out, defeating the
    /// whole point, so the ordering is asserted on the real client log.
    #[test]
    fn load_all_defers_derived_indexes_in_order() {
        let _g = fork_guard();
        let client = logging_client("defer-order", false);
        let loader = loader_with(client.clone());
        let root = parquet_tree("defer-order", false);
        loader.load_all_with(&root, LoadMode::ServerFile).unwrap();
        let log = read_log(&client);

        let at = |needle: &str| {
            log.find(needle)
                .unwrap_or_else(|| panic!("missing {needle}: {log}"))
        };
        let drop_at = at("DROP VIEW IF EXISTS db.scripthash_map_mv");
        let load_at = at("INSERT INTO db.outputs SELECT * FROM file(");
        let backfill_at = at("INSERT INTO db.outputs_by_address ");
        let recreate_at = at("CREATE MATERIALIZED VIEW IF NOT EXISTS db.scripthash_map_mv");
        assert!(drop_at < load_at, "views must be dropped before the load");
        assert!(
            load_at < backfill_at,
            "derived tables must be built after the base load"
        );
        assert!(
            backfill_at < recreate_at,
            "views must be recreated after the backfill"
        );
        // Every MV is dropped and recreated, and every backfill target is hit.
        for name in schema::materialized_view_names() {
            assert!(
                log.contains(&format!("DROP VIEW IF EXISTS db.{name}")),
                "{name}"
            );
            assert!(
                log.contains(&format!("CREATE MATERIALIZED VIEW IF NOT EXISTS db.{name}")),
                "{name}"
            );
        }
        for stmt in schema::backfill_derived_indexes("db") {
            if let Some(rest) = stmt.strip_prefix("INSERT INTO db.") {
                let table = rest.split(' ').next().unwrap();
                assert!(
                    log.contains(&format!("TRUNCATE TABLE db.{table}")),
                    "{table}"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    /// A reorg floor in the middle of a 10,000-block partition cannot drop the
    /// partition (that would take canonical rows below the floor too), so every
    /// base table must get a `DELETE ... WHERE height >= floor`.
    #[test]
    fn retract_from_uses_delete_for_a_mid_partition_height() {
        let _g = fork_guard();
        let client = logging_client("retract-mid", false);
        let loader = loader_with(client.clone());
        loader.retract_from(12_345).unwrap();
        let log = read_log(&client);
        for table in schema::ENTITIES {
            assert!(
                log.contains(&format!("DELETE FROM db.{table} WHERE height >= 12345")),
                "missing delete for {table}: {log}"
            );
        }
        // No partition drops, and the absent witness table is never touched.
        assert!(!log.contains("DROP PARTITION"));
        assert!(!log.contains("db.witnesses"));
        let _ = std::fs::remove_file(&client);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
    }

    /// A floor exactly on a 10,000 boundary lets every base table drop the
    /// whole partition (height / 10000), which is far cheaper than a mutation.
    #[test]
    fn retract_from_drops_partition_when_height_is_aligned() {
        let _g = fork_guard();
        let client = logging_client("retract-aligned", true);
        let loader = loader_with(client.clone());
        loader.retract_from(30_000).unwrap();
        let log = read_log(&client);
        for table in schema::ENTITIES {
            assert!(
                log.contains(&format!("ALTER TABLE db.{table} DROP PARTITION 3")),
                "missing partition drop for {table}: {log}"
            );
        }
        // The witness table exists (probe returned 1) and is deleted by height.
        assert!(
            log.contains("DELETE FROM db.witnesses WHERE height >= 30000"),
            "witness retraction missing: {log}"
        );
        let _ = std::fs::remove_file(&client);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
    }

    #[test]
    fn table_exists_probe_is_a_system_tables_count() {
        let sql = schema::table_exists("db", "witnesses");
        assert!(sql.contains("count() > 0"));
        assert!(sql.contains("database = 'db'"));
        assert!(sql.contains("name = 'witnesses'"));
    }

    #[test]
    fn client_path_exposes_the_resolved_binary() {
        let p = PathBuf::from("/opt/bidx/clickhouse-client");
        let l = ChLoader::with_client(ChConfig::default(), p.clone());
        assert_eq!(l.client_path(), p);
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
        assert_eq!(
            applied,
            vec![2],
            "v2 address_stats rebuild is pending at v0"
        );
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
            assert!(
                v > 0 && v <= schema::SCHEMA_VERSION,
                "migration {v} out of range"
            );
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
    fn load_all_loads_optional_witnesses_when_dir_present() {
        let _g = fork_guard();
        let client = fake_client("loadall-witness", false);
        let loader = loader_with(client.clone());
        let tmp = std::env::temp_dir().join(format!("bidx-loadall-wit-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join(schema::WITNESS_ENTITY)).unwrap();
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
        loader
            .rebuild_index("coinstats_by_height")
            .expect("coinstats ok");
        loader
            .rebuild_index("scripthash_map")
            .expect("scripthash_map ok");
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
        let path =
            std::env::temp_dir().join(format!("bidx-fake-prefix-{tag}-{}", std::process::id()));
        let body = if fail {
            "#!/bin/sh\necho 'simulated failure' >&2\nexit 1\n".to_string()
        } else {
            // Emit the fixed rows for the source SELECT; echo the query for
            // anything else (so TRUNCATE/INSERT calls succeed).
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do case \"$a\" in --query=*DISTINCT*script_type*) printf '%s' '{rows}'; exit 0;; esac; done\n\
                 for a in \"$@\"; do case \"$a\" in --query=*FORMAT*RowBinary*) cat >/dev/null; exit 0;; esac; done\n\
                 for a in \"$@\"; do case \"$a\" in --query=*) echo \"$a\";; esac; done\n\
                 exit 0\n"
            )
        };
        use std::os::unix::fs::PermissionsExt;
        let tmp =
            std::env::temp_dir().join(format!("bidx-fake-prefix-{tag}-{}-tmp", std::process::id()));
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
        assert_eq!(
            loader
                .rebuild_address_prefix(|_, _| Some("x".into()))
                .unwrap(),
            0
        );
        let _ = std::fs::remove_file(&client);

        // An internal blank line (survives the outer trim) is skipped.
        let body = format!("2\t{}\n\n4\t{}\n", "ab".repeat(32), "cd".repeat(32));
        let client = fake_prefix_client("blank", &body, false);
        let loader = loader_with(client.clone());
        assert_eq!(
            loader
                .rebuild_address_prefix(|_, _| Some("x".into()))
                .unwrap(),
            2
        );
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
        let n = loader
            .rebuild_address_prefix(|_, _| Some("1x".into()))
            .unwrap();
        assert_eq!(n, ChLoader::ADDRESS_PREFIX_BATCH + 1);
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn parse_hash32_validates_length_and_hex() {
        assert_eq!(parse_hash32(&"0f".repeat(32)).unwrap()[0], 0x0f);
        assert!(matches!(parse_hash32("abcd"), Err(LoadError::Client(_))));
        assert!(matches!(
            parse_hash32(&"zz".repeat(32)),
            Err(LoadError::Client(_))
        ));
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

    #[test]
    fn run_parallel_handles_empty_success_and_concurrent_errors() {
        run_parallel(Vec::<usize>::new(), 4, |_| Ok(())).unwrap();

        let sum = std::sync::atomic::AtomicUsize::new(0);
        run_parallel((1usize..=8).collect(), 3, |v| {
            sum.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
        assert_eq!(sum.load(std::sync::atomic::Ordering::Relaxed), 36);

        // Concurrent failure path: workers 0 and 1 fail simultaneously (racing
        // `slot.is_none()`) while worker 2 finishes `Ok(())` afterward and
        // observes `!failed.load(...) == false` on the next loop iteration.
        let barrier = std::sync::Barrier::new(3);
        let err = run_parallel((0usize..16).collect(), 3, |i| {
            if i < 3 {
                barrier.wait();
            }
            if i == 2 {
                std::thread::sleep(std::time::Duration::from_millis(5));
                return Ok(());
            }
            Err(LoadError::Client(format!("boom-{i}")))
        })
        .unwrap_err();
        assert!(matches!(err, LoadError::Client(m) if m.starts_with("boom-")));
    }

    #[test]
    fn load_part_streams_or_loads_server_files_and_optionally_unlinks() {
        let _g = fork_guard();
        let client = upload_client("part-stream", false);
        let loader = loader_with(client.clone());
        let root = parquet_tree("part-stream", true);

        // ServerFile without unlinking keeps part-00000.parquet intact.
        loader
            .load_part(&root, 0, true, LoadMode::ServerFile, false)
            .unwrap();
        assert!(root.join("blocks/part-00000.parquet").exists());
        assert!(root.join("witnesses/part-00000.parquet").exists());

        // Upload with `unlink_after = true` deletes every loaded part-00000
        // file while leaving part-00001 untouched.
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        loader
            .load_part(&root, 0, true, LoadMode::Upload, true)
            .unwrap();
        let log = read_log(&client);
        for entity in schema::ENTITIES {
            assert!(
                log.contains(&format!(
                    "INSERT INTO db.{entity} FORMAT Parquet|{entity}-0"
                )),
                "{log}"
            );
            assert!(!root.join(entity).join("part-00000.parquet").exists());
            assert!(root.join(entity).join("part-00001.parquet").exists());
        }
        assert!(log.contains("INSERT INTO db.witnesses FORMAT Parquet|wit-0"));
        assert!(!root.join("witnesses/part-00000.parquet").exists());

        // Also cover `capture_witness = false` on part 1.
        loader
            .load_part(&root, 1, false, LoadMode::ServerFile, true)
            .unwrap();
        assert!(!root.join("blocks/part-00001.parquet").exists());

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn load_all_with_upload_handles_empty_parquet_dir_and_bad_entity_entry() {
        let _g = fork_guard();
        let client = upload_client("empty-all", false);
        let loader = loader_with(client.clone());
        let empty = std::env::temp_dir().join(format!("bidx-up-empty-all-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&empty);
        std::fs::create_dir_all(&empty).unwrap();
        loader.load_all_with(&empty, LoadMode::Upload).unwrap();

        // A file where an entity dir should be propagates `LoadError::Io`.
        std::fs::write(empty.join("blocks"), "not-a-dir").unwrap();
        assert!(matches!(
            loader.load_all_with(&empty, LoadMode::Upload),
            Err(LoadError::Io(_))
        ));
        let _ = std::fs::remove_dir_all(&empty);
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn rebuild_address_prefix_par_and_streaming_error_paths() {
        let _g = fork_guard();
        use std::os::unix::fs::PermissionsExt;

        let row_ok = format!("2\t{}\n", "ab".repeat(32));
        let row_skip = format!("7\t{}\n", "00".repeat(32));
        let mut body = row_ok.repeat(ChLoader::ADDRESS_PREFIX_BATCH + 3);
        body.push_str(&row_skip);

        let client = fake_prefix_client("par-ok", &body, false);
        let loader = loader_with(client.clone());
        let n = loader
            .rebuild_address_prefix_par(|ty, hash| match ty {
                2 => Some(format!("1par{}", hash[0])),
                _ => None,
            })
            .unwrap();
        assert_eq!(n, ChLoader::ADDRESS_PREFIX_BATCH + 3);

        // Chunk where every row renders to `None` must not spawn an INSERT.
        let none_n = loader.rebuild_address_prefix_par(|_, _| None).unwrap();
        assert_eq!(none_n, 0);
        let _ = std::fs::remove_file(&client);

        let write_script = |tag: &str, script: String| -> PathBuf {
            let p =
                std::env::temp_dir().join(format!("bidx-prefix-err-{tag}-{}", std::process::id()));
            let tmp = std::env::temp_dir()
                .join(format!("bidx-prefix-err-{tag}-{}-tmp", std::process::id()));
            std::fs::write(&tmp, script).unwrap();
            let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&tmp, perms).unwrap();
            std::fs::rename(&tmp, &p).unwrap();
            p
        };

        // 1) Mid-stream malformed row AFTER batch 1 already spawned `sink`.
        let mid_bad_rows = format!("{}garbage\n", row_ok.repeat(ChLoader::ADDRESS_PREFIX_BATCH));
        let mid_bad = fake_prefix_client("mid-bad", &mid_bad_rows, false);
        assert!(matches!(
            loader_with(mid_bad.clone()).rebuild_address_prefix(|_, _| Some("1x".into())),
            Err(LoadError::Client(m)) if m.contains("malformed")
        ));
        let _ = std::fs::remove_file(&mid_bad);

        // 2) Source query exits non-zero AFTER batch 1 already spawned `sink`.
        let src_fail = write_script(
            "src-fail",
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do case \"$a\" in --query=*DISTINCT*script_type*) printf '%s' '{}'; echo 'src died' >&2; exit 1;; esac; done\n\
                 for a in \"$@\"; do case \"$a\" in --query=*FORMAT*RowBinary*) cat >/dev/null; exit 0;; esac; done\n\
                 exit 0\n",
                row_ok.repeat(ChLoader::ADDRESS_PREFIX_BATCH)
            ),
        );
        assert!(matches!(
            loader_with(src_fail.clone()).rebuild_address_prefix(|_, _| Some("1x".into())),
            Err(LoadError::Client(m)) if m.contains("src died")
        ));
        let _ = std::fs::remove_file(&src_fail);

        // 3) Sink `FORMAT RowBinary` exits non-zero at EOF.
        let sink_fail = write_script(
            "sink-fail",
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do case \"$a\" in --query=*DISTINCT*script_type*) printf '%s' '{row_ok}'; exit 0;; esac; done\n\
                 for a in \"$@\"; do case \"$a\" in --query=*FORMAT*RowBinary*) cat >/dev/null; echo 'sink died' >&2; exit 1;; esac; done\n\
                 exit 0\n"
            ),
        );
        assert!(matches!(
            loader_with(sink_fail.clone()).rebuild_address_prefix(|_, _| Some("1x".into())),
            Err(LoadError::Client(m)) if m.contains("sink died")
        ));
        let _ = std::fs::remove_file(&sink_fail);

        // 4) Sink closes stdin immediately while >64 KiB of batches stream in
        //    (`BrokenPipe` on `stdin.write_all`).
        let long_addr = "1".repeat(256);
        let many_rows = row_ok.repeat(ChLoader::ADDRESS_PREFIX_BATCH * 3);
        let pipe_fail = write_script(
            "pipe-fail",
            format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do case \"$a\" in --query=*DISTINCT*script_type*) printf '%s' '{many_rows}'; exit 0;; esac; done\n\
                 for a in \"$@\"; do case \"$a\" in --query=*FORMAT*RowBinary*) exit 1;; esac; done\n\
                 exit 0\n"
            ),
        );
        assert!(matches!(
            loader_with(pipe_fail.clone()).rebuild_address_prefix(|_, _| Some(long_addr.clone())),
            Err(LoadError::Client(_))
        ));
        let _ = std::fs::remove_file(&pipe_fail);
    }
}
