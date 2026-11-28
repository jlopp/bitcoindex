use anyhow::{Context, Result};
use bench::{run_bench, BenchConfig};
use bidx_core::Network;
use clap::{Parser, Subcommand};

/// CLI-facing derived-index profile (mirrors `bidx_loader::schema::IndexProfile`).
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum, PartialEq, Eq)]
pub enum IndexProfileArg {
    /// Materialize only the indexes consulted by `bidx serve` (default).
    /// Skips `inputs_by_prev_output`, `outputs_by_address`, and `spends_by_address`,
    /// saving ~565 GB of ClickHouse MergeTree storage on mainnet.
    #[default]
    Serving,
    /// Materialize all 19 historical derived index tables.
    Full,
}

impl From<IndexProfileArg> for bidx_loader::schema::IndexProfile {
    fn from(a: IndexProfileArg) -> Self {
        match a {
            IndexProfileArg::Serving => bidx_loader::schema::IndexProfile::Serving,
            IndexProfileArg::Full => bidx_loader::schema::IndexProfile::Full,
        }
    }
}

/// CLI-facing network enum (mirrors bidx_core::Network but is clap-typed).
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum NetworkArg {
    Mainnet,
    Testnet3,
    Testnet4,
    Signet,
    Regtest,
}

impl From<NetworkArg> for Network {
    fn from(a: NetworkArg) -> Self {
        match a {
            NetworkArg::Mainnet => Network::Mainnet,
            NetworkArg::Testnet3 => Network::Testnet3,
            NetworkArg::Testnet4 => Network::Testnet4,
            NetworkArg::Signet => Network::Signet,
            NetworkArg::Regtest => Network::Regtest,
        }
    }
}
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

/// ClickHouse connection flags shared by every command that talks to the
/// server through `clickhouse-client`. Each flag falls back to the same
/// `BIDX_CLICKHOUSE__*` variable `bidx serve` reads, so one set of env vars
/// (e.g. a Kubernetes Secret) configures every command.
#[derive(Debug, Clone, clap::Args)]
pub struct ChArgs {
    #[arg(long, env = "BIDX_CLICKHOUSE__HOST", default_value = "localhost")]
    host: String,
    /// Native protocol port (9000 plain, 9440 TLS).
    #[arg(long, env = "BIDX_CLICKHOUSE__PORT", default_value_t = 9000)]
    port: u16,
    #[arg(long, env = "BIDX_CLICKHOUSE__USER", default_value = "default")]
    user: String,
    #[arg(long, env = "BIDX_CLICKHOUSE__PASSWORD", hide_env_values = true)]
    password: Option<String>,
    #[arg(long, env = "BIDX_CLICKHOUSE__DATABASE", default_value = "bitcoin")]
    database: String,
    /// Connect over TLS (required by ClickHouse Cloud; use port 9440).
    #[arg(long, env = "BIDX_CLICKHOUSE__SECURE")]
    secure: bool,
}

impl From<ChArgs> for bidx_loader::ChConfig {
    fn from(a: ChArgs) -> Self {
        bidx_loader::ChConfig {
            host: a.host,
            port: a.port,
            user: a.user,
            password: a.password,
            database: a.database,
            secure: a.secure,
        }
    }
}

mod bench;
mod diskguard;
mod live;
mod pipeline;
mod serve;
mod verify;

/// Test-only helpers shared across this crate's unit tests.
#[cfg(test)]
pub mod testlock {
    /// Tests that spawn a child process (`clickhouse-client`, the fake client,
    /// the live/bench paths) fork the whole test process. A concurrent thread
    /// mid-`fs::write` of an executable would have its write fd inherited by
    /// the child, and `execve` of that inode then fails with ETXTBSY. Holding
    /// this process-wide guard for the body of any test that spawns (or writes
    /// an executable script) keeps the fork and the writes from overlapping.
    static FORK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    pub fn fork_guard() -> std::sync::MutexGuard<'static, ()> {
        FORK_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[derive(Parser)]
#[command(
    name = "bidx",
    version,
    about = "Bitcoin blockchain indexer: blk*.dat -> Parquet -> ClickHouse"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,

    /// Log filter (e.g. "info", "bidx=debug").
    #[arg(long, global = true, default_value = "info")]
    log: String,
}

#[derive(Subcommand)]
enum Cmd {
    /// Header pass only: scan blk files, reconstruct the main chain, print stats.
    Headers {
        /// Bitcoin Core blocks directory (contains blkNNNNN.dat).
        #[arg(long)]
        blocks_dir: PathBuf,
    },

    /// Full ingest: parse blocks, link spends via UTXO store, write Parquet (or stream directly to ClickHouse).
    Parse {
        #[arg(long)]
        blocks_dir: PathBuf,
        /// Output directory for Parquet part files (optional when `--direct-clickhouse` is set).
        #[arg(long)]
        out: Option<PathBuf>,
        /// RocksDB UTXO store path (created if missing).
        #[arg(long)]
        utxo: PathBuf,
        /// Start height (inclusive). Default 0 (genesis).
        #[arg(long, default_value_t = 0)]
        start: u32,
        /// End height (exclusive). Default: chain tip.
        #[arg(long)]
        end: Option<u32>,
        /// Blocks per Parquet part file.
        #[arg(long, default_value_t = 10_000)]
        blocks_per_part: u32,
        /// Parser worker threads. Default: num_cpus.
        #[arg(long)]
        threads: Option<usize>,

        /// If given, run the disk-space pre-flight check: query the node's
        /// tip over RPC `getblockcount`, project the index's final size from
        /// the recorded checkpoints, and refuse to start if we'd leave <10 GiB
        /// free on the index filesystem at completion.
        #[arg(long)]
        rpc_url: Option<String>,

        /// Cookie file for the RPC call (used with --rpc-url when no
        /// user/password are needed separately). Defaults to
        /// ~/.bitcoin/.cookie for mainnet.
        #[arg(long)]
        rpc_cookie: Option<PathBuf>,

        /// Skip the disk-space startup check entirely.
        #[arg(long)]
        skip_disk_check: bool,

        /// Chain network override (auto-detected from the first blk file when
        /// absent: mainnet / testnet3 / testnet4 / signet / regtest).
        #[arg(long, value_enum)]
        network: Option<NetworkArg>,

        /// Override the disk-check checkpoint file path. Defaults to
        /// `<parent-of-utxo>/bidx-disk-checkpoints.txt`.
        #[arg(long)]
        checkpoint_file: Option<PathBuf>,

        /// Also capture raw witness stack elements into an optional
        /// `witnesses` Parquet table (and, when loaded, ClickHouse table).
        /// Off by default: the payload roughly doubles on-disk size and no hot
        /// table needs it.
        #[arg(long)]
        capture_witness: bool,

        /// RocksDB block-cache size in MiB. Default: auto-sized from the
        /// host's available memory. A larger cache is the biggest single lever
        /// on bulk-index throughput, since every spent input is a point lookup
        /// that otherwise reads a data block off disk.
        #[arg(long)]
        block_cache_mb: Option<usize>,

        /// In-memory recent-UTXO cache size in MiB. Default: auto-sized from
        /// the host's available memory. Answers spends of recently created
        /// outputs without a RocksDB lookup. Ignored by `live` (undo) mode.
        #[arg(long)]
        utxo_cache_mb: Option<usize>,

        /// Stream completed Parquet part files into ClickHouse in the
        /// background while parsing continues, then build derived indexes and
        /// materialized views at completion (eliminating a separate `bidx load`
        /// pass).
        #[arg(long)]
        stream_clickhouse: bool,

        /// Bypass Parquet files completely: stream enriched rows directly into
        /// ClickHouse over persistent 8 MiB-buffered `RowBinary` pipes, then
        /// export the live UTXO snapshot and build derived indexes at completion.
        #[arg(long)]
        direct_clickhouse: bool,

        /// Derived-index profile to materialize in ClickHouse (`serving` by
        /// default; `full` includes unreferenced analytical index tables).
        #[arg(long, value_enum, default_value_t = IndexProfileArg::Serving)]
        index_profile: IndexProfileArg,

        /// When `--stream-clickhouse` is set, stream Parquet part files over
        /// `clickhouse-client` stdin instead of server-side `file(...)`.
        #[arg(long, env = "BIDX_LOAD__UPLOAD")]
        upload: bool,

        /// When `--stream-clickhouse` is set, delete each Parquet part file as
        /// soon as ClickHouse confirms ingestion so bulk loads do not retain a
        /// second copy of the chain on disk.
        #[arg(long)]
        unlink_loaded_parts: bool,

        #[command(flatten)]
        ch: ChArgs,
    },

    /// Create ClickHouse schema (idempotent).
    InitDb {
        #[command(flatten)]
        ch: ChArgs,
    },

    /// Bulk-load Parquet files into ClickHouse.
    Load {
        /// Parquet output directory from `parse`.
        #[arg(long)]
        input: PathBuf,
        /// Stream the Parquet from this machine over the client connection
        /// instead of having the server read it from its own `user_files`.
        /// Required for ClickHouse Cloud or any server that cannot see the
        /// output directory.
        #[arg(long, env = "BIDX_LOAD__UPLOAD")]
        upload: bool,
        /// Derived-index profile to materialize (`serving` by default; `full`
        /// includes unreferenced analytical index tables).
        #[arg(long, value_enum, default_value_t = IndexProfileArg::Serving)]
        index_profile: IndexProfileArg,
        /// Chain network used to populate `address_prefix` and `scripthash_map`
        /// in a single fused binary pass after loading.
        #[arg(long, value_enum, default_value = "mainnet")]
        network: NetworkArg,
        #[command(flatten)]
        ch: ChArgs,
    },

    /// Track the chain tip live: reconcile with the node, apply new blocks,
    /// and handle reorgs via the UTXO undo log. Optionally appends to Parquet.
    Live {
        /// Bitcoin Core RPC URL.
        #[arg(
            long,
            env = "BIDX_NODE__RPC_URL",
            default_value = "http://127.0.0.1:8332"
        )]
        rpc_url: String,
        /// Path to the node's .cookie file (used if user/password not given).
        #[arg(long, env = "BIDX_NODE__COOKIE")]
        cookie: Option<PathBuf>,
        #[arg(long, env = "BIDX_NODE__USER")]
        rpc_user: Option<String>,
        #[arg(long, env = "BIDX_NODE__PASSWORD", hide_env_values = true)]
        rpc_password: Option<String>,
        /// ZMQ hashblock endpoint, e.g. tcp://127.0.0.1:28332. If omitted,
        /// falls back to a 2s RPC poll loop.
        #[arg(long, env = "BIDX_LIVE__ZMQ")]
        zmq: Option<String>,
        /// RocksDB UTXO store path (must match the one used by `parse`).
        #[arg(long, env = "BIDX_LIVE__UTXO")]
        utxo: PathBuf,
        /// Optional Parquet output directory; if set, live blocks append to
        /// rolling part files in the same layout as `parse`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Max reorg depth to auto-handle before aborting.
        #[arg(long, default_value_t = 100)]
        max_reorg_depth: u32,

        /// Bitcoin Core blocks dir (used only to auto-detect the network for
        /// the disk-space checkpoint section). If omitted, defaults to mainnet.
        #[arg(long)]
        blocks_dir: Option<PathBuf>,
        /// Chain network override (auto-detected from --blocks-dir if not set).
        #[arg(long, value_enum)]
        network: Option<NetworkArg>,
        /// Skip the disk-space startup check.
        #[arg(long)]
        skip_disk_check: bool,
        /// Override the disk-check checkpoint file path.
        #[arg(long)]
        checkpoint_file: Option<PathBuf>,
        /// Also append raw witness stack elements to the optional
        /// `witnesses` Parquet table (requires `--out`). Off by default.
        #[arg(long)]
        capture_witness: bool,

        /// Insert each applied block directly into ClickHouse over the client's
        /// stdin, bypassing Parquet, so the tip is queryable within the block's
        /// own apply time. Mutually exclusive with `--out`.
        #[arg(long, env = "BIDX_LIVE__CH_LIVE")]
        ch_live: bool,
        #[arg(long, env = "BIDX_CLICKHOUSE__HOST", default_value = "localhost")]
        ch_host: String,
        #[arg(long, env = "BIDX_CLICKHOUSE__PORT", default_value_t = 9000)]
        ch_port: u16,
        #[arg(long, env = "BIDX_CLICKHOUSE__USER", default_value = "default")]
        ch_user: String,
        #[arg(long, env = "BIDX_CLICKHOUSE__PASSWORD", hide_env_values = true)]
        ch_password: Option<String>,
        #[arg(long, env = "BIDX_CLICKHOUSE__DATABASE", default_value = "bitcoin")]
        ch_database: String,
        /// Connect the direct-insert pipes over TLS.
        #[arg(long, env = "BIDX_CLICKHOUSE__SECURE")]
        ch_secure: bool,
    },

    /// Parse a single block at a height and print it as JSON (debug).
    Inspect {
        #[arg(long)]
        blocks_dir: PathBuf,
        #[arg(long)]
        height: u32,
    },

    /// Inspect and maintain the query-index catalogue.
    Index {
        #[command(subcommand)]
        action: IndexAction,
    },

    /// Print the ClickHouse config fragment that registers the `bidx-udf`
    /// executable UDFs (`bidxAddress` / `bidxParseAddress`) for rendering
    /// addresses from ad-hoc SQL. Drop the output into the server's
    /// `config.d/`.
    UdfConfig {
        /// Path to an installed `bidx-udf` binary.
        #[arg(long, default_value = "bidx-udf")]
        udf_bin: String,
        /// Chain network the UDFs should render for.
        #[arg(long, value_enum, default_value = "mainnet")]
        network: NetworkArg,
    },

    /// Serve the query API: Esplora-compatible REST, Electrum JSON-RPC, and
    /// Prometheus metrics / health endpoints. Configuration comes from a TOML
    /// file plus `BIDX_*` environment overrides.
    Serve {
        /// Path to a TOML config file. Without one, defaults + env are used.
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Verify index integrity: structural invariants (unique heights, one
    /// coinbase per block, spend linking) plus optional agreement with a
    /// Bitcoin Core node tip. Exits non-zero when any hard check fails.
    Verify {
        #[command(flatten)]
        ch: ChArgs,
        /// Bitcoin Core RPC URL for the node-agreement check.
        #[arg(long)]
        rpc_url: Option<String>,
        /// Node `.cookie` file (used when --rpc-user/--rpc-password are absent).
        #[arg(long)]
        rpc_cookie: Option<PathBuf>,
        #[arg(long)]
        rpc_user: Option<String>,
        #[arg(long)]
        rpc_password: Option<String>,
    },

    /// Apply pending schema migrations and report the schema version.
    Migrate {
        #[command(flatten)]
        ch: ChArgs,
    },

    /// Benchmark the ingest pipeline: run bidx against a blocks directory
    /// and report per-stage timing (header pass, parallel parse, sequential
    /// UTXO apply, Parquet sink) plus wall-clock throughput, CPU utilisation,
    /// and peak RSS. Use mode=headers/parse/utxo/sink to isolate stages.
    Bench(BenchConfig),
}

#[derive(Subcommand)]
enum IndexAction {
    /// List the query-index catalogue (name, mechanism, target table).
    List,
    /// Rebuild a backfilled index. Only `coinstats_by_height`, `scripthash_map`
    /// and `address_prefix` are backfills; every other index is maintained
    /// automatically by its materialized view.
    Rebuild {
        /// Index name (see `bidx index list`).
        name: String,
        /// Chain network, used to render `address_prefix` addresses with the
        /// right base58/bech32 prefix. Ignored by the SQL-only backfills.
        #[arg(long, value_enum, default_value = "mainnet")]
        network: NetworkArg,
        #[command(flatten)]
        ch: ChArgs,
    },
}

// Untested by design (coverage-exempt): `main` is the process entrypoint. It
// parses the real argv and installs the global tracing subscriber exactly once,
// so it cannot run inside a unit-test binary (that would conflict with the test
// harness's own argv and the already-installed subscriber). All logic it
// delegates to is in `run`, which is fully covered; the only uncovered lines
// here are these five glue lines.
// coverage-exempt
fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cli.log))
        .with_target(false)
        .init();
    run(cli)
}

/// Dispatch one parsed CLI invocation. Split from `main` so the command arms
/// are reachable from unit tests (which cannot call `main`, since it parses the
/// process argv and installs a global tracing subscriber).
fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Headers { blocks_dir } => {
            let (index, stats) =
                bidx_parser::build_chain_index(&blocks_dir).context("header pass failed")?;
            println!(
                "scanned {} files, {} records, {} unique blocks",
                stats.files_scanned, stats.total_records, stats.unique_blocks
            );
            println!(
                "main chain: height {} tip {}",
                stats.tip_height, stats.tip_hash
            );
            println!("orphans/stale: {}", stats.orphans);
            println!("chain index entries: {}", index.by_height.len());
        }

        Cmd::Parse {
            blocks_dir,
            out,
            utxo,
            start,
            end,
            blocks_per_part,
            threads,
            rpc_url,
            rpc_cookie,
            skip_disk_check,
            network,
            checkpoint_file,
            capture_witness,
            block_cache_mb,
            utxo_cache_mb,
            stream_clickhouse,
            direct_clickhouse,
            index_profile,
            upload,
            unlink_loaded_parts,
            ch,
        } => {
            if stream_clickhouse && direct_clickhouse {
                anyhow::bail!("--stream-clickhouse and --direct-clickhouse are mutually exclusive");
            }
            if unlink_loaded_parts && !stream_clickhouse {
                anyhow::bail!("--unlink-loaded-parts requires --stream-clickhouse");
            }
            if !direct_clickhouse && out.is_none() {
                anyhow::bail!("--out is required unless --direct-clickhouse is set");
            }
            let out = out.unwrap_or_default();
            let stream_ch = if direct_clickhouse {
                let loader = bidx_loader::ChLoader::new(ch.into())?;
                Some(pipeline::StreamClickHouseConfig {
                    loader,
                    mode: pipeline::StreamChMode::DirectRowBinary,
                    profile: index_profile.into(),
                })
            } else if stream_clickhouse {
                let loader = bidx_loader::ChLoader::new(ch.into())?;
                let mode = if upload {
                    pipeline::StreamChMode::ParquetUpload {
                        unlink_loaded_parts,
                    }
                } else {
                    pipeline::StreamChMode::ParquetServerFile {
                        unlink_loaded_parts,
                    }
                };
                Some(pipeline::StreamClickHouseConfig {
                    loader,
                    mode,
                    profile: index_profile.into(),
                })
            } else {
                None
            };
            pipeline::run_parse(pipeline::ParseConfig {
                blocks_dir,
                out,
                utxo,
                start,
                end,
                blocks_per_part,
                threads,
                rpc_url,
                rpc_cookie,
                skip_disk_check,
                network: network.map(Into::into),
                checkpoint_file,
                orphan_blocks: None,
                rpc_transport: None,
                capture_witness,
                block_cache_mb,
                utxo_cache_mb,
                stream_clickhouse: stream_ch,
            })?;
        }

        Cmd::InitDb { ch } => {
            let loader = bidx_loader::ChLoader::new(ch.into())?;
            loader.init_schema()?;
            println!("schema initialized");
        }

        Cmd::Load {
            input,
            upload,
            index_profile,
            network,
            ch,
        } => {
            let loader = bidx_loader::ChLoader::new(ch.into())?;
            let mode = if upload {
                bidx_loader::LoadMode::Upload
            } else {
                bidx_loader::LoadMode::ServerFile
            };
            loader.load_all_with_profile_and_opts(
                &input,
                mode,
                bidx_loader::BulkLoadOptions {
                    profile: index_profile.into(),
                    skip_utxos_by_address: false,
                    skip_scripthash_map: true,
                },
            )?;
            let net: Network = network.into();
            let (addr_n, sh_n) = loader.rebuild_address_and_scripthash_par(
                |ty, hash| bidx_serve::address::render_address(net, ty, hash),
                bidx_serve::address::scripthash_of,
            )?;
            println!("load complete ({addr_n} addresses, {sh_n} scripthashes indexed)");
        }

        Cmd::Inspect { blocks_dir, height } => {
            pipeline::inspect_block(&blocks_dir, height)?;
        }

        Cmd::Index { action } => match action {
            IndexAction::List => {
                for spec in bidx_loader::schema::index_catalog() {
                    println!("{:<26} {:<20} {}", spec.name, spec.mechanism, spec.target);
                }
            }
            IndexAction::Rebuild { name, network, ch } => {
                let loader = bidx_loader::ChLoader::new(ch.into())?;
                if name == "address_prefix" {
                    // Needs base58/bech32 rendering, which the loader can't do.
                    let net: Network = network.into();
                    let n = loader.rebuild_address_prefix_par(|ty, hash| {
                        bidx_serve::address::render_address(net, ty, hash)
                    })?;
                    println!("rebuilt index {name} ({n} addresses)");
                } else if name == "address_and_scripthash" {
                    let net: Network = network.into();
                    let (addr_n, sh_n) = loader.rebuild_address_and_scripthash_par(
                        |ty, hash| bidx_serve::address::render_address(net, ty, hash),
                        bidx_serve::address::scripthash_of,
                    )?;
                    println!("rebuilt address_prefix ({addr_n}) and scripthash_map ({sh_n})");
                } else {
                    loader.rebuild_index(&name)?;
                    println!("rebuilt index {name}");
                }
            }
        },

        Cmd::Bench(cfg) => run_bench(cfg)?,

        Cmd::UdfConfig { udf_bin, network } => {
            let net: Network = network.into();
            print!("{}", bidx_loader::schema::udf_config_xml(&udf_bin, net));
        }

        Cmd::Serve { config } => serve::run_serve(serve::ServeArgs { config })?,

        Cmd::Verify {
            ch,
            rpc_url,
            rpc_cookie,
            rpc_user,
            rpc_password,
        } => {
            verify::run_verify(verify::VerifyArgs {
                ch: ch.into(),
                rpc_url,
                rpc_cookie,
                rpc_user,
                rpc_password,
            })?;
        }

        Cmd::Migrate { ch } => {
            let loader = bidx_loader::ChLoader::new(ch.into())?;
            let applied = loader.migrate()?;
            if applied.is_empty() {
                println!(
                    "schema up to date (version {})",
                    bidx_loader::schema::SCHEMA_VERSION
                );
            } else {
                println!("applied migrations: {applied:?}");
            }
        }

        Cmd::Live {
            rpc_url,
            cookie,
            rpc_user,
            rpc_password,
            zmq,
            utxo,
            out,
            max_reorg_depth,
            blocks_dir,
            network,
            skip_disk_check,
            checkpoint_file,
            capture_witness,
            ch_live,
            ch_host,
            ch_port,
            ch_user,
            ch_password,
            ch_database,
            ch_secure,
        } => {
            live::run_live(live::LiveConfig {
                rpc_url,
                cookie,
                rpc_user,
                rpc_password,
                zmq,
                utxo,
                out,
                max_reorg_depth,
                blocks_dir,
                network: network.map(Into::into),
                skip_disk_check,
                checkpoint_file,
                capture_witness,
                ch_live,
                ch: bidx_loader::ChConfig {
                    host: ch_host,
                    port: ch_port,
                    user: ch_user,
                    password: ch_password,
                    database: ch_database,
                    secure: ch_secure,
                },
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_arg_maps_to_bidx_core_network() {
        let pairs = [
            (NetworkArg::Mainnet, Network::Mainnet),
            (NetworkArg::Testnet3, Network::Testnet3),
            (NetworkArg::Testnet4, Network::Testnet4),
            (NetworkArg::Signet, Network::Signet),
            (NetworkArg::Regtest, Network::Regtest),
        ];
        for (a, expected) in pairs {
            let back: Network = a.into();
            assert_eq!(back, expected);
        }
    }

    #[test]
    fn cli_parses_all_subcommand_shapes() {
        // Headers
        let c = Cli::try_parse_from(["bidx", "headers", "--blocks-dir", "/tmp/x"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Inspect
        let c = Cli::try_parse_from(["bidx", "inspect", "--blocks-dir", "/tmp/x", "--height", "7"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Parse (minimal)
        let c = Cli::try_parse_from([
            "bidx",
            "parse",
            "--blocks-dir",
            "/tmp/x",
            "--out",
            "/tmp/y",
            "--utxo",
            "/tmp/z",
        ]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Load
        let c = Cli::try_parse_from(["bidx", "load", "--input", "/tmp/y"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // InitDb
        let c = Cli::try_parse_from(["bidx", "init-db", "--host", "db.example"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Live (minimal)
        let c = Cli::try_parse_from(["bidx", "live", "--utxo", "/tmp/z"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Bench with sensible args
        let c = Cli::try_parse_from(["bidx", "bench", "--blocks-dir", "/tmp/x", "--mode", "full"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Serve with no config (defaults + env).
        let c = Cli::try_parse_from(["bidx", "serve"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Serve with a config path.
        let c = Cli::try_parse_from(["bidx", "serve", "--config", "/etc/bidx/bidx.toml"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Verify (minimal and with node RPC).
        let c = Cli::try_parse_from(["bidx", "verify"]);
        assert!(c.is_ok(), "{:?}", c.err());
        let c = Cli::try_parse_from(["bidx", "verify", "--rpc-url", "http://127.0.0.1:8332"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Migrate.
        let c = Cli::try_parse_from(["bidx", "migrate", "--database", "btc"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // UdfConfig (minimal and with args).
        let c = Cli::try_parse_from(["bidx", "udf-config"]);
        assert!(c.is_ok(), "{:?}", c.err());
        let c = Cli::try_parse_from(["bidx", "udf-config", "--network", "signet"]);
        assert!(c.is_ok(), "{:?}", c.err());
        // Reject unknown
        assert!(Cli::try_parse_from(["bidx", "bogus-command"]).is_err());
    }

    #[test]
    fn cli_parse_rejects_conflicting_or_missing_args() {
        // Parse requires --utxo.
        let c = Cli::try_parse_from(["bidx", "parse", "--blocks-dir", "/tmp/x"]);
        assert!(c.is_err());
        // Inspect requires a height.
        let c = Cli::try_parse_from(["bidx", "inspect", "--blocks-dir", "/tmp/x"]);
        assert!(c.is_err());
        // Global log filter accepted.
        let c = Cli::try_parse_from([
            "bidx",
            "--log",
            "bidx=debug",
            "headers",
            "--blocks-dir",
            "/tmp/x",
        ]);
        assert!(c.is_ok());
        assert_eq!(c.unwrap().log, "bidx=debug");
    }

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).unwrap()
    }

    #[test]
    fn ch_args_map_onto_loader_config_including_tls() {
        let c = cli(&[
            "bidx",
            "init-db",
            "--host",
            "ch.cloud",
            "--port",
            "9440",
            "--user",
            "ingest",
            "--password",
            "pw",
            "--database",
            "btc",
            "--secure",
        ]);
        let Cmd::InitDb { ch } = c.cmd else {
            panic!("expected init-db")
        };
        let cfg: bidx_loader::ChConfig = ch.into();
        assert_eq!(
            (
                cfg.host.as_str(),
                cfg.port,
                cfg.user.as_str(),
                cfg.password.as_deref(),
                cfg.database.as_str(),
                cfg.secure
            ),
            ("ch.cloud", 9440, "ingest", Some("pw"), "btc", true)
        );
        // Defaults: plain TCP to localhost:9000, no password.
        let Cmd::Migrate { ch } = cli(&["bidx", "migrate"]).cmd else {
            panic!("expected migrate")
        };
        let cfg: bidx_loader::ChConfig = ch.into();
        assert!(!cfg.secure && cfg.password.is_none() && cfg.port == 9000);
        // load --upload and live --ch-secure parse.
        let Cmd::Load { upload, .. } = cli(&["bidx", "load", "--input", "/x", "--upload"]).cmd
        else {
            panic!("expected load")
        };
        assert!(upload);
        let Cmd::Live { ch_secure, .. } = cli(&["bidx", "live", "--utxo", "/x", "--ch-secure"]).cmd
        else {
            panic!("expected live")
        };
        assert!(ch_secure);
    }

    #[test]
    fn run_load_upload_arm_reaches_success_with_a_fake_client() {
        with_fake_client("upload", || {
            let dir = std::env::temp_dir().join(format!("bidx-cli-upload-{}", std::process::id()));
            std::fs::create_dir_all(dir.join("blocks")).unwrap();
            std::fs::write(dir.join("blocks").join("part-00000.parquet"), b"PAR1").unwrap();
            let c = cli(&["bidx", "load", "--upload", "--input", dir.to_str().unwrap()]);
            assert!(run(c).is_ok());
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    /// Run `f` with `BIDX_CLICKHOUSE_CLIENT` pointing at a fake client that
    /// exits 0, so the ClickHouse-backed dispatch arms reach their success
    /// path. Holds the fork guard so the env mutation and the child spawn don't
    /// race other tests.
    fn with_fake_client(tag: &str, f: impl FnOnce()) {
        let _g = testlock::fork_guard();
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("bidx-cli-ch-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        std::fs::write(&dir, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = std::fs::metadata(&dir).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dir, perms).unwrap();

        let saved = std::env::var_os("BIDX_CLICKHOUSE_CLIENT");
        std::env::set_var("BIDX_CLICKHOUSE_CLIENT", &dir);
        f();
        match saved {
            Some(v) => std::env::set_var("BIDX_CLICKHOUSE_CLIENT", v),
            None => std::env::remove_var("BIDX_CLICKHOUSE_CLIENT"),
        }
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn run_index_list_prints_the_catalogue() {
        // Pure: prints every catalogue entry, no I/O.
        let c = cli(&["bidx", "index", "list"]);
        assert!(run(c).is_ok());
    }

    #[test]
    fn run_udf_config_prints_the_fragment_for_the_network() {
        // Pure: prints the XML fragment; no ClickHouse or child process.
        let c = cli(&[
            "bidx",
            "udf-config",
            "--udf-bin",
            "/opt/bidx-udf",
            "--network",
            "regtest",
        ]);
        assert!(run(c).is_ok());
    }

    #[test]
    fn run_headers_reports_a_missing_blocks_dir() {
        let c = cli(&["bidx", "headers", "--blocks-dir", "/definitely/not/here"]);
        let e = run(c).unwrap_err();
        assert!(e.to_string().contains("header pass failed"), "{e}");
    }

    #[test]
    fn run_inspect_fails_on_a_missing_block() {
        let c = cli(&[
            "bidx",
            "inspect",
            "--blocks-dir",
            "/definitely/not/here",
            "--height",
            "0",
        ]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_initdb_and_migrate_fail_without_a_clickhouse_client() {
        let _g = testlock::fork_guard();
        // No clickhouse-client on PATH in the sandbox → NoClient error, not a
        // panic. Exercises the InitDb and Migrate dispatch arms.
        let c = cli(&["bidx", "init-db", "--host", "localhost"]);
        assert!(run(c).is_err());
        let c = cli(&["bidx", "migrate", "--host", "localhost"]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_load_fails_without_a_clickhouse_client() {
        let _g = testlock::fork_guard();
        let c = cli(&["bidx", "load", "--input", "/tmp/nope"]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_index_rebuild_fails_without_a_clickhouse_client() {
        let _g = testlock::fork_guard();
        let c = cli(&["bidx", "index", "rebuild", "coinstats_by_height"]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_serve_propagates_a_bad_config_path() {
        let c = cli(&["bidx", "serve", "--config", "/definitely/not/here.toml"]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_verify_fails_without_a_clickhouse_client() {
        let _g = testlock::fork_guard();
        let c = cli(&["bidx", "verify", "--host", "localhost"]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_clickhouse_arms_succeed_with_a_fake_client() {
        // With a fake client on BIDX_CLICKHOUSE_CLIENT every ClickHouse-backed
        // arm reaches its success println.
        with_fake_client("ok", || {
            assert!(run(cli(&["bidx", "init-db"])).is_ok());
            assert!(run(cli(&[
                "bidx",
                "load",
                "--input",
                "/tmp/nope",
                "--index-profile",
                "full"
            ]))
            .is_ok());
            assert!(run(cli(&["bidx", "index", "rebuild", "coinstats_by_height"])).is_ok());
            // address_prefix routes through the Rust renderer (0 rows here).
            assert!(run(cli(&["bidx", "index", "rebuild", "address_prefix"])).is_ok());
            // fused binary address_prefix + scripthash_map rebuild (0 rows here).
            assert!(run(cli(&["bidx", "index", "rebuild", "address_and_scripthash"])).is_ok());
            // migrate: schema_version parses "0" → reports up to date.
            assert!(run(cli(&["bidx", "migrate"])).is_ok());
        });
    }

    /// Run `f` with `BIDX_CLICKHOUSE_CLIENT` pointing at a fake client that
    /// answers the `address_prefix` source query with one P2PKH row and echoes
    /// any other query, so the CLI's render closure actually runs.
    fn with_address_prefix_client(tag: &str, f: impl FnOnce()) {
        let _g = testlock::fork_guard();
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("bidx-cli-ap-{tag}-{}", std::process::id()));
        let bin_row =
            std::env::temp_dir().join(format!("bidx-cli-ap-{tag}-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        let _ = std::fs::remove_file(&bin_row);
        let mut raw_33 = vec![2u8];
        raw_33.extend_from_slice(&[
            0x62, 0xe9, 0x07, 0xb1, 0x5c, 0xbf, 0x27, 0xd5, 0x42, 0x53, 0x99, 0xeb, 0xf6, 0xf0,
            0xfb, 0x50, 0xeb, 0xb8, 0x8f, 0x18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]);
        std::fs::write(&bin_row, &raw_33).unwrap();
        // hash160 62e907b1... is the genesis P2PKH address on mainnet.
        let body = format!(
            "#!/bin/sh\n\
             for a in \"$@\"; do case \"$a\" in --query=*SELECT*DISTINCT*FORMAT*RowBinary*) cat '{}'; exit 0;; esac; done\n\
             for a in \"$@\"; do case \"$a\" in --query=*DISTINCT*script_type*) printf '2\\t{}' ; exit 0;; esac; done\n\
             for a in \"$@\"; do case \"$a\" in --query=*FORMAT*RowBinary*) cat >/dev/null; exit 0;; esac; done\n\
             for a in \"$@\"; do case \"$a\" in --query=*) echo \"$a\";; esac; done\n\
             exit 0\n",
            bin_row.display(),
            format!("{}{}", "62e907b15cbf27d5425399ebf6f0fb50ebb88f18", "0".repeat(24))
        );
        std::fs::write(&dir, body).unwrap();
        let mut perms = std::fs::metadata(&dir).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dir, perms).unwrap();

        let saved = std::env::var_os("BIDX_CLICKHOUSE_CLIENT");
        std::env::set_var("BIDX_CLICKHOUSE_CLIENT", &dir);
        f();
        match saved {
            Some(v) => std::env::set_var("BIDX_CLICKHOUSE_CLIENT", v),
            None => std::env::remove_var("BIDX_CLICKHOUSE_CLIENT"),
        }
        let _ = std::fs::remove_file(&bin_row);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn run_index_rebuild_address_prefix_renders_a_mainnet_address() {
        with_address_prefix_client("mainnet", || {
            assert!(run(cli(&["bidx", "index", "rebuild", "address_prefix"])).is_ok());
            assert!(run(cli(
                &["bidx", "index", "rebuild", "address_and_scripthash",]
            ))
            .is_ok());
            assert!(run(cli(&["bidx", "load", "--input", "/tmp/nope",])).is_ok());
            // An explicit network arg is accepted too.
            assert!(run(cli(&[
                "bidx",
                "index",
                "rebuild",
                "address_prefix",
                "--network",
                "testnet3",
            ]))
            .is_ok());
        });
    }

    /// A fake client that answers the schema-version query with `version` and
    /// exits 0 for everything else, so `migrate` takes a deterministic branch:
    /// a version below `SCHEMA_VERSION` applies the pending migrations (the
    /// "applied" arm), and one at/above it applies nothing (the "up to date"
    /// arm).
    fn with_versioned_client(tag: &str, version: u32, f: impl FnOnce()) {
        let _g = testlock::fork_guard();
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("bidx-cli-ver-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        let body = format!(
            "#!/bin/sh\n\
             for a in \"$@\"; do case \"$a\" in --query=*max*version*) echo {version}; exit 0;; esac; done\n\
             exit 0\n"
        );
        std::fs::write(&dir, body).unwrap();
        let mut perms = std::fs::metadata(&dir).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&dir, perms).unwrap();

        let saved = std::env::var_os("BIDX_CLICKHOUSE_CLIENT");
        std::env::set_var("BIDX_CLICKHOUSE_CLIENT", &dir);
        f();
        match saved {
            Some(v) => std::env::set_var("BIDX_CLICKHOUSE_CLIENT", v),
            None => std::env::remove_var("BIDX_CLICKHOUSE_CLIENT"),
        }
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn run_migrate_reports_applied_versions_from_the_fake_client() {
        // A client reporting version 0 has the v2 address_stats rebuild
        // pending, so `migrate` applies it and prints the "applied" arm.
        with_versioned_client("applied", 0, || {
            assert!(run(cli(&["bidx", "migrate", "--database", "btc"])).is_ok());
        });
        // A client already at the current version applies nothing and takes
        // the "schema up to date" arm.
        with_versioned_client("current", bidx_loader::schema::SCHEMA_VERSION, || {
            assert!(run(cli(&["bidx", "migrate", "--database", "btc"])).is_ok());
        });
    }

    #[test]
    fn run_headers_succeeds_over_a_synthetic_blk_file() {
        // A single well-formed record (mainnet magic + 80-byte header) is
        // enough for the header pass to build a one-block chain index and take
        // the success println path.
        let dir = std::env::temp_dir().join(format!("bidx-hdr-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&80u32.to_le_bytes());
        let mut hdr = [0u8; 80];
        hdr[0] = 1; // version
        hdr[79] = 1; // non-zero tail so trailing-zero padding is not trimmed
        data.extend_from_slice(&hdr);
        std::fs::write(dir.join("blk00000.dat"), &data).unwrap();

        let c = cli(&["bidx", "headers", "--blocks-dir", dir.to_str().unwrap()]);
        assert!(run(c).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_parse_arm_builds_config_then_fails_on_a_missing_dir() {
        {
            let _g = testlock::fork_guard();
            let c = cli(&[
                "bidx",
                "parse",
                "--blocks-dir",
                "/definitely/not/here",
                "--out",
                "/tmp/bidx-out",
                "--utxo",
                "/tmp/bidx-utxo",
                "--network",
                "regtest",
            ]);
            assert!(run(c).is_err());

            // `--unlink-loaded-parts` without `--stream-clickhouse` is rejected.
            let bad = cli(&[
                "bidx",
                "parse",
                "--blocks-dir",
                "/definitely/not/here",
                "--out",
                "/tmp/bidx-out",
                "--utxo",
                "/tmp/bidx-utxo",
                "--unlink-loaded-parts",
            ]);
            let err = run(bad).unwrap_err();
            assert!(
                err.to_string().contains("--stream-clickhouse"),
                "got: {err}"
            );

            // `--stream-clickhouse` and `--direct-clickhouse` together is rejected.
            let bad_both = cli(&[
                "bidx",
                "parse",
                "--blocks-dir",
                "/definitely/not/here",
                "--out",
                "/tmp/bidx-out",
                "--utxo",
                "/tmp/bidx-utxo",
                "--stream-clickhouse",
                "--direct-clickhouse",
            ]);
            let err_both = run(bad_both).unwrap_err();
            assert!(
                err_both.to_string().contains("mutually exclusive"),
                "got: {err_both}"
            );

            // Omitting `--out` without `--direct-clickhouse` is rejected.
            let bad_no_out = cli(&[
                "bidx",
                "parse",
                "--blocks-dir",
                "/definitely/not/here",
                "--utxo",
                "/tmp/bidx-utxo",
            ]);
            let err_no_out = run(bad_no_out).unwrap_err();
            assert!(
                err_no_out.to_string().contains("--out is required"),
                "got: {err_no_out}"
            );
        }

        // `--stream-clickhouse` and `--direct-clickhouse` resolve the ClickHouse
        // client and map ServerFile, Upload, and DirectRowBinary modes + profiles.
        with_fake_client("parse-stream", || {
            for extra in [
                &["--out", "/tmp/bidx-out", "--stream-clickhouse"][..],
                &[
                    "--out",
                    "/tmp/bidx-out",
                    "--stream-clickhouse",
                    "--upload",
                    "--unlink-loaded-parts",
                    "--index-profile",
                    "full",
                ][..],
                &["--direct-clickhouse", "--index-profile", "serving"][..],
            ] {
                let mut args = vec![
                    "bidx",
                    "parse",
                    "--blocks-dir",
                    "/definitely/not/here",
                    "--utxo",
                    "/tmp/bidx-utxo",
                ];
                args.extend_from_slice(extra);
                assert!(run(cli(&args)).is_err());
            }
        });
    }

    #[test]
    fn run_bench_arm_fails_on_a_missing_blocks_dir() {
        let _g = testlock::fork_guard();
        let c = cli(&[
            "bidx",
            "bench",
            "--blocks-dir",
            "/definitely/not/here",
            "--mode",
            "headers",
        ]);
        assert!(run(c).is_err());
    }

    #[test]
    fn run_live_arm_builds_config_then_fails_on_a_bad_utxo_path() {
        let _g = testlock::fork_guard();
        // run_live fails fast when the UTXO store path cannot be created.
        let c = cli(&[
            "bidx",
            "live",
            "--utxo",
            "/proc/definitely/not/writable",
            "--skip-disk-check",
        ]);
        assert!(run(c).is_err());
    }
}
