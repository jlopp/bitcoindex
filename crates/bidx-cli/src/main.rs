use anyhow::{Context, Result};
use bench::{run_bench, BenchConfig};
use bidx_core::Network;
use clap::{Parser, Subcommand};

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

mod bench;
mod diskguard;
mod live;
mod pipeline;

#[derive(Parser)]
#[command(name = "bidx", version, about = "Bitcoin blockchain indexer: blk*.dat -> Parquet -> ClickHouse")]
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

    /// Full ingest: parse blocks, link spends via UTXO store, write Parquet.
    Parse {
        #[arg(long)]
        blocks_dir: PathBuf,
        /// Output directory for Parquet part files.
        #[arg(long)]
        out: PathBuf,
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
    },

    /// Create ClickHouse schema (idempotent).
    InitDb {
        #[arg(long, default_value = "localhost")]
        host: String,
        #[arg(long, default_value_t = 9000)]
        port: u16,
        #[arg(long, default_value = "default")]
        user: String,
        #[arg(long)]
        password: Option<String>,
        #[arg(long, default_value = "bitcoin")]
        database: String,
    },

    /// Bulk-load Parquet files into ClickHouse.
    Load {
        /// Parquet output directory from `parse`.
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value = "localhost")]
        host: String,
        #[arg(long, default_value_t = 9000)]
        port: u16,
        #[arg(long, default_value = "default")]
        user: String,
        #[arg(long)]
        password: Option<String>,
        #[arg(long, default_value = "bitcoin")]
        database: String,
    },

    /// Track the chain tip live: reconcile with the node, apply new blocks,
    /// and handle reorgs via the UTXO undo log. Optionally appends to Parquet.
    Live {
        /// Bitcoin Core RPC URL.
        #[arg(long, default_value = "http://127.0.0.1:8332")]
        rpc_url: String,
        /// Path to the node's .cookie file (used if user/password not given).
        #[arg(long)]
        cookie: Option<PathBuf>,
        #[arg(long)]
        rpc_user: Option<String>,
        #[arg(long)]
        rpc_password: Option<String>,
        /// ZMQ hashblock endpoint, e.g. tcp://127.0.0.1:28332. If omitted,
        /// falls back to a 2s RPC poll loop.
        #[arg(long)]
        zmq: Option<String>,
        /// RocksDB UTXO store path (must match the one used by `parse`).
        #[arg(long)]
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
    },

    /// Parse a single block at a height and print it as JSON (debug).
    Inspect {
        #[arg(long)]
        blocks_dir: PathBuf,
        #[arg(long)]
        height: u32,
    },

    /// Benchmark the ingest pipeline: run bidx against a blocks directory
    /// and report per-stage timing (header pass, parallel parse, sequential
    /// UTXO apply, Parquet sink) plus wall-clock throughput, CPU utilisation,
    /// and peak RSS. Use mode=headers/parse/utxo/sink to isolate stages.
    Bench(BenchConfig),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cli.log))
        .with_target(false)
        .init();

    match cli.cmd {
        Cmd::Headers { blocks_dir } => {
            let (index, stats) = bidx_parser::build_chain_index(&blocks_dir)
                .context("header pass failed")?;
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
        } => {
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
            })?;
        }

        Cmd::InitDb {
            host,
            port,
            user,
            password,
            database,
        } => {
            let loader = bidx_loader::ChLoader::new(bidx_loader::ChConfig {
                host,
                port,
                user,
                password,
                database,
            })?;
            loader.init_schema()?;
            println!("schema initialized");
        }

        Cmd::Load {
            input,
            host,
            port,
            user,
            password,
            database,
        } => {
            let loader = bidx_loader::ChLoader::new(bidx_loader::ChConfig {
                host,
                port,
                user,
                password,
                database,
            })?;
            loader.load_all(&input)?;
            println!("load complete");
        }

        Cmd::Inspect { blocks_dir, height } => {
            pipeline::inspect_block(&blocks_dir, height)?;
        }

        Cmd::Bench(cfg) => run_bench(cfg)?,

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
            })?;
        }
    }
    Ok(())
}
