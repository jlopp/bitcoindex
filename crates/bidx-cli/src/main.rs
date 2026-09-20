use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

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
    },

    /// Parse a single block at a height and print it as JSON (debug).
    Inspect {
        #[arg(long)]
        blocks_dir: PathBuf,
        #[arg(long)]
        height: u32,
    },
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
        } => {
            pipeline::run_parse(pipeline::ParseConfig {
                blocks_dir,
                out,
                utxo,
                start,
                end,
                blocks_per_part,
                threads,
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

        Cmd::Live {
            rpc_url,
            cookie,
            rpc_user,
            rpc_password,
            zmq,
            utxo,
            out,
            max_reorg_depth,
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
            })?;
        }
    }
    Ok(())
}
