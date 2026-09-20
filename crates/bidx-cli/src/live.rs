//! `bidx live`: run the live tip tracker, optionally appending each applied
//! block (with its spend rows) to Parquet part files in the same layout as
//! `parse`.

use anyhow::{Context, Result};
use bidx_live::{
    LiveTracker, NodeClient, RpcConfig, TrackerConfig, TrackerEvent, ZmqConfig, ZmqSubscriber,
};
use bidx_store::{ParquetSink, SinkConfig, UtxoStore};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Debug)]
pub struct LiveConfig {
    pub rpc_url: String,
    pub cookie: Option<PathBuf>,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
    pub zmq: Option<String>,
    pub utxo: PathBuf,
    pub out: Option<PathBuf>,
    pub max_reorg_depth: u32,
}

/// Blocks per rolling Parquet part in live mode.
const LIVE_BLOCKS_PER_PART: u32 = 1_000;

/// A live Parquet sink that rotates part files every `blocks_per_part`.
struct LiveSink {
    dir: PathBuf,
    part: u32,
    sink: Option<ParquetSink>,
}

impl LiveSink {
    fn create(dir: &Path, start_part: u32) -> Result<Self> {
        let sink = ParquetSink::create(dir, start_part, SinkConfig::default())?;
        Ok(LiveSink {
            dir: dir.to_path_buf(),
            part: start_part,
            sink: Some(sink),
        })
    }

    fn rotate_if_needed(&mut self, height: u32) -> Result<()> {
        let wanted = height / LIVE_BLOCKS_PER_PART;
        if wanted != self.part {
            if let Some(s) = self.sink.take() {
                s.finish()?;
            }
            self.sink = Some(ParquetSink::create(&self.dir, wanted, SinkConfig::default())?);
            self.part = wanted;
        }
        Ok(())
    }

    fn write_block(
        &mut self,
        block: &bidx_parser::FullBlock,
        spends: &[bidx_core::SpendRow],
    ) -> Result<()> {
        self.rotate_if_needed(block.block_row.height)?;
        let s = self.sink.as_mut().expect("sink present after rotate");
        s.push_block(&block.block_row)?;
        for tx in &block.txs {
            s.push_tx(tx)?;
        }
        for i in &block.inputs {
            s.push_input(i)?;
        }
        for o in &block.outputs {
            s.push_output(o)?;
        }
        for sp in spends {
            s.push_spend(sp)?;
        }
        Ok(())
    }
}

pub fn run_live(cfg: LiveConfig) -> Result<()> {
    let node = NodeClient::new(RpcConfig {
        url: cfg.rpc_url.clone(),
        cookie_path: cfg.cookie.clone(),
        user: cfg.rpc_user.clone(),
        password: cfg.rpc_password.clone(),
    })
    .context("failed to init RPC client")?;

    let zmq = match &cfg.zmq {
        Some(ep) => {
            info!(endpoint = %ep, "connecting ZMQ hashblock subscriber");
            Some(ZmqSubscriber::connect(&ZmqConfig {
                hashblock_endpoint: ep.clone(),
            })?)
        }
        None => {
            warn!("no --zmq endpoint given; using RPC poll fallback");
            None
        }
    };

    // Live mode: WAL + undo logging on.
    let utxo = Arc::new(UtxoStore::open_live(&cfg.utxo)?);
    match utxo.tip()? {
        Some(t) => info!(height = t.height, hash = %t.hash, "resuming from stored tip"),
        None => warn!("no stored tip in UTXO db — run `bidx parse` first for full history"),
    }

    let tracker = LiveTracker::new(
        node,
        zmq,
        Arc::clone(&utxo),
        TrackerConfig {
            max_reorg_depth: cfg.max_reorg_depth,
        },
    );

    // Optional Parquet sink shared with the event callback.
    let sink = match &cfg.out {
        Some(d) => {
            let start_part = utxo
                .tip()?
                .map(|t| (t.height + 1) / LIVE_BLOCKS_PER_PART)
                .unwrap_or(0);
            Some(Mutex::new(LiveSink::create(d, start_part)?))
        }
        None => None,
    };

    tracker.run(|event, block| {
        match &event {
            TrackerEvent::Applied {
                height,
                hash,
                spends,
            } => {
                info!(height, %hash, "applied block");
                if let (Some(sink), Some(b)) = (&sink, block) {
                    let mut guard = sink.lock().unwrap();
                    if let Err(e) = guard.write_block(b, spends) {
                        warn!(error = %e, "failed to write block to parquet");
                    }
                }
            }
            TrackerEvent::Disconnected { height, hash } => {
                warn!(height, %hash, "disconnected block (reorg)");
                // Rows for a disconnected height may already be in a closed
                // Parquet part. Downstream dedupe on load (drop partitions
                // >= reorg floor before reloading) — documented in README.
            }
            TrackerEvent::ReorgStart { .. } => {
                warn!("reorg detected; disconnecting to common ancestor");
            }
            TrackerEvent::CaughtUp { height } => {
                info!(height, "caught up to node tip");
            }
        }
    })?;

    Ok(())
}
