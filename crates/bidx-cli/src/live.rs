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

use crate::diskguard::{DiskGuardConfig, DiskRecorder, DEFAULT_CHECKPOINT_FILE_NAME};

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
    /// Blocks directory (used only to auto-detect the network for the
    /// disk-space checkpoint file's section). If omitted, pass `network`.
    pub blocks_dir: Option<PathBuf>,
    /// Chain network override (auto-detected from --blocks-dir if not set;
    /// defaults to mainnet).
    pub network: Option<bidx_core::Network>,
    /// Skip the disk-space startup check entirely.
    pub skip_disk_check: bool,
    /// Override disk-check checkpoint file. Default:
    /// `<parent-of-utxo>/bidx-disk-checkpoints.txt`.
    pub checkpoint_file: Option<PathBuf>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "bidx-live-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn synthetic_block(height: u32) -> bidx_parser::FullBlock {
        let coinbase_txid = [0x11u8; 32];
        let hdr = bidx_core::BlockHeader {
            version: 1,
            prev_hash: bidx_core::Hash32::ZERO,
            merkle_root: bidx_core::Hash32::ZERO,
            time: 0,
            bits: 0,
            nonce: 0,
        };
        bidx_parser::FullBlock {
            block_row: bidx_core::BlockRow {
                height,
                hash: [0xAAu8; 32],
                prev_hash: [0u8; 32],
                merkle_root: [0u8; 32],
                time: 0,
                bits: 0,
                nonce: 0,
                version: hdr.version,
                tx_count: 1,
                size: 0,
                weight: 0,
                total_fee: 0,
                coinbase_value: 100,
            },
            txs: vec![bidx_core::TxRow {
                height,
                tx_index: 0,
                txid: coinbase_txid,
                version: 1,
                locktime: 0,
                size: 0,
                weight: 0,
                fee: 0,
                input_count: 1,
                output_count: 1,
                has_witness: false,
            }],
            inputs: vec![bidx_core::InputRow {
                height,
                tx_index: 0,
                input_index: 0,
                txid: coinbase_txid,
                prev_txid: [0u8; 32],
                prev_vout: 0xFFFF_FFFF,
                script_sig_len: 0,
                sequence: 0,
                witness_items: 0,
                witness_bytes: 0,
                is_coinbase: true,
            }],
            outputs: vec![bidx_core::OutputRow {
                height,
                tx_index: 0,
                output_index: 0,
                txid: coinbase_txid,
                value_sat: 100,
                script_pubkey_len: 0,
                script_type: 0,
                address_hash: [0u8; 32],
            }],
        }
    }

    #[test]
    fn live_sink_rotates_every_live_blocks_per_part() {
        let dir = tmp("rotate");
        let mut sink = LiveSink::create(&dir, 0).unwrap();
        // First block at h=0 lands in part-0.
        sink.write_block(&synthetic_block(0), &[]).unwrap();
        assert_eq!(sink.part, 0);
        // Still part-0 while height < LIVE_BLOCKS_PER_PART.
        sink.write_block(&synthetic_block(500), &[]).unwrap();
        assert_eq!(sink.part, 0, "h=500 still part 0 (< {})", LIVE_BLOCKS_PER_PART);
        // h = LIVE_BLOCKS_PER_PART rolls to part 1.
        sink.write_block(&synthetic_block(LIVE_BLOCKS_PER_PART), &[]).unwrap();
        assert_eq!(sink.part, 1);
        // h = 2 × LIVE_BLOCKS_PER_PART rolls to part 2.
        sink.write_block(&synthetic_block(2 * LIVE_BLOCKS_PER_PART), &[]).unwrap();
        assert_eq!(sink.part, 2);
        if let Some(s) = sink.sink.take() {
            s.finish().unwrap();
        }
        // Walk entity dirs and count part files across them (files are named
        // part-NNNNN.parquet with 5 digits).
        let mut parts: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for entity in ["blocks", "transactions", "inputs", "outputs", "spends"] {
            for e in std::fs::read_dir(dir.join(entity)).unwrap().flatten() {
                parts.insert(e.file_name().to_string_lossy().to_string());
            }
        }
        assert_eq!(
            parts.iter().cloned().collect::<Vec<_>>(),
            vec!["part-00000.parquet", "part-00001.parquet", "part-00002.parquet"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_sink_write_block_emits_all_row_kinds() {
        let dir = tmp("kinds");
        let mut sink = LiveSink::create(&dir, 0).unwrap();
        let mut block = synthetic_block(7);
        // Add a 2nd tx + regular input + spend row so every sink's write loop
        // is exercised.
        block.block_row.tx_count = 2;
        let txid_s = [0x22u8; 32];
        block.txs.push(bidx_core::TxRow {
            height: 7,
            tx_index: 1,
            txid: txid_s,
            version: 1,
            locktime: 0,
            size: 0,
            weight: 0,
            fee: 5,
            input_count: 1,
            output_count: 1,
            has_witness: false,
        });
        block.inputs.push(bidx_core::InputRow {
            height: 7,
            tx_index: 1,
            input_index: 0,
            txid: txid_s,
            prev_txid: [0xEE; 32],
            prev_vout: 3,
            script_sig_len: 0,
            sequence: 1,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase: false,
        });
        block.outputs.push(bidx_core::OutputRow {
            height: 7,
            tx_index: 1,
            output_index: 0,
            txid: txid_s,
            value_sat: 50,
            script_pubkey_len: 0,
            script_type: 1,
            address_hash: [0xBB; 32],
        });
        let spends = vec![bidx_core::SpendRow {
            height: 7,
            spending_tx_index: 1,
            spending_input_index: 0,
            spending_txid: txid_s,
            spent_txid: [0xEE; 32],
            spent_vout: 3,
            spent_value_sat: 55,
            spent_script_type: 1,
            spent_address_hash: [0xCC; 32],
            spent_height: 0,
        }];
        sink.write_block(&block, &spends).unwrap();
        if let Some(s) = sink.sink.take() {
            s.finish().unwrap();
        }
        // Each of the 5 entities must have produced its part-00000 file.
        for entity in ["blocks", "transactions", "inputs", "outputs", "spends"] {
            let p = dir.join(entity).join("part-00000.parquet");
            assert!(p.exists(), "missing {:?}", p);
        }
        let _ = std::fs::remove_dir_all(&dir);
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

    // ---- disk-space startup check (always possible: we have RPC) ----
    let network = cfg
        .network
        .or_else(|| {
            cfg.blocks_dir
                .as_deref()
                .and_then(bidx_core::Network::detect_from_blocks_dir)
        })
        .unwrap_or(bidx_core::Network::Mainnet);
    let parent_of_utxo = cfg
        .utxo
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let checkpoint_file = cfg
        .checkpoint_file
        .clone()
        .unwrap_or_else(|| parent_of_utxo.join(DEFAULT_CHECKPOINT_FILE_NAME));
    let mut tracked_dirs = vec![cfg.utxo.clone()];
    if let Some(out) = &cfg.out {
        tracked_dirs.push(out.clone());
    }
    let disk_cfg = DiskGuardConfig {
        checkpoint_file,
        network,
        index_root: parent_of_utxo.clone(),
        tracked_dirs,
    };

    if !cfg.skip_disk_check {
        let node_tip = node
            .get_block_count()
            .context("getblockcount for disk pre-flight")?;
        let local_tip_ckpt = bidx_core::read_checkpoints(&disk_cfg.checkpoint_file, network)
            .last()
            .map(|c| c.height)
            .unwrap_or(0);
        let local_tip_utxo = utxo.tip()?.map(|t| t.height).unwrap_or(0);
        let local_tip = local_tip_ckpt.max(local_tip_utxo);
        if local_tip == 0 {
            tracing::info!(
                network = network.name(),
                "no disk checkpoints yet — running without growth projection until height \
                 100,000 is reached for the first time"
            );
        }
        disk_cfg.run_pre_flight(node_tip, local_tip, bidx_core::MIN_FREE_BYTES)?;
    }

    let recorder = Arc::new(DiskRecorder::new(disk_cfg.clone()));
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
                if let Err(e) = recorder
                    .on_block_applied(*height)
                    .map(|_| ())
                {
                    warn!(error = %e, "failed to record disk checkpoint");
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
