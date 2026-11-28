//! `bidx live`: run the live tip tracker, optionally appending each applied
//! block to Parquet part files (in the same layout as `parse`) and/or inserting
//! it straight into ClickHouse.

use anyhow::{Context, Result};
use bidx_live::{
    LiveTracker, NodeClient, RpcConfig, TrackerConfig, TrackerEvent, ZmqConfig, ZmqSubscriber,
};
use bidx_loader::{live_insert::LiveChWriter, ChConfig, ChLoader};
use bidx_store::{ParquetSink, SinkConfig, UtxoStore};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    /// Also append raw witness stack elements to the optional `witnesses`
    /// Parquet table (requires `out`).
    pub capture_witness: bool,
    /// Insert each applied block directly into ClickHouse over the client's
    /// stdin (bypassing Parquet) for sub-minute tip freshness.
    pub ch_live: bool,
    /// ClickHouse connection settings for `ch_live`.
    pub ch: ChConfig,
}

/// Blocks per rolling Parquet part in live mode.
const LIVE_BLOCKS_PER_PART: u32 = 1_000;

/// Sidecar file recording the highest height a reorg has retracted below, so a
/// later `bidx load` can drop the affected ClickHouse partitions before
/// reloading. Written only when a retraction falls below the still-open part
/// (i.e. the rows live in an already-closed part that cannot be rewritten).
const REORG_FLOOR_FILE: &str = ".live-reorg-floor";

/// One block's rows, retained in memory until its part is written. Kept as
/// plain owned rows (rather than the `FullBlock`) so a shallow reorg can retract
/// a height before it ever reaches disk.
struct BufferedBlock {
    block: bidx_core::BlockRow,
    txs: Vec<bidx_core::TxRow>,
    inputs: Vec<bidx_core::InputRow>,
    outputs: Vec<bidx_core::OutputRow>,
    witnesses: Vec<bidx_core::WitnessRow>,
}

/// A live Parquet sink that rotates part files every `blocks_per_part` blocks.
///
/// Unlike the bulk `parse` sink, the live sink buffers a whole part in memory
/// and writes it only when the part rotates (or on `finish`). That extra window
/// is what makes reorg retraction exact: a disconnected height is almost always
/// still in the open part, so it can be dropped from the buffer before any file
/// is written. A floor below the open part is recorded to
/// [`REORG_FLOOR_FILE`] for the loader to act on.
struct LiveSink {
    dir: PathBuf,
    part: u32,
    cfg: SinkConfig,
    window: Vec<BufferedBlock>,
}

impl LiveSink {
    /// Test-only convenience: default sink config.
    #[cfg(test)]
    fn create(dir: &Path, start_part: u32) -> Result<Self> {
        Self::create_with(dir, start_part, SinkConfig::default())
    }

    fn create_with(dir: &Path, start_part: u32, cfg: SinkConfig) -> Result<Self> {
        Ok(LiveSink {
            dir: dir.to_path_buf(),
            part: start_part,
            cfg,
            window: Vec::new(),
        })
    }

    /// Write the buffered part to its Parquet files. No-op when empty, so a
    /// rotation that lands on an empty window does not create stray files.
    fn flush_window(&self) -> Result<()> {
        if self.window.is_empty() {
            return Ok(());
        }
        let mut sink = ParquetSink::create(&self.dir, self.part, self.cfg)?;
        for b in &self.window {
            sink.push_block(&b.block)?;
            for tx in &b.txs {
                sink.push_tx(tx)?;
            }
            for i in &b.inputs {
                sink.push_input(i)?;
            }
            for o in &b.outputs {
                sink.push_output(o)?;
            }
            for w in &b.witnesses {
                sink.push_witness(w)?;
            }
        }
        sink.finish()?;
        Ok(())
    }

    fn rotate_if_needed(&mut self, height: u32) -> Result<()> {
        let wanted = height / LIVE_BLOCKS_PER_PART;
        if wanted != self.part {
            self.flush_window()?;
            self.part = wanted;
            self.window.clear();
        }
        Ok(())
    }

    fn write_block(&mut self, block: &bidx_parser::FullBlock) -> Result<()> {
        self.rotate_if_needed(block.block_row.height)?;
        self.window.push(BufferedBlock {
            block: block.block_row.clone(),
            txs: block.txs.clone(),
            inputs: block.inputs.clone(),
            outputs: block.outputs.clone(),
            witnesses: block.witnesses.clone(),
        });
        Ok(())
    }

    /// Retract every buffered row at `height` or above. Rows still in the open
    /// part are dropped exactly; a floor below the open part is recorded for the
    /// loader (the closed part cannot be rewritten in place).
    fn retract_from(&mut self, height: u32) -> Result<()> {
        let part_start = self.part * LIVE_BLOCKS_PER_PART;
        if height < part_start {
            self.record_reorg_floor(height)?;
            return Ok(());
        }
        self.window.retain(|b| b.block.height < height);
        Ok(())
    }

    /// Persist the highest retracted floor to the sidecar, so a later
    /// `bidx load` knows which closed part to drop before reloading.
    fn record_reorg_floor(&self, height: u32) -> Result<()> {
        let path = self.dir.join(REORG_FLOOR_FILE);
        let prior = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(u32::MAX);
        let floor = prior.min(height);
        std::fs::write(&path, format!("{floor}\n"))
            .with_context(|| format!("writing reorg floor to {}", path.display()))?;
        Ok(())
    }

    /// Flush the open part and stop. Consumes the sink.
    fn finish(self) -> Result<()> {
        self.flush_window()
    }
}

/// Where live mode sends each applied block.
enum LiveOutput {
    /// Rolling Parquet part files (optionally loaded later).
    Parquet(LiveSink),
    /// Direct `RowBinary` inserts into ClickHouse, bypassing Parquet.
    ClickHouse {
        writer: LiveChWriter,
        loader: ChLoader,
    },
}

impl LiveOutput {
    fn write_block(&mut self, block: &bidx_parser::FullBlock) -> Result<()> {
        match self {
            LiveOutput::Parquet(s) => s.write_block(block),
            LiveOutput::ClickHouse { writer, .. } => {
                writer.write_block(&block.block_row);
                for tx in &block.txs {
                    writer.write_tx(tx);
                }
                for i in &block.inputs {
                    writer.write_input(i);
                }
                for o in &block.outputs {
                    writer.write_output(o);
                }
                for w in &block.witnesses {
                    writer.write_witness(w);
                }
                writer.flush()?;
                Ok(())
            }
        }
    }

    /// Retract rows at `height` and above after a disconnect.
    fn retract_from(&mut self, height: u32) -> Result<()> {
        match self {
            LiveOutput::Parquet(s) => s.retract_from(height),
            // ClickHouse rows are inserted as they are applied, so a
            // disconnected height must be removed explicitly.
            LiveOutput::ClickHouse { loader, .. } => {
                loader.retract_from(height).map_err(Into::into)
            }
        }
    }

    fn finish(self) -> Result<()> {
        match self {
            LiveOutput::Parquet(s) => s.finish(),
            LiveOutput::ClickHouse { writer, .. } => writer.finish().map_err(Into::into),
        }
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
                prev_txid: [0u8; 32],
                prev_vout: 0xFFFF_FFFF,
                script_sig_len: 0,
                sequence: 0,
                witness_items: 0,
                witness_bytes: 0,
                is_coinbase: true,
                spent_value_sat: 0,
                spent_script_type: 0,
                spent_address_hash: [0u8; 32],
                spent_height: 0,
            }],
            outputs: vec![bidx_core::OutputRow {
                height,
                tx_index: 0,
                output_index: 0,
                value_sat: 100,
                script_pubkey_len: 0,
                script_type: 0,
                address_hash: [0u8; 32],
            }],
            witnesses: vec![],
        }
    }

    #[test]
    fn live_sink_rotates_every_live_blocks_per_part() {
        let dir = tmp("rotate");
        let mut sink = LiveSink::create(&dir, 0).unwrap();
        // First block at h=0 lands in part-0.
        sink.write_block(&synthetic_block(0)).unwrap();
        assert_eq!(sink.part, 0);
        // Still part-0 while height < LIVE_BLOCKS_PER_PART.
        sink.write_block(&synthetic_block(500)).unwrap();
        assert_eq!(
            sink.part, 0,
            "h=500 still part 0 (< {})",
            LIVE_BLOCKS_PER_PART
        );
        // h = LIVE_BLOCKS_PER_PART rolls to part 1, flushing part 0 first.
        sink.write_block(&synthetic_block(LIVE_BLOCKS_PER_PART))
            .unwrap();
        assert_eq!(sink.part, 1);
        assert!(
            dir.join("blocks/part-00000.parquet").exists(),
            "part 0 flushed on rotate"
        );
        // h = 2 × LIVE_BLOCKS_PER_PART rolls to part 2.
        sink.write_block(&synthetic_block(2 * LIVE_BLOCKS_PER_PART))
            .unwrap();
        assert_eq!(sink.part, 2);
        sink.finish().unwrap();
        // Walk entity dirs and count part files across them (files are named
        // part-NNNNN.parquet with 5 digits).
        let mut parts: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for entity in ["blocks", "transactions", "inputs", "outputs"] {
            for e in std::fs::read_dir(dir.join(entity)).unwrap().flatten() {
                parts.insert(e.file_name().to_string_lossy().to_string());
            }
        }
        assert_eq!(
            parts.iter().cloned().collect::<Vec<_>>(),
            vec![
                "part-00000.parquet",
                "part-00001.parquet",
                "part-00002.parquet"
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_sink_write_block_emits_all_row_kinds() {
        let dir = tmp("kinds");
        let mut sink = LiveSink::create(&dir, 0).unwrap();
        let mut block = synthetic_block(7);
        // Add a 2nd tx + regular enriched input so every sink's write loop
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
            prev_txid: [0xEE; 32],
            prev_vout: 3,
            script_sig_len: 0,
            sequence: 1,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase: false,
            spent_value_sat: 55,
            spent_script_type: 1,
            spent_address_hash: [0xCC; 32],
            spent_height: 0,
        });
        block.outputs.push(bidx_core::OutputRow {
            height: 7,
            tx_index: 1,
            output_index: 0,
            value_sat: 50,
            script_pubkey_len: 0,
            script_type: 1,
            address_hash: [0xBB; 32],
        });
        sink.write_block(&block).unwrap();
        sink.finish().unwrap();
        // Each of the 4 base entities must have produced its part-00000 file.
        for entity in ["blocks", "transactions", "inputs", "outputs"] {
            let p = dir.join(entity).join("part-00000.parquet");
            assert!(p.exists(), "missing {:?}", p);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_sink_captures_witness_payload_when_enabled() {
        let dir = tmp("witness");
        let mut sink = LiveSink::create_with(
            &dir,
            0,
            SinkConfig {
                capture_witness: true,
                ..SinkConfig::default()
            },
        )
        .unwrap();
        let mut block = synthetic_block(3);
        block.witnesses.push(bidx_core::WitnessRow {
            height: 3,
            tx_index: 0,
            input_index: 0,
            item_index: 0,
            item_len: 2,
            item_bytes: vec![0xAA, 0xBB],
        });
        sink.write_block(&block).unwrap();
        sink.finish().unwrap();
        let p = dir.join("witnesses").join("part-00000.parquet");
        assert!(p.exists(), "witnesses part file must exist: {:?}", p);

        // Read the payload back and confirm the bytes survived the roundtrip.
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let f = std::fs::File::open(&p).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut rows = 0usize;
        for batch in reader {
            let batch = batch.unwrap();
            rows += batch.num_rows();
            let col = batch
                .column_by_name("item_bytes")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .unwrap();
            assert_eq!(col.value(0), &[0xAA, 0xBB]);
        }
        assert_eq!(rows, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_sink_retract_drops_buffered_rows_at_or_above_the_floor() {
        let dir = tmp("retract-open");
        let mut sink = LiveSink::create(&dir, 0).unwrap();
        for h in 0..5 {
            sink.write_block(&synthetic_block(h)).unwrap();
        }
        // Reorg at height 3: heights 3 and 4 (still buffered) must vanish.
        sink.retract_from(3).unwrap();
        sink.finish().unwrap();

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let f = std::fs::File::open(dir.join("blocks/part-00000.parquet")).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let mut heights = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch
                .column_by_name("height")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            for i in 0..col.len() {
                heights.push(col.value(i));
            }
        }
        assert_eq!(heights, vec![0, 1, 2]);
        // No reorg-floor sidecar: the retraction was exact.
        assert!(!dir.join(REORG_FLOOR_FILE).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_sink_retract_below_open_part_records_reorg_floor() {
        let dir = tmp("retract-closed");
        let mut sink = LiveSink::create(&dir, 0).unwrap();
        // Roll to part 1 (h=1000), so h < 1000 now lives in a closed part.
        sink.write_block(&synthetic_block(LIVE_BLOCKS_PER_PART))
            .unwrap();
        assert_eq!(sink.part, 1);
        sink.retract_from(42).unwrap();
        sink.finish().unwrap();
        let floor = std::fs::read_to_string(dir.join(REORG_FLOOR_FILE)).unwrap();
        assert_eq!(floor.trim(), "42");

        // A second, shallower retraction keeps the lowest floor seen.
        let mut sink2 = LiveSink::create_with(&dir, 1, SinkConfig::default()).unwrap();
        sink2.retract_from(7).unwrap();
        sink2.finish().unwrap();
        let floor = std::fs::read_to_string(dir.join(REORG_FLOOR_FILE)).unwrap();
        assert_eq!(floor.trim(), "7");

        // Writing the floor sidecar fails cleanly if a directory occupies its path.
        let _ = std::fs::remove_file(dir.join(REORG_FLOOR_FILE));
        std::fs::create_dir_all(dir.join(REORG_FLOOR_FILE)).unwrap();
        let mut sink3 = LiveSink::create_with(&dir, 1, SinkConfig::default()).unwrap();
        let err = sink3.retract_from(5).unwrap_err();
        assert!(
            format!("{err:#}").contains("writing reorg floor"),
            "{err:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_sink_flush_of_empty_window_writes_nothing() {
        let dir = tmp("empty-window");
        let sink = LiveSink::create(&dir, 0).unwrap();
        sink.flush_window().unwrap();
        assert!(!dir.join("blocks").exists(), "no files for an empty window");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_output_parquet_writes_and_retracts_through_the_enum() {
        let dir = tmp("output-parquet");
        let mut out = LiveOutput::Parquet(LiveSink::create(&dir, 0).unwrap());
        out.write_block(&synthetic_block(0)).unwrap();
        out.write_block(&synthetic_block(1)).unwrap();
        out.retract_from(1).unwrap();
        out.finish().unwrap();
        assert!(dir.join("blocks/part-00000.parquet").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `clickhouse-client` stand-in that both sinks `RowBinary` inserts to
    /// `<script>.<table>.out` and logs `--query=` probes to `<script>.log`.
    fn fake_ch_client(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("bidx-live-ch-{tag}-{}", std::process::id()));
        let body = "#!/bin/sh\nq=''\nfor a in \"$@\"; do case \"$a\" in --query=*) q=\"$a\";; esac; done\n\
                    case \"$q\" in\n\
                    --query=INSERT*) t=$(echo \"$q\" | sed -E 's/.*INTO [^.]+[.]([^ ]+) .*/\\1/'); cat > \"$0.$t.out\";;\n\
                    *system.tables*) echo \"$q\" >> \"$0.log\"; echo 0;;\n\
                    *) echo \"$q\" >> \"$0.log\";;\n\
                    esac\nexit 0\n"
            .to_string();
        use std::os::unix::fs::PermissionsExt;
        let tmp =
            std::env::temp_dir().join(format!("bidx-live-ch-{tag}-{}-tmp", std::process::id()));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    #[test]
    fn live_output_clickhouse_writes_and_retracts_through_the_enum() {
        let _g = crate::testlock::fork_guard();
        let client = fake_ch_client("output-ch");
        let loader = ChLoader::with_client(
            ChConfig {
                database: "db".into(),
                ..ChConfig::default()
            },
            client.clone(),
        );
        let writer = LiveChWriter::open(
            &ChConfig {
                database: "db".into(),
                ..ChConfig::default()
            },
            client.clone(),
            false,
        )
        .unwrap();
        let mut out = LiveOutput::ClickHouse { writer, loader };

        let mut block = synthetic_block(5);
        // A witness payload exercises the ClickHouse arm's witness loop; the
        // writer was opened without capture, so `write_witness` is a no-op but
        // the iteration itself must still be covered.
        block.witnesses.push(bidx_core::WitnessRow {
            height: 5,
            tx_index: 0,
            input_index: 0,
            item_index: 0,
            item_len: 1,
            item_bytes: vec![0xAA],
        });
        out.write_block(&block).unwrap();
        // A disconnect at height 5 deletes it back out by height.
        out.retract_from(5).unwrap();
        out.finish().unwrap();

        // The block row reached the blocks pipe.
        let blocks_out = std::fs::read(format!("{}.blocks.out", client.display())).unwrap();
        assert_eq!(blocks_out.len(), 144);
        // The retraction issued deletes for every base table at the floor.
        let log = std::fs::read_to_string(format!("{}.log", client.display())).unwrap_or_default();
        for table in ["blocks", "transactions", "inputs", "outputs"] {
            assert!(
                log.contains(&format!("DELETE FROM db.{table} WHERE height >= 5")),
                "missing retraction for {table}: {log}"
            );
        }

        for t in ["blocks", "transactions", "inputs", "outputs"] {
            let _ = std::fs::remove_file(format!("{}.{t}.out", client.display()));
        }
        let _ = std::fs::remove_file(format!("{}.log", client.display()));
        let _ = std::fs::remove_file(&client);
    }
}

// coverage-exempt — `run_live` wires up RPC/ZMQ/UTXO and then enters
// `LiveTracker::run`, which is an infinite tip-watching loop that cannot return
// under test. Its early failure paths (a bad `--utxo` path) are exercised by
// `main.rs`'s `run_live_arm_builds_config_then_fails_on_a_bad_utxo_path`; the
// disk pre-flight, output construction, and event handling below need a real
// node and so are not reachable in a unit fixture.
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
            capture_witness: cfg.capture_witness,
        },
    );

    // Optional output: rolling Parquet parts and/or direct ClickHouse inserts.
    let mut output: Option<LiveOutput> = if cfg.ch_live {
        let loader =
            ChLoader::new(cfg.ch.clone()).context("init ClickHouse client for live inserts")?;
        let writer = LiveChWriter::open(&cfg.ch, loader.client_path(), cfg.capture_witness)
            .context("open ClickHouse live insert pipes")?;
        Some(LiveOutput::ClickHouse { writer, loader })
    } else {
        match &cfg.out {
            Some(d) => {
                let start_part = utxo
                    .tip()?
                    .map(|t| (t.height + 1) / LIVE_BLOCKS_PER_PART)
                    .unwrap_or(0);
                let sink_cfg = SinkConfig {
                    capture_witness: cfg.capture_witness,
                    ..SinkConfig::default()
                };
                Some(LiveOutput::Parquet(LiveSink::create_with(
                    d, start_part, sink_cfg,
                )?))
            }
            None => None,
        }
    };

    tracker.run(|event, block| {
        match &event {
            TrackerEvent::Applied { height, hash } => {
                info!(height, %hash, "applied block");
                if let (Some(out), Some(b)) = (&mut output, block) {
                    if let Err(e) = out.write_block(b) {
                        warn!(error = %e, "failed to write applied block");
                    }
                }
                if let Err(e) = recorder.on_block_applied(*height).map(|_| ()) {
                    warn!(error = %e, "failed to record disk checkpoint");
                }
            }
            TrackerEvent::Disconnected { height, hash } => {
                warn!(height, %hash, "disconnected block (reorg)");
                // Retract the disconnected height from whichever output is
                // active. Parquet rows still in the open part are dropped
                // exactly; a floor below the open part is recorded for the
                // loader. ClickHouse rows were inserted on apply, so they are
                // deleted by height.
                if let Some(out) = output.as_mut() {
                    if let Err(e) = out.retract_from(*height) {
                        warn!(error = %e, "failed to retract disconnected height");
                    }
                }
            }
            TrackerEvent::ReorgStart { .. } => {
                warn!("reorg detected; disconnecting to common ancestor");
            }
            TrackerEvent::CaughtUp { height } => {
                info!(height, "caught up to node tip");
            }
        }
    })?;

    // Flush any partially-filled part and close the ClickHouse inserts.
    if let Some(out) = output {
        out.finish().context("flushing live output on shutdown")?;
    }

    Ok(())
}
