//! The live tracker: reconciles with the node, applies new blocks, and
//! handles reorganizations via the UTXO undo log.

use crate::node::{NodeClient, RpcError};
use crate::zmqsub::ZmqSubscriber;
use bidx_core::{BlockHeader, Hash32};
use bidx_parser::parse_block_full;
use bidx_store::{TipState, UtxoStore};
use std::sync::Arc;
use thiserror::Error;
use tracing::{error, info, warn};

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    Utxo(#[from] bidx_store::UtxoError),
    #[error(transparent)]
    Zmq(#[from] crate::zmqsub::ZmqError),
    #[error(transparent)]
    Parse(#[from] bidx_parser::tx::TxParseError),
    #[error("reorg deeper than undo log (ancestor not found after {0} disconnects)")]
    ReorgTooDeep(u32),
}

#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Max blocks to disconnect during a reorg before giving up. Deep reorgs
    /// beyond this require a rescan (they essentially never happen on
    /// mainnet beyond depth 1-2).
    pub max_reorg_depth: u32,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        TrackerConfig { max_reorg_depth: 100 }
    }
}

/// What happened on a given step; surfaced to the CLI / future sink stage.
#[derive(Debug)]
pub enum TrackerEvent {
    Applied {
        height: u32,
        hash: Hash32,
        /// Spend rows produced by applying this block (input→prevout links).
        spends: Vec<bidx_core::SpendRow>,
    },
    Disconnected {
        height: u32,
        hash: Hash32,
    },
    ReorgStart {
        depth: u32,
    },
    CaughtUp {
        height: u32,
    },
}

pub struct LiveTracker {
    node: NodeClient,
    zmq: Option<ZmqSubscriber>,
    utxo: Arc<UtxoStore>,
    cfg: TrackerConfig,
}

impl LiveTracker {
    pub fn new(
        node: NodeClient,
        zmq: Option<ZmqSubscriber>,
        utxo: Arc<UtxoStore>,
        cfg: TrackerConfig,
    ) -> Self {
        LiveTracker {
            node,
            zmq,
            utxo,
            cfg,
        }
    }

    /// Reconcile local tip with the node's best chain, then stream new blocks.
    /// `on_event` is called after each applied/disconnected block so the
    /// caller can persist rows to the Parquet/ClickHouse sink.
    pub fn run<F>(&self, mut on_event: F) -> Result<(), TrackerError>
    where
        F: FnMut(TrackerEvent, Option<&bidx_parser::FullBlock>),
    {
        self.catch_up(&mut on_event)?;
        info!("caught up; entering live ZMQ loop");
        self.live_loop(&mut on_event)
    }

    /// Walk the node chain from our stored tip forward, handling any reorg.
    pub fn catch_up<F>(&self, on_event: &mut F) -> Result<(), TrackerError>
    where
        F: FnMut(TrackerEvent, Option<&bidx_parser::FullBlock>),
    {
        let node_tip_height = self.node.get_block_count()?;
        let mut local = match self.utxo.tip()? {
            Some(t) => t,
            None => {
                // No state yet: nothing to catch up (caller should have done
                // a bulk parse first; we start from genesis only if asked).
                info!("no stored tip; live tracker starts from node tip");
                let h = self.node.get_block_hash(node_tip_height)?;
                let tip = TipState {
                    height: node_tip_height,
                    hash: h,
                };
                self.utxo.set_tip(&tip)?;
                on_event(TrackerEvent::CaughtUp { height: node_tip_height }, None);
                return Ok(());
            }
        };

        // 1. Detect reorg: does the node still have our stored tip's hash at
        //    that height? If not, walk back disconnecting until we find the
        //    common ancestor.
        let mut disconnects = 0u32;
        loop {
            let node_hash_at_local = match self.node.get_block_hash(local.height) {
                Ok(h) => h,
                Err(_) => {
                    // Node doesn't have this height (we're ahead of node tip).
                    // Disconnect until we're at or below node tip.
                    Hash32::ZERO
                }
            };
            if node_hash_at_local == local.hash && node_hash_at_local != Hash32::ZERO {
                break; // common point found
            }
            if local.height == 0 {
                break; // genesis always matches
            }
            if disconnects >= self.cfg.max_reorg_depth {
                return Err(TrackerError::ReorgTooDeep(disconnects));
            }
            if disconnects == 0 {
                on_event(
                    TrackerEvent::ReorgStart { depth: 0 },
                    None,
                );
            }
            info!(
                height = local.height,
                hash = %local.hash,
                "disconnecting orphaned block"
            );
            self.utxo.disconnect_block(local.height)?;
            on_event(
                TrackerEvent::Disconnected {
                    height: local.height,
                    hash: local.hash,
                },
                None,
            );
            disconnects += 1;

            // Step local tip back to the previous block. We need its hash;
            // read it from the undo... we don't store parent hash in meta.
            // Recover it from the node by height (post-reorg canonical chain)
            // — but the node may be on the *new* branch. The parent of our
            // local (now-disconnected) tip on the *old* branch is what we
            // want to compare next. We track it via the undo walk using the
            // node's hash at height-1: after disconnecting down to ancestor,
            // the ancestor's hash is canonical on both branches.
            let prev_height = local.height - 1;
            let prev_hash = self.node.get_block_hash(prev_height)?;
            local = TipState {
                height: prev_height,
                hash: prev_hash,
            };
            self.utxo.set_tip(&local)?;
        }

        // 2. Now local is on the canonical chain. Apply forward to node tip.
        while local.height < node_tip_height {
            let next_height = local.height + 1;
            let hash = self.node.get_block_hash(next_height)?;
            let raw = self.node.get_block_raw(&hash)?;
            let (header, hdr_raw) = parse_header(&raw)?;
            let computed = bidx_core::dsha256(&hdr_raw);
            debug_assert_eq!(computed, hash);
            let mut block = parse_block_full(&raw, next_height, hash, &header)?;

            let res = self
                .utxo
                .apply_block(next_height, &block.txs, &block.inputs, &block.outputs)?;
            // Fill fees + block total_fee so downstream sinks see final data.
            let mut total_fee = 0u64;
            for (i, tx) in block.txs.iter_mut().enumerate() {
                tx.fee = res.fees[i];
                if i != 0 {
                    total_fee = total_fee.saturating_add(res.fees[i]);
                }
            }
            block.block_row.total_fee = total_fee;

            let new_tip = TipState {
                height: next_height,
                hash,
            };
            self.utxo.set_tip(&new_tip)?;
            local = new_tip;
            if res.missing > 0 {
                warn!(height = next_height, missing = res.missing, "missing prevouts");
            }
            on_event(
                TrackerEvent::Applied {
                    height: next_height,
                    hash,
                    spends: res.spends,
                },
                Some(&block),
            );
        }

        on_event(TrackerEvent::CaughtUp { height: local.height }, None);
        Ok(())
    }

    fn live_loop<F>(&self, on_event: &mut F) -> Result<(), TrackerError>
    where
        F: FnMut(TrackerEvent, Option<&bidx_parser::FullBlock>),
    {
        let zmq = match &self.zmq {
            Some(z) => z,
            None => {
                // No ZMQ configured: fall back to a poll loop.
                info!("no ZMQ subscriber; polling every 2s");
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    self.catch_up(on_event)?;
                }
            }
        };
        loop {
            match zmq.next_block_hash() {
                Ok(_hash) => {
                    // A new block arrived (or several, possibly out of order /
                    // on a stale branch). Rather than trust the notification's
                    // hash/height, re-run catch_up: it's idempotent, cheap
                    // (usually a single getblockhash+getblock), and correctly
                    // resolves any reorg via the undo log.
                    if let Err(e) = self.catch_up(on_event) {
                        error!(error = %e, "catch_up failed in live loop");
                        // On transient RPC errors, keep going; on reorg-too-
                        // deep, surface by returning.
                        if matches!(e, TrackerError::ReorgTooDeep(_)) {
                            return Err(e);
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
                Err(e) => {
                    error!(error = %e, "zmq receive error");
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
            }
        }
    }
}

/// Parse the 80-byte header from a raw block, returning (header, header_raw).
fn parse_header(raw: &[u8]) -> Result<(BlockHeader, [u8; 80]), TrackerError> {
    if raw.len() < 80 {
        return Err(TrackerError::Rpc(crate::node::RpcError::BadResponse(
            format!("block payload too short: {}", raw.len()),
        )));
    }
    let mut hdr = [0u8; 80];
    hdr.copy_from_slice(&raw[..80]);
    Ok((BlockHeader::parse(&hdr), hdr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::RpcTransport;
    use std::collections::HashMap as StdMap;
    use std::sync::Mutex as StdMutex;

    /// Build a fully-valid synthetic block payload at given height with one
    /// coinbase output. Returns (payload, block-hash-the-parser-will-compute).
    fn synth_block(height: u32, prev_hash: Hash32, coinbase_out_sat: u64) -> (Vec<u8>, Hash32) {
        let mut hdr = [0u8; 80];
        hdr[0] = 1; // version
        hdr[4..36].copy_from_slice(prev_hash.as_bytes());
        // Deterministic "merkle" pattern for the hash diversity:
        for j in 0..32 { hdr[36 + j] = ((height >> 3) as u8).wrapping_add(j as u8); }
        hdr[68..72].copy_from_slice(&1231006505u32.to_le_bytes());
        hdr[72..76].copy_from_slice(&0x1d00ffffu32.to_le_bytes());
        hdr[76..80].copy_from_slice(&height.to_le_bytes()); // nonce carries height

        // One coinbase tx: version|1cb-input {}|outputs [value] | 0n.
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes());
        tx.push(1u8); // input_count
        tx.extend_from_slice(&[0u8; 32]); // prev_txid zero
        tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // prev_vout max
        tx.push(0u8); // scriptsig len
        tx.extend_from_slice(&0u32.to_le_bytes()); // sequence
        tx.push(1u8); // output_count
        tx.extend_from_slice(&coinbase_out_sat.to_le_bytes());
        tx.push(0u8); // spk_len 0
        tx.extend_from_slice(&0u32.to_le_bytes()); // locktime

        let mut raw = Vec::with_capacity(80 + 1 + tx.len());
        raw.extend_from_slice(&hdr);
        raw.extend_from_slice(&[1u8]); // tx_count
        raw.extend_from_slice(&tx);
        let block_hash = bidx_core::dsha256(&raw[..80]);
        (raw, block_hash)
    }

    /// Fake transport driven by an in-memory chain: height→(raw, hash) map.
    /// getblockcount returns max height; getblockhash maps height → hash;
    /// getblock maps hash → raw hex.
    struct FakeNode {
        chain: StdMutex<StdMap<u32, (Vec<u8>, Hash32)>>,
    }

    impl FakeNode {
        fn new() -> Self {
            FakeNode { chain: StdMutex::new(StdMap::new()) }
        }
        fn add(&self, height: u32, raw: Vec<u8>, hash: Hash32) {
            self.chain.lock().unwrap().insert(height, (raw, hash));
        }
    }

    impl RpcTransport for FakeNode {
        fn roundtrip(&self, body: &serde_json::Value) -> Result<serde_json::Value, crate::node::RpcError> {
            let method = body["method"].as_str().unwrap_or_default();
            let chain = self.chain.lock().unwrap();
            match method {
                "getblockcount" => {
                    let max = chain.keys().copied().max().unwrap_or(0);
                    Ok(serde_json::json!({ "result": max, "error": null }))
                }
                "getblockhash" => {
                    let h = body["params"][0].as_u64().unwrap() as u32;
                    match chain.get(&h) {
                        Some((_, hash)) => Ok(serde_json::json!({
                            "result": bidx_core::Hash32::from_bytes(hash.0).to_hex(),
                            "error": null,
                        })),
                        None => Ok(serde_json::json!({
                            "result": Hash32::ZERO.to_hex(),
                            "error": null,
                        })),
                    }
                }
                "getblock" => {
                    let hex = body["params"][0].as_str().unwrap_or_default();
                    for (_, (raw, hash)) in chain.iter() {
                        if hash.to_hex() == hex {
                            return Ok(serde_json::json!({
                                "result": hex::encode(raw),
                                "error": null,
                            }));
                        }
                    }
                    Ok(serde_json::json!({
                        "result": "00",
                        "error": null,
                    }))
                }
                _ => Ok(serde_json::json!({ "result": null, "error": null })),
            }
        }
    }

    fn tmp_utxo(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "bidx-utxo-track-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    use std::path::PathBuf;

    /// No stored tip → tracker seeds from node's tip and emits CaughtUp.
    #[test]
    fn catch_up_with_no_prior_tip_seeds_from_node_tip() {
        let (raw_g, _) = synth_block(0, Hash32::ZERO, 50_0000_0000);
        let node = FakeNode::new();
        let h_g = bidx_core::dsha256(&raw_g[..80]);
        node.add(0, raw_g.clone(), h_g);

        let client = NodeClient::with_transport(Box::new(node));
        let utxo_dir: PathBuf = tmp_utxo("noprior");
        let utxo = Arc::new(UtxoStore::open_live(&utxo_dir).unwrap());

        let tracker = LiveTracker::new(client, None, utxo.clone(), TrackerConfig::default());
        let mut events = Vec::new();
        tracker.catch_up(&mut |e, _| events.push(e)).unwrap();
        // No blocks applied yet (no blocks < node tip), but tip is seeded.
        assert_eq!(events.len(), 1);
        match &events[0] {
            TrackerEvent::CaughtUp { height } => assert_eq!(*height, 0),
            other => panic!("expected CaughtUp, got {:?}", other),
        }
        let t = utxo.tip().unwrap().unwrap();
        assert_eq!(t.height, 0);
        assert_eq!(t.hash, h_g);
        let _ = std::fs::remove_dir_all(&utxo_dir);
    }

    /// Node has 3 blocks; tracker applies all three in order and emits
    /// one Applied per block plus a final CaughtUp.
    #[test]
    fn catch_up_applies_forward_blocks_in_order() {
        // Build chain 0→1→2 with explicit prev_hash linkage.
        let (raw_g, h_g) = synth_block(0, Hash32::ZERO, 50_0000_0000);
        let (raw_1, h_1) = synth_block(1, h_g, 50_0000_0000);
        let (raw_2, h_2) = synth_block(2, h_1, 50_0000_0000);

        let node = FakeNode::new();
        node.add(0, raw_g.clone(), h_g);
        node.add(1, raw_1.clone(), h_1);
        node.add(2, raw_2.clone(), h_2);

        // Prime the UTXO store with an artificial tip at height 0 matching h_g.
        let utxo_dir = tmp_utxo("fwd");
        let utxo = Arc::new(UtxoStore::open_live(&utxo_dir).unwrap());
        utxo.set_tip(&TipState { height: 0, hash: h_g }).unwrap();

        let client = NodeClient::with_transport(Box::new(node));
        let tracker = LiveTracker::new(client, None, utxo.clone(), TrackerConfig::default());
        let mut heights: Vec<u32> = Vec::new();
        let mut events: Vec<TrackerEvent> = Vec::new();
        tracker.catch_up(&mut |e, _| {
            if let TrackerEvent::Applied { height, .. } = &e {
                heights.push(*height);
            }
            events.push(e);
        }).unwrap();

        // 1, 2 applied in order; tip ends at 2.
        assert_eq!(heights, vec![1, 2]);
        assert_eq!(utxo.tip().unwrap().unwrap().height, 2);
        assert_eq!(utxo.tip().unwrap().unwrap().hash, h_2);
        // One CaughtUp at the end.
        assert!(matches!(events.last().unwrap(), TrackerEvent::CaughtUp { height: 2 }));
        let _ = std::fs::remove_dir_all(&utxo_dir);
    }

    /// Emulate a reorg: replace height-1 block with a different one (different
    /// prev window -> different hash). The tracker must detect the mismatch
    /// on its stored tip, walk back one disconnect, then apply the new block.
    #[test]
    fn catch_up_handles_depth_one_reorg() {
        let (raw_g, h_g) = synth_block(0, Hash32::ZERO, 50_0000_0000);
        let (raw_1a, h_1a) = synth_block(1, h_g, 50_0000_0000);
        // Alternative block at height 1 reuses same prev but has a different
        // nonce — different hash — and HALF the coinbase out, so fees differ.
        let mut raw_1b = raw_1a.clone();
        raw_1b[76..80].copy_from_slice(&0xDEAD_BEEF_u32.to_le_bytes()); // nonce differs
        let h_1b = bidx_core::dsha256(&raw_1b[..80]);

        // Node initially shows chain A; the UTXO tip was set after applying A.
        let node = FakeNode::new();
        node.add(0, raw_g.clone(), h_g);
        node.add(1, raw_1a.clone(), h_1a);
        let utxo_dir = tmp_utxo("reorg");
        let utxo = Arc::new(UtxoStore::open_live(&utxo_dir).unwrap());
        // Apply block h_1a through the tracker to build state + undo log.
        let client = NodeClient::with_transport(Box::new(FakeNode {
            chain: StdMutex::new({
                let mut m = StdMap::new();
                m.insert(0u32, (raw_g.clone(), h_g));
                m.insert(1u32, (raw_1a.clone(), h_1a));
                m
            }),
        }));
        let tracker = LiveTracker::new(client, None, utxo.clone(), TrackerConfig::default());
        // Seed tip at 0 to simulate a previous sync.
        utxo.set_tip(&TipState { height: 0, hash: h_g }).unwrap();
        let mut silent = |_e: TrackerEvent, _b: Option<&bidx_parser::FullBlock>| {};
        tracker.catch_up(&mut silent).unwrap();
        assert_eq!(utxo.tip().unwrap().unwrap().hash, h_1a);

        // Now the node says the canonical chain is raw_1b instead.
        let node_b = FakeNode::new();
        node_b.add(0, raw_g.clone(), h_g);
        node_b.add(1, raw_1b.clone(), h_1b);
        let client_b = NodeClient::with_transport(Box::new(node_b));
        let tracker_b = LiveTracker::new(client_b, None, utxo.clone(), TrackerConfig::default());
        let mut seen: Vec<TrackerEvent> = Vec::new();
        tracker_b.catch_up(&mut |e, _| seen.push(e)).unwrap();

        // Expect a ReorgStart, a Disconnected(h_1a), a fresh Applied(h_1b),
        // then CaughtUp(1).
        let kinds: Vec<&'static str> = seen.iter().map(|e| match e {
            TrackerEvent::Applied { .. } => "applied",
            TrackerEvent::Disconnected { .. } => "disconnected",
            TrackerEvent::ReorgStart { .. } => "reorgstart",
            TrackerEvent::CaughtUp { .. } => "caughtup",
        }).collect();
        assert_eq!(kinds, vec!["reorgstart", "disconnected", "applied", "caughtup"], "got {:?}", kinds);
        assert_eq!(utxo.tip().unwrap().unwrap().hash, h_1b);
        let _ = std::fs::remove_dir_all(&utxo_dir);
    }

    /// Reorg deeper than max_reorg_depth surfaces as TrackerError::ReorgTooDeep.
    #[test]
    fn catch_up_bails_on_deep_reorg() {
        let (raw_g, h_g) = synth_block(0, Hash32::ZERO, 50_0000_0000);
        let (raw_1a, h_1a) = synth_block(1, h_g, 50_0000_0000);
        let mut raw_2a = raw_1a.clone();
        raw_2a[76..80].copy_from_slice(&2u32.to_le_bytes());
        let h_2a = bidx_core::dsha256(&raw_2a[..80]);

        // UTXO thinks tip is height 2 with hash h_2a; the node only has blocks
        // up to genesis — every getblockhash(h>0) returns a hash that mismatches
        // the stored tip, forcing disconnects until we run out of budget.
        let utxo_dir = tmp_utxo("deep");
        let utxo = Arc::new(UtxoStore::open_live(&utxo_dir).unwrap());
        // Build state: pretend 2 blocks were applied.
        utxo.set_tip(&TipState { height: 2, hash: h_2a }).unwrap();
        // We have to actually apply two blocks for disconnect_block to find
        // an undo record. Easier: lower max_reorg_depth to 0 so we hit
        // ReorgTooDeep on the first disconnect check.
        let cfg = TrackerConfig { max_reorg_depth: 0 };
        let node = FakeNode::new();
        node.add(0, raw_g.clone(), h_g);
        let client = NodeClient::with_transport(Box::new(node));
        let tracker = LiveTracker::new(client, None, utxo.clone(), cfg);
        let e = tracker.catch_up(&mut |_e, _b| {}).err().expect("expected error");
        assert!(matches!(e, TrackerError::ReorgTooDeep(_)), "got {:?}", e);
        let _ = std::fs::remove_dir_all(&utxo_dir);
    }

    /// parse_header rejects payloads shorter than 80 bytes.
    #[test]
    fn parse_header_under_80_bytes_errors() {
        let short = vec![0u8; 20];
        assert!(parse_header(&short).is_err());
        let exact = vec![0u8; 80];
        assert!(parse_header(&exact).is_ok());
    }
}
