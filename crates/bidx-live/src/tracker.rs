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
    let mut hdr = [0u8; 80];
    hdr.copy_from_slice(&raw[..80]);
    Ok((BlockHeader::parse(&hdr), hdr))
}
