//! Live tip tracking for the indexer.
//!
//! Subscribes to Bitcoin Core's ZMQ `rawblock` feed for low-latency new-block
//! notifications, applies each block forward through the UTXO store, and
//! handles chain reorganizations by disconnecting blocks via the undo log.
//!
//! On startup it reconciles with the node over RPC: it walks back from the
//! stored tip to find the common ancestor with the node's current best
//! chain, disconnects any orphaned blocks, then fetches and applies any
//! blocks the indexer missed while offline.

pub mod node;
pub mod tracker;
pub mod zmqsub;

pub use node::{NodeClient, RpcConfig};
pub use tracker::{LiveTracker, TrackerConfig, TrackerEvent};
pub use zmqsub::{ZmqConfig, ZmqSubscriber};
