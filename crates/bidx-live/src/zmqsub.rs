//! ZMQ subscriber for Bitcoin Core block/hash notifications.
//!
//! Bitcoin Core publishes multipart messages: [topic, payload, sequence].
//! We subscribe to `hashblock` (32-byte block hash) as the low-bandwidth
//! trigger and fetch full block bytes over RPC — that keeps the ZMQ path
//! simple and guarantees we parse exactly the bytes the node considers
//! canonical (identical to what it writes to disk).

use bidx_core::Hash32;
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum ZmqError {
    #[error(transparent)]
    Zmq(#[from] zmq::Error),
    #[error("malformed notification: {0}")]
    Malformed(String),
}

#[derive(Debug, Clone)]
pub struct ZmqConfig {
    /// e.g. "tcp://127.0.0.1:28332" (the node's -zmqpubhashblock endpoint).
    pub hashblock_endpoint: String,
}

pub struct ZmqSubscriber {
    sock: zmq::Socket,
}

impl ZmqSubscriber {
    pub fn connect(cfg: &ZmqConfig) -> Result<Self, ZmqError> {
        let ctx = zmq::Context::new();
        let sock = ctx.socket(zmq::SUB)?;
        sock.set_subscribe(b"hashblock")?;
        // Don't queue unboundedly if we stall; drop to latest.
        sock.set_rcvhwm(1000)?;
        sock.connect(&cfg.hashblock_endpoint)?;
        Ok(ZmqSubscriber { sock })
    }

    /// Block until the next block-hash notification, returning the hash in
    /// internal wire byte order. ZMQ delivers the hash as the raw 32 bytes
    /// (already little-endian wire order), matching our Hash32 storage.
    pub fn next_block_hash(&self) -> Result<Hash32, ZmqError> {
        let msg = self.sock.recv_multipart(0)?;
        if msg.len() < 2 {
            return Err(ZmqError::Malformed(format!(
                "expected >=2 frames, got {}",
                msg.len()
            )));
        }
        let topic = &msg[0];
        if topic != b"hashblock" {
            warn!(topic = ?String::from_utf8_lossy(topic), "unexpected topic");
        }
        let payload = &msg[1];
        if payload.len() != 32 {
            return Err(ZmqError::Malformed(format!(
                "hashblock payload {} bytes, expected 32",
                payload.len()
            )));
        }
        let mut b = [0u8; 32];
        b.copy_from_slice(payload);
        Ok(Hash32::from_bytes(b))
    }
}
