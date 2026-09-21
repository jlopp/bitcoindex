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

/// Parse a multipart ZMQ hashblock message into the 32-byte block hash.
/// Pure function — testable without any socket I/O.
pub(crate) fn parse_hashblock_multipart(msg: &[Vec<u8>]) -> Result<Hash32, ZmqError> {
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
        parse_hashblock_multipart(&msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vb(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }

    #[test]
    fn parses_valid_hashblock_multipart() {
        let topic = vb(b"hashblock");
        let mut raw = [0xabu8; 32];
        raw[0] = 0x01;
        raw[31] = 0xFF;
        let msg = vec![topic, vb(&raw), vb(&[0u8; 4])];
        let h = parse_hashblock_multipart(&msg).unwrap();
        assert_eq!(*h.as_bytes(), raw);
    }

    #[test]
    fn rejects_when_frames_too_few() {
        let msg = vec![vb(b"hashblock")];
        let e = parse_hashblock_multipart(&msg).unwrap_err();
        assert!(
            matches!(e, ZmqError::Malformed(ref s) if s.contains("expected >=2 frames, got 1"))
        );
        let empty: Vec<Vec<u8>> = vec![];
        let e = parse_hashblock_multipart(&empty).unwrap_err();
        assert!(
            matches!(e, ZmqError::Malformed(ref s) if s.contains("expected >=2 frames, got 0"))
        );
    }

    #[test]
    fn tolerates_off_topic_frames_but_still_returns_hash() {
        let msg = vec![vb(b"hashtx"), vb(&[0x12u8; 32])];
        let h = parse_hashblock_multipart(&msg).unwrap();
        assert_eq!(*h.as_bytes(), [0x12u8; 32]);
    }

    #[test]
    fn rejects_short_or_long_payload() {
        for bad_len in [31usize, 33, 0] {
            let msg = vec![vb(b"hashblock"), vec![0u8; bad_len]];
            let e = parse_hashblock_multipart(&msg).unwrap_err();
            match e {
                ZmqError::Malformed(s) => {
                    assert!(s.contains(&format!("payload {} bytes", bad_len)),
                        "missing payload-len info in {s}");
                }
                other => panic!("wrong error: {:?}", other),
            }
        }
    }

    #[test]
    fn error_display_impls() {
        use std::error::Error as _;
        let m = ZmqError::Malformed("foo".into());
        assert_eq!(m.to_string(), "malformed notification: foo");
        assert!(std::error::Error::source(&m).is_none());
    }
}
