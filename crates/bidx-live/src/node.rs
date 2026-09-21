//! Minimal Bitcoin Core RPC client (blocking, cookie or user/pass auth).
//!
//! Only the handful of calls the live tracker needs:
//!   getblockcount, getblockhash, getblockheader, getblock (verbosity 0 -> raw).

use bidx_core::Hash32;
use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// e.g. "http://127.0.0.1:8332"
    pub url: String,
    /// Path to the node's `.cookie` file. Used when `user`/`password` absent.
    pub cookie_path: Option<PathBuf>,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockHeaderInfo {
    pub hash: String,
    pub height: u32,
    pub confirmations: i64,
    #[serde(rename = "previousblockhash")]
    pub previous: Option<String>,
    #[serde(rename = "nextblockhash")]
    pub next: Option<String>,
}

/// Entry in `getchaintips` response.
#[derive(Debug, Clone, Deserialize)]
pub struct ChainTip {
    pub height: u32,
    pub hash: String,
    #[serde(rename = "branchlen")]
    pub branch_len: u32,
    /// One of "invalid", "headers-only", "valid-headers", "valid-fork",
    /// "active". Stale/orphaned blocks live on any branch that is NOT
    /// "active" and NOT "headers-only" (headers-only branches contain no
    /// block data on disk, so cannot appear in blk*.dat).
    pub status: String,
}

#[derive(Deserialize)]
struct RpcResp<T> {
    result: Option<T>,
    error: Option<RpcErr>,
}
#[derive(Deserialize)]
struct RpcErr {
    code: i64,
    message: String,
}

/// Transport the client uses to actually deliver a JSON-RPC round trip.
/// Injecting this behind a trait lets tests run hermetically without a live
/// node. Implementations must be cheap to call (blocking is fine).
pub trait RpcTransport: Send + Sync {
    fn roundtrip(&self, body: &serde_json::Value) -> Result<serde_json::Value, RpcError>;
}

/// HTTP transport to the configured Bitcoin Core endpoint.
pub struct HttpTransport {
    http: reqwest::blocking::Client,
    url: String,
    auth_header: String,
}

impl RpcTransport for HttpTransport {
    fn roundtrip(&self, body: &serde_json::Value) -> Result<serde_json::Value, RpcError> {
        let resp = self
            .http
            .post(&self.url)
            .header("Authorization", &self.auth_header)
            .json(body)
            .send()?;
        Ok(resp.json()?)
    }
}

pub struct NodeClient {
    transport: Box<dyn RpcTransport>,
}

impl NodeClient {
    /// Build a client using HTTP transport against `cfg`.
    pub fn new(cfg: RpcConfig) -> Result<Self, RpcError> {
        let auth = if let (Some(u), Some(p)) = (&cfg.user, &cfg.password) {
            format!("{}:{}", u, p)
        } else if let Some(cp) = &cfg.cookie_path {
            std::fs::read_to_string(cp)?.trim().to_string()
        } else {
            return Err(RpcError::BadResponse(
                "no rpc credentials: provide user/password or cookie_path".into(),
            ));
        };
        let auth_header = format!("Basic {}", base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            auth,
        ));
        let http = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        Ok(NodeClient {
            transport: Box::new(HttpTransport {
                http,
                url: cfg.url,
                auth_header,
            }),
        })
    }

    /// Build a client with a custom transport (tests, alternate backends).
    pub fn with_transport(transport: Box<dyn RpcTransport>) -> Self {
        NodeClient { transport }
    }

    fn call<T: for<'de> Deserialize<'de>>(&self, method: &str, params: serde_json::Value) -> Result<T, RpcError> {
        let body = serde_json::json!({
            "jsonrpc": "1.0",
            "id": "bidx",
            "method": method,
            "params": params,
        });
        let resp = self.transport.roundtrip(&body)?;
        let parsed: RpcResp<T> = serde_json::from_value(resp)?;
        if let Some(e) = parsed.error {
            return Err(RpcError::Rpc {
                code: e.code,
                message: e.message,
            });
        }
        parsed
            .result
            .ok_or_else(|| RpcError::BadResponse(format!("{method}: null result")))
    }

    pub fn get_block_count(&self) -> Result<u32, RpcError> {
        self.call("getblockcount", serde_json::json!([]))
    }

    /// Returns the block hash in internal (wire) byte order.
    pub fn get_block_hash(&self, height: u32) -> Result<Hash32, RpcError> {
        let hex: String = self.call("getblockhash", serde_json::json!([height]))?;
        Hash32::from_hex(&hex).map_err(|e| RpcError::BadResponse(e.to_string()))
    }

    pub fn get_block_header(&self, hash: &Hash32) -> Result<BlockHeaderInfo, RpcError> {
        self.call("getblockheader", serde_json::json!([hash.to_hex()]))
    }

    /// Raw block bytes (witness serialization included), verbosity 0.
    pub fn get_block_raw(&self, hash: &Hash32) -> Result<Vec<u8>, RpcError> {
        let hex: String = self.call("getblock", serde_json::json!([hash.to_hex(), 0]))?;
        hex::decode(hex).map_err(|e| RpcError::BadResponse(e.to_string()))
    }

    /// `getchaintips` — returns all chain tips the node knows about, active
    /// and otherwise. Stale blocks are on the non-active branches; we walk
    /// back `branch_len` hashes per stale branch to compute the orphan set.
    pub fn chain_tips(&self) -> Result<Vec<ChainTip>, RpcError> {
        self.call("getchaintips", serde_json::json!([]))
    }

    /// Return the set of block hashes that are NOT on the active chain and
    /// should be skipped during indexing (orphans / stale side branches).
    ///
    /// "headers-only" tips are excluded: they have no block data on disk,
    /// so they cannot appear in blk*.dat and don't need filtering.
    /// "invalid" tips are included (the invalid branch's blocks should be
    /// skipped too).
    pub fn orphan_block_hashes(&self) -> Result<rustc_hash::FxHashSet<Hash32>, RpcError> {
        let mut out = rustc_hash::FxHashSet::default();
        for tip in self.chain_tips()? {
            if tip.status == "active" || tip.status == "headers-only" {
                continue;
            }
            // Walk back along this branch to collect hashes.
            let mut cur_hash: Hash32 = match Hash32::from_hex(&tip.hash) {
                Ok(h) => h,
                Err(e) => return Err(RpcError::BadResponse(format!(
                    "getchaintips bad hash: {e}"
                ))),
            };
            for _ in 0..tip.branch_len {
                out.insert(cur_hash);
                let hdr: BlockHeaderInfo = self.get_block_header(&cur_hash)?;
                match hdr.previous.as_deref() {
                    Some(prev) => {
                        cur_hash = Hash32::from_hex(prev).map_err(|e| {
                            RpcError::BadResponse(e.to_string())
                        })?;
                    }
                    None => break,
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Scriptable transport: returns each element of `script` in order, then
    /// keeps replaying the last. Calls are recorded so we can assert which
    /// RPCs the tracker made, in what order.
    struct ScriptTransport {
        script: Vec<serde_json::Value>,
        calls: AtomicUsize,
    }

    impl ScriptTransport {
        fn scripted(resp_list: Vec<serde_json::Value>) -> Self {
            ScriptTransport { script: resp_list, calls: AtomicUsize::new(0) }
        }
    }

    impl RpcTransport for ScriptTransport {
        fn roundtrip(&self, _body: &serde_json::Value) -> Result<serde_json::Value, RpcError> {
            let i = self.calls.fetch_add(1, Ordering::SeqCst);
            let idx = i.min(self.script.len().saturating_sub(1));
            Ok(self.script[idx].clone())
        }
    }

    fn client_with(script: Vec<serde_json::Value>) -> NodeClient {
        NodeClient::with_transport(Box::new(ScriptTransport::scripted(script)))
    }

    #[test]
    fn get_block_count_rendering_and_rpc_errors() {
        let c = client_with(vec![serde_json::json!({
            "result": 42u32,
            "error": null,
        })]);
        assert_eq!(c.get_block_count().unwrap(), 42);

        // RPC error path.
        let c = client_with(vec![serde_json::json!({
            "result": null,
            "error": { "code": -5, "message": "not found" },
        })]);
        let e = c.get_block_count().err().unwrap();
        match e {
            RpcError::Rpc { code, message } => {
                assert_eq!(code, -5);
                assert_eq!(message, "not found");
            }
            other => panic!("wrong error: {:?}", other),
        }

        // Null both → BadResponse.
        let c = client_with(vec![serde_json::json!({ "result": null, "error": null })]);
        assert!(matches!(c.get_block_count(), Err(RpcError::BadResponse(_))));
    }

    #[test]
    fn get_block_hash_hex_to_internal_order_roundtrip() {
        let c = client_with(vec![serde_json::json!({
            "result": format!("{:064x}", 0x1234u64),
            "error": null,
        })]);
        let h = c.get_block_hash(123).unwrap();
        // Internal wire order: hex display is big-endian, internal storage
        // reversed. 0x1234 big-endian display = "00...001234", so internal
        // bytes are [0x34, 0x12, 0, 0, 0...].
        assert_eq!(h.0[0], 0x34);
        assert_eq!(h.0[1], 0x12);
        assert!(h.0[2..32].iter().all(|&b| b == 0));

        // Invalid hex from the node → BadResponse, not a panic.
        let c = client_with(vec![serde_json::json!({ "result": "zz", "error": null })]);
        assert!(matches!(c.get_block_hash(1), Err(RpcError::BadResponse(_))));
    }

    #[test]
    fn get_block_raw_hex_decode_and_bad_hex() {
        let raw = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let c = client_with(vec![serde_json::json!({
            "result": hex::encode(&raw),
            "error": null,
        })]);
        let h = Hash32::ZERO;
        assert_eq!(c.get_block_raw(&h).unwrap(), raw);

        let c = client_with(vec![serde_json::json!({ "result": "xx", "error": null })]);
        assert!(matches!(c.get_block_raw(&Hash32::ZERO), Err(RpcError::BadResponse(_))));
    }

    #[test]
    fn get_block_header_deserializes_optional_fields() {
        let c = client_with(vec![serde_json::json!({
            "result": {
                "hash": "00000000",
                "height": 800000u32,
                "confirmations": 5,
                "previousblockhash": "prev"
                // no "nextblockhash" → Option::None path exercised
            },
            "error": null,
        })]);
        let h = c.get_block_header(&Hash32::ZERO).unwrap();
        assert_eq!(h.height, 800000);
        assert_eq!(h.hash, "00000000");
        assert_eq!(h.previous.as_deref(), Some("prev"));
        assert!(h.next.is_none());
    }

    /// Round-trips that a malformed JSON itself hits the Json error variant,
    /// proving the transport roundtrip path is wired.
    #[test]
    fn unparseable_transport_response_is_json_error() {
        struct Bad;
        impl RpcTransport for Bad {
            fn roundtrip(&self, _b: &serde_json::Value) -> Result<serde_json::Value, RpcError> {
                Ok(serde_json::json!("this is not an RpcResp object"))
            }
        }
        let c = NodeClient::with_transport(Box::new(Bad));
        let e = c.get_block_count().err().unwrap();
        assert!(matches!(e, RpcError::Json(_)), "{:?}", e);
    }
}
