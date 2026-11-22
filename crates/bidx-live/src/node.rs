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

    /// Transport that answers `getchaintips` with a fixed list and serves
    /// `getblockheader` by looking the requested hash up in a parent map.
    struct TipTransport {
        tips: serde_json::Value,
        /// hash(hex) -> previous hash(hex) or None
        parents: std::collections::HashMap<String, Option<String>>,
    }

    impl RpcTransport for TipTransport {
        fn roundtrip(&self, body: &serde_json::Value) -> Result<serde_json::Value, RpcError> {
            let method = body["method"].as_str().unwrap_or("");
            let result = match method {
                "getchaintips" => self.tips.clone(),
                "getblockheader" => {
                    let want = body["params"][0].as_str().unwrap_or("").to_string();
                    let prev = self.parents.get(&want).cloned().flatten();
                    serde_json::json!({
                        "hash": want,
                        "height": 1,
                        "confirmations": 0,
                        "previousblockhash": prev,
                    })
                }
                _ => serde_json::Value::Null,
            };
            Ok(serde_json::json!({ "result": result, "error": null }))
        }
    }

    #[test]
    fn chain_tips_deserializes_branchlen() {
        let tips = serde_json::json!([{
            "height": 800_000u32,
            "hash": "aa",
            "branchlen": 3u32,
            "status": "valid-fork",
        }]);
        let c = NodeClient::with_transport(Box::new(TipTransport {
            tips,
            parents: Default::default(),
        }));
        let got = c.chain_tips().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].height, 800_000);
        assert_eq!(got[0].branch_len, 3);
        assert_eq!(got[0].status, "valid-fork");
    }

    #[test]
    fn orphan_block_hashes_skips_active_and_headers_only_and_walks_branches() {
        // Active and headers-only tips are ignored; a valid-fork tip is walked
        // back `branchlen` steps via getblockheader.
        let h1 = format!("{:064x}", 0x1111u64);
        let h0 = format!("{:064x}", 0x2222u64);
        let tips = serde_json::json!([
            { "height": 10u32, "hash": format!("{:064x}", 1u64), "branchlen": 0u32, "status": "active" },
            { "height": 9u32,  "hash": format!("{:064x}", 2u64), "branchlen": 0u32, "status": "headers-only" },
            { "height": 8u32,  "hash": h1, "branchlen": 2u32, "status": "valid-fork" },
        ]);
        let mut parents = std::collections::HashMap::new();
        parents.insert(h1.clone(), Some(h0.clone()));
        parents.insert(h0.clone(), None);
        let c = NodeClient::with_transport(Box::new(TipTransport { tips, parents }));
        let set = c.orphan_block_hashes().unwrap();
        // Both H1 and H0 must be present.
        let h1b = Hash32::from_hex(&h1).unwrap();
        let h0b = Hash32::from_hex(&h0).unwrap();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&h1b));
        assert!(set.contains(&h0b));
    }

    #[test]
    fn orphan_block_hashes_rejects_a_bad_tip_hash() {
        let tips = serde_json::json!([
            { "height": 8u32, "hash": "zz-not-hex", "branchlen": 1u32, "status": "valid-fork" },
        ]);
        let c = NodeClient::with_transport(Box::new(TipTransport {
            tips,
            parents: Default::default(),
        }));
        assert!(matches!(c.orphan_block_hashes(), Err(RpcError::BadResponse(_))));
    }

    #[test]
    fn orphan_block_hashes_breaks_at_genesis_with_no_parent() {
        // A branch whose walk runs off the start of the chain: `getblockheader`
        // reports no `previousblockhash`, so the `None => break` arm fires
        // before `branchlen` is exhausted. The hash walked so far is still
        // recorded, and the call returns Ok rather than erroring.
        let tip = format!("{:064x}", 0x4444u64);
        let tips = serde_json::json!([
            { "height": 5u32, "hash": tip, "branchlen": 3u32, "status": "valid-fork" },
        ]);
        // No parents registered → TipTransport reports `previousblockhash: null`.
        let c = NodeClient::with_transport(Box::new(TipTransport {
            tips,
            parents: Default::default(),
        }));
        let set = c.orphan_block_hashes().unwrap();
        assert_eq!(set.len(), 1, "only the tip is recorded before the break");
        assert!(set.contains(&Hash32::from_hex(&tip).unwrap()));
    }

    #[test]
    fn node_client_new_requires_credentials_and_accepts_both_forms() {
        // No credentials at all → BadResponse before any network use.
        let e = NodeClient::new(RpcConfig {
            url: "http://127.0.0.1:1".into(),
            cookie_path: None,
            user: None,
            password: None,
        })
        .err()
        .unwrap();
        assert!(matches!(e, RpcError::BadResponse(_)), "{e:?}");

        // user/password form → Ok (no request is made at construction).
        let c = NodeClient::new(RpcConfig {
            url: "http://127.0.0.1:1".into(),
            cookie_path: None,
            user: Some("u".into()),
            password: Some("p".into()),
        });
        assert!(c.is_ok());

        // cookie-file form → Ok, and a missing cookie file is an Io error.
        let cookie = std::env::temp_dir().join(format!("bidx-cookie-{}", std::process::id()));
        std::fs::write(&cookie, b"__cookie__:secret\n").unwrap();
        let c = NodeClient::new(RpcConfig {
            url: "http://127.0.0.1:1".into(),
            cookie_path: Some(cookie.clone()),
            user: None,
            password: None,
        });
        assert!(c.is_ok());
        let _ = std::fs::remove_file(&cookie);

        let e = NodeClient::new(RpcConfig {
            url: "http://127.0.0.1:1".into(),
            cookie_path: Some(std::env::temp_dir().join("definitely-missing-cookie")),
            user: None,
            password: None,
        })
        .err()
        .unwrap();
        assert!(matches!(e, RpcError::Io(_)), "{e:?}");
    }

    #[test]
    fn orphan_block_hashes_rejects_a_bad_parent_hash() {
        // The tip hash is valid but `getblockheader` reports a non-hex
        // `previousblockhash`; the walk must surface a BadResponse rather than
        // panic or silently truncate the branch.
        let tip = format!("{:064x}", 0x3333u64);
        struct BadParent(String);
        impl RpcTransport for BadParent {
            fn roundtrip(&self, body: &serde_json::Value) -> Result<serde_json::Value, RpcError> {
                let method = body["method"].as_str().unwrap_or("");
                let result = match method {
                    "getchaintips" => serde_json::json!([{
                        "height": 8u32, "hash": self.0, "branchlen": 2u32, "status": "valid-fork",
                    }]),
                    "getblockheader" => serde_json::json!({
                        "hash": self.0,
                        "height": 7,
                        "confirmations": 0,
                        "previousblockhash": "not-hex-either",
                    }),
                    _ => serde_json::Value::Null,
                };
                Ok(serde_json::json!({ "result": result, "error": null }))
            }
        }
        let c = NodeClient::with_transport(Box::new(BadParent(tip)));
        assert!(matches!(c.orphan_block_hashes(), Err(RpcError::BadResponse(_))));
    }

    /// A one-shot HTTP server that answers a single request with a fixed body,
    /// so `HttpTransport::roundtrip` can be exercised end to end (header,
    /// serialization, send, and JSON parse) without a real bitcoind.
    fn one_shot_server(body: String) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes());
        });
        (format!("http://{addr}"), handle)
    }

    #[test]
    fn http_transport_roundtrips_over_a_real_socket() {
        // Serve a valid JSON-RPC reply; `get_block_count` must parse it via the
        // real HttpTransport (auth header + serde round trip).
        let (url, h) = one_shot_server(r#"{"result":123,"error":null}"#.to_string());
        let c = NodeClient::new(RpcConfig {
            url,
            cookie_path: None,
            user: Some("u".into()),
            password: Some("p".into()),
        })
        .unwrap();
        assert_eq!(c.get_block_count().unwrap(), 123);
        h.join().unwrap();
    }

    #[test]
    fn http_transport_surfaces_a_transport_error() {
        // Nothing is listening on this port: the reqwest send must surface as
        // RpcError::Http rather than panicking.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // free the port so the connect is refused
        let c = NodeClient::new(RpcConfig {
            url: format!("http://{addr}"),
            cookie_path: None,
            user: Some("u".into()),
            password: Some("p".into()),
        })
        .unwrap();
        assert!(matches!(c.get_block_count(), Err(RpcError::Http(_))));
    }
}
