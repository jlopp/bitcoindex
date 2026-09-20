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

pub struct NodeClient {
    cfg: RpcConfig,
    http: reqwest::blocking::Client,
    auth_header: String,
}

impl NodeClient {
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
            cfg,
            http,
            auth_header,
        })
    }

    fn call<T: for<'de> Deserialize<'de>>(&self, method: &str, params: serde_json::Value) -> Result<T, RpcError> {
        let body = serde_json::json!({
            "jsonrpc": "1.0",
            "id": "bidx",
            "method": method,
            "params": params,
        });
        let resp = self
            .http
            .post(&self.cfg.url)
            .header("Authorization", &self.auth_header)
            .json(&body)
            .send()?;
        let parsed: RpcResp<T> = resp.json()?;
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
}
