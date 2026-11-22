//! Electrum JSON-RPC method handling.
//!
//! Implements the subset of the Electrum protocol that Fulcrum/electrs clients
//! (and wallets built on them) use for confirmed-chain queries. Mempool
//! methods return empty results, because this index serves confirmed history
//! only — that is honest and keeps `get_mempool`/fee methods well-defined.
//!
//! Scripthashes arrive in Electrum *display* order (reversed sha256) and are
//! resolved through the `scripthash_map` table.

use crate::config::Config;
use crate::store::{self, Store, StoreError};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// A JSON-RPC error (code + message).
#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcError {
    pub fn method_not_found(m: &str) -> Self {
        RpcError {
            code: -32601,
            message: format!("method not found: {m}"),
        }
    }
    pub fn invalid_params(m: &str) -> Self {
        RpcError {
            code: -32602,
            message: format!("invalid params: {m}"),
        }
    }
    pub fn internal(m: &str) -> Self {
        RpcError {
            code: -32603,
            message: format!("internal error: {m}"),
        }
    }
}

impl From<StoreError> for RpcError {
    fn from(e: StoreError) -> Self {
        RpcError::internal(&e.to_string())
    }
}

/// The Electrum surface.
pub struct Electrum<'a> {
    pub store: &'a Store,
    pub cfg: &'a Config,
}

impl<'a> Electrum<'a> {
    pub fn new(store: &'a Store, cfg: &'a Config) -> Self {
        Electrum { store, cfg }
    }

    /// Dispatch a JSON-RPC call. `params` is the JSON-RPC params value (array
    /// or object); positional params are read by index.
    pub fn handle(&self, method: &str, params: &Value) -> Result<Value, RpcError> {
        match method {
            "server.version" => Ok(json!(["bidx", "1.4"])),
            "server.banner" => Ok(json!("bitcoindex serving layer")),
            "server.ping" => Ok(Value::Null),
            "blockchain.headers.subscribe" => self.headers_subscribe(),
            "blockchain.block.header" => self.block_header(params),
            "blockchain.block.headers" => self.block_headers(params),
            "blockchain.scripthash.get_balance" => self.get_balance(params),
            "blockchain.scripthash.get_history" => self.get_history(params),
            "blockchain.scripthash.get_mempool" => Ok(json!([])),
            "blockchain.scripthash.listunspent" => self.list_unspent(params),
            "blockchain.scripthash.subscribe" => self.subscribe(params),
            "blockchain.estimatefee" => Ok(json!(-1)),
            "blockchain.relayfee" => Ok(json!(0.00001)),
            "mempool.get_fee_histogram" => Ok(json!([])),
            other => Err(RpcError::method_not_found(other)),
        }
    }

    /// Resolve an Electrum scripthash param (display order) to `(type, hash)`.
    fn resolve(&self, params: &Value) -> Result<(u8, [u8; 32]), RpcError> {
        let sh = params
            .get(0)
            .and_then(|v| v.as_str())
            .ok_or_else(|| RpcError::invalid_params("missing scripthash"))?;
        let wire =
            store::reverse_hex(sh).map_err(|_| RpcError::invalid_params("bad scripthash"))?;
        let bytes =
            store::parse_hex32(&wire).map_err(|_| RpcError::invalid_params("bad scripthash"))?;
        self.store
            .resolve_scripthash(&bytes)?
            .ok_or_else(|| RpcError::invalid_params("unknown scripthash"))
    }

    fn headers_subscribe(&self) -> Result<Value, RpcError> {
        let h = self
            .store
            .tip_height()?
            .ok_or_else(|| RpcError::internal("no blocks indexed"))?;
        let block = self
            .store
            .block_by_height(h)?
            .ok_or_else(|| RpcError::internal("tip block missing"))?;
        Ok(json!({ "height": h, "hex": block.header_hex()? }))
    }

    fn block_header(&self, params: &Value) -> Result<Value, RpcError> {
        let height = params
            .get(0)
            .and_then(|v| v.as_u64())
            .ok_or_else(|| RpcError::invalid_params("missing height"))? as u32;
        let block = self
            .store
            .block_by_height(height)?
            .ok_or_else(|| RpcError::invalid_params("height out of range"))?;
        Ok(json!(block.header_hex()?))
    }

    fn block_headers(&self, params: &Value) -> Result<Value, RpcError> {
        let start = params.get(0).and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let count = params.get(1).and_then(|v| v.as_u64()).unwrap_or(1) as u32;
        let count = count.min(2016); // Electrum protocol cap
        let mut hex = String::new();
        let mut got = 0u32;
        for h in start..start.saturating_add(count) {
            match self.store.block_by_height(h)? {
                Some(b) => {
                    hex.push_str(&b.header_hex()?);
                    got += 1;
                }
                None => break,
            }
        }
        Ok(json!({ "count": got, "max": 2016, "hex": hex }))
    }

    fn get_balance(&self, params: &Value) -> Result<Value, RpcError> {
        let (_, hash) = self.resolve(params)?;
        let confirmed = match self.store.address_stats(&hash)? {
            Some(s) => s.funded_sum.saturating_sub(s.spent_sum),
            None => 0,
        };
        Ok(json!({ "confirmed": confirmed, "unconfirmed": 0 }))
    }

    fn get_history(&self, params: &Value) -> Result<Value, RpcError> {
        let (_, hash) = self.resolve(params)?;
        let rows = self.store.address_history(&hash, 1000)?;
        // Dedup by txid, preserving first-seen order.
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for r in rows {
            let disp = store::reverse_hex(&r.txid).unwrap_or(r.txid.clone());
            if seen.insert(disp.clone()) {
                out.push(json!({ "tx_hash": disp, "height": r.height }));
            }
        }
        Ok(json!(out))
    }

    fn list_unspent(&self, params: &Value) -> Result<Value, RpcError> {
        let (_, hash) = self.resolve(params)?;
        let rows = self.store.address_utxos(&hash, 1000)?;
        let out: Vec<Value> = rows
            .iter()
            .map(|u| {
                json!({
                    "tx_hash": store::reverse_hex(&u.txid).unwrap_or(u.txid.clone()),
                    "tx_pos": u.output_index,
                    "height": u.height,
                    "value": u.value_sat,
                })
            })
            .collect();
        Ok(json!(out))
    }

    /// `blockchain.scripthash.subscribe` returns the current status hash (or
    /// null when the address has no history). Without a mempool the status is
    /// a pure function of confirmed history, computed the Electrum way:
    /// `sha256(concat("tx_hash:height:" for each entry, in block order))`.
    fn subscribe(&self, params: &Value) -> Result<Value, RpcError> {
        let (_, hash) = self.resolve(params)?;
        let rows = self.store.address_history(&hash, 1000)?;
        Ok(match scripthash_status(&rows) {
            Some(s) => json!(s),
            None => Value::Null,
        })
    }
}

/// Compute the Electrum status hash over history rows (already height-ordered
/// by the query). Returns `None` for empty history.
fn scripthash_status(rows: &[store::HistoryRow]) -> Option<String> {
    if rows.is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    for r in rows {
        let disp = store::reverse_hex(&r.txid).unwrap_or(r.txid.clone());
        hasher.update(format!("{disp}:{}:", r.height).as_bytes());
    }
    Some(store::to_hex(&hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{hex64, store, wire};

    fn cfg() -> &'static Config {
        Box::leak(Box::new(Config::default()))
    }

    /// A route mock that maps a scripthash to a fixed type/hash and supplies
    /// history/utxo/stats bodies.
    fn scripthash_setup() -> (Store, std::sync::Arc<crate::testutil::RouteMock>) {
        let map = format!(
            r#"{{"data":[{{"script_type":2,"address_hash":"{}"}}],"rows":1}}"#,
            hex64(0x11)
        );
        store(
            &[("FROM bitcoin.scripthash_map", &map)],
            r#"{"data":[],"rows":0}"#,
        )
    }

    #[test]
    fn server_methods() {
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Electrum::new(&s, &cfg());
        assert_eq!(
            e.handle("server.version", &json!([])).unwrap(),
            json!(["bidx", "1.4"])
        );
        assert_eq!(e.handle("server.ping", &json!([])).unwrap(), Value::Null);
        assert_eq!(
            e.handle("blockchain.estimatefee", &json!([2])).unwrap(),
            json!(-1)
        );
        assert_eq!(
            e.handle("blockchain.relayfee", &json!([])).unwrap(),
            json!(0.00001)
        );
        assert_eq!(
            e.handle("mempool.get_fee_histogram", &json!([])).unwrap(),
            json!([])
        );
        assert!(e.handle("bogus.method", &json!([])).is_err());
    }

    #[test]
    fn headers_subscribe_and_block_header() {
        let disp = hex64(0xaa);
        let block = format!(
            r#"{{"data":[{{"height":800000,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}}],"rows":1}}"#,
            wire(&disp),
            wire(&hex64(0)),
            wire(&hex64(0))
        );
        let (s, _) = store(
            &[
                ("max(height)", r#"{"data":[{"height":800000}],"rows":1}"#),
                ("WHERE height = 999999", r#"{"data":[],"rows":0}"#),
                ("FROM bitcoin.blocks", &block),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Electrum::new(&s, &cfg());

        let r = e
            .handle("blockchain.headers.subscribe", &json!([]))
            .unwrap();
        assert_eq!(r["height"], 800000);
        assert_eq!(r["hex"].as_str().unwrap().len(), 160);

        let r = e
            .handle("blockchain.block.header", &json!([800000]))
            .unwrap();
        assert_eq!(r.as_str().unwrap().len(), 160);

        // Missing height param.
        assert!(e.handle("blockchain.block.header", &json!([])).is_err());
        // Out-of-range height.
        assert!(e
            .handle("blockchain.block.header", &json!([999999]))
            .is_err());
    }

    #[test]
    fn block_headers_range_and_cap() {
        let mk = |h: u32| {
            format!(
                r#"{{"data":[{{"height":{},"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}}],"rows":1}}"#,
                h,
                wire(&hex64(0xaa)),
                wire(&hex64(0)),
                wire(&hex64(0))
            )
        };
        // Heights 10 and 11 present, 12 missing.
        let (s, _) = store(
            &[
                ("WHERE height = 10", &mk(10)),
                ("WHERE height = 11", &mk(11)),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Electrum::new(&s, &cfg());
        let r = e
            .handle("blockchain.block.headers", &json!([10, 5]))
            .unwrap();
        assert_eq!(r["count"], 2, "stops at the first missing height");
        assert_eq!(r["hex"].as_str().unwrap().len(), 320);
        assert_eq!(r["max"], 2016);
        // Defaults: start 0, count 1.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Electrum::new(&s, &cfg());
        let r = e.handle("blockchain.block.headers", &json!([])).unwrap();
        assert_eq!(r["count"], 0);
    }

    #[test]
    fn get_balance_and_history_and_unspent() {
        let stats = r#"{"data":[{"funded_count":2,"funded_sum":10000,"spent_count":1,"spent_sum":4000,"history_count":3}],"rows":1}"#;
        let hist = format!(
            r#"{{"data":[{{"txid":"{}","height":50,"value_sat":6000,"kind":0}},{{"txid":"{}","height":60,"value_sat":-1000,"kind":1}}],"rows":2}}"#,
            wire(&hex64(0xaa)),
            wire(&hex64(0xbb))
        );
        let utxos = format!(
            r#"{{"data":[{{"txid":"{}","output_index":2,"value_sat":"6000","height":50}}],"rows":1}}"#,
            wire(&hex64(0xaa))
        );
        let map = format!(
            r#"{{"data":[{{"script_type":2,"address_hash":"{}"}}],"rows":1}}"#,
            hex64(0x11)
        );
        let (s, _) = store(
            &[
                ("FROM bitcoin.scripthash_map", &map),
                ("FROM bitcoin.address_stats", stats),
                ("FROM bitcoin.address_history", &hist),
                ("utxos_by_address", &utxos),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Electrum::new(&s, &cfg());
        let sh = hex64(0x55); // display-order scripthash

        let r = e
            .handle("blockchain.scripthash.get_balance", &json!([sh]))
            .unwrap();
        assert_eq!(r["confirmed"], 6000);
        assert_eq!(r["unconfirmed"], 0);

        let r = e
            .handle("blockchain.scripthash.get_history", &json!([sh]))
            .unwrap();
        let arr = r.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["tx_hash"], hex64(0xaa));
        assert_eq!(arr[0]["height"], 50);

        let r = e
            .handle("blockchain.scripthash.listunspent", &json!([sh]))
            .unwrap();
        assert_eq!(r[0]["tx_hash"], hex64(0xaa));
        assert_eq!(r[0]["tx_pos"], 2);
        assert_eq!(r[0]["value"], 6000);

        let r = e
            .handle("blockchain.scripthash.get_mempool", &json!([sh]))
            .unwrap();
        assert_eq!(r, json!([]));
    }

    #[test]
    fn get_balance_returns_zero_for_address_with_no_stats() {
        // resolve succeeds (map row present) but address_stats returns no rows.
        let map = format!(
            r#"{{"data":[{{"script_type":2,"address_hash":"{}"}}],"rows":1}}"#,
            hex64(0x11)
        );
        let (s, _) = store(
            &[("FROM bitcoin.scripthash_map", &map)],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Electrum::new(&s, &cfg());
        let r = e
            .handle("blockchain.scripthash.get_balance", &json!([hex64(0x55)]))
            .unwrap();
        assert_eq!(r["confirmed"], 0);
        assert_eq!(r["unconfirmed"], 0);
    }

    #[test]
    fn subscribe_returns_status_hash_and_null_when_empty() {
        let map = format!(
            r#"{{"data":[{{"script_type":2,"address_hash":"{}"}}],"rows":1}}"#,
            hex64(0x11)
        );
        let hist = format!(
            r#"{{"data":[{{"txid":"{}","height":50,"value_sat":6000,"kind":0}}],"rows":1}}"#,
            wire(&hex64(0xaa))
        );
        let (s, _) = store(
            &[
                ("FROM bitcoin.scripthash_map", &map),
                ("FROM bitcoin.address_history", &hist),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Electrum::new(&s, &cfg());
        let r = e
            .handle("blockchain.scripthash.subscribe", &json!([hex64(0x55)]))
            .unwrap();
        // Non-empty history → 64-char status hash.
        assert_eq!(r.as_str().unwrap().len(), 64);

        // Empty history → null.
        let (s, _) = scripthash_setup();
        let e = Electrum::new(&s, &cfg());
        let r = e
            .handle("blockchain.scripthash.subscribe", &json!([hex64(0x55)]))
            .unwrap();
        assert_eq!(r, Value::Null);
    }

    #[test]
    fn status_hash_matches_electrum_algorithm() {
        let rows = vec![store::HistoryRow {
            txid: store::to_hex(&hex_decode(&wire(&hex64(0xaa)))),
            height: 50,
            value_sat: 1,
            kind: 0,
        }];
        let status = scripthash_status(&rows).unwrap();
        // Independent computation.
        let mut h = Sha256::new();
        h.update(format!("{}:50:", hex64(0xaa)).as_bytes());
        let expect = store::to_hex(&h.finalize());
        assert_eq!(status, expect);
        assert_eq!(scripthash_status(&[]), None);
    }

    #[test]
    fn invalid_scripthash_params_error() {
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Electrum::new(&s, &cfg());
        // Missing param.
        assert!(e
            .handle("blockchain.scripthash.get_balance", &json!([]))
            .is_err());
        // Bad hex.
        assert!(e
            .handle("blockchain.scripthash.get_balance", &json!(["zz"]))
            .is_err());
        // Unknown scripthash (no map row).
        assert!(e
            .handle("blockchain.scripthash.get_balance", &json!([hex64(0x55)]))
            .is_err());
    }

    #[test]
    fn error_codes_and_messages() {
        assert_eq!(RpcError::method_not_found("x").code, -32601);
        assert_eq!(RpcError::invalid_params("x").code, -32602);
        assert_eq!(RpcError::internal("x").code, -32603);
        let e: RpcError = StoreError::Transport("boom".into()).into();
        assert_eq!(e.code, -32603);
        assert!(e.message.contains("boom"));
    }

    fn hex_decode(s: &str) -> Vec<u8> {
        store::hex_to_bytes(s).unwrap()
    }
}
