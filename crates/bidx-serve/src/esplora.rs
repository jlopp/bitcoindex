//! Esplora-compatible REST responses.
//!
//! Each handler returns a [`Response`] (status + JSON body) so the routing
//! layer in `bidx-cli` stays thin and these are testable without a socket.
//! Field names follow the Esplora API used by mempool.space and its clients.
//!
//! Address paths accept either an address string or a 64-char scripthash;
//! both resolve to the stored `(script_type, address_hash)`.

use crate::address;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::store::{self, BlockRow, Store, StoreError};
use serde_json::{json, Value};

/// An HTTP-style response.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub status: u16,
    pub body: Value,
}

impl Response {
    pub fn ok(body: Value) -> Self {
        Response { status: 200, body }
    }
    pub fn not_found(msg: &str) -> Self {
        Response {
            status: 404,
            body: json!({ "error": msg }),
        }
    }
    pub fn bad_request(msg: &str) -> Self {
        Response {
            status: 400,
            body: json!({ "error": msg }),
        }
    }
    pub fn server_error(msg: &str) -> Self {
        Response {
            status: 500,
            body: json!({ "error": msg }),
        }
    }
    /// Map a `StoreError` to a 500 (transport/parse) response.
    pub fn from_store_error(e: StoreError) -> Self {
        Response::server_error(&e.to_string())
    }
}

/// The Esplora surface.
pub struct Esplora<'a> {
    pub store: &'a Store,
    pub cfg: &'a Config,
    /// Optional metrics handle: when set, address endpoints that resolve to an
    /// address with no indexed history bump `bidx_address_not_found_total`.
    pub metrics: Option<&'a Metrics>,
}

impl<'a> Esplora<'a> {
    pub fn new(store: &'a Store, cfg: &'a Config) -> Self {
        Esplora { store, cfg, metrics: None }
    }

    /// Like [`new`](Self::new) but records address-not-found events.
    pub fn with_metrics(store: &'a Store, cfg: &'a Config, metrics: &'a Metrics) -> Self {
        Esplora { store, cfg, metrics: Some(metrics) }
    }

    fn note_address_not_found(&self) {
        if let Some(m) = self.metrics {
            m.inc_address_not_found();
        }
    }

    fn network(&self) -> bidx_core::Network {
        self.cfg.network.to_core()
    }

    /// Resolve an address path segment (address string or scripthash) to the
    /// stored `(script_type, address_hash)`.
    fn resolve(&self, segment: &str) -> Result<Option<(u8, [u8; 32])>, StoreError> {
        if segment.len() == 64 && segment.chars().all(|c| c.is_ascii_hexdigit()) {
            // Electrum-style scripthash (display order) → internal order.
            let wire = store::reverse_hex(&segment.to_ascii_lowercase())?;
            let sh = store::parse_hex32(&wire)?;
            return self.store.resolve_scripthash(&sh);
        }
        Ok(address::parse_address(self.network(), segment))
    }

    /// `GET /blocks/tip/height` → the tip height as a bare number.
    pub fn tip_height(&self) -> Response {
        match self.store.tip_height() {
            Ok(Some(h)) => Response::ok(json!(h)),
            Ok(None) => Response::not_found("no blocks indexed"),
            Err(e) => Response::from_store_error(e),
        }
    }

    /// `GET /blocks/tip/hash` → the tip block hash (display order).
    pub fn tip_hash(&self) -> Response {
        match self.store.tip_height() {
            Ok(Some(h)) => match self.store.block_by_height(h) {
                Ok(Some(b)) => match store::reverse_hex(&b.hash) {
                    Ok(disp) => Response::ok(json!(disp)),
                    Err(e) => Response::from_store_error(e),
                },
                Ok(None) => Response::not_found("tip block missing"),
                Err(e) => Response::from_store_error(e),
            },
            Ok(None) => Response::not_found("no blocks indexed"),
            Err(e) => Response::from_store_error(e),
        }
    }

    /// `GET /blocks/:height` or `/block/:hash` → block detail.
    pub fn block(&self, height: Option<u32>, hash: Option<&str>) -> Response {
        let row = if let Some(h) = height {
            self.store.block_by_height(h)
        } else if let Some(h) = hash {
            match store::parse_hex32(&store::reverse_hex(h).unwrap_or_default()) {
                Ok(b) => self.store.block_by_hash(&b),
                Err(_) => return Response::bad_request("malformed block hash"),
            }
        } else {
            return Response::bad_request("missing height or hash");
        };
        match row {
            Ok(Some(b)) => match self.block_json(&b) {
                Ok(v) => Response::ok(v),
                Err(e) => Response::from_store_error(e),
            },
            Ok(None) => Response::not_found("block not found"),
            Err(e) => Response::from_store_error(e),
        }
    }

    fn block_json(&self, b: &BlockRow) -> Result<Value, StoreError> {
        let txids: Vec<String> = self
            .store
            .block_txids(b.height)?
            .into_iter()
            .map(|t| store::reverse_hex(&t.txid).unwrap_or(t.txid))
            .collect();
        Ok(json!({
            "id": store::reverse_hex(&b.hash)?,
            "height": b.height,
            "version": b.version,
            "timestamp": b.time,
            "tx_count": b.tx_count,
            "size": b.size,
            "weight": b.weight,
            "merkle_root": store::reverse_hex(&b.merkle_root)?,
            "previousblockhash": store::reverse_hex(&b.prev_hash)?,
            "bits": b.bits,
            "nonce": b.nonce,
            "txs": txids,
        }))
    }

    /// `GET /block-height/:height` → the block hash (display order).
    pub fn block_hash_at_height(&self, height: u32) -> Response {
        match self.store.block_by_height(height) {
            Ok(Some(b)) => match store::reverse_hex(&b.hash) {
                Ok(disp) => Response::ok(json!(disp)),
                Err(e) => Response::from_store_error(e),
            },
            Ok(None) => Response::not_found("block not found"),
            Err(e) => Response::from_store_error(e),
        }
    }

    /// `GET /address/:addr` → confirmed stats + UTXO summary.
    pub fn address(&self, segment: &str) -> Response {
        let (_, hash) = match self.resolve(segment) {
            Ok(Some(x)) => x,
            Ok(None) => return Response::bad_request("invalid address or scripthash"),
            Err(e) => return Response::from_store_error(e),
        };
        match self.store.address_stats(&hash) {
            Ok(Some(s)) => {
                let balance = s.funded_sum.saturating_sub(s.spent_sum);
                Response::ok(json!({
                    "address": segment,
                    "chain_stats": {
                        "funded_txo_count": s.funded_count,
                        "funded_txo_sum": s.funded_sum,
                        "spent_txo_count": s.spent_count,
                        "spent_txo_sum": s.spent_sum,
                        "tx_count": s.history_count,
                    },
                    "mempool_stats": {
                        "funded_txo_count": 0,
                        "funded_txo_sum": 0,
                        "spent_txo_count": 0,
                        "spent_txo_sum": 0,
                        "tx_count": 0,
                    },
                    "balance": balance,
                }))
            }
            Ok(None) => {
                self.note_address_not_found();
                Response::ok(json!({
                    "address": segment,
                    "chain_stats": {
                        "funded_txo_count": 0, "funded_txo_sum": 0,
                        "spent_txo_count": 0, "spent_txo_sum": 0, "tx_count": 0,
                    },
                    "mempool_stats": {
                        "funded_txo_count": 0, "funded_txo_sum": 0,
                        "spent_txo_count": 0, "spent_txo_sum": 0, "tx_count": 0,
                    },
                    "balance": 0,
                }))
            }
            Err(e) => Response::from_store_error(e),
        }
    }

    /// `GET /address/:addr/utxo` → confirmed UTXOs.
    pub fn address_utxo(&self, segment: &str, limit: u32) -> Response {
        let (_, hash) = match self.resolve(segment) {
            Ok(Some(x)) => x,
            Ok(None) => return Response::bad_request("invalid address or scripthash"),
            Err(e) => return Response::from_store_error(e),
        };
        match self.store.address_utxos(&hash, limit) {
            Ok(rows) => {
                let out: Vec<Value> = rows
                    .iter()
                    .map(|u| {
                        json!({
                            "txid": store::reverse_hex(&u.txid).unwrap_or(u.txid.clone()),
                            "vout": u.output_index,
                            "value": u.value_sat,
                            "status": { "confirmed": true, "block_height": u.height },
                        })
                    })
                    .collect();
                Response::ok(json!(out))
            }
            Err(e) => Response::from_store_error(e),
        }
    }

    /// `GET /address/:addr/txs` → transaction summaries for the address.
    pub fn address_txs(&self, segment: &str, limit: u32) -> Response {
        let (_, hash) = match self.resolve(segment) {
            Ok(Some(x)) => x,
            Ok(None) => return Response::bad_request("invalid address or scripthash"),
            Err(e) => return Response::from_store_error(e),
        };
        match self.store.address_history(&hash, limit) {
            Ok(rows) => {
                // Group history rows by txid into per-tx net value summaries.
                let mut order: Vec<String> = Vec::new();
                let mut by_tx: std::collections::BTreeMap<String, (u32, i64)> =
                    std::collections::BTreeMap::new();
                for r in rows {
                    let disp = store::reverse_hex(&r.txid).unwrap_or(r.txid.clone());
                    let entry = by_tx.entry(disp.clone()).or_insert_with(|| {
                        order.push(disp.clone());
                        (r.height, 0)
                    });
                    entry.1 += r.value_sat;
                }
                let out: Vec<Value> = order
                    .iter()
                    .map(|txid| {
                        let (height, value) = by_tx[txid];
                        json!({
                            "txid": txid,
                            "status": { "confirmed": true, "block_height": height },
                            "value": value,
                        })
                    })
                    .collect();
                Response::ok(json!(out))
            }
            Err(e) => Response::from_store_error(e),
        }
    }

    /// `GET /tx/:txid` → transaction detail with vin/vout and status.
    pub fn tx(&self, txid_display: &str) -> Response {
        let wire = match store::parse_hex32(&store::reverse_hex(txid_display).unwrap_or_default()) {
            Ok(b) => b,
            Err(_) => return Response::bad_request("malformed txid"),
        };
        let tx = match self.store.tx(&wire) {
            Ok(Some(t)) => t,
            Ok(None) => return Response::not_found("transaction not found"),
            Err(e) => return Response::from_store_error(e),
        };
        let block_hash = match self.store.block_by_height(tx.height) {
            Ok(Some(b)) => store::reverse_hex(&b.hash).unwrap_or_default(),
            _ => String::new(),
        };
        let inputs = self.store.tx_inputs(&wire).unwrap_or_default();
        let outputs = self.store.tx_outputs(&wire).unwrap_or_default();
        let vin: Vec<Value> = inputs
            .iter()
            .map(|i| {
                json!({
                    "txid": store::reverse_hex(&i.prev_txid).unwrap_or(i.prev_txid.clone()),
                    "vout": i.prev_vout,
                    "prevout": null,
                    "is_coinbase": i.input_index == 0 && tx.height > 0 && i.prev_txid.chars().all(|c| c == '0'),
                })
            })
            .collect();
        let vout: Vec<Value> = outputs
            .iter()
            .map(|o| {
                let addr = store::parse_hex32(&o.address_hash)
                    .ok()
                    .and_then(|h| address::render_address(self.network(), o.script_type, &h));
                json!({
                    "scriptpubkey_address": addr,
                    "value": o.value_sat,
                })
            })
            .collect();
        Response::ok(json!({
            "txid": store::reverse_hex(&tx.txid).unwrap_or(tx.txid.clone()),
            "version": 1,
            "fee": tx.fee,
            "status": {
                "confirmed": true,
                "block_height": tx.height,
                "block_hash": block_hash,
            },
            "vin": vin,
            "vout": vout,
        }))
    }

    /// `GET /tx/:txid/outspends` → one entry per output of the tx.
    pub fn tx_outspends(&self, txid_display: &str) -> Response {
        let wire = match store::parse_hex32(&store::reverse_hex(txid_display).unwrap_or_default()) {
            Ok(b) => b,
            Err(_) => return Response::bad_request("malformed txid"),
        };
        let tx = match self.store.tx(&wire) {
            Ok(Some(t)) => t,
            Ok(None) => return Response::not_found("transaction not found"),
            Err(e) => return Response::from_store_error(e),
        };
        let spends = match self.store.tx_outspends(&wire) {
            Ok(s) => s,
            Err(e) => return Response::from_store_error(e),
        };
        let mut by_vout: std::collections::BTreeMap<u32, &store::OutspendRow> =
            std::collections::BTreeMap::new();
        for s in &spends {
            by_vout.insert(s.spent_vout, s);
        }
        let mut out = Vec::with_capacity(tx.output_count as usize);
        for vout in 0..tx.output_count {
            match by_vout.get(&vout) {
                Some(s) => out.push(json!({
                    "spent": true,
                    "txid": store::reverse_hex(&s.spending_txid).unwrap_or(s.spending_txid.clone()),
                    "status": { "confirmed": true, "block_height": s.height },
                })),
                None => out.push(json!({ "spent": false, "txid": Value::Null })),
            }
        }
        Response::ok(json!(out))
    }

    /// `GET /address-prefix/:prefix` → matching addresses.
    pub fn address_prefix(&self, prefix: &str) -> Response {
        if prefix.is_empty() || prefix.len() > 64 {
            return Response::bad_request("invalid prefix");
        }
        match self.store.address_prefix(prefix, 100) {
            Ok(rows) => {
                let out: Vec<String> = rows.into_iter().map(|r| r.address).collect();
                Response::ok(json!(out))
            }
            Err(e) => Response::from_store_error(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{hex64, store, wire};

    fn cfg() -> &'static Config {
        // Leaked so the borrowed `Config` outlives each test's `Esplora`.
        Box::leak(Box::new(Config::default()))
    }

    #[test]
    fn tip_endpoints() {
        let (s, _) = store(
            &[(
                "max(height)",
                r#"{"data":[{"height":840000}],"rows":1}"#,
            )],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, &cfg());
        let r = e.tip_height();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, json!(840000));

        // Empty index → 404.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        assert_eq!(e.tip_height().status, 404);
    }

    #[test]
    fn tip_hash_reverses_display_order() {
        let disp = hex64(0xab);
        let body = format!(
            r#"{{"data":[{{"height":1,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}}],"rows":1}}"#,
            wire(&disp),
            wire(&hex64(0x01)),
            wire(&hex64(0x02))
        );
        let (s, _) = store(
            &[
                ("max(height)", r#"{"data":[{"height":1}],"rows":1}"#),
                ("WHERE height = 1", &body),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, &cfg());
        assert_eq!(e.tip_hash().body, json!(disp));
    }

    #[test]
    fn block_by_height_and_hash() {
        let disp = hex64(0xaa);
        let block = format!(
            r#"{{"data":[{{"height":100,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1600000000,"version":2,"bits":386089497,"nonce":42,"tx_count":2,"size":1000,"weight":3990}}],"rows":1}}"#,
            wire(&disp),
            wire(&hex64(0xbb)),
            wire(&hex64(0xcc))
        );
        let txids = format!(
            r#"{{"data":[{{"txid":"{}"}},{{"txid":"{}"}}],"rows":2}}"#,
            wire(&hex64(0x11)),
            wire(&hex64(0x22))
        );
        let (s, _) = store(
            &[
                ("FROM bitcoin.transactions", &txids),
                ("FROM bitcoin.blocks", &block),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, &cfg());

        let r = e.block(Some(100), None);
        assert_eq!(r.status, 200);
        assert_eq!(r.body["height"], 100);
        assert_eq!(r.body["id"], disp);
        assert_eq!(r.body["tx_count"], 2);
        assert_eq!(r.body["txs"].as_array().unwrap().len(), 2);
        assert_eq!(r.body["txs"][0], hex64(0x11));

        // By hash (display order).
        let r2 = e.block(None, Some(&disp));
        assert_eq!(r2.status, 200);
        assert_eq!(r2.body["height"], 100);

        // Malformed hash and missing args.
        assert_eq!(e.block(None, Some("xyz")).status, 400);
        assert_eq!(e.block(None, None).status, 400);
    }

    #[test]
    fn block_hash_at_height_and_missing() {
        let disp = hex64(0xaa);
        let block = format!(
            r#"{{"data":[{{"height":7,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}}],"rows":1}}"#,
            wire(&disp),
            wire(&hex64(0)),
            wire(&hex64(0))
        );
        let (s, _) = store(&[("FROM bitcoin.blocks", &block)], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        assert_eq!(e.block_hash_at_height(7).body, json!(disp));
        // Missing.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        assert_eq!(e.block_hash_at_height(7).status, 404);
    }

    #[test]
    fn address_stats_and_utxos_and_txs() {
        let stats = r#"{"data":[{"funded_count":2,"funded_sum":10000,"spent_count":1,"spent_sum":4000,"history_count":3}],"rows":1}"#;
        let utxos = r#"{"data":[{"txid":"abab","output_index":1,"value_sat":"6000","height":50}],"rows":1}"#;
        let hist = r#"{"data":[{"txid":"aa","height":50,"value_sat":6000,"kind":0},{"txid":"bb","height":60,"value_sat":-1000,"kind":1}],"rows":2}"#;
        let (s, _) = store(
            &[
                ("FROM bitcoin.address_stats", stats),
                ("utxos_by_address", utxos),
                ("FROM bitcoin.address_history", hist),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, &cfg());
        // A valid mainnet P2PKH address.
        let addr = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa";

        let r = e.address(addr);
        assert_eq!(r.status, 200);
        assert_eq!(r.body["chain_stats"]["funded_txo_sum"], 10000);
        assert_eq!(r.body["chain_stats"]["spent_txo_sum"], 4000);
        assert_eq!(r.body["balance"], 6000);

        let r = e.address_utxo(addr, 50);
        assert_eq!(r.body[0]["vout"], 1);
        assert_eq!(r.body[0]["value"], 6000);
        assert_eq!(r.body[0]["status"]["block_height"], 50);

        let r = e.address_txs(addr, 50);
        let arr = r.body.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // Net value per txid.
        assert_eq!(arr[0]["value"], 6000);
        assert_eq!(arr[1]["value"], -1000);

        // Unknown address → zeroed stats (200, not 404).
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        let r = e.address(addr);
        assert_eq!(r.status, 200);
        assert_eq!(r.body["balance"], 0);

        // Invalid address → 400.
        assert_eq!(e.address("not-an-address").status, 400);
    }

    #[test]
    fn scripthash_path_resolves_via_map() {
        // A P2WPKH address; its scripthash path must hit scripthash_map.
        let addr = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
        let (_, hash) = address::parse_address(bidx_core::Network::Mainnet, addr).unwrap();
        let sh = address::scripthash(&address::script_pubkey(4, &hash).unwrap());
        let sh_display = store::to_hex(&sh.into_iter().rev().collect::<Vec<_>>());

        let map = format!(
            r#"{{"data":[{{"script_type":4,"address_hash":"{}"}}],"rows":1}}"#,
            store::to_hex(&hash)
        );
        let stats = r#"{"data":[{"funded_count":1,"funded_sum":100,"spent_count":0,"spent_sum":0,"history_count":1}],"rows":1}"#;
        let (s, m) = store(
            &[
                ("FROM bitcoin.scripthash_map", &map),
                ("FROM bitcoin.address_stats", stats),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, &cfg());
        let r = e.address(&sh_display);
        assert_eq!(r.status, 200);
        assert_eq!(r.body["balance"], 100);
        assert!(m.saw("scripthash_map"));

        // An unknown scripthash → 400.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        assert_eq!(e.address(&hex64(0x99)).status, 400);
    }

    #[test]
    fn tx_detail_with_vin_vout_and_outspends() {
        let txid_disp = hex64(0xaa);
        let tx = format!(
            r#"{{"data":[{{"txid":"{}","height":100,"tx_index":1,"fee":"1500","output_count":2}}],"rows":1}}"#,
            wire(&txid_disp)
        );
        let block = format!(
            r#"{{"data":[{{"height":100,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":1,"version":1,"bits":1,"nonce":1,"tx_count":1,"size":1,"weight":1}}],"rows":1}}"#,
            wire(&hex64(0xbb)),
            wire(&hex64(0x00)),
            wire(&hex64(0x00))
        );
        let inputs = format!(
            r#"{{"data":[{{"input_index":0,"prev_txid":"{}","prev_vout":0}}],"rows":1}}"#,
            wire(&hex64(0x77))
        );
        let outputs = format!(
            r#"{{"data":[{{"output_index":0,"value_sat":"5000","script_type":2,"address_hash":"{}"}},{{"output_index":1,"value_sat":"1000","script_type":7,"address_hash":"{}"}}],"rows":2}}"#,
            "62e907b15cbf27d5425399ebf6f0fb50ebb88f18".to_string() + &"0".repeat(24),
            "0".repeat(64)
        );
        let outspends = format!(
            r#"{{"data":[{{"spent_vout":0,"spending_txid":"{}","height":101}}],"rows":1}}"#,
            wire(&hex64(0x88))
        );
        let (s, _) = store(
            &[
                ("FROM bitcoin.transactions_by_txid", &tx),
                ("FROM bitcoin.transactions", &tx),
                ("FROM bitcoin.inputs", &inputs),
                ("FROM bitcoin.outputs", &outputs),
                ("FROM bitcoin.spends_by_spent_outpoint", &outspends),
                ("FROM bitcoin.blocks", &block),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, &cfg());

        let r = e.tx(&txid_disp);
        assert_eq!(r.status, 200);
        assert_eq!(r.body["txid"], txid_disp);
        assert_eq!(r.body["fee"], 1500);
        assert_eq!(r.body["status"]["block_height"], 100);
        assert_eq!(r.body["vin"][0]["txid"], hex64(0x77));
        assert_eq!(r.body["vout"].as_array().unwrap().len(), 2);
        // P2PKH output renders its address.
        assert_eq!(
            r.body["vout"][0]["scriptpubkey_address"],
            "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
        );
        // OP_RETURN output has no address.
        assert!(r.body["vout"][1]["scriptpubkey_address"].is_null());

        // Outspends: output 0 spent, output 1 unspent.
        let r = e.tx_outspends(&txid_disp);
        assert_eq!(r.status, 200);
        let arr = r.body.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["txid"], hex64(0x88));
        assert_eq!(arr[1]["spent"], false);
        assert!(arr[1]["txid"].is_null());

        // Malformed txid → 400.
        assert_eq!(e.tx("nope").status, 400);
        assert_eq!(e.tx_outspends("nope").status, 400);
    }

    #[test]
    fn tx_not_found() {
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        assert_eq!(e.tx(&hex64(0xaa)).status, 404);
        assert_eq!(e.tx_outspends(&hex64(0xaa)).status, 404);
    }

    #[test]
    fn address_prefix_endpoint() {
        let body = r#"{"data":[{"address":"bc1qxyz","script_type":4},{"address":"bc1qabc","script_type":4}],"rows":2}"#;
        let (s, _) = store(&[("address_prefix", body)], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, &cfg());
        let r = e.address_prefix("bc1q");
        assert_eq!(r.status, 200);
        assert_eq!(r.body.as_array().unwrap().len(), 2);
        assert_eq!(r.body[0], "bc1qxyz");
        // Empty / overlong prefix rejected.
        assert_eq!(e.address_prefix("").status, 400);
        assert_eq!(e.address_prefix(&"a".repeat(65)).status, 400);
    }

    #[test]
    fn store_failures_map_to_500() {
        struct Failing;
        impl crate::store::Transport for Failing {
            fn query(&self, _: &str) -> Result<String, StoreError> {
                Err(StoreError::Transport("down".into()))
            }
        }
        let s = Store::new(Box::new(Failing), "bitcoin");
        let e = Esplora::new(&s, cfg());
        let addr = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa";
        assert_eq!(e.tip_height().status, 500);
        assert_eq!(e.tip_hash().status, 500);
        assert_eq!(e.block(Some(1), None).status, 500);
        assert_eq!(e.block_hash_at_height(1).status, 500);
        assert_eq!(e.address(addr).status, 500);
        assert_eq!(e.address_utxo(addr, 10).status, 500);
        assert_eq!(e.address_txs(addr, 10).status, 500);
        assert_eq!(e.tx(&hex64(0xaa)).status, 500);
        assert_eq!(e.tx_outspends(&hex64(0xaa)).status, 500);
        assert_eq!(e.address_prefix("bc1q").status, 500);
        // A 64-hex scripthash path routes through the store's scripthash_map,
        // so the resolve() error branch is taken too.
        assert_eq!(e.address(&hex64(0x55)).status, 500);
        assert_eq!(e.address_utxo(&hex64(0x55), 10).status, 500);
        assert_eq!(e.address_txs(&hex64(0x55), 10).status, 500);
    }

    #[test]
    fn tip_block_missing_is_404() {
        // Tip height present, but the block row itself is absent.
        let (s, _) = store(
            &[("max(height)", r#"{"data":[{"height":9}],"rows":1}"#)],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.tip_hash().status, 404);
    }

    #[test]
    fn tip_hash_surfaces_block_lookup_failure() {
        // tip_height() succeeds but the block lookup fails → the inner Err
        // branch of tip_hash returns 500.
        struct HalfFail;
        impl crate::store::Transport for HalfFail {
            fn query(&self, sql: &str) -> Result<String, StoreError> {
                if sql.contains("max(height)") {
                    Ok(r#"{"data":[{"height":1}],"rows":1}"#.to_string())
                } else {
                    Err(StoreError::Transport("blocks down".into()))
                }
            }
        }
        let s = Store::new(Box::new(HalfFail), "bitcoin");
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.tip_hash().status, 500);
    }

    #[test]
    fn scripthash_path_missing_from_map_is_400() {
        // A valid 64-hex scripthash with no map row resolves to None → 400.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, cfg());
        let sh = hex64(0x55);
        assert_eq!(e.address(&sh).status, 400);
        assert_eq!(e.address_utxo(&sh, 10).status, 400);
        assert_eq!(e.address_txs(&sh, 10).status, 400);
    }

    #[test]
    fn tx_detail_survives_a_missing_block_and_broken_spend_lookup() {
        // tx lookup succeeds, but the block lookup returns nothing → the
        // status.block_hash falls back to an empty string.
        let tx = format!(
            r#"{{"data":[{{"txid":"{}","height":100,"tx_index":0,"fee":"1","output_count":0}}],"rows":1}}"#,
            wire(&hex64(0xaa))
        );
        let (s, _) = store(
            &[
                ("FROM bitcoin.transactions_by_txid", &tx),
                ("FROM bitcoin.transactions", &tx),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, cfg());
        let r = e.tx(&hex64(0xaa));
        assert_eq!(r.status, 200);
        assert_eq!(r.body["status"]["block_hash"], "");

        // tx lookup succeeds, but the outspend lookup returns unparseable JSON
        // → the spends Err branch returns a 500.
        let (s, _) = store(
            &[
                ("FROM bitcoin.transactions_by_txid", &tx),
                ("FROM bitcoin.transactions", &tx),
                ("FROM bitcoin.spends_by_spent_outpoint", "not json"),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.tx_outspends(&hex64(0xaa)).status, 500);
    }

    #[test]
    fn missing_block_and_tip_map_to_404() {
        // Tip present but the tip block row is missing.
        let (s, _) = store(&[("max(height)", r#"{"data":[{"height":5}],"rows":1}"#)], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.tip_hash().status, 404);
        // Empty index → tip_hash 404 too.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.tip_hash().status, 404);
    }

    #[test]
    fn malformed_hashes_from_store_map_to_500() {
        // A block row whose hash is not valid hex makes reverse_hex fail.
        let bad_block = r#"{"data":[{"height":1,"hash":"zz","prev_hash":"zz","merkle_root":"zz","time":1,"version":1,"bits":1,"nonce":1,"tx_count":0,"size":1,"weight":1}],"rows":1}"#;
        let (s, _) = store(&[("FROM bitcoin.blocks", bad_block)], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.block(Some(1), None).status, 500);
        assert_eq!(e.block_hash_at_height(1).status, 500);

        let (s, _) = store(
            &[
                ("max(height)", r#"{"data":[{"height":1}],"rows":1}"#),
                ("FROM bitcoin.blocks", bad_block),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.tip_hash().status, 500);
    }

    #[test]
    fn malformed_txid_hex_in_store_falls_back_to_raw_string() {
        // tx_outspends tolerates a non-hex spending_txid (uses the raw value).
        let tx = format!(
            r#"{{"data":[{{"txid":"{}","height":1,"tx_index":0,"fee":"1","output_count":1}}],"rows":1}}"#,
            wire(&hex64(0xaa))
        );
        let spends = r#"{"data":[{"spent_vout":0,"spending_txid":"zz","height":2}],"rows":1}"#;
        let (s, _) = store(
            &[
                ("FROM bitcoin.transactions_by_txid", &tx),
                ("FROM bitcoin.transactions", &tx),
                ("FROM bitcoin.spends_by_spent_outpoint", spends),
            ],
            r#"{"data":[],"rows":0}"#,
        );
        let e = Esplora::new(&s, cfg());
        let r = e.tx_outspends(&hex64(0xaa));
        assert_eq!(r.status, 200);
        assert_eq!(r.body[0]["txid"], "zz");
    }

    #[test]
    fn address_not_found_is_counted_when_metrics_are_attached() {
        use crate::metrics::Metrics;
        let m = Metrics::new();
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::with_metrics(&s, cfg(), &m);
        // An address with no stats row bumps the counter...
        let r = e.address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa");
        assert_eq!(r.status, 200);
        assert!(m.registry.render().contains("bidx_address_not_found_total 1\n"));
        // ...but an invalid address (400) does not.
        assert_eq!(e.address("not-an-address").status, 400);
        assert!(m.registry.render().contains("bidx_address_not_found_total 1\n"));
    }

    #[test]
    fn address_not_found_without_metrics_does_not_panic() {
        // The default constructor carries no metrics handle; the not-found
        // path must still answer 200.
        let (s, _) = store(&[], r#"{"data":[],"rows":0}"#);
        let e = Esplora::new(&s, cfg());
        assert_eq!(e.address("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa").status, 200);
    }

    #[test]
    fn response_constructors_and_store_error_mapping() {
        assert_eq!(Response::ok(json!(1)).status, 200);
        assert_eq!(Response::not_found("x").status, 404);
        assert_eq!(Response::bad_request("x").status, 400);
        let e = Response::from_store_error(StoreError::Transport("boom".into()));
        assert_eq!(e.status, 500);
        assert!(e.body["error"].as_str().unwrap().contains("boom"));
    }
}
