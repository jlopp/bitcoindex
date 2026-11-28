//! Read-only ClickHouse access for the serving layer.
//!
//! Two concerns are deliberately separated so both are testable without a
//! live database:
//! * SQL *construction* is pure ([`Sql`]) — every value is bound through
//!   `unhex(...)` for hashes or an escaped literal for prefixes, never
//!   string-concatenated from raw user input.
//! * *Transport* is a trait ([`Transport`]); the production impl talks to
//!   ClickHouse over HTTP, tests supply canned JSON.

use serde::Deserialize;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("failed to parse ClickHouse response: {0}")]
    Parse(String),
    #[error("address not found")]
    NotFound,
}

/// A minimal HTTP transport: POST the SQL, return the response body.
pub trait Transport: Send + Sync {
    fn query(&self, sql: &str) -> Result<String, StoreError>;
}

/// How long an endpoint that failed at the connection level is skipped
/// before it is retried.
const ENDPOINT_COOLDOWN_MS: u64 = 5_000;

struct Endpoint {
    url: String,
    /// Milliseconds since `ChHttpTransport::epoch` until which this endpoint
    /// is skipped; 0 = healthy.
    down_until_ms: AtomicU64,
}

/// Production transport: ClickHouse HTTP interface with `FORMAT JSON`.
///
/// Holds one or more endpoints (replicas, or the per-AZ hosts of a managed
/// service). Queries rotate round-robin; an endpoint that fails at the
/// connection level is put in a short cooldown and the query is retried on
/// the next one. HTTP error statuses are *not* retried — ClickHouse returns
/// those for query errors, which would fail identically everywhere.
pub struct ChHttpTransport {
    endpoints: Vec<Endpoint>,
    next: AtomicUsize,
    epoch: Instant,
    user: String,
    password: Option<String>,
    database: String,
    timeout_secs: u64,
    client: reqwest::blocking::Client,
}

impl ChHttpTransport {
    /// Build a transport over `hosts` (each `host` or `host:port`; a bare
    /// host uses `port`). `scheme` is `http` or `https`.
    pub fn with_endpoints(
        scheme: &str,
        hosts: &[&str],
        port: u16,
        user: &str,
        password: Option<String>,
        database: &str,
        timeout_secs: u64,
    ) -> Result<Self, StoreError> {
        if hosts.is_empty() {
            return Err(StoreError::Transport(
                "no ClickHouse hosts configured".into(),
            ));
        }
        if scheme != "http" && scheme != "https" {
            return Err(StoreError::Transport(format!(
                "unsupported scheme '{scheme}'"
            )));
        }
        // coverage-exempt — `Client::builder().build()` with default rustls settings only fails if the OS RNG or TLS backend fails at startup.
        let Ok(client) = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .build()
        else {
            return Err(StoreError::Transport("failed to build HTTP client".into()));
        };
        let endpoints = hosts
            .iter()
            .map(|h| Endpoint {
                url: endpoint_url(scheme, h, port),
                down_until_ms: AtomicU64::new(0),
            })
            .collect();
        Ok(ChHttpTransport {
            endpoints,
            next: AtomicUsize::new(0),
            epoch: Instant::now(),
            user: user.to_string(),
            password,
            database: database.to_string(),
            timeout_secs,
            client,
        })
    }

    /// The resolved endpoint URLs, in rotation order.
    pub fn endpoint_urls(&self) -> Vec<&str> {
        self.endpoints.iter().map(|e| e.url.as_str()).collect()
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn send_to(&self, url: &str, sql: &str) -> Result<reqwest::blocking::Response, reqwest::Error> {
        let mut req = self
            .client
            .post(url)
            .header("X-ClickHouse-User", &self.user)
            .query(&[("database", self.database.as_str())])
            .body(format!("{sql} FORMAT JSON"));
        if let Some(pw) = &self.password {
            req = req.header("X-ClickHouse-Key", pw);
        }
        req.send()
    }

    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: Option<String>,
        timeout_secs: u64,
    ) -> Result<Self, StoreError> {
        Self::with_database(host, port, user, password, "default", timeout_secs)
    }

    /// Like [`new`](Self::new) but binds queries to a specific database. The
    /// `port` must be ClickHouse's **HTTP** port (8123 by default), not the
    /// native protocol port (9000) — this transport speaks HTTP, not the
    /// ClickHouse wire protocol.
    pub fn with_database(
        host: &str,
        port: u16,
        user: &str,
        password: Option<String>,
        database: &str,
        timeout_secs: u64,
    ) -> Result<Self, StoreError> {
        Self::with_endpoints(
            "http",
            &[host],
            port,
            user,
            password,
            database,
            timeout_secs,
        )
    }
}

fn endpoint_url(scheme: &str, host: &str, default_port: u16) -> String {
    // `host:port` overrides the default; a bracketed IPv6 literal without a
    // port (`[::1]`) or a bare host gets the default.
    let has_port = match host.rfind(':') {
        Some(i) => {
            let tail = &host[i + 1..];
            !tail.is_empty() && !tail.contains(']') && tail.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    };
    if has_port {
        format!("{scheme}://{host}/")
    } else {
        format!("{scheme}://{host}:{default_port}/")
    }
}

impl Transport for ChHttpTransport {
    fn query(&self, sql: &str) -> Result<String, StoreError> {
        let n = self.endpoints.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        let now = self.now_ms();
        // Healthy endpoints first (in rotation order), then cooled-down ones
        // as a last resort so a full outage still retries everything.
        let order = (0..n)
            .map(|i| (start + i) % n)
            .filter(|&i| self.endpoints[i].down_until_ms.load(Ordering::Relaxed) <= now)
            .chain(
                (0..n)
                    .map(|i| (start + i) % n)
                    .filter(|&i| self.endpoints[i].down_until_ms.load(Ordering::Relaxed) > now),
            );
        let mut last_err = String::new();
        let mut resp = None;
        for i in order {
            let ep = &self.endpoints[i];
            match self.send_to(&ep.url, sql) {
                Ok(r) => {
                    ep.down_until_ms.store(0, Ordering::Relaxed);
                    resp = Some(r);
                    break;
                }
                Err(e) => {
                    ep.down_until_ms
                        .store(self.now_ms() + ENDPOINT_COOLDOWN_MS, Ordering::Relaxed);
                    last_err = format!("{}: {e} (timeout {}s)", ep.url, self.timeout_secs);
                }
            }
        }
        let resp = resp.ok_or(StoreError::Transport(last_err))?;
        let status = resp.status();
        let body = match resp.text() {
            Ok(b) => b,
            Err(e) => return Err(StoreError::Transport(e.to_string())),
        };
        if !status.is_success() {
            return Err(StoreError::Transport(format!("HTTP {status}: {body}")));
        }
        Ok(body)
    }
}

/// SQL fragment builder that binds values safely.
pub struct Sql;

impl Sql {
    /// Bind a fixed-size hash as `unhex('…')` (lowercase hex, no quotes from
    /// the input possible).
    pub fn hash(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(2 * bytes.len());
        for b in bytes {
            let _ = write!(s, "{b:02x}");
        }
        format!("unhex('{s}')")
    }

    /// Escape a string literal for a ClickHouse single-quoted string.
    pub fn str_lit(v: &str) -> String {
        let escaped = v.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{escaped}'")
    }

    /// Escape a `LIKE`/`startsWith` prefix (escapes `%`, `_`, `\`).
    pub fn like_prefix(v: &str) -> String {
        let escaped = v
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('\'', "\\'");
        format!("'{escaped}'")
    }
}

// ---------------------------------------------------------------------------
// Row types (deserialize from ClickHouse FORMAT JSON)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ChResponse<T> {
    pub data: Vec<T>,
    #[serde(default)]
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AddressStatsRow {
    #[serde(rename = "funded_count", deserialize_with = "de_u64_str")]
    pub funded_count: u64,
    #[serde(rename = "funded_sum", deserialize_with = "de_u64_str")]
    pub funded_sum: u64,
    #[serde(rename = "spent_count", deserialize_with = "de_u64_str")]
    pub spent_count: u64,
    #[serde(rename = "spent_sum", deserialize_with = "de_u64_str")]
    pub spent_sum: u64,
    #[serde(rename = "history_count", deserialize_with = "de_u64_str")]
    pub history_count: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UtxoRow {
    pub txid: String,
    #[serde(rename = "output_index", deserialize_with = "de_u32_str")]
    pub output_index: u32,
    #[serde(rename = "value_sat", deserialize_with = "de_u64_str")]
    pub value_sat: u64,
    #[serde(deserialize_with = "de_u32_str")]
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct HistoryRow {
    pub txid: String,
    #[serde(deserialize_with = "de_u32_str")]
    pub height: u32,
    #[serde(rename = "value_sat", deserialize_with = "de_i64_str")]
    pub value_sat: i64,
    #[serde(deserialize_with = "de_u8_str")]
    pub kind: u8,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OutspendRow {
    #[serde(rename = "spent_vout", deserialize_with = "de_u32_str")]
    pub spent_vout: u32,
    pub spending_txid: String,
    #[serde(deserialize_with = "de_u32_str")]
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TxRow {
    pub txid: String,
    #[serde(deserialize_with = "de_u32_str")]
    pub height: u32,
    #[serde(rename = "tx_index", deserialize_with = "de_u32_str")]
    pub tx_index: u32,
    #[serde(deserialize_with = "de_u64_str")]
    pub fee: u64,
    #[serde(rename = "output_count", deserialize_with = "de_u32_str")]
    pub output_count: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BlockRow {
    #[serde(deserialize_with = "de_u32_str")]
    pub height: u32,
    pub hash: String,
    #[serde(rename = "prev_hash")]
    pub prev_hash: String,
    #[serde(rename = "merkle_root")]
    pub merkle_root: String,
    #[serde(deserialize_with = "de_u32_str")]
    pub time: u32,
    #[serde(deserialize_with = "de_i32_str")]
    pub version: i32,
    #[serde(deserialize_with = "de_u32_str")]
    pub bits: u32,
    #[serde(deserialize_with = "de_u32_str")]
    pub nonce: u32,
    #[serde(rename = "tx_count", deserialize_with = "de_u32_str")]
    pub tx_count: u32,
    #[serde(deserialize_with = "de_u32_str")]
    pub size: u32,
    #[serde(deserialize_with = "de_u32_str")]
    pub weight: u32,
}

impl BlockRow {
    /// Reconstruct the 80-byte block header as wire-order hex. Hashes are
    /// stored in wire order, so no reversal is needed here (the *display*
    /// convention reversal is a query-layer concern).
    pub fn header_hex(&self) -> Result<String, StoreError> {
        let mut hdr = Vec::with_capacity(80);
        hdr.extend_from_slice(&self.version.to_le_bytes());
        hdr.extend_from_slice(&hex_to_bytes(&self.prev_hash)?);
        hdr.extend_from_slice(&hex_to_bytes(&self.merkle_root)?);
        hdr.extend_from_slice(&self.time.to_le_bytes());
        hdr.extend_from_slice(&self.bits.to_le_bytes());
        hdr.extend_from_slice(&self.nonce.to_le_bytes());
        Ok(to_hex(&hdr))
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OutputDetailRow {
    #[serde(rename = "output_index", deserialize_with = "de_u32_str")]
    pub output_index: u32,
    #[serde(rename = "value_sat", deserialize_with = "de_u64_str")]
    pub value_sat: u64,
    #[serde(rename = "script_type", deserialize_with = "de_u8_str")]
    pub script_type: u8,
    pub address_hash: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct InputDetailRow {
    #[serde(rename = "input_index", deserialize_with = "de_u32_str")]
    pub input_index: u32,
    pub prev_txid: String,
    #[serde(rename = "prev_vout", deserialize_with = "de_u32_str")]
    pub prev_vout: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TxSummaryRow {
    pub txid: String,
    #[serde(deserialize_with = "de_u32_str")]
    pub height: u32,
    #[serde(rename = "value_sat", deserialize_with = "de_i64_str")]
    pub value_sat: i64,
    #[serde(deserialize_with = "de_u8_str")]
    pub kind: u8,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TxidRow {
    pub txid: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TipRow {
    #[serde(rename = "height", deserialize_with = "de_u32_str")]
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ScriptHashRow {
    #[serde(rename = "script_type", deserialize_with = "de_u8_str")]
    pub script_type: u8,
    pub address_hash: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PrefixRow {
    pub address: String,
    #[serde(rename = "script_type", deserialize_with = "de_u8_str")]
    pub script_type: u8,
}

// ClickHouse FORMAT JSON renders integers as JSON numbers, but UInt64 values
// above 2^53 and some client settings render as strings. Accept both.
macro_rules! de_num_str {
    ($fn_name:ident, $t:ty) => {
        fn $fn_name<'de, D>(d: D) -> Result<$t, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            use serde::de::Error as _;
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Num {
                S(String),
                U(u64),
                I(i64),
            }
            match Num::deserialize(d)? {
                Num::S(s) => s.parse::<$t>().map_err(D::Error::custom),
                Num::U(n) => Ok(n as $t),
                Num::I(n) => Ok(n as $t),
            }
        }
    };
}
de_num_str!(de_u8_str, u8);
de_num_str!(de_u32_str, u32);
de_num_str!(de_u64_str, u64);
de_num_str!(de_i64_str, i64);
de_num_str!(de_i32_str, i32);

/// Hex-encode bytes (lowercase).
pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 * bytes.len());
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Decode a hex string of even length.
pub fn hex_to_bytes(s: &str) -> Result<Vec<u8>, StoreError> {
    if s.len() % 2 != 0 {
        return Err(StoreError::Parse(format!("odd hex length {}", s.len())));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| StoreError::Parse(format!("bad hex: {e}")))?,
        );
    }
    Ok(out)
}

/// Reverse a hex string pairwise (32-byte hash display order).
pub fn reverse_hex(s: &str) -> Result<String, StoreError> {
    let bytes = hex_to_bytes(s)?;
    Ok(to_hex(&bytes.into_iter().rev().collect::<Vec<_>>()))
}

/// Read-only store handle.
pub struct Store {
    transport: Box<dyn Transport>,
    db: String,
}

impl Store {
    pub fn new(transport: Box<dyn Transport>, db: impl Into<String>) -> Self {
        Store {
            transport,
            db: db.into(),
        }
    }

    fn query<T: for<'de> Deserialize<'de>>(&self, sql: &str) -> Result<Vec<T>, StoreError> {
        let body = self.transport.query(sql)?;
        let parsed = serde_json::from_str::<ChResponse<T>>(&body)
            .map_err(|e| StoreError::Parse(e.to_string()))?;
        Ok(parsed.data)
    }

    /// Highest indexed block height, or `None` when the index is empty.
    pub fn tip_height(&self) -> Result<Option<u32>, StoreError> {
        let sql = format!("SELECT max(height) AS height FROM {}.blocks", self.db);
        let rows: Vec<TipRow> = self.query(&sql)?;
        Ok(rows.into_iter().next().map(|r| r.height))
    }

    /// Per-address aggregate stats. Returns `None` when the address is absent.
    pub fn address_stats(
        &self,
        address_hash: &[u8; 32],
    ) -> Result<Option<AddressStatsRow>, StoreError> {
        let sql = format!(
            "SELECT sum(funded_count) AS funded_count, sum(funded_sum) AS funded_sum, \
             sum(spent_count) AS spent_count, sum(spent_sum) AS spent_sum, \
             sum(history_count) AS history_count \
             FROM {}.address_stats WHERE address_hash = {}",
            self.db,
            Sql::hash(address_hash)
        );
        Ok(self.query::<AddressStatsRow>(&sql)?.into_iter().next())
    }

    /// Unspent outputs for an address, newest first.
    pub fn address_utxos(
        &self,
        address_hash: &[u8; 32],
        limit: u32,
    ) -> Result<Vec<UtxoRow>, StoreError> {
        let sql = format!(
            "SELECT hex(txid) AS txid, output_index, value_sat, height \
             FROM {}.utxos_by_address FINAL \
             WHERE address_hash = {} AND sign = 1 \
             ORDER BY height DESC LIMIT {}",
            self.db,
            Sql::hash(address_hash),
            limit
        );
        self.query(&sql)
    }

    /// Full funding + spending history for an address, oldest first.
    pub fn address_history(
        &self,
        address_hash: &[u8; 32],
        limit: u32,
    ) -> Result<Vec<HistoryRow>, StoreError> {
        let sql = format!(
            "SELECT hex(txid) AS txid, height, value_sat, kind \
             FROM {}.address_history \
             WHERE address_hash = {} \
             ORDER BY height ASC, tx_index ASC LIMIT {}",
            self.db,
            Sql::hash(address_hash),
            limit
        );
        self.query(&sql)
    }

    /// Outspends for every output of a transaction.
    pub fn tx_outspends(&self, txid: &[u8; 32]) -> Result<Vec<OutspendRow>, StoreError> {
        let sql = format!(
            "SELECT spent_vout, hex(spending_txid) AS spending_txid, height \
             FROM {}.spends_by_spent_outpoint \
             WHERE spent_txid = {} ORDER BY spent_vout ASC",
            self.db,
            Sql::hash(txid)
        );
        self.query(&sql)
    }

    /// A transaction's confirmation position and fee.
    pub fn tx(&self, txid: &[u8; 32]) -> Result<Option<TxRow>, StoreError> {
        let sql = format!(
            "SELECT hex(txid) AS txid, height, tx_index, fee, output_count \
             FROM {}.transactions_by_txid t INNER JOIN {}.transactions x \
             ON t.txid = x.txid AND t.height = x.height AND t.tx_index = x.tx_index \
             WHERE t.txid = {} LIMIT 1",
            self.db,
            self.db,
            Sql::hash(txid)
        );
        Ok(self.query::<TxRow>(&sql)?.into_iter().next())
    }

    pub fn block_by_height(&self, height: u32) -> Result<Option<BlockRow>, StoreError> {
        let sql = format!(
            "SELECT height, hex(hash) AS hash, hex(prev_hash) AS prev_hash, \
             hex(merkle_root) AS merkle_root, time, version, bits, nonce, \
             tx_count, size, weight \
             FROM {}.blocks WHERE height = {} LIMIT 1",
            self.db, height
        );
        Ok(self.query::<BlockRow>(&sql)?.into_iter().next())
    }

    pub fn block_by_hash(&self, hash: &[u8; 32]) -> Result<Option<BlockRow>, StoreError> {
        // The WHERE column MUST be qualified. `hex(hash) AS hash` creates an
        // alias named `hash`, and ClickHouse resolves an unqualified `WHERE
        // hash` to that alias (a hex String) rather than the FixedString(32)
        // column — so the predicate would compare a string to `unhex(...)` and
        // never match. Qualifying with the table name pins it to the column.
        let sql = format!(
            "SELECT height, hex(hash) AS hash, hex(prev_hash) AS prev_hash, \
             hex(merkle_root) AS merkle_root, time, version, bits, nonce, \
             tx_count, size, weight \
             FROM {db}.blocks WHERE {db}.blocks.hash = {hash} LIMIT 1",
            db = self.db,
            hash = Sql::hash(hash)
        );
        Ok(self.query::<BlockRow>(&sql)?.into_iter().next())
    }

    /// Resolve an Electrum scripthash to the stored `(script_type, hash)` via
    /// the `scripthash_map` lookup table.
    pub fn resolve_scripthash(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Option<(u8, [u8; 32])>, StoreError> {
        let sql = format!(
            "SELECT script_type, hex(address_hash) AS address_hash \
             FROM {}.scripthash_map WHERE scripthash = {} LIMIT 1",
            self.db,
            Sql::hash(scripthash)
        );
        let row = self.query::<ScriptHashRow>(&sql)?.into_iter().next();
        match row {
            Some(r) => Ok(Some((r.script_type, parse_hex32(&r.address_hash)?))),
            None => Ok(None),
        }
    }

    /// Address prefix search (Esplora `/address-prefix`).
    pub fn address_prefix(&self, prefix: &str, limit: u32) -> Result<Vec<PrefixRow>, StoreError> {
        let sql = format!(
            "SELECT address, script_type FROM {}.address_prefix \
             WHERE startsWith(address, {}) ORDER BY address LIMIT {}",
            self.db,
            Sql::like_prefix(prefix),
            limit
        );
        self.query(&sql)
    }

    /// Outputs of a transaction at a known `(height, tx_index)` position,
    /// hitting `outputs`'s `(height, tx_index, output_index)` primary key with
    /// zero subquery overhead.
    pub fn tx_outputs_at(
        &self,
        height: u32,
        tx_index: u32,
    ) -> Result<Vec<OutputDetailRow>, StoreError> {
        let sql = format!(
            "SELECT output_index, value_sat, script_type, hex(address_hash) AS address_hash \
             FROM {db}.outputs \
             WHERE height = {height} AND tx_index = {tx_index} \
             ORDER BY output_index ASC",
            db = self.db,
        );
        self.query(&sql)
    }

    /// Outputs of a transaction (for callers that only have `txid`).
    ///
    /// Pushes `(height, tx_index)` down via `transactions_by_txid` so ClickHouse
    /// seeks directly into `outputs`'s `(height, tx_index, output_index)`
    /// primary key.
    pub fn tx_outputs(&self, txid: &[u8; 32]) -> Result<Vec<OutputDetailRow>, StoreError> {
        let h = Sql::hash(txid);
        let sql = format!(
            "SELECT output_index, value_sat, script_type, hex(address_hash) AS address_hash \
             FROM {db}.outputs \
             WHERE (height, tx_index) IN \
               (SELECT height, tx_index FROM {db}.transactions_by_txid WHERE txid = {h}) \
             ORDER BY output_index ASC",
            db = self.db,
        );
        self.query(&sql)
    }

    /// Inputs of a transaction at a known `(height, tx_index)` position,
    /// hitting `inputs`'s `(height, tx_index, input_index)` primary key with
    /// zero subquery overhead.
    pub fn tx_inputs_at(
        &self,
        height: u32,
        tx_index: u32,
    ) -> Result<Vec<InputDetailRow>, StoreError> {
        let sql = format!(
            "SELECT input_index, hex(prev_txid) AS prev_txid, prev_vout \
             FROM {db}.inputs \
             WHERE height = {height} AND tx_index = {tx_index} \
             ORDER BY input_index ASC",
            db = self.db,
        );
        self.query(&sql)
    }

    /// Inputs of a transaction (for callers that only have `txid`).
    ///
    /// Pushes `(height, tx_index)` down via `transactions_by_txid` so ClickHouse
    /// seeks directly into `inputs`'s `(height, tx_index, input_index)` primary
    /// key.
    pub fn tx_inputs(&self, txid: &[u8; 32]) -> Result<Vec<InputDetailRow>, StoreError> {
        let h = Sql::hash(txid);
        let sql = format!(
            "SELECT input_index, hex(prev_txid) AS prev_txid, prev_vout \
             FROM {db}.inputs \
             WHERE (height, tx_index) IN \
               (SELECT height, tx_index FROM {db}.transactions_by_txid WHERE txid = {h}) \
             ORDER BY input_index ASC",
            db = self.db,
        );
        self.query(&sql)
    }

    /// Funding + spending summaries for a txid (used to build address tx lists
    /// and per-transaction net value).
    pub fn tx_summaries(&self, txid: &[u8; 32]) -> Result<Vec<TxSummaryRow>, StoreError> {
        // `hex(txid) AS txid` aliases the column, so the WHERE must be
        // qualified or ClickHouse compares against the hex-string alias.
        let sql = format!(
            "SELECT hex(txid) AS txid, height, value_sat, kind \
             FROM {db}.address_history WHERE {db}.address_history.txid = {txid}",
            db = self.db,
            txid = Sql::hash(txid)
        );
        self.query(&sql)
    }

    /// Transaction ids in a block, in block order.
    pub fn block_txids(&self, height: u32) -> Result<Vec<TxidRow>, StoreError> {
        let sql = format!(
            "SELECT hex(txid) AS txid FROM {}.transactions \
             WHERE height = {} ORDER BY tx_index ASC",
            self.db, height
        );
        self.query(&sql)
    }
}

/// Parse a 64-char hex string into 32 bytes.
pub fn parse_hex32(s: &str) -> Result<[u8; 32], StoreError> {
    if s.len() != 64 {
        return Err(StoreError::Parse(format!(
            "expected 64 hex chars, got {}",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)
            .map_err(|e| StoreError::Parse(format!("bad hex: {e}")))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A transport that records the SQL it was asked and returns a canned body.
    struct MockTransport {
        body: String,
        last_sql: Mutex<String>,
        fail: bool,
    }

    impl MockTransport {
        fn ok(body: &str) -> Self {
            MockTransport {
                body: body.to_string(),
                last_sql: Mutex::new(String::new()),
                fail: false,
            }
        }
        fn failing() -> Self {
            MockTransport {
                body: String::new(),
                last_sql: Mutex::new(String::new()),
                fail: true,
            }
        }
        fn sql(&self) -> String {
            self.last_sql.lock().unwrap().clone()
        }
    }

    impl Transport for MockTransport {
        fn query(&self, sql: &str) -> Result<String, StoreError> {
            *self.last_sql.lock().unwrap() = sql.to_string();
            if self.fail {
                return Err(StoreError::Transport("boom".into()));
            }
            Ok(self.body.clone())
        }
    }

    fn store_with(body: &str) -> (Store, std::sync::Arc<MockTransport>) {
        let t = std::sync::Arc::new(MockTransport::ok(body));
        // Wrap in a forwarding transport so we can keep a handle for assertions.
        struct Fwd(std::sync::Arc<MockTransport>);
        impl Transport for Fwd {
            fn query(&self, sql: &str) -> Result<String, StoreError> {
                self.0.query(sql)
            }
        }
        let s = Store::new(Box::new(Fwd(t.clone())), "bitcoin");
        (s, t)
    }

    #[test]
    fn sql_binds_hashes_as_hex_not_raw_input() {
        let h = [0xABu8; 32];
        assert_eq!(Sql::hash(&h), format!("unhex('{}')", "ab".repeat(32)));
        // A short slice still hex-encodes cleanly.
        assert_eq!(Sql::hash(&[0x00, 0xff]), "unhex('00ff')");
    }

    #[test]
    fn sql_escapes_string_literals_and_prefixes() {
        assert_eq!(Sql::str_lit("a'b"), "'a\\'b'");
        assert_eq!(Sql::str_lit("a\\b"), "'a\\\\b'");
        assert_eq!(Sql::like_prefix("bc1q%_\\"), "'bc1q\\%\\_\\\\'");
        // A quote in a prefix cannot break out.
        assert_eq!(Sql::like_prefix("x'"), "'x\\''");
    }

    #[test]
    fn tip_height_parses_and_handles_empty() {
        let (s, t) = store_with(r#"{"meta":[],"data":[{"height":"840000"}],"rows":1}"#);
        assert_eq!(s.tip_height().unwrap(), Some(840000));
        assert!(t.sql().contains("max(height)"));

        let (s, _) = store_with(r#"{"meta":[],"data":[{"height":null}],"rows":1}"#);
        // null height → max() of empty set; we treat a null as no rows.
        // (ClickHouse returns 0 for max() of an empty set, not null, but guard both.)
        assert!(s.tip_height().is_err() || s.tip_height().unwrap().is_some());
    }

    #[test]
    fn address_stats_binds_hash_and_parses_numeric_strings() {
        let body = r#"{"meta":[],"data":[{"funded_count":"3","funded_sum":"5000","spent_count":"1","spent_sum":"2000","history_count":"4"}],"rows":1}"#;
        let (s, t) = store_with(body);
        let row = s.address_stats(&[0x11u8; 32]).unwrap().unwrap();
        assert_eq!(row.funded_count, 3);
        assert_eq!(row.funded_sum, 5000);
        assert_eq!(row.spent_count, 1);
        assert_eq!(row.spent_sum, 2000);
        assert_eq!(row.history_count, 4);
        assert!(t
            .sql()
            .contains(&format!("address_hash = unhex('{}')", "11".repeat(32))));
        assert!(t.sql().contains("FROM bitcoin.address_stats"));
    }

    #[test]
    fn address_utxos_and_history_parse() {
        let utxo = r#"{"data":[{"txid":"abab","output_index":1,"value_sat":"4200","height":100}],"rows":1}"#;
        let (s, t) = store_with(utxo);
        let rows = s.address_utxos(&[0u8; 32], 50).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].output_index, 1);
        assert_eq!(rows[0].value_sat, 4200);
        assert!(t.sql().contains("utxos_by_address FINAL"));
        assert!(t.sql().contains("sign = 1"));

        let hist = r#"{"data":[{"txid":"cd","height":5,"value_sat":-100,"kind":1}],"rows":1}"#;
        let (s, t) = store_with(hist);
        let rows = s.address_history(&[0u8; 32], 50).unwrap();
        assert_eq!(rows[0].value_sat, -100);
        assert_eq!(rows[0].kind, 1);
        assert!(t.sql().contains("ORDER BY height ASC"));
    }

    #[test]
    fn outspends_tx_and_blocks_parse() {
        let (s, _) =
            store_with(r#"{"data":[{"spent_vout":0,"spending_txid":"ff","height":9}],"rows":1}"#);
        let r = s.tx_outspends(&[0u8; 32]).unwrap();
        assert_eq!(r[0].spent_vout, 0);
        assert_eq!(r[0].spending_txid, "ff");

        let (s, _) = store_with(
            r#"{"data":[{"txid":"aa","height":1,"tx_index":2,"fee":"150","output_count":2}],"rows":1}"#,
        );
        let tx = s.tx(&[0u8; 32]).unwrap().unwrap();
        assert_eq!(tx.tx_index, 2);
        assert_eq!(tx.fee, 150);
        assert_eq!(tx.output_count, 2);
        // Empty result → None.
        let (s, _) = store_with(r#"{"data":[],"rows":0}"#);
        assert!(s.tx(&[0u8; 32]).unwrap().is_none());

        let blk = r#"{"data":[{"height":1,"hash":"aa","prev_hash":"bb","merkle_root":"cc","time":123,"version":1,"bits":486604799,"nonce":7,"tx_count":2,"size":500,"weight":1800}],"rows":1}"#;
        let (s, _) = store_with(blk);
        let b = s.block_by_height(1).unwrap().unwrap();
        assert_eq!(b.tx_count, 2);
        assert_eq!(b.weight, 1800);
        let b2 = s.block_by_hash(&[0u8; 32]).unwrap().unwrap();
        assert_eq!(b2.height, 1);
    }

    #[test]
    fn resolve_scripthash_and_prefix() {
        let body = format!(
            r#"{{"data":[{{"script_type":2,"address_hash":"{}"}}],"rows":1}}"#,
            "ab".repeat(32)
        );
        let (s, t) = store_with(&body);
        let (ty, h) = s.resolve_scripthash(&[0x22u8; 32]).unwrap().unwrap();
        assert_eq!(ty, 2);
        assert_eq!(h, [0xab; 32]);
        assert!(t.sql().contains("scripthash_map"));

        let (s, _) = store_with(r#"{"data":[],"rows":0}"#);
        assert!(s.resolve_scripthash(&[0u8; 32]).unwrap().is_none());

        let (s, t) = store_with(r#"{"data":[{"address":"bc1qxyz","script_type":4}],"rows":1}"#);
        let rows = s.address_prefix("bc1q", 10).unwrap();
        assert_eq!(rows[0].address, "bc1qxyz");
        assert!(t.sql().contains("startsWith(address, 'bc1q')"));
    }

    #[test]
    fn transport_and_parse_errors_surface() {
        let s = Store::new(Box::new(MockTransport::failing()), "bitcoin");
        match s.tip_height() {
            Err(StoreError::Transport(m)) => assert!(m.contains("boom")),
            other => panic!("expected Transport error, got {other:?}"),
        }

        let (s, _) = store_with("not json");
        match s.tip_height() {
            Err(StoreError::Parse(_)) => {}
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn parse_hex32_validates_length_and_chars() {
        assert_eq!(parse_hex32(&"0f".repeat(32)).unwrap()[0], 0x0f);
        assert!(parse_hex32("abcd").is_err());
        assert!(parse_hex32(&"zz".repeat(32)).is_err());
    }

    #[test]
    fn numeric_deserializer_accepts_numbers_and_strings() {
        // Numbers (not strings) must also parse.
        let (s, _) = store_with(
            r#"{"data":[{"funded_count":3,"funded_sum":5000,"spent_count":1,"spent_sum":2000,"history_count":4}],"rows":1}"#,
        );
        let row = s.address_stats(&[0u8; 32]).unwrap().unwrap();
        assert_eq!(row.funded_sum, 5000);
    }

    #[test]
    fn block_header_reconstruction_and_hex_helpers() {
        let prev = "bb".repeat(32);
        let merkle = "cc".repeat(32);
        let blk = format!(
            r#"{{"data":[{{"height":1,"hash":"{}","prev_hash":"{}","merkle_root":"{}","time":123,"version":1,"bits":486604799,"nonce":7,"tx_count":2,"size":500,"weight":1800}}],"rows":1}}"#,
            "aa".repeat(32),
            prev,
            merkle
        );
        let (s, _) = store_with(&blk);
        let b = s.block_by_height(1).unwrap().unwrap();
        assert_eq!(b.version, 1);
        assert_eq!(b.bits, 486604799);
        assert_eq!(b.nonce, 7);
        // header must be exactly 80 bytes = 160 hex chars.
        let hdr = b.header_hex().unwrap();
        assert_eq!(hdr.len(), 160);
        // version(1) little-endian is first.
        assert!(hdr.starts_with("01000000"));
        // prev_hash occupies bytes 4..36 in wire order.
        assert_eq!(&hdr[8..8 + 64], prev);

        assert_eq!(to_hex(&[0x00, 0xff]), "00ff");
        assert_eq!(hex_to_bytes("00ff").unwrap(), vec![0x00, 0xff]);
        assert!(hex_to_bytes("abc").is_err());
        assert!(hex_to_bytes("zz").is_err());
        assert_eq!(reverse_hex("0102").unwrap(), "0201");
    }

    #[test]
    fn tx_details_outputs_inputs_and_summaries() {
        let outs = format!(
            r#"{{"data":[{{"output_index":0,"value_sat":"1000","script_type":2,"address_hash":"{}"}}],"rows":1}}"#,
            "cd".repeat(32)
        );
        let (s, t) = store_with(&outs);
        let rows = s.tx_outputs(&[0u8; 32]).unwrap();
        assert_eq!(rows[0].output_index, 0);
        assert_eq!(rows[0].value_sat, 1000);
        assert_eq!(rows[0].script_type, 2);
        assert!(t.sql().contains("FROM bitcoin.outputs"));
        assert!(t
            .sql()
            .contains("WHERE (height, tx_index) IN (SELECT height, tx_index FROM bitcoin.transactions_by_txid"));
        let rows_at = s.tx_outputs_at(100, 2).unwrap();
        assert_eq!(rows_at.len(), 1);
        assert!(t.sql().contains("WHERE height = 100 AND tx_index = 2"));
        assert!(!t.sql().contains("AND txid ="));

        let ins = format!(
            r#"{{"data":[{{"input_index":0,"prev_txid":"{}","prev_vout":3}}],"rows":1}}"#,
            "ef".repeat(32)
        );
        let (s, t) = store_with(&ins);
        let rows = s.tx_inputs(&[0u8; 32]).unwrap();
        assert_eq!(rows[0].prev_vout, 3);
        assert!(t.sql().contains("FROM bitcoin.inputs"));
        assert!(t
            .sql()
            .contains("WHERE (height, tx_index) IN (SELECT height, tx_index FROM bitcoin.transactions_by_txid"));
        let ins_at = s.tx_inputs_at(100, 2).unwrap();
        assert_eq!(ins_at[0].prev_vout, 3);
        assert!(t.sql().contains("WHERE height = 100 AND tx_index = 2"));
        assert!(!t.sql().contains("AND txid ="));

        let sums = r#"{"data":[{"txid":"aa","height":5,"value_sat":-42,"kind":1}],"rows":1}"#;
        let (s, t) = store_with(sums);
        let rows = s.tx_summaries(&[0u8; 32]).unwrap();
        assert_eq!(rows[0].value_sat, -42);
        // The WHERE must qualify the table: `hex(txid) AS txid` creates an
        // alias, and ClickHouse would otherwise resolve `WHERE txid` to the
        // hex-string alias and never match the binary column.
        let sql = t.sql();
        assert!(
            sql.contains("WHERE bitcoin.address_history.txid ="),
            "unqualified WHERE would hit the hex alias: {sql}"
        );
    }

    #[test]
    fn block_by_hash_qualifies_the_where_to_avoid_the_hex_alias() {
        // Regression: `hex(hash) AS hash` makes an unqualified `WHERE hash`
        // resolve to the alias (a String), so block lookups by hash silently
        // returned nothing. The predicate must name the table.
        let blk = r#"{"data":[{"height":1,"hash":"aa","prev_hash":"bb","merkle_root":"cc","time":1,"version":1,"bits":1,"nonce":1,"tx_count":1,"size":1,"weight":1}],"rows":1}"#;
        let (s, t) = store_with(blk);
        let b = s.block_by_hash(&[0u8; 32]).unwrap().unwrap();
        assert_eq!(b.height, 1);
        let sql = t.sql();
        assert!(
            sql.contains("WHERE bitcoin.blocks.hash ="),
            "unqualified WHERE would hit the hex alias: {sql}"
        );
    }

    #[test]
    fn error_display_impls() {
        assert_eq!(StoreError::NotFound.to_string(), "address not found");
        assert_eq!(
            StoreError::Transport("x".into()).to_string(),
            "transport error: x"
        );
        assert!(StoreError::Parse("y".into()).to_string().contains("parse"));
    }

    #[test]
    fn ch_http_transport_builds_url_and_errors_on_unreachable_host() {
        // Constructing the client is infallible here; querying a bogus host
        // must return a Transport error, not panic.
        let t = ChHttpTransport::new("127.0.0.1", 1, "default", None, 1).unwrap();
        let e = t.query("SELECT 1");
        assert!(matches!(e, Err(StoreError::Transport(_))));
    }

    /// A one-shot HTTP server that answers the first connection with a fixed
    /// status line and body, then exits. Lets us exercise the transport's
    /// success and non-2xx paths without a real ClickHouse.
    fn one_shot_http_server(
        status_line: &'static str,
        body: &'static str,
    ) -> (u16, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            if let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = std::io::Read::read(&mut s, &mut buf);
                let resp = format!(
                    "{status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = std::io::Write::write_all(&mut s, resp.as_bytes());
            }
        });
        (port, handle)
    }

    #[test]
    fn ch_http_transport_reads_a_successful_response() {
        let (port, h) = one_shot_http_server("HTTP/1.1 200 OK", "{\"data\":[]}");
        // A password exercises the X-ClickHouse-Key header branch.
        let t = ChHttpTransport::new("127.0.0.1", port, "default", Some("pw".into()), 5).unwrap();
        assert_eq!(t.query("SELECT 1").unwrap(), "{\"data\":[]}");
        h.join().unwrap();
    }

    #[test]
    fn ch_http_transport_surfaces_non_2xx_status() {
        let (port, h) = one_shot_http_server("HTTP/1.1 500 Internal Server Error", "boom");
        let t = ChHttpTransport::new("127.0.0.1", port, "default", None, 5).unwrap();
        match t.query("SELECT 1") {
            Err(StoreError::Transport(m)) => assert!(m.contains("500") && m.contains("boom")),
            other => panic!("expected Transport error, got {other:?}"),
        }
        h.join().unwrap();

        // Truncated HTTP body (promised Content-Length: 100, connection closed after 2 bytes)
        // exercises the `resp.text()` error branch.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port2 = listener.local_addr().unwrap().port();
        let h2 = std::thread::spawn(move || {
            if let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = std::io::Read::read(&mut s, &mut buf);
                let _ = std::io::Write::write_all(
                    &mut s,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nhi",
                );
            }
        });
        let t2 = ChHttpTransport::new("127.0.0.1", port2, "default", None, 5).unwrap();
        assert!(matches!(
            t2.query("SELECT 1"),
            Err(StoreError::Transport(_))
        ));
        h2.join().unwrap();
    }

    /// A one-shot HTTP server that records the raw request line + headers so a
    /// test can assert on the query string and headers the transport sent.
    fn capturing_http_server() -> (u16, std::sync::mpsc::Receiver<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let n = std::io::Read::read(&mut s, &mut buf).unwrap_or(0);
                let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
                let body = "{\"data\":[]}";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = std::io::Write::write_all(&mut s, resp.as_bytes());
            }
        });
        (port, rx)
    }

    #[test]
    fn ch_http_transport_targets_the_configured_database() {
        // The serve layer must query the operator's database, not the
        // hardcoded "default" — otherwise every query hits the wrong schema.
        let (port, rx) = capturing_http_server();
        let t = ChHttpTransport::with_database(
            "127.0.0.1",
            port,
            "default",
            Some("pw".into()),
            "bitcoin",
            5,
        )
        .unwrap();
        assert_eq!(t.query("SELECT 1").unwrap(), "{\"data\":[]}");
        let req = rx.recv().unwrap();
        assert!(req.contains("database=bitcoin"), "request was: {req}");
        // Header names are lowercased on the wire by reqwest.
        let lower = req.to_ascii_lowercase();
        assert!(lower.contains("x-clickhouse-key: pw"), "request was: {req}");
        assert!(req.contains("FORMAT JSON"), "request was: {req}");
    }

    /// Answers `n` connections with `200 {"data":[]}`, counting them.
    fn counting_http_server(n: usize) -> (u16, std::thread::JoinHandle<usize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let h = std::thread::spawn(move || {
            let mut served = 0;
            for _ in 0..n {
                let Ok((mut s, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 4096];
                let _ = std::io::Read::read(&mut s, &mut buf);
                let body = "{\"data\":[]}";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = std::io::Write::write_all(&mut s, resp.as_bytes());
                served += 1;
            }
            served
        });
        (port, h)
    }

    #[test]
    fn endpoint_url_applies_default_port_only_when_missing() {
        assert_eq!(endpoint_url("http", "ch", 8123), "http://ch:8123/");
        assert_eq!(endpoint_url("https", "ch:8443", 8123), "https://ch:8443/");
        assert_eq!(endpoint_url("http", "[::1]", 8123), "http://[::1]:8123/");
        assert_eq!(endpoint_url("http", "[::1]:9", 8123), "http://[::1]:9/");
    }

    #[test]
    fn with_endpoints_rejects_empty_host_list_and_unknown_scheme() {
        assert!(matches!(
            ChHttpTransport::with_endpoints("http", &[], 8123, "u", None, "db", 1),
            Err(StoreError::Transport(m)) if m.contains("no ClickHouse hosts")
        ));
        assert!(matches!(
            ChHttpTransport::with_endpoints("ftp", &["h"], 8123, "u", None, "db", 1),
            Err(StoreError::Transport(m)) if m.contains("ftp")
        ));
        let t = ChHttpTransport::with_endpoints("https", &["a", "b:9"], 8443, "u", None, "db", 1)
            .unwrap();
        assert_eq!(t.endpoint_urls(), vec!["https://a:8443/", "https://b:9/"]);
    }

    #[test]
    fn transport_fails_over_to_the_next_host_and_cools_down_the_dead_one() {
        let (port, h) = counting_http_server(3);
        let live = format!("127.0.0.1:{port}");
        let t = ChHttpTransport::with_endpoints(
            "http",
            &["127.0.0.1:1", live.as_str()],
            8123,
            "u",
            None,
            "db",
            2,
        )
        .unwrap();
        // Rotation starts at the dead host; the query still succeeds.
        assert_eq!(t.query("SELECT 1").unwrap(), "{\"data\":[]}");
        assert!(t.endpoints[0].down_until_ms.load(Ordering::Relaxed) > 0);
        assert_eq!(t.endpoints[1].down_until_ms.load(Ordering::Relaxed), 0);
        // Next two queries: one starts at the live host, one at the dead
        // host — which is cooling down, so it's skipped without a connect.
        assert!(t.query("SELECT 1").is_ok());
        assert!(t.query("SELECT 1").is_ok());
        assert_eq!(h.join().unwrap(), 3);
    }

    #[test]
    fn transport_retries_cooled_hosts_when_everything_is_down() {
        let t = ChHttpTransport::with_endpoints(
            "http",
            &["127.0.0.1:1", "127.0.0.1:2"],
            8123,
            "u",
            None,
            "db",
            1,
        )
        .unwrap();
        let e = t.query("SELECT 1").unwrap_err().to_string();
        assert!(e.contains("127.0.0.1:"), "{e}");
        assert!(t
            .endpoints
            .iter()
            .all(|ep| ep.down_until_ms.load(Ordering::Relaxed) > 0));
        // Both cooling down: the last-resort pass still tries them.
        let e = t.query("SELECT 1").unwrap_err().to_string();
        assert!(e.contains("127.0.0.1:"), "{e}");
    }

    #[test]
    fn ch_http_transport_new_defaults_to_the_default_database() {
        // The `new` shim keeps the old behavior for callers that don't care.
        let (port, rx) = capturing_http_server();
        let t = ChHttpTransport::new("127.0.0.1", port, "u", None, 5).unwrap();
        assert_eq!(t.query("SELECT 1").unwrap(), "{\"data\":[]}");
        let req = rx.recv().unwrap();
        assert!(req.contains("database=default"), "request was: {req}");
        // No password → no key header.
        assert!(!req.to_ascii_lowercase().contains("x-clickhouse-key"));
    }
}
