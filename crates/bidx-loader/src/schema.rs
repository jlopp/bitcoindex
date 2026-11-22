//! ClickHouse DDL for the five core tables plus the full query-index
//! catalogue the lookup service exposes.
//!
//! Design notes:
//! * Hashes are `FixedString(32)` — raw wire-order bytes. Display hex is a
//!   `reverse(hex(...))` helper in the query layer, never stored.
//! * Base entity tables are ordered by their dominant scan pattern and
//!   partitioned in 10k-block ranges (cheap partition drops at the tip).
//! * Every *derived* index is an auto-maintained table fed by a
//!   `MATERIALIZED VIEW` over a base table, so `init-db` + `load` keeps the
//!   whole catalogue in sync with no extra write path. Three indexes
//!   (`coinstats_by_height`, `scripthash_map`, `address_prefix`) are backfills
//!   — see `bidx index rebuild`. The first two are pure SQL; `address_prefix`
//!   additionally needs a Rust-side address renderer.
//! * Reorder-only indexes are ClickHouse *projections* on the base table
//!   (no duplicated column set); ClickHouse selects them automatically.
//!
//! ## Index catalogue (19)
//!
//! | # | Index | Mechanism | Serves |
//! |---|-------|-----------|--------|
//! | 1 | `blocks_by_hash` | projection on `blocks` | block detail by hash |
//! | 2 | `transactions_by_txid` | table + MV | txid point lookup |
//! | 3 | `spends_by_spent_outpoint` | table + MV | outspend / is_spent |
//! | 4 | `outputs_by_address` | table + MV | address funding history |
//! | 5 | `spends_by_address` | table + MV | address spend history |
//! | 6 | `address_history` | table + MV (union) | unified address history |
//! | 7 | `utxos_by_address` | Collapsing + MV | address UTXO / balance |
//! | 8 | `outputs_by_script_type` | projection on `outputs` | type grouping |
//! | 9 | `idx_blocks_time` | minmax skip index | block range by date |
//! | 10 | `idx_outputs_value` | minmax skip index | value-band filters |
//! | 11 | `idx_txs_fee` | minmax skip index | fee-band filters |
//! | 12 | `blocks_by_time` | projection on `blocks` | time-ordered blocks |
//! | 13 | `fee_rate` | materialized column | fee-rate filter/percentiles |
//! | 14 | `spends_by_spent_height` | projection on `spends` | lifespan / CDD |
//! | 15 | `address_stats` | Summing + MV | address chain stats |
//! | 16 | `coinstats_by_height` | table + backfill | UTXO-set snapshots |
//! | 17 | `blocks_by_miner` | table + MV | blocks by miner |
//! | 18 | `address_prefix` | table + backfill | address prefix search |
//! | 19 | `scripthash_map` | table + MV | Electrum scripthash resolution |
//!
//! The pre-existing `inputs_by_prev_output` table is also kept and now
//! auto-maintained by an MV.

pub const ENTITIES: [&str; 5] = ["blocks", "transactions", "inputs", "outputs", "spends"];

/// Script type code for `OP_RETURN` (unspendable data carrier). Excluded from
/// every address-keyed index because its `address_hash` is all zeros and it
/// carries no spendable value.
const SCRIPT_TYPE_OP_RETURN: u8 = 7;

/// One entry in the query-index catalogue, for `bidx index list` and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: &'static str,
    pub mechanism: &'static str,
    pub target: &'static str,
}

/// The 19 indexes the serving layer maintains, in catalogue order.
pub fn index_catalog() -> Vec<IndexSpec> {
    vec![
        IndexSpec { name: "blocks_by_hash", mechanism: "projection", target: "blocks" },
        IndexSpec { name: "transactions_by_txid", mechanism: "table+mv", target: "transactions" },
        IndexSpec { name: "spends_by_spent_outpoint", mechanism: "table+mv", target: "spends" },
        IndexSpec { name: "outputs_by_address", mechanism: "table+mv", target: "outputs" },
        IndexSpec { name: "spends_by_address", mechanism: "table+mv", target: "spends" },
        IndexSpec { name: "address_history", mechanism: "table+mv", target: "outputs+spends" },
        IndexSpec { name: "utxos_by_address", mechanism: "collapsing+mv", target: "outputs+spends" },
        IndexSpec { name: "outputs_by_script_type", mechanism: "projection", target: "outputs" },
        IndexSpec { name: "idx_blocks_time", mechanism: "skip-index", target: "blocks" },
        IndexSpec { name: "idx_outputs_value", mechanism: "skip-index", target: "outputs" },
        IndexSpec { name: "idx_txs_fee", mechanism: "skip-index", target: "transactions" },
        IndexSpec { name: "blocks_by_time", mechanism: "projection", target: "blocks" },
        IndexSpec { name: "fee_rate", mechanism: "materialized-column", target: "transactions" },
        IndexSpec { name: "spends_by_spent_height", mechanism: "projection", target: "spends" },
        IndexSpec { name: "address_stats", mechanism: "summing+mv", target: "outputs+spends" },
        IndexSpec { name: "coinstats_by_height", mechanism: "backfill", target: "outputs+spends" },
        IndexSpec { name: "blocks_by_miner", mechanism: "table+mv", target: "outputs" },
        IndexSpec { name: "address_prefix", mechanism: "backfill", target: "outputs" },
        IndexSpec { name: "scripthash_map", mechanism: "table+mv", target: "outputs" },
    ]
}

/// Every DDL statement needed to bring a database up to the full catalogue,
/// in dependency order (base tables -> base-table indexes -> index tables ->
/// materialized views). All statements are idempotent.
pub fn all_ddl(db: &str) -> Vec<String> {
    vec![
        // --- base entity tables ---
        blocks(db),
        transactions(db),
        inputs(db),
        outputs(db),
        spends(db),
        // --- indexes added in place on base tables (projections, skip
        //     indexes, materialized column) ---
        proj_blocks_by_hash(db),             // 1
        proj_outputs_by_script_type(db),     // 8
        skip_blocks_time(db),                // 9
        skip_outputs_value(db),              // 10
        skip_txs_fee(db),                    // 11
        proj_blocks_by_time(db),             // 12
        col_fee_rate(db),                    // 13
        proj_spends_by_spent_height(db),     // 14
        // --- standalone index tables (populated by the MVs below) ---
        transactions_by_txid(db),            // 2
        inputs_by_prev_output(db),
        outputs_by_address(db),              // 4
        spends_by_spent_outpoint(db),        // 3
        spends_by_address(db),               // 5
        address_history(db),                 // 6
        utxos_by_address(db),                // 7
        address_stats(db),                   // 15
        coinstats_by_height(db),             // 16
        blocks_by_miner(db),                 // 17
        address_prefix(db),                  // 18
        scripthash_map(db),                  // 19
        // --- materialized views (source + target both exist by now) ---
        mv_transactions_by_txid(db),         // 2
        mv_inputs_by_prev_output(db),
        mv_outputs_by_address(db),           // 4
        mv_spends_by_spent_outpoint(db),     // 3
        mv_spends_by_address(db),            // 5
        mv_address_history_fund(db),         // 6
        mv_address_history_spend(db),        // 6
        mv_utxos_fund(db),                   // 7
        mv_utxos_spend(db),                  // 7
        mv_address_stats_fund(db),           // 15
        mv_address_stats_spend(db),          // 15
        mv_blocks_by_miner(db),              // 17
        mv_scripthash_map(db),               // 19
    ]
}

/// Names of every table `all_ddl` creates (base + index tables, not the
/// materialized views). Used by the loader and tests.
pub fn all_table_names() -> Vec<&'static str> {
    vec![
        "blocks",
        "transactions",
        "inputs",
        "outputs",
        "spends",
        "transactions_by_txid",
        "inputs_by_prev_output",
        "outputs_by_address",
        "spends_by_spent_outpoint",
        "spends_by_address",
        "address_history",
        "utxos_by_address",
        "address_stats",
        "coinstats_by_height",
        "blocks_by_miner",
        "address_prefix",
        "scripthash_map",
    ]
}

// ---------------------------------------------------------------------------
// Schema versioning
// ---------------------------------------------------------------------------

/// Current schema version. **Bump this and add a matching entry to
/// [`migrations`] whenever a change alters an existing table's shape** (a new
/// column on a base table, a changed key, a dropped index, a changed engine).
/// Additive, `IF NOT EXISTS`-guarded DDL in [`all_ddl`] does not need a version
/// bump — re-running `init-db` already applies it — but anything an operator
/// must *migrate* rather than merely re-create does.
pub const SCHEMA_VERSION: u32 = 2;

/// Ordered, forward-only migrations applied after [`all_ddl`]. Each entry is
/// `(version, statements)`; a fresh database starts at version
/// [`SCHEMA_VERSION`] because `all_ddl` already contains the current shape,
/// and only *upgrades* of an older database run statements here.
pub fn migrations() -> Vec<(u32, Vec<String>)> {
    // 2: rebuild `address_stats` from AggregatingMergeTree (which silently
    //    dropped rows for addresses funded more than once) to
    //    SummingMergeTree, backfilling the counters from `outputs`/`spends`.
    vec![(2, migrate_address_stats_to_summing("__DB__"))]
}

/// Migration to schema version 2: replace `address_stats` with the
/// `SummingMergeTree` definition from [`address_stats`].
///
/// Why a rebuild rather than an `ALTER`: ClickHouse cannot change a table's
/// engine in place, and the existing rows are already wrong (the old engine
/// dropped them during merges), so they must be recomputed anyway. The
/// sequence is:
///
/// 1. `DROP TABLE IF EXISTS address_stats_new` — clear any leftover from a
///    previous, failed attempt (the backfill below appends, so a retry must
///    start from an empty table or it would double-count).
/// 2. Create `address_stats_new` with the `SummingMergeTree` engine.
/// 3. Backfill it from the ground-truth base tables — one grouped insert from
///    `outputs` (fundings) and one from `spends` — mirroring the two MVs.
/// 4. `DROP TABLE IF EXISTS address_stats`, then `RENAME TABLE
///    address_stats_new TO address_stats`. The two MVs are not dropped;
///    ClickHouse keeps a materialized view bound to a table's *name*, so after
///    the rename they feed the new table (verified against ClickHouse 24.8).
///
/// Every step is idempotent, so a failure at any point is repaired by simply
/// re-running `bidx migrate` (the version is only recorded after all six
/// statements succeed): the leading drop clears a half-built table, and
/// `IF EXISTS` on the old-table drop tolerates a retry after the swap already
/// happened. The rename is last so an interrupted run never leaves the index
/// without an `address_stats` table. The `__DB__` token is replaced with the
/// real database by [`materialize_migrations`] when the migration is applied.
fn migrate_address_stats_to_summing(db: &str) -> Vec<String> {
    vec![
        // 1. Clear any half-built table from a previous failed attempt.
        format!("DROP TABLE IF EXISTS {db}.address_stats_new"),
        // 2. New table with the correct engine.
        address_stats(db).replace("address_stats ", "address_stats_new "),
        // 3. Backfill fundings from `outputs` (same shape as the fund MV).
        format!(
            "INSERT INTO {db}.address_stats_new \
             (address_hash, funded_count, funded_sum, spent_count, spent_sum, history_count) \
             SELECT address_hash, toUInt64(count()), sum(value_sat), \
                    toUInt64(0), toUInt64(0), toUInt64(count()) \
             FROM {db}.outputs WHERE script_type != {SCRIPT_TYPE_OP_RETURN} \
             GROUP BY address_hash"
        ),
        // ...and spends from `spends` (same shape as the spend MV).
        format!(
            "INSERT INTO {db}.address_stats_new \
             (address_hash, funded_count, funded_sum, spent_count, spent_sum, history_count) \
             SELECT spent_address_hash, toUInt64(0), toUInt64(0), \
                    toUInt64(count()), sum(spent_value_sat), toUInt64(count()) \
             FROM {db}.spends WHERE spent_script_type != {SCRIPT_TYPE_OP_RETURN} \
             GROUP BY spent_address_hash"
        ),
        // 4. Swap in the new table. Rename last so a failure leaves the old
        //    table intact; the MVs follow the name.
        format!("DROP TABLE IF EXISTS {db}.address_stats"),
        format!("RENAME TABLE {db}.address_stats_new TO {db}.address_stats"),
    ]
}

/// Replace the `__DB__` token in migration statements with the real database
/// name. Migrations are defined once (at module scope, where the database name
/// is not known) and materialized per target database here.
pub fn materialize_migrations(db: &str) -> Vec<(u32, Vec<String>)> {
    migrations()
        .into_iter()
        .map(|(v, stmts)| {
            (
                v,
                stmts
                    .into_iter()
                    .map(|s| s.replace("__DB__", db))
                    .collect(),
            )
        })
        .collect()
}

/// DDL for the schema-version bookkeeping table.
pub fn meta_schema(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.meta_schema \
         (version UInt32, applied_at DateTime DEFAULT now()) \
         ENGINE = ReplacingMergeTree(applied_at) ORDER BY version"
    )
}

/// Statement recording that `version` has been applied.
pub fn record_schema_version(db: &str, version: u32) -> String {
    format!("INSERT INTO {db}.meta_schema (version) VALUES ({version})")
}

/// Query returning the highest applied schema version (0 if none).
pub fn read_schema_version(db: &str) -> String {
    format!("SELECT ifNull(max(version), 0) AS version FROM {db}.meta_schema")
}

// ---------------------------------------------------------------------------
// Base tables
// ---------------------------------------------------------------------------

fn blocks(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.blocks \
         (height UInt32, hash FixedString(32), prev_hash FixedString(32), \
          merkle_root FixedString(32), time UInt32, bits UInt32, nonce UInt32, \
          version Int32, tx_count UInt32, size UInt32, weight UInt32, \
          total_fee UInt64, coinbase_value UInt64) \
         ENGINE = MergeTree \
         PARTITION BY intDiv(height, 10000) \
         ORDER BY height"
    )
}

fn transactions(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.transactions \
         (height UInt32, tx_index UInt32, txid FixedString(32), version Int32, \
          locktime UInt32, size UInt32, weight UInt32, fee UInt64, \
          input_count UInt32, output_count UInt32, has_witness UInt8) \
         ENGINE = MergeTree \
         PARTITION BY intDiv(height, 10000) \
         ORDER BY (height, tx_index)"
    )
}

fn inputs(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.inputs \
         (height UInt32, tx_index UInt32, input_index UInt32, txid FixedString(32), \
          prev_txid FixedString(32), prev_vout UInt32, script_sig_len UInt32, \
          sequence UInt32, witness_items UInt32, witness_bytes UInt32, is_coinbase UInt8) \
         ENGINE = MergeTree \
         PARTITION BY intDiv(height, 10000) \
         ORDER BY (height, tx_index, input_index)"
    )
}

fn outputs(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.outputs \
         (height UInt32, tx_index UInt32, output_index UInt32, txid FixedString(32), \
          value_sat UInt64, script_pubkey_len UInt32, script_type UInt8, \
          address_hash FixedString(32)) \
         ENGINE = MergeTree \
         PARTITION BY intDiv(height, 10000) \
         ORDER BY (height, tx_index, output_index)"
    )
}

fn spends(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.spends \
         (height UInt32, spending_tx_index UInt32, spending_input_index UInt32, \
          spending_txid FixedString(32), spent_txid FixedString(32), spent_vout UInt32, \
          spent_value_sat UInt64, spent_script_type UInt8, \
          spent_address_hash FixedString(32), spent_height UInt32) \
         ENGINE = MergeTree \
         PARTITION BY intDiv(height, 10000) \
         ORDER BY (height, spending_tx_index, spending_input_index)"
    )
}

// ---------------------------------------------------------------------------
// In-place indexes on base tables
// ---------------------------------------------------------------------------

/// #1 block hash -> row. Reorder-only, so a projection (no duplicate columns
/// beyond the projection's own key storage).
fn proj_blocks_by_hash(db: &str) -> String {
    format!("ALTER TABLE {db}.blocks ADD PROJECTION IF NOT EXISTS blocks_by_hash (SELECT * ORDER BY hash)")
}

/// #8 group outputs by script type over time.
fn proj_outputs_by_script_type(db: &str) -> String {
    format!(
        "ALTER TABLE {db}.outputs ADD PROJECTION IF NOT EXISTS outputs_by_script_type \
         (SELECT * ORDER BY (script_type, height))"
    )
}

/// #9 block date-range scans.
fn skip_blocks_time(db: &str) -> String {
    format!("ALTER TABLE {db}.blocks ADD INDEX IF NOT EXISTS idx_blocks_time time TYPE minmax GRANULARITY 4")
}

/// #10 large-transfer / value-band filters.
fn skip_outputs_value(db: &str) -> String {
    format!("ALTER TABLE {db}.outputs ADD INDEX IF NOT EXISTS idx_outputs_value value_sat TYPE minmax GRANULARITY 4")
}

/// #11 fee-band filters.
fn skip_txs_fee(db: &str) -> String {
    format!("ALTER TABLE {db}.transactions ADD INDEX IF NOT EXISTS idx_txs_fee fee TYPE minmax GRANULARITY 4")
}

/// #12 time-ordered block scans.
fn proj_blocks_by_time(db: &str) -> String {
    format!("ALTER TABLE {db}.blocks ADD PROJECTION IF NOT EXISTS blocks_by_time (SELECT * ORDER BY time)")
}

/// #13 fee rate in sat/vB, derived once at insert time. `weight` is BIP141
/// weight units; sat/vB = fee / vsize = fee * 4 / weight.
fn col_fee_rate(db: &str) -> String {
    format!(
        "ALTER TABLE {db}.transactions ADD COLUMN IF NOT EXISTS fee_rate UInt32 \
         MATERIALIZED if(weight > 0, toUInt32((fee * 4) / weight), 0)"
    )
}

/// #14 output lifespan / coin-days-destroyed grouping by creation height.
fn proj_spends_by_spent_height(db: &str) -> String {
    format!(
        "ALTER TABLE {db}.spends ADD PROJECTION IF NOT EXISTS spends_by_spent_height \
         (SELECT * ORDER BY (spent_height, height))"
    )
}

// ---------------------------------------------------------------------------
// Standalone index tables
// ---------------------------------------------------------------------------

fn transactions_by_txid(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.transactions_by_txid \
         (txid FixedString(32), height UInt32, tx_index UInt32) \
         ENGINE = MergeTree \
         ORDER BY txid"
    )
}

fn inputs_by_prev_output(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.inputs_by_prev_output \
         (prev_txid FixedString(32), prev_vout UInt32, height UInt32, \
          tx_index UInt32, input_index UInt32, txid FixedString(32)) \
         ENGINE = MergeTree \
         ORDER BY (prev_txid, prev_vout)"
    )
}

fn outputs_by_address(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.outputs_by_address \
         (address_hash FixedString(32), script_type UInt8, height UInt32, \
          tx_index UInt32, output_index UInt32, txid FixedString(32), value_sat UInt64) \
         ENGINE = MergeTree \
         ORDER BY (address_hash, height, tx_index, output_index)"
    )
}

/// #3 outpoint -> the input that spent it. Answers Esplora's `/outspends` and
/// Core's `gettxspendingprevout`.
fn spends_by_spent_outpoint(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.spends_by_spent_outpoint \
         (spent_txid FixedString(32), spent_vout UInt32, height UInt32, \
          spending_tx_index UInt32, spending_input_index UInt32, spending_txid FixedString(32)) \
         ENGINE = MergeTree \
         ORDER BY (spent_txid, spent_vout)"
    )
}

/// #5 address -> spends. The spend side of address history and balance.
fn spends_by_address(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.spends_by_address \
         (spent_address_hash FixedString(32), height UInt32, spending_tx_index UInt32, \
          spending_input_index UInt32, spent_txid FixedString(32), spent_vout UInt32, \
          spent_value_sat UInt64, spent_height UInt32) \
         ENGINE = MergeTree \
         ORDER BY (spent_address_hash, height, spending_tx_index, spending_input_index)"
    )
}

/// #6 unified funding + spending history per address. `value_sat` is signed:
/// positive when the address is funded, negative when it spends.
fn address_history(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.address_history \
         (address_hash FixedString(32), height UInt32, tx_index UInt32, \
          txid FixedString(32), value_sat Int64, kind UInt8) \
         ENGINE = MergeTree \
         PARTITION BY intDiv(height, 10000) \
         ORDER BY (address_hash, height, tx_index)"
    )
}

/// #7 per-address live UTXO view. CollapsingMergeTree: a funding row (+1) is
/// cancelled by a matching spend row (-1), so `FINAL` yields the UTXO set.
fn utxos_by_address(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.utxos_by_address \
         (address_hash FixedString(32), script_type UInt8, height UInt32, \
          tx_index UInt32, output_index UInt32, txid FixedString(32), \
          value_sat UInt64, sign Int8) \
         ENGINE = CollapsingMergeTree(sign) \
         ORDER BY (address_hash, txid, output_index)"
    )
}

/// #15 per-address aggregate counters (Esplora `chain_stats`).
///
/// `SummingMergeTree`, **not** `AggregatingMergeTree`. The counters are plain
/// `UInt64` columns, and `AggregatingMergeTree` over plain columns keeps only
/// **one row per sort key** during a background merge — so multiple fundings of
/// the same address collapsed to a single row and the counters under-counted
/// (a merge could even discard a spend). `SummingMergeTree` sums plain columns
/// correctly across merges, and reads stay a plain `sum(...)` with no
/// `-Merge`/`FINAL` gymnastics. See [`migrate_address_stats_to_summing`] for the
/// upgrade path from the old engine.
fn address_stats(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.address_stats \
         (address_hash FixedString(32), funded_count UInt64, funded_sum UInt64, \
          spent_count UInt64, spent_sum UInt64, history_count UInt64) \
         ENGINE = SummingMergeTree \
         ORDER BY address_hash"
    )
}

/// #16 UTXO-set snapshot per height (Core `coinstatsindex` analog). Backfilled
/// by `bidx index rebuild coinstats`.
fn coinstats_by_height(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.coinstats_by_height \
         (height UInt32, utxo_count UInt64, total_value_sat UInt64) \
         ENGINE = MergeTree \
         ORDER BY height"
    )
}

/// #17 coinbase payout address -> blocks. The coinbase output address is a
/// robust miner-identity proxy (no name heuristics).
fn blocks_by_miner(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.blocks_by_miner \
         (miner_hash FixedString(32), height UInt32, tx_index UInt32, coinbase_value UInt64) \
         ENGINE = MergeTree \
         ORDER BY (miner_hash, height)"
    )
}

/// #18 encoded address -> (script_type, address_hash), for prefix search.
/// Backfilled by `bidx index rebuild address_prefix` (Rust-rendered; see
/// [`address_prefix_source`]).
fn address_prefix(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.address_prefix \
         (address String, script_type UInt8, address_hash FixedString(32)) \
         ENGINE = MergeTree \
         ORDER BY address"
    )
}

/// #19 Electrum scripthash -> (script_type, address_hash). Electrum clients
/// send `sha256(scriptPubKey)` (display order) and it cannot be inverted, so
/// the serving layer looks the incoming hash up here. Populated from `outputs`
/// by an MV; the scriptPubKey is reconstructed from `(script_type,
/// address_hash)` with the same bytes the serving layer's
/// `address::script_pubkey` produces.
fn scripthash_map(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.scripthash_map \
         (scripthash FixedString(32), script_type UInt8, address_hash FixedString(32)) \
         ENGINE = ReplacingMergeTree \
         ORDER BY scripthash"
    )
}

// ---------------------------------------------------------------------------
// Materialized views — one per derived index table
// ---------------------------------------------------------------------------

fn mv_transactions_by_txid(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.transactions_by_txid_mv \
         TO {db}.transactions_by_txid AS \
         SELECT txid, height, tx_index FROM {db}.transactions"
    )
}

fn mv_inputs_by_prev_output(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.inputs_by_prev_output_mv \
         TO {db}.inputs_by_prev_output AS \
         SELECT prev_txid, prev_vout, height, tx_index, input_index, txid FROM {db}.inputs"
    )
}

fn mv_outputs_by_address(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.outputs_by_address_mv \
         TO {db}.outputs_by_address AS \
         SELECT address_hash, script_type, height, tx_index, output_index, txid, value_sat \
         FROM {db}.outputs"
    )
}

fn mv_spends_by_spent_outpoint(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.spends_by_spent_outpoint_mv \
         TO {db}.spends_by_spent_outpoint AS \
         SELECT spent_txid, spent_vout, height, spending_tx_index, spending_input_index, spending_txid \
         FROM {db}.spends"
    )
}

fn mv_spends_by_address(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.spends_by_address_mv \
         TO {db}.spends_by_address AS \
         SELECT spent_address_hash, height, spending_tx_index, spending_input_index, \
                spent_txid, spent_vout, spent_value_sat, spent_height \
         FROM {db}.spends"
    )
}

fn mv_address_history_fund(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.address_history_fund_mv \
         TO {db}.address_history AS \
         SELECT address_hash, height, tx_index, txid, toInt64(value_sat) AS value_sat, \
                toUInt8(0) AS kind \
         FROM {db}.outputs WHERE script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

fn mv_address_history_spend(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.address_history_spend_mv \
         TO {db}.address_history AS \
         SELECT spent_address_hash AS address_hash, height, spending_tx_index AS tx_index, \
                spending_txid AS txid, toInt64(-spent_value_sat) AS value_sat, toUInt8(1) AS kind \
         FROM {db}.spends WHERE spent_script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

fn mv_utxos_fund(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.utxos_fund_mv \
         TO {db}.utxos_by_address AS \
         SELECT address_hash, script_type, height, tx_index, output_index, txid, value_sat, \
                toInt8(1) AS sign \
         FROM {db}.outputs WHERE script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

fn mv_utxos_spend(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.utxos_spend_mv \
         TO {db}.utxos_by_address AS \
         SELECT spent_address_hash AS address_hash, spent_script_type AS script_type, height, \
                spending_tx_index AS tx_index, spent_vout AS output_index, spent_txid AS txid, \
                spent_value_sat AS value_sat, toInt8(-1) AS sign \
         FROM {db}.spends WHERE spent_script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

fn mv_address_stats_fund(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.address_stats_fund_mv \
         TO {db}.address_stats AS \
         SELECT address_hash, toUInt64(1) AS funded_count, value_sat AS funded_sum, \
                toUInt64(0) AS spent_count, toUInt64(0) AS spent_sum, toUInt64(1) AS history_count \
         FROM {db}.outputs WHERE script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

fn mv_address_stats_spend(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.address_stats_spend_mv \
         TO {db}.address_stats AS \
         SELECT spent_address_hash AS address_hash, toUInt64(0) AS funded_count, \
                toUInt64(0) AS funded_sum, toUInt64(1) AS spent_count, \
                spent_value_sat AS spent_sum, toUInt64(1) AS history_count \
         FROM {db}.spends WHERE spent_script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

fn mv_blocks_by_miner(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.blocks_by_miner_mv \
         TO {db}.blocks_by_miner AS \
         SELECT address_hash AS miner_hash, height, tx_index, value_sat AS coinbase_value \
         FROM {db}.outputs WHERE tx_index = 0 AND script_type != {SCRIPT_TYPE_OP_RETURN}"
    )
}

/// #19 Electrum scripthash map. The `multiIf` reconstructs exactly the
/// scriptPubKey bytes that `bidx_serve::address::script_pubkey` builds from the
/// same `(script_type, address_hash)`, then hashes it. `SHA256()` returns the
/// raw 32-byte digest in internal byte order, which is what the Electrum
/// surface queries with (it reverses the client's display-order hex first).
///
/// Only types 2-6 have a reconstructable script; P2PK (1), BareMultisig (8) and
/// WitnessUnknown (9) hash the full script/pubkey, which we don't store, so
/// they are excluded (matching `script_pubkey` returning `None`).
fn mv_scripthash_map(db: &str) -> String {
    format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.scripthash_map_mv \
         TO {db}.scripthash_map AS \
         SELECT \
           SHA256(multiIf( \
             script_type = 2, concat(unhex('76a914'), substring(address_hash, 1, 20), unhex('88ac')), \
             script_type = 3, concat(unhex('a914'), substring(address_hash, 1, 20), unhex('87')), \
             script_type = 4, concat(unhex('0014'), substring(address_hash, 1, 20)), \
             script_type = 5, concat(unhex('0020'), substring(address_hash, 1, 32)), \
             script_type = 6, concat(unhex('5120'), substring(address_hash, 1, 32)), \
             unhex(''))) AS scripthash, \
           script_type, address_hash \
         FROM {db}.outputs \
         WHERE script_type IN (2, 3, 4, 5, 6)"
    )
}

// ---------------------------------------------------------------------------
// Backfill queries (not DDL, but the SQL `bidx index rebuild` runs)
// ---------------------------------------------------------------------------

/// Backfill `coinstats_by_height`: cumulative UTXO count and value at each
/// 10k-block checkpoint, computed from the funding/spend history. Idempotent
/// via `TRUNCATE` first.
pub fn backfill_coinstats(db: &str) -> Vec<String> {
    vec![
        format!("TRUNCATE TABLE {db}.coinstats_by_height"),
        format!(
            "INSERT INTO {db}.coinstats_by_height (height, utxo_count, total_value_sat) \
             SELECT height, \
                    toUInt64(sum(sign)) AS utxo_count, \
                    sum(if(sign = 1, value_sat, 0)) AS total_value_sat \
             FROM ( \
                 SELECT intDiv(height, 10000) * 10000 AS height, toInt8(1) AS sign, toInt64(value_sat) AS value_sat \
                 FROM {db}.outputs WHERE script_type != {SCRIPT_TYPE_OP_RETURN} \
                 UNION ALL \
                 SELECT intDiv(height, 10000) * 10000 AS height, toInt8(-1) AS sign, toInt64(0) AS value_sat \
                 FROM {db}.spends WHERE spent_script_type != {SCRIPT_TYPE_OP_RETURN} \
             ) \
             GROUP BY height ORDER BY height"
        ),
    ]
}

/// Distinct `(script_type, address_hash)` pairs an address can be rendered
/// from. ClickHouse has no base58/bech32 encoder, so `bidx index rebuild
/// address_prefix` streams these pairs out, renders each address in Rust (the
/// renderer lives in the serving layer), and inserts the result. Only types
/// with an encoded address form are selected: `OP_RETURN` (7) and `Unknown`
/// (0) have none, and `WitnessUnknown` (9) has no canonical encoding.
pub fn address_prefix_source(db: &str) -> String {
    format!(
        "SELECT DISTINCT script_type, hex(address_hash) AS address_hash \
         FROM {db}.outputs \
         WHERE script_type IN (1, 2, 3, 4, 5, 6, 8)"
    )
}

/// Build one `INSERT` for a batch of rendered `(address, script_type,
/// address_hash_hex)` rows. `address` is emitted as an escaped string literal;
/// `address_hash` is bound through `unhex(...)`, never interpolated raw.
pub fn insert_address_prefix(db: &str, rows: &[(String, u8, String)]) -> String {
    let values: Vec<String> = rows
        .iter()
        .map(|(addr, ty, hash_hex)| {
            format!("({}, {}, unhex('{}'))", SqlLit(addr), ty, hash_hex)
        })
        .collect();
    format!(
        "INSERT INTO {db}.address_prefix (address, script_type, address_hash) VALUES {}",
        values.join(", ")
    )
}

/// Escape a single-quoted ClickHouse string literal. Address strings are
/// base58/bech32 and never contain a quote, but escape defensively so a
/// malformed render can never break out of the literal.
struct SqlLit<'a>(&'a str);

impl std::fmt::Display for SqlLit<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let escaped = self.0.replace('\\', "\\\\").replace('\'', "\\'");
        write!(f, "'{escaped}'")
    }
}

/// Backfill `scripthash_map` from the current `outputs` table. The MV keeps it
/// current for new loads, but a database that was loaded *before* the MV
/// existed has an empty map and the whole Electrum surface 404s, so operators
/// run `bidx index rebuild scripthash-map` once after upgrading. `DISTINCT`
/// collapses the per-output duplicates before the insert.
pub fn backfill_scripthash_map(db: &str) -> Vec<String> {
    vec![
        format!("TRUNCATE TABLE {db}.scripthash_map"),
        format!(
            "INSERT INTO {db}.scripthash_map (scripthash, script_type, address_hash) \
             SELECT DISTINCT \
               SHA256(multiIf( \
                 script_type = 2, concat(unhex('76a914'), substring(address_hash, 1, 20), unhex('88ac')), \
                 script_type = 3, concat(unhex('a914'), substring(address_hash, 1, 20), unhex('87')), \
                 script_type = 4, concat(unhex('0014'), substring(address_hash, 1, 20)), \
                 script_type = 5, concat(unhex('0020'), substring(address_hash, 1, 32)), \
                 script_type = 6, concat(unhex('5120'), substring(address_hash, 1, 32)), \
                 unhex(''))) AS scripthash, \
               script_type, address_hash \
             FROM {db}.outputs \
             WHERE script_type IN (2, 3, 4, 5, 6)"
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_ddl_is_idempotent_and_covers_base_entities() {
        let ddls = all_ddl("testdb");
        let body = ddls.join("\n");
        // Every statement must be guarded so `init-db` can re-run.
        for s in &ddls {
            assert!(
                s.contains("IF NOT EXISTS"),
                "statement is not idempotent: {s}"
            );
        }
        for entity in ENTITIES {
            assert!(body.contains(&format!("testdb.{entity} ")), "missing base table {entity}");
        }
    }

    #[test]
    fn hashes_are_fixed_size_binary_only() {
        let body = all_ddl("db").join("\n");
        assert!(body.contains("FixedString(32)"));
        assert!(!body.contains(" String "), "must not use variable-length String for hashes");
        assert!(!body.contains("(String"), "no bare String columns except address_prefix");
    }

    #[test]
    fn catalogue_has_nineteen_indexes_each_referenced_by_ddl() {
        let cat = index_catalog();
        assert_eq!(cat.len(), 19, "catalogue must list 19 indexes");
        let body = all_ddl("db").join("\n");
        for spec in &cat {
            // Each index name appears in at least one DDL statement.
            assert!(
                body.contains(spec.name),
                "catalogue entry {} has no DDL",
                spec.name
            );
        }
    }

    #[test]
    fn derived_index_tables_all_have_a_materialized_view() {
        // Every table fed by an MV must have that MV in the DDL, and the MV
        // must be defined after its target table.
        let ddls = all_ddl("db");
        let body = ddls.join("\n");
        for (table, mv) in [
            ("transactions_by_txid", "transactions_by_txid_mv"),
            ("inputs_by_prev_output", "inputs_by_prev_output_mv"),
            ("outputs_by_address", "outputs_by_address_mv"),
            ("spends_by_spent_outpoint", "spends_by_spent_outpoint_mv"),
            ("spends_by_address", "spends_by_address_mv"),
            ("address_history", "address_history_fund_mv"),
            ("address_history", "address_history_spend_mv"),
            ("utxos_by_address", "utxos_fund_mv"),
            ("utxos_by_address", "utxos_spend_mv"),
            ("address_stats", "address_stats_fund_mv"),
            ("address_stats", "address_stats_spend_mv"),
            ("blocks_by_miner", "blocks_by_miner_mv"),
            ("scripthash_map", "scripthash_map_mv"),
        ] {
            assert!(body.contains(mv), "missing MV {mv}");
            let table_at = body.find(&format!("db.{table} ")).or_else(|| body.find(&format!("db.{table}\n")));
            let mv_at = body.find(mv).expect("mv present");
            let table_at = table_at.expect("table present");
            assert!(table_at < mv_at, "MV {mv} must be defined after table {table}");
        }
    }

    #[test]
    fn projections_and_skip_indexes_are_present() {        let body = all_ddl("db").join("\n");
        for proj in ["blocks_by_hash", "outputs_by_script_type", "blocks_by_time", "spends_by_spent_height"] {
            assert!(body.contains(&format!("ADD PROJECTION IF NOT EXISTS {proj}")), "missing projection {proj}");
        }
        for idx in ["idx_blocks_time", "idx_outputs_value", "idx_txs_fee"] {
            assert!(body.contains(&format!("ADD INDEX IF NOT EXISTS {idx}")), "missing skip index {idx}");
        }
        assert!(body.contains("ADD COLUMN IF NOT EXISTS fee_rate"), "missing fee_rate column");
    }

    #[test]
    fn fee_rate_expression_is_wellformed() {
        let sql = col_fee_rate("db");
        // sat/vB = fee * 4 / weight, guarded against zero weight.
        assert!(sql.contains("if(weight > 0"));
        assert!(sql.contains("(fee * 4) / weight"));
    }

    #[test]
    fn utxo_view_is_collapsing_on_sign() {
        let sql = utxos_by_address("db");
        assert!(sql.contains("CollapsingMergeTree(sign)"));
        // Funding and spend MVs must produce +1 / -1.
        assert!(mv_utxos_fund("db").contains("toInt8(1) AS sign"));
        assert!(mv_utxos_spend("db").contains("toInt8(-1) AS sign"));
    }

    #[test]
    fn address_stats_uses_summing_engine() {
        let sql = address_stats("db");
        // SummingMergeTree sums the plain counter columns correctly across
        // merges; AggregatingMergeTree over plain columns keeps only one row per
        // key and silently drops rows for repeatedly-funded addresses.
        assert!(sql.contains("SummingMergeTree"), "must be SummingMergeTree: {sql}");
        assert!(
            !sql.contains("AggregatingMergeTree"),
            "AggregatingMergeTree over plain columns drops rows: {sql}"
        );
        // Both MVs must carry all five counter columns with matching names.
        for mv in [mv_address_stats_fund("db"), mv_address_stats_spend("db")] {
            for col in ["funded_count", "funded_sum", "spent_count", "spent_sum", "history_count"] {
                assert!(mv.contains(col), "MV missing counter {col}: {mv}");
            }
        }
    }

    /// The v2 migration must rebuild `address_stats` with the SummingMergeTree
    /// engine and recompute the counters from the base tables, swapping the new
    /// table in last so an interrupted run leaves the old table intact.
    #[test]
    fn address_stats_migration_rebuilds_and_swaps_tables() {
        let stmts = migrate_address_stats_to_summing("db");
        assert_eq!(stmts.len(), 6, "drop-new + create + 2 backfills + drop-old + rename");
        // 1. clear any half-built table so a retry does not double-count.
        assert_eq!(stmts[0], "DROP TABLE IF EXISTS db.address_stats_new");
        // 2. new table, correct engine.
        assert!(stmts[1].contains("CREATE TABLE IF NOT EXISTS db.address_stats_new"));
        assert!(stmts[1].contains("SummingMergeTree"));
        // 3. fundings from `outputs`, matching the fund MV (OP_RETURN excluded).
        assert!(stmts[2].contains("INSERT INTO db.address_stats_new"));
        assert!(stmts[2].contains("FROM db.outputs"));
        assert!(stmts[2].contains("WHERE script_type != 7"));
        assert!(stmts[2].contains("GROUP BY address_hash"));
        // ...and spends from `spends`, matching the spend MV.
        assert!(stmts[3].contains("FROM db.spends"));
        assert!(stmts[3].contains("spent_script_type != 7"));
        assert!(stmts[3].contains("GROUP BY spent_address_hash"));
        // 4. swap: drop the old table (IF EXISTS, so a retry is tolerated), then
        //    rename (last, so a failure before it leaves the old table in place).
        assert_eq!(stmts[4], "DROP TABLE IF EXISTS db.address_stats");
        assert_eq!(stmts[5], "RENAME TABLE db.address_stats_new TO db.address_stats");
    }

    /// The migration is applied as one batch and the version is recorded only
    /// after it succeeds, so a failure at any statement is repaired by
    /// re-running `bidx migrate`. That requires the sequence to *begin* with a
    /// reset (drop the half-built `*_new` table) and the old-table drop to be
    /// `IF EXISTS`, so a retry after the swap already happened is a no-op.
    #[test]
    fn migration_is_rerunnable_from_a_leading_reset() {
        for (_, stmts) in migrations() {
            let first = stmts.first().expect("migration has statements");
            assert!(
                first.starts_with("DROP TABLE IF EXISTS") && first.contains("_new"),
                "migration must begin by clearing its scratch table: {first}"
            );
            assert!(
                stmts.iter().any(|s| s.starts_with("DROP TABLE IF EXISTS") && !s.contains("_new")),
                "the old-table drop must be IF EXISTS so a post-swap retry is a no-op"
            );
            // The rename is the commit point and must come last.
            assert!(
                stmts.last().expect("non-empty").starts_with("RENAME TABLE"),
                "the rename must be the final, atomic commit step"
            );
        }
    }

    /// The migration list is defined once with a `__DB__` placeholder and
    /// materialized per database, because the definition lives at module scope
    /// where the target database name is not known.
    #[test]
    fn migrations_are_materialized_per_database() {
        let raw = migrations();
        assert_eq!(raw.len(), 1, "exactly one migration so far");
        assert_eq!(raw[0].0, 2, "the address_stats rebuild is version 2");
        assert!(
            raw[0].1.iter().any(|s| s.contains("__DB__")),
            "raw migration should still carry the placeholder"
        );

        let mat = materialize_migrations("bitcoin");
        assert_eq!(mat.len(), 1);
        assert_eq!(mat[0].0, 2);
        assert!(
            mat[0].1.iter().all(|s| !s.contains("__DB__")),
            "placeholder left after materialization: {:?}",
            mat[0].1
        );
        assert!(mat[0].1.iter().any(|s| s.contains("bitcoin.address_stats_new")));
        assert!(mat[0].1.iter().any(|s| s == "RENAME TABLE bitcoin.address_stats_new TO bitcoin.address_stats"));
    }

    #[test]
    fn op_return_is_excluded_from_address_indexes() {
        // OP_RETURN outputs carry a zero address_hash, so no address-keyed
        // index may include them.
        for mv in [
            mv_address_history_fund("db"),
            mv_address_history_spend("db"),
            mv_utxos_fund("db"),
            mv_utxos_spend("db"),
            mv_address_stats_fund("db"),
            mv_address_stats_spend("db"),
        ] {
            assert!(mv.contains("!= 7"), "OP_RETURN not excluded: {mv}");
        }
    }

    #[test]
    fn backfill_coinstats_truncates_then_inserts() {
        let stmts = backfill_coinstats("db");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("TRUNCATE TABLE"));
        assert!(stmts[1].contains("INSERT INTO"));
        assert!(stmts[1].contains("GROUP BY height"));
    }

    #[test]
    fn table_names_match_ddl() {
        let body = all_ddl("db").join("\n");
        for name in all_table_names() {
            assert!(
                body.contains(&format!("db.{name} ")),
                "table {name} declared in all_table_names but not created by DDL"
            );
        }
    }

    /// The scripthash MV must reconstruct exactly the scriptPubKey bytes the
    /// serving layer builds, or Electrum lookups resolve to the wrong address.
    /// Assert every script-type arm and that OP_RETURN/unsupported types are
    /// excluded.
    #[test]
    fn scripthash_map_mv_reconstructs_each_script_type() {
        let ddl = mv_scripthash_map("db");
        // One arm per reconstructable type, in the same byte layout as
        // bidx_serve::address::script_pubkey.
        for (ty, prefix) in [
            ("script_type = 2", "76a914"), // P2PKH
            ("script_type = 3", "a914"),   // P2SH
            ("script_type = 4", "0014"),   // P2WPKH
            ("script_type = 5", "0020"),   // P2WSH
            ("script_type = 6", "5120"),   // P2TR
        ] {
            assert!(ddl.contains(ty), "missing arm for {ty}");
            assert!(ddl.contains(&format!("unhex('{prefix}')")), "missing prefix {prefix}");
        }
        // Only the reconstructable types are inserted.
        assert!(ddl.contains("WHERE script_type IN (2, 3, 4, 5, 6)"));
        // P2PKH/P2SH use the 20-byte program; segwit/taproot the 32-byte one.
        assert!(ddl.contains("substring(address_hash, 1, 20)"));
        assert!(ddl.contains("substring(address_hash, 1, 32)"));
        // SHA256() is the raw digest (internal order) the store queries with.
        assert!(ddl.contains("SHA256("));
    }

    /// The backfill must produce the same rows as the MV, so an operator can
    /// populate a pre-existing database after upgrading.
    #[test]
    fn scripthash_backfill_matches_the_mv_and_truncates_first() {
        let stmts = backfill_scripthash_map("db");
        assert_eq!(stmts.len(), 2, "expected truncate + insert");
        assert!(stmts[0].contains("TRUNCATE TABLE db.scripthash_map"));
        assert!(stmts[1].contains("INSERT INTO db.scripthash_map"));
        assert!(stmts[1].contains("SELECT DISTINCT"));
        assert!(stmts[1].contains("WHERE script_type IN (2, 3, 4, 5, 6)"));
        // Same reconstruction expression as the MV body.
        assert!(stmts[1].contains("unhex('76a914')"));
        assert!(stmts[1].contains("unhex('5120')"));
    }

    #[test]
    fn address_prefix_source_selects_only_renderable_types() {
        let sql = address_prefix_source("db");
        assert!(sql.contains("SELECT DISTINCT script_type, hex(address_hash) AS address_hash"));
        assert!(sql.contains("FROM db.outputs"));
        // P2PK(1), P2PKH(2), P2SH(3), P2WPKH(4), P2WSH(5), P2TR(6), multisig(8).
        assert!(sql.contains("script_type IN (1, 2, 3, 4, 5, 6, 8)"));
        // OP_RETURN (7), Unknown (0) and WitnessUnknown (9) are excluded.
        assert!(!sql.contains(", 7)"));
        assert!(!sql.contains("(0,"));
    }

    #[test]
    fn insert_address_prefix_binds_hash_and_escapes_address() {
        let rows = vec![
            ("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa".to_string(), 2u8, "ab".repeat(32)),
            ("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4".to_string(), 4, "cd".repeat(32)),
        ];
        let sql = insert_address_prefix("db", &rows);
        assert!(sql.starts_with("INSERT INTO db.address_prefix (address, script_type, address_hash) VALUES "));
        assert!(sql.contains("('1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', 2, unhex('"));
        assert!(sql.contains("('bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4', 4, unhex('"));
        // A hostile address cannot break out of the literal.
        let evil = vec![("x'); DROP TABLE db.outputs; --".to_string(), 2u8, "00".repeat(32))];
        let sql = insert_address_prefix("db", &evil);
        assert!(sql.contains(r"'x\'); DROP TABLE db.outputs; --'"), "got: {sql}");
    }

    /// Validation harness: writes the exact generated DDL to a temp file (one
    /// statement per line) and always asserts the write path, so this doubles
    /// as a coverage test for the dump. When `BIDX_DDL_DUMP` is set the file is
    /// also copied to that path so CI can pipe it into a real ClickHouse and
    /// prove every statement parses.
    #[test]
    fn dump_ddl_when_requested() {
        let stmts = all_ddl("bitcoin");
        let bf = backfill_coinstats("bitcoin");

        // Exercise the write path into a temp file unconditionally.
        let tmp = std::env::temp_dir().join(format!("bidx-ddl-{}.sql", std::process::id()));
        std::fs::write(&tmp, stmts.join(";\n") + ";\n").unwrap();
        let read_back = std::fs::read_to_string(&tmp).unwrap();
        assert!(read_back.contains("CREATE TABLE IF NOT EXISTS bitcoin.blocks"));
        assert!(read_back.ends_with(";\n"));

        // When asked, publish to the caller-specified path for CI validation.
        if let Ok(path) = std::env::var("BIDX_DDL_DUMP") {
            std::fs::write(&path, stmts.join(";\n") + ";\n").unwrap();
            std::fs::write(format!("{path}.backfill"), bf.join(";\n") + ";\n").unwrap();
        }

        let _ = std::fs::remove_file(&tmp);
    }
}
