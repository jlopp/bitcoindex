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
//!   whole catalogue in sync with no extra write path. Two indexes
//!   (`coinstats_by_height`, `address_prefix`) are backfills — see
//!   `bidx index rebuild`.
//! * Reorder-only indexes are ClickHouse *projections* on the base table
//!   (no duplicated column set); ClickHouse selects them automatically.
//!
//! ## Index catalogue (18)
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
//! | 15 | `address_stats` | Aggregating + MV | address chain stats |
//! | 16 | `coinstats_by_height` | table + backfill | UTXO-set snapshots |
//! | 17 | `blocks_by_miner` | table + MV | blocks by miner |
//! | 18 | `address_prefix` | table + backfill | address prefix search |
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

/// The 18 indexes the serving layer maintains, in catalogue order.
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
        IndexSpec { name: "address_stats", mechanism: "aggregating+mv", target: "outputs+spends" },
        IndexSpec { name: "coinstats_by_height", mechanism: "backfill", target: "outputs+spends" },
        IndexSpec { name: "blocks_by_miner", mechanism: "table+mv", target: "outputs" },
        IndexSpec { name: "address_prefix", mechanism: "backfill", target: "outputs" },
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
    ]
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
fn address_stats(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.address_stats \
         (address_hash FixedString(32), funded_count UInt64, funded_sum UInt64, \
          spent_count UInt64, spent_sum UInt64, history_count UInt64) \
         ENGINE = AggregatingMergeTree \
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
/// Backfilled by `bidx index rebuild address-prefix`.
fn address_prefix(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.address_prefix \
         (address String, script_type UInt8, address_hash FixedString(32)) \
         ENGINE = MergeTree \
         ORDER BY address"
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
    fn catalogue_has_eighteen_indexes_each_referenced_by_ddl() {
        let cat = index_catalog();
        assert_eq!(cat.len(), 18, "catalogue must list 18 indexes");
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
        ] {
            assert!(body.contains(mv), "missing MV {mv}");
            let table_at = body.find(&format!("db.{table} ")).or_else(|| body.find(&format!("db.{table}\n")));
            let mv_at = body.find(mv).expect("mv present");
            let table_at = table_at.expect("table present");
            assert!(table_at < mv_at, "MV {mv} must be defined after table {table}");
        }
    }

    #[test]
    fn projections_and_skip_indexes_are_present() {
        let body = all_ddl("db").join("\n");
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
    fn address_stats_uses_aggregating_engine() {
        assert!(address_stats("db").contains("AggregatingMergeTree"));
        // Both MVs must carry all five counter columns with matching names.
        for mv in [mv_address_stats_fund("db"), mv_address_stats_spend("db")] {
            for col in ["funded_count", "funded_sum", "spent_count", "spent_sum", "history_count"] {
                assert!(mv.contains(col), "MV missing counter {col}: {mv}");
            }
        }
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
