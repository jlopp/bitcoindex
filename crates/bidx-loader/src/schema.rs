//! ClickHouse DDL for the five core tables plus the derived
//! query-optimized variants (by_txid, by_address).
//!
//! Notes on the design:
//! * Hashes are FixedString(32) — raw wire-order bytes. Use
//!   `reverse(hex(hash))`-style helpers in views if display hex is needed.
//! * MergeTree ordering matches the dominant scan pattern for each table.
//! * PARTITION BY 10k-block ranges keeps parts manageable and enables
//!   cheap partition drops for reorg handling at the tip.

pub const ENTITIES: [&str; 5] = ["blocks", "transactions", "inputs", "outputs", "spends"];

pub fn all_ddl(db: &str) -> Vec<String> {
    vec![
        blocks(db),
        transactions(db),
        transactions_by_txid(db),
        inputs(db),
        inputs_by_prev_output(db),
        outputs(db),
        outputs_by_address(db),
        spends(db),
    ]
}

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

fn transactions_by_txid(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.transactions_by_txid \
         (txid FixedString(32), height UInt32, tx_index UInt32) \
         ENGINE = MergeTree \
         ORDER BY txid"
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

fn inputs_by_prev_output(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.inputs_by_prev_output \
         (prev_txid FixedString(32), prev_vout UInt32, height UInt32, \
          tx_index UInt32, input_index UInt32, txid FixedString(32)) \
         ENGINE = MergeTree \
         ORDER BY (prev_txid, prev_vout)"
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

fn outputs_by_address(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.outputs_by_address \
         (address_hash FixedString(32), script_type UInt8, height UInt32, \
          tx_index UInt32, output_index UInt32, txid FixedString(32), value_sat UInt64) \
         ENGINE = MergeTree \
         ORDER BY (address_hash, height, tx_index, output_index)"
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
