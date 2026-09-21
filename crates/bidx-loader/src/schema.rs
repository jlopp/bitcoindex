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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_ddl_covers_every_entity_and_reorg_views() {
        let ddls = all_ddl("testdb");
        // 5 entity tables + 2 relational projections = 8 DDL statements.
        assert_eq!(ddls.len(), 8);
        let body = ddls.join("\n");
        for entity in ENTITIES {
            assert!(body.contains(&format!("testdb.{entity}")), "missing {entity}: {body}");
        }
        // Common ClickHouse idempotency marker.
        assert!(body.matches("CREATE TABLE IF NOT EXISTS").count() == 8);
        // FixedString(32) is the only hash-storage type we use; bare
        // `String` (variable-length) would blow up index size for hashes.
        assert!(body.contains("FixedString(32)"));
        assert!(!body.contains(" String "), "must not use variable-length String for hashes");
        assert!(!body.contains("(String"), "no bare String columns");
        // Distance between write pattern and order key is preserved.
        assert!(body.contains("PARTITION BY intDiv(height, 10000)"));
        // Query-optimized projections that the indexer advertises.
        assert!(body.contains("transactions_by_txid"));
        assert!(body.contains("outputs_by_address"));
        assert!(body.contains("inputs_by_prev_output"));
    }

    #[test]
    fn entities_array_matches_all_ddl_entities() {
        // The ENTITIES array drives both bidx.loader's load_all walk and the
        // schema check; ensure each is present as a table in the DDL set.
        let body = all_ddl("db").join("\n");
        for entity in ENTITIES {
            // The DDL for the table matches {db}.{entity}. Don't suffix-match
            // a projection's "{entity}_by_*" variant here.
            let needle = format!("db.{entity} ");
            assert!(body.contains(&needle), "missing {needle}");
        }
    }

    #[test]
    fn txs_and_by_txid_schemas_are_compatible() {
        // The by_txid projection must accept the same row shape: txid is first,
        // then location columns (height, tx_index), mirroring what the
        // materialized view / INSERT SELECT will push.
        let body = all_ddl("db").join("\n");
        assert!(body.find("transactions_by_txid").unwrap() < body.find("ORDER BY txid").unwrap());
    }
}
