//! ClickHouse `RowBinary` encoders for the base entity rows.
//!
//! Live mode can insert each applied block straight into ClickHouse instead of
//! appending to Parquet and bulk-loading later (see [`crate::live`]). The
//! binary format is used rather than `TabSeparated` because the witness payload
//! carries arbitrary bytes, which a text format would have to escape.
//!
//! `RowBinary` is little-endian and has no alignment or framing:
//! * integers: little-endian, fixed width (`UInt8` = 1 byte, `Int32` = 4, ...);
//! * `FixedString(N)`: exactly N raw bytes, no length prefix;
//! * `String`: a LEB128 varint length followed by that many raw bytes.
//!
//! Hashes are the raw wire-order 32 bytes the Parquet files store, which is
//! exactly what the ClickHouse `FixedString(32)` columns hold.

use bidx_core::{BlockRow, InputRow, OutputRow, SpendRow, TxRow, WitnessRow};

/// Column list for `blocks`, in schema order. `fee_rate` is omitted: it is a
/// materialized column ClickHouse computes itself.
pub const BLOCKS_COLS: &str = "height, hash, prev_hash, merkle_root, time, bits, nonce, \
     version, tx_count, size, weight, total_fee, coinbase_value";

/// Column list for `transactions` (again excluding the materialized `fee_rate`).
pub const TXS_COLS: &str = "height, tx_index, txid, version, locktime, size, weight, fee, \
     input_count, output_count, has_witness";

/// Column list for `inputs`.
pub const INPUTS_COLS: &str = "height, tx_index, input_index, txid, prev_txid, prev_vout, \
     script_sig_len, sequence, witness_items, witness_bytes, is_coinbase";

/// Column list for `outputs`.
pub const OUTPUTS_COLS: &str = "height, tx_index, output_index, txid, value_sat, \
     script_pubkey_len, script_type, address_hash";

/// Column list for `spends`.
pub const SPENDS_COLS: &str = "height, spending_tx_index, spending_input_index, spending_txid, \
     spent_txid, spent_vout, spent_value_sat, spent_script_type, spent_address_hash, spent_height";

/// Column list for the optional `witnesses` payload table.
pub const WITNESSES_COLS: &str = "height, tx_index, input_index, item_index, item_len, item_bytes";

#[inline]
fn u8_(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

#[inline]
fn u32_(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[inline]
fn u64_(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[inline]
fn i32_(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[inline]
fn bin32(out: &mut Vec<u8>, v: &[u8; 32]) {
    out.extend_from_slice(v);
}

/// `String`: LEB128 length then raw bytes.
#[inline]
fn str_(out: &mut Vec<u8>, v: &[u8]) {
    leb128(out, v.len() as u64);
    out.extend_from_slice(v);
}

/// Unsigned LEB128 varint, ClickHouse's `String` length prefix.
fn leb128(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Encode one `blocks` row.
pub fn block(r: &BlockRow, out: &mut Vec<u8>) {
    u32_(out, r.height);
    bin32(out, &r.hash);
    bin32(out, &r.prev_hash);
    bin32(out, &r.merkle_root);
    u32_(out, r.time);
    u32_(out, r.bits);
    u32_(out, r.nonce);
    i32_(out, r.version);
    u32_(out, r.tx_count);
    u32_(out, r.size);
    u32_(out, r.weight);
    u64_(out, r.total_fee);
    u64_(out, r.coinbase_value);
}

/// Encode one `transactions` row. `bool` maps to `UInt8` (0/1).
pub fn tx(r: &TxRow, out: &mut Vec<u8>) {
    u32_(out, r.height);
    u32_(out, r.tx_index);
    bin32(out, &r.txid);
    i32_(out, r.version);
    u32_(out, r.locktime);
    u32_(out, r.size);
    u32_(out, r.weight);
    u64_(out, r.fee);
    u32_(out, r.input_count);
    u32_(out, r.output_count);
    u8_(out, r.has_witness as u8);
}

/// Encode one `inputs` row. `bool` maps to `UInt8` (0/1).
pub fn input(r: &InputRow, out: &mut Vec<u8>) {
    u32_(out, r.height);
    u32_(out, r.tx_index);
    u32_(out, r.input_index);
    bin32(out, &r.txid);
    bin32(out, &r.prev_txid);
    u32_(out, r.prev_vout);
    u32_(out, r.script_sig_len);
    u32_(out, r.sequence);
    u32_(out, r.witness_items);
    u32_(out, r.witness_bytes);
    u8_(out, r.is_coinbase as u8);
}

/// Encode one `outputs` row.
pub fn output(r: &OutputRow, out: &mut Vec<u8>) {
    u32_(out, r.height);
    u32_(out, r.tx_index);
    u32_(out, r.output_index);
    bin32(out, &r.txid);
    u64_(out, r.value_sat);
    u32_(out, r.script_pubkey_len);
    u8_(out, r.script_type);
    bin32(out, &r.address_hash);
}

/// Encode one `spends` row.
pub fn spend(r: &SpendRow, out: &mut Vec<u8>) {
    u32_(out, r.height);
    u32_(out, r.spending_tx_index);
    u32_(out, r.spending_input_index);
    bin32(out, &r.spending_txid);
    bin32(out, &r.spent_txid);
    u32_(out, r.spent_vout);
    u64_(out, r.spent_value_sat);
    u8_(out, r.spent_script_type);
    bin32(out, &r.spent_address_hash);
    u32_(out, r.spent_height);
}

/// Encode one `witnesses` row (`item_bytes` is a `String`).
pub fn witness(r: &WitnessRow, out: &mut Vec<u8>) {
    u32_(out, r.height);
    u32_(out, r.tx_index);
    u32_(out, r.input_index);
    u32_(out, r.item_index);
    u32_(out, r.item_len);
    str_(out, &r.item_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_block() -> BlockRow {
        BlockRow {
            height: 0x0102_0304,
            hash: [0xAA; 32],
            prev_hash: [0xBB; 32],
            merkle_root: [0xCC; 32],
            time: 1,
            bits: 2,
            nonce: 3,
            version: -7,
            tx_count: 4,
            size: 5,
            weight: 6,
            total_fee: 7,
            coinbase_value: 8,
        }
    }

    fn sample_tx() -> TxRow {
        TxRow {
            height: 1,
            tx_index: 2,
            txid: [0x11; 32],
            version: -1,
            locktime: 3,
            size: 4,
            weight: 5,
            fee: 6,
            input_count: 7,
            output_count: 8,
            has_witness: true,
        }
    }

    fn sample_input() -> InputRow {
        InputRow {
            height: 1,
            tx_index: 2,
            input_index: 3,
            txid: [0x22; 32],
            prev_txid: [0x33; 32],
            prev_vout: 4,
            script_sig_len: 5,
            sequence: 6,
            witness_items: 7,
            witness_bytes: 8,
            is_coinbase: true,
        }
    }

    fn sample_output() -> OutputRow {
        OutputRow {
            height: 1,
            tx_index: 2,
            output_index: 3,
            txid: [0x44; 32],
            value_sat: 4,
            script_pubkey_len: 5,
            script_type: 6,
            address_hash: [0x55; 32],
        }
    }

    fn sample_spend() -> SpendRow {
        SpendRow {
            height: 1,
            spending_tx_index: 2,
            spending_input_index: 3,
            spending_txid: [0x66; 32],
            spent_txid: [0x77; 32],
            spent_vout: 4,
            spent_value_sat: 5,
            spent_script_type: 6,
            spent_address_hash: [0x88; 32],
            spent_height: 7,
        }
    }

    /// The full `blocks` row must be exactly the schema width: 4 (height) +
    /// 3×32 (hashes) + 7×4 (time, bits, nonce, version, tx_count, size, weight)
    /// + 2×8 (total_fee, coinbase_value) = 144 bytes, height little-endian first.
    #[test]
    fn block_row_layout_is_little_endian_and_fixed_width() {
        let mut out = Vec::new();
        block(&sample_block(), &mut out);
        assert_eq!(out.len(), 4 + 32 * 3 + 4 * 7 + 8 * 2);
        assert_eq!(&out[0..4], &0x0102_0304u32.to_le_bytes());
        assert_eq!(&out[4..36], &[0xAA; 32]);
        // version is the first Int32 after the three hashes + time/bits/nonce.
        let ver_at = 4 + 32 * 3 + 4 * 3;
        assert_eq!(&out[ver_at..ver_at + 4], &(-7i32).to_le_bytes());
    }

    #[test]
    fn tx_row_encodes_bool_as_one_byte() {
        let mut out = Vec::new();
        tx(&sample_tx(), &mut out);
        assert_eq!(out.len(), 4 + 4 + 32 + 4 + 4 + 4 + 4 + 8 + 4 + 4 + 1);
        assert_eq!(*out.last().unwrap(), 1, "has_witness=true -> 1");
        let mut t = sample_tx();
        t.has_witness = false;
        let mut out2 = Vec::new();
        tx(&t, &mut out2);
        assert_eq!(*out2.last().unwrap(), 0);
    }

    #[test]
    fn input_row_encodes_coinbase_flag() {
        let mut out = Vec::new();
        input(&sample_input(), &mut out);
        assert_eq!(out.len(), 4 + 4 + 4 + 32 + 32 + 4 + 4 + 4 + 4 + 4 + 1);
        assert_eq!(*out.last().unwrap(), 1);
    }

    #[test]
    fn output_and_spend_rows_match_schema_widths() {
        let mut o = Vec::new();
        output(&sample_output(), &mut o);
        assert_eq!(o.len(), 4 + 4 + 4 + 32 + 8 + 4 + 1 + 32);
        assert_eq!(&o[44..52], &4u64.to_le_bytes(), "value_sat follows txid");
        let mut s = Vec::new();
        spend(&sample_spend(), &mut s);
        assert_eq!(s.len(), 4 + 4 + 4 + 32 + 32 + 4 + 8 + 1 + 32 + 4);
    }

    #[test]
    fn witness_string_length_is_leb128() {
        // A 200-byte payload needs a two-byte LEB128 length (200 = 0xC8 0x01).
        let row = WitnessRow {
            height: 1,
            tx_index: 2,
            input_index: 3,
            item_index: 4,
            item_len: 200,
            item_bytes: vec![0x7F; 200],
        };
        let mut out = Vec::new();
        witness(&row, &mut out);
        assert_eq!(out.len(), 4 * 5 + 2 + 200);
        assert_eq!(&out[20..22], &[0xC8, 0x01]);
        assert_eq!(&out[22..], &[0x7F; 200]);
    }

    #[test]
    fn leb128_roundtrips_across_size_classes() {
        for (v, want) in [
            (0u64, vec![0x00]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (16_383, vec![0xff, 0x7f]),
            (16_384, vec![0x80, 0x80, 0x01]),
        ] {
            let mut out = Vec::new();
            leb128(&mut out, v);
            assert_eq!(out, want, "leb128({v})");
        }
    }

    #[test]
    fn empty_witness_payload_encodes_zero_length() {
        let row = WitnessRow {
            height: 0,
            tx_index: 0,
            input_index: 0,
            item_index: 0,
            item_len: 0,
            item_bytes: vec![],
        };
        let mut out = Vec::new();
        witness(&row, &mut out);
        assert_eq!(out.len(), 21, "20 bytes of u32 + a single 0x00 length");
        assert_eq!(out[20], 0x00);
    }

    #[test]
    fn column_lists_omit_the_materialized_fee_rate() {
        assert!(!BLOCKS_COLS.contains("fee_rate"));
        assert!(!TXS_COLS.contains("fee_rate"));
        // ...but every real column is named.
        for col in ["height", "hash", "coinbase_value"] {
            assert!(BLOCKS_COLS.contains(col));
        }
        for col in ["has_witness", "output_count"] {
            assert!(TXS_COLS.contains(col));
        }
        for col in ["is_coinbase", "witness_bytes"] {
            assert!(INPUTS_COLS.contains(col));
        }
        assert!(OUTPUTS_COLS.contains("address_hash"));
        assert!(SPENDS_COLS.contains("spent_height"));
        assert!(WITNESSES_COLS.contains("item_bytes"));
    }
}
