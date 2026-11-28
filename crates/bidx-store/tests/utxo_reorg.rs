//! Reorg correctness: build synthetic blocks, apply them forward, then
//! disconnect and verify the UTXO set returns to its prior state.
//!
//! Convention: `tx_index` is the 0-based position within the block (the
//! pipeline indexes fees/spends by it). A separate `marker` byte seeds each
//! synthetic txid so outpoints are addressable across blocks.

use bidx_core::{BlockRow, InputRow, OutputRow, TxRow};
use bidx_store::{TipState, UtxoStore};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("bidx-utxo-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn txid_of(marker: u8) -> [u8; 32] {
    [marker; 32]
}

fn tx(height: u32, tx_index: u16, marker: u8) -> TxRow {
    TxRow {
        height,
        tx_index,
        txid: txid_of(marker),
        version: 1,
        locktime: 0,
        size: 100,
        weight: 400,
        fee: 0,
        input_count: 1,
        output_count: 1,
        has_witness: false,
    }
}

fn coinbase_out(height: u32, tx_index: u16, _marker: u8, value: u64) -> OutputRow {
    OutputRow {
        height,
        tx_index,
        output_index: 0,
        value_sat: value,
        script_pubkey_len: 25,
        script_type: 2,
        address_hash: [0xaa; 32],
    }
}

fn coinbase_in(height: u32, tx_index: u16, _marker: u8) -> InputRow {
    InputRow {
        height,
        tx_index,
        input_index: 0,
        prev_txid: [0u8; 32],
        prev_vout: 0xFFFF_FFFF,
        script_sig_len: 4,
        sequence: 0xFFFF_FFFF,
        witness_items: 0,
        witness_bytes: 0,
        is_coinbase: true,
        spent_value_sat: 0,
        spent_script_type: 0,
        spent_address_hash: [0u8; 32],
        spent_height: 0,
    }
}

/// A non-coinbase tx spending (prev_marker, prev_vout), creating one output.
fn spend(
    height: u32,
    tx_index: u16,
    marker: u8,
    prev_marker: u8,
    prev_vout: u32,
    out_value: u64,
) -> (TxRow, InputRow, OutputRow) {
    let t = tx(height, tx_index, marker);
    let inp = InputRow {
        height,
        tx_index,
        input_index: 0,
        prev_txid: txid_of(prev_marker),
        prev_vout,
        script_sig_len: 100,
        sequence: 0xFFFF_FFFF,
        witness_items: 0,
        witness_bytes: 0,
        is_coinbase: false,
        spent_value_sat: 0,
        spent_script_type: 0,
        spent_address_hash: [0u8; 32],
        spent_height: 0,
    };
    let out = coinbase_out(height, tx_index, marker, out_value);
    (t, inp, out)
}

#[test]
fn apply_then_disconnect_restores_utxo() {
    let dir = tmpdir("reorg");
    let utxo = UtxoStore::open_live(&dir).unwrap();

    // Block 1: coinbase (marker 0xA1) creates 50 BTC output.
    let mut blk1 = BlockRow::default();
    let mut txs1 = vec![tx(1, 0, 0xA1)];
    let mut ins1 = vec![coinbase_in(1, 0, 0xA1)];
    let outs1 = vec![coinbase_out(1, 0, 0xA1, 5_000_000_000)];
    let r1 = utxo
        .apply_block_in_place(1, &mut blk1, &mut txs1, &mut ins1, &outs1)
        .unwrap();
    assert_eq!(txs1[0].fee, 0);
    assert_eq!(r1.missing, 0);
    assert_eq!(ins1[0].spent_value_sat, 0);

    // Block 2: coinbase (marker 0xB1) + spend (marker 0xC1) of A paying 40.
    let mut blk2 = BlockRow::default();
    let (sp_tx, sp_in, sp_out) = spend(2, 1, 0xC1, 0xA1, 0, 4_000_000_000);
    let mut txs2 = vec![tx(2, 0, 0xB1), sp_tx];
    let mut ins2 = vec![coinbase_in(2, 0, 0xB1), sp_in];
    let outs2 = vec![coinbase_out(2, 0, 0xB1, 5_000_000_000), sp_out];
    let r2 = utxo
        .apply_block_in_place(2, &mut blk2, &mut txs2, &mut ins2, &outs2)
        .unwrap();
    assert_eq!(r2.missing, 0);
    assert_eq!(txs2[0].fee, 0, "coinbase has no fee");
    assert_eq!(txs2[1].fee, 1_000_000_000, "fee = 50 in - 40 out");
    assert_eq!(ins2[1].spent_value_sat, 5_000_000_000);

    utxo.set_tip(&TipState {
        height: 2,
        hash: bidx_core::Hash32::from_bytes([2u8; 32]),
    })
    .unwrap();
    assert_eq!(utxo.tip().unwrap().unwrap().height, 2);

    // Disconnect block 2: A (50 BTC) must be restored.
    utxo.disconnect_block(2).unwrap();

    // Spend A again in a competing block 2' — must succeed (A is back).
    let mut blk2b = BlockRow::default();
    let (sp_tx2, sp_in2, sp_out2) = spend(2, 1, 0xC9, 0xA1, 0, 4_500_000_000);
    let mut txs2b = vec![tx(2, 0, 0xB9), sp_tx2];
    let mut ins2b = vec![coinbase_in(2, 0, 0xB9), sp_in2];
    let outs2b = vec![coinbase_out(2, 0, 0xB9, 5_000_000_000), sp_out2];
    let r2b = utxo
        .apply_block_in_place(2, &mut blk2b, &mut txs2b, &mut ins2b, &outs2b)
        .unwrap();
    assert_eq!(r2b.missing, 0, "A must be spendable again after disconnect");
    assert_eq!(ins2b[1].spent_value_sat, 5_000_000_000);
    assert_eq!(txs2b[1].fee, 500_000_000, "fee = 50 in - 45 out");

    // Disconnect the competing block, then block 1 (full unwind to genesis).
    utxo.disconnect_block(2).unwrap();
    utxo.disconnect_block(1).unwrap();

    // After full unwind, spending A must miss.
    let mut blk3 = BlockRow::default();
    let (sp_tx3, sp_in3, sp_out3) = spend(1, 1, 0xD1, 0xA1, 0, 1_000_000_000);
    let mut txs3 = vec![tx(1, 0, 0xD0), sp_tx3];
    let mut ins3 = vec![coinbase_in(1, 0, 0xD0), sp_in3];
    let r3 = utxo
        .apply_block_in_place(
            1,
            &mut blk3,
            &mut txs3,
            &mut ins3,
            &[coinbase_out(1, 0, 0xD0, 5_000_000_000), sp_out3],
        )
        .unwrap();
    assert_eq!(r3.missing, 1, "A gone after full unwind");
    assert_eq!(ins3[1].spent_value_sat, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn same_block_spend_chain_disconnect() {
    let dir = tmpdir("sameblock");
    let utxo = UtxoStore::open_live(&dir).unwrap();

    // Block 10: coinbase (marker 0x0A) creates output; tx 0x0B spends it in
    // the SAME block; tx 0x0C spends 0x0B's output, also same block.
    let mut blk10 = BlockRow::default();
    let cb_out = coinbase_out(10, 0, 0x0A, 5_000_000_000);
    let (t11, i11, o11) = spend(10, 1, 0x0B, 0x0A, 0, 4_000_000_000);
    let (t12, i12, o12) = spend(10, 2, 0x0C, 0x0B, 0, 3_000_000_000);

    let mut txs = vec![tx(10, 0, 0x0A), t11, t12];
    let mut ins = vec![coinbase_in(10, 0, 0x0A), i11, i12];
    let outs = vec![cb_out, o11, o12];
    let r = utxo
        .apply_block_in_place(10, &mut blk10, &mut txs, &mut ins, &outs)
        .unwrap();
    assert_eq!(r.pending_hits, 2);
    assert_eq!(ins[1].spent_value_sat, 5_000_000_000);
    assert_eq!(ins[2].spent_value_sat, 4_000_000_000);

    // Disconnect: everything from block 10 must vanish. Spending 0x0C's
    // output afterward must miss.
    utxo.disconnect_block(10).unwrap();
    let mut blk11 = BlockRow::default();
    let (t99, i99, o99) = spend(11, 0, 0x63, 0x0C, 0, 1_000_000_000);
    let mut txs11 = vec![t99];
    let mut ins11 = vec![i99];
    let r2 = utxo
        .apply_block_in_place(11, &mut blk11, &mut txs11, &mut ins11, &[o99])
        .unwrap();
    assert_eq!(r2.missing, 1, "0x0C output must be gone after disconnect");

    let _ = std::fs::remove_dir_all(&dir);
}
