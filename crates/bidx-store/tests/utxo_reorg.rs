//! Reorg correctness: build synthetic blocks, apply them forward, then
//! disconnect and verify the UTXO set returns to its prior state.
//!
//! Convention: `tx_index` is the 0-based position within the block (the
//! pipeline indexes fees/spends by it). A separate `marker` byte seeds each
//! synthetic txid so outpoints are addressable across blocks.

use bidx_core::{InputRow, OutputRow, TxRow};
use bidx_store::{TipState, UtxoStore};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("bidx-utxo-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn txid_of(marker: u8) -> [u8; 32] {
    [marker; 32]
}

fn tx(height: u32, tx_index: u32, marker: u8) -> TxRow {
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

fn coinbase_out(height: u32, tx_index: u32, marker: u8, value: u64) -> OutputRow {
    OutputRow {
        height,
        tx_index,
        output_index: 0,
        txid: txid_of(marker),
        value_sat: value,
        script_pubkey_len: 25,
        script_type: 2,
        address_hash: [0xaa; 32],
    }
}

fn coinbase_in(height: u32, tx_index: u32, marker: u8) -> InputRow {
    InputRow {
        height,
        tx_index,
        input_index: 0,
        txid: txid_of(marker),
        prev_txid: [0u8; 32],
        prev_vout: 0xFFFF_FFFF,
        script_sig_len: 4,
        sequence: 0xFFFF_FFFF,
        witness_items: 0,
        witness_bytes: 0,
        is_coinbase: true,
    }
}

/// A non-coinbase tx spending (prev_marker, prev_vout), creating one output.
fn spend(
    height: u32,
    tx_index: u32,
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
        txid: txid_of(marker),
        prev_txid: txid_of(prev_marker),
        prev_vout,
        script_sig_len: 100,
        sequence: 0xFFFF_FFFF,
        witness_items: 0,
        witness_bytes: 0,
        is_coinbase: false,
    };
    let out = coinbase_out(height, tx_index, marker, out_value);
    (t, inp, out)
}

#[test]
fn apply_then_disconnect_restores_utxo() {
    let dir = tmpdir("reorg");
    let utxo = UtxoStore::open_live(&dir).unwrap();

    // Block 1: coinbase (marker 0xA1) creates 50 BTC output.
    let txs1 = vec![tx(1, 0, 0xA1)];
    let ins1 = vec![coinbase_in(1, 0, 0xA1)];
    let outs1 = vec![coinbase_out(1, 0, 0xA1, 5_000_000_000)];
    let r1 = utxo.apply_block(1, &txs1, &ins1, &outs1).unwrap();
    assert_eq!(r1.fees, vec![0]);
    assert_eq!(r1.spends.len(), 0);

    // Block 2: coinbase (marker 0xB1) + spend (marker 0xC1) of A paying 40.
    let (sp_tx, sp_in, sp_out) = spend(2, 1, 0xC1, 0xA1, 0, 4_000_000_000);
    let txs2 = vec![tx(2, 0, 0xB1), sp_tx];
    let ins2 = vec![coinbase_in(2, 0, 0xB1), sp_in];
    let outs2 = vec![coinbase_out(2, 0, 0xB1, 5_000_000_000), sp_out];
    let r2 = utxo.apply_block(2, &txs2, &ins2, &outs2).unwrap();
    assert_eq!(r2.fees[0], 0, "coinbase has no fee");
    assert_eq!(r2.fees[1], 1_000_000_000, "fee = 50 in - 40 out");
    assert_eq!(r2.spends.len(), 1);
    assert_eq!(r2.spends[0].spent_value_sat, 5_000_000_000);

    utxo.set_tip(&TipState {
        height: 2,
        hash: bidx_core::Hash32::from_bytes([2u8; 32]),
    })
    .unwrap();
    assert_eq!(utxo.tip().unwrap().unwrap().height, 2);

    // Disconnect block 2: A (50 BTC) must be restored.
    utxo.disconnect_block(2).unwrap();

    // Spend A again in a competing block 2' — must succeed (A is back).
    let (sp_tx2, sp_in2, sp_out2) = spend(2, 1, 0xC9, 0xA1, 0, 4_500_000_000);
    let txs2b = vec![tx(2, 0, 0xB9), sp_tx2];
    let ins2b = vec![coinbase_in(2, 0, 0xB9), sp_in2];
    let outs2b = vec![coinbase_out(2, 0, 0xB9, 5_000_000_000), sp_out2];
    let r2b = utxo.apply_block(2, &txs2b, &ins2b, &outs2b).unwrap();
    assert_eq!(r2b.spends.len(), 1, "A must be spendable again after disconnect");
    assert_eq!(r2b.fees[1], 500_000_000, "fee = 50 in - 45 out");

    // Disconnect the competing block, then block 1 (full unwind to genesis).
    utxo.disconnect_block(2).unwrap();
    utxo.disconnect_block(1).unwrap();

    // After full unwind, spending A must miss.
    let (sp_tx3, sp_in3, sp_out3) = spend(1, 1, 0xD1, 0xA1, 0, 1_000_000_000);
    let r3 = utxo
        .apply_block(
            1,
            &[tx(1, 0, 0xD0), sp_tx3],
            &[coinbase_in(1, 0, 0xD0), sp_in3],
            &[coinbase_out(1, 0, 0xD0, 5_000_000_000), sp_out3],
        )
        .unwrap();
    assert_eq!(r3.missing, 1, "A gone after full unwind");
    assert_eq!(r3.spends.len(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn same_block_spend_chain_disconnect() {
    let dir = tmpdir("sameblock");
    let utxo = UtxoStore::open_live(&dir).unwrap();

    // Block 10: coinbase (marker 0x0A) creates output; tx 0x0B spends it in
    // the SAME block; tx 0x0C spends 0x0B's output, also same block.
    let cb_out = coinbase_out(10, 0, 0x0A, 5_000_000_000);
    let (t11, i11, o11) = spend(10, 1, 0x0B, 0x0A, 0, 4_000_000_000);
    let (t12, i12, o12) = spend(10, 2, 0x0C, 0x0B, 0, 3_000_000_000);

    let txs = vec![tx(10, 0, 0x0A), t11, t12];
    let ins = vec![coinbase_in(10, 0, 0x0A), i11, i12];
    let outs = vec![cb_out, o11, o12];
    let r = utxo.apply_block(10, &txs, &ins, &outs).unwrap();
    assert_eq!(r.spends.len(), 2);
    // Net UTXO addition from this block: only 0x0C's output (3 BTC), since
    // the coinbase and 0x0B's output were both spent within the block.

    // Disconnect: everything from block 10 must vanish. Spending 0x0C's
    // output afterward must miss.
    utxo.disconnect_block(10).unwrap();
    let (t99, i99, o99) = spend(11, 0, 0x63, 0x0C, 0, 1_000_000_000);
    let r2 = utxo.apply_block(11, &[t99], &[i99], &[o99]).unwrap();
    assert_eq!(r2.missing, 1, "0x0C output must be gone after disconnect");

    let _ = std::fs::remove_dir_all(&dir);
}
