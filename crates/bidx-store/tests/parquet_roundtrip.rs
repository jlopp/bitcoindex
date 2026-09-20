//! Roundtrip: build rows for the genesis block, write them through the
//! Parquet sink, and read them back, asserting field-level fidelity.

use bidx_core::{BlockRow, Hash32, InputRow, OutputRow, ScriptType, SpendRow, TxRow};
use bidx_store::{ParquetSink, SinkConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::PathBuf;

fn outdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("bidx-parquet-test-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn parquet_roundtrip_genesis() {
    let dir = outdir("genesis");
    let mut sink = ParquetSink::create(&dir, 0, SinkConfig { batch_size: 1000 }).unwrap();

    let block_hash = Hash32::from_hex(
        "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
    )
    .unwrap();
    let txid = Hash32::from_hex(
        "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    )
    .unwrap();

    sink.push_block(&BlockRow {
        height: 0,
        hash: *block_hash.as_bytes(),
        prev_hash: [0u8; 32],
        merkle_root: [0u8; 32],
        time: 1231006505,
        bits: 0x1d00ffff,
        nonce: 2083236893,
        version: 1,
        tx_count: 1,
        size: 285,
        weight: 1136,
        total_fee: 0,
        coinbase_value: 5_000_000_000,
    })
    .unwrap();

    sink.push_tx(&TxRow {
        height: 0,
        tx_index: 0,
        txid: *txid.as_bytes(),
        version: 1,
        locktime: 0,
        size: 204,
        weight: 816,
        fee: 0,
        input_count: 1,
        output_count: 1,
        has_witness: false,
    })
    .unwrap();

    sink.push_input(&InputRow {
        height: 0,
        tx_index: 0,
        input_index: 0,
        txid: *txid.as_bytes(),
        prev_txid: [0u8; 32],
        prev_vout: 0xFFFF_FFFF,
        script_sig_len: 77,
        sequence: 0xFFFF_FFFF,
        witness_items: 0,
        witness_bytes: 0,
        is_coinbase: true,
    })
    .unwrap();

    let mut addr = [0u8; 32];
    addr[..20].copy_from_slice(&hex::decode("62e907b15cbf27d5425399ebf6f0fb50ebb88f18").unwrap());
    sink.push_output(&OutputRow {
        height: 0,
        tx_index: 0,
        output_index: 0,
        txid: *txid.as_bytes(),
        value_sat: 5_000_000_000,
        script_pubkey_len: 67,
        script_type: ScriptType::P2PK as u8,
        address_hash: addr,
    })
    .unwrap();

    // No spends in the genesis block, but write one synthetic row to prove
    // the spends file is well-formed.
    sink.push_spend(&SpendRow {
        height: 1,
        spending_tx_index: 0,
        spending_input_index: 0,
        spending_txid: [0x11; 32],
        spent_txid: *txid.as_bytes(),
        spent_vout: 0,
        spent_value_sat: 5_000_000_000,
        spent_script_type: ScriptType::P2PK as u8,
        spent_address_hash: addr,
        spent_height: 0,
    })
    .unwrap();

    sink.finish().unwrap();

    // Read back blocks and verify.
    let blocks_path = dir.join("blocks").join("part-00000.parquet");
    let file = std::fs::File::open(&blocks_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    use arrow::array::{FixedSizeBinaryArray, UInt32Array, UInt64Array};
    let heights = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(heights.value(0), 0);
    let hashes = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(hashes.value(0), block_hash.as_bytes());
    let coinbase = batch
        .column(12)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(coinbase.value(0), 5_000_000_000);

    // Verify outputs file round-trips address hash + value.
    let outputs_path = dir.join("outputs").join("part-00000.parquet");
    let file = std::fs::File::open(&outputs_path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    let values = batch
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(values.value(0), 5_000_000_000);
    let addrs = batch
        .column(7)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(addrs.value(0), &addr);

    let _ = std::fs::remove_dir_all(&dir);
}
