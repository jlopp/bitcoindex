//! Integration test against the real genesis block, embedded as hex.
//! The genesis block is 285 bytes and is the most-tested byte string in
//! Bitcoin, so it's a perfect parser fixture.

use bidx_core::{dsha256, BlockHeader, Hash32};
use bidx_parser::parse_block_full;

const GENESIS_HEX: &str = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c01\
01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff\
0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";

fn genesis_bytes() -> Vec<u8> {
    hex::decode(GENESIS_HEX).unwrap()
}

#[test]
fn parses_genesis_block() {
    let payload = genesis_bytes();
    assert_eq!(payload.len(), 285);

    let mut header_raw = [0u8; 80];
    header_raw.copy_from_slice(&payload[..80]);
    let header = BlockHeader::parse(&header_raw);
    let hash = dsha256(&header_raw);

    // Genesis block hash (display form).
    assert_eq!(
        hash.to_hex(),
        "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    );
    assert_eq!(header.prev_hash, Hash32::ZERO);
    assert_eq!(header.time, 1231006505);
    assert_eq!(header.bits, 0x1d00ffff);

    let block = parse_block_full(&payload, 0, hash, &header).expect("parse");

    // One coinbase transaction.
    assert_eq!(block.txs.len(), 1);
    assert_eq!(block.inputs.len(), 1);
    assert_eq!(block.outputs.len(), 1);

    let tx = &block.txs[0];
    assert_eq!(
        Hash32::from_bytes(tx.txid).to_hex(),
        "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
    );
    assert!(!tx.has_witness);
    assert_eq!(tx.input_count, 1);
    assert_eq!(tx.output_count, 1);

    let inp = &block.inputs[0];
    assert!(inp.is_coinbase);
    assert_eq!(inp.prev_txid, [0u8; 32]);
    assert_eq!(inp.prev_vout, 0xFFFF_FFFF);

    let out = &block.outputs[0];
    assert_eq!(out.value_sat, 50 * 100_000_000);
    // Genesis output is P2PK (65-byte uncompressed pubkey).
    assert_eq!(out.script_type, bidx_core::ScriptType::P2PK as u8);

    // Block-level aggregates.
    assert_eq!(block.block_row.coinbase_value, 50 * 100_000_000);
    assert_eq!(block.block_row.tx_count, 1);
    assert_eq!(block.block_row.size as usize, 285);
}
