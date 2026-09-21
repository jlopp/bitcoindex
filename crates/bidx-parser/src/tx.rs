//! Full block payload parsing: transactions, inputs, outputs, witnesses.
//!
//! The parser is zero-copy where possible: scripts are passed to the
//! classifier as slices, and txids are computed as dsha256 over the stripped
//! serialization (BIP141: txid excludes witness data). To avoid
//! re-serializing we hash two contiguous ranges when witness data is
//! present: [version .. end of outputs] + [locktime].

use bidx_core::{
    cursor::{Cursor, ParseError},
    dsha256, BlockRow, Hash32, InputRow, OutputRow, TxRow,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TxParseError {
    #[error(transparent)]
    Cursor(#[from] ParseError),
    #[error("tx count mismatch: declared {declared}, parsed {parsed}")]
    TxCountMismatch { declared: u64, parsed: u64 },
    #[error("bad satoshi value {0} (exceeds 21M BTC)")]
    BadValue(u64),
    #[error("invalid segwit marker/flag at tx {tx_index}")]
    BadSegwitFlag { tx_index: u32 },
}

const MAX_MONEY: u64 = 21_000_000 * 100_000_000;

pub struct FullBlock {
    pub block_row: BlockRow,
    pub txs: Vec<TxRow>,
    pub inputs: Vec<InputRow>,
    pub outputs: Vec<OutputRow>,
}

/// Parse a block payload (bytes beginning at the 80-byte header) at a known
/// height. `block_hash`/`header` come from the header pass.
///
/// Fees are NOT computed here — they require UTXO lookups and are filled in
/// by the spend-linking stage.
pub fn parse_block_full(
    payload: &[u8],
    height: u32,
    block_hash: Hash32,
    header: &bidx_core::BlockHeader,
) -> Result<FullBlock, TxParseError> {
    let mut c = Cursor::new(payload);
    c.skip(80)?;

    let tx_count = c.read_count()?;
    let mut txs = Vec::with_capacity((tx_count as usize).min(100_000));
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut coinbase_value = 0u64;
    let mut tx_weight_sum = 0u64;

    for tx_index in 0..tx_count as u32 {
        let (tx, tx_inputs, tx_outputs) = parse_tx(&mut c, payload, height, tx_index)?;
        if tx_index == 0 {
            coinbase_value = tx_outputs.iter().map(|o| o.value_sat).sum();
        }
        tx_weight_sum += tx.weight as u64;
        inputs.extend(tx_inputs);
        outputs.extend(tx_outputs);
        txs.push(tx);
    }

    if txs.len() as u64 != tx_count {
        return Err(TxParseError::TxCountMismatch {
            declared: tx_count,
            parsed: txs.len() as u64,
        });
    }

    let block_weight = (tx_weight_sum + 80 * 4).min(u32::MAX as u64) as u32;

    let block_row = BlockRow {
        height,
        hash: *block_hash.as_bytes(),
        prev_hash: *header.prev_hash.as_bytes(),
        merkle_root: *header.merkle_root.as_bytes(),
        time: header.time,
        bits: header.bits,
        nonce: header.nonce,
        version: header.version,
        tx_count: tx_count as u32,
        size: payload.len() as u32,
        weight: block_weight,
        total_fee: 0,
        coinbase_value,
    };

    Ok(FullBlock {
        block_row,
        txs,
        inputs,
        outputs,
    })
}

type ParsedTx = (TxRow, Vec<InputRow>, Vec<OutputRow>);

fn parse_tx<'a>(
    c: &mut Cursor<'a>,
    block: &'a [u8],
    height: u32,
    tx_index: u32,
) -> Result<ParsedTx, TxParseError> {
    let abs_start = c.pos();
    let version = c.read_i32()?;

    // BIP144 witness detection: marker=0x00, flag!=0x00.
    let marker = c.read_u8()?;
    let has_witness = marker == 0x00;
    let input_count = if has_witness {
        let flag = c.read_u8()?;
        if flag == 0 {
            return Err(TxParseError::BadSegwitFlag { tx_index });
        }
        c.read_count()?
    } else {
        // The byte we read was the first (and possibly only) varint byte.
        match marker {
            0x00..=0xfc => marker as u64,
            0xfd => c.read_u16()? as u64,
            0xfe => c.read_u32()? as u64,
            0xff => {
                let n = c.read_u64()?;
                if n > 4_000_000 {
                    return Err(ParseError::CountOverflow(n, c.pos()).into());
                }
                n
            }
        }
    };

    let is_single_input = input_count == 1;
    let mut inputs = Vec::with_capacity((input_count as usize).min(100_000));
    for input_index in 0..input_count as u32 {
        let prev_txid: [u8; 32] = c.read_array()?;
        let prev_vout = c.read_u32()?;
        let script_len = c.read_count()?;
        c.skip(script_len as usize)?;
        let sequence = c.read_u32()?;

        let is_coinbase =
            is_single_input && input_index == 0 && prev_txid == [0u8; 32] && prev_vout == 0xFFFF_FFFF;
        inputs.push(InputRow {
            height,
            tx_index,
            input_index,
            txid: [0u8; 32],
            prev_txid,
            prev_vout,
            script_sig_len: script_len as u32,
            sequence,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase,
        });
    }

    let output_count = c.read_count()?;
    let mut outputs = Vec::with_capacity((output_count as usize).min(100_000));
    for output_index in 0..output_count as u32 {
        let value_sat = c.read_u64()?;
        if value_sat > MAX_MONEY {
            return Err(TxParseError::BadValue(value_sat));
        }
        let spk_len = c.read_count()?;
        let spk = c.read_bytes(spk_len as usize)?;
        let (script_type, address_hash) = bidx_core::classify_script(spk);
        outputs.push(OutputRow {
            height,
            tx_index,
            output_index,
            txid: [0u8; 32],
            value_sat,
            script_pubkey_len: spk_len as u32,
            script_type: script_type as u8,
            address_hash,
        });
    }

    // Position right after outputs = end of the stripped prefix.
    let stripped_prefix_end = c.pos();

    if has_witness {
        for inp in inputs.iter_mut() {
            let item_count = c.read_count()?;
            inp.witness_items = item_count as u32;
            for _ in 0..item_count {
                let item_len = c.read_count()?;
                c.skip(item_len as usize)?;
                inp.witness_bytes += item_len as u32;
            }
        }
    }

    let locktime = c.read_u32()?;
    let abs_end = c.pos();

    // txid = dsha256(stripped serialization). When witness data is present
    // the stripped serialization = [abs_start..stripped_prefix_end] minus the
    // 2-byte marker/flag, plus [abs_end-4..abs_end].
    let txid = if has_witness {
        let prefix = &block[abs_start..stripped_prefix_end];
        let lock = &block[abs_end - 4..abs_end];
        // Hash in three parts to avoid allocating: version || inputs/outputs
        // (skipping marker+flag at prefix[4..6]) || locktime.
        let mut h = sha2::Sha256::new();
        use sha2::Digest;
        h.update(&prefix[..4]);
        h.update(&prefix[6..]);
        h.update(lock);
        let first = h.finalize();
        let mut h2 = sha2::Sha256::new();
        h2.update(first);
        let second = h2.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&second);
        Hash32::from_bytes(out)
    } else {
        dsha256(&block[abs_start..abs_end])
    };

    let txid_bytes = *txid.as_bytes();
    for inp in inputs.iter_mut() {
        inp.txid = txid_bytes;
    }
    for out in outputs.iter_mut() {
        out.txid = txid_bytes;
    }

    let total_len = (abs_end - abs_start) as u64;
    let stripped_len = if has_witness {
        // prefix minus marker/flag, plus locktime
        (stripped_prefix_end - abs_start - 2 + 4) as u64
    } else {
        total_len
    };
    // BIP141: weight = stripped*4 + witness = stripped*3 + total
    let weight = stripped_len * 3 + total_len;

    let tx = TxRow {
        height,
        tx_index,
        txid: txid_bytes,
        version,
        locktime,
        size: total_len as u32,
        weight: weight as u32,
        fee: 0,
        input_count: input_count as u32,
        output_count: output_count as u32,
        has_witness,
    };
    Ok((tx, inputs, outputs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_for(version: i32) -> bidx_core::BlockHeader {
        bidx_core::BlockHeader {
            version,
            prev_hash: Hash32::ZERO,
            merkle_root: Hash32::ZERO,
            time: 0,
            bits: 0,
            nonce: 0,
        }
    }

    /// Non-witness, single-input, single-output synthetic tx body. Used both
    /// standalone and embedded in block payloads. Returns the tx bytes.
    fn synth_tx_body(value_sat: u64) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&1i32.to_le_bytes()); // version
        b.push(1u8); // input_count
        b.extend_from_slice(&[0xABu8; 32]); // prev_txid
        b.extend_from_slice(&0u32.to_le_bytes()); // prev_vout
        b.push(0u8); // script_len = 0
        b.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // sequence
        b.push(1u8); // output_count
        b.extend_from_slice(&value_sat.to_le_bytes());
        b.push(0u8); // script_pubkey_len = 0
        b.extend_from_slice(&0u32.to_le_bytes()); // locktime
        b
    }

    /// Build a block payload: 80-byte header + varint tx_count + tx bytes.
    fn block_with(txs: &[Vec<u8>]) -> Vec<u8> {
        let mut b = vec![0u8; 80];
        b.extend_from_slice(&(txs.len() as u64 as u8).to_le_bytes()); // varint, small counts
        for t in txs {
            b.extend_from_slice(t);
        }
        b
    }

    #[test]
    fn parses_simple_block_weight_size_and_txid_fields() {
        let tx0 = synth_tx_body(42_000);
        let payload = block_with(&[tx0.clone()]);
        let hash = dsha256(&payload[..80]);
        let header = header_for(1);
        let block = parse_block_full(&payload, 5, hash, &header).expect("parse");
        // BlockRow fields.
        assert_eq!(block.block_row.height, 5);
        assert_eq!(block.block_row.tx_count, 1);
        assert_eq!(block.block_row.size, payload.len() as u32);
        assert_eq!(block.block_row.coinbase_value, 42_000);
        assert_eq!(block.block_row.total_fee, 0);
        assert_eq!(block.block_row.hash, *hash.as_bytes());
        // TxRow fields.
        let tx = &block.txs[0];
        assert_eq!(tx.tx_index, 0);
        assert_eq!(tx.height, 5);
        assert_eq!(tx.version, 1);
        assert_eq!(tx.locktime, 0);
        assert_eq!(tx.input_count, 1);
        assert_eq!(tx.output_count, 1);
        assert!(!tx.has_witness);
        assert_eq!(tx.size, tx0.len() as u32);
        assert_eq!(tx.weight, tx0.len() as u32 * 4);
        // txid = dsha256(stripped = full tx since no witness).
        assert_eq!(tx.txid, *dsha256(&tx0).as_bytes());
        // Input row.
        let inp = &block.inputs[0];
        assert_eq!(inp.prev_txid, [0xABu8; 32]);
        assert_eq!(inp.prev_vout, 0);
        assert_eq!(inp.sequence, 0xffff_ffff);
        assert_eq!(inp.txid, tx.txid);
        // Prev txid non-zero and vout not 0xffff_ffff so this is NOT coinbase.
        assert!(!inp.is_coinbase);
        // Output row.
        let out = &block.outputs[0];
        assert_eq!(out.value_sat, 42_000);
        assert_eq!(out.txid, tx.txid);
    }

    #[test]
    fn detects_coinbase_via_prev_out_pattern() {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes());
        tx.push(1u8);
        tx.extend_from_slice(&[0u8; 32]); // prev_txid = zero
        tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes()); // prev_vout = max
        tx.push(0u8);
        tx.extend_from_slice(&0u32.to_le_bytes());
        tx.push(1u8);
        tx.extend_from_slice(&50_0000_0000u64.to_le_bytes()); // 50 BTC
        tx.push(0u8);
        tx.extend_from_slice(&0u32.to_le_bytes());

        let payload = block_with(&[tx]);
        let hash = dsha256(&payload[..80]);
        let block = parse_block_full(&payload, 0, hash, &header_for(1)).unwrap();
        assert!(block.inputs[0].is_coinbase);
        assert_eq!(block.outputs[0].value_sat, 50_0000_0000);
        assert_eq!(block.block_row.coinbase_value, 50_0000_0000);
    }

    #[test]
    fn rejects_value_above_21m_btc() {
        let tx = synth_tx_body(21_000_001u64 * 100_000_000);
        let payload = block_with(&[tx]);
        let hash = dsha256(&payload[..80]);
        let e = parse_block_full(&payload, 0, hash, &header_for(1)).err().unwrap();
        assert!(matches!(e, TxParseError::BadValue(_)), "got {:?}", e);
    }

    #[test]
    fn tx_count_mismatch_when_payload_truncated() {
        // Varint says 2 txs but only 1 tx is present.
        let mut payload = vec![0u8; 80];
        payload.extend_from_slice(&2u64.to_le_bytes()[..1]); // count = 2 (varint)
        payload.extend_from_slice(&synth_tx_body(1_000));
        let hash = dsha256(&payload[..80]);
        let e = parse_block_full(&payload, 0, hash, &header_for(1)).err().unwrap();
        // Either an EOF in cursor machinery or the explicit TxCountMismatch.
        match e {
            TxParseError::Cursor(_) | TxParseError::TxCountMismatch { .. } => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn segwit_tx_parses_witness_bytes_and_weight() {
        // Hand-built segwit tx:
        //   version(4) | marker(0x00) | flag(0x01) | inputs(1) | ... | outputs(1) | ... |
        //   witness (item_count=1, item_len=2, [0xAA, 0xBB]) | locktime
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes());
        tx.push(0x00); // marker
        tx.push(0x01); // flag
        tx.push(1u8); // input_count
        tx.extend_from_slice(&[0xCDu8; 32]);
        tx.extend_from_slice(&7u32.to_le_bytes()); // vout=7
        tx.push(0u8);
        tx.extend_from_slice(&0u32.to_le_bytes());
        tx.push(1u8); // output_count
        tx.extend_from_slice(&10_000u64.to_le_bytes());
        tx.push(0u8);
        // witness for input 0: 1 item, 2 bytes
        tx.push(1u8);
        tx.push(2u8);
        tx.push(0xAA);
        tx.push(0xBB);
        tx.extend_from_slice(&0x1122_3344u32.to_le_bytes()); // locktime

        let payload = block_with(&[tx.clone()]);
        let hash = dsha256(&payload[..80]);
        let block = parse_block_full(&payload, 9, hash, &header_for(2)).unwrap();
        let txrow = &block.txs[0];
        assert!(txrow.has_witness);
        assert_eq!(txrow.locktime, 0x1122_3344);
        assert_eq!(block.inputs[0].witness_items, 1);
        assert_eq!(block.inputs[0].witness_bytes, 2);
        // BIP141 weight math, computed directly from the wire bytes we built:
        //   tx        = 66 bytes (4+1+1+1+41+1+9+4+4)
        //   stripped  = 60 bytes  (4+1+41+1+9+4, marker+flag and witness removed)
        //   weight    = stripped*3 + total = 60*3 + 66 = 246
        let stripped = 4 + 1 + 41 + 1 + 9 + 4;
        let total = tx.len();
        assert_eq!(txrow.size, total as u32);
        assert_eq!(txrow.weight, (stripped * 3 + total) as u32);

        // Recompute the expected segwit txid using the explicit witnessless
        // stripped serialization and assert the parser's incremental hash matches.
        let mut stripped_bytes = Vec::new();
        stripped_bytes.extend_from_slice(&1i32.to_le_bytes()); // version
        stripped_bytes.push(1u8); // input_count (no marker/flag)
        stripped_bytes.extend_from_slice(&[0xCDu8; 32]);
        stripped_bytes.extend_from_slice(&7u32.to_le_bytes());
        stripped_bytes.push(0u8);
        stripped_bytes.extend_from_slice(&0u32.to_le_bytes());
        stripped_bytes.push(1u8); // output_count
        stripped_bytes.extend_from_slice(&10_000u64.to_le_bytes());
        stripped_bytes.push(0u8);
        stripped_bytes.extend_from_slice(&0x1122_3344u32.to_le_bytes());
        let want_txid = dsha256(&stripped_bytes);
        assert_eq!(txrow.txid, *want_txid.as_bytes());
    }

    #[test]
    fn segwit_flag_zero_rejected() {
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes());
        tx.push(0x00); // marker
        tx.push(0x00); // flag = 0 — invalid
        // Whatever follows doesn't matter; the parser must reject before reading.
        tx.extend_from_slice(&[0u8; 32]);
        let payload = block_with(&[tx]);
        let hash = dsha256(&payload[..80]);
        let e = parse_block_full(&payload, 0, hash, &header_for(1)).err().unwrap();
        assert!(matches!(e, TxParseError::BadSegwitFlag { .. }));
    }

    #[test]
    fn multi_tx_block_rolls_up_counts_and_weight() {
        let tx0 = synth_tx_body(1_000);
        let tx1 = synth_tx_body(2_000);
        let payload = block_with(&[tx0.clone(), tx1.clone()]);
        let hash = dsha256(&payload[..80]);
        let block = parse_block_full(&payload, 7, hash, &header_for(1)).unwrap();
        assert_eq!(block.txs.len(), 2);
        assert_eq!(block.inputs.len(), 2);
        assert_eq!(block.outputs.len(), 2);
        assert_eq!(block.txs[0].tx_index, 0);
        assert_eq!(block.txs[1].tx_index, 1);
        assert_ne!(block.txs[0].txid, block.txs[1].txid);
        assert_eq!(block.block_row.tx_count, 2);
        // Block weight = sum(tx weights) + 80*4 (header); enforced by parse_block_full.
        let expect_w = (tx0.len() * 4 + tx1.len() * 4 + 80 * 4) as u32;
        assert_eq!(block.block_row.weight, expect_w);
    }

    #[test]
    fn varint_multi_byte_lengths_for_in_out_counts() {
        // 0xfd (u16) multi-byte varint for input_count path.
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes());
        tx.push(0xfd);
        tx.extend_from_slice(&1u16.to_le_bytes()); // input_count = 1 via 0xfd varint
        tx.extend_from_slice(&[0u8; 32]);
        tx.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        tx.push(0u8);
        tx.extend_from_slice(&0u32.to_le_bytes());
        // 0xfe (u32) varint for output_count path.
        tx.push(0xfe);
        tx.extend_from_slice(&1u32.to_le_bytes()); // output_count = 1 via 0xfe varint
        tx.extend_from_slice(&5_000u64.to_le_bytes());
        tx.push(0u8);
        tx.extend_from_slice(&0u32.to_le_bytes());
        let payload = block_with(&[tx]);
        let hash = dsha256(&payload[..80]);
        let block = parse_block_full(&payload, 4, hash, &header_for(1)).unwrap();
        assert_eq!(block.inputs.len(), 1);
        assert_eq!(block.outputs.len(), 1);
    }

    #[test]
    fn multi_input_witness_each_tracked_independently() {
        // Two-input segwit tx: first input has 0 witness items, second has 2.
        let mut tx = Vec::new();
        tx.extend_from_slice(&1i32.to_le_bytes());
        tx.push(0x00);
        tx.push(0x01);
        tx.push(2u8); // two inputs
        for _ in 0..2 {
            tx.extend_from_slice(&[0xEEu8; 32]);
            tx.extend_from_slice(&0u32.to_le_bytes());
            tx.push(0u8); // script_len
            tx.extend_from_slice(&0u32.to_le_bytes());
        }
        tx.push(1u8); // one output
        tx.extend_from_slice(&1_000u64.to_le_bytes());
        tx.push(0u8);
        // Witness for input 0: 0 items.
        tx.push(0u8);
        // Witness for input 1: 2 items, sizes 1 and 3 -> witness_bytes=4, items=2.
        tx.push(2u8);
        tx.push(1u8);
        tx.push(0xAA);
        tx.push(3u8);
        tx.extend_from_slice(&[0xBB, 0xCC, 0xDD]);
        tx.extend_from_slice(&0u32.to_le_bytes());
        let payload = block_with(&[tx.clone()]);
        let hash = dsha256(&payload[..80]);
        let block = parse_block_full(&payload, 8, hash, &header_for(1)).unwrap();
        assert_eq!(block.inputs.len(), 2);
        assert_eq!(block.inputs[0].witness_items, 0);
        assert_eq!(block.inputs[0].witness_bytes, 0);
        assert_eq!(block.inputs[1].witness_items, 2);
        assert_eq!(block.inputs[1].witness_bytes, 4);

        // Each input is 41 bytes (32+4+1+4); strip marker+flag (2), witness
        // encodings (1 + 1+1+1+3 = 7), and we have:
        //   stripped = 4(version) + 1(cnt) + 2*41(ins) + 1(cnt) + 9(out) + 4(lock) = 101
        let stripped: u32 = 4 + 1 + 2 * 41 + 1 + 9 + 4;
        let total = tx.len() as u32;
        assert_eq!(block.txs[0].size, total);
        assert_eq!(block.txs[0].weight, stripped * 3 + total);
    }
}
