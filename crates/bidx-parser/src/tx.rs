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
