//! Buffered Parquet writers for the four base entity tables (`blocks`,
//! `transactions`, `inputs`, `outputs`) plus optional `witnesses`.
//!
//! One writer instance per output part file. Rows accumulate directly in
//! concrete Struct-of-Arrays (SoA) column buffers without per-cell enum
//! dispatch or unnecessary null bitmaps, and flush as a row group when
//! `batch_size` rows have accumulated. Full batches and part-file closes run
//! Zstd(1) encoding in parallel across entities via Rayon, with dictionary
//! encoding disabled on random 32-byte digests so `ArrowWriter` does not hash
//! billions of unique txids into doomed 1 MiB dictionary pages.

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryArray, Int32Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bidx_core::{BlockRow, InputRow, OutputRow, TxRow, WitnessRow};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy)]
pub struct SinkConfig {
    /// Rows per Arrow record batch / Parquet row group.
    pub batch_size: usize,
    /// Write the optional witness payload table. Off by default: the payload
    /// roughly doubles on-disk size and no hot table needs it.
    pub capture_witness: bool,
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            batch_size: 250_000,
            capture_witness: false,
        }
    }
}

/// Columns whose values repeat frequently enough within a 250k-row group that
/// Parquet dictionary encoding pays off. High-entropy 32-byte cryptographic
/// digests (`txid`, `prev_txid`, `spending_txid`, `spent_txid`, `hash`,
/// `merkle_root`) and raw witness blobs are left plain (`dictionary_enabled =
/// false`) so `ArrowWriter` skips hashing ~8 billion unique digests into a
/// dictionary `HashMap` that always overflows at 1 MiB.
const DICTIONARY_COLUMNS: &[&str] = &[
    "height",
    "spent_height",
    "version",
    "script_type",
    "spent_script_type",
    "script_pubkey_len",
    "script_sig_len",
    "sequence",
    "address_hash",
    "spent_address_hash",
];

/// Uniformly distributed 256-bit SHA-256d digests across a mainnet parse.
/// Feeding cryptographic white noise through Zstd's LZ77 match finder burns
/// ~65-70% of Parquet CPU to emit literal blocks at a `1.000x` ratio; storing
/// these columns `UNCOMPRESSED` is byte-size neutral and ~3x faster.
/// (`address_hash` / `spent_address_hash` stay Zstd+dictionary because ~78%
/// carry 12 trailing zero bytes and addresses repeat frequently.)
const UNCOMPRESSED_HASH_COLUMNS: &[&str] =
    &["hash", "prev_hash", "merkle_root", "txid", "prev_txid"];

/// Non-dictionary integer coordinate, count, and satoshi columns where Parquet
/// `DELTA_BINARY_PACKED` bitpacks monotonic/small deltas before Zstd(1).
const DELTA_INT_COLUMNS: &[&str] = &[
    "tx_index",
    "input_index",
    "output_index",
    "prev_vout",
    "time",
    "nonce",
    "tx_count",
    "size",
    "weight",
    "input_count",
    "output_count",
    "value_sat",
    "spent_value_sat",
    "fee",
    "total_fee",
    "coinbase_value",
    "item_index",
    "item_len",
];

fn writer_props(batch_size: usize) -> WriterProperties {
    // Zstd level 1 gives ~2x faster compression CPU than default level 3 for
    // only ~3-5% larger output, and matching `max_row_group_size` to
    // `batch_size` flushes each `RecordBatch` directly to disk instead of
    // buffering up to 1M rows in `ArrowWriter`'s `InMemoryRowGroup`.
    let level = ZstdLevel::try_new(1).unwrap_or_default();
    let mut b = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .set_dictionary_enabled(false)
        .set_max_row_group_size(batch_size.max(1))
        .set_write_batch_size(8192);
    for &col in DICTIONARY_COLUMNS {
        b = b.set_column_dictionary_enabled(ColumnPath::from(col), true);
    }
    for &col in UNCOMPRESSED_HASH_COLUMNS {
        b = b.set_column_compression(ColumnPath::from(col), Compression::UNCOMPRESSED);
    }
    for &col in DELTA_INT_COLUMNS {
        b = b.set_column_encoding(ColumnPath::from(col), Encoding::DELTA_BINARY_PACKED);
    }
    b.build()
}

/// Write-back buffer for one Parquet part file.
///
/// `ArrowWriter` issues a `write` syscall for every encoded page, so an
/// unbuffered part file pays hundreds of small syscalls per row group. A 1 MiB
/// buffer collapses those into large sequential writes and is flushed on
/// `ArrowWriter::close`.
const PARQUET_WRITE_BUF_BYTES: usize = 1 << 20;

/// Cap initial preallocation per column builder so tiny or synthetic configs
/// do not over-allocate while default 250k batches grow zero times.
const MAX_PREALLOC_ROWS: usize = 250_000;

/// The writer every entity table uses: Parquet over a buffered file.
type EntityWriter = ArrowWriter<BufWriter<File>>;

fn fbin32(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(32), false)
}

// ---- zero-null-bitmap, zero-dispatch typed column buffers ----

struct Fixed32Col {
    buf: Vec<u8>,
    cap_rows: usize,
}

impl Fixed32Col {
    fn new(batch_size: usize) -> Self {
        let cap_rows = batch_size.clamp(1, MAX_PREALLOC_ROWS);
        Self {
            buf: Vec::with_capacity(cap_rows * 32),
            cap_rows,
        }
    }

    #[inline]
    fn push(&mut self, v: &[u8; 32]) {
        self.buf.extend_from_slice(v);
    }

    fn finish(&mut self) -> Result<ArrayRef, arrow::error::ArrowError> {
        let raw = std::mem::replace(&mut self.buf, Vec::with_capacity(self.cap_rows * 32));
        Ok(Arc::new(FixedSizeBinaryArray::try_new(
            32,
            Buffer::from_vec(raw),
            None,
        )?))
    }
}

#[inline]
fn finish_u32(col: &mut Vec<u32>, cap: usize) -> ArrayRef {
    Arc::new(UInt32Array::from(std::mem::replace(
        col,
        Vec::with_capacity(cap),
    )))
}

#[inline]
fn finish_u64(col: &mut Vec<u64>, cap: usize) -> ArrayRef {
    Arc::new(UInt64Array::from(std::mem::replace(
        col,
        Vec::with_capacity(cap),
    )))
}

#[inline]
fn finish_i32(col: &mut Vec<i32>, cap: usize) -> ArrayRef {
    Arc::new(Int32Array::from(std::mem::replace(
        col,
        Vec::with_capacity(cap),
    )))
}

#[inline]
fn finish_u8(col: &mut Vec<u8>, cap: usize) -> ArrayRef {
    Arc::new(UInt8Array::from(std::mem::replace(
        col,
        Vec::with_capacity(cap),
    )))
}

// ---- schemas ----

pub fn blocks_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        fbin32("hash"),
        fbin32("prev_hash"),
        fbin32("merkle_root"),
        Field::new("time", DataType::UInt32, false),
        Field::new("bits", DataType::UInt32, false),
        Field::new("nonce", DataType::UInt32, false),
        Field::new("version", DataType::Int32, false),
        Field::new("tx_count", DataType::UInt32, false),
        Field::new("size", DataType::UInt32, false),
        Field::new("weight", DataType::UInt32, false),
        Field::new("total_fee", DataType::UInt64, false),
        Field::new("coinbase_value", DataType::UInt64, false),
    ]))
}

pub fn transactions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        Field::new("tx_index", DataType::UInt32, false),
        fbin32("txid"),
        Field::new("version", DataType::Int32, false),
        Field::new("locktime", DataType::UInt32, false),
        Field::new("size", DataType::UInt32, false),
        Field::new("weight", DataType::UInt32, false),
        Field::new("fee", DataType::UInt64, false),
        Field::new("input_count", DataType::UInt32, false),
        Field::new("output_count", DataType::UInt32, false),
        Field::new("has_witness", DataType::Boolean, false),
    ]))
}

pub fn inputs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("input_index", DataType::UInt32, false),
        fbin32("prev_txid"),
        Field::new("prev_vout", DataType::UInt32, false),
        Field::new("script_sig_len", DataType::UInt32, false),
        Field::new("sequence", DataType::UInt32, false),
        Field::new("witness_items", DataType::UInt32, false),
        Field::new("witness_bytes", DataType::UInt32, false),
        Field::new("is_coinbase", DataType::Boolean, false),
        Field::new("spent_value_sat", DataType::UInt64, false),
        Field::new("spent_script_type", DataType::UInt8, false),
        fbin32("spent_address_hash"),
        Field::new("spent_height", DataType::UInt32, false),
    ]))
}

pub fn outputs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("output_index", DataType::UInt32, false),
        Field::new("value_sat", DataType::UInt64, false),
        Field::new("script_pubkey_len", DataType::UInt32, false),
        Field::new("script_type", DataType::UInt8, false),
        fbin32("address_hash"),
    ]))
}

/// Optional witness payload table: one row per witness stack element. Only
/// written when parse was run with witness capture enabled, so this file is
/// absent from a normal `parse` output directory.
pub fn witnesses_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("input_index", DataType::UInt32, false),
        Field::new("item_index", DataType::UInt32, false),
        Field::new("item_len", DataType::UInt32, false),
        Field::new("item_bytes", DataType::Binary, false),
    ]))
}

// ---- concrete Struct-of-Arrays builders per entity table ----

struct BlocksTable {
    schema: SchemaRef,
    cap: usize,
    buffered: usize,
    height: Vec<u32>,
    hash: Fixed32Col,
    prev_hash: Fixed32Col,
    merkle_root: Fixed32Col,
    time: Vec<u32>,
    bits: Vec<u32>,
    nonce: Vec<u32>,
    version: Vec<i32>,
    tx_count: Vec<u32>,
    size: Vec<u32>,
    weight: Vec<u32>,
    total_fee: Vec<u64>,
    coinbase_value: Vec<u64>,
}

impl BlocksTable {
    fn new(batch_size: usize) -> Self {
        let cap = batch_size.clamp(1, MAX_PREALLOC_ROWS);
        Self {
            schema: blocks_schema(),
            cap,
            buffered: 0,
            height: Vec::with_capacity(cap),
            hash: Fixed32Col::new(cap),
            prev_hash: Fixed32Col::new(cap),
            merkle_root: Fixed32Col::new(cap),
            time: Vec::with_capacity(cap),
            bits: Vec::with_capacity(cap),
            nonce: Vec::with_capacity(cap),
            version: Vec::with_capacity(cap),
            tx_count: Vec::with_capacity(cap),
            size: Vec::with_capacity(cap),
            weight: Vec::with_capacity(cap),
            total_fee: Vec::with_capacity(cap),
            coinbase_value: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn append(&mut self, r: &BlockRow) {
        self.height.push(r.height);
        self.hash.push(&r.hash);
        self.prev_hash.push(&r.prev_hash);
        self.merkle_root.push(&r.merkle_root);
        self.time.push(r.time);
        self.bits.push(r.bits);
        self.nonce.push(r.nonce);
        self.version.push(r.version);
        self.tx_count.push(r.tx_count);
        self.size.push(r.size);
        self.weight.push(r.weight);
        self.total_fee.push(r.total_fee);
        self.coinbase_value.push(r.coinbase_value);
        self.buffered += 1;
    }

    fn take_batch(&mut self) -> Result<RecordBatch, SinkError> {
        let c = self.cap;
        let arrays: Vec<ArrayRef> = vec![
            finish_u32(&mut self.height, c),
            self.hash.finish()?,
            self.prev_hash.finish()?,
            self.merkle_root.finish()?,
            finish_u32(&mut self.time, c),
            finish_u32(&mut self.bits, c),
            finish_u32(&mut self.nonce, c),
            finish_i32(&mut self.version, c),
            finish_u32(&mut self.tx_count, c),
            finish_u32(&mut self.size, c),
            finish_u32(&mut self.weight, c),
            finish_u64(&mut self.total_fee, c),
            finish_u64(&mut self.coinbase_value, c),
        ];
        self.buffered = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

struct TxsTable {
    schema: SchemaRef,
    cap: usize,
    buffered: usize,
    height: Vec<u32>,
    tx_index: Vec<u32>,
    txid: Fixed32Col,
    version: Vec<i32>,
    locktime: Vec<u32>,
    size: Vec<u32>,
    weight: Vec<u32>,
    fee: Vec<u64>,
    input_count: Vec<u32>,
    output_count: Vec<u32>,
    has_witness: BooleanBuilder,
}

impl TxsTable {
    fn new(batch_size: usize) -> Self {
        let cap = batch_size.clamp(1, MAX_PREALLOC_ROWS);
        Self {
            schema: transactions_schema(),
            cap,
            buffered: 0,
            height: Vec::with_capacity(cap),
            tx_index: Vec::with_capacity(cap),
            txid: Fixed32Col::new(cap),
            version: Vec::with_capacity(cap),
            locktime: Vec::with_capacity(cap),
            size: Vec::with_capacity(cap),
            weight: Vec::with_capacity(cap),
            fee: Vec::with_capacity(cap),
            input_count: Vec::with_capacity(cap),
            output_count: Vec::with_capacity(cap),
            has_witness: BooleanBuilder::with_capacity(cap),
        }
    }

    #[inline]
    fn append(&mut self, r: &TxRow) {
        self.height.push(r.height);
        self.tx_index.push(r.tx_index as u32);
        self.txid.push(&r.txid);
        self.version.push(r.version);
        self.locktime.push(r.locktime);
        self.size.push(r.size);
        self.weight.push(r.weight);
        self.fee.push(r.fee);
        self.input_count.push(r.input_count);
        self.output_count.push(r.output_count);
        self.has_witness.append_value(r.has_witness);
        self.buffered += 1;
    }

    fn take_batch(&mut self) -> Result<RecordBatch, SinkError> {
        let c = self.cap;
        let arrays: Vec<ArrayRef> = vec![
            finish_u32(&mut self.height, c),
            finish_u32(&mut self.tx_index, c),
            self.txid.finish()?,
            finish_i32(&mut self.version, c),
            finish_u32(&mut self.locktime, c),
            finish_u32(&mut self.size, c),
            finish_u32(&mut self.weight, c),
            finish_u64(&mut self.fee, c),
            finish_u32(&mut self.input_count, c),
            finish_u32(&mut self.output_count, c),
            Arc::new(self.has_witness.finish()),
        ];
        self.buffered = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

struct InputsTable {
    schema: SchemaRef,
    cap: usize,
    buffered: usize,
    height: Vec<u32>,
    tx_index: Vec<u32>,
    input_index: Vec<u32>,
    prev_txid: Fixed32Col,
    prev_vout: Vec<u32>,
    script_sig_len: Vec<u32>,
    sequence: Vec<u32>,
    witness_items: Vec<u32>,
    witness_bytes: Vec<u32>,
    is_coinbase: BooleanBuilder,
    spent_value_sat: Vec<u64>,
    spent_script_type: Vec<u8>,
    spent_address_hash: Fixed32Col,
    spent_height: Vec<u32>,
}

impl InputsTable {
    fn new(batch_size: usize) -> Self {
        let cap = batch_size.clamp(1, MAX_PREALLOC_ROWS);
        Self {
            schema: inputs_schema(),
            cap,
            buffered: 0,
            height: Vec::with_capacity(cap),
            tx_index: Vec::with_capacity(cap),
            input_index: Vec::with_capacity(cap),
            prev_txid: Fixed32Col::new(cap),
            prev_vout: Vec::with_capacity(cap),
            script_sig_len: Vec::with_capacity(cap),
            sequence: Vec::with_capacity(cap),
            witness_items: Vec::with_capacity(cap),
            witness_bytes: Vec::with_capacity(cap),
            is_coinbase: BooleanBuilder::with_capacity(cap),
            spent_value_sat: Vec::with_capacity(cap),
            spent_script_type: Vec::with_capacity(cap),
            spent_address_hash: Fixed32Col::new(cap),
            spent_height: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn append(&mut self, r: &InputRow) {
        self.height.push(r.height);
        self.tx_index.push(r.tx_index as u32);
        self.input_index.push(r.input_index as u32);
        self.prev_txid.push(&r.prev_txid);
        self.prev_vout.push(r.prev_vout);
        self.script_sig_len.push(r.script_sig_len);
        self.sequence.push(r.sequence);
        self.witness_items.push(r.witness_items as u32);
        self.witness_bytes.push(r.witness_bytes);
        self.is_coinbase.append_value(r.is_coinbase);
        self.spent_value_sat.push(r.spent_value_sat);
        self.spent_script_type.push(r.spent_script_type);
        self.spent_address_hash.push(&r.spent_address_hash);
        self.spent_height.push(r.spent_height);
        self.buffered += 1;
    }

    fn take_batch(&mut self) -> Result<RecordBatch, SinkError> {
        let c = self.cap;
        let arrays: Vec<ArrayRef> = vec![
            finish_u32(&mut self.height, c),
            finish_u32(&mut self.tx_index, c),
            finish_u32(&mut self.input_index, c),
            self.prev_txid.finish()?,
            finish_u32(&mut self.prev_vout, c),
            finish_u32(&mut self.script_sig_len, c),
            finish_u32(&mut self.sequence, c),
            finish_u32(&mut self.witness_items, c),
            finish_u32(&mut self.witness_bytes, c),
            Arc::new(self.is_coinbase.finish()),
            finish_u64(&mut self.spent_value_sat, c),
            finish_u8(&mut self.spent_script_type, c),
            self.spent_address_hash.finish()?,
            finish_u32(&mut self.spent_height, c),
        ];
        self.buffered = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

struct OutputsTable {
    schema: SchemaRef,
    cap: usize,
    buffered: usize,
    height: Vec<u32>,
    tx_index: Vec<u32>,
    output_index: Vec<u32>,
    value_sat: Vec<u64>,
    script_pubkey_len: Vec<u32>,
    script_type: Vec<u8>,
    address_hash: Fixed32Col,
}

impl OutputsTable {
    fn new(batch_size: usize) -> Self {
        let cap = batch_size.clamp(1, MAX_PREALLOC_ROWS);
        Self {
            schema: outputs_schema(),
            cap,
            buffered: 0,
            height: Vec::with_capacity(cap),
            tx_index: Vec::with_capacity(cap),
            output_index: Vec::with_capacity(cap),
            value_sat: Vec::with_capacity(cap),
            script_pubkey_len: Vec::with_capacity(cap),
            script_type: Vec::with_capacity(cap),
            address_hash: Fixed32Col::new(cap),
        }
    }

    #[inline]
    fn append(&mut self, r: &OutputRow) {
        self.height.push(r.height);
        self.tx_index.push(r.tx_index as u32);
        self.output_index.push(r.output_index as u32);
        self.value_sat.push(r.value_sat);
        self.script_pubkey_len.push(r.script_pubkey_len);
        self.script_type.push(r.script_type);
        self.address_hash.push(&r.address_hash);
        self.buffered += 1;
    }

    fn take_batch(&mut self) -> Result<RecordBatch, SinkError> {
        let c = self.cap;
        let arrays: Vec<ArrayRef> = vec![
            finish_u32(&mut self.height, c),
            finish_u32(&mut self.tx_index, c),
            finish_u32(&mut self.output_index, c),
            finish_u64(&mut self.value_sat, c),
            finish_u32(&mut self.script_pubkey_len, c),
            finish_u8(&mut self.script_type, c),
            self.address_hash.finish()?,
        ];
        self.buffered = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

struct WitnessesTable {
    schema: SchemaRef,
    cap: usize,
    buffered: usize,
    height: Vec<u32>,
    tx_index: Vec<u32>,
    input_index: Vec<u32>,
    item_index: Vec<u32>,
    item_len: Vec<u32>,
    item_bytes: BinaryBuilder,
}

impl WitnessesTable {
    fn new(batch_size: usize) -> Self {
        let cap = batch_size.clamp(1, MAX_PREALLOC_ROWS);
        Self {
            schema: witnesses_schema(),
            cap,
            buffered: 0,
            height: Vec::with_capacity(cap),
            tx_index: Vec::with_capacity(cap),
            input_index: Vec::with_capacity(cap),
            item_index: Vec::with_capacity(cap),
            item_len: Vec::with_capacity(cap),
            item_bytes: BinaryBuilder::with_capacity(cap, cap * 32),
        }
    }

    #[inline]
    fn append(&mut self, r: &WitnessRow) {
        self.height.push(r.height);
        self.tx_index.push(r.tx_index as u32);
        self.input_index.push(r.input_index as u32);
        self.item_index.push(r.item_index);
        self.item_len.push(r.item_len);
        self.item_bytes.append_value(&r.item_bytes);
        self.buffered += 1;
    }

    fn take_batch(&mut self) -> Result<RecordBatch, SinkError> {
        let c = self.cap;
        let arrays: Vec<ArrayRef> = vec![
            finish_u32(&mut self.height, c),
            finish_u32(&mut self.tx_index, c),
            finish_u32(&mut self.input_index, c),
            finish_u32(&mut self.item_index, c),
            finish_u32(&mut self.item_len, c),
            Arc::new(self.item_bytes.finish()),
        ];
        self.buffered = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// One Parquet file per entity, all sharing a part id.
pub struct ParquetSink {
    cfg: SinkConfig,
    #[allow(dead_code)]
    part: u32,

    blocks_w: EntityWriter,
    txs_w: EntityWriter,
    inputs_w: EntityWriter,
    outputs_w: EntityWriter,
    /// Only present when `cfg.capture_witness` is set.
    witnesses_w: Option<EntityWriter>,

    blocks: BlocksTable,
    txs: TxsTable,
    inputs: InputsTable,
    outputs: OutputsTable,
    witnesses: WitnessesTable,
}

fn create_writer(
    dir: &Path,
    entity: &str,
    part: u32,
    schema: SchemaRef,
    batch_size: usize,
) -> Result<EntityWriter, SinkError> {
    let pdir = dir.join(entity);
    std::fs::create_dir_all(&pdir)?;
    let file = File::create(pdir.join(format!("part-{part:05}.parquet")))?;
    let buffered = BufWriter::with_capacity(PARQUET_WRITE_BUF_BYTES, file);
    Ok(ArrowWriter::try_new(
        buffered,
        schema,
        Some(writer_props(batch_size)),
    )?)
}

fn finish_entity(batch: Option<RecordBatch>, mut w: EntityWriter) -> Result<(), SinkError> {
    if let Some(b) = batch {
        w.write(&b)?;
    }
    w.close()?;
    Ok(())
}

impl ParquetSink {
    pub fn create(dir: &Path, part: u32, cfg: SinkConfig) -> Result<Self, SinkError> {
        let bs = cfg.batch_size;
        let witnesses_w = if cfg.capture_witness {
            Some(create_writer(
                dir,
                "witnesses",
                part,
                witnesses_schema(),
                bs,
            )?)
        } else {
            None
        };
        let witness_cap = if cfg.capture_witness { bs } else { 1 };
        Ok(ParquetSink {
            cfg,
            part,
            blocks_w: create_writer(dir, "blocks", part, blocks_schema(), bs)?,
            txs_w: create_writer(dir, "transactions", part, transactions_schema(), bs)?,
            inputs_w: create_writer(dir, "inputs", part, inputs_schema(), bs)?,
            outputs_w: create_writer(dir, "outputs", part, outputs_schema(), bs)?,
            witnesses_w,
            blocks: BlocksTable::new(bs),
            txs: TxsTable::new(bs),
            inputs: InputsTable::new(bs),
            outputs: OutputsTable::new(bs),
            witnesses: WitnessesTable::new(witness_cap),
        })
    }

    pub fn push_block(&mut self, r: &BlockRow) -> Result<(), SinkError> {
        self.blocks.append(r);
        if self.blocks.buffered >= self.cfg.batch_size {
            let b = self.blocks.take_batch()?;
            self.blocks_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_tx(&mut self, r: &TxRow) -> Result<(), SinkError> {
        self.txs.append(r);
        if self.txs.buffered >= self.cfg.batch_size {
            let b = self.txs.take_batch()?;
            self.txs_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_input(&mut self, r: &InputRow) -> Result<(), SinkError> {
        self.inputs.append(r);
        if self.inputs.buffered >= self.cfg.batch_size {
            let b = self.inputs.take_batch()?;
            self.inputs_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_output(&mut self, r: &OutputRow) -> Result<(), SinkError> {
        self.outputs.append(r);
        if self.outputs.buffered >= self.cfg.batch_size {
            let b = self.outputs.take_batch()?;
            self.outputs_w.write(&b)?;
        }
        Ok(())
    }

    /// Append one witness payload row. A no-op unless the sink was created
    /// with `capture_witness`, so callers can push unconditionally.
    pub fn push_witness(&mut self, r: &WitnessRow) -> Result<(), SinkError> {
        let Some(w) = self.witnesses_w.as_mut() else {
            return Ok(());
        };
        self.witnesses.append(r);
        if self.witnesses.buffered >= self.cfg.batch_size {
            let b = self.witnesses.take_batch()?;
            w.write(&b)?;
        }
        Ok(())
    }

    /// Push all rows of an enriched block and flush any full entity row groups
    /// concurrently across Rayon workers so Zstd encoding on `inputs`,
    /// `outputs`, and `transactions` overlaps instead of serializing on a
    /// single sink thread.
    pub fn push_block_rows(
        &mut self,
        block: &BlockRow,
        txs: &[TxRow],
        inputs: &[InputRow],
        outputs: &[OutputRow],
        witnesses: &[WitnessRow],
    ) -> Result<(), SinkError> {
        let bs = self.cfg.batch_size.max(1);
        self.blocks.append(block);

        let mut tx_rem = txs;
        let mut inp_rem = inputs;
        let mut out_rem = outputs;
        let mut wit_rem = if self.witnesses_w.is_some() {
            witnesses
        } else {
            &[]
        };

        loop {
            let tx_take = tx_rem.len().min(bs.saturating_sub(self.txs.buffered));
            for r in &tx_rem[..tx_take] {
                self.txs.append(r);
            }
            tx_rem = &tx_rem[tx_take..];

            let inp_take = inp_rem.len().min(bs.saturating_sub(self.inputs.buffered));
            for r in &inp_rem[..inp_take] {
                self.inputs.append(r);
            }
            inp_rem = &inp_rem[inp_take..];

            let out_take = out_rem.len().min(bs.saturating_sub(self.outputs.buffered));
            for r in &out_rem[..out_take] {
                self.outputs.append(r);
            }
            out_rem = &out_rem[out_take..];

            let wit_take = wit_rem
                .len()
                .min(bs.saturating_sub(self.witnesses.buffered));
            for r in &wit_rem[..wit_take] {
                self.witnesses.append(r);
            }
            wit_rem = &wit_rem[wit_take..];

            self.flush_full_parallel(bs)?;

            if tx_rem.is_empty() && inp_rem.is_empty() && out_rem.is_empty() && wit_rem.is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn flush_full_parallel(&mut self, bs: usize) -> Result<(), SinkError> {
        let blk_b = (self.blocks.buffered >= bs)
            .then(|| self.blocks.take_batch())
            .transpose()?;
        let tx_b = (self.txs.buffered >= bs)
            .then(|| self.txs.take_batch())
            .transpose()?;
        let inp_b = (self.inputs.buffered >= bs)
            .then(|| self.inputs.take_batch())
            .transpose()?;
        let out_b = (self.outputs.buffered >= bs)
            .then(|| self.outputs.take_batch())
            .transpose()?;
        let wit_b = (self.witnesses_w.is_some() && self.witnesses.buffered >= bs)
            .then(|| self.witnesses.take_batch())
            .transpose()?;

        let ready = usize::from(blk_b.is_some())
            + usize::from(tx_b.is_some())
            + usize::from(inp_b.is_some())
            + usize::from(out_b.is_some())
            + usize::from(wit_b.is_some());
        if ready == 0 {
            return Ok(());
        }
        if ready == 1 {
            if let Some(b) = blk_b {
                self.blocks_w.write(&b)?;
            }
            if let Some(b) = tx_b {
                self.txs_w.write(&b)?;
            }
            if let Some(b) = inp_b {
                self.inputs_w.write(&b)?;
            }
            if let Some(b) = out_b {
                self.outputs_w.write(&b)?;
            }
            if let (Some(b), Some(w)) = (wit_b, self.witnesses_w.as_mut()) {
                w.write(&b)?;
            }
            return Ok(());
        }

        let blocks_w = &mut self.blocks_w;
        let txs_w = &mut self.txs_w;
        let inputs_w = &mut self.inputs_w;
        let outputs_w = &mut self.outputs_w;
        let witnesses_w = &mut self.witnesses_w;

        let mut r_blk = Ok(());
        let mut r_tx = Ok(());
        let mut r_inp = Ok(());
        let mut r_out = Ok(());
        let mut r_wit = Ok(());

        rayon::scope(|s| {
            if let Some(b) = blk_b {
                let out = &mut r_blk;
                s.spawn(move |_| *out = blocks_w.write(&b).map_err(SinkError::from));
            }
            if let Some(b) = tx_b {
                let out = &mut r_tx;
                s.spawn(move |_| *out = txs_w.write(&b).map_err(SinkError::from));
            }
            if let Some(b) = inp_b {
                let out = &mut r_inp;
                s.spawn(move |_| *out = inputs_w.write(&b).map_err(SinkError::from));
            }
            if let Some(b) = out_b {
                let out = &mut r_out;
                s.spawn(move |_| *out = outputs_w.write(&b).map_err(SinkError::from));
            }
            if let (Some(b), Some(w)) = (wit_b, witnesses_w.as_mut()) {
                let out = &mut r_wit;
                s.spawn(move |_| *out = w.write(&b).map_err(SinkError::from));
            }
        });
        r_blk?;
        r_tx?;
        r_inp?;
        r_out?;
        r_wit
    }

    /// Flush any partial batches and close all entity files concurrently across
    /// Rayon workers so Zstd page/footer finalization overlaps across all
    /// tables.
    pub fn finish(mut self) -> Result<(), SinkError> {
        let blk_b = (self.blocks.buffered > 0)
            .then(|| self.blocks.take_batch())
            .transpose()?;
        let tx_b = (self.txs.buffered > 0)
            .then(|| self.txs.take_batch())
            .transpose()?;
        let inp_b = (self.inputs.buffered > 0)
            .then(|| self.inputs.take_batch())
            .transpose()?;
        let out_b = (self.outputs.buffered > 0)
            .then(|| self.outputs.take_batch())
            .transpose()?;
        let wit_b = (self.witnesses_w.is_some() && self.witnesses.buffered > 0)
            .then(|| self.witnesses.take_batch())
            .transpose()?;

        let blocks_w = self.blocks_w;
        let txs_w = self.txs_w;
        let inputs_w = self.inputs_w;
        let outputs_w = self.outputs_w;
        let witnesses_w = self.witnesses_w;

        let mut r_blk = Ok(());
        let mut r_tx = Ok(());
        let mut r_inp = Ok(());
        let mut r_out = Ok(());
        let mut r_wit = Ok(());

        rayon::scope(|s| {
            let o_blk = &mut r_blk;
            s.spawn(move |_| *o_blk = finish_entity(blk_b, blocks_w));
            let o_tx = &mut r_tx;
            s.spawn(move |_| *o_tx = finish_entity(tx_b, txs_w));
            let o_inp = &mut r_inp;
            s.spawn(move |_| *o_inp = finish_entity(inp_b, inputs_w));
            let o_out = &mut r_out;
            s.spawn(move |_| *o_out = finish_entity(out_b, outputs_w));
            if let Some(w) = witnesses_w {
                let o_wit = &mut r_wit;
                s.spawn(move |_| *o_wit = finish_entity(wit_b, w));
            }
        });
        r_blk?;
        r_tx?;
        r_inp?;
        r_out?;
        r_wit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, UInt32Array, UInt64Array};
    use arrow::datatypes::DataType;
    use bidx_core::{BlockRow, InputRow, OutputRow, TxRow};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("bidx-parquet-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn block_row(h: u32) -> BlockRow {
        BlockRow {
            height: h,
            hash: [h as u8; 32],
            prev_hash: [0u8; 32],
            merkle_root: [0xCDu8; 32],
            time: 1_600_000_000 + h,
            bits: 0x1d00ffff,
            nonce: h,
            version: 1,
            tx_count: 1,
            size: 100,
            weight: 400,
            total_fee: 0,
            coinbase_value: 50 * h as u64,
        }
    }

    fn tx_row(h: u32, i: u16) -> TxRow {
        TxRow {
            height: h,
            tx_index: i,
            txid: [i as u8; 32],
            version: 1,
            locktime: 0,
            size: 10,
            weight: 40,
            fee: 0,
            input_count: 1,
            output_count: 1,
            has_witness: false,
        }
    }

    fn input_row(h: u32, i: u16) -> InputRow {
        InputRow {
            height: h,
            tx_index: i,
            input_index: 0,
            prev_txid: [0xEEu8; 32],
            prev_vout: 0,
            script_sig_len: 0,
            sequence: 0xffff_ffff,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase: false,
            spent_value_sat: 5_000,
            spent_script_type: 1,
            spent_address_hash: [0xCDu8; 32],
            spent_height: h,
        }
    }

    fn output_row(h: u32, i: u16) -> OutputRow {
        OutputRow {
            height: h,
            tx_index: i,
            output_index: 0,
            value_sat: 100 + h as u64,
            script_pubkey_len: 25,
            script_type: 1,
            address_hash: [0xABu8; 32],
        }
    }

    fn read_first_batch(path: PathBuf) -> arrow::record_batch::RecordBatch {
        let f = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        let mut reader = builder.build().unwrap();
        reader.next().unwrap().unwrap()
    }

    #[test]
    fn sink_flushes_full_batches_and_roundtrips_through_parquet() {
        // batch_size = 2 so pushing two of each entity exercises the
        // "buffered >= batch_size" flush path for every entity. The remaining
        // `finish()` branches see an empty buffer.
        let dir = tmp_dir("flush");
        let mut sink = ParquetSink::create(
            &dir,
            0,
            SinkConfig {
                batch_size: 2,
                ..Default::default()
            },
        )
        .unwrap();
        for h in 0..2u32 {
            sink.push_block(&block_row(h)).unwrap();
            sink.push_tx(&tx_row(h, 0)).unwrap();
            sink.push_input(&input_row(h, 0)).unwrap();
            sink.push_output(&output_row(h, 0)).unwrap();
        }
        sink.finish().unwrap();

        // Read the inputs table back and assert the enriched spend values
        // survived the columnar write → zstd → read round-trip.
        let batch = read_first_batch(dir.join("inputs/part-00000.parquet"));
        assert_eq!(batch.num_rows(), 2);
        let height = batch
            .column_by_name("height")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(height.value(0), 0);
        assert_eq!(height.value(1), 1);
        let spent_value = batch
            .column_by_name("spent_value_sat")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(spent_value.value(0), 5_000);
        let prev_txid = batch
            .column_by_name("prev_txid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(prev_txid.value(0), &[0xEEu8; 32]);

        // Outputs table too, to confirm the second entity's flush path wrote a
        // correct value.
        let obatch = read_first_batch(dir.join("outputs/part-00000.parquet"));
        assert_eq!(obatch.num_rows(), 2);
        let value = obatch
            .column_by_name("value_sat")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(value.value(0), 100);
        assert_eq!(value.value(1), 101);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Part files are written through a `BufWriter`, so a part with more than
    /// one row group exercises the buffer being flushed across a row-group
    /// boundary *and* at close. If the buffer were dropped without flushing —
    /// the failure mode this guards — the reader would see truncated data or a
    /// corrupt footer rather than the full row set.
    #[test]
    fn bufwriter_part_survives_multiple_row_groups() {
        let dir = tmp_dir("bufw");
        let mut sink = ParquetSink::create(
            &dir,
            0,
            SinkConfig {
                batch_size: 2,
                ..Default::default()
            },
        )
        .unwrap();
        // 5 rows at batch_size 2 → row groups of 2, 2, 1: the partial final
        // group forces a flush at `finish()` on top of the two full ones.
        for h in 0..5u32 {
            sink.push_block(&block_row(h)).unwrap();
        }
        sink.finish().unwrap();

        let f = File::open(dir.join("blocks/part-00000.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        let reader = builder.build().unwrap();
        let mut heights = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch
                .column_by_name("height")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                heights.push(col.value(i));
            }
        }
        assert_eq!(
            heights,
            vec![0, 1, 2, 3, 4],
            "every row must survive the buffered write"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sink_finish_flushes_partial_batches_for_all_entities() {
        // batch_size larger than the row counts, so no push triggers a flush and
        // `finish()` must flush all four partial buffers.
        let dir = tmp_dir("partial");
        let mut sink = ParquetSink::create(
            &dir,
            7,
            SinkConfig {
                batch_size: 100,
                ..Default::default()
            },
        )
        .unwrap();
        sink.push_block(&block_row(3)).unwrap();
        sink.push_tx(&tx_row(3, 0)).unwrap();
        sink.push_input(&input_row(3, 0)).unwrap();
        sink.push_output(&output_row(3, 0)).unwrap();
        sink.finish().unwrap();

        for entity in ["blocks", "transactions", "inputs", "outputs"] {
            let batch = read_first_batch(dir.join(format!("{entity}/part-00007.parquet")));
            assert_eq!(batch.num_rows(), 1, "{entity} should have one row");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sink_finish_with_no_rows_writes_empty_files() {
        // No rows at all: every `finish()` branch takes the "buffered == 0" side
        // and the writers still close cleanly, producing a readable Parquet file
        // with zero row groups (an empty file has no record batch to read, but
        // its metadata must be present).
        let dir = tmp_dir("empty");
        let sink = ParquetSink::create(
            &dir,
            0,
            SinkConfig {
                batch_size: 100,
                ..Default::default()
            },
        )
        .unwrap();
        sink.finish().unwrap();
        let path = dir.join("blocks/part-00000.parquet");
        assert!(path.exists(), "empty sink must still create the file");
        let f = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn push_block_rows_flushes_single_and_multi_entity_batches_in_parallel() {
        let dir = tmp_dir("push_block_rows");
        let mut sink = ParquetSink::create(
            &dir,
            0,
            SinkConfig {
                batch_size: 2,
                capture_witness: true,
            },
        )
        .unwrap();

        let wit = |h: u32, idx: u16| WitnessRow {
            height: h,
            tx_index: idx,
            input_index: 0,
            item_index: 0,
            item_len: 2,
            item_bytes: vec![0xAA, 0x55],
        };

        // Call 1 (`batch_size = 2`): 0 entities reach `batch_size` (`ready == 0`),
        // except `inputs` has 2 rows (`ready == 1`, single-entity fast path).
        sink.push_block_rows(
            &block_row(0),
            &[tx_row(0, 0)],
            &[input_row(0, 0), input_row(0, 1)],
            &[output_row(0, 0)],
            &[wit(0, 0)],
        )
        .unwrap();

        // Call 2: `blocks`, `txs`, `inputs`, `outputs`, and `witnesses`
        // ALL reach `batch_size = 2` (`ready >= 2` parallel Rayon flush), AND
        // 3 txs / 3 outputs / 3 witnesses overflow into a second loop
        // iteration (`!rem.is_empty()`), plus a 3rd call exercises single-ready
        // arms for the remaining entities.
        sink.push_block_rows(
            &block_row(1),
            &[tx_row(1, 0), tx_row(1, 1), tx_row(1, 2)],
            &[input_row(1, 0), input_row(1, 1)],
            &[output_row(1, 0), output_row(1, 1), output_row(1, 2)],
            &[wit(1, 0), wit(1, 1), wit(1, 2)],
        )
        .unwrap();

        // Exercise each single-entity `ready == 1` branch individually on a
        // fresh sink with `batch_size = 2`.
        sink.finish().unwrap();

        let dir_single = tmp_dir("push_single_ready");
        let mut s2 = ParquetSink::create(
            &dir_single,
            0,
            SinkConfig {
                batch_size: 2,
                capture_witness: true,
            },
        )
        .unwrap();
        // `blocks` alone hits 2 (`ready == 1` on blocks).
        s2.push_block_rows(&block_row(0), &[], &[], &[], &[])
            .unwrap();
        s2.push_block_rows(&block_row(1), &[], &[], &[], &[])
            .unwrap();
        // `txs` alone hits 2 (`ready == 1` on txs).
        s2.push_block_rows(&block_row(2), &[tx_row(2, 0), tx_row(2, 1)], &[], &[], &[])
            .unwrap();
        s2.push_block_rows(&block_row(3), &[], &[], &[], &[])
            .unwrap(); // flushes blocks (2) -> blocks buffered = 0
        s2.push_block_rows(
            &block_row(4),
            &[],
            &[],
            &[output_row(4, 0), output_row(4, 1)],
            &[],
        )
        .unwrap(); // blocks=1, outputs=2 -> `ready == 1` on outputs!
        s2.push_block_rows(&block_row(5), &[], &[], &[], &[])
            .unwrap(); // blocks=2 -> flushes blocks -> blocks=0
        s2.push_block_rows(&block_row(6), &[], &[], &[], &[wit(6, 0), wit(6, 1)])
            .unwrap(); // blocks=1, witnesses=2 -> `ready == 1` on witnesses!
                       // Also exercise `capture_witness = false` branch inside `push_block_rows`.
        s2.finish().unwrap();

        let dir_nowit = tmp_dir("push_nowit");
        let mut s3 = ParquetSink::create(
            &dir_nowit,
            0,
            SinkConfig {
                batch_size: 4,
                capture_witness: false,
            },
        )
        .unwrap();
        s3.push_block_rows(&block_row(0), &[tx_row(0, 0)], &[], &[], &[wit(0, 0)])
            .unwrap();
        s3.finish().unwrap();

        assert_eq!(
            read_first_batch(dir.join("blocks/part-00000.parquet")).num_rows(),
            2
        );
        for entity in ["transactions", "inputs", "outputs", "witnesses"] {
            assert_eq!(
                read_first_batch(dir.join(entity).join("part-00000.parquet")).num_rows(),
                4,
                "expected 4 total rows persisted for {entity}"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir_single);
        let _ = std::fs::remove_dir_all(&dir_nowit);
    }

    #[test]
    fn writer_props_use_zstd_selective_dictionary_and_row_group_targets() {
        // Pin the Parquet tuning so a change to compression, dictionary policy,
        // or row-group size is caught here rather than only showing up as a
        // throughput/size regression.
        let props = writer_props(SinkConfig::default().batch_size);
        assert!(matches!(
            props.compression(&ColumnPath::from("height")),
            Compression::ZSTD(_)
        ));
        // Repeated columns get dictionary encoding; random 32B digests do not.
        assert!(props.dictionary_enabled(&ColumnPath::from("height")));
        assert!(props.dictionary_enabled(&ColumnPath::from("script_type")));
        assert!(props.dictionary_enabled(&ColumnPath::from("address_hash")));
        assert!(props.dictionary_enabled(&ColumnPath::from("spent_address_hash")));
        assert!(!props.dictionary_enabled(&ColumnPath::from("txid")));
        assert!(!props.dictionary_enabled(&ColumnPath::from("prev_txid")));
        assert!(!props.dictionary_enabled(&ColumnPath::from("hash")));
        assert_eq!(props.max_row_group_size(), 250_000);
        assert_eq!(writer_props(0).max_row_group_size(), 1);
    }

    #[test]
    fn sink_config_default_batch_size_is_250k() {
        // The default row-group target is part of the on-disk contract (it
        // drives row-group sizes and therefore compression); pin it.
        assert_eq!(SinkConfig::default().batch_size, 250_000);
    }

    #[test]
    fn all_schemas_are_non_empty_and_named() {
        for (name, schema) in [
            ("blocks", blocks_schema()),
            ("transactions", transactions_schema()),
            ("inputs", inputs_schema()),
            ("outputs", outputs_schema()),
            ("witnesses", witnesses_schema()),
        ] {
            assert!(!schema.fields().is_empty(), "{name} schema is empty");
            // Every hash column must be FixedSizeBinary(32), never a string.
            for f in schema.fields() {
                if f.name().contains("hash")
                    || f.name().contains("txid")
                    || f.name() == "merkle_root"
                {
                    assert_eq!(
                        *f.data_type(),
                        DataType::FixedSizeBinary(32),
                        "{name}.{} must be FixedSizeBinary(32)",
                        f.name()
                    );
                }
            }
        }
    }

    #[test]
    fn witnesses_schema_uses_variable_length_binary_payload() {
        let s = witnesses_schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "height",
                "tx_index",
                "input_index",
                "item_index",
                "item_len",
                "item_bytes"
            ]
        );
        let item = s.field_with_name("item_bytes").unwrap();
        // The payload is raw stack bytes, not a fixed-size hash.
        assert_eq!(*item.data_type(), DataType::Binary);
        assert_eq!(
            *s.field_with_name("item_len").unwrap().data_type(),
            DataType::UInt32
        );
    }

    #[test]
    fn witness_table_is_absent_unless_capture_is_enabled() {
        // Default sink: push_witness is a no-op and no witnesses/ dir is made.
        let dir = tmp_dir("no-witness");
        let mut sink = ParquetSink::create(&dir, 0, SinkConfig::default()).unwrap();
        let row = WitnessRow {
            height: 1,
            tx_index: 0,
            input_index: 0,
            item_index: 0,
            item_len: 2,
            item_bytes: vec![0xAA, 0xBB],
        };
        sink.push_witness(&row).unwrap();
        sink.finish().unwrap();
        assert!(
            !dir.join("witnesses").exists(),
            "witnesses dir must not exist when capture is off"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn witness_capture_roundtrips_payload_bytes_through_parquet() {
        // batch_size=1 forces the in-loop flush path for the witness writer,
        // then a second row exercises finish()'s partial flush.
        let dir = tmp_dir("witness");
        let mut sink = ParquetSink::create(
            &dir,
            3,
            SinkConfig {
                batch_size: 1,
                capture_witness: true,
            },
        )
        .unwrap();
        sink.push_witness(&WitnessRow {
            height: 5,
            tx_index: 0,
            input_index: 0,
            item_index: 0,
            item_len: 3,
            item_bytes: vec![0xDE, 0xAD, 0xBE],
        })
        .unwrap();
        sink.push_witness(&WitnessRow {
            height: 5,
            tx_index: 1,
            input_index: 2,
            item_index: 1,
            item_len: 0,
            item_bytes: Vec::new(),
        })
        .unwrap();
        sink.finish().unwrap();

        let batch = read_first_batch(dir.join("witnesses/part-00003.parquet"));
        assert_eq!(batch.num_rows(), 2);
        let item = batch
            .column_by_name("item_bytes")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(item.value(0), &[0xDE, 0xAD, 0xBE]);
        assert_eq!(item.value(1), b"");
        let idx = batch
            .column_by_name("input_index")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(idx.value(1), 2);
        let item_len = batch
            .column_by_name("item_len")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(item_len.value(0), 3);
        assert_eq!(item_len.value(1), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn witness_sink_with_capture_and_no_rows_still_writes_empty_file() {
        // capture_witness=true but no rows pushed: the finish() witness branch
        // takes the buffered == 0 side and the file is still created/closed.
        let dir = tmp_dir("witness-empty");
        let sink = ParquetSink::create(
            &dir,
            0,
            SinkConfig {
                batch_size: 10,
                capture_witness: true,
            },
        )
        .unwrap();
        sink.finish().unwrap();
        let path = dir.join("witnesses/part-00000.parquet");
        assert!(path.exists());
        let f = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn row_groups_flush_at_configured_batch_size() {
        // Guard against `ArrowWriter` buffering multiple `RecordBatch`es in
        // memory up to a larger `max_row_group_size`: pushing 4 rows with
        // `batch_size = 2` must emit 2 distinct row groups of 2 rows each.
        let dir = tmp_dir("row-group-bound");
        let mut sink = ParquetSink::create(
            &dir,
            0,
            SinkConfig {
                batch_size: 2,
                capture_witness: false,
            },
        )
        .unwrap();
        for h in 0..4u32 {
            sink.push_block(&block_row(h)).unwrap();
        }
        sink.finish().unwrap();

        let f = File::open(dir.join("blocks/part-00000.parquet")).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 2);
        assert_eq!(builder.metadata().row_group(0).num_rows(), 2);
        assert_eq!(builder.metadata().row_group(1).num_rows(), 2);
        // Verify SHA-256d digest columns are stored UNCOMPRESSED while scalar
        // columns stay ZSTD-compressed.
        let rg0 = builder.metadata().row_group(0);
        assert_eq!(rg0.column(1).compression(), Compression::UNCOMPRESSED); // hash
        assert!(matches!(rg0.column(0).compression(), Compression::ZSTD(_))); // height
        let _ = std::fs::remove_dir_all(&dir);
    }
}
