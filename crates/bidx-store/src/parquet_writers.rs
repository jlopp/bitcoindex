//! Buffered Parquet writers for the five entity tables.
//!
//! One writer instance per output part file. Rows are buffered in a
//! type-erased column builder and flushed as a row group when `batch_size`
//! rows have accumulated, so peak memory stays bounded regardless of input
//! size. Hashes are written as FixedSizeBinary(32) in wire order.

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Int32Builder, UInt8Builder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bidx_core::{BlockRow, InputRow, OutputRow, SpendRow, TxRow};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
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
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            batch_size: 250_000,
        }
    }
}

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(1_000_000)
        .set_write_batch_size(8192)
        .build()
}

fn fbin32(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(32), false)
}

// ---- type-erased column builder ----

enum Col {
    U32(UInt32Builder),
    U64(UInt64Builder),
    I32(Int32Builder),
    U8(UInt8Builder),
    Bool(BooleanBuilder),
    Bin32(FixedSizeBinaryBuilder),
}

impl Col {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Col::U32(b) => Arc::new(b.finish()),
            Col::U64(b) => Arc::new(b.finish()),
            Col::I32(b) => Arc::new(b.finish()),
            Col::U8(b) => Arc::new(b.finish()),
            Col::Bool(b) => Arc::new(b.finish()),
            Col::Bin32(b) => Arc::new(b.finish()),
        }
    }
}

macro_rules! push {
    ($cols:expr, $i:expr, $variant:ident, $v:expr) => {{
        match &mut $cols[$i] {
            Col::$variant(b) => b.append_value($v),
            _ => unreachable!("column type mismatch"),
        }
    }};
}

macro_rules! push_bin {
    ($cols:expr, $i:expr, $v:expr) => {{
        match &mut $cols[$i] {
            Col::Bin32(b) => b.append_value($v)?,
            _ => unreachable!("column type mismatch"),
        }
    }};
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
        fbin32("txid"),
        fbin32("prev_txid"),
        Field::new("prev_vout", DataType::UInt32, false),
        Field::new("script_sig_len", DataType::UInt32, false),
        Field::new("sequence", DataType::UInt32, false),
        Field::new("witness_items", DataType::UInt32, false),
        Field::new("witness_bytes", DataType::UInt32, false),
        Field::new("is_coinbase", DataType::Boolean, false),
    ]))
}

pub fn outputs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("output_index", DataType::UInt32, false),
        fbin32("txid"),
        Field::new("value_sat", DataType::UInt64, false),
        Field::new("script_pubkey_len", DataType::UInt32, false),
        Field::new("script_type", DataType::UInt8, false),
        fbin32("address_hash"),
    ]))
}

pub fn spends_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt32, false),
        Field::new("spending_tx_index", DataType::UInt32, false),
        Field::new("spending_input_index", DataType::UInt32, false),
        fbin32("spending_txid"),
        fbin32("spent_txid"),
        Field::new("spent_vout", DataType::UInt32, false),
        Field::new("spent_value_sat", DataType::UInt64, false),
        Field::new("spent_script_type", DataType::UInt8, false),
        fbin32("spent_address_hash"),
        Field::new("spent_height", DataType::UInt32, false),
    ]))
}

// ---- entity table definition: schema + a `push` that appends one row ----

struct EntityTable {
    schema: SchemaRef,
    cols: Vec<Col>,
    buffered: usize,
}

impl EntityTable {
    fn new(schema: SchemaRef) -> Self {
        let cols = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::UInt32 => Col::U32(UInt32Builder::new()),
                DataType::UInt64 => Col::U64(UInt64Builder::new()),
                DataType::Int32 => Col::I32(Int32Builder::new()),
                DataType::UInt8 => Col::U8(UInt8Builder::new()),
                DataType::Boolean => Col::Bool(BooleanBuilder::new()),
                DataType::FixedSizeBinary(w) => Col::Bin32(FixedSizeBinaryBuilder::new(*w)),
                other => panic!("unsupported column type {other:?}"),
            })
            .collect();
        EntityTable {
            schema,
            cols,
            buffered: 0,
        }
    }

    fn take_batch(&mut self) -> Result<RecordBatch, SinkError> {
        let arrays: Vec<ArrayRef> = self.cols.iter_mut().map(|c| c.finish()).collect();
        self.buffered = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// One Parquet file per entity, all sharing a part id.
pub struct ParquetSink {
    cfg: SinkConfig,
    #[allow(dead_code)]
    part: u32,

    blocks_w: ArrowWriter<File>,
    txs_w: ArrowWriter<File>,
    inputs_w: ArrowWriter<File>,
    outputs_w: ArrowWriter<File>,
    spends_w: ArrowWriter<File>,

    blocks: EntityTable,
    txs: EntityTable,
    inputs: EntityTable,
    outputs: EntityTable,
    spends: EntityTable,
}

fn create_writer(dir: &Path, entity: &str, part: u32, schema: SchemaRef) -> Result<ArrowWriter<File>, SinkError> {
    let pdir = dir.join(entity);
    std::fs::create_dir_all(&pdir)?;
    let file = File::create(pdir.join(format!("part-{part:05}.parquet")))?;
    Ok(ArrowWriter::try_new(file, schema, Some(writer_props()))?)
}

impl ParquetSink {
    pub fn create(dir: &Path, part: u32, cfg: SinkConfig) -> Result<Self, SinkError> {
        Ok(ParquetSink {
            cfg,
            part,
            blocks_w: create_writer(dir, "blocks", part, blocks_schema())?,
            txs_w: create_writer(dir, "transactions", part, transactions_schema())?,
            inputs_w: create_writer(dir, "inputs", part, inputs_schema())?,
            outputs_w: create_writer(dir, "outputs", part, outputs_schema())?,
            spends_w: create_writer(dir, "spends", part, spends_schema())?,
            blocks: EntityTable::new(blocks_schema()),
            txs: EntityTable::new(transactions_schema()),
            inputs: EntityTable::new(inputs_schema()),
            outputs: EntityTable::new(outputs_schema()),
            spends: EntityTable::new(spends_schema()),
        })
    }

    pub fn push_block(&mut self, r: &BlockRow) -> Result<(), SinkError> {
        let c = &mut self.blocks.cols;
        push!(c, 0, U32, r.height);
        push_bin!(c, 1, r.hash);
        push_bin!(c, 2, r.prev_hash);
        push_bin!(c, 3, r.merkle_root);
        push!(c, 4, U32, r.time);
        push!(c, 5, U32, r.bits);
        push!(c, 6, U32, r.nonce);
        push!(c, 7, I32, r.version);
        push!(c, 8, U32, r.tx_count);
        push!(c, 9, U32, r.size);
        push!(c, 10, U32, r.weight);
        push!(c, 11, U64, r.total_fee);
        push!(c, 12, U64, r.coinbase_value);
        self.blocks.buffered += 1;
        if self.blocks.buffered >= self.cfg.batch_size {
            let b = self.blocks.take_batch()?;
            self.blocks_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_tx(&mut self, r: &TxRow) -> Result<(), SinkError> {
        let c = &mut self.txs.cols;
        push!(c, 0, U32, r.height);
        push!(c, 1, U32, r.tx_index);
        push_bin!(c, 2, r.txid);
        push!(c, 3, I32, r.version);
        push!(c, 4, U32, r.locktime);
        push!(c, 5, U32, r.size);
        push!(c, 6, U32, r.weight);
        push!(c, 7, U64, r.fee);
        push!(c, 8, U32, r.input_count);
        push!(c, 9, U32, r.output_count);
        push!(c, 10, Bool, r.has_witness);
        self.txs.buffered += 1;
        if self.txs.buffered >= self.cfg.batch_size {
            let b = self.txs.take_batch()?;
            self.txs_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_input(&mut self, r: &InputRow) -> Result<(), SinkError> {
        let c = &mut self.inputs.cols;
        push!(c, 0, U32, r.height);
        push!(c, 1, U32, r.tx_index);
        push!(c, 2, U32, r.input_index);
        push_bin!(c, 3, r.txid);
        push_bin!(c, 4, r.prev_txid);
        push!(c, 5, U32, r.prev_vout);
        push!(c, 6, U32, r.script_sig_len);
        push!(c, 7, U32, r.sequence);
        push!(c, 8, U32, r.witness_items);
        push!(c, 9, U32, r.witness_bytes);
        push!(c, 10, Bool, r.is_coinbase);
        self.inputs.buffered += 1;
        if self.inputs.buffered >= self.cfg.batch_size {
            let b = self.inputs.take_batch()?;
            self.inputs_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_output(&mut self, r: &OutputRow) -> Result<(), SinkError> {
        let c = &mut self.outputs.cols;
        push!(c, 0, U32, r.height);
        push!(c, 1, U32, r.tx_index);
        push!(c, 2, U32, r.output_index);
        push_bin!(c, 3, r.txid);
        push!(c, 4, U64, r.value_sat);
        push!(c, 5, U32, r.script_pubkey_len);
        push!(c, 6, U8, r.script_type);
        push_bin!(c, 7, r.address_hash);
        self.outputs.buffered += 1;
        if self.outputs.buffered >= self.cfg.batch_size {
            let b = self.outputs.take_batch()?;
            self.outputs_w.write(&b)?;
        }
        Ok(())
    }

    pub fn push_spend(&mut self, r: &SpendRow) -> Result<(), SinkError> {
        let c = &mut self.spends.cols;
        push!(c, 0, U32, r.height);
        push!(c, 1, U32, r.spending_tx_index);
        push!(c, 2, U32, r.spending_input_index);
        push_bin!(c, 3, r.spending_txid);
        push_bin!(c, 4, r.spent_txid);
        push!(c, 5, U32, r.spent_vout);
        push!(c, 6, U64, r.spent_value_sat);
        push!(c, 7, U8, r.spent_script_type);
        push_bin!(c, 8, r.spent_address_hash);
        push!(c, 9, U32, r.spent_height);
        self.spends.buffered += 1;
        if self.spends.buffered >= self.cfg.batch_size {
            let b = self.spends.take_batch()?;
            self.spends_w.write(&b)?;
        }
        Ok(())
    }

    /// Flush any partial batches and close all five files.
    pub fn finish(mut self) -> Result<(), SinkError> {
        if self.blocks.buffered > 0 {
            let b = self.blocks.take_batch()?;
            self.blocks_w.write(&b)?;
        }
        if self.txs.buffered > 0 {
            let b = self.txs.take_batch()?;
            self.txs_w.write(&b)?;
        }
        if self.inputs.buffered > 0 {
            let b = self.inputs.take_batch()?;
            self.inputs_w.write(&b)?;
        }
        if self.outputs.buffered > 0 {
            let b = self.outputs.take_batch()?;
            self.outputs_w.write(&b)?;
        }
        if self.spends.buffered > 0 {
            let b = self.spends.take_batch()?;
            self.spends_w.write(&b)?;
        }
        self.blocks_w.close()?;
        self.txs_w.close()?;
        self.inputs_w.close()?;
        self.outputs_w.close()?;
        self.spends_w.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, UInt32Array, UInt64Array};
    use arrow::datatypes::DataType;
    use bidx_core::{BlockRow, InputRow, OutputRow, SpendRow, TxRow};
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

    fn tx_row(h: u32, i: u32) -> TxRow {
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

    fn input_row(h: u32, i: u32) -> InputRow {
        InputRow {
            height: h,
            tx_index: i,
            input_index: 0,
            txid: [i as u8; 32],
            prev_txid: [0xEEu8; 32],
            prev_vout: 0,
            script_sig_len: 0,
            sequence: 0xffff_ffff,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase: false,
        }
    }

    fn output_row(h: u32, i: u32) -> OutputRow {
        OutputRow {
            height: h,
            tx_index: i,
            output_index: 0,
            txid: [i as u8; 32],
            value_sat: 100 + h as u64,
            script_pubkey_len: 25,
            script_type: 1,
            address_hash: [0xABu8; 32],
        }
    }

    fn spend_row(h: u32, i: u32) -> SpendRow {
        SpendRow {
            height: h,
            spending_tx_index: i,
            spending_input_index: 0,
            spending_txid: [i as u8; 32],
            spent_txid: [0xEEu8; 32],
            spent_vout: 0,
            spent_value_sat: 5_000,
            spent_script_type: 1,
            spent_address_hash: [0xCDu8; 32],
            spent_height: h,
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
        // "buffered >= batch_size" flush path for every entity, including the
        // spend path. The remaining `finish()` branches see an empty buffer.
        let dir = tmp_dir("flush");
        let mut sink = ParquetSink::create(&dir, 0, SinkConfig { batch_size: 2 }).unwrap();
        for h in 0..2u32 {
            sink.push_block(&block_row(h)).unwrap();
            sink.push_tx(&tx_row(h, 0)).unwrap();
            sink.push_input(&input_row(h, 0)).unwrap();
            sink.push_output(&output_row(h, 0)).unwrap();
            sink.push_spend(&spend_row(h, 0)).unwrap();
        }
        sink.finish().unwrap();

        // Read the spends table back and assert the values survived the
        // columnar write → zstd → read round-trip.
        let batch = read_first_batch(dir.join("spends/part-00000.parquet"));
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
        let spent_txid = batch
            .column_by_name("spent_txid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(spent_txid.value(0), &[0xEEu8; 32]);

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

    #[test]
    fn sink_finish_flushes_partial_batches_for_all_entities() {
        // batch_size larger than the row counts, so no push triggers a flush and
        // `finish()` must flush all five partial buffers.
        let dir = tmp_dir("partial");
        let mut sink = ParquetSink::create(&dir, 7, SinkConfig { batch_size: 100 }).unwrap();
        sink.push_block(&block_row(3)).unwrap();
        sink.push_tx(&tx_row(3, 0)).unwrap();
        sink.push_input(&input_row(3, 0)).unwrap();
        sink.push_output(&output_row(3, 0)).unwrap();
        sink.push_spend(&spend_row(3, 0)).unwrap();
        sink.finish().unwrap();

        for entity in ["blocks", "transactions", "inputs", "outputs", "spends"] {
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
        let sink = ParquetSink::create(&dir, 0, SinkConfig { batch_size: 100 }).unwrap();
        sink.finish().unwrap();
        let path = dir.join("blocks/part-00000.parquet");
        assert!(path.exists(), "empty sink must still create the file");
        let f = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn entity_table_new_panics_on_unsupported_column_type() {
        // `EntityTable::new` supports a fixed set of Arrow types; anything else
        // (here Utf8) is a programming error and must panic rather than silently
        // building a table it cannot push into.
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let res = std::panic::catch_unwind(move || {
            let _ = EntityTable::new(schema);
        });
        assert!(res.is_err(), "expected a panic for an unsupported column type");
    }

    #[test]
    fn writer_props_use_zstd_and_row_group_targets() {
        // Pin the Parquet tuning so a change to compression or row-group size is
        // caught here rather than only showing up as a size/throughput shift.
        use parquet::schema::types::ColumnPath;
        let props = writer_props();
        assert!(matches!(
            props.compression(&ColumnPath::from("height")),
            Compression::ZSTD(_)
        ));
        assert_eq!(props.max_row_group_size(), 1_000_000);
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
            ("spends", spends_schema()),
        ] {
            assert!(!schema.fields().is_empty(), "{name} schema is empty");
            // Every hash column must be FixedSizeBinary(32), never a string.
            for f in schema.fields() {
                if f.name().contains("hash") || f.name().contains("txid") || f.name() == "merkle_root" {
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
}
