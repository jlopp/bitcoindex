//! Storage layer: RocksDB UTXO store (for spend linking + fees) and
//! Parquet intermediate writers (for ClickHouse bulk load).

pub mod parquet_writers;
pub mod utxo;

pub use parquet_writers::{ParquetSink, SinkConfig};
pub use utxo::{BlockUndo, SpendResult, TipState, UtxoError, UtxoStore, UtxoValue};
