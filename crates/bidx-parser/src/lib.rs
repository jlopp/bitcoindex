//! Raw blk*.dat reading, header-pass chain reconstruction, and full block parsing.

pub mod blkfile;
pub mod chain;
pub mod tx;

pub use blkfile::{read_xor_key_from_dir, scan_blk_headers, BlkFile, BlkRecord};
pub use chain::{build_chain_index, ChainBuildStats};
pub use tx::{
    parse_block_full, parse_block_full_into, parse_block_full_opts, FullBlock, ParseOptions,
};
