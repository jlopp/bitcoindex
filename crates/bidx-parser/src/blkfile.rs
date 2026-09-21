use bidx_core::{is_known_network_magic, BlockHeader, BlockLocation, BLOCK_HEADER_LEN};
use memmap2::Mmap;
use std::fs::File;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BlkError {
    #[error("io error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("no blk*.dat files found in {0}")]
    NoFiles(PathBuf),
    #[error("corrupt record in blk{file_id:05}.dat at offset {offset}: {reason}")]
    Corrupt {
        file_id: u32,
        offset: u64,
        reason: String,
    },
}

/// One block record as found in a blk file: 8-byte prefix + payload.
pub struct BlkRecord {
    pub location: BlockLocation,
    pub header: BlockHeader,
    pub hash: bidx_core::Hash32,
    pub header_raw: [u8; BLOCK_HEADER_LEN],
}

/// Memory-mapped view over a single blkNNNNN.dat file.
///
/// Supports Bitcoin Core v28+ **blocks XOR obfuscation**: if `xor.dat` is
/// present next to the blk file, the payload is XOR-decoded at open time
/// into an owned buffer (one bulk decode per file). Otherwise we keep the
/// zero-copy mmap view.
pub struct BlkFile {
    pub file_id: u32,
    pub path: PathBuf,
    data: Vec<u8>,
}

impl BlkFile {
    /// Open the blk file, applying XOR de-obfuscation if `xor.dat` is
    /// present beside it (Bitcoin Core v28).
    pub fn open(path: &Path, file_id: u32) -> Result<Self, BlkError> {
        let file = File::open(path).map_err(|e| BlkError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        // SAFETY: we never mutate through the map; caller must ensure the
        // file isn't concurrently truncated (fine for a stopped/quiet node,
        // or a copied snapshot).
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| BlkError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let data = match read_xor_key(path) {
            Some(key) => xor_decode(&mmap, &key),
            None => mmap.to_vec(),
        };
        Ok(BlkFile {
            file_id,
            path: path.to_path_buf(),
            data,
        })
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate all block records in file order (NOT chain order).
    pub fn records(&self) -> BlkRecordIter<'_> {
        BlkRecordIter {
            file_id: self.file_id,
            data: &self.data,
            pos: 0,
        }
    }

    /// Read the full payload (header + txs) for a known location.
    /// Returns bytes starting at the 80-byte header.
    pub fn block_bytes(&self, loc: &BlockLocation) -> Result<&[u8], BlkError> {
        let start = loc.data_offset() as usize;
        let len = loc.record_len as usize - 8;
        let end = start + len;
        if end > self.data.len() {
            return Err(BlkError::Corrupt {
                file_id: self.file_id,
                offset: loc.offset,
                reason: format!("record overruns file: end {end}, file len {}", self.data.len()),
            });
        }
        Ok(&self.data[start..end])
    }
}

/// Read the 8-byte XOR key from `xor.dat` in the same directory as
/// `blk_path`, if present and well-formed.
fn read_xor_key(blk_path: &Path) -> Option<[u8; 8]> {
    let dir = blk_path.parent()?;
    let p = dir.join("xor.dat");
    let raw = std::fs::read(&p).ok()?;
    if raw.len() != 8 {
        return None;
    }
    let mut k = [0u8; 8];
    k.copy_from_slice(&raw);
    // An all-zero key would be a no-op; treat as "no obfuscation".
    if k == [0u8; 8] {
        None
    } else {
        Some(k)
    }
}

/// XOR-decode `buf` against an 8-byte repeating key.
/// Perf note: 8 bytes at a time using a u64 keeps this ~memory-bandwidth
/// bound, on par with the mmap page-in cost the caller is replacing.
fn xor_decode(buf: &[u8], key: &[u8; 8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(buf.len());
    let mut key8 = [0u8; 8];
    key8.copy_from_slice(key);
    let k64 = u64::from_le_bytes(key8);
    let mut chunks = buf.chunks_exact(8);
    for c in &mut chunks {
        let v = u64::from_le_bytes(c.try_into().unwrap());
        out.extend_from_slice(&(v ^ k64).to_le_bytes());
    }
    let rem = chunks.remainder();
    let start = buf.len() - rem.len();
    for (i, b) in rem.iter().enumerate() {
        out.push(b ^ key8[(start + i) % 8]);
    }
    out
}

pub struct BlkRecordIter<'a> {
    file_id: u32,
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for BlkRecordIter<'a> {
    type Item = Result<BlkRecord, BlkError>;

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.data;
        // Skip trailing zero padding at end of file (preallocated space).
        while self.pos < data.len() && data[self.pos] == 0 {
            self.pos += 1;
        }
        if self.pos + 8 > data.len() {
            return None;
        }
        let offset = self.pos as u64;
        let magic = u32::from_le_bytes(data[self.pos..self.pos + 4].try_into().unwrap());
        let len = u32::from_le_bytes(data[self.pos + 4..self.pos + 8].try_into().unwrap()) as usize;

        // Accept any of the known Bitcoin network magics — the indexer
        // itself doesn't validate consensus, so parsing testnet/signet/regtest
        // files for benchmarking and development is a supported path.
        if !is_known_network_magic(magic) {
            return Some(Err(BlkError::Corrupt {
                file_id: self.file_id,
                offset,
                reason: format!("bad magic 0x{magic:08x}"),
            }));
        }
        if len < BLOCK_HEADER_LEN || self.pos + 8 + len > data.len() {
            return Some(Err(BlkError::Corrupt {
                file_id: self.file_id,
                offset,
                reason: format!("bad record len {len}"),
            }));
        }

        let payload = &data[self.pos + 8..self.pos + 8 + len];
        let mut header_raw = [0u8; BLOCK_HEADER_LEN];
        header_raw.copy_from_slice(&payload[..BLOCK_HEADER_LEN]);
        let header = BlockHeader::parse(&header_raw);
        let hash = header.hash(&header_raw);

        let location = BlockLocation {
            file_id: self.file_id,
            offset,
            record_len: (len + 8) as u32,
        };
        self.pos += 8 + len;

        Some(Ok(BlkRecord {
            location,
            header,
            hash,
            header_raw,
        }))
    }
}

/// Discover blkNNNNN.dat files in a blocks directory, sorted by id.
pub fn discover_blk_files(blocks_dir: &Path) -> Result<Vec<(u32, PathBuf)>, BlkError> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(blocks_dir).map_err(|e| BlkError::Io {
        path: blocks_dir.to_path_buf(),
        source: e,
    })?;
    for entry in rd {
        let entry = entry.map_err(|e| BlkError::Io {
            path: blocks_dir.to_path_buf(),
            source: e,
        })?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = parse_blk_name(&name) {
            out.push((id, entry.path()));
        }
    }
    out.sort_unstable_by_key(|(id, _)| *id);
    if out.is_empty() {
        return Err(BlkError::NoFiles(blocks_dir.to_path_buf()));
    }
    Ok(out)
}

fn parse_blk_name(name: &str) -> Option<u32> {
    let stem = name.strip_prefix("blk")?.strip_suffix(".dat")?;
    if stem.len() != 5 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_blk_names() {
        assert_eq!(parse_blk_name("blk00000.dat"), Some(0));
        assert_eq!(parse_blk_name("blk04269.dat"), Some(4269));
        assert_eq!(parse_blk_name("blk0000.dat"), None);
        assert_eq!(parse_blk_name("blk00000.dat.bak"), None);
        assert_eq!(parse_blk_name("rev00000.dat"), None);
    }

    #[test]
    fn xor_decode_roundtrip_and_partial_tail() {
        let key: [u8; 8] = [0x99, 0x73, 0xa9, 0x54, 0x4c, 0x10, 0x97, 0x0c];
        let plaintext: Vec<u8> = (0..100u8).collect();
        let obfuscated: Vec<u8> = plaintext
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        let back = xor_decode(&obfuscated, &key);
        assert_eq!(back, plaintext);
        // Empty input → empty output (no panics on chunks_exact(8)=0).
        assert!(xor_decode(&[], &key).is_empty());
        // Length not divisible by 8 exercises the remainder path.
        let tiny = vec![0xAAu8; 5];
        let xored: Vec<u8> = tiny.iter().enumerate().map(|(i, b)| b ^ key[i % 8]).collect();
        let back = xor_decode(&xored, &key);
        assert_eq!(back, tiny);
    }

    #[test]
    fn read_xor_key_treats_all_zero_as_none() {
        let dir = std::env::temp_dir().join(format!("bidx-xor-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let blk = dir.join("blk00000.dat");
        std::fs::write(&blk, b"xyz").unwrap();
        // No xor.dat → None.
        assert_eq!(read_xor_key(&blk), None);
        // Wrong length → None.
        std::fs::write(dir.join("xor.dat"), b"short").unwrap();
        assert_eq!(read_xor_key(&blk), None);
        // All zeros → None (no-op key).
        std::fs::write(dir.join("xor.dat"), [0u8; 8]).unwrap();
        assert_eq!(read_xor_key(&blk), None);
        // Real key → Some.
        let k = [1u8, 2, 3, 4, 5, 6, 7, 8];
        std::fs::write(dir.join("xor.dat"), k).unwrap();
        assert_eq!(read_xor_key(&blk), Some(k));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
