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
#[derive(Debug)]
pub struct BlkRecord {
    pub location: BlockLocation,
    pub header: BlockHeader,
    pub hash: bidx_core::Hash32,
    pub header_raw: [u8; BLOCK_HEADER_LEN],
}

/// Memory-mapped view over a single blkNNNNN.dat file.
pub struct BlkFile {
    pub file_id: u32,
    pub path: PathBuf,
    mmap: Mmap,
}

impl BlkFile {
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
        Ok(BlkFile {
            file_id,
            path: path.to_path_buf(),
            mmap,
        })
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.mmap
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    /// Iterate all block records in file order (NOT chain order).
    pub fn records(&self) -> BlkRecordIter<'_> {
        BlkRecordIter {
            file_id: self.file_id,
            data: &self.mmap,
            pos: 0,
        }
    }

    /// Read the full payload (header + txs) for a known location.
    /// Returns bytes starting at the 80-byte header.
    pub fn block_bytes(&self, loc: &BlockLocation) -> Result<&[u8], BlkError> {
        let start = loc.data_offset() as usize;
        let len = loc.record_len as usize - 8;
        let end = start + len;
        if end > self.mmap.len() {
            return Err(BlkError::Corrupt {
                file_id: self.file_id,
                offset: loc.offset,
                reason: format!("record overruns file: end {end}, file len {}", self.mmap.len()),
            });
        }
        Ok(&self.mmap[start..end])
    }
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

    /// Build a synthetic blk file in a temp dir containing a sequence of
    /// valid records (the genesis block header, then a trailing zero region).
    /// Returns the tmp path + blk id so callers can clean up.
    fn make_blk_dir(records: usize, file_id: u32) -> (PathBuf, PathBuf) {
        let tmp = std::env::temp_dir().join(format!(
            "bidx-blk-test-{}-{}",
            std::process::id(),
            file_id
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join(format!("blk{:05}.dat", file_id));
        let mut data = Vec::new();
        for i in 0..records {
            // Genesis-ish header: version=1, prev = zero, fake merkle bytes 0..31,
            // time/bits/nonce = arbitrary. Magic prefix = mainnet BLOCK_MAGIC.
            let mut hdr = [0u8; BLOCK_HEADER_LEN];
            hdr[0] = 1;
            // Fill the MERKLE ROOT (bytes 36..68) with distinct bytes per record
            // so the test can verify per-record header bytes parsed correctly.
            for j in 0..32 {
                hdr[36 + j] = (i * 32 + j) as u8;
            }
            let payload_len = 80u32;
            data.extend_from_slice(&BLOCK_MAGIC.to_le_bytes());
            const BLOCK_MAGIC: u32 = bidx_core::BLOCK_MAGIC;
            data.extend_from_slice(&payload_len.to_le_bytes());
            data.extend_from_slice(&hdr);
        }
        // Trailing zero padding so we can validate iterator skips it.
        data.extend_from_slice(&[0u8; 128]);
        std::fs::write(&path, &data).unwrap();
        (tmp, path)
    }

    /// Record for the iterator with all header context we need for assertions.
    #[derive(Debug)]
    struct RecCheck {
        loc: BlockLocation,
        header: BlockHeader,
        hash: bidx_core::Hash32,
    }

    #[test]
    fn records_iter_yields_valid_and_skips_padding() {
        use std::fs;
        let (tmp, path) = make_blk_dir(3, 0);
        let bf = BlkFile::open(&path, 0).unwrap();
        let records: Vec<RecCheck> = bf
            .records()
            .map(|r| {
                let r = r.unwrap();
                RecCheck { loc: r.location, header: r.header, hash: r.hash }
            })
            .collect();
        assert_eq!(records.len(), 3, "iterator should yield 3 blocks then stop at padding");
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.loc.file_id, 0);
            assert_eq!(r.loc.offset, (i * 88) as u64);
            assert_eq!(r.loc.record_len, 88);
            assert_eq!(r.header.version, 1);
            let mut want = [0u8; 32];
            for j in 0..32 { want[j] = (i * 32 + j) as u8; }
            assert_eq!(r.header.merkle_root.0, want);
            // Hash = dsha256(header_raw); just assert non-zero and reproducible.
            assert_ne!(r.hash, bidx_core::Hash32::ZERO);
        }
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn block_bytes_by_location() {
        use std::fs;
        let (tmp, path) = make_blk_dir(2, 1);
        let bf = BlkFile::open(&path, 1).unwrap();
        let blk1 = bf.records().next().unwrap().unwrap();
        let bytes = bf.block_bytes(&blk1.location).unwrap();
        assert_eq!(bytes.len(), 80);
        assert_eq!(&bytes[..4], &[1, 0, 0, 0]); // version
        // Downrranging
        let mut bad = blk1.location;
        bad.record_len = u32::MAX;
        let e = bf.block_bytes(&bad);
        assert!(matches!(e, Err(BlkError::Corrupt { .. })));
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn bad_record_len_rejected() {
        use std::fs;
        let (tmp, path) = make_blk_dir(1, 2);
        let mut data = fs::read(&path).unwrap();
        data[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, &data).unwrap();
        let bf = BlkFile::open(&path, 2).unwrap();
        let it = bf.records().next().unwrap();
        assert!(matches!(it, Err(BlkError::Corrupt { .. })), "got {:?}", it);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn bad_magic_rejected_with_network_in_mind() {
        use std::fs;
        let tmp = std::env::temp_dir().join(format!("bidx-blk-badmagic-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("blk00000.dat");
        let mut hdr = [0u8; 80];
        hdr[0] = 1;
        let mut data = Vec::new();
        data.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // unknown magic
        data.extend_from_slice(&80u32.to_le_bytes());
        data.extend_from_slice(&hdr);
        fs::write(&path, &data).unwrap();
        let bf = BlkFile::open(&path, 0).unwrap();
        let it = bf.records().next().unwrap();
        let err = it.err().expect("expected error");
        match err {
            BlkError::Corrupt { reason, .. } => assert!(reason.contains("bad magic")),
            other => panic!("unexpected: {:?}", other),
        }
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn discover_blk_files_sorts_and_rejects_missing() {
        use std::fs;
        let tmp = std::env::temp_dir().join(format!("bidx-blk-disc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        for id in [10, 0, 7] {
            fs::write(tmp.join(format!("blk{:05}.dat", id)), b"").unwrap();
        }
        let got = discover_blk_files(&tmp).unwrap();
        let ids: Vec<u32> = got.into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec![0, 7, 10]);
        let empty = std::env::temp_dir().join(format!("bidx-blk-empty-{}", std::process::id()));
        std::fs::create_dir_all(&empty).unwrap();
        assert!(matches!(discover_blk_files(&empty), Err(BlkError::NoFiles(_))));
        let _ = fs::remove_dir_all(&tmp);
        let _ = fs::remove_dir_all(&empty);
    }

    #[test]
    fn known_network_magics_all_accepted() {
        use bidx_core as C;
        use std::fs;
        for magic in [
            C::BLOCK_MAGIC,
            C::BLOCK_MAGIC_TESTNET3,
            C::BLOCK_MAGIC_TESTNET4,
            C::BLOCK_MAGIC_SIGNET,
            C::BLOCK_MAGIC_REGTEST,
        ] {
            let tmp = std::env::temp_dir()
                .join(format!("bidx-blk-magic-{}-{:08x}", std::process::id(), magic));
            let _ = fs::remove_dir_all(&tmp);
            std::fs::create_dir_all(&tmp).unwrap();
            let path = tmp.join("blk00000.dat");
            let mut hdr = [0u8; 80];
            hdr[0] = 1;
            let mut data = Vec::new();
            data.extend_from_slice(&magic.to_le_bytes());
            data.extend_from_slice(&80u32.to_le_bytes());
            data.extend_from_slice(&hdr);
            fs::write(&path, &data).unwrap();
            let bf = BlkFile::open(&path, 0).unwrap();
            assert!(
                bf.records().next().unwrap().is_ok(),
                "magic {magic:#010x} should be accepted"
            );
            let _ = fs::remove_dir_all(&tmp);
        }
    }
}
