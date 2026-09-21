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
///
/// Supports Bitcoin Core v28+ **blocks XOR obfuscation**: if `xor.dat` is
/// present next to the blk file, the payload is XOR-decoded at open time
/// into an owned buffer (one bulk decode per file). Otherwise we keep the
/// zero-copy mmap view.
///
/// Bitcoin Core preallocates blk files to 128 MiB with trailing zero
/// padding; we trim at open time so iterators see "no more records" as
/// `None` rather than a fake corrupted record parsed out of the padding.
pub struct BlkFile {
    pub file_id: u32,
    pub path: PathBuf,
    /// Raw mmap of the original file (full file, including trailing
    /// preallocated zero padding when Bitcoin Core preallocated).
    mmap: Mmap,
    /// XOR-obfuscated payload, decoded at open. `None` when file is plain.
    decoded: Option<Vec<u8>>,
    /// End of meaningful data, i.e., the file length minus trailing
    /// preallocated zero padding. Slice with `data()`.
    data_end: usize,
}

impl BlkFile {
    /// Open the blk file following Bitcoin Core's blk*.dat convention.
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
        let xor_key = read_xor_key(path);
        // Trim trailing preallocated zero padding against the RAW view —
        // after XOR decode those zeros turn into the key pattern and are
        // indistinguishable from real data.
        let data_end = last_raw_nonzero(&mmap);
        let decoded = match &xor_key {
            Some(key) => {
                let mut decoded = xor_decode(&mmap, key);
                decoded.truncate(data_end);
                Some(decoded)
            }
            None => None,
        };
        Ok(BlkFile {
            file_id,
            path: path.to_path_buf(),
            mmap,
            decoded,
            data_end,
        })
    }

    /// Return the slice of meaningful (post-trim) bytes.
    #[inline]
    pub fn data(&self) -> &[u8] {
        match &self.decoded {
            Some(v) => v.as_slice(),
            None => &self.mmap[..self.data_end],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data().is_empty()
    }

    /// Iterate all block records in file order (NOT chain order).
    pub fn records(&self) -> BlkRecordIter<'_> {
        BlkRecordIter {
            file_id: self.file_id,
            data: self.data(),
            pos: 0,
        }
    }

    /// Read the full payload (header + txs) for a known location.
    /// Returns bytes starting at the 80-byte header.
    pub fn block_bytes(&self, loc: &BlockLocation) -> Result<&[u8], BlkError> {
        let start = loc.data_offset() as usize;
        let len = loc.record_len as usize - 8;
        let end = start + len;
        let buf = self.data();
        if end > buf.len() {
            return Err(BlkError::Corrupt {
                file_id: self.file_id,
                offset: loc.offset,
                reason: format!("record overruns file: end {end}, file len {}", buf.len()),
            });
        }
        Ok(&buf[start..end])
    }
}

/// Find the highest offset in the RAW (pre-decode) file that isn't zero.
/// Bitcoin Core preallocates blk files with trailing zero padding; XOR
/// decode turns raw zeros into the key pattern, so we must trim against
/// the raw view BEFORE decoding, then decode the trimmed region.
fn last_raw_nonzero(raw: &[u8]) -> usize {
    raw.iter().rposition(|&b| b != 0).map(|i| i + 1).unwrap_or(0)
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
    fn last_raw_nonzero_trims_trailing_zeros_only() {
        assert_eq!(last_raw_nonzero(&[]), 0);
        assert_eq!(last_raw_nonzero(&[0u8]), 0);
        assert_eq!(last_raw_nonzero(&[1u8]), 1);
        assert_eq!(last_raw_nonzero(&[1u8, 2, 0, 0, 0]), 2);
        // Leading zeros must NOT be trimmed — only trailing.
        assert_eq!(last_raw_nonzero(&[0u8, 0, 1]), 3);
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
