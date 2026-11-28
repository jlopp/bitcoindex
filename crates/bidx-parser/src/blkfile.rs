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
    #[error("no block records found in any blk*.dat file under {0}")]
    NoRecords(PathBuf),
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

/// Backing storage for a [`BlkFile`].
enum BlkData {
    /// Zero-copy mmap view for plain `blk*.dat` files, trimmed at `data_end`
    /// and hinted with `POSIX_MADV_SEQUENTIAL` so the kernel prefetches ahead.
    Mmap { mmap: Mmap, data_end: usize },
    /// Owned plaintext payload for XOR-obfuscated `blk*.dat` files, read
    /// sequentially into a single `Vec<u8>` and XOR-decoded in place (avoiding
    /// `mmap` page-fault storms, double-buffering, and `munmap` TLB shootdowns).
    Decoded(Vec<u8>),
}

/// View over a single `blkNNNNN.dat` file.
///
/// If a sibling `xor.dat` holds a non-zero 8-byte key (Bitcoin Core v28+
/// default `-blocksxor=1`), the file is read sequentially into a single buffer,
/// trimmed to the last complete record boundary, and XOR-decoded in place.
/// Otherwise we keep a zero-copy `Mmap` view hinted for sequential readahead.
///
/// Bitcoin Core preallocates blk files to 128 MiB with trailing zero
/// padding; we trim at open time so iterators see "no more records" as
/// `None` rather than a fake corrupted record parsed out of the padding.
pub struct BlkFile {
    pub file_id: u32,
    pub path: PathBuf,
    backing: BlkData,
}

impl BlkFile {
    /// Open the blk file following Bitcoin Core's `blk*.dat` convention,
    /// probing the parent directory's `xor.dat` automatically.
    ///
    /// Hot-path callers that open many files from the same directory should
    /// read the key once via [`read_xor_key_from_dir`] and call
    /// [`BlkFile::open_with_key`] to avoid repeating `xor.dat` syscalls.
    pub fn open(path: &Path, file_id: u32) -> Result<Self, BlkError> {
        Self::open_with_key(path, file_id, read_xor_key(path))
    }

    /// Open the blk file using a pre-resolved optional 8-byte `xor.dat` key.
    pub fn open_with_key(
        path: &Path,
        file_id: u32,
        xor_key: Option<[u8; 8]>,
    ) -> Result<Self, BlkError> {
        Self::open_with_key_hint(path, file_id, xor_key, None)
    }

    /// Open the blk file using a pre-resolved optional `xor.dat` key and an
    /// optional trimmed `known_data_end` byte boundary recorded by the header
    /// pass.
    ///
    /// When `known_data_end` is supplied, both the XOR-decoded reader and the
    /// `Mmap` reader skip walking record headers a second time, and the XOR
    /// reader stops reading at `known_data_end` (skipping trailing preallocated
    /// zero padding on partially filled tip files).
    pub fn open_with_key_hint(
        path: &Path,
        file_id: u32,
        xor_key: Option<[u8; 8]>,
        known_data_end: Option<u64>,
    ) -> Result<Self, BlkError> {
        let backing = match xor_key {
            Some(key) => BlkData::Decoded(read_xor_decoded_streaming(path, &key, known_data_end)?),
            None => {
                let file = File::open(path).map_err(|source| BlkError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
                // SAFETY: we never mutate through the map; caller must ensure
                // the file isn't concurrently truncated (fine for a stopped or
                // quiet node, or a copied snapshot).
                // coverage-exempt: the mmap only fails on an OS-level fault
                // (out of address space, or file truncated between open and
                // map) that cannot be forced portably; `File::open` above
                // covers missing/unreadable paths.
                let mmap = unsafe { Mmap::map(&file) }.map_err(|source| BlkError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
                #[cfg(unix)]
                let _ = mmap.advise(memmap2::Advice::Sequential);
                let data_end = match known_data_end {
                    Some(end) if end > 0 && (end as usize) <= mmap.len() => end as usize,
                    _ => record_walk_end_decoded(&mmap, mmap.first().is_some_and(|&b| b != 0)),
                };
                BlkData::Mmap { mmap, data_end }
            }
        };
        Ok(BlkFile {
            file_id,
            path: path.to_path_buf(),
            backing,
        })
    }

    /// Return the slice of meaningful (post-trim) bytes.
    #[inline]
    pub fn data(&self) -> &[u8] {
        match &self.backing {
            BlkData::Decoded(v) => v.as_slice(),
            BlkData::Mmap { mmap, data_end } => &mmap[..*data_end],
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

    /// Validate `loc` against this file's trimmed length and return the
    /// `(start, end)` byte range of its block payload (starting at the 80-byte
    /// header). Used by zero-copy callers that hold `Arc<BlkFile>` alongside
    /// the slice bounds instead of copying each block into a separate buffer.
    #[inline]
    pub fn block_range(&self, loc: &BlockLocation) -> Result<(usize, usize), BlkError> {
        let start = loc.data_offset() as usize;
        let buf_len = self.len();
        let end = (loc.record_len as usize)
            .checked_sub(8)
            .and_then(|len| start.checked_add(len));
        match end {
            Some(end) if end <= buf_len => Ok((start, end)),
            _ => Err(BlkError::Corrupt {
                file_id: self.file_id,
                offset: loc.offset,
                reason: format!(
                    "record overruns file: offset {}, record_len {}, file len {buf_len}",
                    loc.offset, loc.record_len
                ),
            }),
        }
    }

    /// Read the full payload (header + txs) for a known location.
    /// Returns bytes starting at the 80-byte header.
    #[inline]
    pub fn block_bytes(&self, loc: &BlockLocation) -> Result<&[u8], BlkError> {
        let (start, end) = self.block_range(loc)?;
        Ok(&self.data()[start..end])
    }
}

fn open_with_len(path: &Path) -> std::io::Result<(File, u64)> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    Ok((file, len))
}

#[cfg(unix)]
#[inline]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(not(unix))]
fn read_exact_at(mut file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

/// Scan only the 88-byte record prefixes (`magic[4] + len[4] + header[80]`) of
/// `path`, skipping block transaction payloads via positional `pread`s.
///
/// Matches [`BlkFile::open_with_key`] + [`BlkFile::records`] semantics on
/// preallocated zero padding, partial trailing records, XOR obfuscation, and
/// corrupt initial records without reading, faulting, or XOR-decoding the
/// multi-megabyte transaction bodies (~680 GiB on mainnet vs ~79 MiB of
/// record headers).
pub fn scan_blk_headers(
    path: &Path,
    file_id: u32,
    xor_key: Option<[u8; 8]>,
    mut on_record: impl FnMut(BlkRecord),
) -> Result<u64, BlkError> {
    let (file, file_len) = open_with_len(path).map_err(|source| BlkError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let mut pos = 0u64;
    let mut raw = [0u8; 8 + BLOCK_HEADER_LEN];
    while pos + 8 <= file_len {
        let rem = file_len - pos;
        let want = rem.min((8 + BLOCK_HEADER_LEN) as u64) as usize;
        // coverage-exempt: `pos + want <= file_len` holds by construction, so
        // `read_exact_at` only fails on an OS-level mid-scan I/O fault or
        // concurrent external file truncation (missing/unreadable files are
        // covered via `open_with_len` above).
        read_exact_at(&file, &mut raw[..want], pos).map_err(|source| BlkError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        let raw_first_zero = raw[0] == 0;
        if let Some(k) = &xor_key {
            let base = (pos as usize) & 7;
            for (i, b) in raw[..want].iter_mut().enumerate() {
                *b ^= k[(base + i) & 7];
            }
        }

        let magic = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        let len = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as u64;
        let bad_magic = !is_known_network_magic(magic);
        let bad_len = len < BLOCK_HEADER_LEN as u64 || len > rem - 8;
        if bad_magic || bad_len {
            // At offset 0, distinguish preallocated zero padding (raw zeros on
            // disk or XOR-streamed zeros that decode to `magic == 0`) from a
            // genuinely malformed file so a wrong `--blocks-dir` still surfaces
            // "bad magic" / "bad record len". After at least one valid record
            // (`pos > 0`), any non-magic or truncated tail is preallocated
            // padding or an incomplete write and ends the scan cleanly.
            if pos == 0 && !raw_first_zero && magic != 0 {
                let reason = if bad_magic {
                    format!("bad magic 0x{magic:08x}")
                } else {
                    format!("bad record len {len}")
                };
                return Err(BlkError::Corrupt {
                    file_id,
                    offset: 0,
                    reason,
                });
            }
            break;
        }

        let mut header_raw = [0u8; BLOCK_HEADER_LEN];
        header_raw.copy_from_slice(&raw[8..8 + BLOCK_HEADER_LEN]);
        let header = BlockHeader::parse(&header_raw);
        let hash = header.hash(&header_raw);
        let location = BlockLocation {
            file_id,
            offset: pos,
            record_len: (len + 8) as u32,
        };
        on_record(BlkRecord {
            location,
            header,
            hash,
            header_raw,
        });
        pos += 8 + len;
    }
    Ok(pos)
}

/// Chunk size (64 KiB) for streaming L1/L2-cache-resident read + XOR decode.
const READ_XOR_CHUNK_BYTES: usize = 64 * 1024;

/// Stream-read `path` and XOR-decode each 64 KiB chunk while it is hot in
/// L1/L2 cache before appending to the destination `Vec<u8>`.
///
/// Compared to `std::fs::read` (128 MiB write to DRAM) followed by a separate
/// `xor_decode_in_place` pass (128 MiB cold-DRAM read + 128 MiB DRAM write),
/// chunked L2-hot decoding halves DRAM memory-bus traffic (~1.36 TiB saved
/// across 5,400 mainnet `blk*.dat` files).
fn read_xor_decoded_streaming(
    path: &Path,
    key: &[u8; 8],
    known_data_end: Option<u64>,
) -> Result<Vec<u8>, BlkError> {
    use std::io::Read;

    let (mut file, file_len) = open_with_len(path).map_err(|source| BlkError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let hinted = known_data_end.filter(|&end| end > 0 && end <= file_len);
    let target_len = hinted.unwrap_or(file_len) as usize;
    let mut buf = Vec::with_capacity(target_len);
    let mut scratch = [0u8; READ_XOR_CHUNK_BYTES];
    let mut pos = 0usize;
    let mut first_raw_nonzero = false;

    while pos < target_len {
        let want = (target_len - pos).min(READ_XOR_CHUNK_BYTES);
        // coverage-exempt: `file` was just opened above; `read` only fails on
        // a mid-stream kernel I/O fault.
        let n = file
            .read(&mut scratch[..want])
            .map_err(|source| BlkError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if n == 0 {
            break; // coverage-exempt: concurrent external truncation between metadata() and read()
        }
        if pos == 0 {
            first_raw_nonzero = scratch[0] != 0;
        }
        xor_decode_at_offset(&mut scratch[..n], key, pos);
        buf.extend_from_slice(&scratch[..n]);
        pos += n;
    }

    if hinted.is_none() {
        let data_end = record_walk_end_decoded(&buf, first_raw_nonzero);
        buf.truncate(data_end);
        // Release preallocated trailing megabytes on partially filled tip files
        // while avoiding realloc copies on ordinary files whose trailing padding
        // is only a few kilobytes.
        if buf.capacity().saturating_sub(buf.len()) >= (1 << 20) {
            buf.shrink_to_fit();
        }
    }
    Ok(buf)
}

/// Find where the meaningful data ends by walking the decoded record framing.
///
/// A blk file is a flat sequence of `magic(4) || len(4) || payload(len)`
/// records, optionally followed by trailing preallocated zero padding.
/// `first_raw_nonzero` captures whether byte 0 on disk (prior to any XOR
/// decode) was non-zero, so pure preallocated padding (`0x00` on disk) trims
/// cleanly to `0` while a genuinely malformed non-zero file is preserved intact
/// so `BlkRecordIter` surfaces `"bad magic"` / `"bad record len"`.
fn record_walk_end_decoded(decoded: &[u8], first_raw_nonzero: bool) -> usize {
    let mut pos = 0usize;
    let mut end = 0usize;
    // A record needs at least magic(4) + len(4) + header(80).
    while pos + 8 + BLOCK_HEADER_LEN <= decoded.len() {
        let magic = u32::from_le_bytes([
            decoded[pos],
            decoded[pos + 1],
            decoded[pos + 2],
            decoded[pos + 3],
        ]);
        if !is_known_network_magic(magic) {
            break;
        }
        let len = u32::from_le_bytes([
            decoded[pos + 4],
            decoded[pos + 5],
            decoded[pos + 6],
            decoded[pos + 7],
        ]) as usize;
        // `len` is attacker-controlled in principle; bound it before adding.
        if len < BLOCK_HEADER_LEN || len > decoded.len() - pos - 8 {
            break;
        }
        pos += 8 + len;
        end = pos;
    }

    if end == 0 && !decoded.is_empty() && first_raw_nonzero {
        return decoded.len();
    }
    end
}

/// Read the 8-byte XOR key from `<blocks_dir>/xor.dat`, if present and
/// well-formed (8 bytes, not all-zero).
///
/// Callers that open or scan many `blk*.dat` files in the same directory
/// should call this once and pass the result to [`BlkFile::open_with_key`] or
/// [`scan_blk_headers`] instead of re-reading `xor.dat` per file.
pub fn read_xor_key_from_dir(blocks_dir: &Path) -> Option<[u8; 8]> {
    let raw = std::fs::read(blocks_dir.join("xor.dat")).ok()?;
    let k: [u8; 8] = raw.try_into().ok()?;
    // An all-zero key would be a no-op; treat as "no obfuscation".
    (k != [0u8; 8]).then_some(k)
}

/// Read the 8-byte XOR key from `xor.dat` in the same directory as `blk_path`.
#[inline]
fn read_xor_key(blk_path: &Path) -> Option<[u8; 8]> {
    blk_path.parent().and_then(read_xor_key_from_dir)
}

/// XOR-decode `buf` in place starting at `file_offset` against an 8-byte
/// repeating key.
///
/// Rotates the 64-bit key word by `(file_offset & 7) * 8` bits so arbitrary
/// `read()` chunk alignments still vectorize into wide SIMD `u64` XORs in L1/L2
/// cache without per-byte modulo indexing.
#[inline]
fn xor_decode_at_offset(buf: &mut [u8], key: &[u8; 8], file_offset: usize) {
    let shift = file_offset & 7;
    let k64 = u64::from_le_bytes(*key).rotate_right((shift * 8) as u32);
    let (chunks, rem) = buf.as_chunks_mut::<8>();
    for c in chunks {
        *c = (u64::from_le_bytes(*c) ^ k64).to_le_bytes();
    }
    for (i, b) in rem.iter_mut().enumerate() {
        *b ^= key[(shift + i) & 7];
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
        // coverage-exempt: unreachable given `data()` — `record_walk_end`
        // already trims trailing padding from the view this iterator walks, and
        // no record starts with a zero byte (every network magic's first byte is
        // non-zero), so a zero is never seen at a record boundary. Kept so the
        // iterator stays correct if `data()` ever returns an untrimmed view.
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
        // coverage-exempt: untriggerable in a test — a `read_dir` entry only
        // errors on a mid-iteration filesystem fault (e.g. the directory removed
        // underneath us), which cannot be forced portably. Kept so a fault
        // surfaces as a path-carrying Io error rather than being silently
        // skipped.
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

    fn xor_decode_in_place(buf: &mut [u8], key: &[u8; 8]) {
        xor_decode_at_offset(buf, key, 0);
    }

    fn record_walk_end(raw: &[u8], key: Option<&[u8; 8]>) -> usize {
        let first_raw_nonzero = raw.first().is_some_and(|&b| b != 0);
        match key {
            Some(k) => {
                let mut decoded = raw.to_vec();
                xor_decode_at_offset(&mut decoded, k, 0);
                record_walk_end_decoded(&decoded, first_raw_nonzero)
            }
            None => record_walk_end_decoded(raw, first_raw_nonzero),
        }
    }

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
        let mut buf: Vec<u8> = plaintext
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        xor_decode_in_place(&mut buf, &key);
        assert_eq!(buf, plaintext);
        // Empty input → empty output (no panics on as_chunks_mut::<8>()).
        let mut empty: Vec<u8> = Vec::new();
        xor_decode_in_place(&mut empty, &key);
        assert!(empty.is_empty());
        // Length not divisible by 8 exercises the remainder path.
        let tiny = vec![0xAAu8; 5];
        let mut xored: Vec<u8> = tiny
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        xor_decode_in_place(&mut xored, &key);
        assert_eq!(xored, tiny);
    }

    /// Build `n` synthetic records of `payload_len` bytes each, ending in a
    /// zero byte when `zero_tail` is set (the case that used to break the
    /// last-non-zero trim).
    fn synth_records(n: usize, payload_len: usize, zero_tail: bool) -> Vec<u8> {
        let mut data = Vec::new();
        for i in 0..n {
            data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
            data.extend_from_slice(&(payload_len as u32).to_le_bytes());
            let mut payload = vec![0xABu8; payload_len];
            // Last byte zero only on the final record, when requested.
            if zero_tail && i == n - 1 {
                let last = payload.len() - 1;
                payload[last] = 0;
            }
            data.extend_from_slice(&payload);
        }
        data
    }

    #[test]
    fn record_walk_end_handles_zero_padding() {
        let data = synth_records(3, 100, false);
        // 3 records then 128 bytes of preallocated zero padding.
        let mut padded = data.clone();
        padded.extend_from_slice(&[0u8; 128]);
        assert_eq!(record_walk_end(&padded, None), data.len());
    }

    /// The bug this replaced: a final record that ends in zero bytes must not
    /// be truncated. This is the testnet4 `blk00000.dat` case — the file ended
    /// exactly at EOF with a tx whose locktime was 0, and the old
    /// last-non-zero trim ate four real bytes.
    #[test]
    fn record_walk_end_keeps_final_record_ending_in_zeros() {
        let data = synth_records(3, 100, true);
        assert_eq!(*data.last().unwrap(), 0, "fixture must end in a zero byte");
        assert_eq!(record_walk_end(&data, None), data.len());
    }

    /// A partially written trailing record (declared length runs past EOF)
    /// must be excluded, and the end must land on the previous boundary.
    #[test]
    fn record_walk_end_excludes_partial_trailing_record() {
        let mut data = synth_records(2, 100, false);
        let complete = data.len();
        // Start a third record but cut it short.
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&500u32.to_le_bytes());
        data.extend_from_slice(&[0xCDu8; 40]);
        assert_eq!(record_walk_end(&data, None), complete);
    }

    /// Non-magic garbage ends the walk immediately.
    #[test]
    fn record_walk_end_stops_on_non_magic() {
        let mut data = synth_records(1, 100, false);
        let complete = data.len();
        data.extend_from_slice(&[0xDEu8; 64]);
        assert_eq!(record_walk_end(&data, None), complete);
        assert_eq!(record_walk_end(&[], None), 0);
        assert_eq!(record_walk_end(&[0u8; 16], None), 0);
    }

    /// A declared length below the header size is malformed; the walk must not
    /// land mid-record on it, and (like the other malformed cases) hands the
    /// file back intact so the iterator reports the real error.
    #[test]
    fn record_walk_end_preserves_short_length_for_diagnosis() {
        let mut data = Vec::new();
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(&[0u8; 200]);
        assert_eq!(record_walk_end(&data, None), data.len());
    }

    /// A file that starts with something other than a known magic is handed
    /// back intact rather than trimmed to nothing, so the iterator can report
    /// the real "bad magic" instead of the run silently seeing zero blocks.
    #[test]
    fn record_walk_end_preserves_malformed_file_for_diagnosis() {
        let mut data = Vec::new();
        data.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // unknown magic
        data.extend_from_slice(&80u32.to_le_bytes());
        data.extend_from_slice(&[1u8; 80]);
        assert_eq!(record_walk_end(&data, None), data.len());
    }

    /// A non-zero declared length that overruns the file is likewise kept so
    /// the iterator reports "bad record len" rather than reporting no blocks.
    #[test]
    fn record_walk_end_preserves_overrun_length_for_diagnosis() {
        let mut data = Vec::new();
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&u32::MAX.to_le_bytes());
        data.extend_from_slice(&[1u8; 80]);
        assert_eq!(record_walk_end(&data, None), data.len());
    }

    /// Under XOR obfuscation the raw trailing padding is a non-zero key
    /// pattern, so only the walk can find the end. Lengths and magic must be
    /// read through the key.
    #[test]
    fn record_walk_end_works_under_xor_with_padding() {
        let key = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let plain = synth_records(3, 100, true);
        let mut padded = plain.clone();
        padded.extend_from_slice(&[0u8; 256]); // padding; decodes to key pattern
        let obfuscated: Vec<u8> = padded
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        // The raw tail is NOT zero, so a last-non-zero trim would keep it all.
        assert_ne!(*obfuscated.last().unwrap(), 0);
        assert_eq!(record_walk_end(&obfuscated, Some(&key)), plain.len());
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

    /// Build a synthetic blk file in a temp dir containing a sequence of
    /// valid records (the genesis block header, then a trailing zero region).
    /// Returns the tmp path + blk id so callers can clean up.
    fn make_blk_dir(records: usize, file_id: u32) -> (PathBuf, PathBuf) {
        let tmp =
            std::env::temp_dir().join(format!("bidx-blk-test-{}-{}", std::process::id(), file_id));
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
            // Ensure the record does not END in a zero byte. `BlkFile::open`
            // trims trailing zero padding against the raw bytes, so a final
            // record whose last byte is 0 would be truncated and then fail to
            // parse as a "bad record len". Real block data always ends in a
            // non-zero locktime/nonce byte, so mirror that here.
            hdr[BLOCK_HEADER_LEN - 1] = 0x01;
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
                RecCheck {
                    loc: r.location,
                    header: r.header,
                    hash: r.hash,
                }
            })
            .collect();
        assert_eq!(
            records.len(),
            3,
            "iterator should yield 3 blocks then stop at padding"
        );
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.loc.file_id, 0);
            assert_eq!(r.loc.offset, (i * 88) as u64);
            assert_eq!(r.loc.record_len, 88);
            assert_eq!(r.header.version, 1);
            let mut want = [0u8; 32];
            for j in 0..32 {
                want[j] = (i * 32 + j) as u8;
            }
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
        assert!(matches!(
            discover_blk_files(&empty),
            Err(BlkError::NoFiles(_))
        ));
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
            let tmp = std::env::temp_dir().join(format!(
                "bidx-blk-magic-{}-{:08x}",
                std::process::id(),
                magic
            ));
            let _ = fs::remove_dir_all(&tmp);
            std::fs::create_dir_all(&tmp).unwrap();
            let path = tmp.join("blk00000.dat");
            let mut hdr = [0u8; 80];
            hdr[0] = 1;
            // Non-zero final byte so `BlkFile::open`'s trailing-zero trim
            // cannot swallow the record (see make_blk_dir).
            hdr[79] = 0x01;
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

    #[test]
    fn open_errors_when_path_does_not_exist() {
        // A missing blk file must surface as an Io error carrying the path,
        // not a panic.
        let missing = std::env::temp_dir().join(format!("bidx-blk-missing-{}", std::process::id()));
        let _ = std::fs::remove_file(&missing);
        match BlkFile::open(&missing, 0) {
            Err(BlkError::Io { path, .. }) => assert_eq!(path, missing),
            Err(e) => panic!("expected Io error, got {e:?}"),
            Ok(_) => panic!("expected Io error, got Ok"),
        }
    }

    #[test]
    fn len_and_is_empty_follow_the_trimmed_view() {
        let (tmp, path) = make_blk_dir(2, 9);
        let bf = BlkFile::open(&path, 9).unwrap();
        // Two 88-byte records; the 128 bytes of padding are trimmed.
        assert_eq!(bf.len(), 176);
        assert!(!bf.is_empty());
        let _ = std::fs::remove_dir_all(&tmp);

        // A pure-padding file trims to nothing.
        let empty = std::env::temp_dir().join(format!("bidx-blk-len-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&empty);
        std::fs::create_dir_all(&empty).unwrap();
        let p = empty.join("blk00000.dat");
        std::fs::write(&p, [0u8; 64]).unwrap();
        let bf = BlkFile::open(&p, 0).unwrap();
        assert_eq!(bf.len(), 0);
        assert!(bf.is_empty());
        let _ = std::fs::remove_dir_all(&empty);
    }

    #[test]
    fn iterator_returns_none_for_a_trailing_partial_header() {
        // A file ending in a lone magic + length but fewer than 8 trailing
        // bytes after the last full record: the iterator must stop cleanly
        // (None) rather than erroring or looping.
        let mut data = Vec::new();
        data.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        data.extend_from_slice(&80u32.to_le_bytes());
        let mut hdr = [0u8; 80];
        hdr[79] = 1;
        data.extend_from_slice(&hdr);
        // Append 3 bytes (a truncated next-record prefix).
        data.extend_from_slice(&[0x01, 0x02, 0x03]);

        let tmp = std::env::temp_dir().join(format!("bidx-blk-partial-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let path = tmp.join("blk00000.dat");
        std::fs::write(&path, &data).unwrap();
        let bf = BlkFile::open(&path, 0).unwrap();
        let mut it = bf.records();
        assert!(it.next().unwrap().is_ok(), "first record should parse");
        assert!(it.next().is_none(), "trailing 3 bytes must end iteration");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn discover_blk_files_errors_on_a_missing_directory() {
        let missing = std::env::temp_dir().join(format!("bidx-blk-nodir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&missing);
        match discover_blk_files(&missing) {
            Err(BlkError::Io { path, .. }) => assert_eq!(path, missing),
            Err(e) => panic!("expected Io error, got {e:?}"),
            Ok(_) => panic!("expected Io error, got Ok"),
        }
    }

    #[test]
    fn xor_decode_handles_non_multiple_of_eight_lengths() {
        // `as_chunks_mut::<8>()` leaves a remainder; the tail must be XORed
        // with the correct keystream offset, so a round-trip of a 13-byte
        // buffer must be exact.
        let key = [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let plain: Vec<u8> = (0..13u8).collect();
        let mut enc: Vec<u8> = plain
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        xor_decode_in_place(&mut enc, &key);
        assert_eq!(enc, plain);
        // Empty input round-trips to empty.
        let mut empty: Vec<u8> = Vec::new();
        xor_decode_in_place(&mut empty, &key);
        assert!(empty.is_empty());
    }

    #[test]
    fn record_walk_end_reads_lengths_through_the_xor_key() {
        // Under obfuscation both the magic AND the length word are encoded;
        // a walk that forgets to decode the length would stop after one record.
        let key = [0x9Au8, 0x1B, 0x2C, 0x3D, 0x4E, 0x5F, 0x60, 0x71];
        let plain = synth_records(4, 80, false);
        let obf: Vec<u8> = plain
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        assert_eq!(record_walk_end(&obf, Some(&key)), plain.len());
    }

    #[test]
    fn xor_obfuscated_file_is_decoded_on_open() {
        // `-blocksxor=1` files: the whole file is XORed with the 8-byte key in
        // xor.dat. `BlkFile::open` must detect xor.dat, decode, trim to the
        // record boundary, and hand `data()` back as plaintext records.
        use std::fs;
        let tmp = std::env::temp_dir().join(format!("bidx-blk-xor-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let key = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        fs::write(tmp.join("xor.dat"), key).unwrap();

        // One record whose payload ends non-zero so nothing is trimmed.
        let mut plain = Vec::new();
        plain.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        plain.extend_from_slice(&80u32.to_le_bytes());
        let mut hdr = [0u8; 80];
        hdr[0] = 1;
        hdr[79] = 1;
        plain.extend_from_slice(&hdr);
        let obf: Vec<u8> = plain
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        let path = tmp.join("blk00000.dat");
        fs::write(&path, &obf).unwrap();

        let bf = BlkFile::open(&path, 0).unwrap();
        assert_eq!(bf.data(), plain.as_slice());
        let rec = bf.records().next().unwrap().unwrap();
        assert_eq!(rec.header.version, 1);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn xor_obfuscated_file_with_trailing_padding_is_trimmed() {
        // Padding under XOR is a non-zero key pattern, so the "last non-zero
        // byte" heuristic cannot work; only the record walk can. The decoded
        // view must stop exactly at the last real record.
        use std::fs;
        let tmp = std::env::temp_dir().join(format!("bidx-blk-xorp-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let key = [0x0Au8, 0x1B, 0x2C, 0x3D, 0x4E, 0x5F, 0x60, 0x71];
        fs::write(tmp.join("xor.dat"), key).unwrap();

        let plain = synth_records(2, 80, false);
        let mut padded = plain.clone();
        padded.extend_from_slice(&[0u8; 256]); // decodes to a non-zero pattern
        let obf: Vec<u8> = padded
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        let path = tmp.join("blk00000.dat");
        fs::write(&path, &obf).unwrap();

        let bf = BlkFile::open(&path, 0).unwrap();
        assert!(
            matches!(bf.backing, BlkData::Decoded(_)),
            "XOR-obfuscated blk file must drop Mmap and hold only Decoded"
        );
        assert_eq!(bf.data(), plain.as_slice());
        let count = bf.records().filter(|r| r.is_ok()).count();
        assert_eq!(count, 2);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn open_with_key_shrinks_large_preallocated_padding_and_reports_io_error() {
        use std::fs;
        let tmp = std::env::temp_dir().join(format!("bidx-blk-shrink-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let key = [0x21u8, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87, 0x98];
        fs::write(tmp.join("xor.dat"), key).unwrap();
        assert_eq!(read_xor_key_from_dir(&tmp), Some(key));

        // 1.5 MiB of preallocated trailing padding triggers `shrink_to_fit()`
        // (>= 1 MiB unused tail) so partially filled tip files release excess
        // capacity.
        let plain = synth_records(2, 100, false);
        let mut padded = plain.clone();
        padded.resize(plain.len() + (3 << 19), 0);
        let obf: Vec<u8> = padded
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i & 7])
            .collect();
        let path = tmp.join("blk00000.dat");
        fs::write(&path, &obf).unwrap();

        let bf = BlkFile::open_with_key(&path, 0, Some(key)).unwrap();
        assert_eq!(bf.data(), plain.as_slice());
        match &bf.backing {
            BlkData::Decoded(v) => assert!(
                v.capacity() < (1 << 20),
                "trimmed buffer must release >1 MiB preallocated tail, got cap {}",
                v.capacity()
            ),
            BlkData::Mmap { .. } => panic!("expected Decoded backing"),
        }

        // Missing path under Some(key) surfaces Io carrying the path.
        let missing = tmp.join("blk99999.dat");
        match BlkFile::open_with_key(&missing, 99999, Some(key)) {
            Err(BlkError::Io { path: p, .. }) => assert_eq!(p, missing),
            Err(e) => panic!("expected Io error, got {e:?}"),
            Ok(_) => panic!("expected Io error, got Ok"),
        }

        // `block_range` rejects underflow (`record_len < 8`) without panicking.
        let bad_underflow = BlockLocation {
            file_id: 0,
            offset: 0,
            record_len: 4,
        };
        assert!(matches!(
            bf.block_range(&bad_underflow),
            Err(BlkError::Corrupt { .. })
        ));

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn scan_blk_headers_matches_full_open_across_plain_xor_padded_and_corrupt() {
        use std::fs;
        let tmp = std::env::temp_dir().join(format!("bidx-scan-hdrs-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        // Non-8-aligned payload length (113 bytes -> record size 121) ensures
        // subsequent record offsets land on non-zero `pos & 7` XOR alignments.
        let plain = synth_records(5, 113, true);
        let mut padded = plain.clone();
        padded.extend_from_slice(&[0u8; 512]);
        let plain_path = tmp.join("blk00000.dat");
        fs::write(&plain_path, &padded).unwrap();

        let bf = BlkFile::open_with_key(&plain_path, 0, None).unwrap();
        let expected: Vec<(u32, u64, u32, bidx_core::Hash32)> = bf
            .records()
            .map(|r| {
                let r = r.unwrap();
                (
                    r.location.file_id,
                    r.location.offset,
                    r.location.record_len,
                    r.hash,
                )
            })
            .collect();

        let mut scanned = Vec::new();
        scan_blk_headers(&plain_path, 0, None, |r| {
            scanned.push((
                r.location.file_id,
                r.location.offset,
                r.location.record_len,
                r.hash,
            ));
        })
        .unwrap();
        assert_eq!(scanned, expected);

        // Same records under XOR obfuscation + XOR-streamed padding.
        let key = [0xAAu8, 0x55, 0xF0, 0x0F, 0x12, 0x34, 0x56, 0x78];
        let obf: Vec<u8> = padded
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i & 7])
            .collect();
        let xor_path = tmp.join("blk00001.dat");
        fs::write(&xor_path, &obf).unwrap();

        let mut scanned_xor = Vec::new();
        scan_blk_headers(&xor_path, 1, Some(key), |r| {
            scanned_xor.push((
                0, // normalize file_id to compare against file 0
                r.location.offset,
                r.location.record_len,
                r.hash,
            ));
        })
        .unwrap();
        assert_eq!(scanned_xor, expected);

        // Trailing <8-byte stub after valid record stops cleanly.
        let mut stub = synth_records(1, 80, false);
        stub.extend_from_slice(&[0xDE, 0xAD, 0xBE]);
        let stub_path = tmp.join("blk00002.dat");
        fs::write(&stub_path, &stub).unwrap();
        let mut count = 0usize;
        scan_blk_headers(&stub_path, 2, None, |_| count += 1).unwrap();
        assert_eq!(count, 1);

        // Empty file (<8 bytes) and pure zero padding yield 0 records cleanly,
        // both plain and XOR-streamed.
        let zero_path = tmp.join("blk00003.dat");
        fs::write(&zero_path, [0u8; 64]).unwrap();
        count = 0;
        scan_blk_headers(&zero_path, 3, None, |_| count += 1).unwrap();
        assert_eq!(count, 0);
        let xor_zero: Vec<u8> = (0..64).map(|i| key[i & 7]).collect();
        fs::write(&zero_path, &xor_zero).unwrap();
        scan_blk_headers(&zero_path, 3, Some(key), |_| count += 1).unwrap();
        assert_eq!(count, 0);

        // Corrupt magic at offset 0 returns Corrupt("bad magic ...").
        let bad_magic_path = tmp.join("blk00004.dat");
        let mut bad_magic = Vec::new();
        bad_magic.extend_from_slice(&0xDEADBEEFu32.to_le_bytes());
        bad_magic.extend_from_slice(&80u32.to_le_bytes());
        bad_magic.extend_from_slice(&[1u8; 80]);
        fs::write(&bad_magic_path, &bad_magic).unwrap();
        match scan_blk_headers(&bad_magic_path, 4, None, |_| {}) {
            Err(BlkError::Corrupt { reason, .. }) => assert!(reason.contains("bad magic")),
            other => panic!("expected Corrupt bad magic, got {other:?}"),
        }

        // Corrupt record len at offset 0 returns Corrupt("bad record len ...").
        let bad_len_path = tmp.join("blk00005.dat");
        let mut bad_len = Vec::new();
        bad_len.extend_from_slice(&bidx_core::BLOCK_MAGIC.to_le_bytes());
        bad_len.extend_from_slice(&12u32.to_le_bytes());
        bad_len.extend_from_slice(&[1u8; 80]);
        fs::write(&bad_len_path, &bad_len).unwrap();
        match scan_blk_headers(&bad_len_path, 5, None, |_| {}) {
            Err(BlkError::Corrupt { reason, .. }) => assert!(reason.contains("bad record len")),
            other => panic!("expected Corrupt bad record len, got {other:?}"),
        }

        // Missing file returns Io carrying the path.
        let missing = tmp.join("blk99998.dat");
        match scan_blk_headers(&missing, 99998, None, |_| {}) {
            Err(BlkError::Io { path: p, .. }) => assert_eq!(p, missing),
            other => panic!("expected Io error, got {other:?}"),
        }

        // open_with_key_hint exercises both plain Mmap and multi-chunk (>64 KiB)
        // XOR-decoded streaming with and without `known_data_end`, plus unaligned
        // `xor_decode_at_offset` offsets.
        let big_plain = synth_records(3, 30_000, false); // > 90 KiB across 2x 64 KiB chunks
        let valid_end = big_plain.len() as u64;
        let mut big_padded = big_plain.clone();
        big_padded.resize(big_plain.len() + 4096, 0);
        let plain_hint_path = tmp.join("blk00006.dat");
        fs::write(&plain_hint_path, &big_padded).unwrap();
        let bf_plain_hint =
            BlkFile::open_with_key_hint(&plain_hint_path, 6, None, Some(valid_end)).unwrap();
        assert_eq!(bf_plain_hint.data(), big_plain.as_slice());
        // Out-of-bounds hint falls back to record_walk_end_decoded cleanly.
        let bf_plain_over =
            BlkFile::open_with_key_hint(&plain_hint_path, 6, None, Some(u64::MAX)).unwrap();
        assert_eq!(bf_plain_over.data(), big_plain.as_slice());

        let big_obf: Vec<u8> = big_padded
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i & 7])
            .collect();
        let xor_hint_path = tmp.join("blk00007.dat");
        fs::write(&xor_hint_path, &big_obf).unwrap();
        let end_from_scan = scan_blk_headers(&xor_hint_path, 7, Some(key), |_| {}).unwrap();
        assert_eq!(end_from_scan, valid_end);
        let bf_xor_hint =
            BlkFile::open_with_key_hint(&xor_hint_path, 7, Some(key), Some(valid_end)).unwrap();
        assert_eq!(bf_xor_hint.data(), big_plain.as_slice());
        let bf_xor_unhinted =
            BlkFile::open_with_key_hint(&xor_hint_path, 7, Some(key), None).unwrap();
        assert_eq!(bf_xor_unhinted.data(), big_plain.as_slice());

        // Verify xor_decode_at_offset for every misalignment 0..8 and non-8 tail.
        for shift in 0..8usize {
            let orig: Vec<u8> = (0..23u8).map(|x| x.wrapping_mul(7)).collect();
            let mut encoded: Vec<u8> = orig
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ key[(shift + i) & 7])
                .collect();
            xor_decode_at_offset(&mut encoded, &key, shift);
            assert_eq!(encoded, orig, "shift={shift}");
        }

        let _ = fs::remove_dir_all(&tmp);
    }
}
