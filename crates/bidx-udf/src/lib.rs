//! ClickHouse executable UDFs for the bitcoindex schema.
//!
//! ClickHouse has no base58/bech32 encoder, so rendering a stored
//! `(script_type, address_hash)` pair as an address string requires a
//! Rust-side codec. The [`bidx-serve`] crate already owns that codec (it is
//! the same one the REST surface and the `address_prefix` backfill use); this
//! crate wraps it as an *executable UDF* so ad-hoc SQL can render addresses
//! without a round trip through the indexer.
//!
//! Two streaming subcommands mirror the two directions:
//!
//! * `address` — reads TabSeparated rows `script_type\thash_hex` on stdin and
//!   writes one rendered address per line (`""` for a type with no address
//!   form, e.g. `OP_RETURN`).
//! * `parse` — reads one address per line on stdin and writes
//!   `script_type\thash_hex` (or `\t` for an address that does not parse on
//!   the configured network).
//!
//! The TabSeparated shape is deliberate: a `FixedString(32)` column would put
//! raw hash bytes (which can include tab/newline) on the wire, so the SQL side
//! passes `hex(address_hash)` and gets a printable 64-char string instead.
//! ClickHouse's own TabSeparated reader/writer handles the framing.
//!
//! The network is a process argument, not a column, so an operator registers
//! one UDF name per chain (see the `udf_config_xml` helper in the
//! `bidx-loader` schema module).

use std::io::{BufRead, Write};

use bidx_core::Network;

/// Errors from a UDF stream operation.
#[derive(Debug, thiserror::Error)]
pub enum UdfError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown subcommand '{0}' (expected 'address' or 'parse')")]
    UnknownMode(String),
}

/// Render one `(script_type, hash_hex)` pair to its canonical address.
///
/// Returns `Ok(None)` when the pair has no address form (`OP_RETURN`, unknown
/// scripts) or when `hash_hex` is not 64 hex characters. A malformed hash is
/// treated as "nothing to render" rather than a hard error so a scan over the
/// whole `outputs` table cannot be aborted by a single odd row.
pub fn render_pair(network: Network, script_type: u8, hash_hex: &str) -> Option<String> {
    let hash = parse_hash32(hash_hex)?;
    bidx_serve::address::render_address(network, script_type, &hash)
}

/// Parse one address string into its stored `(script_type, hash_hex)` form.
///
/// Returns `Ok(None)` for an address that does not parse on `network`.
pub fn parse_addr(network: Network, addr: &str) -> Option<(u8, String)> {
    let (ty, hash) = bidx_serve::address::parse_address(network, addr)?;
    Some((ty, hex_encode(&hash)))
}

/// Decode a 64-char hex string into 32 bytes. `None` on any length or hex
/// error, so callers can treat a bad hash as "unrenderable".
fn parse_hash32(s: &str) -> Option<[u8; 32]> {
    let s = s.trim();
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

/// Lowercase hex encode, matching ClickHouse's `hex()` output order (so a
/// round trip `bidxAddress(t, hex(h))` and `bidxParseAddress(a)` agree).
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// Which direction to stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `script_type\thash_hex` -> `address`.
    Address,
    /// `address` -> `script_type\thash_hex`.
    Parse,
}

impl std::str::FromStr for Mode {
    type Err = UdfError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "address" => Ok(Mode::Address),
            "parse" => Ok(Mode::Parse),
            other => Err(UdfError::UnknownMode(other.to_string())),
        }
    }
}

/// Stream one UDF invocation: read rows from `reader`, write rows to `writer`.
///
/// Blank input lines are skipped (ClickHouse can emit a trailing newline).
/// A line that cannot be processed produces an empty output line rather than
/// aborting the stream, so one bad row does not fail an entire `SELECT`.
pub fn run<W: Write, R: BufRead>(
    mode: Mode,
    network: Network,
    reader: R,
    mut writer: W,
) -> Result<(), UdfError> {
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        match mode {
            Mode::Address => {
                // Two tab-separated columns: script_type, hash_hex.
                let rendered = line
                    .split_once('\t')
                    .and_then(|(ty, hash)| ty.trim().parse::<u8>().ok().map(|t| (t, hash)))
                    .and_then(|(ty, hash)| render_pair(network, ty, hash));
                match rendered {
                    Some(addr) => writeln!(writer, "{addr}")?,
                    None => writeln!(writer)?,
                }
            }
            Mode::Parse => match parse_addr(network, line.trim()) {
                Some((ty, hash)) => writeln!(writer, "{ty}\t{hash}")?,
                None => writeln!(writer, "\t")?,
            },
        }
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// hash160 of the genesis coinbase output, as 64-char hex (right-padded to
    /// 32 bytes, matching the stored `address_hash` layout).
    fn genesis_hash_hex() -> String {
        format!("{}", "62e907b15cbf27d5425399ebf6f0fb50ebb88f18".to_string() + &"0".repeat(24))
    }

    fn run_str(mode: Mode, network: Network, input: &str) -> String {
        let mut out = Vec::new();
        run(mode, network, Cursor::new(input.as_bytes()), &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn address_mode_renders_p2pkh_from_the_genesis_hash() {
        let input = format!("2\t{}\n", genesis_hash_hex());
        let out = run_str(Mode::Address, Network::Mainnet, &input);
        assert_eq!(out, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa\n");
    }

    #[test]
    fn address_mode_renders_bech32_for_segwit() {
        // BIP173 P2WPKH vector.
        let hash = "751e76e8199196d454941c45d1b3a323f1433bd6".to_string() + &"0".repeat(24);
        let out = run_str(Mode::Address, Network::Mainnet, &format!("4\t{hash}\n"));
        assert_eq!(out, "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4\n");
    }

    #[test]
    fn address_mode_network_prefix_is_applied() {
        let hash = "751e76e8199196d454941c45d1b3a323f1433bd6".to_string() + &"0".repeat(24);
        let out = run_str(Mode::Address, Network::Testnet3, &format!("4\t{hash}\n"));
        assert!(out.starts_with("tb1"), "got {out}");
    }

    #[test]
    fn address_mode_emits_blank_for_op_return_and_bad_rows() {
        // OP_RETURN (7) has no address form; a short hash is unrenderable; a
        // non-numeric type is unrenderable. All three produce an empty line,
        // and a blank input line is skipped entirely.
        let input = format!("7\t{}\nabcd\nxx\t{}\n\n2\t{}\n", genesis_hash_hex(), genesis_hash_hex(), genesis_hash_hex());
        let out = run_str(Mode::Address, Network::Mainnet, &input);
        assert_eq!(
            out,
            format!("\n\n\n1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa\n"),
            "three unrenderable rows then the valid one"
        );
    }

    #[test]
    fn address_mode_rejects_non_hex_hash_of_correct_length() {
        let bad = "zz".repeat(32);
        let out = run_str(Mode::Address, Network::Mainnet, &format!("2\t{bad}\n"));
        assert_eq!(out, "\n");
    }

    #[test]
    fn parse_mode_roundtrips_a_base58_address() {
        let out = run_str(Mode::Parse, Network::Mainnet, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa\n");
        assert_eq!(out, format!("2\t{}\n", genesis_hash_hex()));
    }

    #[test]
    fn parse_mode_roundtrips_a_bech32_address() {
        let out = run_str(Mode::Parse, Network::Mainnet, "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4\n");
        let want = format!("4\t{}\n", "751e76e8199196d454941c45d1b3a323f1433bd6".to_string() + &"0".repeat(24));
        assert_eq!(out, want);
    }

    #[test]
    fn parse_mode_emits_empty_pair_for_unparseable_or_wrong_network() {
        // Garbage, an empty-ish line, and a mainnet address on testnet all
        // yield the empty `\t` marker.
        let input = "not-an-address\n1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa\n";
        let out = run_str(Mode::Parse, Network::Testnet3, input);
        assert_eq!(out, "\t\n\t\n");
    }

    #[test]
    fn render_and_parse_roundtrip_through_the_udf() {
        // address -> parse -> address must be stable for every renderable type.
        for (ty, hash) in [
            (2u8, "62e907b15cbf27d5425399ebf6f0fb50ebb88f18".to_string() + &"0".repeat(24)),
            (3u8, "4a5e1e4baab89f3a32518a88c31bc87f618f7667".to_string() + &"0".repeat(24)),
            (4u8, "751e76e8199196d454941c45d1b3a323f1433bd6".to_string() + &"0".repeat(24)),
            (5u8, "1863143c14c5166804bd19203356da136c985678cd4d27a1b8c6329604903262".to_string()),
            (6u8, "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798".to_string()),
        ] {
            let addr = render_pair(Network::Mainnet, ty, &hash).expect("renderable");
            let (back_ty, back_hash) = parse_addr(Network::Mainnet, &addr).expect("parses back");
            assert_eq!(back_ty, ty);
            assert_eq!(back_hash, hash, "hash roundtrip for type {ty}");
        }
    }

    #[test]
    fn mode_from_str_accepts_known_and_rejects_unknown() {
        assert_eq!("address".parse::<Mode>().unwrap(), Mode::Address);
        assert_eq!("parse".parse::<Mode>().unwrap(), Mode::Parse);
        let e = "render".parse::<Mode>().unwrap_err();
        assert!(matches!(&e, UdfError::UnknownMode(m) if m == "render"));
        assert!(e.to_string().contains("unknown subcommand 'render'"));
    }

    #[test]
    fn udf_error_displays_io_source() {
        let e = UdfError::Io(std::io::Error::new(std::io::ErrorKind::Other, "boom"));
        assert!(e.to_string().contains("io"));
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn parse_hash32_accepts_only_exactly_64_hex_chars() {
        assert_eq!(parse_hash32(&"0f".repeat(32)).unwrap()[0], 0x0f);
        assert!(parse_hash32("abcd").is_none());
        assert!(parse_hash32(&"zz".repeat(32)).is_none());
        // Leading/trailing whitespace is trimmed (ClickHouse pads fields).
        assert!(parse_hash32(&format!("  {}  ", "ab".repeat(32))).is_some());
    }
}
