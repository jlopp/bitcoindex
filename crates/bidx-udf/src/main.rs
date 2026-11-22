//! `bidx-udf` — a ClickHouse *executable UDF* front end for the bitcoindex
//! address codec.
//!
//! ClickHouse calls an executable UDF once per query with the arguments on the
//! command line, then streams the input rows over stdin (one row per line, in
//! the format named by the SQL `FORMAT` clause) and reads the results from
//! stdout. We use `TabSeparated`:
//!
//! ```text
//! CREATE FUNCTION bidxAddress AS (ty, hash) ->
//!     executable('bidx-udf', 'address --network mainnet', 'TabSeparated');
//! SELECT bidxAddress(script_type, hex(address_hash)) FROM outputs LIMIT 5;
//! ```
//!
//! `hex(address_hash)` keeps the wire bytes off the pipe (a raw
//! `FixedString(32)` could contain a tab or newline and corrupt the framing).

use clap::{Parser, Subcommand};
use std::io::{stdin, stdout};

use bidx_core::Network;

#[derive(Parser)]
#[command(name = "bidx-udf", about = "ClickHouse executable UDFs for bitcoindex address rendering")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Render stored `(script_type, hash_hex)` pairs as address strings.
    Address {
        /// Chain network, selects the base58/bech32 prefix.
        #[arg(long, value_enum, default_value = "mainnet")]
        network: NetworkArg,
    },
    /// Parse address strings back to `(script_type, hash_hex)` pairs.
    Parse {
        #[arg(long, value_enum, default_value = "mainnet")]
        network: NetworkArg,
    },
}

/// CLI-facing network enum (mirrors the `bidx` CLI's `NetworkArg`).
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum NetworkArg {
    Mainnet,
    Testnet3,
    Testnet4,
    Signet,
    Regtest,
}

impl From<NetworkArg> for Network {
    fn from(a: NetworkArg) -> Self {
        match a {
            NetworkArg::Mainnet => Network::Mainnet,
            NetworkArg::Testnet3 => Network::Testnet3,
            NetworkArg::Testnet4 => Network::Testnet4,
            NetworkArg::Signet => Network::Signet,
            NetworkArg::Regtest => Network::Regtest,
        }
    }
}

// Untested by design (coverage-exempt): `main` is the process entrypoint. It
// parses the real argv and reads the process stdin/stdout, so it cannot run
// inside a unit-test binary (that would consume the test harness's argv and
// stdin). All logic it delegates to is in `bidx_udf::run`, which is fully
// covered; the only uncovered lines here are these glue lines.
// coverage-exempt
fn main() {
    let cli = Cli::parse();
    let (mode, network) = match cli.cmd {
        Cmd::Address { network } => (bidx_udf::Mode::Address, network.into()),
        Cmd::Parse { network } => (bidx_udf::Mode::Parse, network.into()),
    };
    if let Err(e) = bidx_udf::run(mode, network, stdin().lock(), stdout().lock()) {
        // A UDF must not write diagnostics to stdout (that would corrupt the
        // TabSeparated stream), so report on stderr and exit non-zero.
        eprintln!("bidx-udf: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_arg_maps_to_bidx_core_network() {
        let pairs = [
            (NetworkArg::Mainnet, Network::Mainnet),
            (NetworkArg::Testnet3, Network::Testnet3),
            (NetworkArg::Testnet4, Network::Testnet4),
            (NetworkArg::Signet, Network::Signet),
            (NetworkArg::Regtest, Network::Regtest),
        ];
        for (a, expected) in pairs {
            let back: Network = a.into();
            assert_eq!(back, expected);
        }
    }

    #[test]
    fn cli_parses_both_subcommands_and_defaults_to_mainnet() {
        let c = Cli::try_parse_from(["bidx-udf", "address"]).unwrap();
        assert!(matches!(c.cmd, Cmd::Address { network: NetworkArg::Mainnet }));
        let c = Cli::try_parse_from(["bidx-udf", "parse", "--network", "testnet3"]).unwrap();
        assert!(matches!(c.cmd, Cmd::Parse { network: NetworkArg::Testnet3 }));
        assert!(Cli::try_parse_from(["bidx-udf", "bogus"]).is_err());
    }
}
