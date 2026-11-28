//! `bidx verify` — integrity checks an operator can run before trusting the
//! index, and in CI after a load.
//!
//! Three kinds of check:
//! * **Structural invariants** that must hold for any correctly loaded index
//!   (heights unique, one coinbase per block, spend linking consistent with
//!   the input count). These are pure arithmetic over `count()` aggregates, so
//!   they are cheap and catch the class of bug that corrupts a run silently.
//! * **Node agreement** — the indexed tip must never be ahead of the node tip.
//! * **Full-chain coverage** (informational): whether the index starts at
//!   genesis, which a partial-range load will not.
//!
//! [`evaluate`] is pure and unit-tested; [`run_verify`] performs the I/O and
//! returns a non-zero exit when any hard check fails.

use anyhow::Result;
use bidx_loader::{ChConfig, ChLoader};
use std::path::PathBuf;

pub struct VerifyArgs {
    pub ch: ChConfig,
    /// Optional Bitcoin Core RPC URL for the node-agreement check.
    pub rpc_url: Option<String>,
    /// Optional node `.cookie` file (used if user/password are not given).
    pub rpc_cookie: Option<PathBuf>,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
}

/// The raw counts gathered from ClickHouse and the node.
#[derive(Debug, Clone, PartialEq)]
pub struct Checks {
    pub indexed_height: u32,
    pub node_height: Option<u32>,
    pub blocks: u64,
    pub distinct_heights: u64,
    pub inputs: u64,
    pub coinbase_inputs: u64,
    pub spends: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Pass,
    Fail,
    Warn,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckResult {
    pub name: &'static str,
    pub status: CheckStatus,
    pub detail: String,
}

impl CheckResult {
    fn pass(name: &'static str, detail: String) -> Self {
        CheckResult {
            name,
            status: CheckStatus::Pass,
            detail,
        }
    }
    fn fail(name: &'static str, detail: String) -> Self {
        CheckResult {
            name,
            status: CheckStatus::Fail,
            detail,
        }
    }
    fn warn(name: &'static str, detail: String) -> Self {
        CheckResult {
            name,
            status: CheckStatus::Warn,
            detail,
        }
    }
}

/// Evaluate the invariants. Pure — no I/O.
pub fn evaluate(c: &Checks) -> Vec<CheckResult> {
    let mut out = Vec::new();

    // 1. No duplicate block heights.
    if c.distinct_heights == c.blocks {
        out.push(CheckResult::pass(
            "unique_block_heights",
            format!("{} heights, {} rows", c.distinct_heights, c.blocks),
        ));
    } else {
        out.push(CheckResult::fail(
            "unique_block_heights",
            format!(
                "{} distinct heights but {} rows (duplicate heights present)",
                c.distinct_heights, c.blocks
            ),
        ));
    }

    // 2. Exactly one coinbase input per block.
    if c.coinbase_inputs == c.blocks {
        out.push(CheckResult::pass(
            "one_coinbase_per_block",
            format!("{} coinbases for {} blocks", c.coinbase_inputs, c.blocks),
        ));
    } else {
        out.push(CheckResult::fail(
            "one_coinbase_per_block",
            format!(
                "{} coinbase inputs but {} blocks",
                c.coinbase_inputs, c.blocks
            ),
        ));
    }

    // 3. Spend linking: every non-coinbase input has exactly one spend row.
    let expected_spends = c.inputs.saturating_sub(c.coinbase_inputs);
    if c.spends == expected_spends {
        out.push(CheckResult::pass(
            "spend_linking",
            format!(
                "{} spends == {} non-coinbase inputs",
                c.spends, expected_spends
            ),
        ));
    } else {
        out.push(CheckResult::fail(
            "spend_linking",
            format!(
                "{} spends != {} non-coinbase inputs ({} - {})",
                c.spends, expected_spends, c.inputs, c.coinbase_inputs
            ),
        ));
    }

    // 4. Node agreement: never ahead of the node.
    match c.node_height {
        Some(node) if c.indexed_height <= node => out.push(CheckResult::pass(
            "node_agreement",
            format!("indexed {} <= node {}", c.indexed_height, node),
        )),
        Some(node) => out.push(CheckResult::fail(
            "node_agreement",
            format!(
                "indexed height {} is AHEAD of node {} (impossible; suspect corruption)",
                c.indexed_height, node
            ),
        )),
        None => out.push(CheckResult::warn(
            "node_agreement",
            "node height unavailable (no --rpc-url or node unreachable)".into(),
        )),
    }

    // 5. Full-chain coverage (informational).
    if c.blocks == c.indexed_height as u64 + 1 {
        out.push(CheckResult::pass(
            "full_chain_from_genesis",
            format!("{} blocks covers 0..={}", c.blocks, c.indexed_height),
        ));
    } else {
        out.push(CheckResult::warn(
            "full_chain_from_genesis",
            format!(
                "{} blocks for tip {} — partial range (expected for a ranged load)",
                c.blocks, c.indexed_height
            ),
        ));
    }

    out
}

/// True when no check failed.
pub fn all_passed(results: &[CheckResult]) -> bool {
    results.iter().all(|r| r.status != CheckStatus::Fail)
}

/// Run the checks against ClickHouse (and optionally the node).
pub fn run_verify(args: VerifyArgs) -> Result<()> {
    let loader = ChLoader::new(args.ch.clone())?;
    run_verify_with(&loader, &args) // coverage-exempt: needs a working clickhouse-client on PATH; the delegate is covered via run_verify_with, and the no-client path via run_verify_fails_fast_without_clickhouse_client
}

/// The query + evaluation half of [`run_verify`], split out so it can be
/// driven with an injected loader (tests use a fake `clickhouse-client`).
pub fn run_verify_with(loader: &ChLoader, args: &VerifyArgs) -> Result<()> {
    let db = &args.ch.database;

    let indexed_height =
        parse_scalar(&loader.query_scalar(&format!("SELECT max(height) FROM {db}.blocks"))?)
            .unwrap_or(0) as u32;
    let blocks = parse_scalar(&loader.query_scalar(&format!("SELECT count() FROM {db}.blocks"))?)
        .unwrap_or(0);
    let distinct_heights =
        parse_scalar(&loader.query_scalar(&format!("SELECT uniqExact(height) FROM {db}.blocks"))?)
            .unwrap_or(0);
    let inputs = parse_scalar(&loader.query_scalar(&format!("SELECT count() FROM {db}.inputs"))?)
        .unwrap_or(0);
    let coinbase_inputs = parse_scalar(&loader.query_scalar(&format!(
        "SELECT count() FROM {db}.inputs WHERE is_coinbase = 1"
    ))?)
    .unwrap_or(0);
    let spends = parse_scalar(&loader.query_scalar(&format!(
        "SELECT count() FROM {db}.spends_by_spent_outpoint"
    ))?)
    .unwrap_or(0);

    let node_height = match &args.rpc_url {
        Some(url) => fetch_node_height(
            url,
            args.rpc_cookie.as_ref(),
            args.rpc_user.as_deref(),
            args.rpc_password.as_deref(),
        ),
        None => None,
    };

    let checks = Checks {
        indexed_height,
        node_height,
        blocks,
        distinct_heights,
        inputs,
        coinbase_inputs,
        spends,
    };
    let results = evaluate(&checks);
    for r in &results {
        let tag = match r.status {
            CheckStatus::Pass => "PASS",
            CheckStatus::Fail => "FAIL",
            CheckStatus::Warn => "WARN",
        };
        println!("[{tag}] {:<24} {}", r.name, r.detail);
    }

    if all_passed(&results) {
        println!("\nverify: OK");
        Ok(())
    } else {
        anyhow::bail!(
            "verify: {} check(s) failed",
            results
                .iter()
                .filter(|r| r.status == CheckStatus::Fail)
                .count()
        )
    }
}

/// Parse a scalar ClickHouse response, tolerating whitespace.
pub fn parse_scalar(s: &str) -> Option<u64> {
    s.trim().parse().ok()
}

/// Best-effort node tip height via `getblockcount`.
fn fetch_node_height(
    rpc_url: &str,
    cookie: Option<&PathBuf>,
    user: Option<&str>,
    password: Option<&str>,
) -> Option<u32> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .ok()?;
    // Resolve credentials: explicit user/password, else the cookie file
    // ("user:password").
    let (u, p) = if let (Some(u), Some(p)) = (user, password) {
        (Some(u.to_string()), Some(p.to_string()))
    } else if let Some(path) = cookie {
        let raw = std::fs::read_to_string(path).ok()?;
        match raw.trim().split_once(':') {
            Some((a, b)) => (Some(a.to_string()), Some(b.to_string())),
            None => (None, None),
        }
    } else {
        (None, None)
    };
    let body = serde_json::json!({
        "jsonrpc": "1.0", "id": "bidx-verify", "method": "getblockcount", "params": []
    });
    let mut req = client.post(rpc_url).json(&body);
    if let Some(u) = u {
        req = req.basic_auth(u, p);
    }
    let resp = req.send().ok()?;
    let v: serde_json::Value = resp.json().ok()?;
    v.get("result")?.as_u64().map(|n| n as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy() -> Checks {
        Checks {
            indexed_height: 99,
            node_height: Some(100),
            blocks: 100,
            distinct_heights: 100,
            inputs: 300,
            coinbase_inputs: 100,
            spends: 200,
        }
    }

    #[test]
    fn healthy_index_passes_every_check() {
        let r = evaluate(&healthy());
        assert!(all_passed(&r));
        assert_eq!(r.len(), 5);
        assert!(r.iter().all(|c| c.status == CheckStatus::Pass));
    }

    #[test]
    fn duplicate_heights_fail() {
        let mut c = healthy();
        c.distinct_heights = 99; // 100 rows, 99 distinct
        let r = evaluate(&c);
        assert!(!all_passed(&r));
        let dup = r.iter().find(|x| x.name == "unique_block_heights").unwrap();
        assert_eq!(dup.status, CheckStatus::Fail);
        assert!(dup.detail.contains("duplicate"));
    }

    #[test]
    fn missing_or_extra_coinbase_fails() {
        let mut c = healthy();
        c.coinbase_inputs = 99;
        let r = evaluate(&c);
        assert!(r
            .iter()
            .any(|x| x.name == "one_coinbase_per_block" && x.status == CheckStatus::Fail));
    }

    #[test]
    fn broken_spend_linking_fails() {
        let mut c = healthy();
        c.spends = 199;
        let r = evaluate(&c);
        let s = r.iter().find(|x| x.name == "spend_linking").unwrap();
        assert_eq!(s.status, CheckStatus::Fail);
        assert!(s.detail.contains("199"));
    }

    #[test]
    fn node_agreement_warns_without_node_and_fails_when_ahead() {
        let mut c = healthy();
        c.node_height = None;
        let r = evaluate(&c);
        assert!(r
            .iter()
            .any(|x| x.name == "node_agreement" && x.status == CheckStatus::Warn));
        // Warnings alone do not fail the run.
        assert!(all_passed(&r));

        let mut c = healthy();
        c.node_height = Some(50); // indexed 99 > node 50
        let r = evaluate(&c);
        assert!(r
            .iter()
            .any(|x| x.name == "node_agreement" && x.status == CheckStatus::Fail));
        assert!(!all_passed(&r));
    }

    #[test]
    fn partial_range_warns_but_does_not_fail() {
        let mut c = healthy();
        c.indexed_height = 500;
        c.blocks = 100; // not 0..=500
        c.distinct_heights = 100;
        c.coinbase_inputs = 100;
        c.inputs = 300;
        c.spends = 200;
        c.node_height = Some(600);
        let r = evaluate(&c);
        assert!(r
            .iter()
            .any(|x| x.name == "full_chain_from_genesis" && x.status == CheckStatus::Warn));
        assert!(all_passed(&r), "partial range is a warning, not a failure");
    }

    #[test]
    fn genesis_only_index_is_full_chain() {
        let c = Checks {
            indexed_height: 0,
            node_height: Some(0),
            blocks: 1,
            distinct_heights: 1,
            inputs: 1,
            coinbase_inputs: 1,
            spends: 0,
        };
        let r = evaluate(&c);
        assert!(r
            .iter()
            .any(|x| x.name == "full_chain_from_genesis" && x.status == CheckStatus::Pass));
        assert!(all_passed(&r));
    }

    #[test]
    fn parse_scalar_handles_whitespace_and_garbage() {
        assert_eq!(parse_scalar(" 42\n"), Some(42));
        assert_eq!(parse_scalar("0"), Some(0));
        assert_eq!(parse_scalar("not a number"), None);
        assert_eq!(parse_scalar(""), None);
    }

    #[test]
    fn run_verify_fails_fast_without_clickhouse_client() {
        let _g = fork_guard();
        // No clickhouse-client in the sandbox PATH → a clear error, not a panic.
        let e = run_verify(VerifyArgs {
            ch: ChConfig::default(),
            rpc_url: None,
            rpc_cookie: None,
            rpc_user: None,
            rpc_password: None,
        });
        // Either the client is missing (NoClient) or the query fails against a
        // nonexistent server; both must be Err.
        assert!(e.is_err());
    }

    // --- injected-loader coverage ------------------------------------------

    fn args() -> VerifyArgs {
        VerifyArgs {
            ch: ChConfig {
                database: "db".into(),
                ..ChConfig::default()
            },
            rpc_url: None,
            rpc_cookie: None,
            rpc_user: None,
            rpc_password: None,
        }
    }

    /// A fake `clickhouse-client` that echoes a scalar chosen by the query
    /// substring, so `run_verify_with` can be driven end-to-end. `uniq` is the
    /// `uniqExact(height)` value, letting a test force a duplicate-height fail.
    fn fake_client(tag: &str, uniq: u32) -> PathBuf {
        let path = std::env::temp_dir().join(format!("bidx-verify-{tag}-{}", std::process::id()));
        let body = format!(
            "#!/bin/sh\n\
             q=\"\"\n\
             for a in \"$@\"; do case \"$a\" in --query=*) q=\"$a\";; esac; done\n\
             case \"$q\" in\n\
             *uniqExact*) echo {uniq} ;;\n\
             *is_coinbase*) echo 100 ;;\n\
             *max*) echo 99 ;;\n\
             *'FROM db.blocks'*) echo 100 ;;\n\
             *'FROM db.inputs'*) echo 300 ;;\n\
             *'FROM db.spends_by_spent_outpoint'*) echo 200 ;;\n\
             *) echo 0 ;;\n\
             esac\n"
        );
        // Write privately then rename into place so the final inode is never
        // open for writing; the exec target's fd can't be inherited mid-write.
        use std::os::unix::fs::PermissionsExt;
        let tmp =
            std::env::temp_dir().join(format!("bidx-verify-{tag}-{}-tmp", std::process::id()));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    /// Spawning the fake client forks the whole test process, so a sibling
    /// thread mid-`fs::write` would leave its write fd inherited by the child
    /// and `execve` would fail with ETXTBSY. Tests that spawn a client hold
    /// this guard for their whole body to keep the fork and the writes apart.
    fn fork_guard() -> std::sync::MutexGuard<'static, ()> {
        crate::testlock::fork_guard()
    }

    fn loader(client: PathBuf) -> ChLoader {
        ChLoader::with_client(
            ChConfig {
                database: "db".into(),
                ..ChConfig::default()
            },
            client,
        )
    }

    #[test]
    fn run_verify_with_passes_on_a_healthy_fake_client() {
        let _g = fork_guard();
        let script = fake_client("ok", 100);
        let l = loader(script.clone());
        let res = run_verify_with(&l, &args());
        assert!(res.is_ok(), "{res:?}");
        let _ = std::fs::remove_file(&script);
    }

    #[test]
    fn run_verify_with_fails_when_an_invariant_breaks() {
        let _g = fork_guard();
        // uniqExact returns 99 for 100 block rows → duplicate-height failure.
        let script = fake_client("dup", 99);
        let l = loader(script.clone());
        let e = run_verify_with(&l, &args()).unwrap_err();
        assert!(e.to_string().contains("check(s) failed"), "{e}");
        let _ = std::fs::remove_file(&script);
    }

    #[test]
    fn run_verify_with_queries_the_node_when_an_rpc_url_is_given() {
        let _g = fork_guard();
        // A healthy fake client plus a node that reports a higher tip must
        // still pass, exercising the `Some(url)` arm of the node lookup.
        let script = fake_client("rpc", 100);
        let l = loader(script.clone());
        let (url, h) = spawn_rpc(r#"{"result":150}"#);
        let mut a = args();
        a.rpc_url = Some(url);
        let res = run_verify_with(&l, &a);
        h.join().unwrap();
        assert!(res.is_ok(), "{res:?}");
        let _ = std::fs::remove_file(&script);
    }

    #[test]
    fn run_verify_with_fails_when_the_index_is_ahead_of_the_node() {
        let _g = fork_guard();
        let script = fake_client("ahead", 100);
        let l = loader(script.clone());
        let (url, h) = spawn_rpc(r#"{"result":10}"#); // node 10 < indexed 99
        let mut a = args();
        a.rpc_url = Some(url);
        let e = run_verify_with(&l, &a).unwrap_err();
        h.join().unwrap();
        assert!(e.to_string().contains("check(s) failed"), "{e}");
        let _ = std::fs::remove_file(&script);
    }

    #[test]
    fn run_verify_with_surfaces_client_failure() {
        let _g = fork_guard();
        let path = std::env::temp_dir().join(format!("bidx-verify-bad-{}", std::process::id()));
        std::fs::write(&path, "#!/bin/sh\necho boom >&2\nexit 1\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();

        let l = loader(path.clone());
        assert!(run_verify_with(&l, &args()).is_err());
        let _ = std::fs::remove_file(&path);
    }

    // --- node RPC coverage -------------------------------------------------

    /// A one-shot HTTP server that answers any request with `body`.
    fn spawn_rpc(body: &'static str) -> (String, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                use std::io::{Read, Write};
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
            }
        });
        (format!("http://{addr}"), handle)
    }

    #[test]
    fn fetch_node_height_reads_the_result_field() {
        let (url, h) = spawn_rpc(r#"{"jsonrpc":"1.0","id":"bidx-verify","result":840000}"#);
        assert_eq!(fetch_node_height(&url, None, None, None), Some(840_000));
        h.join().unwrap();
    }

    #[test]
    fn fetch_node_height_uses_explicit_credentials() {
        let (url, h) = spawn_rpc(r#"{"result":5}"#);
        assert_eq!(fetch_node_height(&url, None, Some("u"), Some("p")), Some(5));
        h.join().unwrap();
    }

    #[test]
    fn fetch_node_height_parses_a_cookie_file() {
        let cookie = std::env::temp_dir().join(format!("bidx-cookie-{}", std::process::id()));
        std::fs::write(&cookie, "__cookie__:sekret\n").unwrap();
        let (url, h) = spawn_rpc(r#"{"result":7}"#);
        assert_eq!(fetch_node_height(&url, Some(&cookie), None, None), Some(7));
        h.join().unwrap();
        let _ = std::fs::remove_file(&cookie);
    }

    #[test]
    fn fetch_node_height_tolerates_malformed_cookie_and_missing_file() {
        // Cookie with no ':' → no credentials, still attempts the request.
        let no_colon = std::env::temp_dir().join(format!("bidx-cookie-nc-{}", std::process::id()));
        std::fs::write(&no_colon, "no-separator-here\n").unwrap();
        let (url, h) = spawn_rpc(r#"{"result":1}"#);
        assert_eq!(
            fetch_node_height(&url, Some(&no_colon), None, None),
            Some(1)
        );
        h.join().unwrap();
        let _ = std::fs::remove_file(&no_colon);

        // Missing cookie file → early None.
        let missing = PathBuf::from("/definitely/not/here/.cookie");
        assert_eq!(
            fetch_node_height("http://127.0.0.1:1", Some(&missing), None, None),
            None
        );
    }

    #[test]
    fn fetch_node_height_returns_none_on_bad_payloads_and_unreachable_node() {
        // No `result` key.
        let (url, h) = spawn_rpc(r#"{"jsonrpc":"1.0","id":"x"}"#);
        assert_eq!(fetch_node_height(&url, None, None, None), None);
        h.join().unwrap();

        // `result` present but not a u64.
        let (url, h) = spawn_rpc(r#"{"result":null}"#);
        assert_eq!(fetch_node_height(&url, None, None, None), None);
        h.join().unwrap();

        // Nothing listening → send fails, returns None.
        assert_eq!(
            fetch_node_height("http://127.0.0.1:1", None, None, None),
            None
        );
    }
}
