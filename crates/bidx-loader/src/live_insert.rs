//! Direct ClickHouse inserts for live mode.
//!
//! Bulk ingest goes through Parquet and the `file()` table function (see
//! [`crate::ChLoader`]), which is the right shape for millions of rows but
//! leaves a gap at the tip: the newest block is only queryable after a part
//! file is closed and loaded. `LiveChWriter` closes that gap by streaming each
//! applied block straight into ClickHouse over the client's stdin in
//! [`RowBinary`](crate::rowbinary) format — no Parquet round-trip, so the tip
//! is queryable within the block's own apply time.
//!
//! One child `clickhouse-client` process is kept open per table (`INSERT INTO
//! ... FORMAT RowBinary` reads rows from stdin until EOF), so a block costs a
//! single `write` per table instead of a process spawn. Materialized views fire
//! exactly as they do for a bulk load, so the derived indexes stay in sync.

use crate::rowbinary as rb;
use crate::{ChConfig, LoadError};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};

/// A single open `INSERT ... FORMAT RowBinary` pipe to one ClickHouse table.
struct Pipe {
    table: &'static str,
    child: Child,
    stdin: ChildStdin,
    /// Rows buffered for the current block; written to `stdin` on `flush`.
    buf: Vec<u8>,
}

impl Pipe {
    fn spawn(
        client: &PathBuf,
        cfg: &ChConfig,
        table: &'static str,
        cols: &str,
    ) -> Result<Self, LoadError> {
        let mut args = vec![
            format!("--host={}", cfg.host),
            format!("--port={}", cfg.port),
            format!("--user={}", cfg.user),
        ];
        if let Some(p) = &cfg.password {
            args.push(format!("--password={p}"));
        }
        args.push(format!(
            "--query=INSERT INTO {db}.{table} ({cols}) FORMAT RowBinary",
            db = cfg.database,
        ));
        let mut child = Command::new(client)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().ok_or_else(|| {
            // coverage-exempt: 61 — unreachable: the child is spawned with
            // `Stdio::piped()` for stdin, so `take()` always yields `Some`.
            // Kept as a defensive `ok_or_else` rather than `.unwrap()` (no
            // panics in production paths).
            LoadError::Client(format!("could not open stdin for table {table}"))
        })?;
        Ok(Pipe {
            table,
            child,
            stdin,
            buf: Vec::new(),
        })
    }

    /// Flush buffered rows to the client's stdin. A no-op when nothing is
    /// buffered, so callers can flush every table unconditionally.
    fn flush(&mut self) -> Result<(), LoadError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.stdin.write_all(&self.buf).map_err(|e| {
            LoadError::Client(format!("write to {} failed: {e}", self.table))
        })?;
        self.buf.clear();
        Ok(())
    }

    /// Flush, close stdin (EOF ends the insert), and reap the child.
    fn finish(mut self) -> Result<(), LoadError> {
        self.flush()?;
        // Drop the stdin handle so the client sees EOF and finishes the insert.
        drop(self.stdin);
        let out = self.child.wait_with_output()?;
        if !out.status.success() {
            return Err(LoadError::Client(format!(
                "insert into {} failed: {}",
                self.table,
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }
}

/// Streams base-entity rows into ClickHouse for live mode, one open insert pipe
/// per table. The `witnesses` pipe exists only when witness capture is enabled.
pub struct LiveChWriter {
    blocks: Pipe,
    txs: Pipe,
    inputs: Pipe,
    outputs: Pipe,
    spends: Pipe,
    witnesses: Option<Pipe>,
}

impl LiveChWriter {
    /// Open one `RowBinary` insert pipe per base table. `capture_witness` also
    /// opens the optional `witnesses` pipe.
    pub fn open(cfg: &ChConfig, client: PathBuf, capture_witness: bool) -> Result<Self, LoadError> {
        let witnesses = if capture_witness {
            Some(Pipe::spawn(&client, cfg, "witnesses", rb::WITNESSES_COLS)?)
        } else {
            None
        };
        Ok(LiveChWriter {
            blocks: Pipe::spawn(&client, cfg, "blocks", rb::BLOCKS_COLS)?,
            txs: Pipe::spawn(&client, cfg, "transactions", rb::TXS_COLS)?,
            inputs: Pipe::spawn(&client, cfg, "inputs", rb::INPUTS_COLS)?,
            outputs: Pipe::spawn(&client, cfg, "outputs", rb::OUTPUTS_COLS)?,
            spends: Pipe::spawn(&client, cfg, "spends", rb::SPENDS_COLS)?,
            witnesses,
        })
    }

    pub fn write_block(&mut self, r: &bidx_core::BlockRow) {
        rb::block(r, &mut self.blocks.buf);
    }

    pub fn write_tx(&mut self, r: &bidx_core::TxRow) {
        rb::tx(r, &mut self.txs.buf);
    }

    pub fn write_input(&mut self, r: &bidx_core::InputRow) {
        rb::input(r, &mut self.inputs.buf);
    }

    pub fn write_output(&mut self, r: &bidx_core::OutputRow) {
        rb::output(r, &mut self.outputs.buf);
    }

    pub fn write_spend(&mut self, r: &bidx_core::SpendRow) {
        rb::spend(r, &mut self.spends.buf);
    }

    /// Buffer one witness payload row. A no-op unless the writer was opened
    /// with `capture_witness`, so callers can push unconditionally.
    pub fn write_witness(&mut self, r: &bidx_core::WitnessRow) {
        if let Some(w) = self.witnesses.as_mut() {
            rb::witness(r, &mut w.buf);
        }
    }

    /// Push every buffered row set to its table. Called once per applied block.
    pub fn flush(&mut self) -> Result<(), LoadError> {
        self.blocks.flush()?;
        self.txs.flush()?;
        self.inputs.flush()?;
        self.outputs.flush()?;
        self.spends.flush()?;
        if let Some(w) = self.witnesses.as_mut() {
            w.flush()?;
        }
        Ok(())
    }

    /// Flush any pending rows and close every insert, surfacing any per-table
    /// failure. Consumes the writer.
    pub fn finish(self) -> Result<(), LoadError> {
        let LiveChWriter {
            blocks,
            txs,
            inputs,
            outputs,
            spends,
            witnesses,
        } = self;
        blocks.finish()?;
        txs.finish()?;
        inputs.finish()?;
        outputs.finish()?;
        spends.finish()?;
        if let Some(w) = witnesses {
            w.finish()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// A `clickhouse-client` stand-in that copies its stdin to
    /// `<script>.<table>.out`, deriving the table from the `--query` argument.
    /// Writes the file via a private temp then renames, so a concurrent fork
    /// cannot observe a partially-written script (see the loader tests).
    fn fake_client(tag: &str, fail: bool) -> PathBuf {
        let path = std::env::temp_dir().join(format!("bidx-live-ch-{tag}-{}", std::process::id()));
        let body = if fail {
            "#!/bin/sh\ncat >/dev/null\necho 'simulated failure' >&2\nexit 1\n".to_string()
        } else {
            "#!/bin/sh\nq=''\nfor a in \"$@\"; do case \"$a\" in --query=INSERT*) q=\"$a\";; esac; done\n\
             t=$(echo \"$q\" | sed -E 's/.*INTO [^.]+[.]([^ ]+) .*/\\1/')\n\
             cat > \"$0.$t.out\"\nexit 0\n"
                .to_string()
        };
        use std::os::unix::fs::PermissionsExt;
        let tmp = std::env::temp_dir().join(format!(
            "bidx-live-ch-{tag}-{}-tmp",
            std::process::id()
        ));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        path
    }

    fn read_out(script: &Path, table: &str) -> Vec<u8> {
        let p = PathBuf::from(format!("{}.{table}.out", script.display()));
        std::fs::read(&p).unwrap_or_default()
    }

    fn sample_block() -> bidx_core::BlockRow {
        bidx_core::BlockRow {
            height: 42,
            hash: [0xAA; 32],
            prev_hash: [0xBB; 32],
            merkle_root: [0xCC; 32],
            time: 1,
            bits: 2,
            nonce: 3,
            version: 1,
            tx_count: 1,
            size: 100,
            weight: 400,
            total_fee: 5,
            coinbase_value: 50,
        }
    }

    fn sample_tx() -> bidx_core::TxRow {
        bidx_core::TxRow {
            height: 42,
            tx_index: 0,
            txid: [0x11; 32],
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

    fn sample_input() -> bidx_core::InputRow {
        bidx_core::InputRow {
            height: 42,
            tx_index: 0,
            input_index: 0,
            txid: [0x11; 32],
            prev_txid: [0; 32],
            prev_vout: 0xFFFF_FFFF,
            script_sig_len: 0,
            sequence: 0,
            witness_items: 0,
            witness_bytes: 0,
            is_coinbase: true,
        }
    }

    fn sample_output() -> bidx_core::OutputRow {
        bidx_core::OutputRow {
            height: 42,
            tx_index: 0,
            output_index: 0,
            txid: [0x11; 32],
            value_sat: 50,
            script_pubkey_len: 0,
            script_type: 0,
            address_hash: [0; 32],
        }
    }

    fn sample_spend() -> bidx_core::SpendRow {
        bidx_core::SpendRow {
            height: 42,
            spending_tx_index: 0,
            spending_input_index: 0,
            spending_txid: [0x11; 32],
            spent_txid: [0x22; 32],
            spent_vout: 0,
            spent_value_sat: 10,
            spent_script_type: 1,
            spent_address_hash: [0x33; 32],
            spent_height: 0,
        }
    }

    #[test]
    fn writer_streams_every_base_table_and_closes_cleanly() {
        let _g = crate::tests::fork_guard();
        let client = fake_client("ok", false);
        let mut w = LiveChWriter::open(&ChConfig::default(), client.clone(), false).unwrap();
        w.write_block(&sample_block());
        w.write_tx(&sample_tx());
        w.write_input(&sample_input());
        w.write_output(&sample_output());
        w.write_spend(&sample_spend());
        // No witness pipe when capture is off: the call must be a silent no-op.
        w.write_witness(&bidx_core::WitnessRow {
            height: 42,
            tx_index: 0,
            input_index: 0,
            item_index: 0,
            item_len: 1,
            item_bytes: vec![0xAA],
        });
        w.flush().unwrap();
        w.finish().unwrap();

        // Each table received exactly one encoded row.
        assert_eq!(read_out(&client, "blocks").len(), 144);
        assert_eq!(read_out(&client, "transactions").len(), 4 + 4 + 32 + 4 + 4 + 4 + 4 + 8 + 4 + 4 + 1);
        assert_eq!(read_out(&client, "inputs").len(), 4 + 4 + 4 + 32 + 32 + 4 + 4 + 4 + 4 + 4 + 1);
        assert_eq!(read_out(&client, "outputs").len(), 4 + 4 + 4 + 32 + 8 + 4 + 1 + 32);
        assert_eq!(read_out(&client, "spends").len(), 4 + 4 + 4 + 32 + 32 + 4 + 8 + 1 + 32 + 4);
        // No witnesses file was produced.
        let wit = PathBuf::from(format!("{}.witnesses.out", client.display()));
        assert!(!wit.exists());

        for t in ["blocks", "transactions", "inputs", "outputs", "spends", "witnesses"] {
            let _ = std::fs::remove_file(format!("{}.{t}.out", client.display()));
        }
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn writer_emits_witness_rows_when_capture_enabled() {
        let _g = crate::tests::fork_guard();
        let client = fake_client("wit", false);
        let mut w = LiveChWriter::open(&ChConfig::default(), client.clone(), true).unwrap();
        w.write_witness(&bidx_core::WitnessRow {
            height: 1,
            tx_index: 2,
            input_index: 3,
            item_index: 4,
            item_len: 2,
            item_bytes: vec![0xDE, 0xAD],
        });
        w.flush().unwrap();
        w.finish().unwrap();
        let bytes = read_out(&client, "witnesses");
        assert_eq!(bytes.len(), 20 + 1 + 2, "five u32s + a 1-byte length + payload");
        assert_eq!(&bytes[20..23], &[0x02, 0xDE, 0xAD]);
        for t in ["blocks", "transactions", "inputs", "outputs", "spends", "witnesses"] {
            let _ = std::fs::remove_file(format!("{}.{t}.out", client.display()));
        }
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn flush_with_no_buffered_rows_writes_nothing() {
        let _g = crate::tests::fork_guard();
        let client = fake_client("empty", false);
        let mut w = LiveChWriter::open(&ChConfig::default(), client.clone(), false).unwrap();
        w.flush().unwrap();
        w.finish().unwrap();
        // Empty buffers mean no bytes written; the fake client still creates
        // the (empty) output files on EOF.
        assert!(read_out(&client, "blocks").is_empty());
        for t in ["blocks", "transactions", "inputs", "outputs", "spends", "witnesses"] {
            let _ = std::fs::remove_file(format!("{}.{t}.out", client.display()));
        }
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn finish_surfaces_a_failing_insert() {
        let _g = crate::tests::fork_guard();
        let client = fake_client("fail", true);
        let mut w = LiveChWriter::open(&ChConfig::default(), client.clone(), false).unwrap();
        w.write_block(&sample_block());
        w.flush().unwrap();
        let e = w.finish().err().expect("failing client must error");
        assert!(
            matches!(e, LoadError::Client(ref m) if m.contains("insert into blocks failed")),
            "got: {e:?}"
        );
        let _ = std::fs::remove_file(&client);
    }

    /// A `clickhouse-client` stand-in that appends its argv (one arg per line)
    /// to `<script>.args`, so a test can assert the connection flags the writer
    /// passes. It also drains stdin so the parent's writes never block.
    fn fake_client_recording_args(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("bidx-live-chargs-{tag}-{}", std::process::id()));
        let body = "#!/bin/sh\nfor a in \"$@\"; do echo \"$a\"; done >> \"$0.args\"\n\
                    cat >/dev/null\nexit 0\n"
            .to_string();
        use std::os::unix::fs::PermissionsExt;
        let tmp = std::env::temp_dir().join(format!(
            "bidx-live-chargs-{tag}-{}-tmp",
            std::process::id()
        ));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        let _ = std::fs::remove_file(format!("{}.args", path.display()));
        path
    }

    #[test]
    fn open_passes_connection_flags_including_password_when_set() {
        let _g = crate::tests::fork_guard();
        let client = fake_client_recording_args("flags");
        let cfg = ChConfig {
            host: "ch.example".into(),
            port: 9440,
            user: "reader".into(),
            password: Some("s3cr3t".into()),
            database: "btc".into(),
        };
        let w = LiveChWriter::open(&cfg, client.clone(), false).unwrap();
        // Close the pipes so every child has recorded its argv.
        w.finish().unwrap();
        let args = std::fs::read_to_string(format!("{}.args", client.display())).unwrap();
        assert!(args.contains("--host=ch.example"), "got {args}");
        assert!(args.contains("--port=9440"), "got {args}");
        assert!(args.contains("--user=reader"), "got {args}");
        // The password branch only pushes the flag when `password` is `Some`.
        assert!(args.contains("--password=s3cr3t"), "got {args}");
        let _ = std::fs::remove_file(format!("{}.args", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn open_omits_password_flag_when_unset() {
        let _g = crate::tests::fork_guard();
        let client = fake_client_recording_args("nopass");
        let cfg = ChConfig {
            password: None,
            ..ChConfig::default()
        };
        let w = LiveChWriter::open(&cfg, client.clone(), false).unwrap();
        w.finish().unwrap();
        let args = std::fs::read_to_string(format!("{}.args", client.display())).unwrap();
        assert!(!args.contains("--password"), "got {args}");
        let _ = std::fs::remove_file(format!("{}.args", client.display()));
        let _ = std::fs::remove_file(&client);
    }

    #[test]
    fn flush_surfaces_a_broken_pipe_when_the_client_exits() {
        // A client that exits without reading stdin closes the pipe's read end,
        // so the next `write_all` fails with EPIPE. This exercises the flush
        // error arm. Rust ignores SIGPIPE at startup, so the write returns an
        // error instead of killing the test process.
        let _g = crate::tests::fork_guard();
        let path = std::env::temp_dir().join(format!("bidx-live-chexit-{}", std::process::id()));
        let body = "#!/bin/sh\nexit 0\n";
        use std::os::unix::fs::PermissionsExt;
        let tmp = std::env::temp_dir().join(format!("bidx-live-chexit-{}-tmp", std::process::id()));
        std::fs::write(&tmp, body).unwrap();
        let mut perms = std::fs::metadata(&tmp).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms).unwrap();
        std::fs::rename(&tmp, &path).unwrap();

        let mut w = LiveChWriter::open(&ChConfig::default(), path.clone(), false).unwrap();
        // Give every child time to exit so the read ends are certainly closed.
        std::thread::sleep(std::time::Duration::from_millis(300));
        w.write_block(&sample_block());
        let e = w.flush().expect_err("writing to a closed pipe must error");
        assert!(
            matches!(e, LoadError::Client(ref m) if m.contains("write to blocks failed")),
            "got: {e:?}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
