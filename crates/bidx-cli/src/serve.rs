//! `bidx serve` — run the REST + Electrum JSON-RPC + metrics service.
//!
//! [`build_service`] assembles the service from configuration with no socket
//! I/O (and is unit-tested); [`run_serve`] adds the listener loop and the
//! periodic health refresh.

use anyhow::{Context, Result};
use bidx_serve::config::Config;
use bidx_serve::health::Health;
use bidx_serve::metrics::Metrics;
use bidx_serve::service::{serve, serve_listener, Service};
use bidx_serve::store::{ChHttpTransport, Store};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub struct ServeArgs {
    /// Optional TOML config file; `BIDX_*` env vars override it.
    pub config: Option<PathBuf>,
}

/// Assemble a `Service` from configuration. No sockets are opened here, so
/// this is directly testable.
pub fn build_service(cfg: Config) -> Result<Arc<Service>> {
    cfg.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
    let transport = ChHttpTransport::with_database(
        &cfg.clickhouse.host,
        cfg.clickhouse.http_port,
        &cfg.clickhouse.user,
        cfg.clickhouse.password.clone(),
        &cfg.clickhouse.database,
        cfg.clickhouse.timeout_secs,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let store = Arc::new(Store::new(
        Box::new(transport),
        cfg.clickhouse.database.clone(),
    ));
    let metrics = Arc::new(Metrics::new());
    let health = Arc::new(Health::new());
    Ok(Arc::new(Service::new(
        store,
        Arc::new(cfg),
        metrics,
        health,
    )))
}

pub fn run_serve(args: ServeArgs) -> Result<()> {
    let cfg = Config::load(args.config.as_deref()).map_err(|e| anyhow::anyhow!("{e}"))?;
    run_serve_with(cfg, serve)
}

/// The serve setup, split from [`run_serve`] so the health priming, background
/// refresh, and ops-listener wiring can be exercised with a serve loop that
/// returns immediately instead of blocking on `accept`. `serve_fn` is the
/// blocking API loop (`serve` in production).
pub fn run_serve_with<F>(cfg: Config, serve_fn: F) -> Result<()>
where
    F: FnOnce(&str, Arc<Service>) -> std::io::Result<()>,
{
    let metrics_addr = cfg.metrics_addr.clone();
    let api_addr = cfg.api_addr.clone();
    let service = build_service(cfg)?;

    // Prime readiness/metrics before accepting traffic.
    service.refresh_health();

    // Periodic health + metrics refresh (node height, lag).
    {
        let svc = service.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
            svc.refresh_health();
        });
    }

    // Ops listener (metrics/health) on its own address, if distinct. Bind
    // synchronously so a bad address is logged immediately rather than
    // surfacing later inside a detached thread.
    match std::net::TcpListener::bind(&metrics_addr) {
        Ok(listener) => {
            tracing::info!("bidx metrics listening on {metrics_addr}");
            let svc = service.clone();
            std::thread::spawn(move || serve_listener(listener, svc));
        }
        Err(e) => tracing::error!("failed to bind metrics addr {metrics_addr}: {e}"),
    }

    // Main API listener (blocks).
    serve_fn(&api_addr, service).context("serve failed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_service_succeeds_with_default_config() {
        let svc = build_service(Config::default()).unwrap();
        // No ClickHouse reachable, so the service starts but is not ready.
        assert!(!svc.health.ready());
        // The ops surface still answers.
        let req = bidx_serve::service::Request::new("GET", "/healthz", "");
        assert_eq!(svc.handle(&req).status, 200);
    }

    #[test]
    fn build_service_rejects_invalid_config() {
        let mut cfg = Config::default();
        cfg.metrics_addr = cfg.api_addr.clone();
        let e = build_service(cfg).err().unwrap();
        assert!(e.to_string().contains("must differ"));
    }

    #[test]
    fn run_serve_fails_fast_on_missing_config_file() {
        let e = run_serve(ServeArgs {
            config: Some(PathBuf::from("/definitely/not/here.toml")),
        })
        .unwrap_err();
        assert!(e.to_string().contains("not/here.toml"));
    }

    #[test]
    fn run_serve_with_primes_health_and_starts_ops_listener() {
        // Bind the ops listener on an ephemeral port so the test can't collide
        // with a real service; the API loop is a stub that returns at once.
        let mut cfg = Config::default();
        cfg.api_addr = "127.0.0.1:0".into();
        cfg.metrics_addr = "127.0.0.1:0".into();
        // validate() rejects equal addrs, so give them distinct-but-ephemeral
        // shapes (both parse; only the metrics one is actually bound).
        cfg.api_addr = "127.0.0.1:18432".into();

        let mut primed = false;
        let res = run_serve_with(cfg, |addr, svc| {
            // The API address is forwarded verbatim.
            assert_eq!(addr, "127.0.0.1:18432");
            // Startup must have primed the metrics/health state already.
            primed = svc.metrics.registry.render().contains("bidx_indexed_height");
            Ok(())
        });
        assert!(res.is_ok(), "{res:?}");
        assert!(primed, "refresh_health should run before serving");
    }

    #[test]
    fn run_serve_with_logs_bad_metrics_addr_and_propagates_serve_error() {
        let mut cfg = Config::default();
        // An unparseable bind address makes the ops-listener bind fail; that
        // path must log rather than panic and must not stop the API loop.
        cfg.metrics_addr = "not-a-valid-addr:99999".into();
        cfg.api_addr = "127.0.0.1:18433".into();

        let res = run_serve_with(cfg, |_, _| {
            Err(std::io::Error::new(std::io::ErrorKind::AddrInUse, "boom"))
        });
        let e = res.unwrap_err();
        assert!(e.to_string().contains("serve failed"), "{e}");
    }

    #[test]
    fn run_serve_propagates_bind_failure_from_the_real_loop() {
        // Point the API listener at an unparseable address so the real `serve`
        // bind fails immediately instead of blocking on accept. The config
        // file is used (not env) to avoid racing other tests.
        let dir = std::env::temp_dir().join(format!("bidx-serve-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("bad.toml");
        std::fs::write(
            &cfg_path,
            "api_addr = \"not-a-valid-addr:99999\"\nmetrics_addr = \"127.0.0.1:19101\"\n",
        )
        .unwrap();

        let e = run_serve(ServeArgs { config: Some(cfg_path) }).unwrap_err();
        assert!(e.to_string().contains("serve failed"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
