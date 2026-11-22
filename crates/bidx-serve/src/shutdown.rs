//! Graceful shutdown.
//!
//! On SIGTERM (what Kubernetes, ECS, Nomad and `docker stop` send) the
//! service:
//! 1. flips readiness to false (`/readyz` → 503) so load balancers stop
//!    routing new traffic,
//! 2. keeps serving for `drain_delay` while endpoint removal propagates,
//! 3. stops accepting, finishes in-flight requests, and exits.
//!
//! The signal handler only sets an atomic flag (async-signal-safe); a
//! normal thread polls it and runs the drain sequence.

use crate::health::Health;
use crate::metrics::Metrics;
use parking_lot::Mutex;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

static SIGNALED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    SIGNALED.store(true, Ordering::SeqCst);
}

/// Route SIGTERM and SIGINT to the shutdown flag.
pub fn install_signal_handlers() -> std::io::Result<()> {
    for sig in [libc::SIGTERM, libc::SIGINT] {
        // SAFETY: `on_signal` only performs an atomic store, which is
        // async-signal-safe; the handler has the C ABI `signal` expects.
        let prev = unsafe {
            libc::signal(
                sig,
                on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t,
            )
        };
        if prev == libc::SIG_ERR {
            // coverage-exempt: signal() only fails for invalid signal numbers.
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn signaled() -> bool {
    SIGNALED.load(Ordering::SeqCst)
}

/// Block until SIGTERM/SIGINT has been received.
pub fn wait_for_signal(poll: Duration) {
    while !signaled() {
        std::thread::sleep(poll);
    }
}

/// Stop flag shared by listeners, plus the addresses to poke so blocked
/// `accept` calls return promptly.
#[derive(Default)]
pub struct Shutdown {
    stopping: AtomicBool,
    wake: Mutex<Vec<SocketAddr>>,
}

impl Shutdown {
    pub fn new() -> Self {
        Shutdown::default()
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Record a listener address to wake on [`trigger`](Self::trigger).
    pub fn register(&self, addr: SocketAddr) {
        self.wake.lock().push(wake_target(addr));
    }

    /// Stop all listeners. Idempotent.
    pub fn trigger(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let addrs: Vec<SocketAddr> = self.wake.lock().clone();
        for addr in addrs {
            let _ = TcpStream::connect_timeout(&addr, Duration::from_secs(1));
        }
    }

    /// The full drain sequence: report not-ready, keep serving for `delay`,
    /// then stop the listeners.
    pub fn drain(&self, health: &Health, metrics: &Metrics, delay: Duration) {
        tracing::info!(
            drain_delay_secs = delay.as_secs(),
            "shutdown requested; draining"
        );
        health.set_draining(true);
        metrics.set_draining(true);
        std::thread::sleep(delay);
        self.trigger();
    }
}

/// A listener bound to the unspecified address is reachable on loopback.
fn wake_target(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), addr.port())
        }
        _ => addr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn sigterm_sets_the_flag_and_wait_returns() {
        install_signal_handlers().unwrap();
        // SAFETY: raising a signal we just installed a handler for.
        unsafe {
            libc::raise(libc::SIGTERM);
        }
        wait_for_signal(Duration::from_millis(1));
        assert!(signaled());
    }

    #[test]
    fn wake_target_maps_unspecified_to_loopback() {
        let v4: SocketAddr = "0.0.0.0:8432".parse().unwrap();
        assert_eq!(wake_target(v4), "127.0.0.1:8432".parse().unwrap());
        let v6: SocketAddr = "[::]:8432".parse().unwrap();
        assert_eq!(wake_target(v6), "[::1]:8432".parse().unwrap());
        let concrete: SocketAddr = "10.1.2.3:1".parse().unwrap();
        assert_eq!(wake_target(concrete), concrete);
    }

    #[test]
    fn trigger_sets_stopping_and_wakes_registered_listeners() {
        let l = TcpListener::bind("0.0.0.0:0").unwrap();
        let s = Shutdown::new();
        s.register(l.local_addr().unwrap());
        // Also register a dead address: trigger must not fail on it.
        s.register("127.0.0.1:1".parse().unwrap());
        assert!(!s.is_stopping());
        let h = std::thread::spawn(move || l.accept().is_ok());
        s.trigger();
        assert!(s.is_stopping());
        assert!(h.join().unwrap(), "blocked accept was woken");
        s.trigger(); // idempotent
    }

    #[test]
    fn drain_reports_not_ready_before_stopping() {
        let s = Shutdown::new();
        let health = Health::new();
        health.set_clickhouse_ok(true);
        health.set_indexed_height(Some(1));
        let metrics = Metrics::new();
        s.drain(&health, &metrics, Duration::from_millis(1));
        assert!(!health.ready());
        assert!(s.is_stopping());
        assert!(metrics.render().contains("bidx_draining 1\n"));
    }
}
