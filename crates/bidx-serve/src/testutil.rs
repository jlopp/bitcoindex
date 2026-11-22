//! Test-only transport that routes SQL to canned responses by substring match.
//!
//! Multi-query handlers (e.g. `/tx/:txid` reads the tx, its outputs, and its
//! inputs) cannot be tested with a single canned body, so tests register a
//! list of `(substring, body)` routes and the first matching substring wins.
//! Every query is recorded so tests can assert on the generated SQL.

use crate::store::{Store, StoreError, Transport};
use std::sync::{Arc, Mutex};

pub struct RouteMock {
    routes: Vec<(String, String)>,
    default: String,
    calls: Mutex<Vec<String>>,
}

impl RouteMock {
    pub fn new(routes: &[(&str, &str)], default: &str) -> Self {
        RouteMock {
            routes: routes
                .iter()
                .map(|(m, b)| ((*m).to_string(), (*b).to_string()))
                .collect(),
            default: default.to_string(),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    /// True if any recorded query contains `needle`.
    pub fn saw(&self, needle: &str) -> bool {
        self.calls().iter().any(|c| c.contains(needle))
    }
}

struct Fwd(Arc<RouteMock>);

impl Transport for Fwd {
    fn query(&self, sql: &str) -> Result<String, StoreError> {
        self.0.calls.lock().unwrap().push(sql.to_string());
        for (m, body) in &self.0.routes {
            if sql.contains(m.as_str()) {
                return Ok(body.clone());
            }
        }
        Ok(self.0.default.clone())
    }
}

/// Build a `Store` over a routing mock, returning both.
pub fn store(routes: &[(&str, &str)], default: &str) -> (Store, Arc<RouteMock>) {
    let mock = Arc::new(RouteMock::new(routes, default));
    let s = Store::new(Box::new(Fwd(mock.clone())), "bitcoin");
    (s, mock)
}

/// A 64-char display-order hash hex from a repeating byte (for fixtures).
pub fn hex64(byte: u8) -> String {
    format!("{:02x}", byte).repeat(32)
}

/// Reverse a 32-byte display hash to wire order (what the index stores).
pub fn wire(display_hex: &str) -> String {
    crate::store::reverse_hex(display_hex).unwrap()
}
