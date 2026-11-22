//! Serving layer for the bitcoindex pipeline.
//!
//! This crate turns the ClickHouse index into a queryable service:
//! * [`address`] — base58check/bech32 codecs and Electrum scripthash math.
//! * [`store`] — read-only ClickHouse access (parameterized SQL).
//! * [`esplora`] — Esplora-compatible REST responses.
//! * [`electrum`] — Electrum JSON-RPC method handling.
//! * [`metrics`] — Prometheus text exposition.
//! * [`health`] — liveness/readiness state.
//! * [`config`] — file + env configuration.
//! * [`service`] — a dependency-free HTTP router tying it all together.

pub mod address;
pub mod config;
pub mod electrum;
pub mod esplora;
pub mod health;
pub mod metrics;
pub mod service;
pub mod store;

#[cfg(test)]
mod testutil;
