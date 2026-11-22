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
//! * [`service`] — the HTTP router tying it all together.
//! * [`limits`] — API-key auth and token-bucket rate limits.
//! * [`cache`] — in-process response cache.
//! * [`server`] — worker-pool HTTP/1.1 server with keep-alive and shedding.
//! * [`shutdown`] — SIGTERM handling and graceful drain.

pub mod address;
pub mod cache;
pub mod config;
pub mod electrum;
pub mod esplora;
pub mod health;
pub mod limits;
pub mod metrics;
pub mod server;
pub mod service;
pub mod shutdown;
pub mod store;

#[cfg(test)]
mod testutil;
