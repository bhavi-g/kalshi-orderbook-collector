//! Kalshi order-book collector.
//!
//! Connects to Kalshi production over redundant authenticated WebSockets,
//! maintains a live order book per regular (non-MVE) market, and streams SBE
//! snapshots + deltas to stdout via the challenge's `AlignedSbeWriter`. Logs go
//! to stderr so stdout stays a clean binary stream. SIGINT flushes and exits.

mod book;
mod codec;
mod collector;
mod connection;
mod emit;
mod fixedpoint;
mod lifecycle;
mod manager_logic;
mod model;
mod monitor;
mod output;

use std::path::PathBuf;
use std::sync::Arc;

use codec::kalshi::KalshiCodec;
use codec::polymarket::PolymarketCodec;
use collector::{run_collector, CollectorConfig};
use output::spawn_writer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = CollectorConfig::from_env();
    let exchange = std::env::var("EXCHANGE").unwrap_or_else(|_| "kalshi".to_string());

    let (out_tx, out_rx) = tokio::sync::mpsc::channel(200_000);
    let writer = spawn_writer(PathBuf::from("/dev/stdout"), out_rx);

    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("[main] SIGINT received, shutting down");
        let _ = sd_tx.send(true);
    });

    // The only exchange-specific line: pick the adapter. The collector is generic.
    let result = match exchange.as_str() {
        "polymarket" => {
            eprintln!("[main] exchange: Polymarket");
            let codec = Arc::new(PolymarketCodec::new()?);
            run_collector(codec, cfg, out_tx, sd_rx).await
        }
        _ => {
            eprintln!("[main] exchange: Kalshi");
            let key_id = std::env::var("KALSHI_KEY_ID")
                .map_err(|_| anyhow::anyhow!("set KALSHI_KEY_ID"))?;
            let key_path = std::env::var("KALSHI_KEY_PATH")
                .map_err(|_| anyhow::anyhow!("set KALSHI_KEY_PATH"))?;
            let key_pem = std::fs::read_to_string(&key_path)
                .map_err(|e| anyhow::anyhow!("read key file {key_path}: {e}"))?;
            // Keys made at kalshi.com are production; set KALSHI_ENV=demo to override.
            let production = std::env::var("KALSHI_ENV").map(|v| v != "demo").unwrap_or(true);
            let codec = Arc::new(KalshiCodec::new(key_id, &key_pem, production)?);
            run_collector(codec, cfg, out_tx, sd_rx).await
        }
    };
    // run_collector owns the only OutMsg sender; it is dropped here, so the
    // writer drains, flushes the final chunk, and exits.
    match writer.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("[main] writer error: {e:#}"),
        Err(e) => eprintln!("[main] writer join error: {e:#}"),
    }
    eprintln!("[main] shutdown complete");
    result
}
