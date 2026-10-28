//! Polymarket adapter: public CLOB market channel (no auth). Discovers outcome
//! token ids via the Gamma API and decodes `book` + `price_changes` messages.
//!
//! Demonstrates the generic core with a second exchange whose model differs from
//! Kalshi's: Polymarket sends **absolute** level sizes (not signed changes) and
//! has no per-connection sequence number.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::codec::ExchangeCodec;
use crate::fixedpoint::decimal_to_fp;
use crate::model::{Amount, NormalizedEvent};
use engineering_challenge::Side;

const WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA: &str = "https://gamma-api.polymarket.com/markets";

pub struct PolymarketCodec {
    http: reqwest::Client,
}

impl PolymarketCodec {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
        })
    }
}

fn ts_from_ms_str(s: Option<&str>, fallback: DateTime<Utc>) -> DateTime<Utc> {
    s.and_then(|x| x.parse::<i64>().ok())
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or(fallback)
}

fn parse_book_levels(arr: &Value) -> Vec<(i64, i64)> {
    arr.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|l| {
                    let p = decimal_to_fp(l["price"].as_str()?).ok()?;
                    let s = decimal_to_fp(l["size"].as_str()?).ok()?;
                    Some((p, s))
                })
                .collect()
        })
        .unwrap_or_default()
}

impl ExchangeCodec for PolymarketCodec {
    fn ws_url(&self) -> &str {
        WS
    }

    fn ws_path(&self) -> &str {
        "/ws/market"
    }

    fn auth_headers(&self, _method: &str, _path: &str) -> Result<Vec<(String, String)>> {
        Ok(Vec::new()) // public channel; no auth
    }

    async fn list_symbols(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut tokens = Vec::new();
        for page in 0..50 {
            let url = format!(
                "{GAMMA}?closed=false&active=true&limit=100&offset={}&order=volume24hr&ascending=false",
                page * 100
            );
            let body: Value = self
                .http
                .get(&url)
                .send()
                .await
                .context("gamma request")?
                .json()
                .await
                .context("gamma json")?;
            let arr = body.as_array().cloned().unwrap_or_default();
            if arr.is_empty() {
                break;
            }
            for m in &arr {
                if let Some(s) = m["clobTokenIds"].as_str() {
                    if let Ok(ids) = serde_json::from_str::<Vec<String>>(s) {
                        tokens.extend(ids);
                    }
                }
            }
            if let Some(limit) = limit {
                if tokens.len() >= limit {
                    break;
                }
            }
        }
        tokens.sort();
        tokens.dedup();
        Ok(tokens)
    }

    fn subscribe_frame(&self, _id: u64, markets: &[String]) -> String {
        serde_json::json!({ "assets_ids": markets, "type": "market" }).to_string()
    }

    fn update_add_frame(&self, _id: u64, _sid: u64, markets: &[String]) -> String {
        // Best effort: re-subscribe the added assets on the same socket.
        serde_json::json!({ "assets_ids": markets, "type": "market" }).to_string()
    }

    fn update_remove_frame(&self, _id: u64, _sid: u64, _markets: &[String]) -> String {
        // Market channel has no single-asset unsubscribe; rely on socket recycle.
        String::new()
    }

    fn decode(&self, raw: &str, recv_at: DateTime<Utc>) -> Result<Vec<NormalizedEvent>> {
        let v: Value = serde_json::from_str(raw)?;
        // Initial books can arrive as an array; everything else is a single object.
        let items: Vec<Value> = match v {
            Value::Array(a) => a,
            other => vec![other],
        };
        let mut out = Vec::new();
        for it in &items {
            if let Some(changes) = it["price_changes"].as_array() {
                let exch_ts = ts_from_ms_str(it["timestamp"].as_str(), recv_at);
                for ch in changes {
                    let Some(asset) = ch["asset_id"].as_str() else { continue };
                    let (Ok(price), Ok(size)) = (
                        decimal_to_fp(ch["price"].as_str().unwrap_or("0")),
                        decimal_to_fp(ch["size"].as_str().unwrap_or("0")),
                    ) else {
                        continue;
                    };
                    let side = match ch["side"].as_str().unwrap_or("") {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        _ => continue,
                    };
                    out.push(NormalizedEvent::Delta {
                        symbol: asset.to_string(),
                        side,
                        price_level: price,
                        amount: Amount::Absolute(size), // Polymarket size is absolute
                        exch_ts,
                        recv_at,
                        seq: 0, // Polymarket has no per-connection sequence
                    });
                }
            } else if it.get("bids").is_some() || it.get("asks").is_some() {
                let Some(asset) = it["asset_id"].as_str() else { continue };
                let exch_ts = ts_from_ms_str(it["timestamp"].as_str(), recv_at);
                out.push(NormalizedEvent::Snapshot {
                    symbol: asset.to_string(),
                    bids: parse_book_levels(&it["bids"]),
                    asks: parse_book_levels(&it["asks"]),
                    exch_ts,
                    recv_at,
                    seq: 0,
                });
            }
            // else: last_trade_price / tick_size_change — ignored.
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_price_change_as_absolute() {
        let codec = PolymarketCodec::new().unwrap();
        let raw = r#"{"market":"0x","price_changes":[{"asset_id":"TOK","price":"0.43","size":"18865.85","side":"BUY"}],"timestamp":"1793168332063"}"#;
        let evs = codec.decode(raw, Utc::now()).unwrap();
        match &evs[0] {
            NormalizedEvent::Delta { symbol, side, price_level, amount, .. } => {
                assert_eq!(symbol, "TOK");
                assert_eq!(*side, Side::Buy);
                assert_eq!(*price_level, 430_000);
                assert_eq!(*amount, Amount::Absolute(18_865_850_000));
            }
            _ => panic!("expected delta"),
        }
    }

    #[test]
    fn decodes_book_snapshot() {
        let codec = PolymarketCodec::new().unwrap();
        let raw = r#"{"asset_id":"TOK","timestamp":"1","bids":[{"price":"0.01","size":"100"}],"asks":[{"price":"0.99","size":"50"}]}"#;
        let evs = codec.decode(raw, Utc::now()).unwrap();
        match &evs[0] {
            NormalizedEvent::Snapshot { symbol, bids, asks, .. } => {
                assert_eq!(symbol, "TOK");
                assert_eq!(bids[0], (10_000, 100_000_000));
                assert_eq!(asks[0], (990_000, 50_000_000));
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn decodes_initial_book_array() {
        let codec = PolymarketCodec::new().unwrap();
        let raw = r#"[{"asset_id":"A","bids":[{"price":"0.5","size":"1"}],"asks":[]},{"asset_id":"B","bids":[],"asks":[{"price":"0.5","size":"2"}]}]"#;
        let evs = codec.decode(raw, Utc::now()).unwrap();
        assert_eq!(evs.len(), 2);
    }
}
