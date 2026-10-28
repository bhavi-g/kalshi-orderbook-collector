//! Kalshi adapter: Ed25519 auth signing, REST symbol discovery, subscription
//! frames, and WS decode including the YES/NO -> bid/ask transform.

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use ed25519_dalek::{pkcs8::DecodePrivateKey, Signer, SigningKey};
use engineering_challenge::Side;
use serde_json::Value;

use crate::codec::ExchangeCodec;
use crate::fixedpoint::{ask_price_fp, decimal_to_fp};
use crate::lifecycle::{filter_regular, parse_market_tickers};
use crate::model::NormalizedEvent;

const PROD_WS: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const PROD_REST: &str = "https://api.elections.kalshi.com";
const DEMO_WS: &str = "wss://demo-api.kalshi.co/trade-api/ws/v2";
const DEMO_REST: &str = "https://demo-api.kalshi.co";
const WS_PATH: &str = "/trade-api/ws/v2";
const MARKETS_PATH: &str = "/trade-api/v2/markets";

pub struct KalshiCodec {
    key_id: String,
    signing_key: SigningKey,
    http: reqwest::Client,
    ws_url: String,
    rest_base: String,
}

impl KalshiCodec {
    pub fn new(key_id: String, key_pem: &str, production: bool) -> Result<Self> {
        let signing_key = SigningKey::from_pkcs8_pem(key_pem)
            .map_err(|e| anyhow!("parse Ed25519 PKCS#8 PEM: {e}"))?;
        Ok(Self::from_parts(key_id, signing_key, production))
    }

    pub(crate) fn from_parts(key_id: String, signing_key: SigningKey, production: bool) -> Self {
        let (ws_url, rest_base) = if production {
            (PROD_WS, PROD_REST)
        } else {
            (DEMO_WS, DEMO_REST)
        };
        Self {
            key_id,
            signing_key,
            http: reqwest::Client::new(),
            ws_url: ws_url.to_string(),
            rest_base: rest_base.to_string(),
        }
    }

    /// Sign `"{ts_ms}{METHOD}{path}"` with Ed25519, base64 the raw signature.
    pub(crate) fn sign(&self, ts_ms: i64, method: &str, path: &str) -> String {
        let msg = format!("{ts_ms}{method}{path}");
        let sig = self.signing_key.sign(msg.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
    }
}

fn exch_ts_from_ms(ms: i64, fallback: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(fallback)
}

/// Convert a Kalshi level array `[["0.59","798.00"], ...]` into fixed-point
/// `(price, size)` levels. For the NO side, price maps to the YES ask `1 - q`.
fn parse_levels(arr: &Value, as_ask: bool) -> Result<Vec<(i64, i64)>> {
    let mut out = Vec::new();
    if let Some(a) = arr.as_array() {
        for lvl in a {
            let pair = lvl.as_array().ok_or_else(|| anyhow!("level not array"))?;
            if pair.len() < 2 {
                continue;
            }
            let price = decimal_to_fp(pair[0].as_str().unwrap_or("0"))?;
            let size = decimal_to_fp(pair[1].as_str().unwrap_or("0"))?;
            let p = if as_ask { ask_price_fp(price) } else { price };
            out.push((p, size));
        }
    }
    Ok(out)
}

impl ExchangeCodec for KalshiCodec {
    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    fn ws_path(&self) -> &str {
        WS_PATH
    }

    fn auth_headers(&self, method: &str, path: &str) -> Result<Vec<(String, String)>> {
        let ts = Utc::now().timestamp_millis();
        let sig = self.sign(ts, method, path);
        Ok(vec![
            ("KALSHI-ACCESS-KEY".to_string(), self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP".to_string(), ts.to_string()),
            ("KALSHI-ACCESS-SIGNATURE".to_string(), sig),
        ])
    }

    async fn list_symbols(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let mut all: Vec<String> = Vec::new();
        let mut cursor = String::new();
        // Page through the open-markets listing (bounded by a safety cap), stopping
        // early once we have `limit` regular markets.
        for _page in 0..200 {
            let headers = self.auth_headers("GET", MARKETS_PATH)?;
            let mut url = format!("{}{}?limit=1000&status=open", self.rest_base, MARKETS_PATH);
            if !cursor.is_empty() {
                url.push_str(&format!("&cursor={cursor}"));
            }
            let mut req = self.http.get(&url);
            for (k, v) in &headers {
                req = req.header(k, v);
            }
            let resp = req.send().await.context("markets request")?;
            let status = resp.status();
            let body: Value = resp.json().await.context("markets json")?;
            anyhow::ensure!(status.is_success(), "markets failed: {status}");
            let (tickers, next) = parse_market_tickers(&body);
            all.extend(tickers);
            if let Some(limit) = limit {
                if filter_regular(all.clone()).len() >= limit {
                    break;
                }
            }
            match next {
                Some(c) => cursor = c,
                None => break,
            }
        }
        Ok(filter_regular(all))
    }

    fn subscribe_frame(&self, id: u64, markets: &[String]) -> String {
        serde_json::json!({
            "id": id,
            "cmd": "subscribe",
            "params": { "channels": ["orderbook_delta"], "market_tickers": markets }
        })
        .to_string()
    }

    fn update_add_frame(&self, id: u64, sid: u64, markets: &[String]) -> String {
        serde_json::json!({
            "id": id,
            "cmd": "update_subscription",
            "params": { "sids": [sid], "action": "add_markets", "market_tickers": markets }
        })
        .to_string()
    }

    fn update_remove_frame(&self, id: u64, sid: u64, markets: &[String]) -> String {
        serde_json::json!({
            "id": id,
            "cmd": "update_subscription",
            "params": { "sids": [sid], "action": "delete_markets", "market_tickers": markets }
        })
        .to_string()
    }

    fn decode(&self, raw: &str, recv_at: DateTime<Utc>) -> Result<Vec<NormalizedEvent>> {
        let v: Value = serde_json::from_str(raw)?;
        let typ = v["type"].as_str().unwrap_or("");
        let seq = v["seq"].as_u64().unwrap_or(0);
        let sid = v["sid"].as_u64().unwrap_or(0);
        match typ {
            "orderbook_snapshot" => {
                let msg = &v["msg"];
                let symbol = msg["market_ticker"].as_str().unwrap_or("").to_string();
                let exch_ts = exch_ts_from_ms(
                    v["sending_ts_ms"].as_i64().unwrap_or(recv_at.timestamp_millis()),
                    recv_at,
                );
                let bids = parse_levels(&msg["yes_dollars_fp"], false)?;
                let asks = parse_levels(&msg["no_dollars_fp"], true)?;
                Ok(vec![NormalizedEvent::Snapshot {
                    symbol,
                    bids,
                    asks,
                    exch_ts,
                    recv_at,
                    seq,
                }])
            }
            "orderbook_delta" => {
                let msg = &v["msg"];
                let symbol = msg["market_ticker"].as_str().unwrap_or("").to_string();
                let exch_ts = exch_ts_from_ms(
                    msg["ts_ms"].as_i64().unwrap_or(recv_at.timestamp_millis()),
                    recv_at,
                );
                let price_fp = decimal_to_fp(msg["price_dollars"].as_str().unwrap_or("0"))?;
                let change = decimal_to_fp(msg["delta_fp"].as_str().unwrap_or("0"))?;
                let (side, price_level) = match msg["side"].as_str().unwrap_or("") {
                    "yes" => (Side::Buy, price_fp),
                    "no" => (Side::Sell, ask_price_fp(price_fp)),
                    other => return Err(anyhow!("unknown side `{other}`")),
                };
                Ok(vec![NormalizedEvent::Delta {
                    symbol,
                    side,
                    price_level,
                    amount: crate::model::Amount::Relative(change),
                    exch_ts,
                    recv_at,
                    seq,
                }])
            }
            "subscribed" => {
                let sub_sid = v["msg"]["sid"].as_u64().unwrap_or(sid);
                Ok(vec![NormalizedEvent::SubscribeAck { sid: sub_sid }])
            }
            _ => Ok(vec![NormalizedEvent::Other]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Verifier, VerifyingKey};

    // A throwaway Ed25519 PKCS#8 PEM (not a real credential) to exercise parsing.
    const TEST_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIJGFV/++bGVFCnL0u0t7P9S597shY6j0EkZyn0wzjKhX\n-----END PRIVATE KEY-----\n";

    fn test_codec() -> (KalshiCodec, VerifyingKey) {
        let mut seed = [7u8; 32];
        seed[0] = 1;
        let sk = SigningKey::from_bytes(&seed);
        let vk = sk.verifying_key();
        (
            KalshiCodec::from_parts("kid-123".to_string(), sk, true),
            vk,
        )
    }

    #[test]
    fn new_parses_real_pem() {
        assert!(KalshiCodec::new("kid".to_string(), TEST_PEM, true).is_ok());
    }

    #[test]
    fn signature_verifies_with_public_key() {
        let (codec, vk) = test_codec();
        let sig_b64 = codec.sign(1_700_000_000_000, "GET", WS_PATH);
        let raw = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .unwrap();
        let sig = ed25519_dalek::Signature::from_slice(&raw).unwrap();
        let msg = format!("1700000000000GET{WS_PATH}");
        assert!(vk.verify(msg.as_bytes(), &sig).is_ok());
    }

    #[test]
    fn auth_headers_present_and_ms_timestamp() {
        let (codec, _) = test_codec();
        let h = codec.auth_headers("GET", WS_PATH).unwrap();
        let get = |k: &str| h.iter().find(|(hk, _)| hk == k).map(|(_, v)| v.clone());
        assert_eq!(get("KALSHI-ACCESS-KEY").as_deref(), Some("kid-123"));
        let ts: i64 = get("KALSHI-ACCESS-TIMESTAMP").unwrap().parse().unwrap();
        assert!(ts >= 1_000_000_000_000, "timestamp should be ms");
        assert!(!get("KALSHI-ACCESS-SIGNATURE").unwrap().is_empty());
    }

    #[test]
    fn decodes_snapshot_yes_bid_no_ask() {
        let (codec, _) = test_codec();
        let raw = r#"{"type":"orderbook_snapshot","sid":1,"seq":4,"msg":{"market_ticker":"KXT","yes_dollars_fp":[["0.5900","798.00"]],"no_dollars_fp":[["0.3000","703.00"]]},"sending_ts_ms":1793164663946}"#;
        let evs = codec.decode(raw, Utc::now()).unwrap();
        match &evs[0] {
            NormalizedEvent::Snapshot { bids, asks, seq, .. } => {
                assert_eq!(*seq, 4);
                assert_eq!(bids[0], (590_000, 798_000_000));
                assert_eq!(asks[0], (700_000, 703_000_000)); // 1 - 0.30
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn decodes_delta_no_side_to_ask_change() {
        let (codec, _) = test_codec();
        let raw = r#"{"type":"orderbook_delta","sid":1,"seq":9,"msg":{"market_ticker":"KXT","price_dollars":"0.5900","delta_fp":"-1.00","side":"no","ts_ms":1793164664481},"sending_ts_ms":1793164664487}"#;
        let evs = codec.decode(raw, Utc::now()).unwrap();
        match &evs[0] {
            NormalizedEvent::Delta { side, price_level, amount, .. } => {
                assert_eq!(*side, Side::Sell);
                assert_eq!(*price_level, 410_000); // 1 - 0.59
                assert_eq!(*amount, crate::model::Amount::Relative(-1_000_000));
            }
            _ => panic!("expected delta"),
        }
    }

    #[test]
    fn subscribe_frame_shape() {
        let (codec, _) = test_codec();
        let f = codec.subscribe_frame(1, &["A".to_string(), "B".to_string()]);
        let v: Value = serde_json::from_str(&f).unwrap();
        assert_eq!(v["cmd"], "subscribe");
        assert_eq!(v["params"]["channels"][0], "orderbook_delta");
        assert_eq!(v["params"]["market_tickers"][1], "B");
    }

    #[test]
    fn update_frames_shape() {
        let (codec, _) = test_codec();
        let add: Value = serde_json::from_str(&codec.update_add_frame(2, 5, &["A".into()])).unwrap();
        assert_eq!(add["params"]["action"], "add_markets");
        assert_eq!(add["params"]["sids"][0], 5);
        let rm: Value =
            serde_json::from_str(&codec.update_remove_frame(3, 5, &["A".into()])).unwrap();
        assert_eq!(rm["params"]["action"], "delete_markets");
    }
}
