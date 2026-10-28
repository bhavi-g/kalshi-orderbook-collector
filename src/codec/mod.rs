//! The one exchange-specific seam. Everything else in the collector is generic
//! over `ExchangeCodec`, so adding an exchange (e.g. Polymarket) means writing a
//! new adapter and nothing more.

pub mod kalshi;
pub mod polymarket;

use chrono::{DateTime, Utc};

use crate::model::NormalizedEvent;

#[allow(async_fn_in_trait)]
pub trait ExchangeCodec: Send + Sync + 'static {
    /// WebSocket URL to connect to.
    fn ws_url(&self) -> &str;
    /// The request path that must be signed for the WS handshake.
    fn ws_path(&self) -> &str;
    /// Auth headers for a request of `method` to `path` (key, value) pairs.
    fn auth_headers(&self, method: &str, path: &str) -> anyhow::Result<Vec<(String, String)>>;
    /// Discover regular (tradeable) symbols. `limit` stops discovery early once
    /// that many are found; `None` means discover the entire universe.
    async fn list_symbols(&self, limit: Option<usize>) -> anyhow::Result<Vec<String>>;
    /// Initial subscription frame for `markets` on a fresh connection.
    fn subscribe_frame(&self, id: u64, markets: &[String]) -> String;
    /// Frame to add markets to an existing subscription `sid`.
    fn update_add_frame(&self, id: u64, sid: u64, markets: &[String]) -> String;
    /// Frame to remove markets from an existing subscription `sid`.
    fn update_remove_frame(&self, id: u64, sid: u64, markets: &[String]) -> String;
    /// Decode one raw text frame into zero or more normalized events.
    fn decode(&self, raw: &str, recv_at: DateTime<Utc>) -> anyhow::Result<Vec<NormalizedEvent>>;
}
