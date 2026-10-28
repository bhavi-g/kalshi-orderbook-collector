//! Exchange-agnostic normalized events produced by any `ExchangeCodec`.
//!
//! The codec has already resolved the exchange's quoting convention into a
//! standard bid/ask book in fixed point, so the collector core never sees
//! anything exchange-specific (no YES/NO, no dollars, no `delta_fp`).

use chrono::{DateTime, Utc};
use engineering_challenge::Side;

/// Fixed-point value, `1_000_000 = 1 unit`.
pub type Fp = i64;

#[derive(Debug, Clone)]
pub enum NormalizedEvent {
    /// Full book for one symbol (absolute quantities).
    Snapshot {
        symbol: String,
        bids: Vec<(Fp, Fp)>,
        asks: Vec<(Fp, Fp)>,
        exch_ts: DateTime<Utc>,
        recv_at: DateTime<Utc>,
        seq: u64,
    },
    /// A signed change to one price level on one side.
    Delta {
        symbol: String,
        side: Side,
        price_level: Fp,
        change: Fp,
        exch_ts: DateTime<Utc>,
        recv_at: DateTime<Utc>,
        seq: u64,
    },
    /// Acknowledgement that a subscription is live (carries its server `sid`).
    SubscribeAck { sid: u64 },
    /// Anything we don't act on (heartbeats, errors, etc.).
    Other,
}
