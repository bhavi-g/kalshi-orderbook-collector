//! Exchange-agnostic normalized events produced by any `ExchangeCodec`.
//!
//! The codec has already resolved the exchange's quoting convention into a
//! standard bid/ask book in fixed point, so the collector core never sees
//! anything exchange-specific (no YES/NO, no dollars, no `delta_fp`).

use chrono::{DateTime, Utc};
use engineering_challenge::Side;

/// Fixed-point value, `1_000_000 = 1 unit`.
pub type Fp = i64;

/// How an exchange expresses a level update. Kalshi sends signed changes;
/// Polymarket sends the new absolute size. The book applies either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Amount {
    /// Add this signed change to the current level quantity.
    Relative(Fp),
    /// Set the level to this absolute quantity.
    Absolute(Fp),
}

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
    /// An update to one price level on one side (relative or absolute).
    Delta {
        symbol: String,
        side: Side,
        price_level: Fp,
        amount: Amount,
        exch_ts: DateTime<Utc>,
        recv_at: DateTime<Utc>,
        seq: u64,
    },
    /// Acknowledgement that a subscription is live (carries its server `sid`).
    SubscribeAck { sid: u64 },
    /// Anything we don't act on (heartbeats, errors, etc.).
    Other,
}
