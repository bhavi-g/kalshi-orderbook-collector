//! Build the template's SBE domain messages from normalized book state.

use chrono::{DateTime, Utc};
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use engineering_challenge::Side;

pub fn build_snapshot(
    symbol: &str,
    bids: &[(i64, i64)],
    asks: &[(i64, i64)],
    exch_ts: DateTime<Utc>,
    recv_at: DateTime<Utc>,
) -> SnapshotDomain {
    SnapshotDomain {
        received_at: recv_at,
        exchange_timestamp: exch_ts,
        bids: bids
            .iter()
            .map(|&(price_level, quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: asks
            .iter()
            .map(|&(price_level, quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: symbol.to_string(),
    }
}

pub fn build_update(
    symbol: &str,
    side: Side,
    price_level: i64,
    abs_qty: i64,
    exch_ts: DateTime<Utc>,
    recv_at: DateTime<Utc>,
) -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        backtest_timestamp: recv_at,
        exchange_timestamp: exch_ts,
        price_level,
        quantity: abs_qty,
        side,
        symbol: symbol.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[test]
    fn build_update_maps_fields() {
        let u = build_update("EX", Side::Sell, 410_000, 702_000_000, ts(), ts());
        assert_eq!(u.price_level, 410_000);
        assert_eq!(u.quantity, 702_000_000);
        assert_eq!(u.side, Side::Sell);
        assert_eq!(u.symbol, "EX");
    }

    #[test]
    fn build_snapshot_maps_levels() {
        let s = build_snapshot("EX", &[(100, 5)], &[(900, 7)], ts(), ts());
        assert_eq!(s.bids.len(), 1);
        assert_eq!(s.asks[0].price_level, 900);
        assert_eq!(s.asks[0].quantity, 7);
    }
}
