//! In-memory order book: absolute price levels maintained from snapshots and
//! signed deltas.

use std::collections::BTreeMap;

use engineering_challenge::Side;

use crate::model::Fp;

/// A single market's two-sided book, each side a `price_level -> abs_qty` map.
#[derive(Default)]
pub struct OrderBook {
    bids: BTreeMap<Fp, Fp>,
    asks: BTreeMap<Fp, Fp>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the entire book with the snapshot's levels (drops zero levels).
    pub fn apply_snapshot(&mut self, bids: &[(Fp, Fp)], asks: &[(Fp, Fp)]) {
        self.bids.clear();
        self.asks.clear();
        for &(p, q) in bids {
            if q > 0 {
                self.bids.insert(p, q);
            }
        }
        for &(p, q) in asks {
            if q > 0 {
                self.asks.insert(p, q);
            }
        }
    }

    /// Apply a signed change at a level; return the resulting absolute quantity
    /// (clamped at 0). A result of 0 removes the level.
    pub fn apply_delta(&mut self, side: Side, price: Fp, change: Fp) -> Fp {
        let map = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let cur = map.get(&price).copied().unwrap_or(0);
        let next = (cur + change).max(0);
        if next == 0 {
            map.remove(&price);
        } else {
            map.insert(price, next);
        }
        next
    }

    /// Current levels, each side ascending by price.
    pub fn levels(&self) -> (Vec<(Fp, Fp)>, Vec<(Fp, Fp)>) {
        (
            self.bids.iter().map(|(&p, &q)| (p, q)).collect(),
            self.asks.iter().map(|(&p, &q)| (p, q)).collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_on_empty_creates_level() {
        let mut b = OrderBook::new();
        assert_eq!(b.apply_delta(Side::Buy, 590_000, 5_000_000), 5_000_000);
    }

    #[test]
    fn delta_accumulates_then_deletes() {
        let mut b = OrderBook::new();
        b.apply_delta(Side::Sell, 700_000, 10_000_000);
        assert_eq!(b.apply_delta(Side::Sell, 700_000, -10_000_000), 0);
        let (_, asks) = b.levels();
        assert!(asks.iter().all(|(p, _)| *p != 700_000));
    }

    #[test]
    fn readd_after_delete() {
        let mut b = OrderBook::new();
        b.apply_delta(Side::Buy, 100, 1_000_000);
        b.apply_delta(Side::Buy, 100, -1_000_000);
        assert_eq!(b.apply_delta(Side::Buy, 100, 2_000_000), 2_000_000);
    }

    #[test]
    fn negative_result_clamps_to_zero() {
        let mut b = OrderBook::new();
        assert_eq!(b.apply_delta(Side::Buy, 100, -5_000_000), 0);
    }

    #[test]
    fn snapshot_replaces_state() {
        let mut b = OrderBook::new();
        b.apply_delta(Side::Buy, 100, 9);
        b.apply_snapshot(&[(200, 3_000_000)], &[(800, 1_000_000)]);
        let (bids, asks) = b.levels();
        assert_eq!(bids, vec![(200, 3_000_000)]);
        assert_eq!(asks, vec![(800, 1_000_000)]);
    }
}
