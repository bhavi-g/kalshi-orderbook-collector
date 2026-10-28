//! Per-connection sequence-gap detection and per-symbol update-rate tracking.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqStatus {
    First,
    Ok,
    Gap { missing: u64 },
}

/// Tracks the per-subscription `seq` to detect missed messages.
#[derive(Default)]
pub struct SeqTracker {
    last: Option<u64>,
}

impl SeqTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check(&mut self, seq: u64) -> SeqStatus {
        let status = match self.last {
            None => SeqStatus::First,
            Some(l) if seq == l + 1 => SeqStatus::Ok,
            Some(l) if seq > l + 1 => SeqStatus::Gap { missing: seq - l - 1 },
            // out-of-order or duplicate: don't false-trigger a resync
            Some(_) => SeqStatus::Ok,
        };
        // Only advance on forward progress.
        if self.last.map_or(true, |l| seq > l) {
            self.last = Some(seq);
        }
        status
    }
}

/// Sliding-window event-rate tracker keyed by symbol.
pub struct RateTracker {
    window: Duration,
    events: HashMap<String, VecDeque<Instant>>,
}

impl RateTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            events: HashMap::new(),
        }
    }

    pub fn record(&mut self, key: &str, now: Instant) {
        let q = self.events.entry(key.to_string()).or_default();
        q.push_back(now);
        if let Some(cutoff) = now.checked_sub(self.window) {
            while q.front().map_or(false, |&t| t < cutoff) {
                q.pop_front();
            }
        }
    }

    pub fn rate_per_sec(&self, key: &str, now: Instant) -> f64 {
        let Some(q) = self.events.get(key) else {
            return 0.0;
        };
        let cutoff = now.checked_sub(self.window);
        let count = q
            .iter()
            .filter(|&&t| cutoff.map_or(true, |c| t >= c))
            .count();
        count as f64 / self.window.as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_first_then_ok() {
        let mut t = SeqTracker::new();
        assert!(matches!(t.check(5), SeqStatus::First));
        assert!(matches!(t.check(6), SeqStatus::Ok));
    }

    #[test]
    fn seq_detects_gap() {
        let mut t = SeqTracker::new();
        t.check(1);
        assert!(matches!(t.check(4), SeqStatus::Gap { missing: 2 }));
    }

    #[test]
    fn rate_counts_then_decays() {
        let mut r = RateTracker::new(Duration::from_secs(1));
        let t0 = Instant::now();
        r.record("S", t0);
        r.record("S", t0);
        assert!(r.rate_per_sec("S", t0) > 0.0);
        let later = t0 + Duration::from_secs(5);
        assert_eq!(r.rate_per_sec("S", later), 0.0);
    }
}
