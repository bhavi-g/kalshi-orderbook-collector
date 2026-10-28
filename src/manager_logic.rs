//! Pure connection load-balancing and source-selection logic.

/// Split symbols into connection-sized groups.
pub fn shard(symbols: &[String], per_conn: usize) -> Vec<Vec<String>> {
    if per_conn == 0 {
        return vec![symbols.to_vec()];
    }
    symbols.chunks(per_conn).map(|c| c.to_vec()).collect()
}

#[derive(Clone, Debug)]
pub struct ConnHealth {
    pub id: usize,
    pub healthy: bool,
    pub last_msg_age_ms: u64,
}

/// Pick the healthiest candidate connection (freshest last message).
pub fn choose_source(candidates: &[ConnHealth]) -> Option<usize> {
    candidates
        .iter()
        .filter(|c| c.healthy)
        .min_by_key(|c| c.last_msg_age_ms)
        .map(|c| c.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(n: usize) -> Vec<String> {
        (0..n).map(|i| i.to_string()).collect()
    }

    #[test]
    fn shard_splits_evenly() {
        assert_eq!(shard(&v(5), 2).len(), 3); // 2 + 2 + 1
    }

    #[test]
    fn shard_never_exceeds_cap() {
        for g in shard(&v(201), 200) {
            assert!(g.len() <= 200);
        }
    }

    #[test]
    fn source_prefers_healthy_freshest() {
        let c = [
            ConnHealth { id: 0, healthy: true, last_msg_age_ms: 50 },
            ConnHealth { id: 1, healthy: true, last_msg_age_ms: 5 },
            ConnHealth { id: 2, healthy: false, last_msg_age_ms: 1 },
        ];
        assert_eq!(choose_source(&c), Some(1));
    }

    #[test]
    fn source_none_when_all_unhealthy() {
        let c = [ConnHealth { id: 0, healthy: false, last_msg_age_ms: 1 }];
        assert_eq!(choose_source(&c), None);
    }
}
