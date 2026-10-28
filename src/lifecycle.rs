//! Symbol discovery parsing and subscription diffing.

use std::collections::HashSet;

use serde_json::Value;

/// Extract market tickers from one `/markets` REST page, plus the next cursor.
///
/// Returns both the page's own `markets[].ticker` and any nested
/// `market_ticker` values (MVE markets embed their regular constituent markets),
/// so even an MVE-dominated page yields regular markets to harvest.
pub fn parse_market_tickers(page: &Value) -> (Vec<String>, Option<String>) {
    let mut out = Vec::new();
    if let Some(arr) = page["markets"].as_array() {
        for m in arr {
            if let Some(t) = m["ticker"].as_str() {
                out.push(t.to_string());
            }
            collect_nested_market_tickers(m, &mut out);
        }
    }
    let cursor = page["cursor"]
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_string);
    (out, cursor)
}

fn collect_nested_market_tickers(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            for (k, val) in map {
                if k == "market_ticker" {
                    if let Some(s) = val.as_str() {
                        out.push(s.to_string());
                    }
                } else {
                    collect_nested_market_tickers(val, out);
                }
            }
        }
        Value::Array(a) => {
            for val in a {
                collect_nested_market_tickers(val, out);
            }
        }
        _ => {}
    }
}

/// Keep only regular (non-MVE) markets, de-duplicated, order-preserving.
pub fn filter_regular<I: IntoIterator<Item = String>>(tickers: I) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for t in tickers {
        if !t.starts_with("KXMVE") && seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}

/// Compute (to add, to remove) between the current and target symbol sets.
pub fn diff(current: &HashSet<String>, target: &HashSet<String>) -> (Vec<String>, Vec<String>) {
    let add = target.difference(current).cloned().collect();
    let remove = current.difference(target).cloned().collect();
    (add, remove)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_extracts_tickers_and_cursor() {
        let page = json!({
            "markets": [
                {"ticker": "KXNBA-GSW", "sub_markets": [{"market_ticker": "KXSUB-1"}]},
                {"ticker": "KXMVEX-9"}
            ],
            "cursor": "abc"
        });
        let (tickers, cursor) = parse_market_tickers(&page);
        assert!(tickers.contains(&"KXNBA-GSW".to_string()));
        assert!(tickers.contains(&"KXSUB-1".to_string()));
        assert_eq!(cursor, Some("abc".to_string()));
    }

    #[test]
    fn filter_drops_mve() {
        assert_eq!(
            filter_regular(["KXMVEX-1".to_string(), "KXNBA-GSW".to_string()]),
            vec!["KXNBA-GSW".to_string()]
        );
    }

    #[test]
    fn diff_adds_and_removes() {
        let current: HashSet<String> = ["A", "B"].iter().map(|s| s.to_string()).collect();
        let target: HashSet<String> = ["B", "C"].iter().map(|s| s.to_string()).collect();
        let (mut add, mut remove) = diff(&current, &target);
        add.sort();
        remove.sort();
        assert_eq!(add, vec!["C".to_string()]);
        assert_eq!(remove, vec!["A".to_string()]);
    }
}
