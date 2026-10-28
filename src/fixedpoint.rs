//! Decimal-string <-> fixed-point (`1_000_000 = 1 unit`) conversion.
//!
//! Kalshi quotes prices and sizes as decimal strings (e.g. `"0.5900"`,
//! `"798.00"`, `"-1.00"`). The SBE output uses `i64` fixed point where
//! `1_000_000 = 1 USDC` (price) or `1 contract` (quantity).

use anyhow::{anyhow, Result};

/// Fixed-point representation of `1.0`.
pub const ONE: i64 = 1_000_000;

/// Parse a decimal string into fixed point (`round`/truncate to 6 dp).
///
/// Kalshi uses at most 4 fractional digits for price and 2 for size, so the
/// 6-digit fixed point is exact for all real inputs; extra digits truncate.
pub fn decimal_to_fp(s: &str) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty decimal"));
    }
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (int_part, frac_part) = rest.split_once('.').unwrap_or((rest, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(anyhow!("bad decimal `{s}`"));
    }
    let int_val: i64 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().map_err(|_| anyhow!("bad integer in `{s}`"))?
    };
    let mut frac = String::with_capacity(6);
    for (i, c) in frac_part.chars().enumerate() {
        if !c.is_ascii_digit() {
            return Err(anyhow!("bad fraction in `{s}`"));
        }
        if i < 6 {
            frac.push(c);
        }
    }
    while frac.len() < 6 {
        frac.push('0');
    }
    let frac_val: i64 = frac.parse().unwrap_or(0);
    let val = int_val
        .checked_mul(ONE)
        .and_then(|v| v.checked_add(frac_val))
        .ok_or_else(|| anyhow!("overflow in `{s}`"))?;
    Ok(if neg { -val } else { val })
}

/// A Kalshi NO bid at price `q` is a YES ask at `1 - q`.
pub fn ask_price_fp(no_price_fp: i64) -> i64 {
    ONE - no_price_fp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cents() {
        assert_eq!(decimal_to_fp("0.5900").unwrap(), 590_000);
    }
    #[test]
    fn parses_size_fraction() {
        assert_eq!(decimal_to_fp("798.00").unwrap(), 798_000_000);
    }
    #[test]
    fn parses_negative_change() {
        assert_eq!(decimal_to_fp("-1.00").unwrap(), -1_000_000);
    }
    #[test]
    fn parses_quarter() {
        assert_eq!(decimal_to_fp("0.25").unwrap(), 250_000);
    }
    #[test]
    fn parses_fine_tick() {
        assert_eq!(decimal_to_fp("0.0001").unwrap(), 100);
    }
    #[test]
    fn ask_from_no_boundary() {
        assert_eq!(ask_price_fp(decimal_to_fp("0.9900").unwrap()), 10_000);
    }
}
