//! Fixed-scale decimal type backed by an `i64` mantissa.
//!
//! A `Decimal` represents `mantissa * 10^(-scale)`. For example
//! `from_i64_with_scale(12345, 2)` represents `123.45`.
//!
//! # Limits
//!
//! - `MAX_SCALE = 18` and `MAX_PRECISION = 18` — covers TPC-C's `NUMERIC(12,2)`
//!   and TPC-H's `NUMERIC(15,2)` with headroom, while ensuring every valid
//!   value fits in `i64` (whose 19th digit is partial: i64::MAX ≈ 9.22e18).
//!
//! # Arithmetic semantics
//!
//! - `add` / `sub` / `div_keeping_scale` require operands to have the same scale.
//!   Cross-scale operations return `Error::DecimalArithmetic`. SQL-level scale
//!   alignment is the planner's job (Phase 11).
//! - `mul` produces a result whose scale is the sum of operand scales (ANSI SQL
//!   convention). Intermediate computation uses `i128` to detect overflow.
//! - `div_keeping_scale` truncates toward zero (matches SQL `INT/INT` semantic)
//!   and preserves the dividend's scale. Division by zero is an explicit error.

use serde::{Deserialize, Serialize};

use crate::common::{Error, Result};

/// A fixed-scale decimal value: `mantissa * 10^(-scale)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Decimal {
    mantissa: i64,
    scale: u8,
}

impl Decimal {
    /// Maximum supported scale (digits after the decimal point).
    /// 18 ensures every valid value fits in `i64` with room for arithmetic.
    pub const MAX_SCALE: u8 = 18;

    /// Maximum supported precision (total significant digits).
    /// Equal to MAX_SCALE; further widening would require switching to `i128`.
    pub const MAX_PRECISION: u8 = 18;

    /// Construct from a raw mantissa + scale.
    /// Asserts `scale <= MAX_SCALE`.
    pub const fn from_i64_with_scale(mantissa: i64, scale: u8) -> Self {
        assert!(scale <= Self::MAX_SCALE, "Decimal scale exceeds MAX_SCALE");
        Self { mantissa, scale }
    }

    /// Raw integer mantissa.
    pub const fn mantissa(&self) -> i64 {
        self.mantissa
    }

    /// Number of digits after the decimal point.
    pub const fn scale(&self) -> u8 {
        self.scale
    }

    /// True if this decimal's scale equals `scale`.
    pub const fn matches_scale(&self, scale: u8) -> bool {
        self.scale == scale
    }

    /// True if `|mantissa|` requires more than `precision` significant digits.
    /// (Mantissa = 0 has zero significant digits; never exceeds.)
    pub fn exceeds_precision(&self, precision: u8) -> bool {
        if self.mantissa == 0 {
            return false;
        }
        let abs = self.mantissa.unsigned_abs();
        // 10^precision; saturates if precision > 19 (out of u64 range).
        let bound = 10_u64.checked_pow(precision as u32).unwrap_or(u64::MAX);
        abs >= bound
    }

    /// Add two same-scale decimals. Errors on scale mismatch or i64 overflow.
    pub fn add(&self, other: &Decimal) -> Result<Decimal> {
        if self.scale != other.scale {
            return Err(Error::DecimalArithmetic(format!(
                "add: scale mismatch: {} vs {}",
                self.scale, other.scale
            )));
        }
        let mantissa = self.mantissa.checked_add(other.mantissa).ok_or_else(|| {
            Error::DecimalArithmetic(format!(
                "add: overflow: {} + {}",
                self.mantissa, other.mantissa
            ))
        })?;
        Ok(Decimal {
            mantissa,
            scale: self.scale,
        })
    }

    /// Subtract two same-scale decimals. Errors on scale mismatch or i64 overflow.
    pub fn sub(&self, other: &Decimal) -> Result<Decimal> {
        if self.scale != other.scale {
            return Err(Error::DecimalArithmetic(format!(
                "sub: scale mismatch: {} vs {}",
                self.scale, other.scale
            )));
        }
        let mantissa = self.mantissa.checked_sub(other.mantissa).ok_or_else(|| {
            Error::DecimalArithmetic(format!(
                "sub: overflow: {} - {}",
                self.mantissa, other.mantissa
            ))
        })?;
        Ok(Decimal {
            mantissa,
            scale: self.scale,
        })
    }

    /// Multiply two decimals. Result scale = sum of operand scales (ANSI SQL).
    /// Errors if the result scale exceeds `MAX_SCALE`, or the i64 mantissa overflows
    /// (intermediate computed in i128).
    pub fn mul(&self, other: &Decimal) -> Result<Decimal> {
        let result_scale = self.scale.checked_add(other.scale).ok_or_else(|| {
            Error::DecimalArithmetic(format!(
                "mul: scale sum overflow: {} + {}",
                self.scale, other.scale
            ))
        })?;
        if result_scale > Self::MAX_SCALE {
            return Err(Error::DecimalArithmetic(format!(
                "mul: result scale {} exceeds MAX_SCALE {}",
                result_scale,
                Self::MAX_SCALE
            )));
        }
        let product = (self.mantissa as i128) * (other.mantissa as i128);
        if product > i64::MAX as i128 || product < i64::MIN as i128 {
            return Err(Error::DecimalArithmetic(format!(
                "mul: mantissa overflow: {} * {}",
                self.mantissa, other.mantissa
            )));
        }
        Ok(Decimal {
            mantissa: product as i64,
            scale: result_scale,
        })
    }

    /// Divide two same-scale decimals; result keeps `self`'s scale.
    ///
    /// TRUE decimal division (O4): the result is `self / other` rounded to
    /// this scale, half away from zero — `10.00 / 4.00 = 2.50`. (The prior
    /// implementation divided raw mantissas with truncation, yielding
    /// `0.02` for that input — an INT/INT semantic that was wrong for any
    /// scale > 0.) Errors on scale mismatch, division by zero, or a result
    /// mantissa exceeding i64.
    pub fn div_keeping_scale(&self, other: &Decimal) -> Result<Decimal> {
        if self.scale != other.scale {
            return Err(Error::DecimalArithmetic(format!(
                "div: scale mismatch: {} vs {}",
                self.scale, other.scale
            )));
        }
        if other.mantissa == 0 {
            return Err(Error::DecimalArithmetic("div: division by zero".into()));
        }
        // value = m_a / m_b (scales cancel); mantissa at scale s is
        // round(m_a·10^s / m_b). i128 headroom: |m_a| < 2^63 and
        // 10^s ≤ 10^MAX_SCALE = 1e18, so the product < 1e37 << i128::MAX.
        let numerator = self.mantissa as i128 * 10i128.pow(self.scale as u32);
        let mantissa = div_i128_round_half_away(numerator, other.mantissa as i128);
        let mantissa = i64::try_from(mantissa).map_err(|_| {
            Error::DecimalArithmetic(format!(
                "div: result mantissa overflows i64: {} / {} at scale {}",
                self.mantissa, other.mantissa, self.scale
            ))
        })?;
        Ok(Decimal {
            mantissa,
            scale: self.scale,
        })
    }
}

/// Integer division rounding half away from zero (SQL rounding), instead of
/// Rust's truncation toward zero. Shared by decimal division and the AVG
/// finalizers (E14/O4). `den` must be non-zero (callers check).
pub(crate) fn div_i128_round_half_away(num: i128, den: i128) -> i128 {
    debug_assert!(den != 0, "div_i128_round_half_away: zero divisor");
    let quotient = num / den;
    let remainder = num % den;
    if 2 * remainder.abs() >= den.abs() {
        // The discarded fraction is ≥ half: bump one step in the true
        // quotient's sign direction.
        if (num < 0) == (den < 0) {
            quotient + 1
        } else {
            quotient - 1
        }
    } else {
        quotient
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_i64_with_scale_basic() {
        let d = Decimal::from_i64_with_scale(12345, 2);
        assert_eq!(d.mantissa(), 12345);
        assert_eq!(d.scale(), 2);
        assert!(d.matches_scale(2));
        assert!(!d.matches_scale(3));
    }

    #[test]
    #[should_panic(expected = "Decimal scale exceeds MAX_SCALE")]
    fn from_i64_with_scale_panics_above_max() {
        let _ = Decimal::from_i64_with_scale(0, 19);
    }

    #[test]
    fn exceeds_precision_zero_never_does() {
        let d = Decimal::from_i64_with_scale(0, 5);
        assert!(!d.exceeds_precision(0));
        assert!(!d.exceeds_precision(18));
    }

    #[test]
    fn exceeds_precision_boundary() {
        // 999 has 3 significant digits.
        let d = Decimal::from_i64_with_scale(999, 0);
        assert!(!d.exceeds_precision(3));
        assert!(d.exceeds_precision(2));
        // -1000 has 4 (sign doesn't count).
        let d = Decimal::from_i64_with_scale(-1000, 0);
        assert!(!d.exceeds_precision(4));
        assert!(d.exceeds_precision(3));
    }

    #[test]
    fn add_same_scale() {
        // 1.23 + 4.56 = 5.79
        let a = Decimal::from_i64_with_scale(123, 2);
        let b = Decimal::from_i64_with_scale(456, 2);
        let r = a.add(&b).unwrap();
        assert_eq!(r.mantissa(), 579);
        assert_eq!(r.scale(), 2);
    }

    #[test]
    fn add_scale_mismatch_errors() {
        let a = Decimal::from_i64_with_scale(123, 2);
        let b = Decimal::from_i64_with_scale(456, 3);
        let err = a.add(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("scale mismatch")));
    }

    #[test]
    fn add_overflow_errors() {
        let a = Decimal::from_i64_with_scale(i64::MAX, 0);
        let b = Decimal::from_i64_with_scale(1, 0);
        let err = a.add(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("overflow")));
    }

    #[test]
    fn sub_same_scale() {
        // 5.79 - 1.23 = 4.56
        let a = Decimal::from_i64_with_scale(579, 2);
        let b = Decimal::from_i64_with_scale(123, 2);
        let r = a.sub(&b).unwrap();
        assert_eq!(r.mantissa(), 456);
        assert_eq!(r.scale(), 2);
    }

    #[test]
    fn sub_overflow_errors() {
        let a = Decimal::from_i64_with_scale(i64::MIN, 0);
        let b = Decimal::from_i64_with_scale(1, 0);
        let err = a.sub(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("overflow")));
    }

    #[test]
    fn mul_scale_sums() {
        // 1.23 * 4.5 = 5.535 (scale 2 + scale 1 = scale 3)
        let a = Decimal::from_i64_with_scale(123, 2);
        let b = Decimal::from_i64_with_scale(45, 1);
        let r = a.mul(&b).unwrap();
        assert_eq!(r.mantissa(), 5535);
        assert_eq!(r.scale(), 3);
    }

    #[test]
    fn mul_uses_i128_intermediate() {
        // i64::MAX * 2 overflows i64 but fits in i128. We must catch it.
        let a = Decimal::from_i64_with_scale(i64::MAX, 0);
        let b = Decimal::from_i64_with_scale(2, 0);
        let err = a.mul(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("mantissa overflow")));
    }

    #[test]
    fn mul_result_scale_overflow_errors() {
        let a = Decimal::from_i64_with_scale(1, 10);
        let b = Decimal::from_i64_with_scale(1, 10);
        let err = a.mul(&b).unwrap_err();
        // 10 + 10 = 20 > MAX_SCALE (18).
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("MAX_SCALE")));
    }

    #[test]
    fn div_is_true_decimal_division_rounded() {
        // O4: 10.00 / 4.00 = 2.50 (the old mantissa-division gave 0.02).
        let a = Decimal::from_i64_with_scale(1000, 2);
        let b = Decimal::from_i64_with_scale(400, 2);
        let r = a.div_keeping_scale(&b).unwrap();
        assert_eq!(r.mantissa(), 250);
        assert_eq!(r.scale(), 2);
        // 7 / 3 at scale 0: 2.33… rounds to 2; -7 / 3 → -2 (symmetric).
        let a = Decimal::from_i64_with_scale(7, 0);
        let b = Decimal::from_i64_with_scale(3, 0);
        assert_eq!(a.div_keeping_scale(&b).unwrap().mantissa(), 2);
        let a = Decimal::from_i64_with_scale(-7, 0);
        assert_eq!(a.div_keeping_scale(&b).unwrap().mantissa(), -2);
        // Half cases round away from zero: 5/2 → 3, -5/2 → -3.
        let a = Decimal::from_i64_with_scale(5, 0);
        let b = Decimal::from_i64_with_scale(2, 0);
        assert_eq!(a.div_keeping_scale(&b).unwrap().mantissa(), 3);
        let a = Decimal::from_i64_with_scale(-5, 0);
        assert_eq!(a.div_keeping_scale(&b).unwrap().mantissa(), -3);
        // 2/3 at scale 0: 0.67 rounds to 1 (old truncation gave 0).
        let a = Decimal::from_i64_with_scale(2, 0);
        let b = Decimal::from_i64_with_scale(3, 0);
        assert_eq!(a.div_keeping_scale(&b).unwrap().mantissa(), 1);
    }

    #[test]
    fn div_by_zero_errors() {
        let a = Decimal::from_i64_with_scale(5, 0);
        let b = Decimal::from_i64_with_scale(0, 0);
        let err = a.div_keeping_scale(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("division by zero")));
    }

    #[test]
    fn serde_roundtrip_via_bincode() {
        // bincode is already a runtime dep (used for catalog blob serialization).
        // serde_json would also work but isn't in deps; bincode covers the
        // serde-derive correctness check we actually need.
        let d = Decimal::from_i64_with_scale(12345, 2);
        let bytes = bincode::serialize(&d).unwrap();
        let back: Decimal = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, d);
    }
}
