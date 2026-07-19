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
//! - `add` / `sub` require operands to have the same scale. Cross-scale
//!   operations return `Error::DecimalArithmetic`. SQL-level scale alignment
//!   is the planner's job (Phase 11).
//! - `mul` produces a result whose scale is the sum of operand scales (ANSI SQL
//!   convention). Intermediate computation uses `i128` to detect overflow.
//! - `div` produces a result at scale `max(s1, s2)` — NO equal-scale
//!   requirement: it rescales both operands to that common scale and divides,
//!   rounding half away from zero. This makes TPC-H Q14's verbatim
//!   `100.00 * sum(..) / sum(..)` (scale 4 ÷ scale 2) legal. Division by zero
//!   and mantissa overflow are explicit errors.

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

    /// Divide two decimals; result scale is `max(self.scale, other.scale)` —
    /// there is NO equal-scale requirement (unlike `add`/`sub`), so TPC-H
    /// Q14's verbatim left-associative `100.00 * sum(..) / sum(..)` (a
    /// scale-4 ÷ scale-2 division) is legal and yields a scale-4 result.
    ///
    /// TRUE decimal division: rescale both operands' mantissas to the common
    /// scale `s = max(s1, s2)`, then the result mantissa is
    /// `round_half_away_from_zero(a_scaled · 10^s / b_scaled)`. Rounding is
    /// HALF AWAY FROM ZERO (SQL rounding): a tie rounds up in magnitude,
    /// symmetric across sign (`5/2 → 3`, `-5/2 → -3`). The scaled numerator
    /// lives in `i128`; every widening multiply is checked and the final
    /// narrow to `i64` is checked, so any overflow is an error, never a wrong
    /// value. Division by zero is an explicit error.
    pub fn div(&self, other: &Decimal) -> Result<Decimal> {
        if other.mantissa == 0 {
            return Err(Error::DecimalArithmetic("div: division by zero".into()));
        }
        let result_scale = self.scale.max(other.scale);
        // Rescale each operand to the common scale (only the lower-scale one
        // actually gains digits). A single rescale is safe in i128 —
        // |m|·10^s ≤ 1e18·1e18 = 1e36 < i128::MAX — but stays checked per the
        // division contract.
        let a_scaled = rescale_mantissa(self.mantissa, self.scale, result_scale)?;
        let b_scaled = rescale_mantissa(other.mantissa, other.scale, result_scale)?;
        // Mantissa at scale s is round(a_scaled·10^s / b_scaled). The ·10^s is
        // checked: a near-i64 dividend at a high scale can push the numerator
        // past i128.
        let numerator = a_scaled
            .checked_mul(10i128.pow(result_scale as u32))
            .ok_or_else(|| {
                Error::DecimalArithmetic(format!(
                    "div: numerator overflow: {} / {} at scale {}",
                    self.mantissa, other.mantissa, result_scale
                ))
            })?;
        let mantissa = div_i128_round_half_away(numerator, b_scaled);
        let mantissa = i64::try_from(mantissa).map_err(|_| {
            Error::DecimalArithmetic(format!(
                "div: result mantissa overflows i64: {} / {} at scale {}",
                self.mantissa, other.mantissa, result_scale
            ))
        })?;
        Ok(Decimal {
            mantissa,
            scale: result_scale,
        })
    }
}

/// Widen `mantissa` (interpreted at `from_scale`) to `to_scale`, in `i128`.
/// `to_scale >= from_scale` is guaranteed by the sole caller (`div` picks the
/// max of the two operand scales).
///
/// The `checked_mul` Err path is UNREACHABLE from a validly-constructed
/// `Decimal`: the mantissa is an `i64` (|m| ≤ ~9.2e18) and both scales are
/// bounded by `MAX_SCALE` (18), so the widening factor is at most `10^18` and
/// the product is at most ~9.2e18·10^18 ≈ 9.2e36 < `i128::MAX` (~1.7e38) — it
/// cannot overflow. The check is kept as defense-in-depth: the derived serde
/// `Deserialize` does NOT re-validate the `scale ≤ MAX_SCALE` invariant that
/// `from_i64_with_scale` asserts, so a corrupt/hand-forged blob could carry an
/// out-of-range scale; a checked Err is the correct crash-on-corruption
/// response, never a wrapped value.
fn rescale_mantissa(mantissa: i64, from_scale: u8, to_scale: u8) -> Result<i128> {
    debug_assert!(to_scale >= from_scale, "rescale_mantissa: narrowing");
    (mantissa as i128)
        .checked_mul(10i128.pow((to_scale - from_scale) as u32))
        .ok_or_else(|| {
            Error::DecimalArithmetic(format!(
                "div: rescale overflow: {} from scale {} to {}",
                mantissa, from_scale, to_scale
            ))
        })
}

/// Integer division rounding half away from zero (SQL rounding), instead of
/// Rust's truncation toward zero. Shared by decimal division and the AVG
/// finalizers (E14/O4). `den` must be non-zero (callers check).
pub fn div_i128_round_half_away(num: i128, den: i128) -> i128 {
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
        // Equal-scale: 10.00 / 4.00 = 2.50, result scale = max(2,2) = 2.
        let a = Decimal::from_i64_with_scale(1000, 2);
        let b = Decimal::from_i64_with_scale(400, 2);
        let r = a.div(&b).unwrap();
        assert_eq!(r.mantissa(), 250);
        assert_eq!(r.scale(), 2);
        // 7 / 3 at scale 0: 2.33… rounds to 2; -7 / 3 → -2 (symmetric).
        let a = Decimal::from_i64_with_scale(7, 0);
        let b = Decimal::from_i64_with_scale(3, 0);
        assert_eq!(a.div(&b).unwrap().mantissa(), 2);
        let a = Decimal::from_i64_with_scale(-7, 0);
        assert_eq!(a.div(&b).unwrap().mantissa(), -2);
        // Half cases round AWAY FROM ZERO: 5/2 → 3, -5/2 → -3 (both signs).
        let a = Decimal::from_i64_with_scale(5, 0);
        let b = Decimal::from_i64_with_scale(2, 0);
        assert_eq!(a.div(&b).unwrap().mantissa(), 3);
        let a = Decimal::from_i64_with_scale(-5, 0);
        assert_eq!(a.div(&b).unwrap().mantissa(), -3);
        // 2/3 at scale 0: 0.67 rounds to 1 (truncation would give 0).
        let a = Decimal::from_i64_with_scale(2, 0);
        let b = Decimal::from_i64_with_scale(3, 0);
        assert_eq!(a.div(&b).unwrap().mantissa(), 1);
    }

    #[test]
    fn div_mixed_scale_takes_max_scale() {
        // scale4 ÷ scale2 → scale max(4,2)=4. 7.0000 / 2.00 = 3.5000.
        //   a_scaled = 70000·10^(4-4) = 70000 ; b_scaled = 200·10^(4-2) = 20000
        //   round(70000·10^4 / 20000) = round(700000000/20000) = 35000
        let a = Decimal::from_i64_with_scale(70000, 4);
        let b = Decimal::from_i64_with_scale(200, 2);
        let r = a.div(&b).unwrap();
        assert_eq!(r.mantissa(), 35000);
        assert_eq!(r.scale(), 4);
        // Reverse operand order → same max scale 4. -2.00 / 4.0000 = -0.5000.
        //   a_scaled = -200·10^2 = -20000 ; b_scaled = 40000
        //   round(-20000·10^4 / 40000) = round(-200000000/40000) = -5000
        let a = Decimal::from_i64_with_scale(-200, 2);
        let b = Decimal::from_i64_with_scale(40000, 4);
        let r = a.div(&b).unwrap();
        assert_eq!(r.mantissa(), -5000);
        assert_eq!(r.scale(), 4);
    }

    #[test]
    fn div_by_zero_errors() {
        // Zero divisor at any scale is an explicit error, not a NaN/panic.
        let a = Decimal::from_i64_with_scale(5, 0);
        let b = Decimal::from_i64_with_scale(0, 4);
        let err = a.div(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("division by zero")));
    }

    #[test]
    fn div_numerator_overflow_errors() {
        // i64::MAX (scale 0) ÷ 1e-18 (scale 18): result_scale = max(0,18) = 18.
        // Rescaling i64::MAX up to scale 18 is i64::MAX·10^18 ≈ 9.2e36, which is
        // < i128::MAX (~1.7e38) — so the rescale SUCCEEDS. It is the subsequent
        // numerator ·10^18 (≈ 9.2e54) that overflows i128. So this exercises
        // div()'s own numerator `checked_mul` Err path, NOT `rescale_mantissa`'s
        // (that one is unreachable from validly-constructed Decimals — see its
        // doc comment).
        let a = Decimal::from_i64_with_scale(i64::MAX, 0);
        let b = Decimal::from_i64_with_scale(1, 18);
        let err = a.div(&b).unwrap_err();
        assert!(matches!(err, Error::DecimalArithmetic(ref m) if m.contains("numerator overflow")));
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
