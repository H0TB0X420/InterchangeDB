//! Proleptic-Gregorian civil-calendar math, dependency-free.
//!
//! Howard Hinnant's `days_from_civil` / `civil_from_days` algorithms
//! (public domain, <http://howardhinnant.github.io/date_algorithms.html>).
//! Days are counted from the Unix epoch: 1970-01-01 = day 0, matching
//! `Value::Date(i32)`. We reuse Hinnant rather than pull in a date crate so
//! `DATE` arithmetic carries zero new dependencies (CLAUDE.md: minimize deps).
//!
//! Both functions are *total* over the SQL `DATE` domain (years ~0001–9999,
//! whose day counts fit `i32` with room to spare). They do NOT validate their
//! inputs — `days_from_ymd(2000, 2, 30)` returns a day that `ymd_from_days`
//! maps to March 1st. Validity is checked at the parse boundary by asserting
//! the round-trip `ymd_from_days(days_from_ymd(y, m, d)) == (y, m, d)`, which
//! is exactly how "does this day exist in this month" is decided.

/// Days since 1970-01-01 for civil date (`year`, `month`, `day`).
/// `month` in 1..=12, `day` in 1..=31. Raw Hinnant algorithm — the caller
/// validates the calendar (see the module note on round-trip validation).
pub fn days_from_ymd(year: i32, month: u32, day: u32) -> i32 {
    // Shift so March is the year's first month: the leap day then lands at
    // the very end of the shifted year, which the era arithmetic below needs.
    let year = if month <= 2 { year - 1 } else { year };
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = (year - era * 400) as u32; // [0, 399]
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1; // [0, 365]
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year; // [0, 146096]
    era * 146097 + day_of_era as i32 - 719468
}

/// Inverse of `days_from_ymd`: the civil (`year`, `month`, `day`) a day count
/// lands on. `month` is 1..=12, `day` is 1..=31, always a real calendar date.
pub fn ymd_from_days(days: i32) -> (i32, u32, u32) {
    let days = days + 719468;
    let era = (if days >= 0 { days } else { days - 146096 }) / 146097;
    let day_of_era = (days - era * 146097) as u32; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365; // [0, 399]
    let year = year_of_era as i32 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let month_prime = (5 * day_of_year + 2) / 153; // [0, 11]
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1; // [1, 31]
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    }; // [1, 12]
       // Undo the March-first shift: Jan/Feb belong to the following civil year.
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The epoch anchor and its immediate neighbours pin the zero point and
    /// the sign convention (day before the epoch is -1, not +something).
    #[test]
    fn epoch_and_neighbours() {
        assert_eq!(days_from_ymd(1970, 1, 1), 0);
        assert_eq!(ymd_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_ymd(1969, 12, 31), -1);
        assert_eq!(ymd_from_days(-1), (1969, 12, 31));
        assert_eq!(days_from_ymd(1970, 1, 2), 1);
    }

    /// Hand-computed anchors: 1994-01-01 (TPC-H's date-filter era) and the
    /// 2000-01-01 century turn. 1970→1994 is 24 years with 6 leap days
    /// (1972,76,80,84,88,92) → 24*365+6 = 8766. 1970→2000 is 30 years with
    /// 7 leap days (…,1996) → 30*365+7 = 10957.
    #[test]
    fn hand_computed_anchors() {
        assert_eq!(days_from_ymd(1994, 1, 1), 8766);
        assert_eq!(days_from_ymd(2000, 1, 1), 10957);
        assert_eq!(ymd_from_days(8766), (1994, 1, 1));
        assert_eq!(ymd_from_days(10957), (2000, 1, 1));
    }

    /// Round-trip identity over a wide spread of dates, INCLUDING pre-epoch
    /// negatives, century boundaries, and both ends of the year. A single
    /// off-by-one in the era math breaks this for some offset.
    #[test]
    fn round_trip_over_a_spread() {
        // Step across ~1200 years in irregular strides so leap/century
        // interactions are hit at many phases, not just aligned ones.
        let mut day = days_from_ymd(1400, 1, 1);
        let end = days_from_ymd(2400, 12, 31);
        while day <= end {
            let (y, m, d) = ymd_from_days(day);
            assert_eq!(
                days_from_ymd(y, m, d),
                day,
                "round-trip broke at day {day} → {y:04}-{m:02}-{d:02}"
            );
            day += 37; // coprime-ish stride to sample many month/day phases
        }
    }

    /// Leap-year validity is decided by the round-trip: 2000-02-29 is a real
    /// date (2000 is a leap year — divisible by 400), so it round-trips;
    /// 1900-02-29 is NOT (1900 is divisible by 100 but not 400), so
    /// `days_from_ymd` normalizes it to March 1st and the round-trip fails.
    /// This IS the validity check the parser relies on.
    #[test]
    fn leap_day_validity_via_round_trip() {
        // Valid leap day round-trips exactly.
        let valid = days_from_ymd(2000, 2, 29);
        assert_eq!(ymd_from_days(valid), (2000, 2, 29));

        // Invalid "leap" day on a non-leap century normalizes forward.
        let invalid = days_from_ymd(1900, 2, 29);
        assert_ne!(ymd_from_days(invalid), (1900, 2, 29));
        assert_eq!(ymd_from_days(invalid), (1900, 3, 1));

        // Ordinary non-leap year: Feb 29 also doesn't exist.
        let invalid_2001 = days_from_ymd(2001, 2, 29);
        assert_ne!(ymd_from_days(invalid_2001), (2001, 2, 29));
    }

    /// Century and 400-year boundaries: the three consecutive year starts
    /// around 2000 differ by exactly 366 (2000 leap) then 365 (2001).
    #[test]
    fn century_boundaries() {
        let y2000 = days_from_ymd(2000, 1, 1);
        let y2001 = days_from_ymd(2001, 1, 1);
        let y2002 = days_from_ymd(2002, 1, 1);
        assert_eq!(y2001 - y2000, 366, "2000 is a leap year");
        assert_eq!(y2002 - y2001, 365, "2001 is not");
        // 1900 is not a leap year (÷100, not ÷400): 1900→1901 is 365 days.
        let y1900 = days_from_ymd(1900, 1, 1);
        let y1901 = days_from_ymd(1901, 1, 1);
        assert_eq!(y1901 - y1900, 365, "1900 is not a leap year");
    }
}
