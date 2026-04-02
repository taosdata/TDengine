use std::fmt::{self, Debug, Display};

use chrono::{Local, TimeZone};
use serde::{Deserialize, Serialize};

use super::Precision;

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Timestamp {
    Milliseconds(i64),
    Microseconds(i64),
    Nanoseconds(i64),
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            match self {
                Self::Milliseconds(arg0) => f.debug_tuple("Milliseconds").field(arg0).finish(),
                Self::Microseconds(arg0) => f.debug_tuple("Microseconds").field(arg0).finish(),
                Self::Nanoseconds(arg0) => f.debug_tuple("Nanoseconds").field(arg0).finish(),
            }
        } else {
            Debug::fmt(&self.to_naive_datetime(), f)
        }
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.to_datetime_with_tz().to_rfc3339(), f)
    }
}

impl Timestamp {
    pub fn new(raw: i64, precision: Precision) -> Self {
        match precision {
            Precision::Millisecond => Timestamp::Milliseconds(raw),
            Precision::Microsecond => Timestamp::Microseconds(raw),
            Precision::Nanosecond => Timestamp::Nanoseconds(raw),
        }
    }

    pub fn precision(&self) -> Precision {
        match self {
            Timestamp::Milliseconds(_) => Precision::Millisecond,
            Timestamp::Microseconds(_) => Precision::Microsecond,
            Timestamp::Nanoseconds(_) => Precision::Nanosecond,
        }
    }

    pub fn as_raw_i64(&self) -> i64 {
        match self {
            Timestamp::Milliseconds(raw)
            | Timestamp::Microseconds(raw)
            | Timestamp::Nanoseconds(raw) => *raw,
        }
    }

    pub fn to_naive_datetime(&self) -> chrono::NaiveDateTime {
        let duration = match self {
            Timestamp::Milliseconds(raw) => chrono::Duration::milliseconds(*raw),
            Timestamp::Microseconds(raw) => chrono::Duration::microseconds(*raw),
            Timestamp::Nanoseconds(raw) => chrono::Duration::nanoseconds(*raw),
        };

        chrono::DateTime::from_timestamp(0, 0)
            .expect("timestamp value could always be mapped to a chrono::NaiveDateTime")
            .checked_add_signed(duration)
            .unwrap()
            .naive_utc()
    }

    #[inline]
    pub fn to_datetime_with_tz(&self) -> chrono::DateTime<Local> {
        self.to_datetime_with_custom_tz(&Local)
    }

    #[inline]
    pub fn to_datetime_with_custom_tz<Tz: TimeZone>(&self, tz: &Tz) -> chrono::DateTime<Tz> {
        tz.from_utc_datetime(&self.to_naive_datetime())
    }

    pub fn cast_precision(&self, precision: Precision) -> Timestamp {
        let raw = self.as_raw_i64();
        match (self.precision(), precision) {
            (Precision::Millisecond, Precision::Microsecond) => Timestamp::Microseconds(raw * 1000),
            (Precision::Millisecond, Precision::Nanosecond) => {
                Timestamp::Nanoseconds(raw * 1_000_000)
            }
            (Precision::Microsecond, Precision::Millisecond) => Timestamp::Milliseconds(raw / 1000),
            (Precision::Microsecond, Precision::Nanosecond) => Timestamp::Nanoseconds(raw * 1000),
            (Precision::Nanosecond, Precision::Millisecond) => {
                Timestamp::Milliseconds(raw / 1_000_000)
            }
            (Precision::Nanosecond, Precision::Microsecond) => Timestamp::Microseconds(raw / 1000),
            _ => Timestamp::new(raw, precision),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_new() {
        use Precision::*;
        for prec in [Millisecond, Microsecond, Nanosecond] {
            let ts = Timestamp::new(0, prec);
            assert!(ts.as_raw_i64() == 0);
            assert!(
                ts.to_naive_datetime()
                    == chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc()
            );
            dbg!(ts.to_datetime_with_tz());
        }
    }

    #[test]
    fn ts_cast_precision() {
        let precisions = [
            Precision::Millisecond,
            Precision::Microsecond,
            Precision::Nanosecond,
        ];
        for (i, prec) in precisions.iter().enumerate() {
            let ts = Timestamp::new(1_000_000 * (i as i64), *prec);
            for new_prec in precisions.iter() {
                let new_ts = ts.cast_precision(*new_prec);
                assert_eq!(
                    new_ts.precision(),
                    *new_prec,
                    "from {:?} to {:?}",
                    prec,
                    new_prec
                );
                assert_eq!(
                    new_ts.to_naive_datetime(),
                    ts.to_naive_datetime(),
                    "from {:?} to {:?}",
                    prec,
                    new_prec
                );
                match (prec, new_prec) {
                    (Precision::Millisecond, Precision::Microsecond) => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64) * 1000);
                    }
                    (Precision::Millisecond, Precision::Nanosecond) => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64) * 1_000_000);
                    }
                    (Precision::Microsecond, Precision::Millisecond) => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64) / 1000);
                    }
                    (Precision::Microsecond, Precision::Nanosecond) => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64) * 1000);
                    }
                    (Precision::Nanosecond, Precision::Millisecond) => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64) / 1_000_000);
                    }
                    (Precision::Nanosecond, Precision::Microsecond) => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64) / 1000);
                    }
                    _ => {
                        assert_eq!(new_ts.as_raw_i64(), 1_000_000 * (i as i64));
                    }
                }
            }
        }
    }

    #[test]
    fn ts_debug() {
        let ts = Timestamp::new(0, Precision::Millisecond);
        assert_eq!(format!("{:?}", ts), "1970-01-01T00:00:00");
        assert_eq!(format!("{:#?}", ts), "Milliseconds(\n    0,\n)");
        assert_eq!(format!("{}", ts), "1970-01-01T08:00:00+08:00");
    }
}
