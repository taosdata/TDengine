use std::fmt::{self, Debug, Display};

use chrono::Local;
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
        Display::fmt(&self.to_naive_datetime(), f)
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
        chrono::NaiveDateTime::from_timestamp(0, 0)
            .checked_add_signed(duration)
            .unwrap()
    }

    // todo: support to tz.
    pub fn to_datetime_with_tz(&self) -> chrono::DateTime<Local> {
        use chrono::TimeZone;
        Local.from_utc_datetime(&self.to_naive_datetime())
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
            assert!(ts.to_naive_datetime() == chrono::NaiveDateTime::from_timestamp(0, 0));
        }
    }

    #[test]
    fn ts_debug() {
        let ts = Timestamp::new(0, Precision::Millisecond);
        assert_eq!(format!("{:?}", ts), "1970-01-01T00:00:00");
        assert_eq!(format!("{:#?}", ts), "Milliseconds(\n    0,\n)");
        assert_eq!(format!("{}", ts), "1970-01-01 00:00:00");
    }
}
