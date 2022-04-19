use serde::{Deserialize, Serialize};

use super::Precision;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Timestamp {
    Milliseconds(i64),
    Microseconds(i64),
    Nanoseconds(i64),
}

impl Timestamp {
    pub fn new(raw: i64, precision: Precision) -> Self {
        match precision {
            Precision::Millisecond => Timestamp::Milliseconds(raw),
            Precision::Microsecond => Timestamp::Microseconds(raw),
            Precision::Nanosecond => Timestamp::Nanoseconds(raw),
        }
    }
    pub fn as_raw_i64(&self) -> &i64 {
        match self {
            Timestamp::Milliseconds(raw)
            | Timestamp::Microseconds(raw)
            | Timestamp::Nanoseconds(raw) => raw,
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
    pub fn to_datetime_with_tz(&self) {}
}
