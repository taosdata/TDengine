use serde::{Deserialize, Serialize};
use taos_sys::TimestampPrecision;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum TimestampValue {
    Milliseconds(i64),
    Microseconds(i64),
    Nanoseconds(i64),
}

impl TimestampValue {
    pub fn new(raw: i64, precision: TimestampPrecision) -> Self {
        match precision {
            TimestampPrecision::Millisecond => TimestampValue::Milliseconds(raw),
            TimestampPrecision::Microsecond => TimestampValue::Microseconds(raw),
            TimestampPrecision::Nanosecond => TimestampValue::Nanoseconds(raw),
        }
    }
    pub fn as_raw_i64(&self) -> &i64 {
        match self {
            TimestampValue::Milliseconds(raw)
            | TimestampValue::Microseconds(raw)
            | TimestampValue::Nanoseconds(raw) => raw,
        }
    }
    pub fn to_naive_datetime(&self) -> chrono::NaiveDateTime {
        let duration = match self {
            TimestampValue::Milliseconds(raw) => chrono::Duration::milliseconds(*raw),
            TimestampValue::Microseconds(raw) => chrono::Duration::microseconds(*raw),
            TimestampValue::Nanoseconds(raw) => chrono::Duration::nanoseconds(*raw),
        };
        chrono::NaiveDateTime::from_timestamp(0, 0)
            .checked_add_signed(duration)
            .unwrap()
    }
}
