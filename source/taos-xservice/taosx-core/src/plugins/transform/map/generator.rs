use std::sync::Arc;
use std::sync::atomic::{self, AtomicI64};
use std::time::SystemTime;

use arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum TimePrecision {
    #[serde(rename = "ns")]
    #[default]
    Nanosecond,
    #[serde(rename = "us")]
    Microsecond,
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "s")]
    Second,
}

impl std::fmt::Display for TimePrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimePrecision::Nanosecond => write!(f, "ns"),
            TimePrecision::Microsecond => write!(f, "us"),
            TimePrecision::Millisecond => write!(f, "ms"),
            TimePrecision::Second => write!(f, "s"),
        }
    }
}

impl TimePrecision {
    fn current_timestamp(&self) -> i64 {
        let epoch_duration = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        match self {
            TimePrecision::Nanosecond => epoch_duration.as_nanos() as i64,
            TimePrecision::Microsecond => epoch_duration.as_micros() as i64,
            TimePrecision::Millisecond => epoch_duration.as_millis() as i64,
            TimePrecision::Second => epoch_duration.as_secs() as i64,
        }
    }

    fn make_timestamp_array(&self, values: Vec<i64>) -> ArrayRef {
        match self {
            TimePrecision::Nanosecond => {
                Arc::new(TimestampNanosecondArray::from(values).with_timezone_utc())
            }
            TimePrecision::Microsecond => {
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone_utc())
            }
            TimePrecision::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(values).with_timezone_utc())
            }
            TimePrecision::Second => {
                Arc::new(TimestampSecondArray::from(values).with_timezone_utc())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorValueBuilder {
    generator: String,
    #[serde(default)]
    precision: TimePrecision,
    #[serde(default)]
    incremental: bool,
    #[serde(skip)]
    last_generated_time: Arc<AtomicI64>,
}

impl PartialEq for GeneratorValueBuilder {
    fn eq(&self, other: &Self) -> bool {
        self.generator == other.generator
            && self.precision == other.precision
            && self.incremental == other.incremental
    }
}

impl ValueBuilder for GeneratorValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let len = record.num_rows();
        match self.generator.as_str() {
            "now" => {
                if self.incremental {
                    let current_timestamp = self.precision.current_timestamp();
                    let last_timestamp = self
                        .last_generated_time
                        .fetch_max(current_timestamp, atomic::Ordering::SeqCst)
                        .max(current_timestamp);

                    if last_timestamp > current_timestamp {
                        tracing::warn!(
                            "generator time is {}{} ahead of the system time",
                            (last_timestamp - current_timestamp),
                            self.precision
                        );
                    }

                    let mut time_array = Vec::with_capacity(len);
                    for _ in 0..len {
                        time_array.push(
                            self.last_generated_time
                                .fetch_add(1, atomic::Ordering::SeqCst),
                        );
                    }
                    Ok(self.precision.make_timestamp_array(time_array))
                } else {
                    let ts = self.precision.current_timestamp();
                    Ok(self.precision.make_timestamp_array(vec![ts; len]))
                }
            }
            _ => {
                let msg = format!("generator does not support: {}", self.generator);
                Err(ValueBuilderError::Generator(msg))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::{DataType, TimeUnit};

    use super::*;

    #[test]
    fn test_now() {
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now"}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("ts", &batch, None).unwrap();

        assert_eq!(field.name(), "ts");
        assert_eq!(
            *field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
        );
        assert_eq!(value.len(), 3);
        let values = value
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        // All rows in the batch must share the same timestamp (default batch mode)
        let first = values.value(0);
        assert!(first > 0, "timestamp must be positive");
        for i in 1..values.len() {
            assert_eq!(
                values.value(i),
                first,
                "row {i} should share the same timestamp"
            );
        }
    }

    #[test]
    fn test_invalid() {
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "invalid"}"#).unwrap();
        let batch = init_record_batch();

        let result = builder.build_field("ts", &batch, None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "generator error, cause: generator does not support: invalid"
        );
    }

    fn init_record_batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "f1",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn dup_timestamp_ms_test() -> anyhow::Result<()> {
        let builder: GeneratorValueBuilder = serde_json::from_str(
            r#"{ "generator": "now", "precision": "ms", "incremental": true }"#,
        )
        .unwrap();
        let mut pre = 0;
        for _ in 0..10 {
            let array: Int32Array = std::iter::repeat_n(42, 1000).collect();
            let batch = RecordBatch::try_from_iter([("f1", Arc::new(array) as ArrayRef)]).unwrap();

            let (field, value) = builder.build_field("ts", &batch, None).unwrap();
            assert_eq!(field.name(), "ts");
            let values = value
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();

            for value in values {
                let v = value.unwrap();
                assert!(v > pre);
                pre = v;
            }
        }
        assert!(
            (SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64)
                < pre
        );
        Ok(())
    }

    #[test]
    fn dup_timestamp_s_test() -> anyhow::Result<()> {
        let builder: GeneratorValueBuilder = serde_json::from_str(
            r#"{ "generator": "now", "precision": "s", "incremental": true }"#,
        )
        .unwrap();
        let mut pre = 0;
        for _ in 0..10 {
            let array: Int32Array = std::iter::repeat_n(42, 1000).collect();
            let batch = RecordBatch::try_from_iter([("f1", Arc::new(array) as ArrayRef)]).unwrap();

            let (field, value) = builder.build_field("ts", &batch, None).unwrap();
            assert_eq!(field.name(), "ts");
            let values = value
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();

            for value in values {
                let v = value.unwrap();
                assert!(v > pre);
                pre = v;
            }
        }
        assert!(
            (SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64)
                < pre
        );
        Ok(())
    }
    #[test]
    fn dup_timestamp_us_test() -> anyhow::Result<()> {
        let builder: GeneratorValueBuilder = serde_json::from_str(
            r#"{ "generator": "now", "precision": "us", "incremental": true }"#,
        )
        .unwrap();
        let mut pre = 0;
        let mut first = None;
        for _ in 0..10 {
            let array: Int32Array = std::iter::repeat_n(42, 1000).collect();
            let batch = RecordBatch::try_from_iter([("f1", Arc::new(array) as ArrayRef)]).unwrap();

            let (field, value) = builder.build_field("ts", &batch, None).unwrap();
            assert_eq!(field.name(), "ts");
            let values = value
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();

            for value in values {
                let v = value.unwrap();
                if first.is_none() {
                    first = Some(v);
                }
                assert!(v > pre);
                pre = v;
            }
        }
        let first = first.expect("expected at least one generated timestamp");
        assert!(
            pre - first >= (10 * 1000 - 1),
            "expected microsecond timestamps to advance by at least {} across the generated rows, got {}",
            10 * 1000 - 1,
            pre - first
        );
        Ok(())
    }

    #[test]
    fn dup_timestamp_ns_test() -> anyhow::Result<()> {
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now", "incremental": true }"#).unwrap();
        let mut pre = 0;
        for _ in 0..10 {
            let array: Int32Array = std::iter::repeat_n(42, 1000).collect();
            let batch = RecordBatch::try_from_iter([("f1", Arc::new(array) as ArrayRef)]).unwrap();

            let (field, value) = builder.build_field("ts", &batch, None).unwrap();
            assert_eq!(field.name(), "ts");
            let values = value
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();

            for value in values {
                let v = value.unwrap();
                assert!(v > pre);
                pre = v;
            }
        }
        Ok(())
    }

    #[test]
    fn test_now_batch_same_timestamp() {
        // default (no incremental field) -> every row in a batch must share the same timestamp
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now", "precision": "ms" }"#).unwrap();

        let make_batch = || {
            let array: Int32Array = std::iter::repeat_n(42, 100).collect();
            RecordBatch::try_from_iter([("f1", Arc::new(array) as ArrayRef)]).unwrap()
        };

        let before_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let (_, value) = builder.build_field("ts", &make_batch(), None).unwrap();
        let after_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let values = value
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        let first = values.value(0);
        // Timestamp must be produced during this build_from call.
        assert!(
            first >= before_ms && first <= after_ms,
            "timestamp {first} should be between before={before_ms} and after={after_ms}"
        );
        // All rows within a batch must share the same value
        for i in 1..values.len() {
            assert_eq!(
                values.value(i),
                first,
                "row {i} timestamp differs from row 0 - expected same timestamp per batch"
            );
        }
    }
}
