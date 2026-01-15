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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorValueBuilder {
    generator: String,
    #[serde(default)]
    precision: TimePrecision,
    #[serde(skip)]
    last_generated_time: Arc<AtomicI64>,
}

impl PartialEq for GeneratorValueBuilder {
    fn eq(&self, other: &Self) -> bool {
        self.generator == other.generator && self.precision == other.precision
    }
}

impl ValueBuilder for GeneratorValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let len = record.num_rows();

        match self.generator.as_str() {
            "now" => {
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
                let array: ArrayRef = match self.precision {
                    TimePrecision::Nanosecond => {
                        Arc::new(TimestampNanosecondArray::from(time_array).with_timezone_utc())
                    }
                    TimePrecision::Microsecond => {
                        Arc::new(TimestampMicrosecondArray::from(time_array).with_timezone_utc())
                    }
                    TimePrecision::Millisecond => {
                        Arc::new(TimestampMillisecondArray::from(time_array).with_timezone_utc())
                    }
                    TimePrecision::Second => {
                        Arc::new(TimestampSecondArray::from(time_array).with_timezone_utc())
                    }
                };

                Ok(array)
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
        dbg!(&value);
        assert_eq!(value.len(), 3);
        let ts = value
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        dbg!(ts);
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
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now", "precision": "ms" }"#).unwrap();
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
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now", "precision": "s" }"#).unwrap();
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
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now", "precision": "us" }"#).unwrap();
        let mut pre = 0;
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
                assert!(v > pre);
                pre = v;
            }
        }
        assert!(
            (SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64)
                < pre
        );
        Ok(())
    }

    #[test]
    fn dup_timestamp_ns_test() -> anyhow::Result<()> {
        let builder: GeneratorValueBuilder =
            serde_json::from_str(r#"{ "generator": "now" }"#).unwrap();
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
}
