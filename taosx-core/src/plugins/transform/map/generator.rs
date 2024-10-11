use std::sync::Arc;

use arrow::array::TimestampNanosecondArray;
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorValueBuilder {
    generator: String,
}

impl ValueBuilder for GeneratorValueBuilder {
    fn build_from(&self, _record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let len = _record.num_rows();

        match self.generator.as_str() {
            "now" => {
                let mut time_array = Vec::with_capacity(len);
                for _ in 0..len {
                    time_array.push(Utc::now().timestamp_nanos_opt());
                }
                Ok(Arc::new(
                    TimestampNanosecondArray::from(time_array).with_timezone_utc(),
                ))
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
    use arrow::array::StringArray;
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
}
