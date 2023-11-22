use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::FieldRef, record_batch::RecordBatch};
use arrow::array::TimestampNanosecondArray;
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use taosx_ipc::prelude::IpcDataType;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratorValueBuilder {
    generator: String,
}

impl ValueBuilder for GeneratorValueBuilder {
    fn build_field(
        &self,
        name: &str,
        _record: &RecordBatch,
        _as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        let len = _record.num_rows();

        match self.generator.as_str() {
            "now" => {
                let now = Utc::now().timestamp_nanos_opt().unwrap();
                Ok((
                    Arc::new(Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), false)),
                    Arc::new(TimestampNanosecondArray::from(vec![now; len]).with_timezone_utc()),
                ))
            }
            _ => {
                let msg = format!("generator does not support: {}", self.generator);
                Err(ValueBuilderError::GeneratorError(msg))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;

    use super::*;

    #[test]
    fn test_now() {
        let builder: GeneratorValueBuilder = serde_json::from_str(r#"{ "generator": "now"}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("ts", &batch, None).unwrap();

        assert_eq!(field.name(), "ts");
        assert_eq!(*field.data_type(), DataType::Timestamp(TimeUnit::Nanosecond, None));
        assert_eq!(value.len(), 3);
        let ts = value.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(0);
        dbg!(ts);
    }

    #[test]
    fn test_invalid() {
        let builder: GeneratorValueBuilder = serde_json::from_str(r#"{ "generator": "invalid"}"#).unwrap();
        let batch = init_record_batch();

        let result = builder.build_field("ts", &batch, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "invalid value builder");
    }

    fn init_record_batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "f1",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        )]).unwrap()
    }
}
