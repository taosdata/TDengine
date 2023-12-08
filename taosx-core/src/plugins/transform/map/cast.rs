use arrow::{array::ArrayRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastValueBuilder {
    cast: String,
}

impl ValueBuilder for CastValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let schema = record.schema();
        schema
            .index_of(&self.cast)
            .map_err(ValueBuilderError::CastError)
            .map(|index| record.column(index).clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::{
        array::{Array, Int32Array, StringArray, TimestampNanosecondArray},
        datatypes::DataType,
    };
    use taosx_ipc::prelude::IpcDataType;

    use super::*;

    fn init_record_batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "f1",
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
            (
                "int",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn test_string() {
        let builder: CastValueBuilder = serde_json::from_str(r#"{"cast": "f1"}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
    }

    #[test]
    fn test_int_as_timestamp() {
        let builder: CastValueBuilder = serde_json::from_str(r#"{"cast": "int"}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder
            .build_field(
                "n1",
                &batch,
                Some(IpcDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond)),
            )
            .unwrap();

        dbg!(&field, &value);

        assert_eq!(field.name(), "n1");
        assert_eq!(
            *field.data_type(),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(value.len(), 3);
        let array = value
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert!(array.is_null(0));
    }

    #[test]
    fn test_string_as_int() {
        let builder: CastValueBuilder = serde_json::from_str(r#"{"cast": "int"}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder
            .build_field("n1", &batch, Some(IpcDataType::Int32))
            .unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Int32);
        assert_eq!(value.len(), 3);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }
}
