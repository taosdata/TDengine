use arrow::{
    array::{ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::plugins::transform::parse::ArrayForTaos;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastValueBuilder {
    cast: String,
    default: Option<String>,
}

impl ValueBuilder for CastValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let schema = record.schema();

        schema
            .index_of(&self.cast)
            .map(|index| {
                let mut values = Vec::new();
                // get column values and judge if some of them are null
                let array = record.column(index);
                for i in 0..array.len() {
                    if array.is_null(i) || array.taos_value(i).is_null() {
                        if let Some(default) = &self.default {
                            values.push(Some(default.clone()));
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(Some(format!("{}", array.taos_value(i))));
                    }
                }
                return Ok(Arc::new(StringArray::from(values)) as ArrayRef);
            })
            .unwrap_or_else(|_| {
                if let Some(default) = &self.default {
                    Ok(
                        Arc::new(StringArray::from(vec![default.as_str(); record.num_rows()]))
                            as ArrayRef,
                    )
                } else {
                    Ok(Arc::new(StringArray::new_null(record.num_rows())) as ArrayRef)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{
            Array, BooleanArray, Float32Array, Int16Array, Int32Array, NullArray,
            TimestampNanosecondArray,
        },
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
            ("int", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "intstr",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn test_field_default() {
        let batch = init_record_batch();

        // default string
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "undefined", "default": "abc"}"#).unwrap();
        let (field, value) = builder.build_field("n1", &batch, None).unwrap();
        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "abc"
        );

        // default int/bigint/...
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "undefined", "default": "10"}"#).unwrap();
        let (field, value) = builder
            .build_field("n2", &batch, Some(IpcDataType::Int32))
            .unwrap();
        assert_eq!(field.name(), "n2");
        assert_eq!(*field.data_type(), DataType::Int32);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );

        // default float/double
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "undefined", "default": "10.18"}"#).unwrap();
        let (field, value) = builder
            .build_field("n3", &batch, Some(IpcDataType::Float32))
            .unwrap();
        assert_eq!(field.name(), "n3");
        assert_eq!(*field.data_type(), DataType::Float32);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0),
            0.1018e2
        );

        // default bool
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "undefined", "default": "true"}"#).unwrap();
        let (field, value) = builder
            .build_field("n4", &batch, Some(IpcDataType::Bool))
            .unwrap();
        assert_eq!(field.name(), "n4");
        assert_eq!(*field.data_type(), DataType::Boolean);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0),
            true
        );

        // default timestamp
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "undefined", "default": "2024-01-01 08:00:00"}"#)
                .unwrap();
        let (field, value) = builder
            .build_field(
                "n5",
                &batch,
                Some(IpcDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond)),
            )
            .unwrap();
        assert_eq!(field.name(), "n5");
        assert_eq!(
            *field.data_type(),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            1704096000000000000
        );

        // test default value with empty value
        let batch_with_none = RecordBatch::try_from_iter([
            (
                "f1",
                Arc::new(StringArray::from(vec!["a", "b", ""])) as ArrayRef,
            ),
            (
                "int",
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        // empty string
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "f1", "default": "abc"}"#).unwrap();
        let (field, value) = builder.build_field("n1", &batch_with_none, None).unwrap();
        dbg!(&field, &value);
        // none integer
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "int", "default": "10"}"#).unwrap();
        let (field, value) = builder.build_field("n1", &batch_with_none, None).unwrap();
        dbg!(&field, &value);
    }

    #[test]
    fn test_field_default_full_type() {
        // test default value with empty value
        let batch_with_none = RecordBatch::try_from_iter([
            ("null", Arc::new(NullArray::new(3)) as ArrayRef),
            (
                "boolean",
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef,
            ),
            (
                "string",
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            ),
            (
                "int16",
                Arc::new(Int16Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            ),
            (
                "int32",
                Arc::new(Int32Array::from(vec![Some(4), None, Some(6)])) as ArrayRef,
            ),
            (
                "float32",
                Arc::new(Float32Array::from(vec![Some(1.2), None, Some(2.3)])) as ArrayRef,
            ),
        ])
        .unwrap();

        // null
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "null", "default": "n1"}"#).unwrap();
        let (field, value) = builder
            .build_field("newcol", &batch_with_none, None)
            .unwrap();
        dbg!(&field, &value);

        // boolean
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "boolean", "default": "true"}"#).unwrap();
        let (field, value) = builder
            .build_field("newcol", &batch_with_none, None)
            .unwrap();
        dbg!(&field, &value);

        // string
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "string", "default": "s3"}"#).unwrap();
        let (field, value) = builder
            .build_field("newcol", &batch_with_none, None)
            .unwrap();
        dbg!(&field, &value);

        // int16
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "int16", "default": "10"}"#).unwrap();
        let (field, value) = builder
            .build_field("newcol", &batch_with_none, None)
            .unwrap();
        dbg!(&field, &value);

        // int32
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "int32", "default": "20"}"#).unwrap();
        let (field, value) = builder
            .build_field("newcol", &batch_with_none, None)
            .unwrap();
        dbg!(&field, &value);

        // float32
        let builder: CastValueBuilder =
            serde_json::from_str(r#"{"cast": "float32", "default": "1.1"}"#).unwrap();
        let (field, value) = builder
            .build_field("newcol", &batch_with_none, None)
            .unwrap();
        dbg!(&field, &value);
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
        assert!(array.is_valid(0));
    }

    #[test]
    fn test_string_as_int() {
        let builder: CastValueBuilder = serde_json::from_str(r#"{"cast": "intstr"}"#).unwrap();
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
