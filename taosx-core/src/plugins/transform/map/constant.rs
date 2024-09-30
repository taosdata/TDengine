use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

use super::{JsonValue, ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantValueBuilder {
    value: JsonValue,
}

impl ValueBuilder for ConstantValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let len = record.num_rows();

        match &self.value {
            JsonValue::Null => Ok(Arc::new(StringArray::new_null(len)) as ArrayRef),
            JsonValue::Bool(value) => {
                Ok(Arc::new(BooleanArray::from(vec![*value; len])) as ArrayRef)
            }
            JsonValue::Number(value) => {
                if value.is_f64() {
                    Ok(
                        Arc::new(Float64Array::from(vec![value.as_f64().unwrap(); len]))
                            as ArrayRef,
                    )
                } else if value.is_i64() {
                    Ok(Arc::new(Int64Array::from(vec![value.as_i64().unwrap(); len])) as ArrayRef)
                } else {
                    Ok(Arc::new(UInt64Array::from(vec![value.as_u64().unwrap(); len])) as ArrayRef)
                }
            }
            JsonValue::String(value) => {
                Ok(Arc::new(StringArray::from(vec![value.as_str(); len])) as ArrayRef)
            }
            JsonValue::Array(array) => {
                // TODO: support array to arrow array.
                let value = serde_json::to_string(array).map_err(|err| {
                    let err_msg = format!("failed to serialize object, cause: {}", err);
                    ValueBuilderError::ConstantError(err_msg)
                })?;
                Ok(Arc::new(StringArray::from(vec![value.as_str(); len])) as ArrayRef)
            }
            JsonValue::Object(value) => {
                let value = serde_json::to_string(value).map_err(|err| {
                    let err_msg = format!("failed to serialize object, cause: {}", err);
                    ValueBuilderError::ConstantError(err_msg)
                })?;
                Ok(Arc::new(StringArray::from(vec![value.as_str(); len])) as ArrayRef)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow_schema::DataType;

    use super::*;

    fn init_record_batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "f1",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn test_null() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": null}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        assert!(value
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn test_bool() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": true}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Boolean);
        assert_eq!(value.len(), 3);
        assert!(value
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn test_int() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": 1}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();
        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Int64);
        assert_eq!(value.len(), 3);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn test_float() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": 1.1}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Float64);
        assert_eq!(value.len(), 3);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.1
        );
    }

    #[test]
    fn test_u64() {
        let builder: ConstantValueBuilder =
            serde_json::from_str(r#"{"value": 18446744073709551615}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::UInt64);
        assert_eq!(value.len(), 3);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            18446744073709551615
        );
    }

    #[test]
    fn test_string() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": "hello"}"#).unwrap();
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
            "hello"
        );
    }

    #[test]
    #[ignore = "array is not supported, the behavior is not defined"]
    fn test_array() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": [1,2,3]}"#).unwrap();
        let batch = init_record_batch();

        let record = builder.build_field("n1", &batch, None);

        dbg!(&record);
        assert!(record.is_err());
        assert_eq!(
            record.unwrap_err().to_string(),
            "constant error, cause: array value is not supported"
        );
    }

    #[test]
    fn test_object() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": {"a": 1}}"#).unwrap();
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
            r#"{"a":1}"#
        );
    }
}
