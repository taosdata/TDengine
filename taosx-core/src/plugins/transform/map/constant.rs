use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, FieldRef},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use taosx_ipc::prelude::IpcDataType;

use super::{JsonValue, ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantValueBuilder {
    value: JsonValue,
}

impl ValueBuilder for ConstantValueBuilder {
    fn build_field(
        &self,
        _name: &str,
        _record: &RecordBatch,
        _as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        let len = _record.num_rows();
        match &self.value {
            JsonValue::Null => Ok((
                Arc::new(Field::new(_name, DataType::Utf8, true)),
                Arc::new(StringArray::new_null(len)),
            )),
            JsonValue::Bool(value) => Ok((
                Arc::new(Field::new(_name, DataType::Boolean, false)),
                Arc::new(BooleanArray::from(vec![*value; len])),
            )),
            JsonValue::Number(value) => {
                if value.is_f64() {
                    Ok((
                        Arc::new(Field::new(_name, DataType::Float64, false)),
                        Arc::new(Float64Array::from(vec![value.as_f64().unwrap(); len])),
                    ))
                } else if value.is_i64() {
                    Ok((
                        Arc::new(Field::new(_name, DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![value.as_i64().unwrap(); len])),
                    ))
                } else {
                    Ok((
                        Arc::new(Field::new(_name, DataType::UInt64, false)),
                        Arc::new(UInt64Array::from(vec![value.as_u64().unwrap(); len])),
                    ))
                }
            }
            JsonValue::String(value) => Ok((
                Arc::new(Field::new(_name, DataType::Utf8, false)),
                Arc::new(StringArray::from(vec![value.as_str(); len])),
            )),
            JsonValue::Array(_) => Err(ValueBuilderError::InvalidValueBuilder),
            JsonValue::Object(value) => {
                let value = serde_json::to_string(value)
                    .map_err(|_| ValueBuilderError::InvalidValueBuilder)?;
                Ok((
                    Arc::new(Field::new(_name, DataType::Utf8, false)),
                    Arc::new(StringArray::from(vec![value.as_str(); len])),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;

    fn init_record_batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "f1",
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        )]).unwrap()
    }

    #[test]
    fn test_null() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": null}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<StringArray>().unwrap().is_null(0), true);
    }

    #[test]
    fn test_bool() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": true}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Boolean);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<BooleanArray>().unwrap().value(0), true);
    }

    #[test]
    fn test_int() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": 1}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();
        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Int64);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<Int64Array>().unwrap().value(0), 1);
    }

    #[test]
    fn test_float() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": 1.1}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Float64);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<Float64Array>().unwrap().value(0), 1.1);
    }

    #[test]
    fn test_u64() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": 18446744073709551615}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::UInt64);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<UInt64Array>().unwrap().value(0), 18446744073709551615);
    }

    #[test]
    fn test_string() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": "hello"}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<StringArray>().unwrap().value(0), "hello");
    }

    #[test]
    fn test_array() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": [1,2,3]}"#).unwrap();
        let batch = init_record_batch();

        let record = builder.build_field("n1", &batch, None);

        assert!(record.is_err());
        assert_eq!(record.unwrap_err().to_string(), "invalid value builder");
    }

    #[test]
    fn test_object() {
        let builder: ConstantValueBuilder = serde_json::from_str(r#"{"value": {"a": 1}}"#).unwrap();
        let batch = init_record_batch();

        let (field, value) = builder.build_field("n1", &batch, None).unwrap();

        assert_eq!(field.name(), "n1");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        assert_eq!(value.as_any().downcast_ref::<StringArray>().unwrap().value(0), r#"{"a":1}"#);
    }
}
