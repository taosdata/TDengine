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
            JsonValue::Array(_) => todo!(),
            JsonValue::Object(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let builder = ConstantValueBuilder {
            value: JsonValue::Null,
        };
        let record = builder
            .build_field(
                "n1",
                &RecordBatch::try_from_iter([(
                    "f1",
                    Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                )])
                .unwrap(),
                None,
            )
            .unwrap();
        assert_eq!(record.0.name(), "n1");
        assert_eq!(*record.0.data_type(), DataType::Utf8);
        assert_eq!(record.1.len(), 3);
    }
    #[test]
    fn test_int() {
        let builder = ConstantValueBuilder {
            value: JsonValue::Number(1.into()),
        };
        let record = builder
            .build_field(
                "n1",
                &RecordBatch::try_from_iter([(
                    "f1",
                    Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                )])
                .unwrap(),
                None,
            )
            .unwrap();
        assert_eq!(record.0.name(), "n1");
        assert_eq!(*record.0.data_type(), DataType::Int64);
        assert_eq!(record.1.len(), 3);
    }
    #[test]
    fn test_float() {
        let builder = ConstantValueBuilder {
            value: JsonValue::Number(serde_json::Number::from_f64(1.0).unwrap().into()),
        };
        let record = builder
            .build_field(
                "n1",
                &RecordBatch::try_from_iter([(
                    "f1",
                    Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                )])
                .unwrap(),
                None,
            )
            .unwrap();
        assert_eq!(record.0.name(), "n1");
        assert_eq!(*record.0.data_type(), DataType::Float64);
        assert_eq!(record.1.len(), 3);
    }
    #[test]
    fn test_u64() {
        let builder = ConstantValueBuilder {
            value: JsonValue::Number(u64::MAX.into()),
        };
        let record = builder
            .build_field(
                "n1",
                &RecordBatch::try_from_iter([(
                    "f1",
                    Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                )])
                .unwrap(),
                None,
            )
            .unwrap();
        assert_eq!(record.0.name(), "n1");
        assert_eq!(*record.0.data_type(), DataType::UInt64);
        assert_eq!(record.1.len(), 3);
    }
}
