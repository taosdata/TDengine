use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::Array;
use arrow::{array::ArrayRef, datatypes::FieldRef, record_batch::RecordBatch};
use arrow_schema::Field;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use taosx_ipc::prelude::IpcDataType;

use crate::plugins::expr::Expr;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumValueBuilder {
    sum: Vec<String>,
}

impl ValueBuilder for SumValueBuilder {
    fn build_field(
        &self,
        name: &str,
        records: &RecordBatch,
        _as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        let sum_expr = self.sum.iter().join("+");
        let expr = Expr::try_new(sum_expr, true).map_err(|err| {
            let err_msg = format!("failed to build sum expression, cause: {}", err.to_string());
            return ValueBuilderError::SumError(err_msg);
        })?;

        let values = expr
            .eval(records, _as.map(|d_type| d_type.arrow_data_type()))
            .map_err(|err| {
                let err_msg = format!("failed to calculate sum, cause: {}", err.to_string());
                return ValueBuilderError::SumError(err_msg);
            })?;

        Ok((
            Arc::new(Field::new(name, values.data_type().clone(), true)),
            values,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{new_null_array, Array, Float32Array, Float64Array, Int64Array};
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn test_sum() {
        let builder: SumValueBuilder = serde_json::from_str(r#"{"sum": ["a", "b", "c"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder.build_field("sum", &batch, None).unwrap();

        assert_eq!(field.name(), "sum");
        assert_eq!(field.data_type(), &DataType::Int64);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(arr.value(0), 3);
        assert_eq!(arr.value(1), 6);
        assert_eq!(arr.value(2), 9);

        let builder: SumValueBuilder = serde_json::from_str(r#"{"sum": ["a", "b", "c"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(Float32Array::from(vec![1.1, 2.2, 3.3])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(Float32Array::from(vec![1.1, 2.2, 3.3])) as ArrayRef,
            ),
            (
                "c",
                Arc::new(Float32Array::from(vec![1.1, 2.2, 3.3])) as ArrayRef,
            ),
        ])
        .unwrap();

        let (field, value) = builder.build_field("sum", &batch, None).unwrap();

        assert_eq!(field.name(), "sum");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 3.3f64).abs() < 1e-6);
        assert!((arr.value(1) - 6.6f64).abs() < 1e-6);
        assert!((arr.value(2) - 9.9f64).abs() < 1e-6);
    }

    #[test]
    fn test_sum_with_null() {
        let builder: SumValueBuilder = serde_json::from_str(r#"{"sum": ["a", "b", "c"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "b",
                Arc::new(new_null_array(&DataType::Int64, 3)) as ArrayRef,
            ),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = builder.build_field("sum", &batch, None);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "sum error, cause: failed to calculate sum, cause: invalid result"
        );
    }

    #[test]
    fn test_sum_as_float64() {
        let builder: SumValueBuilder = serde_json::from_str(r#"{"sum": ["a", "b", "c"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "b",
                Arc::new(new_null_array(&DataType::Int64, 3)) as ArrayRef,
            ),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder
            .build_field("sum", &batch, Some(IpcDataType::Float64))
            .unwrap();

        assert_eq!(field.name(), "sum");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 0f64);
        assert_eq!(arr.value(1), 0f64);
        assert_eq!(arr.value(2), 0f64);
    }
}
