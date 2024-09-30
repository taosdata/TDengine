use std::fmt::Debug;

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};

use crate::plugins::expr::Expr;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumValueBuilder {
    sum: Vec<String>,
}

impl ValueBuilder for SumValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        if self.sum.is_empty() {
            return Err(ValueBuilderError::SumError(
                "sum fields must greater than 1".to_string(),
            ));
        }
        // use itertools::Itertools;
        // let sum_expr = self.sum.iter().join("+");

        // FIXME: commented code is for null support.

        let sum_iter = self.sum.iter();
        let mut sum_expr = "".to_string();
        for (idx, field) in sum_iter.enumerate() {
            if idx > 0 {
                sum_expr.push_str(".add_or_set(");
                sum_expr.push_str(field);
                sum_expr.push(')');
            } else {
                sum_expr.push_str(&field.to_string());
            }
        }

        let expr = Expr::try_new(sum_expr, false).map_err(|err| {
            let err_msg = format!("failed to build sum expression, cause: {}", err);
            ValueBuilderError::SumError(err_msg)
        })?;

        let values = expr.eval(record, None).map_err(|err| {
            let err_msg = format!("failed to calculate sum, cause: {}", err);
            ValueBuilderError::SumError(err_msg)
        })?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        new_null_array, Array, Float32Array, Float64Array, Int64Array, StringArray,
    };
    use arrow_schema::DataType;
    use taosx_ipc::prelude::IpcDataType;

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

        dbg!(&result);
        assert!(result.is_ok());
        // FIXME: what's the expected result if there is a null value in the sum?

        // assert!(result.is_err());
        // assert_eq!(
        //     result.unwrap_err().to_string(),
        //     "sum error, cause: failed to calculate sum, cause: invalid result"
        // );
    }

    #[test]
    fn test_sum_with_not_exist_field() {
        let builder: SumValueBuilder = serde_json::from_str(r#"{"sum": ["a", "b", "c"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let result = builder.build_field("sum", &batch, None);

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "sum error, cause: failed to calculate sum, cause: Eval `a.add_or_set(b).add_or_set(c)` error: Variable not found: b (line 1, position 14)"
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
        assert_eq!(arr.value(0), 2f64);
        assert_eq!(arr.value(1), 4f64);
        assert_eq!(arr.value(2), 6f64);
    }

    #[test]
    fn test_sum_string_and_int() {
        let builder: SumValueBuilder = serde_json::from_str(r#"{"sum": ["a", "b", "c"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
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
        assert_eq!(arr.value(0), 11f64);
        assert_eq!(arr.value(1), 22f64);
        assert_eq!(arr.value(2), 33f64);
    }
}
