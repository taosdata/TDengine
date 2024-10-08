use arrow::{array::ArrayRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};

use crate::plugins::expr::Expr;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExprValueBuilder(Expr);

impl ExprValueBuilder {
    pub fn new(expr: Expr) -> Self {
        Self(expr)
    }
}

impl ValueBuilder for ExprValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let values = self.0.eval(record, None).map_err(|err| {
            let err_msg = format!("failed to eval expression, cause: {}", err);
            ValueBuilderError::Expr(err_msg)
        })?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Int64Array};
    use arrow_schema::DataType;
    use taosx_ipc::prelude::IpcDataType;

    use super::*;

    #[test]
    #[ignore = "array is not supported, the behavior is not defined"]
    fn test_eval_failed() {
        let builder: ExprValueBuilder = serde_json::from_str(r#"{ "expr": "a + b"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let record = builder.build_field("c", &batch, None);
        assert!(record.is_err());
        assert_eq!(
            record.unwrap_err().to_string(),
            "expr error, cause: failed to eval expression, cause: invalid result"
        );
    }

    #[test]
    fn test_eval_success() {
        let builder: ExprValueBuilder = serde_json::from_str(r#"{ "expr": "a + b"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder.build_field("c", &batch, None).unwrap();

        assert_eq!(field.name(), "c");
        assert_eq!(*field.data_type(), DataType::Int64);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 2);
        assert_eq!(arr.value(1), 4);
        assert_eq!(arr.value(2), 6);
    }

    #[test]
    fn test_eval_as_failed() {
        let builder: ExprValueBuilder =
            serde_json::from_str(r#"{"expr": "a - b", "null_if_error": false}"#).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let result = builder.build_field("c", &batch, Some(IpcDataType::Bool));

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "expr error, cause: failed to eval expression, cause: Eval `a - b` error: Variable not found: b (line 1, position 5)"
        );
    }

    #[test]
    fn test_eval_as_success() {
        let builder: ExprValueBuilder = serde_json::from_str(r#"{ "expr": "a - b"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder
            .build_field("c", &batch, Some(IpcDataType::Bool))
            .unwrap();

        assert_eq!(field.name(), "c");
        assert_eq!(*field.data_type(), DataType::Boolean);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(arr.value(1));
        assert!(arr.value(2));
    }
}
