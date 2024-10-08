use arrow::{array::ArrayRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};

use crate::plugins::expr::Expr;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatValueBuilder {
    format: String,
}

impl ValueBuilder for FormatValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let expr = Expr::try_new(format!("`{}`", self.format), true).map_err(|err| {
            let err_msg = format!("failed build format expression, cause: {}", err);
            ValueBuilderError::Format(err_msg)
        })?;

        let values = expr.eval(record, None).map_err(|err| {
            let err_msg = format!("failed to format, cause: {}", err);
            ValueBuilderError::Format(err_msg)
        })?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow_schema::DataType;
    use std::sync::Arc;
    use taosx_ipc::prelude::IpcDataType;

    use super::*;

    #[test]
    fn test_format_failed() {
        let builder: FormatValueBuilder =
            serde_json::from_str(r#"{"format": "${a}-${b}"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let record = builder.build_field("c", &batch, None);

        assert!(record.is_ok());

        // FIXME: format with null is supported now.
        // assert!(record.is_err());
        // assert_eq!(
        //     record.unwrap_err().to_string(),
        //     "format error, cause: failed to format, cause: invalid result"
        // );
    }

    #[test]
    fn test_format() {
        let builder: FormatValueBuilder =
            serde_json::from_str(r#"{"format": "${a}-${b}"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder.build_field("c", &batch, None).unwrap();

        assert_eq!(field.name(), "c");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "1-1");
        assert_eq!(arr.value(1), "2-2");
        assert_eq!(arr.value(2), "3-3");
    }

    #[test]
    fn test_format_with_null() {
        let builder: FormatValueBuilder =
            serde_json::from_str(r#"{"format": "${a}-${b}"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "b",
                Arc::new(arrow::array::new_null_array(&DataType::Int64, 3)) as ArrayRef,
            ),
        ])
        .unwrap();

        let (field, value) = builder
            .build_field("c", &batch, Some(IpcDataType::VarChar(32)))
            .unwrap();

        assert_eq!(field.name(), "c");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "1-");
        assert_eq!(arr.value(1), "2-");
        assert_eq!(arr.value(2), "3-");
    }
}
