use arrow::{array::ArrayRef, datatypes::FieldRef, record_batch::RecordBatch};
use arrow_schema::Field;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::plugins::expr::Expr;
use taosx_ipc::prelude::IpcDataType;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatValueBuilder {
    format: String,
}

impl ValueBuilder for FormatValueBuilder {
    fn build_field(
        &self,
        name: &str,
        record: &RecordBatch,
        _as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        let expr = Expr::try_new(format!("`{}`", self.format), true).map_err(|err| {
            let err_msg = format!("failed build format expression, cause: {}", err.to_string());
            ValueBuilderError::FormatError(err_msg)
        })?;

        let values = expr
            .eval(record, _as.map(|data_type| data_type.arrow_data_type()))
            .map_err(|err| {
                let err_msg = format!("failed to format, cause: {}", err.to_string());
                ValueBuilderError::FormatError(err_msg)
            })?;

        Ok((
            Arc::new(Field::new(name, values.data_type().clone(), true)),
            values,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::plugins::transform::map::format::FormatValueBuilder;
    use crate::plugins::transform::map::ValueBuilder;
    use arrow::array::Int64Array;
    use arrow_schema::DataType;

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

        assert!(record.is_err());
        assert_eq!(
            record.unwrap_err().to_string(),
            "format error, cause: failed to format, cause: invalid result"
        );
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
        assert_eq!(arr.value(0), "");
        assert_eq!(arr.value(1), "");
        assert_eq!(arr.value(2), "");
    }
}
