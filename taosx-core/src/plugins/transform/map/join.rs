use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::compute::cast;
use arrow::compute::kernels::concat_elements::concat_elements_utf8_many;
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use arrow_schema::DataType;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinValueBuilder {
    join: Vec<String>,
    with: Option<String>,
}

impl ValueBuilder for JoinValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let join_columns = match self.with.clone() {
            None => {
                let mut values = Vec::new();
                for field in self.join.iter() {
                    let col = record.column_by_name(field);
                    if col.is_none() {
                        continue;
                    }
                    let col = cast(col.unwrap(), &DataType::Utf8)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone();
                    values.push(col);
                }
                values
            }
            Some(separator) => {
                let items_len = self.join.len();
                let mut values = Vec::new();
                for (i, field) in self.join.iter().enumerate() {
                    let col = record.column_by_name(field);
                    if col.is_none() {
                        continue;
                    }
                    let col = arrow::compute::cast(col.unwrap(), &DataType::Utf8)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone();
                    values.push(col);
                    if i != items_len - 1 {
                        values.push(StringArray::from(vec![
                            separator.as_str();
                            record.num_rows()
                        ]));
                    }
                }
                values
            }
        };

        let values = Arc::new(
            concat_elements_utf8_many(join_columns.iter().collect_vec().as_slice()).map_err(
                |err| {
                    let err_msg = format!("failed to join, cause: {}", err);
                    ValueBuilderError::Join(err_msg)
                },
            )?,
        ) as ArrayRef;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;

    use super::*;

    #[test]
    fn test_join_not_exist_field() {
        let builder: JoinValueBuilder =
            serde_json::from_str(r#"{"join": ["a", "b", "c", "d"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let (field, value) = builder.build_field("join", &batch, None).unwrap();

        assert_eq!(field.name(), "join");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1");
        assert_eq!(arr.value(1), "2");
        assert_eq!(arr.value(2), "3");
    }

    #[test]
    fn test_join() {
        let builder: JoinValueBuilder =
            serde_json::from_str(r#"{"join": ["a", "b", "c", "d"]}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("d", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder.build_field("join", &batch, None).unwrap();

        assert_eq!(field.name(), "join");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1111");
        assert_eq!(arr.value(1), "2222");
        assert_eq!(arr.value(2), "3333");
    }

    #[test]
    fn test_join_with() {
        let builder: JoinValueBuilder =
            serde_json::from_str(r#"{"join": ["a", "b", "c"], "with": "-"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("d", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let (field, value) = builder.build_field("join", &batch, None).unwrap();

        assert_eq!(field.name(), "join");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1-1-1");
        assert_eq!(arr.value(1), "2-2-2");
        assert_eq!(arr.value(2), "3-3-3");

        let batch = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
            (
                "c",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
            (
                "d",
                Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
            ),
        ])
        .unwrap();

        let (field, value) = builder.build_field("join", &batch, None).unwrap();

        assert_eq!(field.name(), "join");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1-1-1");
        assert_eq!(arr.value(1), "2-2-2");
        assert_eq!(arr.value(2), "3-3-3");
        dbg!(&arr);
    }

    #[test]
    fn test_join_with_not_exist_field() {
        let builder: JoinValueBuilder =
            serde_json::from_str(r#"{"join": ["a", "b", "c", "d"], "with": "-"}"#).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();

        let (field, value) = builder.build_field("join", &batch, None).unwrap();

        assert_eq!(field.name(), "join");
        assert_eq!(*field.data_type(), DataType::Utf8);
        assert_eq!(value.len(), 3);
        let arr = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1-");
        assert_eq!(arr.value(1), "2-");
        assert_eq!(arr.value(2), "3-");
    }
}
