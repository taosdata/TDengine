use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::{
    array::{Array, RecordBatch},
    compute::take,
    error::ArrowError,
};
use arrow_schema::Field;
use arrow_schema::Schema;
use serde_json::{Map as JsonMap, Value as JsonValue};

pub trait RecordBatchExt {
    fn take(&self, indices: &dyn Array) -> Result<RecordBatch, ArrowError>;

    /// 左右拼接行数相同的 RecordBatch, 如果 right 包含 left 已存在的列，则会忽略。
    fn concat_by_columns(&self, right: &RecordBatch) -> Result<RecordBatch, ArrowError>;

    fn to_json_rows(&self) -> Result<Vec<JsonMap<String, JsonValue>>, ArrowError>;
}

impl RecordBatchExt for RecordBatch {
    fn take(&self, indices: &dyn Array) -> Result<Self, ArrowError> {
        let columns = self
            .columns()
            .iter()
            .map(|column| take(column, indices, None))
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new(self.schema().clone(), columns)
    }

    fn concat_by_columns(&self, right: &RecordBatch) -> Result<Self, ArrowError> {
        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut added_name = std::collections::BTreeSet::<&str>::new();
        let left_schema = self.schema();
        let right_schema = right.schema();
        for i in 0..left_schema.fields().len() {
            let name = left_schema.field(i).name().as_str();
            if added_name.contains(name) {
                continue;
            }
            added_name.insert(name);
            fields.push(left_schema.field(i).clone());
            columns.push(self.column(i).clone());
        }
        for i in 0..right_schema.fields().len() {
            let name = right_schema.field(i).name().as_str();
            if added_name.contains(name) {
                continue;
            }
            added_name.insert(name);
            fields.push(right_schema.field(i).clone());
            columns.push(right.column(i).clone());
        }
        let schema = Schema::new(fields);
        RecordBatch::try_new(Arc::new(schema), columns)
    }

    fn to_json_rows(&self) -> Result<Vec<JsonMap<String, JsonValue>>, ArrowError> {
        let buf = Vec::new();
        let mut writer = arrow::json::ArrayWriter::new(buf);
        writer.write(self)?;
        writer.finish()?;
        let json_data = writer.into_inner();
        if json_data.is_empty() {
            return Ok(vec![]);
        }
        serde_json::from_reader(json_data.as_slice()).map_err(|err| {
            ArrowError::JsonError(format!("Can't cast batch to json rows: {:?}", err))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_take() {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("a", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("b", arrow_schema::DataType::Utf8, false),
        ]);
        let a = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let b = arrow::array::StringArray::from(vec!["a", "b", "c"]);
        let record_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();
        let indices = arrow::array::Int64Array::from(vec![1, 2]);
        let record_batch = record_batch.take(&indices).unwrap();
        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(
            record_batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            record_batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            "b"
        );
    }

    #[test]
    fn test_json_rows() {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("b", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("a", arrow_schema::DataType::Utf8, false),
        ]);
        let a = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let b = arrow::array::StringArray::from(vec!["a", "b", "c"]);
        let record_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();
        let json_rows = record_batch.to_json_rows().unwrap();
        dbg!(&json_rows);

        let json_rows_str = serde_json::to_string(&json_rows).unwrap();
        assert_eq!(
            json_rows_str,
            r#"[{"b":1,"a":"a"},{"b":2,"a":"b"},{"b":3,"a":"c"}]"#
        );

        let buf = Vec::new();
        let mut writer = arrow::json::ArrayWriter::new(buf);
        writer.write(&record_batch).unwrap();
        writer.finish().unwrap();
        let json_data = writer.into_inner();
        let json_rows: Vec<BTreeMap<String, JsonValue>> =
            serde_json::from_reader(json_data.as_slice()).unwrap();
        dbg!(&json_rows);

        let json_rows_str = serde_json::to_string(&json_rows).unwrap();
        assert_eq!(
            json_rows_str,
            r#"[{"a":"a","b":1},{"a":"b","b":2},{"a":"c","b":3}]"#
        );
    }

    #[test]
    fn test_concat() {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("a", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("b", arrow_schema::DataType::Utf8, false),
        ]);
        let a = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let b = arrow::array::StringArray::from(vec!["a", "b", "c"]);
        let left = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("c", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("d", arrow_schema::DataType::Utf8, false),
        ]);
        let a = arrow::array::Int64Array::from(vec![4, 5, 6]);
        let b = arrow::array::StringArray::from(vec!["d", "e", "f"]);
        let right = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();

        let record_batch = left.concat_by_columns(&right).unwrap();
        assert_eq!(record_batch.num_columns(), 4);

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("c", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("d", arrow_schema::DataType::Utf8, false),
        ]);
        let a = arrow::array::Int64Array::from(vec![4, 5, 6]);
        let b = arrow::array::StringArray::from(vec!["d", "e", "f"]);
        let right = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();

        let record_batch = record_batch.concat_by_columns(&right).unwrap();
        assert_eq!(record_batch.num_columns(), 4);

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("e", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("e", arrow_schema::DataType::Utf8, false),
        ]);
        let a = arrow::array::Int64Array::from(vec![4, 5, 6]);
        let b = arrow::array::StringArray::from(vec!["d\0", "e\0", "f\0"]);
        let right = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();

        let record_batch = right.concat_by_columns(&record_batch).unwrap();
        assert_eq!(record_batch.num_columns(), 5);
    }
}
