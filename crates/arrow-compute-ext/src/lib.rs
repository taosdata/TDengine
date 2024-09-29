use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::{
    array::{Array, RecordBatch},
    compute::take,
    error::ArrowError,
};
use arrow_schema::Field;
use arrow_schema::Schema;
pub trait RecordBatchExt {
    fn take(&self, indices: &dyn Array) -> Result<RecordBatch, ArrowError>;

    /// 左右拼接行数相同的 RecordBatch, 如果 right 包含 left 已存在的列，则会忽略。
    fn concat_by_columns(&self, right: &RecordBatch) -> Result<RecordBatch, ArrowError>;
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
}
