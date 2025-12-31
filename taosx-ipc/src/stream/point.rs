use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::{
    array::{Array, ListArray, StructArray},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};

use crate::prelude::IpcMessage;

#[derive(Debug)]
pub struct PointMessage {
    records: Vec<RecordMessage>,
}

impl PointMessage {
    pub fn new(records: Vec<RecordMessage>) -> Self {
        PointMessage { records }
    }

    pub fn records(&self) -> &Vec<RecordMessage> {
        &self.records
    }
}

impl IpcMessage for PointMessage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn nrows(&self) -> usize {
        self.records.iter().map(|r| r.record().num_rows()).sum()
    }
}

#[derive(Debug)]
pub struct RecordMessage {
    pub(crate) record: RecordBatch,
}

impl From<RecordBatch> for RecordMessage {
    fn from(record: RecordBatch) -> Self {
        Self { record }
    }
}

impl RecordMessage {
    pub fn from_record(record: RecordBatch) -> Self {
        Self { record }
    }

    pub fn schema(&self) -> SchemaRef {
        self.record.schema()
    }

    pub fn schema_ref(&self) -> &SchemaRef {
        self.record.schema_ref()
    }

    pub fn record(&self) -> &RecordBatch {
        &self.record
    }

    pub fn record_owned(self) -> RecordBatch {
        self.record
    }

    /// get column_type by name
    /// # Arguments
    /// * `col_name` - column name
    pub fn column_type_by_name(&self, col_name: &str) -> Option<DataType> {
        self.schema()
            .field_with_name(col_name)
            .map(|f| Some(f.data_type().clone()))
            .unwrap_or(None)
    }

    /// get a cloned column by name and data type
    /// # Arguments
    /// * `col_name` - column name
    /// * `col_type` - column data type
    pub fn clone_column_by_name(&self, col_name: &str) -> anyhow::Result<ArrayRef> {
        self.record
            .column_by_name(col_name)
            .cloned()
            .ok_or(anyhow::anyhow!(
                "column: {} not exist in record message",
                col_name
            ))
    }
}

impl From<Arc<dyn Array>> for RecordMessage {
    fn from(value: Arc<dyn Array>) -> Self {
        let s = value
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("parse records list");
        let v = s.value(0);
        let s = v
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("parse records struct");

        // todo!()
        let names = s.column_names();
        let columns = s.columns();
        let record = RecordBatch::try_from_iter(
            names
                .into_iter()
                .zip(columns)
                .map(|(name, value)| (name, value.clone())),
        )
        .unwrap();
        Self { record }
    }
}

pub struct RecordTransform {
    pub column_name: Option<String>,
    pub transform_expression: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn make_record_batch() -> RecordBatch {
        let id = Int32Array::from(vec![1, 2, 3]);
        let name = StringArray::from(vec!["a", "b", "c"]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id), Arc::new(name)]).unwrap()
    }

    #[test]
    fn point_message_nrows_and_records() {
        let rb = make_record_batch();
        let pm = PointMessage::new(vec![RecordMessage::from_record(rb.clone())]);
        assert_eq!(pm.nrows(), rb.num_rows());
        assert_eq!(pm.records().len(), 1);
    }

    #[test]
    fn record_message_accessors_and_type_queries() {
        let rb = make_record_batch();
        let rm = RecordMessage::from_record(rb);
        assert_eq!(rm.schema().fields().len(), 2);
        assert!(rm.schema_ref().fields().len() == 2);
        assert_eq!(rm.record().num_rows(), 3);
        assert!(matches!(
            rm.column_type_by_name("id"),
            Some(DataType::Int32)
        ));
        assert!(matches!(
            rm.column_type_by_name("name"),
            Some(DataType::Utf8)
        ));
        assert!(rm.column_type_by_name("missing").is_none());
    }

    #[test]
    fn record_message_clone_column_by_name_ok_and_err() {
        let rb = make_record_batch();
        let rm = RecordMessage::from_record(rb);
        let col = rm.clone_column_by_name("name").unwrap();
        assert_eq!(col.len(), rm.record().num_rows());
        let err = rm.clone_column_by_name("nope").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("not exist"));
    }
}
