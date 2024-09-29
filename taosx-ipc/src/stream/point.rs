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
    pub(crate) fn new(records: Vec<RecordMessage>) -> Self {
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

    pub fn record(&self) -> &RecordBatch {
        &self.record
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
