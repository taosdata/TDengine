use std::{sync::Arc};

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

impl RecordMessage {
    pub fn schema(&self) -> SchemaRef {
        self.record.schema()
    }

    pub fn record(&self) -> &RecordBatch {
        &self.record
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
