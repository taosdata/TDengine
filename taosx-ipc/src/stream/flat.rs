use crate::prelude::IpcMessage;

use super::point::RecordMessage;

#[derive(Debug)]
pub struct FlatMessage {
    records: Vec<RecordMessage>,
}

impl FlatMessage {}

impl IpcMessage for FlatMessage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl FlatMessage {
    pub fn new(records: Vec<RecordMessage>) -> Self {
        FlatMessage { records }
    }

    pub fn records(&self) -> &Vec<RecordMessage> {
        &self.records
    }

    pub fn num_rows(&self) -> usize {
        self.records.iter().map(|r| r.record.num_rows()).sum()
    }
}
