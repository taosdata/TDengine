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

    fn nrows(&self) -> usize {
        self.records.iter().map(|r| r.record.num_rows()).sum()
    }
}

impl FlatMessage {
    pub fn new(records: Vec<RecordMessage>) -> Self {
        FlatMessage { records }
    }

    pub fn records(&self) -> &Vec<RecordMessage> {
        &self.records
    }

    pub fn records_owned(self) -> Vec<RecordMessage> {
        self.records
    }

    pub fn num_rows(&self) -> usize {
        self.records.iter().map(|r| r.record.num_rows()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_record_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let id_array = Int32Array::from((0..num_rows as i32).collect::<Vec<_>>());
        let value_array = Int32Array::from(vec![100; num_rows]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)]).unwrap()
    }

    fn create_test_record_message(num_rows: usize) -> RecordMessage {
        RecordMessage {
            record: create_test_record_batch(num_rows),
        }
    }

    #[test]
    fn test_flat_message_new() {
        let record1 = create_test_record_message(5);
        let record2 = create_test_record_message(3);
        let records = vec![record1, record2];

        let flat_msg = FlatMessage::new(records);
        assert_eq!(flat_msg.records().len(), 2);
    }

    #[test]
    fn test_flat_message_new_empty() {
        let flat_msg = FlatMessage::new(vec![]);
        assert_eq!(flat_msg.records().len(), 0);
    }

    #[test]
    fn test_flat_message_records() {
        let record = create_test_record_message(10);
        let flat_msg = FlatMessage::new(vec![record]);

        let records = flat_msg.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.num_rows(), 10);
    }

    #[test]
    fn test_flat_message_records_owned() {
        let record = create_test_record_message(5);
        let flat_msg = FlatMessage::new(vec![record]);

        let owned_records = flat_msg.records_owned();
        assert_eq!(owned_records.len(), 1);
        assert_eq!(owned_records[0].record.num_rows(), 5);
    }

    #[test]
    fn test_flat_message_num_rows_single_record() {
        let record = create_test_record_message(10);
        let flat_msg = FlatMessage::new(vec![record]);

        assert_eq!(flat_msg.num_rows(), 10);
    }

    #[test]
    fn test_flat_message_num_rows_multiple_records() {
        let record1 = create_test_record_message(5);
        let record2 = create_test_record_message(8);
        let record3 = create_test_record_message(3);
        let flat_msg = FlatMessage::new(vec![record1, record2, record3]);

        assert_eq!(flat_msg.num_rows(), 16);
    }

    #[test]
    fn test_flat_message_num_rows_empty() {
        let flat_msg = FlatMessage::new(vec![]);
        assert_eq!(flat_msg.num_rows(), 0);
    }

    #[test]
    fn test_flat_message_nrows_matches_num_rows() {
        let record1 = create_test_record_message(7);
        let record2 = create_test_record_message(4);
        let flat_msg = FlatMessage::new(vec![record1, record2]);

        assert_eq!(flat_msg.nrows(), flat_msg.num_rows());
        assert_eq!(flat_msg.nrows(), 11);
    }

    #[test]
    fn test_flat_message_debug() {
        let record = create_test_record_message(2);
        let flat_msg = FlatMessage::new(vec![record]);

        let debug_str = format!("{:?}", flat_msg);
        assert!(debug_str.contains("FlatMessage"));
    }

    #[test]
    fn test_ipc_message_as_any() {
        let record = create_test_record_message(5);
        let flat_msg = FlatMessage::new(vec![record]);

        let any = flat_msg.as_any();
        assert!(any.is::<FlatMessage>());
    }

    #[test]
    fn test_ipc_message_nrows() {
        let record1 = create_test_record_message(10);
        let record2 = create_test_record_message(20);
        let flat_msg = FlatMessage::new(vec![record1, record2]);

        let ipc_msg: &dyn IpcMessage = &flat_msg;
        assert_eq!(ipc_msg.nrows(), 30);
    }

    #[test]
    fn test_flat_message_with_zero_row_record() {
        let record = create_test_record_message(0);
        let flat_msg = FlatMessage::new(vec![record]);

        assert_eq!(flat_msg.num_rows(), 0);
        assert_eq!(flat_msg.nrows(), 0);
    }

    #[test]
    fn test_flat_message_large_number_of_records() {
        let mut records = Vec::new();
        for _ in 0..100 {
            records.push(create_test_record_message(1));
        }
        let flat_msg = FlatMessage::new(records);

        assert_eq!(flat_msg.records().len(), 100);
        assert_eq!(flat_msg.num_rows(), 100);
    }

    #[test]
    fn test_flat_message_records_reference_does_not_consume() {
        let record = create_test_record_message(5);
        let flat_msg = FlatMessage::new(vec![record]);

        let _records_ref = flat_msg.records();
        let _records_ref2 = flat_msg.records();
        // Should compile - records() doesn't consume self
    }

    #[test]
    fn test_flat_message_multiple_operations() {
        let record1 = create_test_record_message(3);
        let record2 = create_test_record_message(7);
        let flat_msg = FlatMessage::new(vec![record1, record2]);

        assert_eq!(flat_msg.records().len(), 2);
        assert_eq!(flat_msg.num_rows(), 10);
        assert_eq!(flat_msg.nrows(), 10);

        let debug_str = format!("{:?}", flat_msg);
        assert!(debug_str.contains("FlatMessage"));
    }
}
