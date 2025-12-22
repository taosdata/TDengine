use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::{filter::Filter, map::Map, parse::ParserImpl, TransformExt};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Mutate {
    Extract(ParserImpl),
    Filter(Filter),
    Map(Map),
}

impl TransformExt for Mutate {
    #[instrument(skip_all)]
    fn transform_record_batch(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, super::Error> {
        match self {
            Mutate::Extract(parser) => parser.transform_record_batch(records),
            Mutate::Filter(filter) => filter.transform_record_batch(records),
            Mutate::Map(map) => map.transform_record_batch(records),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn test_mutate_as_filter() {
        let mutate = r#"{"filter": ["a > b && c != 0"]}"#;
        let mutate: Mutate = serde_json::from_str(mutate).unwrap();
        dbg!(mutate);
    }

    #[test]
    fn test_mutate_as_map() {
        let mutate = r#"{"map":{"new1":{"sum":["a","b"],"as":"INT"},"new2":{"join":["a","b"],"with":"&&"}}}"#;
        let mutate: Mutate = serde_json::from_str(mutate).unwrap();
        dbg!(mutate);
    }

    #[test]
    fn test_mutate_as_extract() {
        let mutate = r#"{"extract":{"payload": {"json": ""}}}"#;
        let mutate: Mutate = serde_json::from_str(mutate).unwrap();
        dbg!(mutate);
    }

    #[test]
    fn test_mutate_as_extract_join() {
        let mutate = r#"{"extract":{"payload": {"join": ","}}}"#;
        let mutate: Mutate = serde_json::from_str(mutate).unwrap();
        dbg!(mutate);
    }

    #[test]
    fn test_mutate_filter_filters_rows() {
        let mutate: Mutate = serde_json::from_str(r#"{"filter":["a > 1"]}"#).unwrap();
        let batch = sample_batch();

        let filtered = mutate.transform_record_batch(&batch).unwrap();

        assert_eq!(filtered.num_rows(), 1);
        let a = filtered
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), 2);
    }

    #[test]
    fn test_mutate_map_adds_column() {
        let mutate: Mutate =
            serde_json::from_str(r#"{"map":{"concat":{"join":["a","b"],"with":"-"}}}"#).unwrap();
        let batch = sample_batch();

        let mapped = mutate.transform_record_batch(&batch).unwrap();

        assert_eq!(mapped.num_columns(), 3);
        let concat = mapped
            .column_by_name("concat")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(concat.value(0), "1-10");
        assert_eq!(concat.value(1), "2-20");
    }

    #[test]
    fn test_mutate_extract_renames_field() {
        let mutate: Mutate =
            serde_json::from_str(r#"{"extract":{"a":{"alias":"renamed"}}}"#).unwrap();
        let batch = sample_batch();

        let extracted = mutate.transform_record_batch(&batch).unwrap();

        assert!(extracted.column_by_name("a").is_none());
        let renamed = extracted
            .column_by_name("renamed")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(renamed.len(), 2);
        assert_eq!(renamed.value(0), 1);
        assert_eq!(renamed.value(1), 2);

        let b = extracted
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(b.value(0), 10);
        assert_eq!(b.value(1), 20);
    }

    fn sample_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        RecordBatch::try_new(Arc::new(schema), vec![a, b]).unwrap()
    }
}
