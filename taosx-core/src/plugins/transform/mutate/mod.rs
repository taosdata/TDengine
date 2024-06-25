use serde::{Deserialize, Serialize};

use super::{filter::Filter, map::Map, parse::ParserImpl, TransformExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mutate {
    Extract(ParserImpl),
    Filter(Filter),
    Map(Map),
}

impl TransformExt for Mutate {
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
}
