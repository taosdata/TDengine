use serde::{Deserialize, Serialize};

use super::{filter::Filter, map::Map, TransformExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mutate {
    Filter(Filter),
    Map(Map),
}

impl TransformExt for Mutate {
    fn transform_record_batch(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, super::Error> {
        match self {
            Mutate::Filter(filter) => filter.transform_record_batch(records),
            Mutate::Map(map) => map.transform_record_batch(records),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutate() {
        let mutate = r#"{"filter": ["a > b && c != 0"]}"#;
        let mutate: Mutate = serde_json::from_str(mutate).unwrap();
        dbg!(mutate);
    }
}
