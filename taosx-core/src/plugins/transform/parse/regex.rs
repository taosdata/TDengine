use serde::{Deserialize, Serialize};

use crate::plugins::transform::Select;

use super::Parse;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Regex {
    #[serde(with = "serde_regex")]
    regex: ::regex::Regex,
    #[serde(default)]
    select: Option<Select>,
    #[serde(default)]
    keep: bool,
}

impl Parse for Regex {
    fn num_rows_will_be_changed(&self) -> bool {
        false
    }

    fn num_columns_will_be_changed(&self) -> bool {
        !self.keep && self.regex.capture_names().filter_map(|v| v).count() == 1
    }

    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(arrow::record_batch::RecordBatch, Option<Vec<usize>>), super::ParseError> {
        let _ = (field, array);
        todo!()
    }
}
