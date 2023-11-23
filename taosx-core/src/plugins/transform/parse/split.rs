use arrow::{
    array::{Array, ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

use super::Parse;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Split {
    split: SplitImpl,
}

#[derive(Debug, Error)]
#[error("Invalid split rule, error: {error:?}")]
pub struct SplitError {
    error: String,
}

impl Parse for Split {
    fn num_rows_will_be_changed(&self) -> bool {
        match &self.split {
            SplitImpl::SplitBySep(split) => split.num_rows_will_be_changed(),
            SplitImpl::SplitBySeps(split) => split.num_rows_will_be_changed(),
            SplitImpl::SplitByAt(split) => split.num_rows_will_be_changed(),
        }
    }

    fn num_columns_will_be_changed(&self) -> bool {
        match &self.split {
            SplitImpl::SplitBySep(split) => split.num_columns_will_be_changed(),
            SplitImpl::SplitBySeps(split) => split.num_columns_will_be_changed(),
            SplitImpl::SplitByAt(split) => split.num_columns_will_be_changed(),
        }
    }

    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(arrow::record_batch::RecordBatch, Option<Vec<usize>>), super::ParseError> {
        match &self.split {
            SplitImpl::SplitBySep(split) => split.parse_array(field, array),
            SplitImpl::SplitBySeps(split) => split.parse_array(field, array),
            SplitImpl::SplitByAt(split) => split.parse_array(field, array),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SplitImpl {
    SplitBySep(SplitBySepImpl),
    SplitBySeps(SplitBySepsImpl),
    SplitByAt(SplitByAtImpl),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitBySepImpl {
    field: String,
    sep: char,
    n: usize,
    reverse: Option<bool>,
    remove: Option<bool>,
    inplace: Option<bool>,
    names: Vec<String>,
}

impl Parse for SplitBySepImpl {
    fn num_rows_will_be_changed(&self) -> bool {
        false
    }

    fn num_columns_will_be_changed(&self) -> bool {
        self.n > 1 || !self.remove.unwrap_or(false)
    }

    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        // check field name
        if field.name() != &self.field {
            Err(SplitError {
                error: String::from("inconsistent field name"),
            })?;
        }
        // check if names.len = n
        if self.names.len() != self.n {
            Err(SplitError {
                error: String::from("names.len != n"),
            })?;
        }
        // downcast field values to string
        let array_utf8 = arrow::compute::cast(array, &DataType::Utf8)?;
        // loop and split
        let values: Vec<_> = array_utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| match value {
                Some(value) => {
                    let mut strs: Vec<&str> = value.split(self.sep).collect();
                    strs.resize(self.n, "");
                    strs
                }
                None => vec![""; self.n],
            })
            .collect_vec();
        // new arrays
        let mut arrays = Vec::new();
        let mut names = self.names.clone();
        // whether reverse
        if self.reverse.unwrap_or(false) {
            names.reverse()
        }
        // loop and package
        for i in 0..self.n {
            let datas = values.iter().map(|value| Some(value[i])).collect_vec();
            let array: ArrayRef = Arc::new(StringArray::from_iter(datas));
            arrays.push((&names[i], array));
        }
        // whether remove
        if !self.remove.unwrap_or(false) {
            arrays.push((field.name(), array.clone()));
        }
        let records = RecordBatch::try_from_iter(arrays).unwrap();
        Ok((records, None))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitBySepsImpl {
    field: String,
    sep: Vec<char>,
    n: usize,
    reverse: Option<bool>,
    remove: Option<bool>,
    inplace: Option<bool>,
    names: Vec<String>,
}

impl Parse for SplitBySepsImpl {
    fn num_rows_will_be_changed(&self) -> bool {
        false
    }

    fn num_columns_will_be_changed(&self) -> bool {
        self.n > 1 || !self.remove.unwrap_or(false)
    }

    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        // check field name
        if field.name() != &self.field {
            Err(SplitError {
                error: String::from("inconsistent field name"),
            })?;
        }
        // check if names.len = n
        if self.names.len() != self.n {
            Err(SplitError {
                error: String::from("names.len != n"),
            })?;
        }
        // downcast field values to string
        let array_utf8 = arrow::compute::cast(array, &DataType::Utf8)?;
        // loop and split
        let values: Vec<_> = array_utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| match value {
                Some(value) => {
                    let mut strs: Vec<&str> = value.split_terminator(self.sep.as_slice()).collect();
                    strs.resize(self.n, "");
                    strs
                }
                None => vec![""; self.n],
            })
            .collect_vec();
        // new arrays
        let mut arrays = Vec::new();
        let mut names = self.names.clone();
        // whether reverse
        if self.reverse.unwrap_or(false) {
            names.reverse()
        }
        // loop and package
        for i in 0..self.n {
            let datas = values.iter().map(|value| Some(value[i])).collect_vec();
            let array: ArrayRef = Arc::new(StringArray::from_iter(datas));
            arrays.push((&names[i], array));
        }
        // whether remove
        if !self.remove.unwrap_or(false) {
            arrays.push((field.name(), array.clone()));
        }
        let records = RecordBatch::try_from_iter(arrays).unwrap();
        Ok((records, None))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitByAtImpl {
    field: String,
    at: usize,
    remove: Option<bool>,
    inplace: Option<bool>,
    names: Vec<String>,
}

impl Parse for SplitByAtImpl {
    fn num_rows_will_be_changed(&self) -> bool {
        false
    }

    fn num_columns_will_be_changed(&self) -> bool {
        true
    }

    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        // check field name
        if field.name() != &self.field {
            Err(SplitError {
                error: String::from("inconsistent field name"),
            })?;
        }
        // check if names.len = 2
        if self.names.len() != 2 {
            Err(SplitError {
                error: String::from("names.len != 2"),
            })?;
        }
        // downcast field values to string
        let array_utf8 = arrow::compute::cast(array, &DataType::Utf8)?;
        // loop and split
        let values: Vec<_> = array_utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| match value {
                Some(value) => {
                    let (first, last) = value.split_at(self.at);
                    vec![first, last]
                }
                None => vec![""; 2],
            })
            .collect_vec();
        // new arrays
        let mut arrays = Vec::new();
        // loop and package
        for i in 0..2 {
            let datas = values.iter().map(|value| Some(value[i])).collect_vec();
            let array: ArrayRef = Arc::new(StringArray::from_iter(datas));
            arrays.push((&self.names[i], array));
        }
        // whether remove
        if !self.remove.unwrap_or(false) {
            arrays.push((field.name(), array.clone()));
        }
        let records = RecordBatch::try_from_iter(arrays).unwrap();
        Ok((records, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_split() {
        let string = "abcdef,gh ijklm nop,qrst.uvw";
        let sep = ',';
        let seps = [' ', ',', '.'];
        let at = 4;

        let mut vec1: Vec<&str> = string.split(sep).collect();
        let mut vec2: Vec<&str> = string.split_terminator(seps).collect();
        let vec3 = string.split_at(at);

        // test resize
        vec1.resize(5, "");
        vec2.resize(5, "");

        assert_eq!(vec1, ["abcdef", "gh ijklm nop", "qrst.uvw", "", ""]);
        assert_eq!(vec2, ["abcdef", "gh", "ijklm", "nop", "qrst"]);
        assert_eq!(vec3, ("abcd", "ef,gh ijklm nop,qrst.uvw"));
    }

    #[test]
    fn test_split_by_sep() {
        let split = r#"{
            "split": {
                "field": "field_name_1",
                "sep": ",",
                "n": 3,
                "reverse": true,
                "remove": false,
                "inplace": true,
                "names": ["n1", "n2", "n3"]
            }
        }"#;
        let split: Split = serde_json::from_str(split).unwrap();
        dbg!(split.clone());

        let field = Field::new("field_name_1", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            "a,1,11", "a,2,22", "b,1,11", "b,2,22",
        ]));

        let (records, _) = split.parse_array(&field, &array).unwrap();
        dbg!(&records);
        assert_eq!(records.num_rows(), 4);
        assert_eq!(records.num_columns(), 4);
    }

    #[test]
    fn test_split_by_seps() {
        let split = r#"{
            "split": {
                "field": "field_name_2",
                "sep": [" ", ".", ":"],
                "n": 3,
                "reverse": false,
                "remove": true,
                "inplace": true,
                "names": ["n1", "n2", "n3"]
            }
        }"#;
        let split: Split = serde_json::from_str(split).unwrap();
        dbg!(split.clone());

        let field = Field::new("field_name_2", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            "a 1.11", "a.2 22", "b:1.11", "b.2:22",
        ]));

        let (records, _) = split.parse_array(&field, &array).unwrap();
        dbg!(&records);
        assert_eq!(records.num_rows(), 4);
        assert_eq!(records.num_columns(), 3);
    }

    #[test]
    fn test_split_by_at() {
        let split = r#"{
            "split": {
                "field": "field_name_3",
                "at": 2,
                "remove": true,
                "inplace": true,
                "names": ["n1", "n2"]
            }
        }"#;
        let split: Split = serde_json::from_str(split).unwrap();
        dbg!(split.clone());

        let field = Field::new("field_name_3", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a111", "a222", "b111", "b222"]));

        let (records, _) = split.parse_array(&field, &array).unwrap();
        dbg!(&records);
        assert_eq!(records.num_rows(), 4);
        assert_eq!(records.num_columns(), 2);
    }
}
