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

impl Parse for Split {
    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        self.split.parse_array(field, array)
    }
}

#[derive(Debug, Error)]
#[error("Invalid split rule, error: {error:?}")]
pub struct SplitError {
    error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitImpl {
    #[serde(flatten)]
    sep: SplitOps,
    n: Option<usize>,
    reverse: Option<bool>,
    keep: Option<bool>,
    inplace: Option<bool>,
    #[serde(default)]
    names: Vec<String>,
}

impl Parse for SplitImpl {
    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        let (n, mut names) = match (self.n, self.names.len()) {
            (Some(n), 0) => (
                n,
                (0..n)
                    .map(|i| format!("{}_{}", field.name(), i))
                    .collect_vec(),
            ),
            (Some(n), l) if n == l => (n, self.names.clone()),
            (Some(n), l) => Err(SplitError {
                error: format!("Expecting n({}) == names.len({})", n, l),
            })?,
            (None, 0) => Err(SplitError {
                error: String::from("names should not be empty"),
            })?,
            (None, l) => (l, self.names.clone()),
        };
        // downcast field values to string
        let array_utf8 = arrow::compute::cast(array, &DataType::Utf8)?;
        // loop and split
        let values: Vec<_> = array_utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| match value {
                Some(value) => self
                    .sep
                    .split(value)
                    .map(|vec| (0..n).map(|i| vec.get(i).cloned()).collect_vec()),
                None => Ok(vec![None; n]),
            })
            .try_collect()?;
        // new arrays
        let mut arrays = Vec::new();
        // whether reverse
        if self.reverse.unwrap_or(false) {
            names.reverse()
        }
        // loop and package
        for i in 0..n {
            let data = values.iter().map(|value| value[i]).collect_vec();
            let array: ArrayRef = Arc::new(StringArray::from_iter(data));
            arrays.push((&names[i], array));
        }
        // whether remove
        if self.keep.unwrap_or(false) {
            arrays.push((field.name(), array.clone()));
        }
        let records = RecordBatch::try_from_iter(arrays).unwrap();
        Ok((records, None))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum SplitOps {
    At { at: usize },
    Ats { at: Vec<usize> },
    Sep { sep: String },
    Seps { sep: Vec<String> },
}

impl SplitOps {
    pub fn split<'a>(&self, str: &'a str) -> Result<Vec<&'a str>, SplitError> {
        match self {
            SplitOps::At { at } => {
                if *at > str.len() {
                    Err(SplitError {
                        error: format!(
                            "Expecting position at({at}) <= len({len})",
                            at = at,
                            len = str.len()
                        ),
                    })?;
                }
                let (first, last) = str.split_at(*at);
                Ok(vec![first, last])
            }
            SplitOps::Ats { at } => {
                let mut strs: Vec<&str> = Vec::new();
                let mut last = 0;
                for i in at {
                    if *i > str.len() {
                        Err(SplitError {
                            error: format!(
                                "Expecting position at({at}) <= len({len})",
                                at = i,
                                len = str.len()
                            ),
                        })?;
                    }
                    let (first, _) = str.split_at(*i);
                    strs.push(&first[last..]);
                    last = *i;
                }
                strs.push(&str[last..]);
                Ok(strs)
            }
            SplitOps::Sep { sep } => Ok(str.split(sep).collect()),
            SplitOps::Seps { sep } => {
                if sep.is_empty() {
                    Err(SplitError {
                        error: String::from("Expecting separators num > 0"),
                    })?;
                }
                fn split_iter<'a>(str: &'a str, sep: &[String]) -> Vec<&'a str> {
                    debug_assert!(!sep.is_empty());
                    str.split(&sep[0])
                        .flat_map(|s| {
                            if sep.len() > 1 {
                                split_iter(s, &sep[1..])
                            } else {
                                vec![s]
                            }
                        })
                        .collect()
                }
                Ok(split_iter(str, sep))
            }
        }
    }
}

#[test]
fn split_ops() {
    let str = "abcdef,gh ijklm nop,qrst.uvw";
    let ops = SplitOps::Sep {
        sep: String::from(","),
    };
    let strs = ops.split(str).unwrap();
    dbg!(&strs);
    assert_eq!(strs, ["abcdef", "gh ijklm nop", "qrst.uvw"]);
    let ops = SplitOps::Seps {
        sep: vec![String::from(" "), String::from(","), String::from(".")],
    };
    let strs = ops.split(str).unwrap();
    dbg!(&strs);
    assert_eq!(strs, ["abcdef", "gh", "ijklm", "nop", "qrst", "uvw"]);
    let ops = SplitOps::At { at: 4 };
    let strs = ops.split(str).unwrap();
    dbg!(&strs);
    assert_eq!(strs, ["abcd", "ef,gh ijklm nop,qrst.uvw"]);
    let ops = SplitOps::Ats { at: vec![2, 4, 8] };
    let strs = ops.split(str).unwrap();
    dbg!(&strs);
    assert_eq!(strs, ["ab", "cd", "ef,g", "h ijklm nop,qrst.uvw"]);
    let ops = SplitOps::Ats {
        at: vec![2, 4, 8, 40],
    };
    let strs = ops.split(str);
    dbg!(&strs);
    assert!(strs.is_err());
}
#[cfg(test)]
mod tests {
    use super::*;

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
    #[ignore]
    fn test_split_by_sep() {
        let split = r#"{
            "split": {
                "sep": ",",
                "n": 3,
                "reverse": true,
                "remove": false,
                "inplace": true,
                "names": ["n1", "n2", "n3"]
            }
        }"#;
        let split: SplitImpl = serde_json::from_str(split).unwrap();
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
    #[ignore]
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
        let split: SplitImpl = serde_json::from_str(split).unwrap();
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
    #[ignore]
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
        let split: SplitImpl = serde_json::from_str(split).unwrap();
        dbg!(split.clone());

        let field = Field::new("field_name_3", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a111", "a222", "b111", "b222"]));

        let (records, _) = split.parse_array(&field, &array).unwrap();
        dbg!(&records);
        assert_eq!(records.num_rows(), 4);
        assert_eq!(records.num_columns(), 2);
    }
}
