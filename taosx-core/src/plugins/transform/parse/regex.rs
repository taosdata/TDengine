use arrow::array::{BooleanArray, Float32Array, Float64Array, StringBuilder};
use arrow::datatypes::Field;
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::plugins::transform::Select;

use super::Parse;
use thiserror::Error;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Regex {
    #[serde(with = "serde_regex")]
    regex: regex::Regex,
    #[serde(default)]
    select: Option<Select>,
    #[serde(default)]
    keep: bool,
}

#[derive(Debug)]
enum ExtractRule<'a> {
    ByCaptureNames(Vec<&'a str>),
    ByCaptureLocations(usize),
    ByRegexMatch,
}

impl ExtractRule<'_> {
    fn schema(&self, name: &str) -> Schema {
        match self {
            ExtractRule::ByCaptureNames(names) => {
                let fields = names
                    .iter()
                    .map(|name| Field::new(*name, DataType::Utf8, true))
                    .collect_vec();
                Schema::new(fields)
            }
            ExtractRule::ByCaptureLocations(n) => {
                let fields = (0..*n)
                    .map(|i| Field::new(&format!("{}{}", name, i), DataType::Utf8, true))
                    .collect_vec();
                Schema::new(fields)
            }
            ExtractRule::ByRegexMatch => Schema::new(vec![Field::new(name, DataType::Utf8, true)]),
        }
    }
}
impl Regex {
    fn extract_rule(&self) -> ExtractRule {
        let names = self
            .regex
            .capture_names()
            .flatten()
            .map(|s| s)
            .collect_vec();
        dbg!(&names);
        if names.len() > 0 {
            // TODO: extract by capture names
            return ExtractRule::ByCaptureNames(names);
        }
        // capture len is the number of capture groups + 1
        let caps = self.regex.captures_len();
        if caps > 1 {
            // TODO: extract by capture locations
            return ExtractRule::ByCaptureLocations(caps);
        }
        // TODO: extract by regex match

        ExtractRule::ByRegexMatch
    }

    fn to_empty(&self, name: &str) -> RecordBatch {
        RecordBatch::new_empty(Arc::new(self.schema(name)))
    }

    fn schema(&self, name: &str) -> Schema {
        let schema = self.extract_rule().schema(name);
        if let Some(select) = self.select.as_ref() {
            select.schema(&schema)
        } else {
            schema
        }
    }
}

#[derive(Debug, Error)]
#[error("Regex error str: {item:?} info: {info:?} regex: {regex:?}")]
pub struct RegexError {
    info: String,
    item: String,
    regex: String,
}

impl Parse for Regex {
    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(arrow::record_batch::RecordBatch, Option<Vec<usize>>), super::ParseError> {
        if array.len() == 0 {
            return Ok((self.to_empty(field.name()), None));
        }
        let array = arrow::compute::cast(array, &DataType::Utf8).map_err(|err| RegexError {
            info: format!(
                "Unsupported data type {:?} for parsing as regex",
                array.data_type(),
            ),
            item: err.to_string(),
            regex: self.regex.to_string(),
        })?;
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| RegexError {
                info: "Failed to downcast ArrayRef to StringArray".to_owned(),
                item: "".to_string(),
                regex: self.regex.to_string(),
            })?;
        let num_rows = string_array.len();

        let rule = self.extract_rule();

        match rule {
            ExtractRule::ByCaptureNames(names) => {
                let fields: Vec<_> = names
                    .into_iter()
                    .map(|s| Field::new(s, DataType::Utf8, true))
                    .collect();

                let mut schema = Schema::new(fields);
                if let Some(select) = self.select.as_ref() {
                    schema = select.schema(&schema);
                }

                let mut arrays = Vec::new();
                let fields = schema.fields.clone();
                // dbg!(&schema);
                // dbg!(&fields);
                if self.keep {
                    if fields.iter().any(|f| f.name() == field.name()) {
                        Err(anyhow::anyhow!(
                            "Keep field name {:?} is already in the regex capture names",
                            field.name()
                        ))?;
                    }
                    arrays.push((field.name(), array.clone()));
                }

                let mut captures_groups = Vec::with_capacity(num_rows);
                for row_index in 0..num_rows {
                    if string_array.is_null(row_index) {
                        captures_groups.push(None)
                    } else {
                        let value = string_array.value(row_index);
                        let caps = self.regex.captures(&value).ok_or_else(|| RegexError {
                            info: format!("Regex pattern {:?} has no capture groups", self.regex),
                            item: value.to_string(),
                            regex: self.regex.to_string(),
                        })?;
                        captures_groups.push(Some(caps));
                    }
                }
                for f in &fields {
                    let name = f.name();
                    let dt = f.data_type();
                    macro_rules! get_values {
                        ($ty:ty) => {
                            captures_groups
                                .iter()
                                .map(|caps| {
                                    if let Some(caps) = caps {
                                        caps.name(name)
                                            .map(|v| {
                                                v.as_str().parse::<$ty>().map_err(|err| {
                                                    RegexError {
                                                        info: err.to_string(),
                                                        item: v.as_str().to_string(),
                                                        regex: self.regex.to_string(),
                                                    }
                                                })
                                            })
                                            .transpose()
                                    } else {
                                        Ok(None)
                                    }
                                })
                                .try_collect::<_, Vec<_>, _>()
                        };
                    }
                    match dt {
                        DataType::UInt8 => {
                            let values = get_values!(u8)?;
                            let array: ArrayRef = Arc::new(UInt8Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::UInt16 => {
                            let values = get_values!(u16)?;
                            let array: ArrayRef = Arc::new(UInt16Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::UInt32 => {
                            let values = get_values!(u32)?;
                            let array: ArrayRef = Arc::new(UInt32Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::UInt64 => {
                            let values = get_values!(u64)?;
                            let array: ArrayRef = Arc::new(UInt64Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Int8 => {
                            let values = get_values!(i8)?;
                            let array: ArrayRef = Arc::new(Int8Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Int16 => {
                            let values = get_values!(i16)?;
                            let array: ArrayRef = Arc::new(Int16Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Int32 => {
                            let values = get_values!(i32)?;
                            let array: ArrayRef = Arc::new(Int32Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Int64 => {
                            let values = get_values!(i64)?;
                            let array: ArrayRef = Arc::new(Int64Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Float32 => {
                            let values = get_values!(f32)?;
                            let array: ArrayRef = Arc::new(Float32Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Float64 => {
                            let values = get_values!(f64)?;
                            let array: ArrayRef = Arc::new(Float64Array::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Boolean => {
                            let values = get_values!(bool)?;
                            let array: ArrayRef = Arc::new(BooleanArray::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Utf8 | DataType::LargeUtf8 => {
                            let values = captures_groups
                                .iter()
                                .map(|caps| {
                                    if let Some(caps) = caps {
                                        caps.name(name).map(|v| v.as_str())
                                    } else {
                                        None
                                    }
                                })
                                .collect_vec();
                            let array: ArrayRef = Arc::new(StringArray::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        DataType::Binary | DataType::LargeBinary => {
                            let values = captures_groups
                                .iter()
                                .map(|caps| {
                                    if let Some(caps) = caps {
                                        caps.name(name).map(|v| v.as_str().as_bytes())
                                    } else {
                                        None
                                    }
                                })
                                .collect_vec();
                            let array: ArrayRef = Arc::new(BinaryArray::from_iter(values));
                            arrays.push((f.name(), array));
                        }
                        dt => {
                            tracing::error!("Regex field parser doest not support type: {dt}");
                            // return Ok((RecordBatch::new_empty(Arc::new(schema)), None));
                        }
                    }
                }
                let records = RecordBatch::try_from_iter(arrays)?;
                Ok((records, None))
            }
            ExtractRule::ByCaptureLocations(caps) => {
                let names = (0..caps)
                    .map(|i| Field::new(format!("{}{}", field.name(), i), DataType::Utf8, true))
                    .collect_vec();
                let mut arrays = std::iter::repeat_with(|| StringBuilder::new())
                    .take(caps)
                    .collect_vec();
                for row_index in 0..num_rows {
                    if string_array.is_null(row_index) {
                        for array in &mut arrays {
                            array.append_null();
                        }
                    } else {
                        let value = string_array.value(row_index);
                        match self.regex.captures(&value) {
                            Some(caps) => {
                                for i in 0..caps.len() {
                                    let array = arrays.get_mut(i).unwrap();
                                    if let Some(value) = caps.get(i) {
                                        array.append_value(value.as_str());
                                        continue;
                                    } else {
                                        array.append_null();
                                    }
                                }
                            }
                            None => {
                                for array in &mut arrays {
                                    array.append_null();
                                }
                            }
                        }
                    }
                }

                let schema = Arc::new(Schema::new(names));
                let columns = arrays
                    .into_iter()
                    .map(|mut array| Arc::new(array.finish()) as ArrayRef)
                    .collect_vec();

                let mut records = RecordBatch::try_new(schema, columns).unwrap();
                if let Some(select) = self.select.as_ref() {
                    records = select.record_batch(&records)?;
                }
                Ok((records, None))
            }
            ExtractRule::ByRegexMatch => {
                let mut array = StringBuilder::new();
                for row_index in 0..num_rows {
                    if string_array.is_null(row_index) {
                        array.append_null();
                    } else {
                        let value = string_array.value(row_index);
                        match self.regex.captures(&value) {
                            Some(caps) => {
                                if let Some(cap) = caps.get(0) {
                                    array.append_value(cap.as_str());
                                } else {
                                    array.append_null();
                                }
                            }
                            None => {
                                array.append_null();
                            }
                        }
                    }
                }

                let schema = Arc::new(Schema::new(vec![Field::new(
                    field.name(),
                    DataType::Utf8,
                    true,
                )]));
                let columns = vec![Arc::new(array.finish()) as ArrayRef];
                let records = RecordBatch::try_new(schema, columns).unwrap();
                Ok((records, None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn regex_test() {
        let re = Regex {
            regex: regex::Regex::new(r"'(?P<title>[^']+)'\s+\((?P<year>\d{4})\)").unwrap(),
            select: Some(serde_json::from_str(&r#"["title::nchar(100)", "year::i32"]"#).unwrap()),
            keep: false,
        };
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"Not my favorite movie: 'Citizen Kane' (1941)."#,
            r#"Not my favorite movie: 'Movie1' (1942)."#,
            r#"Not my favorite movie: 'Movie2' (1943)."#,
        ]));
        let (records, _) = re.parse_array(&field, &array).unwrap();

        dbg!(&records);
        assert_eq!(records.num_columns(), 2);
        assert_eq!(records.num_rows(), 3);
        let values = vec!["Citizen Kane", "Movie1", "Movie2"];
        let array: ArrayRef = Arc::new(StringArray::from(values));
        assert_eq!(records.column(0), &array);
        let values = vec![1941i32, 1942i32, 1943i32];
        let array: ArrayRef = Arc::new(Int32Array::from(values));
        assert_eq!(records.column(1), &array);
    }

    #[test]
    fn regex_caps() {
        let re = Regex {
            regex: regex::Regex::new(r"'([^']+)'\s+\((\d{4})\)").unwrap(),
            select: Some(serde_json::from_str(r#"["a1::nchar(100)", "a2::i32"]"#).unwrap()),
            keep: false,
        };
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"Not my favorite movie: 'Citizen Kane' (1941)."#,
            r#"Not my favorite movie: 'Movie1' (1942)."#,
            r#"Not my favorite movie: 'Movie2' (1943)."#,
        ]));
        let (records, _) = re.parse_array(&field, &array).unwrap();

        dbg!(&records);
        assert_eq!(records.num_columns(), 2);
        assert_eq!(records.num_rows(), 3);
        let values = vec!["Citizen Kane", "Movie1", "Movie2"];
        let array: ArrayRef = Arc::new(StringArray::from(values));
        assert_eq!(records.column(0), &array);
        let values = vec![1941i32, 1942i32, 1943i32];
        let array: ArrayRef = Arc::new(Int32Array::from(values));
        assert_eq!(records.column(1), &array);
    }

    #[test]
    fn regex_single() {
        let re = Regex {
            regex: regex::Regex::new(r"'[^']+'\s+\(\d{4}\)").unwrap(),
            select: None,
            keep: false,
        };
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![
            r#"Not my favorite movie: 'Citizen Kane' (1941)."#.as_bytes(),
            r#"Not my favorite movie: 'Movie1' (1942)."#.as_bytes(),
            r#"Not my favorite movie: 'Movie2' (1943)."#.as_bytes(),
        ]));
        let (records, _) = re.parse_array(&field, &array).unwrap();

        dbg!(&records);
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 3);
        let values = vec![
            "'Citizen Kane' (1941)",
            "'Movie1' (1942)",
            "'Movie2' (1943)",
        ];
        let array: ArrayRef = Arc::new(StringArray::from(values));
        assert_eq!(records.column(0), &array);
    }
}
