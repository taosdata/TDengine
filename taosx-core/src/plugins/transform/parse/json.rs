use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use super::{super::Select, Parse};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Json {
    json: Option<Select>,
    #[serde(default)]
    flatten: bool,
    #[serde(default)]
    keep: bool,
}

#[derive(Debug, Error)]
#[error("Invalid select rule from str: {item:?}, error: {source}")]
pub struct JsonParserError {
    item: String,
    source: serde_json::Error,
    // backtrace: Backtrace,
}

impl FromStr for Json {
    type Err = JsonParserError;

    /// A [Json] parser could be built from string like:
    ///
    /// - `*`: Extract all fields from the json object.
    /// - `a,b`: Extract selected fields from json object.
    /// - `[a,b]`: Extract selected fields from json object with flatten enabled
    /// - `+[a,b]`: Extract selected fields from json object with flatten/keep enabled
    ///
    /// # Example
    ///
    /// For a string array contains json: `{"a": 0, "b": 1, "c": 2}`,
    /// use json parser with option `a, b` will result to a record batch:
    ///
    /// ```text
    /// +++++++++
    /// | a | b |
    /// +===+===+
    /// | 0 | 1 |
    /// +++++++++
    /// ```
    ///
    /// If keep enabled, the result batch is:
    ///
    /// ```text
    /// +--------------------------+---+---+
    /// |  original field name     | a | b |
    /// +==========================+===+===+
    /// | {"a": 0, "b": 1, "c": 2} | 0 | 1 |
    /// +--------------------------+---+---+
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut s = s.trim();
        let mut slf = Self::default();
        if s.is_empty() || s == "." || s == "*" {
            return Ok(slf);
        }
        if s.starts_with('+') {
            slf.keep = true;
            s = s.trim_start_matches('+');
        }
        if s.starts_with('[') {
            slf.flatten = true;
            s = s.trim_matches(|c| c == '[' || c == ']');
        }

        slf.json.replace(
            s.trim()
                .parse::<Select>()
                .map_err(|source| JsonParserError {
                    item: s.to_string(),
                    source,
                })?,
        );
        Ok(slf)
    }
}

impl Parse for Json {
    fn num_rows_will_be_changed(&self) -> bool {
        !self.flatten
    }

    fn num_columns_will_be_changed(&self) -> bool {
        true
    }

    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        let array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string = array.as_any().downcast_ref::<StringArray>().unwrap();
        let num_rows = string.len();

        let json_data: Vec<_> = (0..string.len())
            .filter_map(|index| {
                if string.is_null(index) {
                    None
                } else {
                    let value = string.value(index);
                    let value: serde_json::Value = serde_json::from_str(&value).ok()?;

                    if value.is_array() && self.flatten {
                        let values = value.as_array().unwrap();
                        Some(values.clone())
                    } else {
                        Some(vec![value])
                    }
                }
            })
            .flatten()
            .map(Ok)
            .collect();
        if json_data.len() == 0 {
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }
        let mut schema =
            arrow::json::reader::infer_json_schema_from_iterator(json_data.into_iter())?;
        if let Some(select) = self.json.as_ref() {
            schema = select.schema(&schema);
        }
        let json_values: Vec<_> = (0..num_rows)
            .enumerate()
            .flat_map(|(n, i)| {
                if string.is_null(i) {
                    vec![(n, None)]
                } else {
                    let str = string.value(i);
                    let value = serde_json::from_str::<serde_json::Value>(&str);

                    match value {
                        Ok(JsonValue::Array(array)) => {
                            if !self.flatten {
                                return vec![(n, None)];
                            }
                            array
                                .into_iter()
                                .map(|v| {
                                    (n, {
                                        if v.is_null() {
                                            None
                                        } else {
                                            debug_assert!(v.is_object());
                                            Some(v)
                                        }
                                    })
                                })
                                .collect()
                        }
                        Ok(JsonValue::Null) => {
                            vec![(n, None)]
                        }
                        Ok(JsonValue::Object(object)) => {
                            vec![(n, Some(JsonValue::Object(object)))]
                        }
                        Err(err) => {
                            tracing::error!("Parsing json error: {err}, from string: `{str}`");
                            vec![]
                        }
                        Ok(v) => {
                            tracing::error!("Expect json object or array, found: {v:?}");
                            vec![]
                        }
                    }
                }
            })
            .collect();

        let mut arrays = Vec::new();

        let fields = schema.fields().clone();
        // dbg!(&fields);
        if self.keep {
            arrays.push((field.name(), array.clone()));
            let len = schema.fields().len();
            schema = Schema::try_merge(vec![Schema::new(vec![field.clone()]), schema]).unwrap();
            // New schema has original field and the json-parsed fields in order.
            debug_assert!(len + 1 == schema.fields().len());
        }

        for f in &fields {
            let dt = f.data_type();
            let name = f.metadata().get("query").unwrap_or(f.name());

            let path = serde_json_path::JsonPath::parse(&name).ok();
            let getter = |v| {
                path.as_ref()
                    .and_then(|path| path.query(v).first())
                    .or_else(|| v.get(name))
            };
            match dt {
                DataType::UInt8 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64().map(|v| v as u8)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt8Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::UInt16 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64().map(|v| v as u16)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt16Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::UInt32 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64().map(|v| v as u32)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt32Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::UInt64 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64()
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt64Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Int8 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64().map(|v| v as i8)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int8Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Int16 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64().map(|v| v as i16)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int16Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Int32 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64().map(|v| v as i32)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int32Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Int64 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64()
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int64Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Float32 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_f64().map(|f| f as f32)
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Float32Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Float64 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_f64()
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Float64Array::from_iter(values));

                    arrays.push((f.name(), array));
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_str()
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(StringArray::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Binary | DataType::LargeBinary => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_str().map(|s| s.as_bytes())
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(BinaryArray::from_iter(values));
                    arrays.push((f.name(), array));
                }
                _ => todo!(),
            }
        }

        let records = RecordBatch::try_from_iter(arrays).unwrap();

        let indices = if self.flatten {
            Some(json_values.iter().map(|(i, _)| *i).collect_vec())
        } else {
            None
        };
        Ok((records, indices))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::ArrayRef, datatypes::Field};

    use super::*;

    #[test]
    fn json_extract() {
        let extract = Json {
            // select: None,
            json: Some(serde_json::from_str(&r#"["a1=a::nchar(100)", "b1=b1::f32"]"#).unwrap()),
            flatten: true,
            keep: false,
        };
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"a1": "a1", "b1": 1}, {"a1": "a1", "b1": "none"}]"#,
            r#"{"a1": "a2", "c1": 1}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 2);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
    }
    #[test]
    fn json_de() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["a1=a::nchar(100)", "b1::f32"],
                "flatten": true
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"a1": "a1", "b1": 1}, {"a1": "a1", "b1": "none"}]"#,
            r#"{"a1": "a2", "c1": 1}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 2);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
    }

    #[test]
    fn json_de_err() {
        pretty_env_logger::init();
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["a1=a::nchar(100)", "b1::f32"],
                "flatten": true
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"a1": "a1", "b1": 1}, {"a1": "a1", "b1": "none", }]"#,
            r#"{"a1": "a2", "c1": 1}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 1);
        assert_eq!(indices, Some(vec![1]));
    }
}
