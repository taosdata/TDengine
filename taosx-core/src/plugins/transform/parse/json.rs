use std::{borrow::Cow, str::FromStr, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, ListBuilder,
        NullArray, StringArray, StringBuilder, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{
        DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
        TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    record_batch::RecordBatch,
};
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use super::{super::Select, Parse};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Json {
    pub(crate) json: Option<Select>,
    #[serde(default)]
    pub(crate) keep: bool,
    pub(crate) depth: Option<usize>,
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
    /// - `+[a,b]`: Extract selected fields from json object with keep enabled
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
    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        if array.len() == 0 {
            // Return empty record batch.
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }
        let mut flatten = false;

        let array = arrow::compute::cast(array, &DataType::Utf8)?;

        let string = array.as_any().downcast_ref::<StringArray>().unwrap();
        let num_rows = string.len();

        let mut json_data = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            if string.is_null(i) {
                continue;
            }
            let s = string.value(i);
            let value = serde_json::from_str::<serde_json::Value>(&s);
            let value = match value {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "{:#}",
                        super::ParseError::JsonDeserializeError(s.to_string(), e)
                    );
                    JsonValue::Null
                }
            };
            match value {
                JsonValue::Null => (),
                JsonValue::Object(object) => {
                    json_data.push(Ok(JsonValue::Object(object)));
                }
                JsonValue::Array(array) => {
                    flatten = true;
                    for value in array {
                        if value.is_null() {
                            continue;
                        } else if value.is_object() {
                            json_data.push(Ok(value));
                        } else {
                            return Err(super::ParseError::UnsupportedJsonValue(value));
                        }
                    }
                }
                v => {
                    return Err(super::ParseError::UnsupportedJsonValue(v));
                }
            }
        }
        if json_data.len() == 0 {
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }

        let mut schema =
            arrow::json::reader::infer_json_schema_from_iterator(json_data.into_iter())?;

        match self.json.as_ref() {
            Some(select) if select != &Select::pattern(Regex::new("").unwrap()) => {
                schema = select.schema(&schema);
            }
            _ => {
                if let Some(depth) = self.depth {
                    let keys = flat_fields(schema.fields(), &String::new(), depth);
                    let keys = serde_json::to_string(&keys).context("Fields to json string")?;
                    let select = Select::from_str(&keys).context("Json string to select")?;
                    schema = select.schema(&schema);
                }
            }
        }
        // dbg!(&schema);
        let json_values: Vec<_> = (0..num_rows)
            .enumerate()
            .flat_map(|(n, i)| {
                if string.is_null(i) {
                    return vec![(n, None)];
                }

                let str = string.value(i);
                let value = serde_json::from_str::<serde_json::Value>(&str);

                match value {
                    Ok(JsonValue::Array(array)) => array
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
                        .collect(),
                    Ok(JsonValue::Null) => {
                        vec![(n, None)]
                    }
                    Ok(JsonValue::Object(object)) => {
                        vec![(n, Some(JsonValue::Object(object)))]
                    }
                    Err(e) => {
                        tracing::warn!(
                            "{:#}",
                            super::ParseError::JsonDeserializeError(str.to_string(), e)
                        );
                        vec![(n, None)]
                    }
                    _ => unreachable!(),
                }
            })
            .collect();

        let mut arrays = Vec::new();

        let fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect_vec();
        // dbg!(&fields);
        if self.keep {
            arrays.push((field.name(), array.clone()));
            let len = schema.fields().len();
            schema = Schema::try_merge(vec![Schema::new(vec![field.clone()]), schema]).unwrap();
            // New schema has original field and the json-parsed fields in order.
            debug_assert!(len + 1 == schema.fields().len());
        }

        let mut r_fields = Vec::with_capacity(fields.len());
        let mut r_arrays = Vec::with_capacity(fields.len());
        for f in fields {
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
                                v.as_u64()
                                    .map(|v| v as u8)
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_i64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt8Array::from_iter(values));

                    // arrays.push((f.name(), array));
                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::UInt16 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64()
                                    .map(|v| v as u16)
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_i64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt16Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::UInt32 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64()
                                    .map(|v| v as u32)
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_i64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt32Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::UInt64 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_u64()
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_i64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt64Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int8 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64()
                                    .map(|v| v as i8)
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_u64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int8Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int16 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64()
                                    .map(|v| v as i16)
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_u64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int16Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int32 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64()
                                    .map(|v| v as i32)
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_u64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int32Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int64 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_i64()
                                    .or_else(|| v.as_f64().map(|v| v as _))
                                    .or_else(|| v.as_u64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int64Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Float32 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_f64()
                                    .map(|f| f as f32)
                                    .or_else(|| v.as_i64().map(|v| v as _))
                                    .or_else(|| v.as_u64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Float32Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Float64 => {
                    //
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_f64()
                                    .or_else(|| v.as_i64().map(|v| v as _))
                                    .or_else(|| v.as_u64().map(|v| v as _))
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Float64Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_str()
                                    .map(Cow::Borrowed)
                                    .or_else(|| serde_json::to_string(v).map(Cow::Owned).ok())
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(StringArray::from_iter(values));
                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Binary | DataType::LargeBinary => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_str()
                                    .map(|s| s.as_bytes())
                                    .map(Cow::Borrowed)
                                    .or_else(|| serde_json::to_vec(v).map(Cow::Owned).ok())
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(BinaryArray::from_iter(values));
                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Timestamp(time_unit, _) => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                if v.is_string() {
                                    v.as_str().and_then(|s| {
                                        chrono::DateTime::parse_from_rfc3339(s).ok().map(|dt| {
                                            match time_unit {
                                                TimeUnit::Second => dt.timestamp(),
                                                TimeUnit::Millisecond => dt.timestamp_millis(),
                                                TimeUnit::Microsecond => dt.timestamp_micros(),
                                                TimeUnit::Nanosecond => {
                                                    dt.timestamp_nanos_opt().unwrap_or(i64::MAX)
                                                }
                                            }
                                        })
                                    })
                                } else {
                                    v.as_i64()
                                }
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = match time_unit {
                        TimeUnit::Second => todo!(),
                        TimeUnit::Millisecond => {
                            Arc::new(TimestampMillisecondArray::from_iter(values))
                        }
                        TimeUnit::Microsecond => {
                            Arc::new(TimestampMicrosecondArray::from_iter(values))
                        }
                        TimeUnit::Nanosecond => {
                            Arc::new(TimestampNanosecondArray::from_iter(values))
                        }
                    };
                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Boolean => {
                    let values = json_values
                        .iter()
                        .map(|(_n, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                v.as_bool()
                                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(BooleanArray::from_iter(values));
                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::List(field) => {
                    let array = match field.data_type() {
                        DataType::UInt8 => ListArray::from_iter_primitive::<UInt8Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_u64()
                                                .map(|v| v as u8)
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_i64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::UInt16 => ListArray::from_iter_primitive::<UInt16Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_u64()
                                                .map(|v| v as u16)
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_i64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::UInt32 => ListArray::from_iter_primitive::<UInt32Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_u64()
                                                .map(|v| v as u32)
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_i64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::UInt64 => ListArray::from_iter_primitive::<UInt64Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_u64()
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_i64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Int8 => ListArray::from_iter_primitive::<Int8Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_i64()
                                                .map(|v| v as i8)
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_u64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Int16 => ListArray::from_iter_primitive::<Int16Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_i64()
                                                .map(|v| v as i16)
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_u64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Int32 => ListArray::from_iter_primitive::<Int32Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_i64()
                                                .map(|v| v as i32)
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_u64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Int64 => ListArray::from_iter_primitive::<Int64Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_i64()
                                                .or_else(|| v.as_f64().map(|v| v as _))
                                                .or_else(|| v.as_u64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Binary | DataType::LargeBinary => {
                            let mut array =
                                ListBuilder::with_capacity(BinaryBuilder::new(), json_values.len());
                            array.extend(json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_str()
                                                .map(|s| s.as_bytes())
                                                .map(Cow::Borrowed)
                                                .or_else(|| {
                                                    serde_json::to_vec(v).map(Cow::Owned).ok()
                                                })
                                        })
                                    })
                                })
                            }));
                            array.finish()
                        }
                        DataType::Float32 => ListArray::from_iter_primitive::<Float32Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_f64()
                                                .map(|f| f as f32)
                                                .or_else(|| v.as_i64().map(|v| v as _))
                                                .or_else(|| v.as_u64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Float64 => ListArray::from_iter_primitive::<Float64Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_f64()
                                                .or_else(|| v.as_i64().map(|v| v as _))
                                                .or_else(|| v.as_u64().map(|v| v as _))
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }),
                        ),
                        DataType::Boolean => {
                            let mut array = ListBuilder::with_capacity(
                                BooleanBuilder::new(),
                                json_values.len(),
                            );
                            array.extend(json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_bool()
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    })
                                })
                            }));
                            array.finish()
                        }
                        _ => {
                            let field = Field::new_list(
                                f.name(),
                                Field::new_list_field(DataType::Utf8, true),
                                f.is_nullable(),
                            );
                            // utf8 type and other types...
                            let mut array =
                                ListBuilder::with_capacity(StringBuilder::new(), json_values.len());
                            array.extend(json_values.iter().map(|(_, v)| {
                                v.as_ref().and_then(getter).and_then(|v| {
                                    v.as_array().map(|a| {
                                        a.into_iter().map(|v| {
                                            v.as_str().map(Cow::Borrowed).or_else(|| {
                                                serde_json::to_string(v).map(Cow::Owned).ok()
                                            })
                                        })
                                    })
                                })
                            }));
                            r_fields.push(field);
                            r_arrays.push(Arc::new(array.finish()) as ArrayRef);
                            continue;
                        }
                    };
                    r_fields.push(f);
                    r_arrays.push(Arc::new(array) as ArrayRef)
                }
                DataType::Struct(_) => {
                    let values = json_values
                        .iter()
                        .map(|(_n, v)| {
                            if let Some(v) = v.as_ref().and_then(getter) {
                                serde_json::to_string(v).ok()
                            } else {
                                None
                            }
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(StringArray::from_iter(values));
                    // set field type to Utf8
                    let field = Field::new(f.name(), DataType::Utf8, true);
                    r_fields.push(field);
                    r_arrays.push(array);
                }
                DataType::Null => {
                    let values = json_values
                        .iter()
                        .map(|(_n, v)| {
                            if let Some(value) = v.as_ref().and_then(getter) {
                                if value.is_null() {
                                    None
                                } else {
                                    Some(value)
                                }
                            } else {
                                None
                            }
                        })
                        .collect_vec();

                    if let Some(v) = values.iter().flatten().next() {
                        match v {
                            JsonValue::Null => unreachable!("null value should be handled above"),
                            JsonValue::Bool(_) => {
                                let array: ArrayRef = Arc::new(BooleanArray::from_iter(
                                    values.into_iter().map(|v| {
                                        v.and_then(|v| {
                                            v.as_bool()
                                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                        })
                                    }),
                                ));
                                let f = f.with_data_type(DataType::Boolean);
                                r_fields.push(f);
                                r_arrays.push(array);
                            }
                            JsonValue::Number(number) => {
                                if number.is_f64() {
                                    let array: ArrayRef = Arc::new(Float64Array::from_iter(
                                        values.into_iter().map(|v| {
                                            v.and_then(|v| {
                                                v.as_f64().or_else(|| {
                                                    v.as_str().and_then(|s| s.parse().ok())
                                                })
                                            })
                                        }),
                                    ));
                                    let f = f.with_data_type(DataType::Float64);
                                    r_fields.push(f);
                                    r_arrays.push(array);
                                } else if number.is_i64() {
                                    let array: ArrayRef = Arc::new(Int64Array::from_iter(
                                        values.into_iter().map(|v| {
                                            v.and_then(|v| {
                                                v.as_i64().or_else(|| {
                                                    v.as_str().and_then(|s| s.parse().ok())
                                                })
                                            })
                                        }),
                                    ));
                                    let f = f.with_data_type(DataType::Int64);
                                    r_fields.push(f);
                                    r_arrays.push(array);
                                } else if number.is_u64() {
                                    let array: ArrayRef = Arc::new(UInt64Array::from_iter(
                                        values.into_iter().map(|v| {
                                            v.and_then(|v| {
                                                v.as_u64().or_else(|| {
                                                    v.as_str().and_then(|s| s.parse().ok())
                                                })
                                            })
                                        }),
                                    ));
                                    let f = f.with_data_type(DataType::UInt64);
                                    r_fields.push(f);
                                    r_arrays.push(array);
                                } else {
                                    unreachable!("number should be f64, i64 or u64")
                                }
                            }
                            JsonValue::String(_) => {
                                let array: ArrayRef = Arc::new(StringArray::from_iter(
                                    values.into_iter().map(|v| v.and_then(|v| v.as_str())),
                                ));
                                let f = f.with_data_type(DataType::Utf8);
                                r_fields.push(f);
                                r_arrays.push(array);
                            }
                            JsonValue::Array(_) => {
                                let array: ArrayRef = Arc::new(StringArray::from_iter(
                                    values
                                        .into_iter()
                                        .map(|v| v.and_then(|v| serde_json::to_string(v).ok())),
                                ));
                                let f = f.with_data_type(DataType::Utf8);
                                r_fields.push(f);
                                r_arrays.push(array);
                            }
                            JsonValue::Object(_) => {
                                let array: ArrayRef = Arc::new(StringArray::from_iter(
                                    values
                                        .into_iter()
                                        .map(|v| v.and_then(|v| serde_json::to_string(v).ok())),
                                ));
                                let f = f.with_data_type(DataType::Utf8);
                                r_fields.push(f);
                                r_arrays.push(array);
                            }
                        }
                    } else {
                        r_fields.push(f);
                        r_arrays.push(Arc::new(NullArray::new(values.len())));
                    }
                }
                _ => {
                    return Err(super::ParseError::UnsupportedDataType(dt.clone()));
                }
            }
        }

        schema.fields = Fields::from(r_fields);
        let records = RecordBatch::try_new(Arc::new(schema), r_arrays)?;
        // let records = records.with_schema(Arc::new(schema)).unwrap();
        let indices = if flatten {
            Some(json_values.iter().map(|(i, _)| *i).collect_vec())
        } else {
            None
        };
        Ok((records, indices))
    }
}

fn flat_fields(fields: &Fields, current: &String, depth: usize) -> Vec<String> {
    let keys = &mut Vec::new();
    fields.iter().for_each(|field| {
        let field_name = field.name();
        let field_type = field.data_type();
        // renew current field name
        let current = if current.is_empty() {
            field_name.clone()
        } else {
            format!("{}.{}", current, field_name)
        };
        // when depth > 0, we need to flat the nested fields
        if depth > 0 {
            match field_type {
                DataType::Struct(fields) => {
                    keys.append(flat_fields(fields, &current, depth - 1).as_mut());
                }
                _ => {
                    keys.push(format!(
                        "$.{}={}",
                        current.clone(),
                        current.clone().replace(".", "_")
                    ));
                }
            }
        } else {
            keys.push(format!(
                "$.{}={}",
                current.clone(),
                current.clone().replace(".", "_")
            ));
        }
    });
    keys.clone()
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    fn parse_json(json_str: &str) -> Result<serde_json::Value, serde_json::Error> {
        let json = serde_json::from_str::<serde_json::Value>(&json_str);
        json
    }

    fn build_schema_by_json(json: serde_json::Value) -> Result<Schema, arrow::error::ArrowError> {
        let mut json_data = Vec::with_capacity(1);
        match json {
            JsonValue::Object(object) => {
                json_data.push(Ok(JsonValue::Object(object)));
            }
            _ => unreachable!(),
        }
        let schema = arrow::json::reader::infer_json_schema_from_iterator(json_data.into_iter());
        schema
    }

    #[test]
    fn test_parse_json() {
        let json_str =
            r#"{"a":1,"b":"2","c":[3,4],"d":{"d1":1,"d2":{"d21":1,"d22":{"d221":1,"d222":2}}}}"#;
        let json = parse_json(json_str).unwrap();
        dbg!(&json);
    }

    #[test]
    fn test_build_schema_by_json() {
        let json_str =
            r#"{"a":1,"b":"2","c":[3,4],"d":{"d1":1,"d2":{"d21":1,"d22":{"d221":1,"d222":2}}}}"#;
        let json = parse_json(json_str).unwrap();
        let schema = build_schema_by_json(json).unwrap();
        dbg!(&schema);
        dbg!(&schema.fields());
    }

    #[test]
    fn test_flat_fields_by_depth() {
        let depth = 2;
        let json_str =
            r#"{"a":1,"b":"2","c":[3,4],"d":{"d1":1,"d2":{"d21":1,"d22":{"d221":1,"d222":2}}}}"#;
        let json = parse_json(json_str).unwrap();
        let schema = build_schema_by_json(json).unwrap();
        let keys = flat_fields(schema.fields(), &String::new(), depth);
        dbg!(keys);
        // println!("{}", serde_json::to_string(&keys).unwrap());
    }

    #[test]
    fn json_extract() {
        let extract = Json {
            // select: None,
            json: Some(serde_json::from_str(&r#"["a1=a::nchar(100)", "b1=b1::int"]"#).unwrap()),
            keep: false,
            depth: Some(0),
        };
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"a1": "a1", "b1": 1.2, "c1": true, "d1": 18392, "e1": null}, {"a1": "a1", "b1": "none", "e1": 1}]"#,
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
    fn json_extract_object() {
        let extract = Json::from_str("").unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a1": "a1", "b1": 1.2}"#,
            r#"{"a1": "a2", "b1": 2.5, "e1": 1}"#,
            r#"{"a1": "a3", "c1": 1}"#,
        ]));

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 4);
        assert_eq!(records.num_rows(), 3);
        assert!(indices.is_none());
    }

    #[test]
    fn json_extract_object_by_depth() {
        let extract = Json {
            // select: None,
            json: None,
            keep: false,
            depth: Some(2),
        };

        let field = Field::new("a1", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a1": "a1", "b1": 1.2}"#,
            r#"{"a1": "a2", "b1": 2.5, "e1": 1}"#,
            r#"{"a1": "a3", "c1": 1}"#,
            r#"{"a":1,"b":"2","c":[3,4],"d":{"d1":1,"d2":{"d21":1,"d22":{"d221":1,"d222":2}}}}"#,
        ]));

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 10);
        assert_eq!(records.num_rows(), 4);
        assert!(indices.is_none());
    }

    #[test]
    fn json_nested() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["$.nested.a1=a1::nchar(100)", "$.nested.b1=b1::f32", "$.nested.d1=d1::bool"]
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"nested":{"a1": "a1", "b1": 1}}, {"nested":{"a1": "a1", "b1": "none"}}]"#,
            r#"{"nested": {"a1": "a2", "c1": 1, "d1": true}}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 3);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
        let strings = records
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(strings, vec![Some("a1"), Some("a1"), Some("a2")]);
        let floats = records
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(floats, vec![Some(1.0f32), None, None]);
        let booleans = records
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(booleans, vec![None, None, Some(true)]);
    }

    #[test]
    fn json_nested_array_index_without_type() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["$.nested.a1[0]=a1", "$.nested.b1[1]=b1::i32", "$.nested.d1[0]=d1"]
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"nested":{"a1": ["a1"], "b1": [1,2]}}, {"nested":{"a1": ["a1"], "b1": ["none"]}}]"#,
            r#"{"nested": {"a1": ["a2"], "c1": 1, "d1": [true]}}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 3);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
        let strings = records
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(strings, vec![Some("a1"), Some("a1"), Some("a2")]);
        let ints = records
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(ints, vec![Some(2), None, None]);
        let booleans = records
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(booleans, vec![None, None, Some(true)]);
    }
    #[test]
    fn json_nested_array_index() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["$.nested.a1[0]=a1::nchar(100)", "$.nested.b1[1]=b1::f32", "$.nested.d1[0]=d1::bool"]
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"nested":{"a1": ["a1"], "b1": [1,2]}}, {"nested":{"a1": ["a1"], "b1": ["none"]}}]"#,
            r#"{"nested": {"a1": ["a2"], "c1": 1, "d1": [true]}}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 3);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
        let strings = records
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(strings, vec![Some("a1"), Some("a1"), Some("a2")]);
        let floats = records
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(floats, vec![Some(2.0f32), None, None]);
        let booleans = records
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .into_iter()
            .collect_vec();
        assert_eq!(booleans, vec![None, None, Some(true)]);
    }
    #[test]
    fn json_de() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["a1=a::nchar(100)", "b1::f32", "d1::bool"]
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"a1": "a1", "b1": 1}, {"a1": "a1", "b1": "none"}]"#,
            r#"{"a1": "a2", "c1": 1, "d1": true}"#,
        ]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.num_columns(), 3);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
    }

    #[test]
    fn json_de_contains_array() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ""
            }"#,
        )
        .unwrap();
        dbg!(&extract);

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"[{"a1": "a1", "b1": [1,2,3]}, {"a1": "a1", "b1": ["1","2","3"]}, {"a1": "a1", "b1": [true, false, true]}]"#,
            r#"[{"a1": "a2", "b1": []}, {"a1": "a2", "b1": null}]"#,
        ]));

        let (records, indices) = extract.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
    }

    #[test]
    fn json_de_err() {
        let extract: Json = serde_json::from_str(
            r#"{
                "json": ["a1=a::nchar(100)", "b1::f32"]
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

        let v = extract.parse_array(&field, &array);
        assert!(!v.is_err());

        let (records, indices) = v.unwrap();
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
    }
}
