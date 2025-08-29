use std::{
    borrow::Cow,
    collections::HashSet,
    sync::{Arc, LazyLock},
};

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

use crate::plugins::transform::parse::duplicate_rows;

use super::{super::Select, Parse};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Json {
    pub(crate) json: Select,
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

impl Parse for Json {
    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        if array.is_empty() {
            // Return empty record batch.
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }
        let mut flatten = false;

        let utf8_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let field = field
            .clone()
            .with_data_type(DataType::Utf8)
            .with_nullable(true);

        let string_array = utf8_array.as_any().downcast_ref::<StringArray>().unwrap();
        let num_rows = string_array.len();

        let parse_value = |value: JsonValue| -> Option<JsonValue> {
            match (&self.json, &self.depth) {
                (Select::All, Some(depth)) => flat_depth(value, *depth),
                (select, _) => select.parse_json(field.name(), value),
            }
        };

        let mut json_values = Vec::with_capacity(num_rows);
        let mut drain_idx = (0..num_rows).collect::<HashSet<_>>();
        for i in 0..num_rows {
            if string_array.is_null(i) {
                continue;
            }
            drain_idx.remove(&i);
            let s = string_array.value(i);
            let value = serde_json::from_str::<JsonValue>(s);
            let value = match value {
                Ok(v) => v,
                Err(e) if e.is_syntax() => {
                    let s = fix_json_control_chars(s);
                    match serde_json::from_str::<JsonValue>(&s) {
                        Ok(value) => value,
                        Err(e) => {
                            tracing::warn!(
                                "{:#}",
                                super::ParseError::JsonDeserializeError(s.to_string(), e)
                            );
                            JsonValue::Null
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Parse json row {i} error: {:#}",
                        super::ParseError::JsonDeserializeError(s.to_string(), e)
                    );
                    JsonValue::Null
                }
            };
            match value {
                JsonValue::Null => (),
                value @ JsonValue::Object(_) => {
                    if let Some(value) = parse_value(value) {
                        json_values.push((i, value));
                    }
                }
                JsonValue::Array(array) => {
                    flatten = true;
                    for value in array {
                        if value.is_null() {
                            continue;
                        } else if value.is_object() {
                            if let Some(value) = parse_value(value) {
                                json_values.push((i, value));
                            }
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
        if json_values.is_empty() {
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }

        let mut schema = arrow::json::reader::infer_json_schema_from_iterator(
            json_values.iter().map(|(_, v)| Ok(v)),
        )?;

        let fields = self.json.rebuild_fields_type(schema.fields());

        let mut r_fields = Vec::with_capacity(fields.len());
        let mut r_arrays = Vec::with_capacity(fields.len());
        for f in fields.iter().map(|f| f.as_ref().clone()) {
            let name = f.name();
            match f.data_type() {
                DataType::UInt8 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_u64()
                                        .map(|v| v as u8)
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_i64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt8Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::UInt16 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_u64()
                                        .map(|v| v as u16)
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_i64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt16Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::UInt32 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_u64()
                                        .map(|v| v as u32)
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_i64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt32Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::UInt64 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_u64()
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_i64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(UInt64Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int8 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_i64()
                                        .map(|v| v as i8)
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_u64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int8Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int16 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_i64()
                                        .map(|v| v as i16)
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_u64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int16Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int32 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_i64()
                                        .map(|v| v as i32)
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_u64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int32Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Int64 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_i64()
                                        .or_else(|| v.as_f64().map(|v| v as _))
                                        .or_else(|| v.as_u64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Int64Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Float32 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_f64()
                                        .map(|f| f as f32)
                                        .or_else(|| v.as_i64().map(|v| v as _))
                                        .or_else(|| v.as_u64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                        })
                        .collect_vec();
                    let array: ArrayRef = Arc::new(Float32Array::from_iter(values));

                    r_fields.push(f);
                    r_arrays.push(array);
                }
                DataType::Float64 => {
                    let values = json_values
                        .iter()
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_f64()
                                        .or_else(|| v.as_i64().map(|v| v as _))
                                        .or_else(|| v.as_u64().map(|v| v as _))
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
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
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_str()
                                        .map(Cow::Borrowed)
                                        .or_else(|| serde_json::to_string(v).map(Cow::Owned).ok())
                                })
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
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_str()
                                        .map(|s| s.as_bytes())
                                        .map(Cow::Borrowed)
                                        .or_else(|| serde_json::to_vec(v).map(Cow::Owned).ok())
                                })
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
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
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
                                })
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
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| {
                                    v.as_bool()
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
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
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_u64()
                                                    .map(|v| v as u8)
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_i64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::UInt16 => ListArray::from_iter_primitive::<UInt16Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_u64()
                                                    .map(|v| v as u16)
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_i64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::UInt32 => ListArray::from_iter_primitive::<UInt32Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_u64()
                                                    .map(|v| v as u32)
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_i64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::UInt64 => ListArray::from_iter_primitive::<UInt64Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_u64()
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_i64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::Int8 => ListArray::from_iter_primitive::<Int8Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_i64()
                                                    .map(|v| v as i8)
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_u64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::Int16 => ListArray::from_iter_primitive::<Int16Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_i64()
                                                    .map(|v| v as i16)
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_u64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::Int32 => ListArray::from_iter_primitive::<Int32Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_i64()
                                                    .map(|v| v as i32)
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_u64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::Int64 => ListArray::from_iter_primitive::<Int64Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_i64()
                                                    .or_else(|| v.as_f64().map(|v| v as _))
                                                    .or_else(|| v.as_u64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::Binary | DataType::LargeBinary => {
                            let mut array =
                                ListBuilder::with_capacity(BinaryBuilder::new(), json_values.len());
                            array.extend(json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
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
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_f64()
                                                    .map(|f| f as f32)
                                                    .or_else(|| v.as_i64().map(|v| v as _))
                                                    .or_else(|| v.as_u64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
                                            })
                                        })
                                    })
                            }),
                        ),
                        DataType::Float64 => ListArray::from_iter_primitive::<Float64Type, _, _>(
                            json_values.iter().map(|(_, v)| {
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_f64()
                                                    .or_else(|| v.as_i64().map(|v| v as _))
                                                    .or_else(|| v.as_u64().map(|v| v as _))
                                                    .or_else(|| {
                                                        v.as_str().and_then(|s| s.parse().ok())
                                                    })
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
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
                                                v.as_bool().or_else(|| {
                                                    v.as_str().and_then(|s| s.parse().ok())
                                                })
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
                                v.as_object()
                                    .and_then(|v| v.get(name))
                                    .filter(|v| !v.is_null())
                                    .and_then(|v| {
                                        v.as_array().map(|a| {
                                            a.iter().map(|v| {
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
                        .map(|(_, v)| {
                            v.as_object()
                                .and_then(|v| v.get(name))
                                .filter(|v| !v.is_null())
                                .and_then(|v| serde_json::to_string(v).ok())
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
                        .map(|(_, v)| v.as_object().and_then(|v| v.get(name)))
                        .collect_vec();

                    if let Some(v) = values.iter().flatten().next() {
                        match v {
                            JsonValue::Null => {
                                let array: ArrayRef = Arc::new(NullArray::new(values.len()));
                                let f = f.with_data_type(DataType::Null);
                                r_fields.push(f);
                                r_arrays.push(array);
                            }
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
                                if number.as_f64().is_some() {
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
                dt => {
                    return Err(super::ParseError::UnsupportedDataType(dt.clone()));
                }
            }
        }

        let indices = if flatten || !drain_idx.is_empty() {
            Some(json_values.iter().map(|(i, _)| *i).collect_vec())
        } else {
            None
        };

        if self.keep {
            if drain_idx.is_empty() {
                r_arrays.push(utf8_array.clone());
                r_fields.push(field);
            } else if indices.is_some() {
                let new_array = duplicate_rows(&utf8_array, indices.as_ref().unwrap());
                r_arrays.push(new_array);
                r_fields.push(field);
            }

            if !drain_idx.is_empty() {
                log_illegal_array(array, drain_idx.into_iter().map(|x| x as u64).collect())?;
            }
        }

        schema.fields = Fields::from(r_fields);
        let records = RecordBatch::try_new(Arc::new(schema), r_arrays)?;
        Ok((records, indices))
    }
}

pub fn flat_depth(value: JsonValue, depth: usize) -> Option<JsonValue> {
    let mut out = serde_json::Map::new();
    flat_depth_inner(&value, Vec::new(), depth + 1, &mut out);
    if out.is_empty() {
        None
    } else {
        Some(JsonValue::Object(out))
    }
}

fn flat_depth_inner(
    value: &JsonValue,
    path: Vec<String>,
    cur_depth: usize,
    out: &mut serde_json::Map<String, JsonValue>,
) {
    match value {
        JsonValue::Object(map) if cur_depth > 0 => {
            for (k, v) in map {
                let mut new_path = path.clone();
                new_path.push(k.clone());
                flat_depth_inner(v, new_path, cur_depth - 1, out);
            }
        }
        _ => {
            let key = if path.len() == 1 {
                // 根节点，直接用原始 key
                path[0].clone()
            } else {
                // 嵌套节点，拼接 key
                path.join("_")
            };
            out.insert(key, value.clone());
        }
    }
}

fn fix_json_control_chars(json_str: &str) -> String {
    static RE: LazyLock<regex::Regex> =
        LazyLock::new(|| Regex::new(r#""((?:\\.|[^"\\])*)""#).unwrap());

    let result = RE.replace_all(json_str, |caps: &regex::Captures| {
        let content = &caps[1]; // 引号内的内容

        // 只替换内容中的换行符，保持转义序列不变
        let fixed_content = content
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t");

        format!("\"{fixed_content}\"")
    });

    result.into_owned()
}

fn log_illegal_array(array: &ArrayRef, remove_idxs: Vec<u64>) -> anyhow::Result<()> {
    let idx_array = UInt64Array::from(remove_idxs);
    let removed_array = arrow::compute::take(array.as_ref(), &idx_array, None)?;
    let binary_array = arrow::compute::cast(removed_array.as_ref(), &DataType::Binary)?;
    let binary_array = binary_array.as_any().downcast_ref::<BinaryArray>().unwrap();
    for i in 0..binary_array.len() {
        if binary_array.is_null(i) {
            continue;
        }
        let illegal_row = String::from_utf8_lossy(binary_array.value(i));
        tracing::warn!("Json parse drop illegal data: {:?}", illegal_row);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampNanosecondBuilder;
    use arrow_schema::Field;
    use chrono::Local;
    use serde_json::json;

    use super::*;

    fn parse_json(json_str: &str) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str::<serde_json::Value>(json_str)
    }

    fn build_schema_by_json(json: serde_json::Value) -> Result<Schema, arrow::error::ArrowError> {
        let mut json_data = Vec::with_capacity(1);
        match json {
            JsonValue::Object(object) => {
                json_data.push(Ok(JsonValue::Object(object)));
            }
            _ => unreachable!(),
        }
        arrow::json::reader::infer_json_schema_from_iterator(json_data.into_iter())
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
    fn json_extract() {
        let extract = Json {
            // select: None,
            json: serde_json::from_str(r#"["a1=a::nchar(100)", "b1=b1::int"]"#).unwrap(),
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
        let extract = Json {
            json: Select::All,
            keep: false,
            depth: None,
        };
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
            json: Select::All,
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
        assert!(v.is_ok());

        let (records, indices) = v.unwrap();
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indices, Some(vec![0, 0, 1]));
    }

    #[test]
    fn json_parse_newline_test() -> anyhow::Result<()> {
        let extract = Json {
            json: Select::All,
            keep: false,
            depth: None,
        };

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![r#"{"a": "b\nc"}"#]));
        let (batch, _) = extract.parse_array(&field, &array)?;
        dbg!(batch);
        Ok(())
    }

    #[test]
    fn parse_array_null_test() -> anyhow::Result<()> {
        let json: Json = serde_json::from_value(serde_json::json!({"json": ""})).unwrap();
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"ts": 100, "value": 0.1}"#,
            r#"{"ts": 100, "value": null}"#,
        ]));
        let (batch, _) = json.parse_array(&Field::new("payload", DataType::Utf8, false), &array)?;
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&[batch])?.to_string(),
            "\
+-----+-------+
| ts  | value |
+-----+-------+
| 100 | 0.1   |
| 100 |       |
+-----+-------+"
        );

        let json: Json = serde_json::from_value(serde_json::json!({"json": ""})).unwrap();
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"ts": 100, "value": "0.1"}"#,
            r#"{"ts": 100, "value": null}"#,
        ]));
        let (batch, _) = json.parse_array(&Field::new("payload", DataType::Utf8, false), &array)?;
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&[batch.clone()])?.to_string(),
            "\
+-----+-------+
| ts  | value |
+-----+-------+
| 100 | 0.1   |
| 100 |       |
+-----+-------+"
        );

        assert!(batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .is_null(1));
        Ok(())
    }

    #[test]
    fn fix_json_control_chars_test() {
        let s = fix_json_control_chars("{\"a\": \n \"b\\\"\nc\"}");
        assert!(serde_json::from_str::<serde_json::Value>(&s).is_ok());

        let s = fix_json_control_chars("{\"a\": \n \"bc\"}");
        assert!(serde_json::from_str::<serde_json::Value>(&s).is_ok());

        let s = fix_json_control_chars("{\"a\": \n \"b\nc\"}");
        assert!(serde_json::from_str::<serde_json::Value>(&s).is_ok());

        let s = fix_json_control_chars("{\"a\": \"b\tc\"}");
        assert!(serde_json::from_str::<serde_json::Value>(&s).is_ok());
    }

    #[test]
    fn parse_json_array_test() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();

        let json = r#"{
        "json": [
            "$[\"dataType\"]=dataType",
            "$[\"dataTime\"]=dataTime",
            "$[\"saveTime\"]=saveTime",
            "$[\"vin\"]=vin",
            "$[\"payload\"][\"speed\"]=speed",
            "$[\"payload\"][\"atmoPres\"]=atmoPres",
            "$[\"payload\"][\"outPutWrest\"]=outPutWrest",
            "$[\"payload\"][\"rubWrest\"]=rubWrest",
            "$[\"payload\"][\"rev\"]=rev",
            "$[\"payload\"][\"fuelVelocityFlow\"]=fuelVelocityFlow",
            "$[\"payload\"][\"upperSCRNOxOutPut\"]=upperSCRNOxOutPut",
            "$[\"payload\"][\"downSCRNOxOutPut\"]=downSCRNOxOutPut",
            "$[\"payload\"][\"percentReactant\"]=percentReactant",
            "$[\"payload\"][\"airInput\"]=airInput",
            "$[\"payload\"][\"temperSCRInput\"]=temperSCRInput",
            "$[\"payload\"][\"temperSCROutput\"]=temperSCROutput",
            "$[\"payload\"][\"DPFDifferentPress\"]=DPFDifferentPress",
            "$[\"payload\"][\"temperCoolant\"]=temperCoolant",
            "$[\"payload\"][\"percentOil\"]=percentOil",
            "$[\"payload\"][\"fixState\"]=fixState",
            "$[\"payload\"][\"longitude\"]=longitude",
            "$[\"payload\"][\"latitude\"]=latitude",
            "$[\"payload\"][\"mileage\"]=mileage"
        ],
        "depth": 2,
        "keep": true
        }"#;
        let json_parser: Json = serde_json::from_str(json).unwrap();
        // dbg!(&json_parser);

        let flat_columns = vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("key", DataType::Binary, true),
            Field::new("value", DataType::Binary, false),
        ];
        let schema = Arc::new(Schema::new(flat_columns));

        let mut timestamp = TimestampNanosecondBuilder::new();
        let mut key = BinaryBuilder::new();
        let mut value = BinaryBuilder::new();

        let gbk_bytes: &[u8] = &[
            0xD4, 0xDA, 0xB1, 0xB1, 0xBE, 0xA9, 0xD1, 0xA7, 0xCF, 0xB0, 0x72, 0x75, 0x73, 0x74,
            0xB1, 0xE0, 0xB3, 0xCC,
        ];

        timestamp.append_value(Local::now().timestamp_nanos_opt().unwrap());
        key.append_value(b"key");
        value.append_value(br#"{"dataType":"DATA_CVI_OBD","dataTime":"2025-06-27 10:49:48","saveTime":"2025-06-27 10:55:34","vin":"YS2G6X237M5622467","payload":{"detectProtocol":2,"milState":2,"detectState":2,"detectReadyState":2,"identifyCode":"LBZWANGXUD0NG0826","idVersion":"200000000000000002","calibrateVerify":"300000000000000003","IUPR":"000000000000000000000000000000000000","errorCodeCount":3,"errorCodes":["0118002A","0118002A","0118002A"]}}"#);

        timestamp.append_value(Local::now().timestamp_nanos_opt().unwrap());
        key.append_value(b"key");
        value.append_value(gbk_bytes);
        // value.append_value(b"");

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(timestamp.finish()),
                Arc::new(key.finish()),
                Arc::new(value.finish()),
            ],
        )?;

        let (_, field) = schema.fields().find("value").unwrap();
        let array = batch.column_by_name("value").unwrap();

        let (batch, _) = json_parser
            .parse_array(field, array)
            .map_err(|error| {
                panic!("json parser parse_array error: {:?}", error);
            })
            .unwrap();

        dbg!(&batch);
        // println!(
        //     "\nfinal batch: {:?}",
        //     arrow::util::pretty::pretty_format_batches(&[batch])?.to_string()
        // );
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn log_illegal_array_test() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::WARN)
            .init();

        let batch = arrow::array::record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Utf8, ["123", "456", "789"]),
            ("c", Binary, [b"111", b"222", b"333"])
        )
        .unwrap();
        let remove_idxs = (0..batch.num_rows() as u64)
            .filter(|x| x % 2 == 0)
            .collect::<Vec<_>>();

        let array = batch.column_by_name("a").unwrap();
        log_illegal_array(array, remove_idxs.clone())?;

        let array = batch.column_by_name("b").unwrap();
        log_illegal_array(array, remove_idxs.clone())?;

        let array = batch.column_by_name("c").unwrap();
        log_illegal_array(array, remove_idxs)?;
        Ok(())
    }

    #[test]
    fn json_path_test() {
        let parser: Json =
            serde_json::from_value(json!({"json": ["$['a']=a::double", "$['b']=b"]})).unwrap();
        let field = Field::new("payload", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a": 123, "b": 123}"#,
            r#"{"a": 123, "b": 123.4}"#,
        ]));
        let (batch, _) = parser.parse_array(&field, &array).unwrap();
        let schema = batch.schema();
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &DataType::Float64
        );

        let parser: Json =
            serde_json::from_value(json!({"json": ["$['a']=a::double", "$['b']=b"]})).unwrap();
        let field = Field::new("payload", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![r#"{"b": 123}"#, r#"{"b": 123.4}"#]));
        let (batch, _) = parser.parse_array(&field, &array).unwrap();
        let schema = batch.schema();
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &DataType::Float64
        );

        let parser: Json =
            serde_json::from_value(json!({"json": ["$['a']=a::double", "$['b']=b"]})).unwrap();
        let field = Field::new("payload", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"b": 123, "c": 234}"#,
            r#"{"b": 123.4, "c": 567}"#,
        ]));
        let (batch, _) = parser.parse_array(&field, &array).unwrap();
        let schema = batch.schema();
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &DataType::Float64
        );
        assert!(schema.field_with_name("c").is_err());

        let parser: Json =
            serde_json::from_value(json!({"json": ["$['a']=a::double", "b"]})).unwrap();
        let field = Field::new("payload", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a": 123, "b": 123}"#,
            r#"{"a": "123", "b": "123.4"}"#,
        ]));
        let (batch, _) = parser.parse_array(&field, &array).unwrap();
        let schema = batch.schema();
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &DataType::Utf8
        );

        let parser: Json =
            serde_json::from_value(json!({"json": ["$['c']=a::double", "d"]})).unwrap();
        let field = Field::new("payload", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a": 123, "b": 123}"#,
            r#"{"a": "123", "b": "123.4"}"#,
        ]));
        let (batch, _) = parser.parse_array(&field, &array).unwrap();
        let schema = batch.schema();
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(batch.column_by_name("a").unwrap().null_count(), 2);
        assert_eq!(
            schema.field_with_name("d").unwrap().data_type(),
            &DataType::Null
        );
    }

    #[test]
    fn build_fields_test() {
        let json = json!({
            "ts": 1725518750053i64,
            "id": 500,
            "current": "10.77",
            "phase": "0.77",
            "voltage": "220",
            "groupid": 1,
            "location": "California.SanDiego",
            "column1": 9223372036854775807u64,
            "column2": 18446744073709551615u64,
            "column3": 32767,
            "column4": 65535,
            "column5": 127,
            "data": {
                "column6": 255,
                "column7": "true",
                "column8": "MqttTest"
            },
            "column9": "true",
            "events": [
                {
                    "price": "28405.2",
                    "delta": "6",
                    "boolValue": "true"
                },
                {
                    "price": "38405.2",
                    "delta": "7",
                    "boolValue": "false"
                }
            ]
        });

        let parser: Json =
            serde_json::from_value(json!({"json": ["$.ts","$.id","$.current","$.phase","$.voltage","$.groupid","$.location","$.column1","$.column2=column2::BIGINT UNSIGNED","$.column3","$.column4","$.column5","$.data.column6","$.data.column7=column7","$.data.column8=column8","$.column9","$.events[1].price=price","$.events[0].delta=delta","$.events[1].boolValue=boolValue"]})).unwrap();
        let field = Field::new("payload", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![json.to_string()]));
        let (batch, _) = parser.parse_array(&field, &array).unwrap();
        let a = arrow::util::pretty::pretty_format_batches(&[batch]).unwrap();
        assert_eq!(a.to_string(), "\
+---------------+-----+---------+-------+---------+---------+---------------------+---------------------+----------------------+---------+---------+---------+---------+---------+----------+---------+---------+-------+-----------+
| ts            | id  | current | phase | voltage | groupid | location            | column1             | column2              | column3 | column4 | column5 | column6 | column7 | column8  | column9 | price   | delta | boolValue |
+---------------+-----+---------+-------+---------+---------+---------------------+---------------------+----------------------+---------+---------+---------+---------+---------+----------+---------+---------+-------+-----------+
| 1725518750053 | 500 | 10.77   | 0.77  | 220     | 1       | California.SanDiego | 9223372036854775807 | 18446744073709551615 | 32767   | 65535   | 127     | 255     | true    | MqttTest | true    | 38405.2 | 6     | false     |
+---------------+-----+---------+-------+---------+---------+---------------------+---------------------+----------------------+---------+---------+---------+---------+---------+----------+---------+---------+-------+-----------+")
    }
}
