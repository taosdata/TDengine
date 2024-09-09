extern crate dlopen2;

use std::collections::HashMap;
use std::fs;
use std::os::raw::c_char;
use std::os::raw::c_void;
use std::sync::RwLock;

use dlopen2::wrapper::{Container, WrapperApi};

use lazy_static::lazy_static;

use serde::{Deserialize, Serialize};

use std::{borrow::Cow, sync::Arc};

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

use super::super::super::runners::get_plugins_home_dir;

use super::Parse;
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize, Serialize)]
pub struct PluginInfo {
    pub id: String,
    pub name: String,
    pub version: String,
}

#[repr(C)]
pub struct ParserResponse {
    e: i32,
    p: *mut c_void,
}

#[derive(WrapperApi, Clone, Debug)]
struct PluginLib {
    parser_name: extern "C" fn() -> *mut c_char,
    parser_version: extern "C" fn() -> *mut c_char,
    parser_new: extern "C" fn(ctx: *const c_char, len: i32) -> ParserResponse,
    parser_mutate: extern "C" fn(
        cp: *mut c_void,
        input_p: *const u8,
        input_l: u32,
        output_p: *mut *mut u8,
        output_l: *mut u32,
    ) -> *const c_char,
    parser_free: extern "C" fn(cp: *mut c_void),
}

struct ParserContainer {
    container: Container<PluginLib>,
    lib_version: String,
    lib_id: String,
    lib_name: String,
}

#[derive(Clone, Debug)]
struct ParserObject {
    p: *mut c_void,
    plugin_lib: PluginLib,
}

// *mut c_void is not Sync and Send，不能放到 PLUGIN_MAP 中
unsafe impl Send for ParserObject {}
unsafe impl Sync for ParserObject {}

impl Drop for ParserObject {
    fn drop(&mut self) {
        self.plugin_lib.parser_free(self.p);
    }
}

impl ParserObject {
    pub fn mutate(&self, input: &[u8]) -> Result<String, String> {
        let mut output_p = std::ptr::null_mut();
        let mut output_l: u32 = 0;
        let result = (self.plugin_lib.parser_mutate)(
            self.p,
            input.as_ptr(),
            input.len() as u32,
            &mut output_p,
            &mut output_l,
        );
        if !result.is_null() {
            return Err("parser_mutate failed".to_string());
        }
        let parsed_data = unsafe {
            String::from_utf8_lossy(std::slice::from_raw_parts(
                output_p as *const u8,
                output_l as usize,
            ))
            .to_string()
        };
        Ok(parsed_data)
    }
}

impl PluginLib {
    fn new_parser(&self, ctx: &str) -> Result<ParserObject, String> {
        let parser_response = self.parser_new(ctx.as_ptr() as *const i8, ctx.len() as i32);
        if parser_response.e != 0 {
            return Err("parser_new failed".to_string());
        }
        Ok(ParserObject {
            p: parser_response.p,
            plugin_lib: self.clone(),
        })
    }
}

lazy_static! {
    static ref PLUGIN_MAP: RwLock<HashMap<String, Arc<ParserContainer>>> = {
        let mut plugin_map = HashMap::new();
        let plugin_path = get_plugins_home_dir();
        let lib_path = plugin_path.join("parsers");

        if let Ok(entries) = fs::read_dir(lib_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() {
                        let container: Container<PluginLib> = unsafe { Container::load(path) }.unwrap();
                        let parser_name = container.parser_name();
                        let parser_name = unsafe { std::ffi::CStr::from_ptr(parser_name).to_str().unwrap() };
                        let parser_version = container.parser_version();
                        let parser_version = unsafe { std::ffi::CStr::from_ptr(parser_version).to_str().unwrap() };

                        let plugin_container = ParserContainer {
                            container,
                            lib_version: parser_version.to_string(),
                            lib_name: parser_name.to_string(),
                            lib_id: entry.file_name().to_str().unwrap().to_string(),
                            // parsers: HashMap::new(),
                        };
                        tracing::debug!("load plugin: {parser_name}");
                        plugin_map.insert(parser_name.to_string(), Arc::new(plugin_container));
                    }
                }
            }
        }

        RwLock::new(plugin_map)
    };
}

/**
 * Parser plugin for extracting fields from JSON object.
 * 比如河北电力使用 {"plugin_type": "hebeipower", "plugin_params": "U,DATA_TYPE"} 来解析数据
 */
#[derive(Debug, Deserialize, Serialize)]
#[serde(try_from = "ParserPluginPreDeserialize")]
pub struct ParserPlugin {
    pub(crate) plugin_type: String,
    pub(crate) plugin_params: String,
    #[serde(skip_serializing)]
    parser_object: ParserObject,
}

impl Clone for ParserPlugin {
    /// ## Safety
    ///
    /// parser_object should always be created by the same PluginLib
    fn clone(&self) -> Self {
        Self::new(&self.plugin_type, &self.plugin_params).unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ParserPluginPreDeserialize {
    pub(crate) plugin_type: String,
    pub(crate) plugin_params: String,
}

impl TryFrom<ParserPluginPreDeserialize> for ParserPlugin {
    type Error = String;

    fn try_from(value: ParserPluginPreDeserialize) -> Result<Self, Self::Error> {
        let plugin_type = value.plugin_type;
        let plugin_params = value.plugin_params;
        Self::new(&plugin_type, &plugin_params)
    }
}

impl Parse for ParserPlugin {
    fn parse_array(
        &self,
        _: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        if array.len() == 0 {
            // Return empty record batch.
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }

        let array = arrow::compute::cast(array, &DataType::Utf8)?;

        let string = array.as_any().downcast_ref::<StringArray>().unwrap();
        let num_rows = string.len();

        let mut json_data = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            if string.is_null(i) {
                continue;
            }

            let value = self.parser_object.mutate(string.value(i).as_bytes());
            if value.is_err() {
                tracing::warn!("plugin parser failed with raw data: {}", string.value(i));
                continue;
            }
            let s = value.unwrap();

            let value = serde_json::from_str::<serde_json::Value>(&s);
            let value = match value {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("{:#}", super::ParseError::JsonDeserializeError(s, e));
                    JsonValue::Null
                }
            };
            match value {
                JsonValue::Object(object) => {
                    json_data.push(Ok(JsonValue::Object(object)));
                }
                JsonValue::Array(array) => {
                    for v in array {
                        if v.is_object() {
                            json_data.push(Ok(v));
                        } else {
                            tracing::warn!(
                                "plugin should return json object array, but one item is: {}",
                                v
                            );
                            continue;
                        }
                    }
                }
                _ => {
                    tracing::warn!(
                        "plugin should return a json array, but return value as: {}",
                        string.value(i)
                    );
                    continue;
                }
            }
        }
        if json_data.len() == 0 {
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }

        let mut schema =
            arrow::json::reader::infer_json_schema_from_iterator(json_data.into_iter())?;

        let json_values: Vec<_> = (0..num_rows)
            .enumerate()
            .flat_map(|(n, i)| {
                if string.is_null(i) {
                    return vec![(n, None)];
                }

                let value = self.parser_object.mutate(string.value(i).as_bytes());
                if value.is_err() {
                    return vec![(n, None)];
                }

                let str = value.unwrap();
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

        let fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect_vec();

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
        let indices = Some(json_values.iter().map(|(i, _)| *i).collect_vec());

        Ok((records, indices))
    }
}

impl ParserPlugin {
    pub fn new(plugin_type: &str, plugin_params: &str) -> Result<Self, String> {
        let plugin_map = PLUGIN_MAP.write().unwrap();
        let plugin_container = plugin_map
            .get(plugin_type)
            .ok_or(format!("plugin {plugin_type} not found"))?;
        let parser_object = plugin_container.container.new_parser(plugin_params)?;
        Ok(Self {
            plugin_type: plugin_type.to_string(),
            plugin_params: plugin_params.to_string(),
            parser_object,
        })
    }

    pub fn list_all_plugins() -> Vec<PluginInfo> {
        let plugin_map = PLUGIN_MAP.read().unwrap();
        plugin_map
            .values()
            .map(|plugin_container| PluginInfo {
                name: plugin_container.lib_name.clone(),
                version: plugin_container.lib_version.clone(),
                id: plugin_container.lib_id.clone(),
            })
            .collect()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_list_all_plugins() {
//         let plugin_list = ParserPlugin::list_all_plugins();
//         println!("plugin_list====: {:?}", plugin_list);
//         // assert_eq!(plugin_list.len(), 1);
//         // assert_eq!(plugin_list[0], "hebeipower");
//     }

//     #[test]
//     fn test_parse_hebeipower() {
//         println!("test_parse_hebeipower");
//         let plugin = ParserPlugin::new("hebeipower", "U,DATA_TYPE");
//         let parser_object = plugin.get_plugin_object();
//         let parser_object = parser_object.unwrap_or_else(|| {
//             println!("new plugin object");
//             plugin.new_plugin_object().unwrap()
//         });

//         println!("parser_object====:");

//         let mutated = parser_object.mutate(b"{}").unwrap();
//         println!("parsed====:{}", mutated);
//     }

//     #[test]
//     fn json_extract_object_by_depth() {
//         let plugin = ParserPlugin::new("hebeipower", "U,DATA_TYPE");

//         let field = Field::new("a1", DataType::Utf8, false);
//         let array: ArrayRef = Arc::new(StringArray::from(vec![
//             r#"{"a1": "a1", "b1": 1.2}"#,
//             r#"{"a1": "a2", "b1": 2.5, "e1": 1}"#,
//             r#"{"a1": "a3", "c1": 1}"#,
//             r#"{"a":1,"b":"2","c1": 35}"#,
//         ]));

//         let (records, indices) = plugin.parse_array(&field, &array).unwrap();

//         dbg!(&records);
//         dbg!(&indices);
//     }
// }
