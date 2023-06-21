use std::{any::Any, collections::HashMap, sync::Arc, task::Poll};

use arrow::{
    array::{
        make_builder, Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array, BooleanArray,
    },
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use either::Either;
use futures::{Sink, Stream};
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use taos::{
    taos_query::common::{Describe, RawData},
    JsonMeta, RawBlock, Value,
};

use serde_json::Value as JsonValue;

use crate::plugins::transform::MessageArrowRecords;

use super::Select;
use super::{Error, Message, TransformExt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Json {
    at: String,
    flatten: bool,
    select: Option<Select>,
    keep: bool,
}

impl TransformExt for Json {
    fn transform_message(&self, item: Message) -> Result<Option<Message>, Error> {
        match item {
            Message::Records(records) => {
                Ok(Some(Message::Records(
                    records
                        .into_iter()
                        .map(|v| {
                            let old_schema = v.records.schema();
                            let num_columns = v.records.num_columns();
                            if let Ok(at) = v.records.schema().index_of(&self.at) {
                                let col = v.records.column(at);
                                let col = arrow::compute::cast(&col, &DataType::Utf8).unwrap();
                                let string = col.as_any().downcast_ref::<StringArray>().unwrap();
                                let mut schema =
                                    arrow::json::reader::infer_json_schema_from_iterator(
                                        (0..string.len())
                                            .filter_map(|index| {
                                                if string.is_null(index) {
                                                    None
                                                } else {
                                                    let value = string.value(index);
                                                    let value: serde_json::Value =
                                                        serde_json::from_str(&value).unwrap();

                                                    if value.is_array() {
                                                        let values = value.as_array().unwrap();
                                                        Some(values.clone())
                                                    } else {
                                                        Some(vec![value])
                                                    }
                                                }
                                            })
                                            .flatten()
                                            .map(Ok),
                                    )
                                    .unwrap();

                                dbg!(&schema);
                                if let Some(select) = self.select.as_ref() {
                                    schema = select.schema(&schema);
                                    dbg!(&schema);
                                    // records = select.record_batch(&records).unwrap();
                                    // schema = Schema::new(
                                    //     select
                                    //         .iter()
                                    //         .filter_map(|name| {
                                    //             schema.field_with_name(name).ok().map(Clone::clone)
                                    //         })
                                    //         .collect_vec(),
                                    // );
                                }

                                let fields = schema.fields().clone();
                                let mut origin = old_schema.fields().clone();
                                if self.keep {
                                    schema = Schema::try_merge(vec![
                                        old_schema.as_ref().clone(),
                                        schema,
                                    ])
                                    .unwrap();
                                } else {
                                    let indices = (0..at).chain(at + 1..num_columns).collect_vec();
                                    let old_schema = old_schema.project(&indices).unwrap();
                                    origin = old_schema.fields().clone();
                                    schema = Schema::try_merge(vec![old_schema, schema]).unwrap();
                                }

                                let schema_ref = Arc::new(schema);
                                let row = v.records.num_rows();

                                let builder: HashMap<_, _> = schema_ref
                                    .fields()
                                    .iter()
                                    .map(|f| (f.name(), make_builder(f.data_type(), row)))
                                    .collect();

                                if !self.flatten {
                                    for f in origin.iter() {
                                        let name = f.name();
                                        let _column = v.records.column_by_name(name).unwrap();
                                        // todo: flatten into multiple rows if json is array.
                                    }
                                }

                                let json_values: Vec<_> = (0..row)
                                    .enumerate()
                                    .flat_map(|(n, i)| {
                                        if string.is_null(i) {
                                            vec![(n, None)]
                                        } else {
                                            let value = string.value(i);
                                            let value: serde_json::Value =
                                                serde_json::from_str(&value).unwrap();

                                            match value {
                                                JsonValue::Array(array) => array
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
                                                JsonValue::Null => {
                                                    vec![(n, None)]
                                                }
                                                JsonValue::Object(object) => {
                                                    vec![(n, Some(JsonValue::Object(object)))]
                                                }
                                                _ => unreachable!(),
                                            }
                                        }
                                    })
                                    .collect();

                                let mut batch_container = Vec::new();

                                for f in &origin {
                                    let array = v.records.column_by_name(f.name()).unwrap().clone();
                                    batch_container.push((f.name(), array));
                                }

                                for f in &fields {
                                    let dt = f.data_type();
                                    let name = f.metadata().get("name").unwrap_or(f.name());
                                    match dt {
                                        DataType::UInt8 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_u64().map(|v| v as u8)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(UInt8Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::UInt16 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_u64().map(|v| v as u16)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(UInt16Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::UInt32 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_u64().map(|v| v as u32)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(UInt32Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::UInt64 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_u64()
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(UInt64Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Int8 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_i64().map(|v| v as i8)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(Int8Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Int16 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_i64().map(|v| v as i16)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(Int16Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Int32 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_i64().map(|v| v as i32)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(Int32Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Int64 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_i64()
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(Int64Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Float32 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_f64().map(|f| f as f32)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(Float32Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Float64 => {
                                            //
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_f64()
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(Float64Array::from_iter(values));

                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Utf8 | DataType::LargeUtf8 => {
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_str()
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(StringArray::from_iter(values));
                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Binary | DataType::LargeBinary => {
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_str().map(|s| s.as_bytes())
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(BinaryArray::from_iter(values));
                                            batch_container.push((f.name(), array));
                                        }
                                        DataType::Boolean => {
                                            let values = json_values
                                                .iter()
                                                .map(|(n, v)| {
                                                    if let Some(v) =
                                                        v.as_ref().and_then(|v| v.get(name))
                                                    {
                                                        v.as_bool()
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect_vec();
                                            let array: ArrayRef =
                                                Arc::new(BooleanArray::from_iter(values));
                                            batch_container.push((f.name(), array));
                                        }
                                        _ => todo!(),
                                    }
                                }

                                let records = RecordBatch::try_from_iter(batch_container).unwrap();
                                dbg!(&records);

                                MessageArrowRecords {
                                    table: v.table.clone(),
                                    records,
                                }
                            } else {
                                v
                            }
                        })
                        .collect(),
                )))
            }
            item => Ok(Some(item)),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::ArrayRef;
    use futures::{SinkExt, StreamExt};

    use crate::plugins::transform::MessageTableMeta;

    use super::*;

    #[test]
    fn json_extract() {
        let extract = Json {
            at: "a".to_string(),
            flatten: true,
            select: Some(serde_json::from_str(&r#"["a1=a3::nchar(100)", "b1=b1::f32"]"#).unwrap()),
            // select: None,
            keep: false,
        };

        let b: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a1": "a1", "b1": 1}"#,
            r#"{"a1": "a2", "c1": 1}"#,
        ]));

        let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let item = Message::records(vec![MessageArrowRecords {
            table: MessageTableMeta::new(Arc::new("tb1".to_string()), None, None),
            records,
        }]);

        let records = extract.transform_message(item).unwrap();

        dbg!(&records);
    }
}
