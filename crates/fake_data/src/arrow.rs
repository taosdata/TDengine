//! 按 Arrow RecordBatch 格式生成假数据，基于 JSON schema 类型扩展 `rand_array`。

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, OnceLock},
};

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use snafu::ResultExt;

use crate::json;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    ReadFile { source: std::io::Error },
    ParseToml { source: toml::de::Error },
    BuildBatch { source: arrow::error::ArrowError },
    Json { source: json::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct ObjectSchema {
    properties: HashMap<String, DataFakeSchema>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DataFakeSchema {
    /// from json to utf8
    Json(json::ObjectSchema),
    Utf8(json::StringSchema),
    Boolean(json::BoolSchema),
    Int8(json::NumberSchema<i8>),
    Int16(json::NumberSchema<i16>),
    Int32(json::NumberSchema<i32>),
    Int64(json::NumberSchema<i64>),
    UInt8(json::NumberSchema<u8>),
    UInt16(json::NumberSchema<u16>),
    UInt32(json::NumberSchema<u32>),
    UInt64(json::NumberSchema<u64>),
    Float32(json::NumberSchema<f32>),
    Float64(json::NumberSchema<f64>),
    Timestamp(json::TimestampSchema),
}

impl DataFakeSchema {
    fn rand_array(&self, batch_size: usize) -> Result<ArrayRef> {
        Ok(match self {
            DataFakeSchema::Json(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Utf8(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Boolean(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Int8(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Int16(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Int32(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Int64(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::UInt8(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::UInt16(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::UInt32(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::UInt64(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Float32(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Float64(schema) => Arc::new(schema.rand_array(batch_size)?) as ArrayRef,
            DataFakeSchema::Timestamp(schema) => schema.rand_array(batch_size)?,
        })
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct DataFaker {
    batch_size: usize,
    #[serde(skip_deserializing)]
    schema: OnceLock<Schema>,
    columns: HashMap<String, DataFakeSchema>,
}

impl DataFaker {
    pub fn from_file(batch_size: usize, path: impl AsRef<Path>) -> Result<Self> {
        let buf = std::fs::read_to_string(path).context(ReadFileSnafu)?;
        Self::from_string(batch_size, &buf)
    }

    pub fn from_string(batch_size: usize, buf: &str) -> Result<Self> {
        Ok(Self {
            batch_size,
            schema: OnceLock::new(),
            columns: toml::from_str(buf).context(ParseTomlSnafu)?,
        })
    }

    // TODO: use `get_or_try_init` when stable
    pub fn get_schema(&self) -> Schema {
        self.schema
            .get_or_init(|| {
                let meta = HashMap::from_iter([
                    ("version".to_string(), "1.0".to_string()),
                    ("stream".to_string(), "flat".to_string()),
                    ("ack".to_string(), "lush".to_string()),
                ]);
                Schema::new_with_metadata(
                    self.columns
                        .iter()
                        .map(|(name, schema)| match schema {
                            DataFakeSchema::Json(_) => Field::new(name, DataType::Utf8, false),
                            DataFakeSchema::Utf8(_) => Field::new(name, DataType::Utf8, false),
                            DataFakeSchema::Boolean(_) => {
                                Field::new(name, DataType::Boolean, false)
                            }
                            DataFakeSchema::Int8(_) => Field::new(name, DataType::Int8, false),
                            DataFakeSchema::Int16(_) => Field::new(name, DataType::Int16, false),
                            DataFakeSchema::Int32(_) => Field::new(name, DataType::Int32, false),
                            DataFakeSchema::Int64(_) => Field::new(name, DataType::Int64, false),
                            DataFakeSchema::UInt8(_) => Field::new(name, DataType::UInt8, false),
                            DataFakeSchema::UInt16(_) => Field::new(name, DataType::UInt16, false),
                            DataFakeSchema::UInt32(_) => Field::new(name, DataType::UInt32, false),
                            DataFakeSchema::UInt64(_) => Field::new(name, DataType::UInt64, false),
                            DataFakeSchema::Float32(_) => {
                                Field::new(name, DataType::Float32, false)
                            }
                            DataFakeSchema::Float64(_) => {
                                Field::new(name, DataType::Float64, false)
                            }
                            DataFakeSchema::Timestamp(schema) => match schema.interval {
                                json::TimestampInterval::Integer(_) => unimplemented!(),
                                json::TimestampInterval::Second(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Second, None),
                                    false,
                                ),
                                json::TimestampInterval::Millisecond(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Millisecond, None),
                                    false,
                                ),
                                json::TimestampInterval::Microsecond(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Microsecond, None),
                                    false,
                                ),
                                json::TimestampInterval::Nanosecond(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                                    false,
                                ),
                            },
                        })
                        .collect::<Vec<_>>(),
                    meta,
                )
            })
            .clone()
    }

    pub fn rand_record_batch(&self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::new(self.get_schema()),
            self.columns
                .par_iter()
                .map(|(_, schema)| schema.rand_array(self.batch_size))
                .collect::<Result<Vec<_>>>()?,
        )
        .context(BuildBatchSnafu)
    }
}

impl json::ObjectSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<StringArray> {
        Ok(StringArray::from_iter_values(
            (0..batch_size)
                .into_par_iter()
                .map(|_| self.rand_object().context(JsonSnafu).map(|v| v.to_string()))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl json::StringSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<StringArray> {
        Ok(StringArray::from_iter_values(
            (0..batch_size)
                .into_par_iter()
                .map(|_| self.rand_value().context(JsonSnafu))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl json::BoolSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<BooleanArray> {
        Ok((0..batch_size)
            .into_par_iter()
            .map(|_| self.rand_value().context(JsonSnafu))
            .collect::<Result<Vec<bool>>>()?
            .into())
    }
}

macro_rules! impl_rand_number_array {
    ($array_t: ty, $data_t: ty) => {
        impl json::NumberSchema<$data_t> {
            pub fn rand_array(&self, batch_size: usize) -> Result<$array_t> {
                Ok(<$array_t>::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.rand_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
        }
    };
}

impl_rand_number_array!(Int8Array, i8);
impl_rand_number_array!(Int16Array, i16);
impl_rand_number_array!(Int32Array, i32);
impl_rand_number_array!(Int64Array, i64);
impl_rand_number_array!(UInt8Array, u8);
impl_rand_number_array!(UInt16Array, u16);
impl_rand_number_array!(UInt32Array, u32);
impl_rand_number_array!(UInt64Array, u64);
impl_rand_number_array!(Float32Array, f32);
impl_rand_number_array!(Float64Array, f64);

impl json::TimestampSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<ArrayRef> {
        Ok(match self.interval {
            json::TimestampInterval::Integer(_) => unimplemented!(),
            json::TimestampInterval::Second(_) => Arc::new(TimestampSecondArray::from_iter_values(
                (0..batch_size)
                    .into_par_iter()
                    .map(|_| self.next_value().context(JsonSnafu))
                    .collect::<Result<Vec<_>>>()?,
            )),
            json::TimestampInterval::Millisecond(_) => {
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.next_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            json::TimestampInterval::Microsecond(_) => {
                Arc::new(TimestampMicrosecondArray::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.next_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            json::TimestampInterval::Nanosecond(_) => {
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.next_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;

    fn parse_faker(batch_size: usize, toml_str: &str) -> Result<DataFaker> {
        DataFaker::from_string(batch_size, toml_str)
    }

    #[test]
    fn from_string_parses_full_schema() -> anyhow::Result<()> {
        let s = r#"
[topic]
type = "utf8"
fixed = "topic"

[qos]
type = "int8"
fixed = 0

[payload]
type = "json"

[payload.properties]
ts = { type = "timestamp", start_time = 2025-10-01T00:00:00.888000999, interval = "1ms" }
value = { type = "number", fixed = 1000 }

[site_controller_id]
type = "utf8"
fixed = "site_controller_3"

[data_type]
type = "utf8"
fixed = "integer"

[point_name]
type = "utf8"
random = { length = { fixed = 3 }, charset = "abcd" }
        "#;
        let faker = parse_faker(100, s).map_err(|e| anyhow::anyhow!("{:?}", e))?;
        assert_eq!(faker.columns.len(), 6);
        Ok(())
    }

    #[test]
    fn get_schema_returns_correct_field_types() {
        let s = r#"
[id]
type = "int64"
fixed = 1
[name]
type = "utf8"
fixed = "x"
[ok]
type = "boolean"
fixed = true
        "#;
        let faker = parse_faker(10, s).expect("parse");
        let schema = faker.get_schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let types: Vec<_> = schema.fields().iter().map(|f| f.data_type()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"ok"));
        assert!(types.contains(&&DataType::Int64));
        assert!(types.contains(&&DataType::Utf8));
        assert!(types.contains(&&DataType::Boolean));
    }

    #[test]
    fn rand_record_batch_produces_correct_length() {
        let s = r#"
[a]
type = "int32"
fixed = 42
[b]
type = "utf8"
fixed = "hello"
        "#;
        let batch_size = 50;
        let faker = parse_faker(batch_size, s).expect("parse");
        let batch = faker.rand_record_batch().expect("rand_record_batch");
        assert_eq!(batch.num_rows(), batch_size);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn rand_record_batch_fixed_values_are_stable() {
        let s = r#"
[v]
type = "int64"
fixed = 999
        "#;
        let faker = parse_faker(20, s).expect("parse");
        let batch = faker.rand_record_batch().expect("rand_record_batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..col.len() {
            assert_eq!(col.value(i), 999);
        }
    }

    #[test]
    fn utf8_fixed_column_content() {
        let s = r#"
[msg]
type = "utf8"
fixed = "same_every_time"
        "#;
        let faker = parse_faker(5, s).expect("parse");
        let batch = faker.rand_record_batch().expect("rand_record_batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        for i in 0..col.len() {
            assert_eq!(col.value(i), "same_every_time");
        }
    }

    #[test]
    fn boolean_fixed_column() {
        let s = r#"
[flag]
type = "boolean"
fixed = false
        "#;
        let faker = parse_faker(10, s).expect("parse");
        let batch = faker.rand_record_batch().expect("rand_record_batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");
        for i in 0..col.len() {
            assert!(!col.value(i));
        }
    }

    #[test]
    fn timestamp_millisecond_column() {
        let s = r#"
[ts]
type = "timestamp"
start_time = 2025-01-01T00:00:00
interval = "1ms"
        "#;
        let faker = parse_faker(3, s).expect("parse");
        let batch = faker.rand_record_batch().expect("rand_record_batch");
        assert_eq!(batch.num_rows(), 3);
        let col = batch.column(0);
        assert!(
            col.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .is_some()
        );
    }
}
