use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, OnceLock},
};

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use snafu::ResultExt;

use crate::fake_json;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    ReadFile { source: std::io::Error },
    ParseToml { source: toml::de::Error },
    BuildBatch { source: arrow::error::ArrowError },
    Json { source: fake_json::Error },
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
    Json(fake_json::ObjectSchema),
    Utf8(fake_json::StringSchema),
    Boolean(fake_json::BoolSchema),
    Int8(fake_json::NumberSchema<i8>),
    Int16(fake_json::NumberSchema<i16>),
    Int32(fake_json::NumberSchema<i32>),
    Int64(fake_json::NumberSchema<i64>),
    UInt8(fake_json::NumberSchema<u8>),
    UInt16(fake_json::NumberSchema<u16>),
    UInt32(fake_json::NumberSchema<u32>),
    UInt64(fake_json::NumberSchema<u64>),
    Float32(fake_json::NumberSchema<f32>),
    Float64(fake_json::NumberSchema<f64>),
    Timestamp(fake_json::TimestampSchema),
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

    fn from_string(batch_size: usize, buf: &str) -> Result<Self> {
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
                                fake_json::TimestampInterval::Integer(_) => unimplemented!(),
                                fake_json::TimestampInterval::Second(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Second, None),
                                    false,
                                ),
                                fake_json::TimestampInterval::Millisecond(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Millisecond, None),
                                    false,
                                ),
                                fake_json::TimestampInterval::Microsecond(_) => Field::new(
                                    name,
                                    DataType::Timestamp(TimeUnit::Microsecond, None),
                                    false,
                                ),
                                fake_json::TimestampInterval::Nanosecond(_) => Field::new(
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

impl fake_json::ObjectSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<StringArray> {
        Ok(StringArray::from_iter_values(
            (0..batch_size)
                .into_par_iter()
                .map(|_| self.rand_object().context(JsonSnafu).map(|v| v.to_string()))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl fake_json::StringSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<StringArray> {
        Ok(StringArray::from_iter_values(
            (0..batch_size)
                .into_par_iter()
                .map(|_| self.rand_value().context(JsonSnafu))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl fake_json::BoolSchema {
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
        impl fake_json::NumberSchema<$data_t> {
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

impl fake_json::TimestampSchema {
    pub fn rand_array(&self, batch_size: usize) -> Result<ArrayRef> {
        Ok(match self.interval {
            fake_json::TimestampInterval::Integer(_) => unimplemented!(),
            fake_json::TimestampInterval::Second(_) => {
                Arc::new(TimestampSecondArray::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.next_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            fake_json::TimestampInterval::Millisecond(_) => {
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.next_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            fake_json::TimestampInterval::Microsecond(_) => {
                Arc::new(TimestampMicrosecondArray::from_iter_values(
                    (0..batch_size)
                        .into_par_iter()
                        .map(|_| self.next_value().context(JsonSnafu))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            fake_json::TimestampInterval::Nanosecond(_) => {
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

    use super::*;

    #[test]
    fn test_parse() -> anyhow::Result<()> {
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

        assert!(DataFaker::from_string(100, s).is_ok());

        Ok(())
    }
}
