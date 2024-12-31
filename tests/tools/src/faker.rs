use std::{
    collections::HashMap,
    path::Path,
    str::FromStr,
    sync::{
        atomic::{self, AtomicI64, AtomicU32},
        OnceLock,
    },
};

use chrono::{FixedOffset, Local};
use rand::{
    distributions::{Alphanumeric, DistString, Slice},
    Rng,
};
use serde_json as json;
use serde_with::serde_as;
use snafu::{ensure, ResultExt};

use crate::topic::TopicFaker;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Open schema file error"))]
    OpenFile { source: std::io::Error },
    #[snafu(display("Read schema file error"))]
    ReadFile { source: std::io::Error },
    #[snafu(display("Desiralize toml error"))]
    DeserializeToml { source: toml::de::Error },
    #[snafu(display("Parse timestamp error"))]
    ParseDateTime { source: chrono::ParseError },
    #[snafu(display("Expected timestamp type"))]
    ExpectedTimestamp,
    #[snafu(display("Invalid timestamp"))]
    InvalidTimestamp,
    #[snafu(display("Invalid timestamp interval"))]
    InvalidTimestampInterval { source: std::num::ParseIntError },
    #[snafu(display("Expected length field"))]
    ExpectedLength,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct ObjectSchema {
    properties: HashMap<String, DataFakeSchema>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct ArraySchema {
    elements: Box<DataFakeSchema>,
    length: UnsignedNumberSchema,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct StringSchema {
    fixed: Option<String>,
    charset: Option<String>,
    length: Option<UnsignedNumberSchema>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsignedNumberSchema {
    Fixed(u64),
    Range(UnsignedRangeSchema),
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct UnsignedRangeSchema {
    min: Option<u64>,
    max: Option<u64>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignedNumberSchema {
    Fixed(i64),
    Range(SignedRangeSchema),
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct SignedRangeSchema {
    min: Option<i64>,
    max: Option<i64>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct BoolSchema {
    fixed: Option<bool>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct OptionSchema {
    value: Box<DataFakeSchema>,
}

#[serde_as]
#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct TimestampSchema {
    start_time: Option<TimestampValue>,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    interval: Option<TimestampInterval>,
    tick: Option<u32>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(untagged)]
pub enum TimestampValue {
    Integer(i64),
    DateTime(toml::value::Datetime),
}

#[derive(Debug, PartialEq)]
pub enum TimestampInterval {
    Integer(i64),
    Second(i64),
    Millisecond(i64),
    Microsecond(i64),
    Nanosecond(i64),
}

impl FromStr for TimestampInterval {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Some(interval) = s.strip_suffix("ns") {
            return Ok(TimestampInterval::Nanosecond(
                interval
                    .parse::<i64>()
                    .context(InvalidTimestampIntervalSnafu)?,
            ));
        }

        if let Some(interval) = s.strip_suffix("Ms") {
            return Ok(TimestampInterval::Microsecond(
                interval
                    .parse::<i64>()
                    .context(InvalidTimestampIntervalSnafu)?,
            ));
        }

        if let Some(interval) = s.strip_suffix("ms") {
            return Ok(TimestampInterval::Millisecond(
                interval
                    .parse::<i64>()
                    .context(InvalidTimestampIntervalSnafu)?,
            ));
        }

        if let Some(interval) = s.strip_suffix("s") {
            return Ok(TimestampInterval::Second(
                interval
                    .parse::<i64>()
                    .context(InvalidTimestampIntervalSnafu)?,
            ));
        }

        Ok(TimestampInterval::Integer(
            s.parse::<i64>().context(InvalidTimestampIntervalSnafu)?,
        ))
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FloatSchema {
    Fixed(f64),
    Range(FloatRangeSchema),
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct FloatRangeSchema {
    min: Option<f64>,
    max: Option<f64>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DataFakeSchema {
    Object(ObjectSchema),
    Array(ArraySchema),
    String(StringSchema),
    Number(SignedNumberSchema),
    Float(FloatSchema),
    Bool(BoolSchema),
    Option(OptionSchema),
    Timestamp(TimestampSchema),
}

impl DataFakeSchema {
    #[cfg(test)]
    fn object(&self) -> Option<&ObjectSchema> {
        match self {
            DataFakeSchema::Object(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn array(&self) -> Option<&ArraySchema> {
        match self {
            DataFakeSchema::Array(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn string(&self) -> Option<&StringSchema> {
        match self {
            DataFakeSchema::String(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn number(&self) -> Option<&SignedNumberSchema> {
        match self {
            DataFakeSchema::Number(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn float(&self) -> Option<&FloatSchema> {
        match self {
            DataFakeSchema::Float(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn bool(&self) -> Option<&BoolSchema> {
        match self {
            DataFakeSchema::Bool(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn option(&self) -> Option<&OptionSchema> {
        match self {
            DataFakeSchema::Option(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn timestamp(&self) -> Option<&TimestampSchema> {
        match self {
            DataFakeSchema::Timestamp(schema) => Some(schema),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct SchemaFaker {
    pub schema: Vec<Schema>,
}

impl SchemaFaker {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let buf = std::fs::read_to_string(path).context(ReadFileSnafu)?;
        toml::from_str(&buf).context(DeserializeTomlSnafu)
    }
}

#[serde_as]
#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct Schema {
    #[serde_as(as = "Vec<serde_with::DisplayFromStr>")]
    pub topics: Vec<TopicFaker>,
    pub qos: Option<u8>,
    pub payload: DataFaker,
}

#[derive(Debug)]
struct Timestamp {
    ts: AtomicI64,
    interval: i64,
    curr_tick: AtomicU32,
    tick: u32,
}

#[derive(Debug, serde::Deserialize)]
pub struct DataFaker {
    #[serde(skip_deserializing)]
    ts: OnceLock<Timestamp>,
    #[serde(flatten)]
    schema: DataFakeSchema,
}

impl PartialEq for DataFaker {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
    }
}

impl DataFaker {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let buf = std::fs::read_to_string(path).context(ReadFileSnafu)?;

        Ok(Self {
            ts: OnceLock::new(),
            schema: toml::from_str(&buf).context(DeserializeTomlSnafu)?,
        })
    }

    pub fn new(schema: DataFakeSchema) -> Result<Self> {
        Ok(Self {
            ts: OnceLock::new(),
            schema,
        })
    }

    pub fn rand_json(&self) -> Result<json::Value> {
        self.rand_json_value(&self.schema)
    }

    fn rand_json_value(&self, schema: &DataFakeSchema) -> Result<json::Value> {
        match schema {
            DataFakeSchema::Object(schema) => self.rand_object(schema),
            DataFakeSchema::Array(schema) => self.rand_array(schema),
            DataFakeSchema::String(schema) => self.rand_string(schema),
            DataFakeSchema::Number(schema) => self.rand_sign_number(schema),
            DataFakeSchema::Float(schema) => self.rand_float(schema),
            DataFakeSchema::Bool(schema) => self.rand_bool(schema),
            DataFakeSchema::Option(schema) => self.rand_option(schema),
            DataFakeSchema::Timestamp(schema) => self.get_timestamp(schema),
        }
    }

    fn rand_object(&self, ObjectSchema { properties }: &ObjectSchema) -> Result<json::Value> {
        let mut value = json::Map::with_capacity(properties.len());
        for (field, schema) in properties {
            value.insert(field.clone(), self.rand_json_value(schema)?);
        }

        Ok(json::json!(value))
    }

    fn rand_array(&self, ArraySchema { elements, length }: &ArraySchema) -> Result<json::Value> {
        let len = self.rand_unsigned_number(length)?.as_u64().unwrap() as usize;

        Ok(json::json!((0..len)
            .map(|_| self.rand_json_value(elements))
            .collect::<Result<Vec<_>>>()?))
    }

    fn rand_string(
        &self,
        StringSchema {
            fixed,
            charset,
            length,
        }: &StringSchema,
    ) -> Result<json::Value> {
        if let Some(fixed) = fixed {
            return Ok(json::json!(fixed));
        }
        ensure!(length.is_some(), ExpectedLengthSnafu);
        let length = length.as_ref().unwrap();
        let len = self.rand_unsigned_number(length)?.as_u64().unwrap() as usize;
        let mut rng = rand::thread_rng();
        let value: String = match charset {
            Some(charset) => rng
                .sample_iter(Slice::new(charset.as_bytes()).unwrap())
                .take(len)
                .map(|c| *c as char)
                .collect(),
            None => Alphanumeric.sample_string(&mut rng, len),
        };

        Ok(json::json!(value))
    }

    fn rand_bool(&self, BoolSchema { fixed }: &BoolSchema) -> Result<json::Value> {
        Ok(fixed
            .filter(|x| *x)
            .map(|x| json::json!(x))
            .unwrap_or_else(|| json::json!(rand::random::<bool>())))
    }

    fn rand_option(&self, OptionSchema { value }: &OptionSchema) -> Result<json::Value> {
        if rand::random() {
            return Ok(json::Value::Null);
        }

        self.rand_json_value(value)
    }

    fn rand_unsigned_number(&self, schema: &UnsignedNumberSchema) -> Result<json::Value> {
        match schema {
            UnsignedNumberSchema::Fixed(fixed) => Ok(json::json!(fixed)),
            UnsignedNumberSchema::Range(UnsignedRangeSchema { min, max }) => {
                let (min, max) = (min.unwrap_or(u64::MIN), max.unwrap_or(u64::MAX));
                Ok(json::json!(rand::thread_rng().gen_range(min..=max)))
            }
        }
    }

    fn rand_sign_number(&self, schema: &SignedNumberSchema) -> Result<json::Value> {
        match schema {
            SignedNumberSchema::Fixed(fixed) => Ok(json::json!(fixed)),
            SignedNumberSchema::Range(SignedRangeSchema { min, max }) => {
                let (min, max) = (min.unwrap_or(i64::MIN), max.unwrap_or(i64::MAX));
                Ok(json::json!(rand::thread_rng().gen_range(min..=max)))
            }
        }
    }

    fn rand_float(&self, schema: &FloatSchema) -> Result<json::Value> {
        match schema {
            FloatSchema::Fixed(fixed) => Ok(json::json!(fixed)),
            FloatSchema::Range(FloatRangeSchema { min, max }) => {
                let (min, max) = (min.unwrap_or(f64::MIN), max.unwrap_or(f64::MAX));
                Ok(json::json!(rand::thread_rng().gen_range(min..=max)))
            }
        }
    }

    fn get_timestamp(
        &self,
        TimestampSchema {
            start_time,
            interval,
            tick,
        }: &TimestampSchema,
    ) -> Result<json::Value> {
        let ts = match (start_time, interval) {
            (Some(TimestampValue::Integer(value)), Some(TimestampInterval::Integer(interval))) => {
                self.ts.get_or_init(|| Timestamp {
                    ts: AtomicI64::new(*value),
                    interval: *interval,
                    curr_tick: AtomicU32::default(),
                    tick: tick.unwrap_or(1),
                })
            }
            // TODO: use `get_or_try_init` when stable
            (Some(TimestampValue::DateTime(value)), Some(interval)) => self.ts.get_or_init(|| {
                let date = value.date.expect("invalid timestamp");
                let time = value.time.expect("invalid timestamp");
                let offset = value.offset;
                let dt = chrono::NaiveDate::from_ymd_opt(
                    date.year as i32,
                    date.month as u32,
                    date.day as u32,
                )
                .unwrap()
                .and_hms_nano_opt(
                    time.hour as u32,
                    time.minute as u32,
                    time.second as u32,
                    time.nanosecond,
                )
                .unwrap();
                let dt = match offset {
                    Some(toml::value::Offset::Z) => dt.and_utc(),
                    Some(toml::value::Offset::Custom { minutes }) => dt
                        .and_local_timezone(FixedOffset::east_opt((minutes as i32) * 60).unwrap())
                        .unwrap()
                        .to_utc(),
                    None => dt.and_local_timezone(Local).unwrap().to_utc(),
                };

                match interval {
                    TimestampInterval::Integer(_) => panic!("duration timestamp interval needed"),
                    TimestampInterval::Second(interval) => Timestamp {
                        ts: AtomicI64::new(dt.timestamp()),
                        interval: *interval,
                        curr_tick: AtomicU32::default(),
                        tick: tick.unwrap_or_default(),
                    },
                    TimestampInterval::Millisecond(interval) => Timestamp {
                        ts: AtomicI64::new(dt.timestamp_millis()),
                        interval: *interval,
                        curr_tick: AtomicU32::default(),
                        tick: tick.unwrap_or_default(),
                    },
                    TimestampInterval::Microsecond(interval) => Timestamp {
                        ts: AtomicI64::new(dt.timestamp_micros()),
                        interval: *interval,
                        curr_tick: AtomicU32::default(),
                        tick: tick.unwrap_or_default(),
                    },
                    TimestampInterval::Nanosecond(interval) => Timestamp {
                        ts: AtomicI64::new(
                            dt.timestamp_nanos_opt().expect("get nano timestamp error"),
                        ),
                        interval: *interval,
                        curr_tick: AtomicU32::default(),
                        tick: tick.unwrap_or_default(),
                    },
                }
            }),
            _ => return InvalidTimestampSnafu.fail(),
        };
        let curr_tick = ts.curr_tick.fetch_add(1, atomic::Ordering::SeqCst);
        let ts = if curr_tick >= ts.tick {
            ts.curr_tick.store(0, atomic::Ordering::SeqCst);
            ts.ts.fetch_add(ts.interval, atomic::Ordering::SeqCst)
        } else {
            ts.ts.load(atomic::Ordering::SeqCst)
        };

        Ok(json::json!(ts))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn parse_datafaker_test() -> anyhow::Result<()> {
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "string"
                fixed = "abc"
                length = { range = { max = 999 } }
                charset = "abcdefg"
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.string();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &StringSchema {
                    fixed: Some("abc".to_string()),
                    charset: Some("abcdefg".to_string()),
                    length: Some(UnsignedNumberSchema::Range(UnsignedRangeSchema {
                        min: None,
                        max: Some(999)
                    }))
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "number"
                fixed = 5
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.number();
            assert!(schema.is_some());
            assert_eq!(schema.unwrap(), &SignedNumberSchema::Fixed(5))
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "float"
                range = { min = 999 }
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.float();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &FloatSchema::Range(FloatRangeSchema {
                    min: Some(999.0),
                    max: None
                })
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "bool"
                fixed = false
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.bool();
            assert!(schema.is_some());
            assert_eq!(schema.unwrap(), &BoolSchema { fixed: Some(false) })
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "object"
                [properties.a]
                type = "bool"
                fixed = false
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.object();
            assert!(schema.is_some());

            assert_eq!(
                schema.unwrap(),
                &ObjectSchema {
                    properties: HashMap::from_iter([(
                        "a".to_string(),
                        DataFakeSchema::Bool(BoolSchema { fixed: Some(false) })
                    )])
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "array"
                length = { range = { max = 999 } }
                [elements]
                type = "bool"
                fixed = false
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.array();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &ArraySchema {
                    elements: Box::new(DataFakeSchema::Bool(BoolSchema { fixed: Some(false) })),
                    length: UnsignedNumberSchema::Range(UnsignedRangeSchema {
                        min: None,
                        max: Some(999)
                    })
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "option"
                [value]
                type = "bool"
                fixed = false
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.option();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &OptionSchema {
                    value: Box::new(DataFakeSchema::Bool(BoolSchema { fixed: Some(false) }))
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "timestamp"
                start_time = 123
                interval = "1ns"
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.timestamp();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &TimestampSchema {
                    start_time: Some(TimestampValue::Integer(123)),
                    interval: Some(TimestampInterval::Nanosecond(1)),
                    tick: None
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "timestamp"
                start_time = 2024-11-02T17:35:34
                interval = "3ns"
                tick = 100
            "#,
            )?;
            let schema = schema.timestamp();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &TimestampSchema {
                    start_time: Some(TimestampValue::DateTime(
                        toml::value::Datetime::from_str("2024-11-02T17:35:34").unwrap()
                    )),
                    interval: Some(TimestampInterval::Nanosecond(3)),
                    tick: Some(100)
                }
            )
        }

        Ok(())
    }

    #[test]
    fn parse_schema_faker_test() -> anyhow::Result<()> {
        let schema: SchemaFaker = toml::from_str(
            r#"
[[schema]]
topics = [
    "ems/site/{::60}/root/{::60}/string",
    "ems/site/{::60}/{::60}/{::60}/{::60}/string",
    "ems/site/{::60}/unit/{::60}/root/{::60}/string",
    "ems/site/{::60}/unit/{::60}/{::60}/{::60}/{::60}/string",
]
qos = 0

[schema.payload]
type = "object"

[schema.payload.properties]
ts = { type = "timestamp", start_time = 2025-10-01T00:00:00.888888888, precision = "ns" }
value = { type = "option", value = { type = "string", length = { range = { min = 10, max = 1000 } } } }
        "#,
        )?;
        assert_eq!(schema.schema.len(), 1);
        assert_eq!(
            schema.schema[0].topics,
            [
                "ems/site/{::60}/root/{::60}/string".parse()?,
                "ems/site/{::60}/{::60}/{::60}/{::60}/string".parse()?,
                "ems/site/{::60}/unit/{::60}/root/{::60}/string".parse()?,
                "ems/site/{::60}/unit/{::60}/{::60}/{::60}/{::60}/string".parse()?,
            ]
        );
        assert_eq!(schema.schema[0].qos, Some(0));
        assert_eq!(
            schema.schema[0].payload,
            toml::from_str(
                r#"
type = "object"
[properties.ts]
type = "timestamp"
start_time = 2025-10-01T00:00:00.888888888
precision = "ns"

[properties.value]
type = "option"

[properties.value.value]
type = "string"
length = { range = { min = 10, max = 1000 } }
        "#
            )?
        );
        Ok(())
    }
}
