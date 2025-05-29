use std::{
    collections::HashMap,
    path::Path,
    str::FromStr,
    sync::{
        atomic::{self, AtomicI64},
        OnceLock,
    },
};

use chrono::{FixedOffset, Local};
use rand::{
    distributions::{Alphanumeric, DistString, Slice},
    seq::SliceRandom,
    Rng,
};
use serde_json as json;
use serde_with::serde_as;
use snafu::{OptionExt, ResultExt};

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
    #[snafu(display("String samples empty"))]
    EmptyStringSample,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct ObjectSchema {
    properties: HashMap<String, DataFakeSchema>,
}

impl ObjectSchema {
    pub fn rand_object(&self) -> Result<json::Value> {
        let mut value = json::Map::with_capacity(self.properties.len());
        for (field, schema) in &self.properties {
            value.insert(field.clone(), schema.rand_json_value()?);
        }

        Ok(json::json!(value))
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct ArraySchema {
    elements: Box<DataFakeSchema>,
    length: NumberSchema<u64>,
}

impl ArraySchema {
    pub fn rand_array(&self) -> Result<json::Value> {
        let len = self.length.rand_value()? as usize;

        Ok(json::json!((0..len)
            .map(|_| self.elements.rand_json_value())
            .collect::<Result<Vec<_>>>()?))
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StringSchema {
    Fixed(String),
    Random {
        charset: Option<String>,
        length: NumberSchema<u64>,
    },
    Samples(Vec<String>),
}

impl StringSchema {
    pub fn rand_value(&self) -> Result<String> {
        let mut rng = rand::thread_rng();
        let value = match self {
            StringSchema::Fixed(fixed) => fixed.clone(),
            StringSchema::Random { charset, length } => {
                let len = length.rand_value()? as usize;
                match charset {
                    Some(charset) => rng
                        .sample_iter(Slice::new(charset.as_bytes()).unwrap())
                        .take(len)
                        .map(|c| *c as char)
                        .collect(),
                    None => Alphanumeric.sample_string(&mut rng, len),
                }
            }
            StringSchema::Samples(samples) => samples
                .choose(&mut rng)
                .context(EmptyStringSampleSnafu)?
                .clone(),
        };

        Ok(value)
    }

    fn rand_string(&self) -> Result<json::Value> {
        Ok(json::json!(self.rand_value()?))
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct BoolSchema {
    pub(crate) fixed: Option<bool>,
}

impl BoolSchema {
    pub fn rand_value(&self) -> Result<bool> {
        Ok(self.fixed.unwrap_or_else(rand::random))
    }

    fn rand_bool(&self) -> Result<json::Value> {
        Ok(json::json!(self.rand_value()?))
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct OptionSchema {
    value: Box<DataFakeSchema>,
}

impl OptionSchema {
    pub fn rand_option(&self) -> Result<json::Value> {
        if rand::random() {
            return Ok(json::Value::Null);
        }

        self.value.rand_json_value()
    }
}

#[derive(Debug)]
struct Timestamp {
    ts: AtomicI64,
    interval: i64,
}

#[serde_as]
#[derive(Debug, Default, serde::Deserialize)]
pub struct TimestampSchema {
    #[serde(skip_deserializing)]
    ts: OnceLock<Timestamp>,
    start_time: Option<TimestampValue>,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub(crate) interval: TimestampInterval,
}

impl PartialEq for TimestampSchema {
    fn eq(&self, other: &Self) -> bool {
        self.start_time == other.start_time && self.interval == other.interval
    }
}

impl TimestampSchema {
    pub fn next_value(&self) -> Result<i64> {
        let start_time = self
            .start_time
            .unwrap_or_else(|| TimestampValue::Integer(Local::now().timestamp_millis()));
        let ts = match (start_time, &self.interval) {
            (TimestampValue::Integer(value), TimestampInterval::Integer(interval)) => {
                self.ts.get_or_init(|| Timestamp {
                    ts: AtomicI64::new(value),
                    interval: *interval,
                })
            }
            // TODO: use `get_or_try_init` when stable
            (TimestampValue::DateTime(value), interval) => self.ts.get_or_init(|| {
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
                    },
                    TimestampInterval::Millisecond(interval) => Timestamp {
                        ts: AtomicI64::new(dt.timestamp_millis()),
                        interval: *interval,
                    },
                    TimestampInterval::Microsecond(interval) => Timestamp {
                        ts: AtomicI64::new(dt.timestamp_micros()),
                        interval: *interval,
                    },
                    TimestampInterval::Nanosecond(interval) => Timestamp {
                        ts: AtomicI64::new(
                            dt.timestamp_nanos_opt().expect("get nano timestamp error"),
                        ),
                        interval: *interval,
                    },
                }
            }),
            _ => return InvalidTimestampSnafu.fail(),
        };

        Ok(ts.ts.fetch_add(ts.interval, atomic::Ordering::SeqCst))
    }

    pub fn next_timestamp(&self) -> Result<json::Value> {
        Ok(json::json!(self.next_value()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize)]
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

impl Default for TimestampInterval {
    fn default() -> Self {
        TimestampInterval::Millisecond(1)
    }
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

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumberSchema<T> {
    Fixed(T),
    Range(NumberRangeSchema<T>),
}

macro_rules! impl_rand_number {
    ($t: ty) => {
        impl NumberSchema<$t> {
            pub fn rand_value(&self) -> Result<$t> {
                match self {
                    NumberSchema::Fixed(fixed) => Ok(*fixed),
                    NumberSchema::Range(NumberRangeSchema { min, max }) => {
                        let (min, max) = (min.unwrap_or(<$t>::MIN), max.unwrap_or(<$t>::MAX));
                        Ok(rand::thread_rng().gen_range(min..=max))
                    }
                }
            }

            pub fn rand_number(&self) -> Result<json::Value> {
                Ok(json::json!(self.rand_value()?))
            }
        }
    };
}

impl_rand_number!(i8);
impl_rand_number!(i16);
impl_rand_number!(i32);
impl_rand_number!(i64);
impl_rand_number!(u8);
impl_rand_number!(u16);
impl_rand_number!(u32);
impl_rand_number!(u64);
impl_rand_number!(f32);
impl_rand_number!(f64);

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct NumberRangeSchema<T> {
    min: Option<T>,
    max: Option<T>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DataFakeSchema {
    Object(ObjectSchema),
    Array(ArraySchema),
    String(StringSchema),
    Number(NumberSchema<i64>),
    Float(NumberSchema<f64>),
    Bool(BoolSchema),
    Option(OptionSchema),
    Timestamp(TimestampSchema),
}

impl DataFakeSchema {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        toml::from_str(&std::fs::read_to_string(path).context(ReadFileSnafu)?)
            .context(DeserializeTomlSnafu)
    }

    pub fn rand_json_value(&self) -> Result<json::Value> {
        match self {
            DataFakeSchema::Object(schema) => schema.rand_object(),
            DataFakeSchema::Array(schema) => schema.rand_array(),
            DataFakeSchema::String(schema) => schema.rand_string(),
            DataFakeSchema::Number(schema) => schema.rand_number(),
            DataFakeSchema::Float(schema) => schema.rand_number(),
            DataFakeSchema::Bool(schema) => schema.rand_bool(),
            DataFakeSchema::Option(schema) => schema.rand_option(),
            DataFakeSchema::Timestamp(schema) => schema.next_timestamp(),
        }
    }

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
    fn number(&self) -> Option<&NumberSchema<i64>> {
        match self {
            DataFakeSchema::Number(schema) => Some(schema),
            _ => None,
        }
    }

    #[cfg(test)]
    fn float(&self) -> Option<&NumberSchema<f64>> {
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
                random = { length = { range = { max = 999 } } , charset = "abcdefg" }
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.string();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &StringSchema::Random {
                    charset: Some("abcdefg".to_string()),
                    length: NumberSchema::Range(NumberRangeSchema {
                        min: None,
                        max: Some(999)
                    })
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
            assert_eq!(schema.unwrap(), &NumberSchema::Fixed(5))
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
                &NumberSchema::Range(NumberRangeSchema {
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
                    length: NumberSchema::Range(NumberRangeSchema {
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
                    interval: TimestampInterval::Nanosecond(1),
                    ts: OnceLock::new()
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
                    interval: TimestampInterval::Nanosecond(3),
                    ts: OnceLock::new()
                }
            )
        }

        Ok(())
    }
}
