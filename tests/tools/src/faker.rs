use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{self, AtomicI64},
        OnceLock,
    },
};

use chrono::{FixedOffset, Local};
use rand::{
    distributions::{Alphanumeric, DistString},
    seq::SliceRandom,
    Rng,
};
use serde_json as json;
use snafu::{ensure, ResultExt};
use tokio::io::AsyncReadExt;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Open schema file error: {source}"))]
    OpenFile { source: std::io::Error },
    #[snafu(display("Read schema file error: {source}"))]
    ReadFile { source: std::io::Error },
    #[snafu(display("Desiralize toml error: {source}"))]
    DeserializeToml { source: toml::de::Error },
    #[snafu(display("Parse timestamp error: {source}"))]
    ParseDateTimeError { source: chrono::ParseError },
    #[snafu(display("Expected timestamp type"))]
    ExpectedTimestamp,
    #[snafu(display("Invalid timestamp"))]
    InvalidTimestamp,
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
pub struct UnsignedNumberSchema {
    fixed: Option<u64>,
    range: Option<UnsignedRangeSchema>,
}

impl UnsignedNumberSchema {
    fn check(&self) -> Result<()> {
        ensure!(
            self.fixed.is_some() || self.range.is_some(),
            ExpectedLengthSnafu
        );
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct UnsignedRangeSchema {
    min: Option<u64>,
    max: Option<u64>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct SignedNumberSchema {
    fixed: Option<i64>,
    range: Option<SignedRangeSchema>,
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

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct TimestampSchema {
    start_time: Option<TimestampValue>,
    precision: Option<TimestampPrecision>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(untagged)]
pub enum TimestampValue {
    Integer(i64),
    DateTime(toml::Value),
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub enum TimestampPrecision {
    #[serde(rename = "s")]
    Second,
    #[serde(rename = "ms")]
    MilliSecond,
    #[serde(rename = "ns")]
    Nanosecond,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct FloatSchema {
    fixed: Option<f64>,
    range: Option<FloatRangeSchema>,
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

pub struct DataFaker {
    ts: OnceLock<AtomicI64>,
    schema: DataFakeSchema,
}

impl DataFaker {
    pub async fn from_toml(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = tokio::fs::File::open(path).await.context(OpenFileSnafu)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf).await.context(ReadFileSnafu)?;

        Ok(Self {
            ts: OnceLock::new(),
            schema: toml::from_str(&buf).context(DeserializeTomlSnafu)?,
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
        length.check()?;
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
        length.check()?;
        let len = self.rand_unsigned_number(length)?.as_u64().unwrap() as usize;
        let mut rng = rand::thread_rng();
        let value: String = match charset {
            Some(charset) => charset
                .as_bytes()
                .choose_multiple(&mut rng, len)
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

    fn rand_unsigned_number(
        &self,
        UnsignedNumberSchema { fixed, range }: &UnsignedNumberSchema,
    ) -> Result<json::Value> {
        if let Some(fixed) = fixed {
            return Ok(json::json!(fixed));
        }

        if let Some(UnsignedRangeSchema { min, max }) = range {
            let (min, max) = (min.unwrap_or(u64::MIN), max.unwrap_or(u64::MAX));
            return Ok(json::json!(rand::thread_rng().gen_range(min..=max)));
        }

        Ok(json::json!(rand::random::<u64>()))
    }

    fn rand_sign_number(
        &self,
        SignedNumberSchema { fixed, range }: &SignedNumberSchema,
    ) -> Result<json::Value> {
        if let Some(fixed) = fixed {
            return Ok(json::json!(fixed));
        }

        if let Some(SignedRangeSchema { min, max }) = range {
            let (min, max) = (min.unwrap_or(i64::MIN), max.unwrap_or(i64::MAX));
            return Ok(json::json!(rand::thread_rng().gen_range(min..=max)));
        }

        Ok(json::json!(rand::random::<i64>()))
    }

    fn rand_float(&self, FloatSchema { fixed, range }: &FloatSchema) -> Result<json::Value> {
        if let Some(fixed) = fixed {
            return Ok(json::json!(fixed));
        }

        if let Some(FloatRangeSchema { min, max }) = range {
            let (min, max) = (min.unwrap_or(f64::MIN), max.unwrap_or(f64::MAX));
            return Ok(json::json!(rand::thread_rng().gen_range(min..=max)));
        }

        Ok(json::json!(rand::random::<f64>()))
    }

    fn get_timestamp(
        &self,
        TimestampSchema {
            start_time,
            precision,
        }: &TimestampSchema,
    ) -> Result<json::Value> {
        let precision = precision
            .as_ref()
            .unwrap_or(&TimestampPrecision::Nanosecond);
        let ts = match start_time {
            Some(value) => match value {
                TimestampValue::Integer(value) => self.ts.get_or_init(|| AtomicI64::new(*value)),
                TimestampValue::DateTime(value) => {
                    let toml::Value::Datetime(value) = value else {
                        return ExpectedTimestampSnafu.fail();
                    };
                    ensure!(value.date.is_some(), InvalidTimestampSnafu);
                    let date = value.date.unwrap();
                    let time = value.time.unwrap();
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
                            .and_local_timezone(
                                FixedOffset::east_opt((minutes as i32) * 60).unwrap(),
                            )
                            .unwrap()
                            .to_utc(),
                        None => dt.and_local_timezone(Local).unwrap().to_utc(),
                    };

                    let value = match precision {
                        TimestampPrecision::Second => dt.timestamp(),
                        TimestampPrecision::MilliSecond => dt.timestamp_millis(),
                        TimestampPrecision::Nanosecond => dt.timestamp_nanos_opt().unwrap(),
                    };
                    self.ts.get_or_init(|| AtomicI64::new(value))
                }
            },
            None => {
                let now = Local::now();
                let value = match precision {
                    TimestampPrecision::Second => now.timestamp(),
                    TimestampPrecision::MilliSecond => now.timestamp_millis(),
                    TimestampPrecision::Nanosecond => now.timestamp_nanos_opt().unwrap(),
                };
                self.ts.get_or_init(|| AtomicI64::new(value))
            }
        };

        Ok(json::json!(ts.fetch_add(1, atomic::Ordering::Relaxed)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn parse_test() {
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
                    length: Some(UnsignedNumberSchema {
                        fixed: None,
                        range: Some(UnsignedRangeSchema {
                            min: None,
                            max: Some(999)
                        })
                    })
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "number"
                fixed = 5
                range = { min = 999 }
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.number();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &SignedNumberSchema {
                    fixed: Some(5),
                    range: Some(SignedRangeSchema {
                        min: Some(999),
                        max: None
                    })
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "float"
                fixed = 5.8
                range = { min = 999 }
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.float();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &FloatSchema {
                    fixed: Some(5.8),
                    range: Some(FloatRangeSchema {
                        min: Some(999.0),
                        max: None
                    })
                }
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
                    length: UnsignedNumberSchema {
                        fixed: None,
                        range: Some(UnsignedRangeSchema {
                            min: None,
                            max: Some(999)
                        })
                    }
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
                precision = "ns" 
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
                    precision: Some(TimestampPrecision::Nanosecond)
                }
            )
        }
        {
            let schema = toml::from_str::<DataFakeSchema>(
                r#"
                type = "timestamp"
                start_time = 2024-11-02T17:35:34
                precision = "ns" 
            "#,
            );
            assert!(schema.is_ok());
            let schema = schema.unwrap();
            let schema = schema.timestamp();
            assert!(schema.is_some());
            assert_eq!(
                schema.unwrap(),
                &TimestampSchema {
                    start_time: Some(TimestampValue::DateTime(toml::Value::Datetime(
                        toml::value::Datetime::from_str("2024-11-02T17:35:34").unwrap()
                    ))),
                    precision: Some(TimestampPrecision::Nanosecond)
                }
            )
        }
    }
}
