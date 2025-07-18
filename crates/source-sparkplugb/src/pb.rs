use std::collections::HashMap;

use faststr::FastStr;
use prost::Message;
use snafu::{OptionExt, ResultExt};

use super::proto::{
    self,
    payload::{metric, property_value},
};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("unsupported datatype: {datatype}"))]
    UnsupportedDataType { datatype: u32 },
    #[snafu(display("datatype and value missmatch: {datatype}"))]
    DataTypeValueMissMatch { datatype: u32 },
    #[snafu(display("serialize metadata error"))]
    SerializeMetadata { source: serde_json::Error },
    #[snafu(display("serialize properties error"))]
    SerializeProperties { source: serde_json::Error },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Payload {
    pub timestamp: Option<u64>,
    pub metrics: Vec<Metric>,
    pub seq: Option<u64>,
}

impl TryFrom<proto::Payload> for Payload {
    type Error = Error;

    fn try_from(payload: proto::Payload) -> Result<Self> {
        let metrics = payload
            .metrics
            .into_iter()
            .map(|metric| metric.try_into())
            .collect::<Result<_>>()?;
        Ok(Self {
            timestamp: payload.timestamp,
            metrics,
            seq: payload.seq,
        })
    }
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct Template {
    pub version: Option<String>,
    pub metrics: Vec<Metric>,
    pub parameters: Vec<Parameter>,
    pub template_ref: Option<String>,
    pub is_definition: Option<bool>,
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct Parameter {
    pub name: Option<String>,
    pub r#type: Option<u32>,
    pub value: Option<Value>,
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct DataSet {
    pub num_of_columns: Option<u64>,
    pub columns: Vec<String>,
    pub types: Vec<u32>,
    pub rows: Vec<Row>,
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct DataSetValue {
    pub value: Option<Value>,
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct Row {
    pub elements: Vec<DataSetValue>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PropertyValue {
    pub r#type: Option<u32>,
    pub is_null: Option<bool>,
    #[serde(flatten)]
    pub value: Option<Value>,
}

impl TryFrom<proto::payload::PropertyValue> for PropertyValue {
    type Error = Error;

    fn try_from(value: proto::payload::PropertyValue) -> Result<Self> {
        Ok(Self {
            r#type: value.r#type,
            is_null: value.is_null,
            value: match (value.r#type, value.value) {
                (Some(datatype), Some(value)) => Some((datatype, value).try_into()?),
                _ => None,
            },
        })
    }
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct PropertySet {
    pub keys: Vec<FastStr>,
    pub values: Vec<PropertyValue>,
}

#[derive(Clone, PartialEq, serde::Serialize)]
pub struct PropertySetList {
    pub propertyset: Vec<PropertySet>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MetaData {
    pub is_multi_part: Option<bool>,
    pub content_type: Option<FastStr>,
    pub size: Option<u64>,
    pub seq: Option<u64>,
    pub file_name: Option<FastStr>,
    pub file_type: Option<FastStr>,
    pub md5: Option<FastStr>,
    pub description: Option<FastStr>,
}

impl TryFrom<proto::payload::MetaData> for MetaData {
    type Error = Error;

    fn try_from(metadata: proto::payload::MetaData) -> Result<Self> {
        Ok(Self {
            is_multi_part: metadata.is_multi_part,
            content_type: metadata.content_type.map(FastStr::from),
            size: metadata.size,
            seq: metadata.seq,
            file_name: metadata.file_name.map(FastStr::from),
            file_type: metadata.file_type.map(FastStr::from),
            md5: metadata.md5.map(FastStr::from),
            description: metadata.description.map(FastStr::from),
        })
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Metric {
    pub name: Option<FastStr>,
    pub alias: Option<u64>,
    pub timestamp: Option<u64>,
    pub datatype: Option<u32>,
    pub is_historical: Option<bool>,
    pub is_transient: Option<bool>,
    pub is_null: Option<bool>,
    pub metadata: Option<String>,
    pub properties: Option<String>,
    #[serde(flatten)]
    pub value: Option<Value>,
}

impl TryFrom<proto::payload::Metric> for Metric {
    type Error = Error;

    fn try_from(metric: proto::payload::Metric) -> Result<Self> {
        let properties: Option<HashMap<FastStr, PropertyValue>> = match metric.properties {
            Some(props) => {
                let mut ret = HashMap::with_capacity(props.keys.len());
                let mut keys = props.keys.into_iter();
                let mut values = props.values.into_iter();

                while let (Some(key), Some(value)) = (keys.next(), values.next()) {
                    ret.insert(FastStr::from(key), value.try_into()?);
                }
                Some(ret)
            }
            None => None,
        };
        let properties_str = properties
            .map(|v| serde_json::to_string(&v))
            .transpose()
            .context(SerializePropertiesSnafu)?;
        let metadata: Option<MetaData> = metric.metadata.map(|v| v.try_into()).transpose()?;
        let metadata_str = metadata
            .map(|v| serde_json::to_string(&v))
            .transpose()
            .context(SerializeMetadataSnafu)?;
        let value = {
            match (metric.datatype, metric.value) {
                (Some(datatype), Some(metric)) => Some((datatype, metric).try_into()?),
                _ => None,
            }
        };
        Ok(Self {
            name: metric.name.map(FastStr::from),
            alias: metric.alias,
            timestamp: metric.timestamp,
            datatype: metric.datatype,
            is_historical: metric.is_historical,
            is_transient: metric.is_transient,
            is_null: metric.is_null,
            metadata: metadata_str,
            properties: properties_str,
            value,
        })
    }
}

pub fn rebirth_payload() -> Vec<u8> {
    let timestamp = Some(chrono::Utc::now().timestamp_millis() as u64);
    proto::Payload {
        timestamp,
        metrics: vec![proto::payload::Metric {
            name: Some(String::from("Node Control/Rebirth")),
            timestamp,
            datatype: Some(11),
            value: Some(proto::payload::metric::Value::BooleanValue(true)),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec()
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "datatype_str", content = "value")]
pub enum Value {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(FastStr),
    DateTime(u64),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int8(_) => "Int8",
            Value::Int16(_) => "Int16",
            Value::Int32(_) => "Int32",
            Value::Int64(_) => "Int64",
            Value::UInt8(_) => "UInt8",
            Value::UInt16(_) => "UInt16",
            Value::UInt32(_) => "UInt32",
            Value::UInt64(_) => "UInt64",
            Value::Float(_) => "Float",
            Value::Double(_) => "Double",
            Value::Boolean(_) => "Boolean",
            Value::String(_) => "String",
            Value::DateTime(_) => "DateTime",
        }
    }
}

impl TryFrom<(u32, metric::Value)> for Value {
    type Error = Error;

    fn try_from((datatype, value): (u32, metric::Value)) -> Result<Self> {
        let data_type =
            proto::DataType::from_u32(datatype).context(UnsupportedDataTypeSnafu { datatype })?;
        Ok(match (data_type, value) {
            (proto::DataType::Int8, metric::Value::IntValue(v)) => Value::Int8(v as _),
            (proto::DataType::Int16, metric::Value::IntValue(v)) => Value::Int16(v as _),
            (proto::DataType::Int32, metric::Value::IntValue(v)) => Value::Int32(v as _),
            (proto::DataType::Int64, metric::Value::LongValue(v)) => Value::Int64(v as _),
            (proto::DataType::UInt8, metric::Value::IntValue(v)) => Value::UInt8(v as _),
            (proto::DataType::UInt16, metric::Value::IntValue(v)) => Value::UInt16(v as _),
            (proto::DataType::UInt32, metric::Value::LongValue(v)) => Value::UInt32(v as _),
            (proto::DataType::UInt64, metric::Value::LongValue(v)) => Value::UInt64(v),
            (proto::DataType::Float, metric::Value::FloatValue(v)) => Value::Float(v),
            (proto::DataType::Double, metric::Value::DoubleValue(v)) => Value::Double(v),
            (proto::DataType::Boolean, metric::Value::BooleanValue(v)) => Value::Boolean(v),
            (proto::DataType::String, metric::Value::StringValue(v)) => Value::String(v.into()),
            (proto::DataType::DateTime, metric::Value::LongValue(v)) => Value::DateTime(v),
            (
                proto::DataType::Int8
                | proto::DataType::Int16
                | proto::DataType::Int32
                | proto::DataType::Int64
                | proto::DataType::UInt8
                | proto::DataType::UInt16
                | proto::DataType::UInt32
                | proto::DataType::UInt64
                | proto::DataType::Float
                | proto::DataType::Double
                | proto::DataType::Boolean
                | proto::DataType::String
                | proto::DataType::DateTime,
                _,
            ) => return DataTypeValueMissMatchSnafu { datatype }.fail(),
            _ => return UnsupportedDataTypeSnafu { datatype }.fail(),
        })
    }
}

impl TryFrom<(u32, property_value::Value)> for Value {
    type Error = Error;

    fn try_from((datatype, value): (u32, property_value::Value)) -> Result<Self> {
        let data_type =
            proto::DataType::from_u32(datatype).context(UnsupportedDataTypeSnafu { datatype })?;
        Ok(match (data_type, value) {
            (proto::DataType::Int8, property_value::Value::IntValue(v)) => Self::Int8(v as _),
            (proto::DataType::Int16, property_value::Value::IntValue(v)) => Self::Int16(v as _),
            (proto::DataType::Int32, property_value::Value::IntValue(v)) => Self::Int32(v as _),
            (proto::DataType::Int64, property_value::Value::LongValue(v)) => Self::Int64(v as _),
            (proto::DataType::UInt8, property_value::Value::IntValue(v)) => Self::UInt8(v as _),
            (proto::DataType::UInt16, property_value::Value::IntValue(v)) => Self::UInt16(v as _),
            (proto::DataType::UInt32, property_value::Value::LongValue(v)) => Self::UInt32(v as _),
            (proto::DataType::UInt64, property_value::Value::LongValue(v)) => Self::UInt64(v),
            (proto::DataType::Float, property_value::Value::FloatValue(v)) => Self::Float(v),
            (proto::DataType::Double, property_value::Value::DoubleValue(v)) => Self::Double(v),
            (proto::DataType::Boolean, property_value::Value::BooleanValue(v)) => Self::Boolean(v),
            (proto::DataType::String, property_value::Value::StringValue(v)) => {
                Self::String(v.into())
            }
            (proto::DataType::DateTime, property_value::Value::LongValue(v)) => Self::DateTime(v),
            (
                proto::DataType::Int8
                | proto::DataType::Int16
                | proto::DataType::Int32
                | proto::DataType::Int64
                | proto::DataType::UInt8
                | proto::DataType::UInt16
                | proto::DataType::UInt32
                | proto::DataType::UInt64
                | proto::DataType::Float
                | proto::DataType::Double
                | proto::DataType::Boolean
                | proto::DataType::String
                | proto::DataType::DateTime,
                _,
            ) => return DataTypeValueMissMatchSnafu { datatype }.fail(),
            _ => return UnsupportedDataTypeSnafu { datatype }.fail(),
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn serialize_test() {
        let paylaod = Payload {
            timestamp: Some(12345),
            metrics: vec![Metric {
                name: Some("metric_1".into()),
                alias: Some(1),
                timestamp: Some(12345),
                datatype: Some(1),
                is_historical: Some(true),
                is_transient: Some(false),
                is_null: Some(false),
                metadata: Some(
                    serde_json::to_string(&MetaData {
                        is_multi_part: Some(true),
                        content_type: Some("application/json".into()),
                        size: Some(10),
                        seq: Some(1),
                        file_name: Some("file1".into()),
                        file_type: Some("fileA".into()),
                        md5: Some("aabc".into()),
                        description: Some("description".into()),
                    })
                    .unwrap(),
                ),
                properties: Some(
                    serde_json::to_string::<HashMap<_, _>>(&HashMap::<_, _, _>::from_iter([(
                        "prop1".to_string(),
                        PropertyValue {
                            r#type: Some(1),
                            is_null: Some(false),
                            value: Some(Value::Int8(98)),
                        },
                    )]))
                    .unwrap(),
                ),
                value: Some(Value::Int8(99)),
            }],
            seq: Some(1),
        };
        let a = serde_json::to_string(&paylaod).unwrap();
        assert_eq!(
            a,
            "{\"timestamp\":12345,\"metrics\":[{\"name\":\"metric_1\",\"alias\":1,\"timestamp\":12345,\"datatype\":1,\"is_historical\":true,\"is_transient\":false,\"is_null\":false,\"metadata\":\"{\\\"is_multi_part\\\":true,\\\"content_type\\\":\\\"application/json\\\",\\\"size\\\":10,\\\"seq\\\":1,\\\"file_name\\\":\\\"file1\\\",\\\"file_type\\\":\\\"fileA\\\",\\\"md5\\\":\\\"aabc\\\",\\\"description\\\":\\\"description\\\"}\",\"properties\":\"{\\\"prop1\\\":{\\\"type\\\":1,\\\"is_null\\\":false,\\\"datatype_str\\\":\\\"Int8\\\",\\\"value\\\":98}}\",\"datatype_str\":\"Int8\",\"value\":99}],\"seq\":1}"
        );
    }
}
