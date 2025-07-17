pub mod message_type;
mod pb;
pub mod topic;

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{self, AtomicU64, AtomicU8},
        Arc, LazyLock,
    },
};

use faststr::FastStr;
use prost::Message;
use snafu::ResultExt;

use crate::fake_json::{self, BoolSchema, NumberSchema, StringSchema, TimestampSchema};

pub const NAMESPACE: &str = "spBv1.0";

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("read schema file error"))]
    ReadFile { source: std::io::Error },
    #[snafu(display("parse toml error"))]
    ParseToml { source: toml::de::Error },
    #[snafu(display("generate fake value error"))]
    FakeJson { source: fake_json::Error },
}

type Result<T> = std::result::Result<T, Error>;

macro_rules! get_rand_value {
    ($field: expr) => {{
        $field
            .as_ref()
            .map(|v| v.rand_value())
            .transpose()
            .context(FakeJsonSnafu)?
    }};
}

#[derive(Debug, serde::Deserialize)]
pub struct Schema {
    pub group_id: FastStr,
    pub timestamp: Arc<TimestampSchema>,
    pub node_devices: Vec<NodeDeviceSchema>,
}

impl Schema {
    pub async fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let s = tokio::fs::read_to_string(path)
            .await
            .context(ReadFileSnafu)?;
        toml::from_str(&s).context(ParseTomlSnafu)
    }

    pub fn node_devices(self) -> HashMap<FastStr, (Option<NodeDeviceFaker>, Vec<NodeDeviceFaker>)> {
        let mut ret = HashMap::new();
        for node_device in self.node_devices {
            let node = node_device.node;
            match node_device.device {
                Some(device) => {
                    let node_devices: &mut (Option<NodeDeviceFaker>, Vec<NodeDeviceFaker>) =
                        ret.entry(node.clone()).or_default();
                    node_devices.1.push(NodeDeviceFaker::new_device(
                        self.group_id.clone(),
                        self.timestamp.clone(),
                        node,
                        device,
                        node_device.metrics,
                    ));
                }
                None => {
                    let node_devices: &mut (Option<NodeDeviceFaker>, Vec<NodeDeviceFaker>) =
                        ret.entry(node.clone()).or_default();
                    node_devices.0 = Some(NodeDeviceFaker::new_node(
                        self.group_id.clone(),
                        self.timestamp.clone(),
                        node,
                        node_device.metrics,
                    ));
                }
            }
        }

        ret
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct NodeDeviceSchema {
    node: FastStr,
    device: Option<FastStr>,
    pub metrics: Arc<Vec<MetricSchema>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct MetricSchema {
    pub name: FastStr,
    pub is_historical: Option<BoolSchema>,
    pub is_transient: Option<BoolSchema>,
    pub value: ValueSchema,
    pub metadata: Option<MetadataSchema>,
    pub properties: Option<Properties>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Properties(HashMap<String, ValueSchema>);

impl Properties {
    fn pb_properties(&self) -> Result<pb::payload::PropertySet> {
        let mut keys = Vec::with_capacity(self.0.len());
        let mut values = Vec::with_capacity(self.0.len());
        for (key, value) in &self.0 {
            keys.push(key.clone());
            let datatype = value.pb_datatyep();
            let value = value.pb_property_value()?;
            values.push(pb::payload::PropertyValue {
                r#type: Some(datatype as _),
                is_null: Some(value.is_none()),
                value,
            });
        }
        Ok(pb::payload::PropertySet { keys, values })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct MetadataSchema {
    pub is_multi_part: Option<BoolSchema>,
    pub content_type: Option<StringSchema>,
    pub size: Option<NumberSchema<u64>>,
    pub seq: Option<NumberSchema<u64>>,
    pub file_name: Option<StringSchema>,
    pub file_type: Option<StringSchema>,
    pub md5: Option<StringSchema>,
    pub description: Option<StringSchema>,
}

impl MetadataSchema {
    fn pb_value(&self) -> Result<pb::payload::MetaData> {
        Ok(pb::payload::MetaData {
            is_multi_part: get_rand_value!(self.is_multi_part),
            content_type: get_rand_value!(self.content_type),
            size: get_rand_value!(self.size),
            seq: get_rand_value!(self.seq),
            file_name: get_rand_value!(self.file_name),
            file_type: get_rand_value!(self.file_type),
            md5: get_rand_value!(self.md5),
            description: get_rand_value!(self.description),
        })
    }
}

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct OptionSchema {
    value: Box<ValueSchema>,
}

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum ValueSchema {
    Boolean(BoolSchema),
    Int8(NumberSchema<i8>),
    Int16(NumberSchema<i16>),
    Int32(NumberSchema<i32>),
    Int64(NumberSchema<i64>),
    UInt8(NumberSchema<u8>),
    UInt16(NumberSchema<u16>),
    UInt32(NumberSchema<u32>),
    UInt64(NumberSchema<u64>),
    Float(NumberSchema<f32>),
    Double(NumberSchema<f64>),
    String(StringSchema),
    DateTime(TimestampSchema),
    Option(OptionSchema),
}

impl ValueSchema {
    fn pb_datatyep(&self) -> pb::DataType {
        match self {
            ValueSchema::Boolean(_) => pb::DataType::Boolean,
            ValueSchema::Int8(_) => pb::DataType::Int8,
            ValueSchema::Int16(_) => pb::DataType::Int16,
            ValueSchema::Int32(_) => pb::DataType::Int32,
            ValueSchema::Int64(_) => pb::DataType::Int64,
            ValueSchema::UInt8(_) => pb::DataType::UInt8,
            ValueSchema::UInt16(_) => pb::DataType::UInt16,
            ValueSchema::UInt32(_) => pb::DataType::UInt32,
            ValueSchema::UInt64(_) => pb::DataType::UInt64,
            ValueSchema::Float(_) => pb::DataType::Float,
            ValueSchema::Double(_) => pb::DataType::Double,
            ValueSchema::String(_) => pb::DataType::String,
            ValueSchema::DateTime(_) => pb::DataType::DateTime,
            ValueSchema::Option(schema) => schema.value.pb_datatyep(),
        }
    }

    fn pb_metric_value(&self) -> Result<Option<pb::payload::metric::Value>> {
        use pb::payload::metric::Value;
        Ok(Some(match self {
            ValueSchema::Boolean(schema) => {
                Value::BooleanValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::Int8(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Int16(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Int32(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Int64(schema) => {
                Value::LongValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt8(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt16(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt32(schema) => {
                Value::LongValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt64(schema) => {
                Value::LongValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Float(schema) => {
                Value::FloatValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::Double(schema) => {
                Value::DoubleValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::String(schema) => {
                Value::StringValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::DateTime(schema) => {
                Value::LongValue(schema.next_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Option(schema) => {
                if rand::random() {
                    return schema.value.pb_metric_value();
                } else {
                    return Ok(None);
                }
            }
        }))
    }

    fn pb_property_value(&self) -> Result<Option<pb::payload::property_value::Value>> {
        use pb::payload::property_value::Value;
        Ok(Some(match self {
            ValueSchema::Boolean(schema) => {
                Value::BooleanValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::Int8(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Int16(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Int32(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Int64(schema) => {
                Value::LongValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt8(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt16(schema) => {
                Value::IntValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt32(schema) => {
                Value::LongValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::UInt64(schema) => {
                Value::LongValue(schema.rand_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Float(schema) => {
                Value::FloatValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::Double(schema) => {
                Value::DoubleValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::String(schema) => {
                Value::StringValue(schema.rand_value().context(FakeJsonSnafu)?)
            }
            ValueSchema::DateTime(schema) => {
                Value::LongValue(schema.next_value().context(FakeJsonSnafu)? as _)
            }
            ValueSchema::Option(schema) => {
                if rand::random() {
                    return schema.value.pb_property_value();
                } else {
                    return Ok(None);
                }
            }
        }))
    }
}

#[derive(Clone)]
pub struct NodeDeviceFaker {
    pub group_id: FastStr,
    pub timestamp: Arc<TimestampSchema>,
    pub bd_seq_id: Arc<SeqId>,
    pub seq_id: Arc<SeqId>,
    pub node_id: FastStr,
    pub device_id: Option<FastStr>,
    pub metrics: Arc<Vec<MetricSchema>>,
    pub metric_alias: Option<HashMap<FastStr, u64>>,
    pub alias_generator: Arc<LazyLock<AtomicU64>>,
}

impl NodeDeviceFaker {
    pub fn new_node(
        group_id: FastStr,
        start_time: Arc<TimestampSchema>,
        node_id: FastStr,
        metrics: Arc<Vec<MetricSchema>>,
    ) -> Self {
        Self {
            group_id: group_id.clone(),
            timestamp: start_time.clone(),
            bd_seq_id: Arc::new(SeqId::default()),
            seq_id: Arc::new(SeqId::default()),
            node_id,
            device_id: None,
            metrics,
            metric_alias: None,
            alias_generator: Arc::new(LazyLock::new(AtomicU64::default)),
        }
    }
    pub fn new_device(
        group_id: FastStr,
        start_time: Arc<TimestampSchema>,
        node_id: FastStr,
        device_id: FastStr,
        metrics: Arc<Vec<MetricSchema>>,
    ) -> Self {
        Self {
            group_id: group_id.clone(),
            timestamp: start_time.clone(),
            bd_seq_id: Arc::new(SeqId::default()),
            seq_id: Arc::new(SeqId::default()),
            node_id,
            device_id: Some(device_id),
            metrics,
            metric_alias: None,
            alias_generator: Arc::new(LazyLock::new(AtomicU64::default)),
        }
    }

    pub fn display_id(&self) -> String {
        match &self.device_id {
            Some(device_id) => format!("{}/{}", self.node_id, device_id),
            None => self.node_id.to_string(),
        }
    }

    pub fn ncmd_topic(&self) -> String {
        format!("{NAMESPACE}/{}/NCMD/{}", self.group_id, self.node_id)
    }

    pub fn birth_topic(&self) -> String {
        self.gen_topic("BIRTH")
    }

    pub fn death_topic(&self) -> String {
        self.gen_topic("DEATH")
    }

    pub fn data_topic(&self) -> String {
        self.gen_topic("DATA")
    }

    fn gen_topic(&self, message_type: &str) -> String {
        let group_id = &self.group_id;
        let node_id = &self.node_id;
        match &self.device_id {
            Some(device_id) => {
                format!("{NAMESPACE}/{group_id}/D{message_type}/{node_id}/{device_id}")
            }
            None => format!("{NAMESPACE}/{group_id}/N{message_type}/{node_id}"),
        }
    }

    pub fn birth_payload(&mut self) -> Result<Vec<u8>> {
        let timestamp = self.timestamp.next_value().context(FakeJsonSnafu)? as u64;
        let mut metrics = match self.device_id {
            Some(_) => vec![],
            None => {
                vec![
                    pb::payload::Metric {
                        name: Some("bdSeq".into()),
                        alias: Some(self.get_alias(&"bdSeq".into())),
                        timestamp: Some(timestamp),
                        datatype: Some(pb::DataType::UInt64 as _),
                        value: Some(pb::payload::metric::Value::LongValue(
                            self.bd_seq_id.next() as _
                        )),
                        ..Default::default()
                    },
                    pb::payload::Metric {
                        name: Some("Node Control/Rebirth".into()),
                        alias: Some(self.get_alias(&"Node Control/Rebirth".into())),
                        timestamp: Some(timestamp),
                        datatype: Some(pb::DataType::Boolean as _),
                        value: Some(pb::payload::metric::Value::BooleanValue(false)),
                        ..Default::default()
                    },
                ]
            }
        };
        let custom_metrics = self.metric_values(true, timestamp)?;
        metrics.extend(custom_metrics);
        for metric in &metrics {
            println!(
                "name: {}, alias: {}",
                metric.name.as_ref().unwrap(),
                metric.alias.unwrap()
            );
        }
        let payload = pb::Payload {
            timestamp: Some(timestamp),
            metrics,
            seq: Some(self.seq_id.next() as _),
            ..Default::default()
        };
        Ok(payload.encode_to_vec())
    }

    pub fn death_payload(&self) -> Result<Vec<u8>> {
        let timestamp = self.timestamp.next_value().context(FakeJsonSnafu)? as u64;
        let payload = match self.device_id {
            Some(_) => pb::Payload {
                timestamp: Some(timestamp),
                metrics: vec![],
                seq: Some(self.seq_id.next() as _),
                ..Default::default()
            },
            None => pb::Payload {
                timestamp: Some(timestamp),
                metrics: vec![pb::payload::Metric {
                    name: Some("bdSeq".into()),
                    timestamp: Some(timestamp),
                    datatype: Some(pb::DataType::UInt64 as _),
                    value: Some(pb::payload::metric::Value::LongValue(
                        self.bd_seq_id.get() as _
                    )),
                    ..Default::default()
                }],
                seq: Some(self.seq_id.next() as _),
                ..Default::default()
            },
        };
        Ok(payload.encode_to_vec())
    }

    pub fn data_payload(&mut self) -> Result<Vec<u8>> {
        let timestamp = self.timestamp.next_value().context(FakeJsonSnafu)? as u64;
        Ok(pb::Payload {
            timestamp: Some(timestamp),
            metrics: self.metric_values(false, timestamp)?,
            seq: Some(self.seq_id.next() as _),
            ..Default::default()
        }
        .encode_to_vec())
    }

    fn metric_values(
        &mut self,
        is_birth: bool,
        timestamp: u64,
    ) -> Result<Vec<pb::payload::Metric>> {
        let mut ret = Vec::with_capacity(self.metrics.len());
        for schema in self.metrics.clone().iter() {
            let name = &schema.name;
            let alias = self.get_alias(name);
            let datatype = schema.value.pb_datatyep() as u32;
            let value = schema.value.pb_metric_value()?;
            ret.push(pb::payload::Metric {
                name: is_birth.then_some(name.to_string()),
                alias: Some(alias),
                timestamp: Some(timestamp),
                datatype: Some(datatype),
                is_historical: get_rand_value!(schema.is_historical),
                is_transient: get_rand_value!(schema.is_transient),
                is_null: Some(value.is_none()),
                metadata: schema.metadata.as_ref().map(|v| v.pb_value()).transpose()?,
                properties: schema
                    .properties
                    .as_ref()
                    .map(|v| v.pb_properties())
                    .transpose()?,
                value,
            });
        }
        Ok(ret)
    }

    fn get_alias(&mut self, name: &FastStr) -> u64 {
        match self.metric_alias.as_mut() {
            Some(map) => match map.get_mut(name) {
                Some(alias) => *alias,
                None => {
                    let alias = self.alias_generator.fetch_add(1, atomic::Ordering::Relaxed);
                    map.insert(name.clone(), alias);
                    alias
                }
            },
            None => {
                let map = self.metric_alias.get_or_insert_default();
                let alias = self.alias_generator.fetch_add(1, atomic::Ordering::Relaxed);
                map.insert(name.clone(), alias);
                alias
            }
        }
    }
}

#[derive(Default)]
pub struct SeqId(AtomicU8);

impl SeqId {
    pub fn next(&self) -> u8 {
        self.0.fetch_add(1, atomic::Ordering::Relaxed)
    }

    pub fn get(&self) -> u8 {
        self.0.load(atomic::Ordering::Acquire)
    }
}
