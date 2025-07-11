use std::{collections::HashMap, sync::Arc};

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMillisecondBuilder,
    UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use faststr::FastStr;
use prost::Message;
use serde_with::{DisplayFromStr, serde_as};
use snafu::{OptionExt, ResultExt};

use crate::config::MessageType;
use source_mqtt::client::Message as MqttMessage;

use super::{
    pb, proto,
    topic::{self, TopicComponents},
    variables::{self, *},
};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("parse variables error"))]
    ParseVariable {
        source: variables::Error,
    },
    #[snafu(display("unsupported metric datatype: {datatype}"))]
    UnsupportedMetricDataType {
        datatype: u32,
    },
    #[snafu(display("unsupported arrow datatype: {datatype}"))]
    UnsupportedArrowDataType {
        datatype: arrow::datatypes::DataType,
    },
    #[snafu(display("arrow builder conflict with schema"))]
    BuilderMissMatch,
    #[snafu(display("serialize metric metadata error"))]
    SerializeMetadata {
        source: serde_json::Error,
    },
    #[snafu(display("serialize metric properties error"))]
    SerializeProperties {
        source: serde_json::Error,
    },
    #[snafu(display("parse topic error"))]
    ParseTopic {
        source: topic::Error,
    },
    #[snafu(display("deserialize STATE payload error"))]
    DeserializeState {
        source: serde_json::Error,
    },
    #[snafu(display("deserialize metric paylaod error"))]
    DeserializePayload {
        source: prost::DecodeError,
    },
    #[snafu(display("parse proto payload error"))]
    ParsePayload {
        source: pb::Error,
    },
    #[snafu(display("metric name or alias not found"))]
    MissingMetricNameOrAlias,
    BuildRecordBatch {
        source: arrow::error::ArrowError,
    },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct State {
    timestamp: i64,
    online: bool,
}

#[serde_as]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchState {
    pub namespace: FastStr,
    pub group_id: FastStr,
    #[serde_as(as = "DisplayFromStr")]
    pub message_type: MessageType,
    pub edge_node_id: FastStr,
    pub device_id: Option<FastStr>,
    #[serde(flatten)]
    pub state: State,
}

impl BatchState {
    pub fn new(
        TopicComponents {
            namespace,
            group_id,
            message_type,
            edge_node_id,
            device_id,
        }: &TopicComponents,
        state: State,
    ) -> Self {
        Self {
            namespace: namespace.clone(),
            group_id: group_id.clone(),
            message_type: *message_type,
            edge_node_id: edge_node_id.clone(),
            device_id: device_id.clone(),
            state,
        }
    }
}

#[derive(Debug)]
enum Payload {
    Metric(pb::Payload),
    State(State),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum BatchPayload {
    Metric(BatchMetric),
    State(BatchState),
}

#[serde_as]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchMetric {
    pub namespace: FastStr,
    pub group_id: FastStr,
    #[serde_as(as = "DisplayFromStr")]
    pub message_type: MessageType,
    pub edge_node_id: FastStr,
    pub device_id: Option<FastStr>,
    pub payload_ts: Option<u64>,
    pub payload_seq: Option<u64>,
    #[serde(flatten)]
    pub metric: Option<pb::Metric>,
}

impl BatchMetric {
    pub fn new(
        TopicComponents {
            namespace,
            group_id,
            message_type,
            edge_node_id,
            device_id,
        }: &TopicComponents,
        payload_ts: Option<u64>,
        payload_seq: Option<u64>,
        metric: Option<pb::Metric>,
    ) -> Result<(Arc<Schema>, Self)> {
        let mut fields = vec![
            Field::new(VAR_NAMESPACE, DataType::Utf8, false),
            Field::new(VAR_GROUP_ID, DataType::Utf8, false),
            Field::new(VAR_MESSAGE_TYPE, DataType::Utf8, false),
            Field::new(VAR_EDGE_NODE_ID, DataType::Utf8, false),
            Field::new(VAR_DEVICE_ID, DataType::Utf8, true),
            Field::new(
                VAR_PAYLOAD_TIMESTAMP,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(VAR_PAYLOAD_SEQ, DataType::UInt64, true),
        ];
        if let Some(metric) = metric.as_ref() {
            fields.extend_from_slice(&[
                Field::new(VAR_METRIC_NAME, DataType::Utf8, true),
                Field::new(VAR_METRIC_ALIAS, DataType::UInt64, true),
                Field::new(
                    VAR_METRIC_TIMESTAMP,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ]);
            if let Some(datatype) = metric.datatype {
                fields.extend_from_slice(&[
                    Field::new(VAR_METRIC_DATATYPE_STR, DataType::Utf8, true),
                    Field::new(VAR_METRIC_DATATYPE_INT, DataType::UInt32, true),
                ]);
                let data_type = proto::DataType::from_u32(datatype)
                    .context(UnsupportedMetricDataTypeSnafu { datatype })?;
                let arrow_datatype = match data_type {
                    proto::DataType::Int8 => DataType::Int8,
                    proto::DataType::Int16 => DataType::Int16,
                    proto::DataType::Int32 => DataType::Int32,
                    proto::DataType::Int64 => DataType::Int64,
                    proto::DataType::UInt8 => DataType::UInt8,
                    proto::DataType::UInt16 => DataType::UInt16,
                    proto::DataType::UInt32 => DataType::UInt32,
                    proto::DataType::UInt64 => DataType::UInt64,
                    proto::DataType::Float => DataType::Float32,
                    proto::DataType::Double => DataType::Float64,
                    proto::DataType::Boolean => DataType::Boolean,
                    proto::DataType::String => DataType::Utf8,
                    proto::DataType::DateTime => DataType::Timestamp(TimeUnit::Millisecond, None),
                    _ => return UnsupportedMetricDataTypeSnafu { datatype }.fail(),
                };
                fields.push(Field::new(VAR_METRIC_VALUE, arrow_datatype, true));
            }
            fields.extend_from_slice(&[
                Field::new(VAR_METRIC_IS_HISTORICAL, DataType::Boolean, true),
                Field::new(VAR_METRIC_IS_TRANSIENT, DataType::Boolean, true),
                Field::new(VAR_METRIC_IS_NULL, DataType::Boolean, true),
            ]);
            // metadata 和 properties 字段直接序列化为 json 字符串交给 transformer 处理
            fields.extend_from_slice(&[
                Field::new(VAR_METRIC_METADATA, DataType::Utf8, true),
                Field::new(VAR_METRIC_PROPERTIES, DataType::Utf8, true),
            ]);
        }

        let metadata = HashMap::<_, _, _>::from_iter(
            [("version", "1.0"), ("stream", "flat"), ("ack", "lush")]
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );

        Ok((
            Arc::new(Schema::new_with_metadata(fields, metadata)),
            Self {
                namespace: namespace.clone(),
                group_id: group_id.clone(),
                message_type: *message_type,
                edge_node_id: edge_node_id.clone(),
                device_id: device_id.clone(),
                payload_ts,
                payload_seq,
                metric,
            },
        ))
    }

    pub fn fill_metric_name(&mut self, map: &HashMap<u64, FastStr>) -> Result<bool> {
        let Some(metric) = self.metric.as_mut() else {
            return Ok(true);
        };
        if metric.name.is_some() {
            return Ok(true);
        }

        let Some(alias) = metric.alias else {
            return MissingMetricNameOrAliasSnafu.fail();
        };

        if let Some(name) = map.get(&alias) {
            metric.name = Some(name.clone());
            return Ok(true);
        }

        Ok(false)
    }

    pub fn id(&self) -> FastStr {
        match &self.device_id {
            Some(device_id) => {
                format!("{}/{}/{device_id}", self.group_id, self.edge_node_id).into()
            }
            None => format!("{}/{}", self.group_id, self.edge_node_id).into(),
        }
    }

    pub fn rebirth_topic(&self) -> String {
        format!("{NAMESPACE}/{}/NCMD/{}", self.group_id, self.edge_node_id)
    }

    pub fn name_alias(&self) -> Option<(FastStr, u64)> {
        self.metric
            .as_ref()
            .and_then(|m| m.name.clone().zip(m.alias))
    }
}

#[derive(Debug)]
pub struct BatchEntry {
    pub topic: TopicComponents,
    payload: Payload,
}

impl TryFrom<MqttMessage> for BatchEntry {
    type Error = Error;

    fn try_from(value: MqttMessage) -> Result<Self> {
        let topic: TopicComponents = value.topic.parse().context(ParseTopicSnafu)?;
        let payload = match topic.message_type {
            MessageType::State => {
                let state =
                    serde_json::from_slice(&value.payload).context(DeserializeStateSnafu)?;
                Payload::State(state)
            }
            _ => {
                let payload =
                    proto::Payload::decode(value.payload).context(DeserializePayloadSnafu)?;
                Payload::Metric(payload.try_into().context(ParsePayloadSnafu)?)
            }
        };
        Ok(Self { topic, payload })
    }
}

impl BatchEntry {
    pub fn payloads(self) -> Result<Vec<(Arc<Schema>, BatchPayload)>> {
        Ok(match self.payload {
            Payload::Metric(payload) if payload.metrics.is_empty() => {
                let (schema, metric) =
                    BatchMetric::new(&self.topic, payload.timestamp, payload.seq, None)?;
                vec![(schema, BatchPayload::Metric(metric))]
            }
            Payload::Metric(payload) => {
                let mut ret = Vec::with_capacity(payload.metrics.len());
                for metric in payload.metrics {
                    let (schema, metric) = BatchMetric::new(
                        &self.topic,
                        payload.timestamp,
                        payload.seq,
                        Some(metric),
                    )?;
                    ret.push((schema, BatchPayload::Metric(metric)))
                }
                ret
            }
            Payload::State(state) => {
                let fields = vec![
                    Field::new(VAR_NAMESPACE, DataType::Utf8, false),
                    Field::new(VAR_GROUP_ID, DataType::Utf8, false),
                    Field::new(VAR_MESSAGE_TYPE, DataType::Utf8, false),
                    Field::new(VAR_EDGE_NODE_ID, DataType::Utf8, false),
                    Field::new(VAR_DEVICE_ID, DataType::Utf8, false),
                    Field::new(
                        VAR_PAYLOAD_TIMESTAMP,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    ),
                    Field::new(VAR_PAYLOAD_ONLINE, DataType::Utf8, true),
                ];
                let metadata = HashMap::<_, _, _>::from_iter(
                    [("version", "1.0"), ("stream", "flat"), ("ack", "lush")]
                        .map(|(k, v)| (k.to_string(), v.to_string())),
                );
                vec![(
                    Arc::new(Schema::new_with_metadata(fields, metadata)),
                    BatchPayload::State(BatchState::new(&self.topic, state)),
                )]
            }
        })
    }
}

pub struct BatchBuilder {
    schema: Arc<Schema>,
    fields: Vec<(Variable, Box<dyn ArrayBuilder>)>,
}

impl BatchBuilder {
    pub fn new(schema: Arc<Schema>) -> Result<Self> {
        let schema_fields = schema.fields();
        let mut fields = Vec::with_capacity(schema_fields.len());
        for field in schema_fields.into_iter() {
            let builder: Box<dyn ArrayBuilder> = match field.data_type() {
                DataType::Boolean => Box::new(BooleanBuilder::new()),
                DataType::Int8 => Box::new(Int8Builder::new()),
                DataType::Int16 => Box::new(Int16Builder::new()),
                DataType::Int32 => Box::new(Int32Builder::new()),
                DataType::Int64 => Box::new(Int64Builder::new()),
                DataType::UInt8 => Box::new(UInt8Builder::new()),
                DataType::UInt16 => Box::new(UInt16Builder::new()),
                DataType::UInt32 => Box::new(UInt32Builder::new()),
                DataType::UInt64 => Box::new(UInt64Builder::new()),
                DataType::Float32 => Box::new(Float32Builder::new()),
                DataType::Float64 => Box::new(Float64Builder::new()),
                DataType::Utf8 => Box::new(StringBuilder::new()),
                DataType::Timestamp(TimeUnit::Millisecond, None) => {
                    Box::new(TimestampMillisecondBuilder::new())
                }
                datatype => {
                    return UnsupportedArrowDataTypeSnafu {
                        datatype: datatype.clone(),
                    }
                    .fail();
                }
            };
            let name: Variable = field.name().parse().context(ParseVariableSnafu)?;
            fields.push((name, builder));
        }
        Ok(Self { schema, fields })
    }
}

impl BatchBuilder {
    pub fn build(mut self, entries: &[BatchPayload]) -> Result<RecordBatch> {
        for entry in entries {
            match entry {
                BatchPayload::Metric(metric) => {
                    for (field, builder) in self.fields.iter_mut() {
                        match field {
                            Variable::Namespace => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(&metric.namespace);
                            }
                            Variable::GroupId => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(&metric.group_id);
                            }
                            Variable::MessageType => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(metric.message_type.to_string());
                            }
                            Variable::EdgeNodeId => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(&metric.edge_node_id);
                            }
                            Variable::DeviceId => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_option(metric.device_id.as_ref());
                            }
                            Variable::PayloadTimestamp => {
                                get_builder::<TimestampMillisecondBuilder>(builder)?
                                    .append_option(metric.payload_ts.map(|v| v as i64));
                            }
                            Variable::PayloadSeq => {
                                get_builder::<UInt64Builder>(builder)?
                                    .append_option(metric.payload_seq);
                            }
                            Variable::MetricName => {
                                get_builder::<StringBuilder>(builder)?.append_option(
                                    metric.metric.as_ref().and_then(|v| v.name.as_ref()),
                                );
                            }
                            Variable::MetricAlias => {
                                get_builder::<UInt64Builder>(builder)?
                                    .append_option(metric.metric.as_ref().and_then(|v| v.alias));
                            }
                            Variable::MetricTimestamp => {
                                get_builder::<TimestampMillisecondBuilder>(builder)?.append_option(
                                    metric
                                        .metric
                                        .as_ref()
                                        .and_then(|v| v.timestamp.map(|v| v as i64)),
                                );
                            }
                            Variable::MetricDataTypeStr => {
                                get_builder::<StringBuilder>(builder)?.append_option(
                                    metric
                                        .metric
                                        .as_ref()
                                        .and_then(|v| v.value.as_ref().map(|v| v.type_name())),
                                );
                            }
                            Variable::MetricDataTypeInt => {
                                get_builder::<UInt32Builder>(builder)?
                                    .append_option(metric.metric.as_ref().and_then(|v| v.datatype));
                            }
                            Variable::MetricValue => {
                                let Some(datatype) = metric
                                    .metric
                                    .as_ref()
                                    .and_then(|v| v.datatype.and_then(proto::DataType::from_u32))
                                else {
                                    continue;
                                };

                                match metric.metric.as_ref().and_then(|v| v.value.as_ref()) {
                                    Some(value) => match value {
                                        pb::Value::Int8(v) => {
                                            get_builder::<Int8Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::Int16(v) => {
                                            get_builder::<Int16Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::Int32(v) => {
                                            get_builder::<Int32Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::Int64(v) => {
                                            get_builder::<Int64Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::UInt8(v) => {
                                            get_builder::<UInt8Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::UInt16(v) => {
                                            get_builder::<UInt16Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::UInt32(v) => {
                                            get_builder::<UInt32Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::UInt64(v) => {
                                            get_builder::<UInt64Builder>(builder)?.append_value(*v);
                                        }
                                        pb::Value::Float(v) => {
                                            get_builder::<Float32Builder>(builder)?
                                                .append_value(*v);
                                        }
                                        pb::Value::Double(v) => {
                                            get_builder::<Float64Builder>(builder)?
                                                .append_value(*v);
                                        }
                                        pb::Value::Boolean(v) => {
                                            get_builder::<BooleanBuilder>(builder)?
                                                .append_value(*v);
                                        }
                                        pb::Value::String(v) => {
                                            get_builder::<StringBuilder>(builder)?.append_value(v);
                                        }
                                        pb::Value::DateTime(v) => {
                                            get_builder::<TimestampMillisecondBuilder>(builder)?
                                                .append_value(*v as _);
                                        }
                                    },
                                    None => match datatype {
                                        proto::DataType::Int8 => {
                                            get_builder::<Int8Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::Int16 => {
                                            get_builder::<Int16Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::Int32 => {
                                            get_builder::<Int32Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::Int64 => {
                                            get_builder::<Int64Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::UInt8 => {
                                            get_builder::<UInt8Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::UInt16 => {
                                            get_builder::<UInt16Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::UInt32 => {
                                            get_builder::<UInt32Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::UInt64 => {
                                            get_builder::<UInt64Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::Float => {
                                            get_builder::<Float32Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::Double => {
                                            get_builder::<Float64Builder>(builder)?.append_null();
                                        }
                                        proto::DataType::Boolean => {
                                            get_builder::<BooleanBuilder>(builder)?.append_null();
                                        }
                                        proto::DataType::String => {
                                            get_builder::<StringBuilder>(builder)?.append_null();
                                        }
                                        proto::DataType::DateTime => {
                                            get_builder::<TimestampMillisecondBuilder>(builder)?
                                                .append_null();
                                        }
                                        _ => {
                                            return UnsupportedMetricDataTypeSnafu {
                                                datatype: datatype as u32,
                                            }
                                            .fail();
                                        }
                                    },
                                }
                            }
                            Variable::MetricIsHistorical => {
                                get_builder::<BooleanBuilder>(builder)?.append_option(
                                    metric.metric.as_ref().and_then(|v| v.is_historical),
                                );
                            }
                            Variable::MetricIsTransient => {
                                get_builder::<BooleanBuilder>(builder)?.append_option(
                                    metric.metric.as_ref().and_then(|v| v.is_transient),
                                );
                            }
                            Variable::MetricIsNull => {
                                get_builder::<BooleanBuilder>(builder)?
                                    .append_option(metric.metric.as_ref().and_then(|v| v.is_null));
                            }
                            Variable::MetricMetadata => {
                                let metadata = metric
                                    .metric
                                    .as_ref()
                                    .and_then(|v| v.metadata.as_ref())
                                    .map(serde_json::to_string)
                                    .transpose()
                                    .context(SerializeMetadataSnafu)?;
                                get_builder::<StringBuilder>(builder)?
                                    .append_option(metadata.as_ref());
                            }
                            Variable::MetricProperties => {
                                let properties = metric
                                    .metric
                                    .as_ref()
                                    .and_then(|v| v.properties.as_ref())
                                    .map(serde_json::to_string)
                                    .transpose()
                                    .context(SerializePropertiesSnafu)?;
                                get_builder::<StringBuilder>(builder)?
                                    .append_option(properties.as_ref());
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                BatchPayload::State(state) => {
                    for (field, builder) in &mut self.fields {
                        match field {
                            Variable::Namespace => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(&state.namespace);
                            }
                            Variable::GroupId => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(&state.group_id);
                            }
                            Variable::MessageType => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(state.message_type.to_string());
                            }
                            Variable::EdgeNodeId => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_value(&state.edge_node_id);
                            }
                            Variable::DeviceId => {
                                get_builder::<StringBuilder>(builder)?
                                    .append_option(state.device_id.as_ref());
                            }
                            Variable::PayloadTimestamp => {
                                get_builder::<TimestampMillisecondBuilder>(builder)?
                                    .append_value(state.state.timestamp);
                            }
                            Variable::PayloadOnline => {
                                get_builder::<BooleanBuilder>(builder)?
                                    .append_value(state.state.online);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }

        let columns = self
            .fields
            .iter_mut()
            .map(|(_, builder)| builder.finish())
            .collect();
        RecordBatch::try_new(self.schema.clone(), columns).context(BuildRecordBatchSnafu)
    }
}

fn get_builder<B>(builder: &mut Box<dyn ArrayBuilder>) -> Result<&mut B>
where
    B: 'static,
{
    builder
        .as_any_mut()
        .downcast_mut::<B>()
        .context(BuilderMissMatchSnafu)
}
