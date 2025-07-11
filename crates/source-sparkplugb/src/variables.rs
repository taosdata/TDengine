use std::str::FromStr;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    UnsupportedVariable { s: String },
}

type Result<T> = std::result::Result<T, Error>;

pub const NAMESPACE: &str = "spBv1.0";

pub const VAR_NAMESPACE: &str = "namespace";

pub const VAR_GROUP_ID: &str = "group_id";
pub const VAR_MESSAGE_TYPE: &str = "message_type";
pub const VAR_EDGE_NODE_ID: &str = "edge_node_id";
pub const VAR_DEVICE_ID: &str = "device_id";

pub const VAR_PAYLOAD_TIMESTAMP: &str = "payload_ts";
pub const VAR_PAYLOAD_SEQ: &str = "payload_seq";
pub const VAR_PAYLOAD_ONLINE: &str = "payload_online";

pub const VAR_METRIC_NAME: &str = "name";
pub const VAR_METRIC_ALIAS: &str = "alias";
pub const VAR_METRIC_TIMESTAMP: &str = "timestamp";
pub const VAR_METRIC_DATATYPE_STR: &str = "datatype_str";
pub const VAR_METRIC_DATATYPE_INT: &str = "datatype";
pub const VAR_METRIC_VALUE: &str = "value";
pub const VAR_METRIC_IS_HISTORICAL: &str = "is_historical";
pub const VAR_METRIC_IS_TRANSIENT: &str = "is_transient";
pub const VAR_METRIC_IS_NULL: &str = "is_null";
pub const VAR_METRIC_METADATA: &str = "metadata";
pub const VAR_METRIC_PROPERTIES: &str = "properties";

pub enum Variable {
    Namespace,
    GroupId,
    MessageType,
    EdgeNodeId,
    DeviceId,
    PayloadTimestamp,
    PayloadSeq,
    PayloadOnline,
    MetricName,
    MetricAlias,
    MetricTimestamp,
    MetricDataTypeStr,
    MetricDataTypeInt,
    MetricValue,
    MetricIsHistorical,
    MetricIsTransient,
    MetricIsNull,
    MetricMetadata,
    MetricProperties,
}

impl FromStr for Variable {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            VAR_NAMESPACE => Self::Namespace,
            VAR_GROUP_ID => Self::GroupId,
            VAR_MESSAGE_TYPE => Self::MessageType,
            VAR_EDGE_NODE_ID => Self::EdgeNodeId,
            VAR_DEVICE_ID => Self::DeviceId,
            VAR_PAYLOAD_TIMESTAMP => Self::PayloadTimestamp,
            VAR_PAYLOAD_SEQ => Self::PayloadSeq,
            VAR_PAYLOAD_ONLINE => Self::PayloadOnline,
            VAR_METRIC_NAME => Self::MetricName,
            VAR_METRIC_ALIAS => Self::MetricAlias,
            VAR_METRIC_TIMESTAMP => Self::MetricTimestamp,
            VAR_METRIC_DATATYPE_STR => Self::MetricDataTypeStr,
            VAR_METRIC_DATATYPE_INT => Self::MetricDataTypeInt,
            VAR_METRIC_VALUE => Self::MetricValue,
            VAR_METRIC_IS_HISTORICAL => Self::MetricIsHistorical,
            VAR_METRIC_IS_TRANSIENT => Self::MetricIsTransient,
            VAR_METRIC_IS_NULL => Self::MetricIsNull,
            VAR_METRIC_METADATA => Self::MetricMetadata,
            VAR_METRIC_PROPERTIES => Self::MetricProperties,
            _ => return UnsupportedVariableSnafu { s }.fail(),
        })
    }
}
