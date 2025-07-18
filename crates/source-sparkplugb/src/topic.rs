use std::str::FromStr;

use faststr::FastStr;
use snafu::OptionExt;

use crate::variables::NAMESPACE;

use super::config::MessageType;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("invalid topic: {topic}"))]
    InvalidTopic { topic: String },
    #[snafu(context(false))]
    Config { source: super::config::Error },
    #[snafu(display("unsupported namespace: {namespace}"))]
    UnsupportedProtocol { namespace: String },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct TopicComponents {
    pub namespace: FastStr,
    pub group_id: FastStr,
    pub message_type: MessageType,
    pub edge_node_id: FastStr,
    pub device_id: Option<FastStr>,
}

impl FromStr for TopicComponents {
    type Err = Error;

    fn from_str(topic: &str) -> Result<Self> {
        let mut components = topic.split('/').map(|s| s.trim());
        let filter_map = |s: &str| (!s.is_empty()).then(|| FastStr::from(s.to_string()));
        let namespace = components
            .next()
            .and_then(filter_map)
            .context(InvalidTopicSnafu { topic })?;
        snafu::ensure!(
            namespace == NAMESPACE,
            UnsupportedProtocolSnafu { namespace }
        );
        let ret = Self {
            namespace,
            group_id: components
                .next()
                .and_then(filter_map)
                .context(InvalidTopicSnafu { topic })?,
            message_type: components
                .next()
                .and_then(filter_map)
                .map(|s| s.try_into())
                .transpose()?
                .context(InvalidTopicSnafu { topic })?,
            edge_node_id: components
                .next()
                .and_then(filter_map)
                .context(InvalidTopicSnafu { topic })?,
            device_id: components.next().and_then(filter_map),
        };
        snafu::ensure!(components.next().is_none(), InvalidTopicSnafu { topic });
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_topic_test() -> anyhow::Result<()> {
        let components: TopicComponents = "spBv1.0/group_1/NCMD/node_1".parse()?;
        assert_eq!(components.namespace, NAMESPACE);
        assert_eq!(components.group_id, "group_1");
        assert_eq!(components.message_type, MessageType::NCmd);
        assert_eq!(components.edge_node_id, "node_1");

        assert!(
            "spBv2.0/group_1/NCMD/node_1"
                .parse::<TopicComponents>()
                .is_err()
        );
        assert!("spBv1.0/NCMD/node_1".parse::<TopicComponents>().is_err());
        assert!(
            "spBv1.0/group_1/NCMDD/node_1"
                .parse::<TopicComponents>()
                .is_err()
        );
        assert!(
            "spBv1.0/group_1/NCMD/node_1/d1/d2"
                .parse::<TopicComponents>()
                .is_err()
        );
        Ok(())
    }
}
