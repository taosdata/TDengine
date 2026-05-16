use std::str::FromStr;

use anyhow::Context;
use faststr::FastStr;

use super::message_type::MessageType;

#[derive(Debug)]
pub struct TopicComponents {
    pub namespace: FastStr,
    pub group_id: FastStr,
    pub message_type: MessageType,
    pub edge_node_id: FastStr,
    pub device_id: Option<FastStr>,
}

impl FromStr for TopicComponents {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut components = s.split('/').map(|s| s.trim());
        let filter_map = |s: &str| (!s.is_empty()).then(|| FastStr::from(s.to_string()));
        Ok(Self {
            namespace: components
                .next()
                .and_then(filter_map)
                .context("topic namespace not found")?,
            group_id: components
                .next()
                .and_then(filter_map)
                .context("topic group_id not found")?,
            message_type: components
                .next()
                .and_then(filter_map)
                .map(|s| s.try_into())
                .transpose()?
                .context("topic message type not found")?,
            edge_node_id: components
                .next()
                .and_then(filter_map)
                .context("topic edge_node_id not found")?,
            device_id: components.next().and_then(filter_map),
        })
    }
}
