use std::{collections::HashSet, time::Duration};

use snafu::OptionExt;
use taos::Dsn;

use source_mqtt::{
    client::Version,
    config::{
        Certificates, MqttConnectConfig, parse_client_id, parse_keep_alive, parse_tls_certificates,
        parse_version,
    },
};
use taosx_core::utils::dsn::{option_param, parse_option_param};

use super::variables::NAMESPACE;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("unsupported message type: {s}"))]
    UnsupportedMessageType { s: String },
    #[snafu(context(false))]
    MqttConfig { source: anyhow::Error },
    #[snafu(display("brokers param not found"))]
    MissingBrokers,
    #[snafu(display("group_id param not found"))]
    MissingGroupId,
    #[snafu(display("message_type param not found"))]
    MissingMessageType,
    #[snafu(display("invalid config param: {key}"))]
    InvalidParam { key: String },
}

type Result<T> = std::result::Result<T, Error>;

pub struct Config {
    pub(crate) mqtt: ConnectConfig,
    pub(crate) subscribe: SubscribeConfig,
}

impl TryFrom<&Dsn> for Config {
    type Error = Error;

    fn try_from(dsn: &Dsn) -> Result<Self> {
        Ok(Self {
            mqtt: dsn.try_into()?,
            subscribe: dsn.try_into()?,
        })
    }
}

pub struct ConnectConfig {
    pub brokers: Vec<(String, u16)>,
    pub version: Version,
    pub client_id: String,
    pub keep_alive: Duration,
    /// username and password
    pub auth: Option<(String, String)>,
    pub certs: Option<Certificates>,
}

impl TryFrom<&Dsn> for ConnectConfig {
    type Error = Error;

    fn try_from(dsn: &Dsn) -> Result<Self> {
        let brokers = parse_multi_host_port(dsn);
        snafu::ensure!(!brokers.is_empty(), MissingBrokersSnafu);
        Ok(Self {
            brokers,
            version: parse_version(dsn)?,
            client_id: parse_client_id(dsn)?,
            keep_alive: parse_keep_alive(dsn)?,
            auth: dsn.username.clone().zip(dsn.password.clone()),
            certs: parse_tls_certificates(dsn)?,
        })
    }
}

impl ConnectConfig {
    pub fn mqtt_config(&self) -> Result<Vec<MqttConnectConfig>> {
        let mut ret = Vec::with_capacity(self.brokers.len());
        for (host, port) in &self.brokers {
            let config = MqttConnectConfig {
                host: host.clone(),
                port: *port,
                version: self.version,
                client_id: self.client_id.clone(),
                username: self.auth.as_ref().map(|s| s.0.clone()),
                password: self.auth.as_ref().map(|s| s.1.clone()),
                keep_alive: self.keep_alive,
                clean_session: true,
                certificates: self.certs.clone(),
            };
            ret.push(config)
        }
        Ok(ret)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MessageType {
    NBirth,
    NDeath,
    DBirth,
    DDeath,
    NData,
    DData,
    NCmd,
    DCmd,
    State,
}

impl TryFrom<faststr::FastStr> for MessageType {
    type Error = Error;

    fn try_from(value: faststr::FastStr) -> Result<Self> {
        value.as_str().parse()
    }
}

impl std::str::FromStr for MessageType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "NBIRTH" => Self::NBirth,
            "NDEATH" => Self::NDeath,
            "DBIRTH" => Self::DBirth,
            "DDEATH" => Self::DDeath,
            "NDATA" => Self::NData,
            "DDATA" => Self::DData,
            "NCMD" => Self::NCmd,
            "DCMD" => Self::DCmd,
            "STATE" => Self::State,
            s => return UnsupportedMessageTypeSnafu { s }.fail(),
        })
    }
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MessageType::NBirth => "NBIRTH",
                MessageType::NDeath => "NDEATH",
                MessageType::DBirth => "DBIRTH",
                MessageType::DDeath => "DDEATH",
                MessageType::NData => "NDATA",
                MessageType::DData => "DDATA",
                MessageType::NCmd => "NCMD",
                MessageType::DCmd => "DCMD",
                MessageType::State => "STATE",
            }
        )
    }
}

pub struct SubscribeConfig {
    rebirth_cmd: Option<bool>,
    group_id: String,
    node_device_list: Option<Vec<String>>,
    message_types: Vec<MessageType>,
}

impl SubscribeConfig {
    pub fn send_rebirth_cmd(&self) -> bool {
        self.rebirth_cmd.is_some_and(|v| v)
    }
}

impl TryFrom<&Dsn> for SubscribeConfig {
    type Error = Error;

    fn try_from(dsn: &Dsn) -> Result<Self> {
        let rebirth_cmd = parse_option_param::<bool>(dsn, "rebirth_cmd")
            .map_err(|_| InvalidParamSnafu { key: "rebirth_cmd" }.build())?;
        let group_id = option_param(dsn, "group_id").context(MissingGroupIdSnafu)?;
        let node_device_list = option_param(dsn, "node_device_list").map(|s| {
            s.split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        });
        let message_types = option_param(dsn, "message_types")
            .map(|s| {
                parse_list(s)
                    .map(|s| s.parse::<MessageType>())
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .context(MissingMessageTypeSnafu)?;
        Ok(Self {
            rebirth_cmd,
            group_id: group_id.to_string(),
            node_device_list,
            message_types,
        })
    }
}

impl SubscribeConfig {
    pub fn rebirth_topics(&self) -> Option<Vec<String>> {
        self.node_device_list.as_ref().map(|v| {
            v.iter()
                .filter_map(|s| {
                    s.split('/')
                        .next()
                        .map(|node| node.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| format!("{NAMESPACE}/{}/NCMD/{s}", self.group_id))
                })
                .collect::<Vec<_>>()
        })
    }

    pub fn subscriptions(&self) -> Vec<(String, u8)> {
        let group_id = &self.group_id;

        let node_device_list = match &self.node_device_list {
            Some(list) => list.clone(),
            None => {
                return self
                    .message_types
                    .iter()
                    .map(|message_type| (format!("{NAMESPACE}/{group_id}/{message_type}/#"), 1))
                    .collect();
            }
        };
        let capacity =
            self.message_types.len() * self.node_device_list.as_ref().map(|v| v.len()).unwrap_or(1);
        let mut ret = HashSet::with_capacity(capacity);

        for message_type in &self.message_types {
            for node_device in &node_device_list {
                // node_name, node_name/+
                let (node, device) = match node_device.split_once('/') {
                    Some((node, device)) => (node, Some(device)),
                    None => (node_device.as_str(), None),
                };
                match (message_type, node, device) {
                    (MessageType::NBirth, node, None)
                    | (MessageType::NDeath, node, None)
                    | (MessageType::NData, node, None)
                    | (MessageType::NCmd, node, None) => {
                        ret.insert((format!("{NAMESPACE}/{group_id}/{message_type}/{node}"), 1));
                    }
                    (MessageType::DBirth, node, Some(device))
                    | (MessageType::DDeath, node, Some(device))
                    | (MessageType::DData, node, Some(device))
                    | (MessageType::DCmd, node, Some(device)) => {
                        ret.insert((
                            format!("{NAMESPACE}/{group_id}/{message_type}/{node}/{device}"),
                            1,
                        ));
                    }
                    (MessageType::State, node, _) => {
                        ret.insert((format!("{NAMESPACE}/{group_id}/{message_type}/{node}"), 1));
                    }
                    _ => {}
                }
            }
        }

        Vec::from_iter(ret)
    }
}

fn parse_multi_host_port(dsn: &Dsn) -> Vec<(String, u16)> {
    dsn.addresses
        .iter()
        .filter_map(|addr| addr.host.clone().zip(addr.port))
        .collect()
}

fn parse_list(s: &str) -> impl Iterator<Item = String> + use<'_> {
    s.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn subscribe_config_test() -> anyhow::Result<()> {
        let config = SubscribeConfig::try_from(&Dsn::from_str(
            "mqtt://localhost:1883?group_id=group_1&node_device_list=node_1&message_types=DDATA",
        )?)?;
        assert_eq!(config.subscriptions(), vec![]);
        assert_eq!(
            config.rebirth_topics(),
            Some(vec!["spBv1.0/group_1/NCMD/node_1".into()])
        );
        Ok(())
    }
}
