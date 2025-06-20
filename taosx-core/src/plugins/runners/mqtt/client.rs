use std::{future::Future, string::FromUtf8Error};

use bytes::Bytes;

use super::config::MqttConnectConfig;

mod v3;
mod v5;

const MIN_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_RETRY_COUNT: i32 = 10;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Invalid mqtt tls config"))]
    InvalidTls { source: anyhow::Error },
    #[snafu(display("Invalid QoS: {qos}"))]
    InvalidQoS { qos: u8 },
    #[snafu(display("Receive unexpected MQTT packet, expected ConnAck"))]
    ExpectedConnAck,
    #[snafu(display("Receive unexpected MQTT packet, expected SubAck"))]
    ExpectedSubAck,
    #[snafu(display("MQTT connection error"))]
    ConnectionErrorV3 {
        source: Box<rumqttc::ConnectionError>,
    },
    #[snafu(display("MQTT connect failed with code: {code:?}"))]
    ConnFailedWithCodeV3 { code: rumqttc::ConnectReturnCode },
    #[snafu(display("MQTT subscribe {topic:?} failed with code: {code:?}"))]
    SubFailedWithCodeV3 {
        topic: Option<String>,
        code: rumqttc::SubscribeReasonCode,
    },
    #[snafu(display("MQTT connection error"))]
    ConnectionErrorV5 {
        source: rumqttc::v5::ConnectionError,
    },
    #[snafu(display("MQTT connect failed with code: {code:?}"))]
    ConnFailedWithCodeV5 {
        code: rumqttc::v5::mqttbytes::v5::ConnectReturnCode,
    },
    #[snafu(display("MQTT subscribe {topic:?} failed with code: {code:?}"))]
    SubFailedWithCodeV5 {
        topic: Option<String>,
        code: rumqttc::v5::mqttbytes::v5::SubscribeReasonCode,
    },
    #[snafu(display("MQTT task exited"))]
    TaskExited,
    #[snafu(display("MQTT connection error"))]
    UnexpectedPollErrorV3 {
        source: Box<rumqttc::ConnectionError>,
    },
    #[snafu(display("MQTT connection error"))]
    UnexpectedPollErrorV5 {
        source: rumqttc::v5::ConnectionError,
    },
    #[snafu(display("MQTT reconnect failed for too many times"))]
    RetryTooManyTimesV3 {
        source: Box<rumqttc::ConnectionError>,
    },
    #[snafu(display("MQTT reconnect failed for too many times"))]
    RetryTooManyTimesV5 {
        source: rumqttc::v5::ConnectionError,
    },
    #[snafu(display("Invalid UTF-8"))]
    InvalidUtf8 { source: FromUtf8Error },
    #[snafu(display("MQTT subscription not found"))]
    SubscriptionEmpty,
    #[snafu(display("Invalid MQTT version"))]
    InvalidVersion { version: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Version {
    V3,
    V5,
}

impl std::str::FromStr for Version {
    type Err = Error;

    fn from_str(version: &str) -> Result<Self, Self::Err> {
        match version {
            "3.1" | "3.1.1" => Ok(Version::V3),
            "5.0" | "5" => Ok(Version::V5),
            _ => InvalidVersionSnafu { version }.fail(),
        }
    }
}

pub trait MessagePoller {
    fn from_config<I>(
        config: &MqttConnectConfig,
        subscriptions: I,
    ) -> impl Future<Output = Result<Self, Error>> + Send
    where
        I: IntoIterator<Item = (String, u8)> + Send,
        Self: Sized;

    fn try_connect(config: &MqttConnectConfig) -> impl Future<Output = Result<(), Error>> + Send;

    fn poll(&mut self) -> impl Future<Output = Result<Message, Error>> + Send;
}

#[derive(Clone)]
pub struct Message {
    pub ts: i64,
    pub topic: String,
    pub qos: u8,
    pub payload: Bytes,
}

pub enum GenericMessagePoller {
    V3(Box<v3::MessagePoller>),
    V5(Box<v5::MessagePoller>),
}

impl MessagePoller for GenericMessagePoller {
    async fn from_config<I>(config: &MqttConnectConfig, subscriptions: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (String, u8)> + Send,
    {
        match config.version {
            Version::V3 => Ok(Self::V3(Box::new(
                v3::MessagePoller::from_config(config, subscriptions).await?,
            ))),
            Version::V5 => Ok(Self::V5(Box::new(
                v5::MessagePoller::from_config(config, subscriptions).await?,
            ))),
        }
    }

    async fn try_connect(config: &MqttConnectConfig) -> Result<(), Error> {
        match config.version {
            Version::V3 => v3::MessagePoller::try_connect(config).await,
            Version::V5 => v5::MessagePoller::try_connect(config).await,
        }
    }

    async fn poll(&mut self) -> Result<Message, Error> {
        match self {
            GenericMessagePoller::V3(poller) => poller.poll().await,
            GenericMessagePoller::V5(poller) => poller.poll().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_test() -> anyhow::Result<()> {
        assert_eq!("3.1".parse::<Version>()?, Version::V3);
        assert_eq!("3.1.1".parse::<Version>()?, Version::V3);
        assert_eq!("5.0".parse::<Version>()?, Version::V5);
        assert_eq!("5".parse::<Version>()?, Version::V5);

        assert!("4".parse::<Version>().is_err());
        Ok(())
    }
}
