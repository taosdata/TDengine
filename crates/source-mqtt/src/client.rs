use std::future::Future;

use bytes::Bytes;

use super::config::MqttConnectConfig;

mod v3;
mod v5;

const MIN_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_RETRY_COUNT: i32 = 10;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Invalid MQTT version"))]
    InvalidVersion { version: String },
    #[snafu(display("Invalid QoS: {qos}"))]
    InvalidQoS { qos: u8 },
    #[snafu(context(false))]
    V3 { source: v3::Error },
    #[snafu(context(false))]
    V5 { source: v5::Error },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    type Client;
    type Error;

    fn client(&self) -> Self::Client;

    fn from_config<I>(
        config: &MqttConnectConfig,
        subscriptions: I,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send
    where
        I: IntoIterator<Item = (String, u8)> + Send,
        Self: Sized;

    fn try_connect(
        config: &MqttConnectConfig,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn poll(&mut self) -> impl Future<Output = Result<Message, Self::Error>> + Send;
}

#[derive(Clone)]
pub struct Message {
    pub ts: i64,
    pub topic: String,
    pub qos: u8,
    pub payload: Bytes,
}

#[derive(Clone)]
pub enum GenericClient {
    V3(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

impl GenericClient {
    pub async fn publish(&self, topic: &str, qos: u8, payload: Vec<u8>) -> Result<bool, Error> {
        match self {
            GenericClient::V3(client) => {
                let qos = rumqttc::qos(qos).map_err(|_| InvalidQoSSnafu { qos }.build())?;
                Ok(client.publish(topic, qos, false, payload).await.is_ok())
            }
            GenericClient::V5(client) => {
                let qos =
                    rumqttc::v5::mqttbytes::qos(qos).ok_or(InvalidQoSSnafu { qos }.build())?;
                Ok(client.publish(topic, qos, false, payload).await.is_ok())
            }
        }
    }
}

pub enum GenericMessagePoller {
    V3(Box<v3::MessagePoller>),
    V5(Box<v5::MessagePoller>),
}

impl MessagePoller for GenericMessagePoller {
    type Client = GenericClient;
    type Error = Error;

    fn client(&self) -> Self::Client {
        match self {
            GenericMessagePoller::V3(poller) => GenericClient::V3(poller.client()),
            GenericMessagePoller::V5(poller) => GenericClient::V5(poller.client()),
        }
    }

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
            Version::V3 => Ok(v3::MessagePoller::try_connect(config).await?),
            Version::V5 => Ok(v5::MessagePoller::try_connect(config).await?),
        }
    }

    async fn poll(&mut self) -> Result<Message, Error> {
        match self {
            GenericMessagePoller::V3(poller) => Ok(poller.poll().await?),
            GenericMessagePoller::V5(poller) => Ok(poller.poll().await?),
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
