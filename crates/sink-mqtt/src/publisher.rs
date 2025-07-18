use snafu::ResultExt;
use std::{future::Future, sync::Arc};
use tokio_util::sync::CancellationToken;

use super::{config::MqttConfig, metrics::Metrics};
use source_mqtt::client::Version;

mod v3;
mod v5;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("v3 publisher error"))]
    V3 { source: v3::Error },
    #[snafu(display("v5 publisher error"))]
    V5 { source: v5::Error },
}

/// 需要确认发送到 broker 后再返回
pub trait Publisher {
    type Error: std::error::Error;

    fn publish(
        &self,
        topic: &str,
        payload: Vec<u8>,
        cancel: &CancellationToken,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub enum GenericPublisher {
    V3(v3::Publisher),
    V5(v5::Publisher),
}

impl GenericPublisher {
    pub async fn new(
        config: MqttConfig,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
    ) -> Result<Self, Error> {
        Ok(match config.version {
            Version::V3 => Self::V3(
                v3::Publisher::new(config, metrics, cancel)
                    .await
                    .context(V3Snafu)?,
            ),
            Version::V5 => Self::V5(
                v5::Publisher::new(config, metrics, cancel)
                    .await
                    .context(V5Snafu)?,
            ),
        })
    }
}

impl Publisher for GenericPublisher {
    type Error = Error;

    async fn publish(
        &self,
        topic: &str,
        payload: Vec<u8>,
        cancel: &CancellationToken,
    ) -> Result<(), Self::Error> {
        match self {
            GenericPublisher::V3(publisher) => publisher
                .publish(topic, payload, cancel)
                .await
                .context(V3Snafu),
            GenericPublisher::V5(publisher) => publisher
                .publish(topic, payload, cancel)
                .await
                .context(V5Snafu),
        }
    }
}
