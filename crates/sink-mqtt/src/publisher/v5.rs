use std::sync::Arc;

use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, ConnectionError, Event, Incoming, MqttOptions,
        mqttbytes::{
            QoS, qos,
            v5::{ConnAck, ConnectProperties, ConnectReturnCode},
        },
    },
};
use snafu::{IntoError, OptionExt, ResultExt};
use tokio_util::sync::CancellationToken;

use crate::{
    MAX_RETRY_COUNT, MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL, config::MqttConfig, metrics::Metrics,
};
use taosx_core::utils::defer::defer;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("connection error"))]
    ConnectionFailed { source: ConnectionError },
    #[snafu(display("connect to broker failed, code: {code:?}"))]
    ConnectionFailedWithCode { code: ConnectReturnCode },
    #[snafu(display("expected connect ack packet"))]
    ExpectedConnAck,
    #[snafu(display("connection retry too many times"))]
    RetryTooManyTimes { source: ConnectionError },
    #[snafu(display("unexpected connection error"))]
    UnexpectedPollFailed { source: ConnectionError },
    #[snafu(display("connection poll task exit"))]
    ConnectionTaskExit,
    #[snafu(display("invalid qos: {qos}"))]
    InvalidQoS { qos: u8 },
}

type Result<T> = std::result::Result<T, Error>;

pub struct Publisher {
    client: AsyncClient,
    qos: QoS,
}

impl Publisher {
    pub async fn new(
        config: MqttConfig,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let qos = qos(config.qos).context(InvalidQoSSnafu { qos: config.qos })?;
        let (client, mut event_loop) = AsyncClient::new(config.try_into()?, 1024);
        // connect
        match event_loop.poll().await.context(ConnectionFailedSnafu)? {
            Event::Incoming(Incoming::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                ..
            })) => {}
            Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) => {
                return ConnectionFailedWithCodeSnafu { code }.fail();
            }
            _ => return ExpectedConnAckSnafu.fail(),
        }
        tokio::spawn(async move {
            let _cancel_guard = cancel.clone().drop_guard();
            let _guard = defer(|| {
                tracing::info!("tmq_to_mqtt polling task exit");
            });

            let mut retry_count = 0;
            let mut retry_interval = None;
            loop {
                let Some(event) = cancel.run_until_cancelled(event_loop.poll()).await else {
                    break;
                };

                match event {
                    Ok(Event::Incoming(Incoming::PubAck(_puback))) => {
                        // QoS == 1
                        metrics.add_published_messages();
                    }
                    Ok(Event::Incoming(Incoming::PubRec(_pubrec))) => {
                        // QoS == 2
                        metrics.add_published_messages();
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(0))) => {
                        // QoS == 0
                        metrics.add_published_messages();
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(_pkid))) => {
                        // QoS 1 和 QoS 2 会重复发送 Publish 消息
                    }
                    Err(e) => {
                        tracing::error!(retry_count, "MQTT polling connection error: {e}");
                        match e {
                            ConnectionError::MqttState(_)
                            | ConnectionError::Timeout(_)
                            | ConnectionError::Io(_) => {
                                if retry_count >= MAX_RETRY_COUNT {
                                    return Err(RetryTooManyTimesSnafu.into_error(e));
                                }
                                retry_count += 1;
                                let duration = match retry_interval {
                                    Some(duration) => {
                                        let new_duration: std::time::Duration = duration * 2;
                                        (new_duration).min(MAX_RETRY_INTERVAL)
                                    }
                                    None => MIN_RETRY_INTERVAL,
                                };
                                retry_interval = Some(duration);
                                tracing::warn!("Wait for {duration:?} to reconnect...");
                                tokio::time::sleep(duration).await;
                            }
                            ConnectionError::Tls(_)
                            | ConnectionError::RequestsDone
                            | ConnectionError::ConnectionRefused(_)
                            | ConnectionError::NotConnAck(_) => {
                                return Err(UnexpectedPollFailedSnafu.into_error(e));
                            }
                        }
                    }
                    _ => {}
                }
            }

            Ok(())
        });

        Ok(Self { client, qos })
    }
}

impl super::Publisher for Publisher {
    type Error = Error;

    async fn publish(
        &self,
        topic: &str,
        payload: Vec<u8>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        if cancel
            .run_until_cancelled(self.client.publish(topic, self.qos, false, payload))
            .await
            .is_none_or(|e| e.is_err())
        {
            return ConnectionTaskExitSnafu.fail();
        };

        Ok(())
    }
}

impl TryFrom<MqttConfig> for MqttOptions {
    type Error = Error;

    fn try_from(config: MqttConfig) -> Result<Self> {
        let client_id = if config.concurrency > 1 {
            format!("{}_{}", config.client_id, uuid::Uuid::new_v4().simple())
        } else {
            config.client_id
        };
        let mut options = MqttOptions::new(client_id, config.host, config.port);
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            options.set_credentials(username, password);
        }

        options.set_keep_alive(config.keep_alive);
        options.set_clean_start(config.clean_session);
        if !config.clean_session {
            let mut props = ConnectProperties::new();
            props.session_expiry_interval = Some(60);
            options.set_connect_properties(props);
        }

        options.set_max_packet_size(Some(u32::MAX));
        Ok(options)
    }
}
