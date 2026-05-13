use std::string::FromUtf8Error;

use chrono::Utc;
use rumqttc::{
    AsyncClient, ConnAck, ConnectReturnCode, ConnectionError, Event, EventLoop, Incoming,
    MqttOptions, Publish, SubAck, SubscribeFilter, SubscribeReasonCode, Transport,
};
use snafu::{IntoError, OptionExt, ResultExt};

use crate::config::{MqttConnectConfig, build_tls_config};

use super::{MAX_RETRY_COUNT, MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL};

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
    #[snafu(display("MQTT task exited"))]
    TaskExited,
    #[snafu(display("Invalid UTF-8"))]
    InvalidUtf8 { source: FromUtf8Error },
    #[snafu(display("MQTT subscription not found"))]
    SubscriptionEmpty,
    #[snafu(display("MQTT connection error"))]
    ConnectionFailed {
        source: Box<rumqttc::ConnectionError>,
    },
    #[snafu(display("MQTT connect failed with code: {code:?}"))]
    ConnFailedWithCode { code: rumqttc::ConnectReturnCode },
    #[snafu(display("MQTT subscribe {topic:?} failed with code: {code:?}"))]
    SubFailedWithCode {
        topic: Option<String>,
        code: rumqttc::SubscribeReasonCode,
    },
    #[snafu(display("MQTT connection error"))]
    UnexpectedPollFailed {
        source: Box<rumqttc::ConnectionError>,
    },
    #[snafu(display("MQTT reconnect failed for too many times"))]
    RetryTooManyTimes {
        source: Box<rumqttc::ConnectionError>,
    },
    #[snafu(display("MQTT pending sub filters not found"))]
    PendingFiltersNotFound,
    #[snafu(display("MQTT keep alive exceeds broker limit: {keep_alive_secs}s"))]
    InvalidKeepAlive { keep_alive_secs: u64 },
}

impl Error {
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            Error::ConnectionFailed { .. }
                | Error::ConnFailedWithCode { .. }
                | Error::UnexpectedPollFailed { .. }
                | Error::RetryTooManyTimes { .. }
        )
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct MessagePoller {
    client: AsyncClient,
    event_loop: EventLoop,
    filters: Vec<SubscribeFilter>,
    pending_filters: Option<Vec<SubscribeFilter>>,
}

impl super::MessagePoller for MessagePoller {
    type Client = AsyncClient;
    type Error = Error;

    fn client(&self) -> Self::Client {
        self.client.clone()
    }

    async fn from_config<I>(config: &MqttConnectConfig, subscriptions: I) -> Result<Self>
    where
        I: IntoIterator<Item = (String, u8)>,
    {
        let filters = build_subscribe_filters(subscriptions)?;
        snafu::ensure!(!filters.is_empty(), SubscriptionEmptySnafu);

        let (client, mut event_loop, session_present) = try_connect(config).await?;

        if session_present {
            tracing::info!("MQTT client connected with present session");
            return Ok(Self {
                client,
                event_loop,
                filters,
                pending_filters: None,
            });
        }
        client
            .subscribe_many(filters.clone())
            .await
            .map_err(|_| TaskExitedSnafu.build())?;
        // sub ack
        loop {
            match event_loop
                .poll()
                .await
                .map_err(Box::new)
                .context(ConnectionFailedSnafu)?
            {
                Event::Incoming(Incoming::SubAck(SubAck { return_codes, .. })) => {
                    for (idx, code) in return_codes.into_iter().enumerate() {
                        let topic = filters.get(idx).map(|f| f.path.clone());
                        match code {
                            SubscribeReasonCode::Success(qos) => {
                                tracing::info!(?topic, ?qos, "subscribe success");
                            }
                            code => return SubFailedWithCodeSnafu { topic, code }.fail(),
                        }
                    }
                    return Ok(Self {
                        client,
                        event_loop,
                        filters,
                        pending_filters: None,
                    });
                }
                Event::Incoming(_) => return ExpectedSubAckSnafu.fail(),
                Event::Outgoing(_) => {}
            }
        }
    }

    async fn try_connect(config: &MqttConnectConfig) -> Result<()> {
        let _ = try_connect(config).await?;
        Ok(())
    }

    async fn poll(&mut self) -> Result<super::Message> {
        let mut retry_count = 0;
        let mut retry_interval = None;

        loop {
            match self.event_loop.poll().await {
                // connect
                Ok(Event::Incoming(Incoming::ConnAck(ConnAck {
                    session_present,
                    code,
                    ..
                }))) => {
                    if code != ConnectReturnCode::Success {
                        tracing::error!("MQTT reconnect refused by server with code: {code:?}");
                        return Err(UnexpectedPollFailedSnafu
                            .into_error(Box::new(ConnectionError::ConnectionRefused(code))));
                    }
                    // reset retry state
                    retry_interval.take();
                    retry_count = 0;
                    // resubscribe if needed
                    if session_present {
                        tracing::info!("MQTT connection reconnected with session");
                    } else {
                        tracing::info!("MQTT reconnect successfully, start to resubscribe");
                        self.client
                            .subscribe_many(self.filters.clone())
                            .await
                            .map_err(|_| TaskExitedSnafu.build())?;
                        self.pending_filters = Some(self.filters.clone());
                    }
                }
                // subscribe
                Ok(Event::Incoming(Incoming::SubAck(SubAck { return_codes, .. }))) => {
                    let pending_filters = self
                        .pending_filters
                        .take()
                        .context(PendingFiltersNotFoundSnafu)?;
                    let mut failed_sub_filters = Vec::with_capacity(pending_filters.len());
                    for (idx, code) in return_codes.into_iter().enumerate() {
                        let topic = pending_filters.get(idx).map(|f| f.path.clone());
                        match code {
                            SubscribeReasonCode::Success(qos) => {
                                tracing::info!(?topic, ?qos, "subscribe success");
                            }
                            code => {
                                tracing::error!("subscribe error with code: {code:?}, retry...");
                                if let Some(filter) = pending_filters.get(idx) {
                                    failed_sub_filters.push(filter.clone());
                                }
                            }
                        }
                    }
                    // subscribe retry
                    if !failed_sub_filters.is_empty() {
                        // interval
                        self.client
                            .subscribe_many(failed_sub_filters.clone())
                            .await
                            .map_err(|_| TaskExitedSnafu.build())?;
                        self.pending_filters = Some(failed_sub_filters);
                    }
                }
                // message
                Ok(Event::Incoming(Incoming::Publish(Publish {
                    topic,
                    qos,
                    payload,
                    ..
                }))) => {
                    return Ok(super::Message {
                        ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        topic,
                        qos: qos as u8,
                        payload,
                    });
                }
                // disconnect
                Ok(Event::Incoming(Incoming::Disconnect)) => {
                    tracing::error!("received DISCONNECT packet from broker");
                }
                Err(e) => {
                    tracing::error!(retry_count, "MQTT polling connection error: {e}");
                    match e {
                        ConnectionError::MqttState(_)
                        | ConnectionError::Io(_)
                        | ConnectionError::NetworkTimeout
                        | ConnectionError::FlushTimeout => {
                            if retry_count >= MAX_RETRY_COUNT {
                                return Err(RetryTooManyTimesSnafu.into_error(Box::new(e)));
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
                            return Err(UnexpectedPollFailedSnafu.into_error(Box::new(e)));
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// 尝试 tcp -> tls without ca / tls with ca 方法
async fn try_connect(config: &MqttConnectConfig) -> Result<(AsyncClient, EventLoop, bool)> {
    if config.certificates.is_none() {
        // tcp
        let mut opts = build_options(config)?;
        opts.set_transport(Transport::tcp());
        match try_connect_inner(opts).await {
            Ok(res) => return Ok(res),
            Err(e) => {
                tracing::error!(
                    "MQTT tcp connect error: {:#}, try with tls",
                    anyhow::Error::new(e)
                );
            }
        }
    }

    // tls
    let mut opts = build_options(config)?;
    let tls_config = build_tls_config(config.certificates.as_ref()).context(InvalidTlsSnafu)?;
    opts.set_transport(Transport::Tls(tls_config));
    try_connect_inner(opts).await
}

async fn try_connect_inner(config: MqttOptions) -> Result<(AsyncClient, EventLoop, bool)> {
    let (client, mut event_loop) = AsyncClient::new(config, 10);

    match event_loop
        .poll()
        .await
        .map_err(Box::new)
        .context(ConnectionFailedSnafu)?
    {
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present,
            code: ConnectReturnCode::Success,
            ..
        })) => Ok((client, event_loop, session_present)),
        Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) => {
            ConnFailedWithCodeSnafu { code }.fail()
        }
        _ => ExpectedConnAckSnafu.fail(),
    }
}

fn build_options(config: &MqttConnectConfig) -> Result<MqttOptions> {
    let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);

    // username, password
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        options.set_credentials(username, password);
    }

    // keepalive
    let keep_alive_secs: u16 =
        config
            .keep_alive
            .as_secs()
            .try_into()
            .ok()
            .context(InvalidKeepAliveSnafu {
                keep_alive_secs: config.keep_alive.as_secs(),
            })?;
    options.set_keep_alive(keep_alive_secs);

    // session
    options.set_clean_session(config.clean_session);

    // packet size
    options.set_max_packet_size(usize::MAX, usize::MAX);

    Ok(options)
}

fn build_subscribe_filters<I>(subscriptions: I) -> Result<Vec<SubscribeFilter>>
where
    I: IntoIterator<Item = (String, u8)>,
{
    subscriptions
        .into_iter()
        .map(|(topic, qos)| {
            rumqttc::qos(qos)
                .ok()
                .context(InvalidQoSSnafu { qos })
                .map(|qos| SubscribeFilter::new(topic, qos))
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    fn mqtt_config(keep_alive: std::time::Duration) -> MqttConnectConfig {
        MqttConnectConfig {
            host: "mqtt.example.com".to_string(),
            port: 1884,
            version: crate::client::Version::V3,
            client_id: "client-1".to_string(),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            keep_alive,
            clean_session: false,
            certificates: None,
            connect_user_properties: None,
            subscribe_user_properties: None,
        }
    }

    #[test]
    fn build_subscribe_filters_test() {
        assert!(build_subscribe_filters([("abc".to_string(), 0)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 1)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 2)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 3)]).is_err());
    }

    #[test]
    fn build_subscribe_filters_preserves_topic_order_and_qos() {
        let filters = build_subscribe_filters([
            ("sensors/temperature".to_string(), 0),
            ("sensors/humidity".to_string(), 2),
        ])
        .unwrap();

        let topics = filters
            .iter()
            .map(|filter| (filter.path.as_str(), filter.qos as u8))
            .collect::<Vec<_>>();
        assert_eq!(
            topics,
            vec![("sensors/temperature", 0), ("sensors/humidity", 2)]
        );
    }

    #[test]
    fn build_options_copies_connection_settings() {
        let config = mqtt_config(std::time::Duration::from_secs(42));

        let options = build_options(&config).unwrap();

        assert_eq!(
            options.broker_address(),
            ("mqtt.example.com".to_string(), 1884)
        );
        assert_eq!(options.client_id(), "client-1");
        assert_eq!(options.keep_alive(), std::time::Duration::from_secs(42));
        assert!(!options.clean_session());
        assert_eq!(options.max_packet_size(), usize::MAX);
        let credentials = options.credentials().unwrap();
        assert_eq!(credentials.username, "user");
        assert_eq!(credentials.password, "pass");
    }

    #[test]
    fn build_options_rejects_keep_alive_above_broker_limit() {
        let config = mqtt_config(std::time::Duration::from_secs(u64::from(u16::MAX) + 1));

        let error = build_options(&config).unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidKeepAlive {
                keep_alive_secs
            } if keep_alive_secs == u64::from(u16::MAX) + 1
        ));
    }

    #[test]
    fn is_connection_error_classifies_only_connection_failures() {
        let connection_errors = [
            Error::ConnectionFailed {
                source: Box::new(ConnectionError::NetworkTimeout),
            },
            Error::ConnFailedWithCode {
                code: ConnectReturnCode::NotAuthorized,
            },
            Error::UnexpectedPollFailed {
                source: Box::new(ConnectionError::RequestsDone),
            },
            Error::RetryTooManyTimes {
                source: Box::new(ConnectionError::FlushTimeout),
            },
        ];

        for error in connection_errors {
            assert!(error.is_connection_error(), "{error:?}");
        }

        let non_connection_errors = [
            Error::InvalidQoS { qos: 3 },
            Error::ExpectedConnAck,
            Error::ExpectedSubAck,
            Error::TaskExited,
            Error::SubscriptionEmpty,
            Error::PendingFiltersNotFound,
            Error::InvalidKeepAlive { keep_alive_secs: 1 },
        ];

        for error in non_connection_errors {
            assert!(!error.is_connection_error(), "{error:?}");
        }
    }
}
