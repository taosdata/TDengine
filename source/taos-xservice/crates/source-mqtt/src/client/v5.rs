use std::string::FromUtf8Error;

use chrono::Utc;
use rumqttc::{
    Transport,
    v5::{
        AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions,
        mqttbytes::v5::{
            ConnAck, ConnectProperties, ConnectReturnCode, Disconnect, Filter, Publish, SubAck,
            SubscribeProperties, SubscribeReasonCode,
        },
    },
};
use snafu::{IntoError, OptionExt, ResultExt};

use crate::{
    client::MAX_RETRY_COUNT,
    config::{MqttConnectConfig, build_tls_config},
};

use super::{MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL};

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
    #[snafu(display("MQTT subscribe request failed"))]
    SubscribeFailed {
        source: Box<rumqttc::v5::ClientError>,
    },
    #[snafu(display("Invalid UTF-8"))]
    InvalidUtf8 { source: FromUtf8Error },
    #[snafu(display("MQTT subscription not found"))]
    SubscriptionEmpty,
    #[snafu(display("Invalid MQTT version"))]
    InvalidVersion { version: String },
    #[snafu(display("MQTT connection error"))]
    ConnectionFailed {
        source: rumqttc::v5::ConnectionError,
    },
    #[snafu(display("MQTT connect failed with code: {code:?}"))]
    ConnFailedWithCode {
        code: rumqttc::v5::mqttbytes::v5::ConnectReturnCode,
    },
    #[snafu(display("MQTT subscribe {topic:?} failed with code: {code:?}"))]
    SubFailedWithCode {
        topic: Option<String>,
        code: rumqttc::v5::mqttbytes::v5::SubscribeReasonCode,
    },
    #[snafu(display("MQTT connection error"))]
    UnexpectedPollFailed {
        source: rumqttc::v5::ConnectionError,
    },
    #[snafu(display("MQTT reconnect failed for too many times"))]
    RetryTooManyTimes {
        source: rumqttc::v5::ConnectionError,
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
    filters: Vec<Filter>,
    subscribe_properties: Option<SubscribeProperties>,
    pending_filters: Option<Vec<Filter>>,
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
        let subscribe_properties = build_subscribe_properties(config);

        let (client, mut event_loop, session_present) = try_connect(config).await?;

        if session_present {
            tracing::info!("MQTT client connected with present session");
            return Ok(Self {
                client,
                event_loop,
                filters,
                subscribe_properties,
                pending_filters: None,
            });
        }

        if let Some(ref props) = subscribe_properties {
            client
                .subscribe_many_with_properties(filters.clone(), props.clone())
                .await
                .map_err(Box::new)
                .context(SubscribeFailedSnafu)?;
        } else {
            client
                .subscribe_many(filters.clone())
                .await
                .map_err(Box::new)
                .context(SubscribeFailedSnafu)?;
        }
        // sub ack
        loop {
            match event_loop.poll().await.context(ConnectionFailedSnafu)? {
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
                        subscribe_properties,
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
                            .into_error(ConnectionError::ConnectionRefused(code)));
                    }
                    // reset retry state
                    retry_interval.take();
                    retry_count = 0;
                    // resubscribe if needed
                    if session_present {
                        tracing::info!("MQTT connection reconnected with session");
                    } else {
                        tracing::info!("MQTT reconnect successfully, start to resubscribe");
                        if let Some(ref props) = self.subscribe_properties {
                            self.client
                                .subscribe_many_with_properties(self.filters.clone(), props.clone())
                                .await
                                .map_err(Box::new)
                                .context(SubscribeFailedSnafu)?;
                        } else {
                            self.client
                                .subscribe_many(self.filters.clone())
                                .await
                                .map_err(Box::new)
                                .context(SubscribeFailedSnafu)?;
                        }
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
                        if let Some(ref props) = self.subscribe_properties {
                            self.client
                                .subscribe_many_with_properties(
                                    failed_sub_filters.clone(),
                                    props.clone(),
                                )
                                .await
                                .map_err(Box::new)
                                .context(SubscribeFailedSnafu)?;
                        } else {
                            self.client
                                .subscribe_many(failed_sub_filters.clone())
                                .await
                                .map_err(Box::new)
                                .context(SubscribeFailedSnafu)?;
                        }
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
                        topic: String::from_utf8(topic.to_vec()).context(InvalidUtf8Snafu)?,
                        qos: qos as u8,
                        payload,
                    });
                }
                // disconnect
                Ok(Event::Incoming(Incoming::Disconnect(Disconnect {
                    reason_code,
                    properties,
                }))) => {
                    if let Some(reason) = properties.and_then(|p| p.reason_string) {
                        tracing::error!(
                            ?reason_code,
                            "received DISCONNECT packet from broker: {reason}"
                        );
                    } else {
                        tracing::error!(?reason_code, "received DISCONNECT packet from broker");
                    }
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
                        | ConnectionError::AuthProcessingError
                        | ConnectionError::ConnectionRefused(_)
                        | ConnectionError::NotConnAck(_) => {
                            return Err(UnexpectedPollFailedSnafu.into_error(e));
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

    match event_loop.poll().await.context(ConnectionFailedSnafu)? {
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
    options.set_clean_start(config.clean_session);

    // ConnectProperties: session_expiry and connect_user_properties are independent
    let needs_session_expiry = !config.clean_session;
    let has_connect_props = config
        .connect_user_properties
        .as_ref()
        .is_some_and(|p| !p.is_empty());

    if needs_session_expiry || has_connect_props {
        let mut props = ConnectProperties::new();
        if needs_session_expiry {
            props.session_expiry_interval = Some(60);
        }
        if let Some(user_props) = &config.connect_user_properties {
            props.user_properties = user_props.clone();
        }
        options.set_connect_properties(props);
    }

    // packet size
    options.set_max_packet_size(Some(u32::MAX));

    Ok(options)
}

fn build_subscribe_properties(config: &MqttConnectConfig) -> Option<SubscribeProperties> {
    let props = config.subscribe_user_properties.as_ref()?;
    if props.is_empty() {
        return None;
    }
    Some(SubscribeProperties {
        id: None,
        user_properties: props.clone(),
    })
}

fn build_subscribe_filters<I>(subscriptions: I) -> Result<Vec<Filter>>
where
    I: IntoIterator<Item = (String, u8)>,
{
    subscriptions
        .into_iter()
        .map(|(topic, qos)| {
            rumqttc::v5::mqttbytes::qos(qos)
                .context(InvalidQoSSnafu { qos })
                .map(|qos| Filter::new(topic, qos))
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn build_subscribe_filters_test() {
        assert!(build_subscribe_filters([("abc".to_string(), 0)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 1)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 2)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 3)]).is_err());
    }
}
