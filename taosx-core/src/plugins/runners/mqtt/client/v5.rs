use chrono::Utc;
use rumqttc::{
    v5::{
        mqttbytes::v5::{
            ConnAck, ConnectProperties, ConnectReturnCode, Disconnect, Filter, Publish, SubAck,
            SubscribeReasonCode,
        },
        AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions,
    },
    Transport,
};
use snafu::{IntoError, OptionExt, ResultExt};

use crate::runners::mqtt::{
    client::RetryTooManyTimesV5Snafu,
    config::{build_tls_config, MqttConnectConfig},
};

use super::{
    ConnFailedWithCodeV5Snafu, ConnectionErrorV5Snafu, ExpectedConnAckSnafu, ExpectedSubAckSnafu,
    InvalidQoSSnafu, InvalidTlsSnafu, InvalidUtf8Snafu, SubFailedWithCodeV5Snafu,
    SubscriptionEmptySnafu, TaskExitedSnafu, UnexpectedPollErrorV5Snafu, MAX_RETRY_COUNT,
    MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL,
};

pub struct MessagePoller {
    client: AsyncClient,
    event_loop: EventLoop,
    filters: Vec<Filter>,
    pending_filters: Option<Vec<Filter>>,
}

impl super::MessagePoller for MessagePoller {
    async fn from_config<I>(
        config: &MqttConnectConfig,
        subscriptions: I,
    ) -> Result<Self, super::Error>
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
            match event_loop.poll().await.context(ConnectionErrorV5Snafu)? {
                Event::Incoming(Incoming::SubAck(SubAck { return_codes, .. })) => {
                    for (idx, code) in return_codes.into_iter().enumerate() {
                        let topic = filters.get(idx).map(|f| f.path.clone());
                        match code {
                            SubscribeReasonCode::Success(qos) => {
                                tracing::info!(?topic, ?qos, "subscribe success");
                            }
                            code => return SubFailedWithCodeV5Snafu { topic, code }.fail(),
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

    async fn try_connect(config: &MqttConnectConfig) -> Result<(), super::Error> {
        let _ = try_connect(config).await?;
        Ok(())
    }

    async fn poll(&mut self) -> Result<super::Message, super::Error> {
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
                        return Err(UnexpectedPollErrorV5Snafu
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
                        .expect("Unexpected SubAck packet");
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
                                return Err(RetryTooManyTimesV5Snafu.into_error(e));
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
                            return Err(UnexpectedPollErrorV5Snafu.into_error(e));
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// 尝试 tcp -> tls without ca / tls with ca 方法
async fn try_connect(
    config: &MqttConnectConfig,
) -> Result<(AsyncClient, EventLoop, bool), super::Error> {
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

async fn try_connect_inner(
    config: MqttOptions,
) -> Result<(AsyncClient, EventLoop, bool), super::Error> {
    let (client, mut event_loop) = AsyncClient::new(config, 10);

    match event_loop.poll().await.context(ConnectionErrorV5Snafu)? {
        Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present,
            code: ConnectReturnCode::Success,
            ..
        })) => Ok((client, event_loop, session_present)),
        Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) => {
            ConnFailedWithCodeV5Snafu { code }.fail()
        }
        _ => ExpectedConnAckSnafu.fail(),
    }
}

fn build_options(config: &MqttConnectConfig) -> Result<MqttOptions, super::Error> {
    let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);

    // username, password
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        options.set_credentials(username, password);
    }

    // keepalive
    options.set_keep_alive(config.keep_alive);

    // session
    options.set_clean_start(config.clean_session);
    if !config.clean_session {
        let mut props = ConnectProperties::new();
        props.session_expiry_interval = Some(60);
        options.set_connect_properties(props);
    }

    // packet size
    options.set_max_packet_size(Some(u32::MAX));

    Ok(options)
}

fn build_subscribe_filters<I>(subscriptions: I) -> Result<Vec<Filter>, super::Error>
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
