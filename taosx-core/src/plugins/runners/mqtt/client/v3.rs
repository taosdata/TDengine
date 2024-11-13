use chrono::Utc;
use rumqttc::{
    AsyncClient, ConnAck, ConnectReturnCode, ConnectionError, Event, EventLoop, Incoming,
    MqttOptions, Publish, SubAck, SubscribeFilter, SubscribeReasonCode, Transport,
};
use snafu::{IntoError, OptionExt, ResultExt};

use crate::runners::mqtt::{
    client::RetryTooManyTimesV3Snafu,
    config::{build_tls_config, MqttConnectConfig},
};

use super::{
    ConnFailedWithCodeV3Snafu, ConnectionErrorV3Snafu, ExpectedConnAckSnafu, ExpectedSubAckSnafu,
    InvalidAddressSnafu, InvalidQoSSnafu, InvalidTlsSnafu, SubFailedWithCodeV3Snafu,
    SubscriptionEmptySnafu, TaskExitedSnafu, UnexpectedPollErrorV3Snafu, MAX_RETRY_COUNT,
    MAX_RETRY_INTERVAL, MIN_RETRY_INTERVAL,
};

pub struct MessagePoller {
    client: AsyncClient,
    event_loop: EventLoop,
    filters: Vec<SubscribeFilter>,
    pending_filters: Option<Vec<SubscribeFilter>>,
}

impl super::MessagePoller for MessagePoller {
    async fn from_config<I>(
        config: &MqttConnectConfig,
        subscriptions: I,
    ) -> Result<Self, super::Error>
    where
        I: IntoIterator<Item = (String, u8)>,
    {
        let (client, mut event_loop) = AsyncClient::new(build_options(config)?, 100);

        // conn ack
        match event_loop.poll().await.context(ConnectionErrorV3Snafu)? {
            Event::Incoming(Incoming::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                session_present,
                ..
            })) => {
                let filters = build_subscribe_filters(subscriptions)?;
                snafu::ensure!(!filters.is_empty(), SubscriptionEmptySnafu);

                if session_present {
                    tracing::info!("MQTT client reconnect with old session");
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
                    match event_loop.poll().await.context(ConnectionErrorV3Snafu)? {
                        Event::Incoming(Incoming::SubAck(SubAck { return_codes, .. })) => {
                            for (idx, code) in return_codes.into_iter().enumerate() {
                                let topic = filters.get(idx).map(|f| f.path.clone());
                                match code {
                                    SubscribeReasonCode::Success(qos) => {
                                        tracing::info!(?topic, ?qos, "subscribe success");
                                    }
                                    code => return SubFailedWithCodeV3Snafu { topic, code }.fail(),
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
            Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) => {
                ConnFailedWithCodeV3Snafu { code }.fail()
            }
            _ => ExpectedConnAckSnafu.fail(),
        }
    }

    async fn try_connect(config: &MqttConnectConfig) -> Result<(), super::Error> {
        let (_, mut event_loop) = AsyncClient::new(build_options(config)?, 10);

        match event_loop.poll().await.context(ConnectionErrorV3Snafu)? {
            Event::Incoming(Incoming::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                ..
            })) => Ok(()),
            Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) => {
                ConnFailedWithCodeV3Snafu { code }.fail()
            }
            _ => ExpectedConnAckSnafu.fail(),
        }
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
                        return Err(UnexpectedPollErrorV3Snafu
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
                        topic,
                        qos: qos as u8,
                        payload,
                    })
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
                                return Err(RetryTooManyTimesV3Snafu.into_error(e));
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
                            return Err(UnexpectedPollErrorV3Snafu.into_error(e));
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

fn build_options(config: &MqttConnectConfig) -> Result<MqttOptions, super::Error> {
    let (host, port) = config.host_port().context(InvalidAddressSnafu)?;
    let mut options = MqttOptions::new(&config.client_id, host, port);

    // username, password
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        options.set_credentials(username, password);
    }

    // ssl
    if let Some((ca, client_cert, client_key)) = config.ssl() {
        let tls_config = build_tls_config(ca, client_cert, client_key).context(InvalidTlsSnafu)?;
        options.set_transport(Transport::tls_with_config(tls_config));
    }

    // keepalive
    options.set_keep_alive(config.keep_alive());

    // session
    options.set_clean_session(config.clean_session());

    // packet size
    options.set_max_packet_size(usize::MAX, usize::MAX);

    Ok(options)
}

fn build_subscribe_filters<I>(subscriptions: I) -> Result<Vec<SubscribeFilter>, super::Error>
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

    use bytes::Bytes;
    use rumqttc::QoS;
    use taos::Dsn;

    use crate::runners::mqtt::client::MessagePoller;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn try_connect_success() {
        assert!(try_connect(
            "mqtt://emqx:1883?version=3.1&client_id=abc&username=admin&password=public"
        )
        .await
        .is_ok());

        assert!(try_connect(
            "mqtt://192.168.1.1:1883?version=3.1&client_id=abc&username=admin&password=public"
        )
        .await
        .is_err());

        assert!(try_connect(
            "mqtt://192.168.1.1:1884?version=3.1&client_id=abc&username=admin&password=public"
        )
        .await
        .is_err());

        assert!(try_connect(
            "mqtt://192.168.1.1:1884?version=3.1&client_id=abc&username=admin&password=private"
        )
        .await
        .is_err());
    }

    async fn try_connect(dsn_str: &str) -> Result<(), crate::runners::mqtt::client::Error> {
        let config = dsn_str.parse::<Dsn>().unwrap().try_into().unwrap();
        super::MessagePoller::try_connect(&config).await
    }

    #[ignore]
    #[tokio::test]
    async fn subscribe_test() {
        const TOPIC: &str = "tp_test";
        const PAYLOAD: &[u8] = b"hello, world";

        let s: MqttConnectConfig =
            "mqtt://emqx:1883?version=3.1.1&client_id=sub_test&clean_session=true&username=admin&password=public"
                .parse::<Dsn>()
                .unwrap()
                .try_into()
                .unwrap();
        let mut poller = super::MessagePoller::from_config(&s, [(TOPIC.to_string(), 1)])
            .await
            .unwrap();
        let client = poller.client.clone();

        tokio::join!(
            async {
                for _ in 0..5 {
                    let message = poller.poll().await.unwrap();
                    assert_eq!(message.topic, TOPIC.to_string());
                    assert_eq!(message.qos, 1);
                    assert_eq!(message.payload, Bytes::from_static(PAYLOAD));
                }
            },
            async {
                for _ in 0..5 {
                    client
                        .publish(TOPIC, QoS::AtLeastOnce, false, PAYLOAD)
                        .await
                        .unwrap();
                }
            }
        );
    }

    #[test]
    fn build_subscribe_filters_test() {
        assert!(build_subscribe_filters([("abc".to_string(), 0)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 1)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 2)]).is_ok());
        assert!(build_subscribe_filters([("abc".to_string(), 3)]).is_err());
    }
}
