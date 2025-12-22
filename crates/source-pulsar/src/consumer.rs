use crate::{
    METRIC_CONSUMERS,
    config::{
        connect::{DataVendor, PulsarConnectConfig},
        task::PulsarTaskConfig,
        tuya::TuyaAuthentication,
    },
    context::CustomContext,
};
use pulsar::{
    Authentication, Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor,
    authentication::basic::BasicAuthentication, consumer::InitialPosition,
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use taosx_core::core_metrics::CoreMetrics;

pub struct CustomConsumer {
    pub consumer: Consumer<Vec<u8>, TokioExecutor>,
    pub context: Arc<CustomContext>,
    pub pulsar: Pulsar<TokioExecutor>,
}

impl CustomConsumer {
    pub fn new(
        consumer: Consumer<Vec<u8>, TokioExecutor>,
        context: Arc<CustomContext>,
        pulsar: Pulsar<TokioExecutor>,
    ) -> Self {
        Self {
            consumer,
            context,
            pulsar,
        }
    }

    pub fn context(&self) -> Arc<CustomContext> {
        self.context.clone()
    }
}

impl Deref for CustomConsumer {
    type Target = Consumer<Vec<u8>, TokioExecutor>;

    fn deref(&self) -> &Self::Target {
        &self.consumer
    }
}

impl DerefMut for CustomConsumer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.consumer
    }
}

pub async fn build_consumer(
    config: &PulsarTaskConfig,
    topic: &str,
    metrics: &Arc<CoreMetrics>,
) -> anyhow::Result<CustomConsumer> {
    let mut context = CustomContext::new(metrics.clone(), config.connect.data_vendor);
    if config.connect.data_vendor == DataVendor::Tuya {
        context.tuya_access_key = config.connect.tuya_access_key.clone();
    }
    build_consumer_with_context(config, topic, context).await
}

pub async fn build_consumer_with_context(
    config: &PulsarTaskConfig,
    topic: &str,
    context: CustomContext,
) -> anyhow::Result<CustomConsumer> {
    let joins = context.fetch_add_joins();
    tracing::info!("build_consumer_with_context, joins times: {:?}", joins);

    let pulsar = build_pulsar(&config.connect).await?;
    let new_consumer =
        async |initial_position: InitialPosition| -> anyhow::Result<Consumer<_, TokioExecutor>> {
            pulsar
                .consumer()
                .with_topic(topic)
                .with_consumer_name(&config.consumer_name)
                .with_subscription(&config.subscription)
                .with_subscription_type(SubType::Failover)
                .with_options(ConsumerOptions {
                    initial_position,
                    ..Default::default()
                })
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("build consumer error: {:#}", e))
        };
    let mut consumer = new_consumer(config.initial_position.clone()).await?;

    if let Some(true) = &config.seek_to_end {
        if config.connect.data_vendor == DataVendor::Tuya {
            tracing::warn!("tuya consumer can't unsubscribe, does not support seek to end");
        } else {
            consumer.unsubscribe().await?;
        }
        consumer.close().await?;
        consumer = new_consumer(InitialPosition::Latest).await?;
    }

    let consumer = CustomConsumer {
        consumer,
        context: Arc::new(context),
        pulsar,
    };
    consumer
        .context()
        .metrics()
        .add_extra_metric(&METRIC_CONSUMERS, 1);

    Ok(consumer)
}

pub async fn build_pulsar(
    conn_config: &PulsarConnectConfig,
) -> anyhow::Result<Pulsar<TokioExecutor>> {
    let mut builder = Pulsar::builder(&conn_config.broker_url, TokioExecutor);

    if conn_config.data_vendor == DataVendor::Tuya {
        let auth = TuyaAuthentication::new(
            conn_config
                .tuya_access_id
                .as_ref()
                .ok_or(anyhow::anyhow!("tuya access_id is empty"))?
                .clone(),
            conn_config
                .tuya_access_key
                .as_ref()
                .ok_or(anyhow::anyhow!("tuya access_key is empty"))?
                .clone(),
        );
        builder = builder
            .with_allow_insecure_connection(true)
            .with_tls_hostname_verification_enabled(false)
            .with_auth_provider(auth);
    } else if let (Some(ba_username), Some(ba_password)) =
        (&conn_config.ba_username, &conn_config.ba_password)
    {
        builder = builder.with_auth_provider(BasicAuthentication::new(
            ba_username.as_str(),
            ba_password.as_str(),
        ));
    } else if let Some(ref token) = conn_config.jwt_token {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.clone().into_bytes(),
        };

        builder = builder.with_auth(authentication);
    } else if conn_config.use_ssl {
        builder = builder.with_certificate_chain(conn_config.get_cert_chain());
    } else if let (Some(auth_name), Some(auth_data)) =
        (&conn_config.custom_auth_name, &conn_config.custom_auth_data)
    {
        let auth = Authentication {
            name: auth_name.clone(),
            data: auth_data.clone().into_bytes(),
        };
        builder = builder.with_auth(auth);
    }

    builder
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Build pulsar error: {:?}", e))
}

pub async fn split_topics(config: &PulsarTaskConfig, topic: &str) -> anyhow::Result<Vec<String>> {
    let pulsar = build_pulsar(&config.connect).await?;
    //todo: tuya
    let consumer: Consumer<Vec<u8>, TokioExecutor> = pulsar
        .consumer()
        .with_topic(topic)
        .with_consumer_name(&config.consumer_name)
        .with_subscription(&config.subscription)
        .with_subscription_type(SubType::Failover)
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("build consumer error: {:#}", e))?;

    Ok(consumer.topics())
}

#[cfg(test)]
mod tests {
    use crate::{config::task::PulsarTaskConfig, consumer::split_topics};
    use std::env;
    use taos::IntoDsn;

    #[tokio::test]
    #[ignore]
    async fn test_split_topics() -> anyhow::Result<()> {
        let pulsar_dsn = env::var("PULSAR_DSN_SPLIT")
            .unwrap_or("pulsar://192.168.2.131:6650?topics=persistent://public/default/pt-zgc&subscription=zgcdev1&consumer_name=zgcc1".to_string());
        let dsn = pulsar_dsn.into_dsn().expect("always valid");
        let config = PulsarTaskConfig::from_dsn(&dsn)?;
        dbg!(&config);
        let topic = config.topics.first().ok_or(anyhow::anyhow!(
            "pulsar task config must have at least one topic"
        ))?;
        let topics = split_topics(&config, topic).await?;
        dbg!(topics);
        Ok(())
    }
}
