use crate::{
    config::{
        connect::{DataVendor, PulsarConnectConfig},
        task::PulsarTaskConfig,
        tuya::ENCRYPT_MODEL,
    },
    consumer::build_pulsar,
    decrypt::Decryptor,
    message_sender::descypt_tuya,
};
use anyhow::Context;
use futures::TryStreamExt;
use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use rand::Rng;
use serde_json::json;
use std::{collections::HashMap, time::Duration};
use taos::Dsn;
use taosx_core::{task_set::prelude::DsSampleIn, utils::codec::Processor};

pub async fn get_sample(dsn: &Dsn, limit: usize, timeout: Duration) -> anyhow::Result<DsSampleIn> {
    let sample_list = get_sample_impl(dsn, limit, timeout).await?;

    let sample_json = json!({
        "input": sample_list,
        "parser": {}
    });

    let sample: DsSampleIn = serde_json::from_value(sample_json.clone()).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse pulsar sample data: {:?}, cause: {:?}",
            sample_json,
            err
        )
    })?;

    Ok(sample)
}

async fn get_sample_impl(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<HashMap<&'static str, String>>> {
    tracing::debug!(
        dsn = %dsn,
        limit = limit,
        timeout = timeout.as_millis(),
        "get_sample_impl params",
    );
    let conn_config = PulsarConnectConfig::from_dsn(dsn)?;
    let init_position = PulsarTaskConfig::parse_initial_position(dsn)?;
    let task_config = PulsarTaskConfig::from_dsn(dsn)?;
    let randi32 = rand::thread_rng().gen_range(100..999);
    let subscription_name = if conn_config.data_vendor == DataVendor::Tuya {
        task_config.subscription.clone()
    } else {
        format!("taosx-sample-{}", randi32)
    };
    let consumer_name = format!("taosx-{}", randi32);
    let topics = task_config.topics;
    // create consumer
    let pulsar: Pulsar<_> = build_pulsar(&conn_config).await?;
    let mut consumer: Consumer<Vec<u8>, _> = pulsar
        .consumer()
        .with_topics(&topics)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Failover)
        .with_options(ConsumerOptions {
            initial_position: init_position,
            ..Default::default()
        })
        .build()
        .await?;

    tracing_all_topics(&topics, &mut consumer).await?;

    let processor = PulsarTaskConfig::parse_codec_processor(dsn)?;

    // polling message from pulsar
    let deadline = tokio::time::Instant::now() + timeout;
    let mut count = 0;
    let mut payload_list = Vec::with_capacity(limit);
    loop {
        let message = tokio::time::timeout_at(deadline, consumer.try_next()).await;
        let Ok(message) = message else { break };
        match message {
            Ok(msg) => {
                let mut res = HashMap::new();
                let Some(msg) = msg else { continue };
                let decryptor = msg
                    .metadata()
                    .properties
                    .iter()
                    .filter_map(|kv| {
                        if kv.key == ENCRYPT_MODEL {
                            Some(Decryptor::from(kv.value.as_str()))
                        } else {
                            None
                        }
                    })
                    .next();

                let value = msg.deserialize();
                if value.is_empty() {
                    continue;
                }
                let value = processor.process(value)?;
                let payload = if conn_config.data_vendor == DataVendor::Tuya {
                    let value =
                        descypt_tuya(decryptor, conn_config.tuya_access_key.as_ref(), &value)
                            .with_context(|| {
                                format!(
                                    "pulsar tuya decrypt message error, msg properties: {:?}",
                                    msg.metadata().properties
                                )
                            })?;
                    String::from_utf8(value)?
                } else {
                    String::from_utf8(value)?
                };
                res.insert("payload", payload);
                payload_list.push(res);
                consumer.ack(&msg).await?;
            }
            Err(err) => {
                anyhow::bail!("Pulsar polling error: {:#}", err);
            }
        }
        count += 1;
        if !deadline.elapsed().is_zero() || count >= limit {
            break;
        }
    }
    if conn_config.data_vendor == DataVendor::Standard {
        consumer.unsubscribe().await?;
    }
    consumer.close().await?;

    Ok(payload_list)
}

async fn tracing_all_topics(
    topics: &[String],
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
) -> anyhow::Result<()> {
    let last_message_id = consumer.get_last_message_id().await;
    tracing::info!("last message id: {:?}", last_message_id);
    let tracking_topics = consumer.topics();
    tracing::info!("subscribed topics: {:?}", tracking_topics);
    let stats = consumer.get_stats().await?;
    tracing::info!("consumer stats: {:?}", stats);

    tracing::info!("client given topics: {:?}", topics);
    tracing::info!("pulsar tracking topics: {:?}", tracking_topics);
    Ok(())
}
