mod config;
mod message;
mod metrics;
mod publisher;
mod subscriber;
mod template;

use std::sync::Arc;

use anyhow::{Context, bail};
use taos::{AsAsyncConsumer, AsyncTBuilder, Dsn, Offset, TmqBuilder};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use template::Template;

use taosx_core::{
    core_metrics::get_metrics_arc_from_i64,
    tmq::{check_tmq_dsn, check_wal_enabled},
    utils::defer::defer,
};

use message::Message;
use metrics::Metrics;
use publisher::{GenericPublisher, Publisher};
use subscriber::Subscriber;

const MIN_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_RETRY_COUNT: i32 = 10;

/// from example: "tmq+ws://root:taosdata@taosd:6041/udt?group.id=astro_test_group&client.id=astro&auto.offset.reset=latest&with.meta=true&with.meta.delete=false&with.meta.drop=true"
/// to example: "mqtt://mosquitto:1883?version=5.0&qos=0&client_id=abcid&topic=taosx/tmq/sink/topic/{tmq_topic}/data&meta_topic=taosx/tmq/sink/topic/{tmq_topic}/meta"
pub async fn tmq_to_mqtt(
    from: &Dsn,
    to: &Dsn,
    task_id: Option<i64>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let mut tmq_config = config::TmqConfig::try_from(from)?;
    let mqtt_config = config::MqttConfig::try_from(to)?;

    let metrics = Arc::new(Metrics::new(get_metrics_arc_from_i64(task_id).await));
    metrics.reset();

    let mut tasks = JoinSet::new();

    let (message_tx, message_rx) = flume::bounded::<(Offset, Vec<Message>)>(1024);

    let cancel = cancel.child_token();

    for _ in 0..mqtt_config.concurrency {
        tasks.spawn({
            let cancel = cancel.clone();
            let publisher =
                GenericPublisher::new(mqtt_config.clone(), metrics.clone(), cancel.clone())
                    .await
                    .context("build mqtt publisher error")?;
            let message_rx = message_rx.clone();
            let topic = Template::new(&mqtt_config.topic).context("build topic template error")?;
            let meta_topic = mqtt_config
                .meta_topic
                .clone()
                .map(Template::new)
                .transpose()
                .context("build meta topic template error")?;
            async move {
                let _cancel_guard = cancel.clone().drop_guard();
                let _guard = defer(|| {
                    tracing::info!("tmq2mqtt publisher task exit");
                });

                loop {
                    let Some(Ok((_offset, messages))) =
                        cancel.run_until_cancelled(message_rx.recv_async()).await
                    else {
                        break;
                    };

                    for message in messages {
                        let topic = match (&message.inner, meta_topic.as_ref()) {
                            (message::MessageInner::Meta(_), Some(template)) => {
                                match template.render(&message.vars).map_err(anyhow::Error::new) {
                                    Ok(topic) => topic,
                                    Err(e) => {
                                        tracing::warn!(
                                            value = ?message.inner,
                                            vars = ?message.vars,
                                            "render meta topic template error: {e:#}"
                                        );
                                        continue;
                                    }
                                }
                            }
                            _ => match topic.render(&message.vars).map_err(anyhow::Error::new) {
                                Ok(topic) => topic,
                                Err(e) => {
                                    tracing::warn!(
                                        value = ?message.inner,
                                        vars = ?message.vars,
                                        "render topic template error: {e:#}"
                                    );
                                    continue;
                                }
                            },
                        };

                        let payload =
                            serde_json::to_vec(&message).context("serialize mqtt message error")?;
                        match cancel
                            .run_until_cancelled(publisher.publish(&topic, payload))
                            .await
                        {
                            Some(Ok(_)) => {}
                            Some(Err(e)) => match e {
                                publisher::Error::V3 {
                                    source: publisher::v3::Error::ConnectionTaskExit,
                                } => {
                                    break;
                                }
                                publisher::Error::V5 {
                                    source: publisher::v5::Error::ConnectionTaskExit,
                                } => {
                                    break;
                                }
                                e => {
                                    bail!("publish mqtt message error: {:#}", anyhow::Error::new(e))
                                }
                            },
                            None => break,
                        }
                    }
                }

                anyhow::Ok(())
            }
        });
    }
    drop(message_rx);

    // 因为 mqtt publish 目前 API 无法获取发送状态，因此此处使用自动提交
    tmq_config.dsn.set("enable.auto.commit", "true");
    tmq_config.dsn.remove("with.meta");
    let (tmq_dsn, builder, topics, with_meta_delete, with_meta_drop) =
        check_tmq_dsn(tmq_config.dsn).await?;
    check_wal_enabled(&builder, &topics).await?;

    for topic in topics {
        let vgroups = topic.vgroups;
        let name = topic.name;
        let tmq_builder =
            Arc::new(TmqBuilder::from_dsn(&tmq_dsn).context("tmq builder from dsn error")?);
        let with_meta = tmq_config.with_meta;
        for _ in 0..vgroups {
            tasks.spawn({
                let cancel = cancel.clone();
                let metrics = metrics.clone();
                let message_tx = message_tx.clone();
                let tmq_builder = tmq_builder.clone();
                let topic_name = name.clone();
                async move {
                    let _cancel_guard = cancel.clone().drop_guard();
                    let _guard = defer(|| {
                        tracing::info!("tmq2mqtt subscriber on topic {topic_name} task exit");
                    });
                    let mut retry_count = 0;
                    let mut retry_interval = None;
                    'outer: loop {
                        let mut consumer = tmq_builder
                            .build()
                            .await
                            .context("build tmq consumer error")?;
                        let res = consumer.subscribe([&topic_name]).await;
                        let subscriber = match res {
                            Ok(_) => Subscriber::new(
                                consumer,
                                with_meta,
                                with_meta_delete,
                                with_meta_drop,
                            ),
                            Err(e) => {
                                tracing::error!("consumer subscribe error: {e:#}");
                                if retry_count >= MAX_RETRY_COUNT {
                                    anyhow::bail!("tmq builder retry too many times, exit task");
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
                                tracing::warn!(retry_count, "Wait for {duration:?} to reconnect");
                                if cancel
                                    .run_until_cancelled(tokio::time::sleep(duration))
                                    .await
                                    .is_none()
                                {
                                    break;
                                }
                                continue;
                            }
                        };
                        tracing::info!("TMQ subscribe successfully");
                        loop {
                            let Some(res) = cancel.run_until_cancelled(subscriber.next()).await
                            else {
                                break 'outer;
                            };
                            let sub = match res {
                                Ok(res) => res,
                                Err(e) => {
                                    let err_str = match &e {
                                        subscriber::Error::FetchMessage { source } => {
                                            format!("{source:#}")
                                        }
                                        subscriber::Error::FetchRawBlock { source } => {
                                            format!("{source:#}")
                                        }
                                    };

                                    if !(err_str.contains("0xE001")
                                        || err_str.contains("0xE002")
                                        || err_str.contains("0xE003")
                                        || err_str.contains("0xE004")
                                        || err_str.contains("0xE00B"))
                                    {
                                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                        // 0xE001: internal error
                                        // 0xE002: connection closed
                                        // 0xE003: send timeout
                                        // 0xE004: receive timeout
                                        // 0x000B: unable to establish connection
                                        bail!(
                                            "consumer exit cause fetch message error: {:#}",
                                            anyhow::Error::new(e)
                                        );
                                    }
                                    tracing::error!(
                                        "subscribe message error: {:#}",
                                        anyhow::Error::new(e)
                                    );
                                    break;
                                }
                            };
                            if let Some((offset, messages)) = sub {
                                if messages.is_empty() {
                                    continue;
                                }
                                let len = messages.len();
                                if cancel
                                    .run_until_cancelled(message_tx.send_async((offset, messages)))
                                    .await
                                    .is_none_or(|r| r.is_err())
                                {
                                    break 'outer;
                                }
                                metrics.add_received_messages(len as _);
                            }
                        }
                    }
                    Ok(())
                }
            });
        }
    }

    drop(message_tx);

    let mut has_error = false;
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("tmq2mqtt task exit with error: {e:#}");
                has_error = true;
            }
            Err(e) => {
                tracing::error!("tmq2mqtt task panicked: {e}");
                has_error = true;
            }
        }
    }
    if has_error {
        anyhow::bail!("task exit with error, waiting to restart");
    }

    Ok(())
}
