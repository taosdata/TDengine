mod config;
mod message;
mod metrics;
mod publisher;
mod subscriber;
mod template;

use std::sync::Arc;

use anyhow::Context;
use taos::{Dsn, Offset};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use template::Template;

use taosx_core::{core_metrics::get_metrics_arc_from_i64, utils::defer::defer};

use message::Message;
use metrics::Metrics;
use publisher::{GenericPublisher, Publisher};
use subscriber::Subscriber;

const MIN_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_RETRY_COUNT: i32 = 10;

/// from example: "tmq+ws://root:taosdata@fractal:6041/dmeters?group.id=astro_test_group&client.id=astro_test_client&auto.offset.reset=earliest&experimental.snapshot.enable=true&with_meta=true&enable.auto.commit=true"
/// to example: "mqtt://localhost:1883?version=5.0&qos=0&client_id=abcid&topic=taosx/tmq/sink/topic/{tmq_topic}/data&meta_topic=taosx/tmq/sink/topic/{tmq_topic}/meta"
pub async fn tmq_to_mqtt(
    from: &Dsn,
    to: &Dsn,
    task_id: Option<i64>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let tmq_config = config::TmqConfig::try_from(from)?;
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
                        publisher
                            .publish(&topic, payload, &cancel)
                            .await
                            .context("publish mqtt message error")?;
                    }
                }

                anyhow::Ok(())
            }
        });
    }
    drop(message_rx);

    for _ in 0..tmq_config.concurrency {
        tasks.spawn({
            let tmq_config = tmq_config.clone();
            let cancel = cancel.clone();
            let metrics = metrics.clone();
            let message_tx = message_tx.clone();
            async move {
                let _cancel_guard = cancel.clone().drop_guard();
                let _guard = defer(|| {
                    tracing::info!("tmq2mqtt subscriber task exit");
                });

                let mut retry_count = 0;
                let mut retry_interval = None;
                'outer: loop {
                    let Some(res) = cancel
                        .run_until_cancelled(Subscriber::new(tmq_config.clone()))
                        .await
                    else {
                        break;
                    };
                    let subscriber = match res.map_err(anyhow::Error::new) {
                        Ok(subscriber) => subscriber,
                        Err(e) => {
                            tracing::error!("create subscriber error: {e:#}");
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
                        let Some(res) = cancel.run_until_cancelled(subscriber.next()).await else {
                            break 'outer;
                        };
                        let sub = match res.map_err(anyhow::Error::new) {
                            Ok(res) => res,
                            Err(e) => {
                                tracing::error!("subscribe message error: {e:#}");
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
