use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use arrow::ipc::writer::StreamWriter;
use arrow_schema::Schema;
use batch::{RecordBatchBuilder, build_schema};
use client::{GenericMessagePoller, MessagePoller};
use config::MqttConnectConfig;
use flume::TrySendError;
use futures::pin_mut;
use metrics::MqttMetrics;
use serde_json::json;
use taos::Dsn;
use taosx_ipc::ack::{AckReaderBuilder, AckType};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

use taosx_core::core_metrics::get_metrics_arc_from_i64;
use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::runners::{get_data_dir, set_tcp_keepalive};
use taosx_core::plugins::transform::sample::DsSampleIn;
use taosx_core::sink::persist::PersistConfig;
use taosx_core::utils::codec::Processor;
use taosx_core::{Parser, Transferred, build_ipc};

use crate::config::MqttConfig;

mod batch;
pub mod client;
pub mod config;
mod dump;
mod metrics;
pub mod topic;

pub const MQTT_ID: &str = "mqtt";

/// Run the mqtt DataIn task
#[instrument(skip_all)]
pub async fn mqtt_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    upstream_cancel_token: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: taosx_core::TaskNotifySender,
) -> anyhow::Result<()> {
    let cancel_token = upstream_cancel_token.child_token();
    let _drop_token_guard = cancel_token.clone().drop_guard();

    tracing::info!(task_id, ?from, ?to, "MQTT task start");

    if with_agent.is_some() {
        let task_id = task_id.context("Task id not found for agent runner")?;
        let _ = taosx_core::core_metrics::init_task_metrics(&from, &to, task_id, None).await;
    }
    let metrics = get_metrics_arc_from_i64(task_id).await;
    let metrics = Arc::new(MqttMetrics::new(metrics));

    metrics.reset_metrics();

    let config: MqttConfig = from.try_into()?;
    let schema = Arc::new(build_schema(config.topic_pattern.as_ref()));

    let tid = task_id
        .or(with_agent.as_ref().map(|a| a.0))
        .context("task id not found")?;
    let persist_config = config.persist_data.as_ref().map(|c| PersistConfig {
        task_id: tid,
        record_metrics: true,
        schemas: HashMap::from_iter([(
            schema.clone(),
            c.dir.clone().unwrap_or_else(|| {
                get_data_dir()
                    .join("tasks")
                    .join(tid.to_string())
                    .join("persist_queue")
            }),
        )]),
        batch_size: Some(config.task.batch_size),
        batch_timeout: Some(Duration::from_millis(config.task.batch_timeout as u64)),
        batch_chunk_size: None,
    });

    let (mut ipc_server_handle, socket) = build_ipc(
        None,
        parser,
        &to,
        Some(MQTT_ID),
        None,
        None,
        &cancel_token,
        with_agent,
        transferred,
        task_id,
        notify.clone(),
        persist_config,
    )
    .await
    .context("build ipc error")?;

    let mut tasks = match execute(
        socket,
        task_id,
        config,
        schema.clone(),
        metrics.clone(),
        cancel_token.clone(),
    )
    .await
    {
        Ok(tasks) => tasks,
        Err(e) => {
            tracing::error!("start execute MQTT tasks error: {e:#}");
            cancel_token.cancel();
            ipc_server_handle.close().await.ok();
            return Err(e).context("start execute MQTT tasks error");
        }
    };

    macro_rules! safe_exit {
        () => {
            cancel_token.cancel();
            tasks.abort_all();
            ipc_server_handle.close().await.ok();
            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok(_) => {}
                    Err(err) if err.is_cancelled() => {}
                    Err(err) if err.is_panic() => {
                        tracing::error!("mqtt job paniced: {err}")
                    }
                    Err(err) => {
                        tracing::warn!("mqtt job exit with err: {err}")
                    }
                }
            }
        };
    }

    loop {
        tokio::select! {
            res = tasks.join_next() => {
                match res {
                    Some(Ok(Ok(_))) => {},
                    Some(Ok(Err(e))) => {
                        safe_exit!();
                        tracing::error!("MQTT client exit with error: {e:#}");
                        return Err(e);
                    }
                    Some(Err(e)) => {
                        safe_exit!();
                        tracing::error!("MQTT client paniced: {e:#}");
                        return Err(e).context("MQTT task paniced");
                    }
                    None => break,
                }
            },

            err = ipc_server_handle.recv_error() => {
                if let Some(e) = err {
                    tracing::info!("have received worker thread panicked message, terminate child process: {e:#}");
                    safe_exit!();
                    bail!("MQTT ipc write error: {e}");
                }
            }

            _ = upstream_cancel_token.cancelled() => {
                tracing::info!("MQTT task received shutdown signal");
                break
            }
        }
    }

    safe_exit!();
    tracing::info!(task_id, "MQTT task finished");

    Ok(())
}

#[instrument(skip_all)]
async fn execute(
    socket: std::net::SocketAddr,
    task_id: Option<i64>,
    config: MqttConfig,
    schema: Arc<Schema>,
    mqtt_metrics: Arc<MqttMetrics>,
    cancel_token: CancellationToken,
) -> anyhow::Result<JoinSet<Result<(), anyhow::Error>>> {
    let mut tasks = JoinSet::new();

    tasks.spawn(
        {
            let token = cancel_token.clone();
            let mqtt_metrics = mqtt_metrics.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {
                            mqtt_metrics.update_metrics();
                        }
                        _ = token.cancelled() => break,
                    }
                }
                Ok(())
            }
        }
        .instrument(tracing::info_span!("mqtt_metrics_update")),
    );

    // read ack
    let (permit_tx, permit_rx) = flume::bounded(config.task.maximum_processing_batch);
    // add permits
    while permit_tx.try_send(()).is_ok() {}

    let ack_read_stream =
        std::net::TcpStream::connect(socket).context("Connect to MQTT IPC server error")?;

    let ipc_stream = ack_read_stream
        .try_clone()
        .context("Clone tcp stream as ipc write stream error")?;

    tasks.spawn_blocking({
        let metrics = mqtt_metrics.clone();
        let span = tracing::info_span!("mqtt_ack_receiver");
        move || {
            let _entered_span = span.enter();
            let ack_stream = {
                set_tcp_keepalive(&ack_read_stream).context("Set ack read stream keepalive error")?;
                ack_read_stream
                    .set_read_timeout(None)
                    .context("Set ack read stream read timeout error")?;
                AckReaderBuilder::new(AckType::Lush).open(ack_read_stream).context("failed to open ack stream")?
            };

            for ack in ack_stream {
                metrics.add_fetched_acks();
                // add permit
                permit_tx.send(()).ok();
                // handle ack error
                if !ack.success() {
                    metrics.add_ack_fails();
                    tracing::error!(ack.code = %ack.code(), ack.message = ack.message(), ack.context = ack.context(), "MQTT ack error");
                    match ack.message() {
                        Some(message) => bail!("MQTT ipc writer error: {message}"),
                        None => bail!("MQTT ipc writer error with code: {}", ack.code()),
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }
    });

    // ipc writer
    // let ipc_limiter = Arc::new(Semaphore::new(config.task.maximum_processing_batch));
    let (batch_tx, batch_rx) = flume::bounded(config.task.maximum_processing_batch);
    let mut ipc_writer = {
        set_tcp_keepalive(&ipc_stream).context("Set ipc write stream keepalive error")?;
        ipc_stream
            .set_read_timeout(None)
            .context("Set ipc write stream read timeout error")?;
        StreamWriter::try_new(ipc_stream, &schema).context("Build ipc stream writer error")?
    };
    tasks.spawn_blocking({
        let metrics = mqtt_metrics.clone();
        let span = tracing::info_span!("mqtt_ipc_writer");
        move || {
            let _entered_span = span.enter();
            loop {
                if permit_rx.recv().is_err() {
                    break;
                }
                let Ok(batch) = batch_rx.recv() else { break };
                ipc_writer
                    .write(&batch)
                    .context("Write batch to ipc writer error")?;
                metrics.add_sent_batches();
            }
            ipc_writer.finish().context("Flush ipc writer error")?;
            Ok(())
        }
    });

    // build client
    let mut poller = GenericMessagePoller::from_config(&config.mqtt, config.topics)
        .await
        .context("build MQTT poller from config error")?;

    // read from mqtt
    let (message_tx, message_rx) = flume::bounded(config.task.unprocessed_messages_buffer_size);
    let (dump_tx, dump_rx) = config
        .dump
        .as_ref()
        .is_some_and(|c| c.enable)
        .then(|| flume::bounded(10000))
        .unzip();

    let persist_enable = config.persist_data.is_some();
    tasks.spawn({
        let mqtt_metrics = mqtt_metrics.clone();
        async move {
            loop {
                match poller.poll().await {
                    Ok(message) => {
                        mqtt_metrics.clone().add_fetched_messages();
                        if persist_enable {
                            match message_tx.send_async(message.clone()).await {
                                Ok(_) => {
                                    mqtt_metrics.add_unprocessed_messages();
                                    mqtt_metrics.add_received_bytes(message.payload.len() as _);
                                }
                                Err(_) => {
                                    tracing::warn!("MQTT task exit, stop polling...");
                                    bail!("MQTT task exit")
                                }
                            }
                        } else {
                            match message_tx.try_send(message.clone()) {
                                Ok(_) => {
                                    mqtt_metrics.add_unprocessed_messages();
                                    mqtt_metrics.add_received_bytes(message.payload.len() as _);
                                }
                                Err(TrySendError::Full(_)) => {
                                    mqtt_metrics.add_discarded_messages();
                                }
                                Err(TrySendError::Disconnected(_)) => {
                                    tracing::warn!("MQTT task exit, stop polling...");
                                    bail!("MQTT task exit")
                                }
                            };
                        }

                        if let Some(dump_tx) = &dump_tx {
                            if persist_enable {
                                match dump_tx.send_async(message).await {
                                    Ok(_) => {}
                                    Err(_) => {
                                        tracing::warn!("MQTT task exit, stop polling...");
                                        bail!("MQTT task exit")
                                    }
                                }
                            } else {
                                match dump_tx.try_send(message) {
                                    Ok(_) => {}
                                    Err(TrySendError::Full(_)) => {
                                        mqtt_metrics.clone().add_discarded_dump_messages();
                                    }
                                    Err(TrySendError::Disconnected(_)) => {
                                        tracing::warn!("MQTT task exit, stop polling...");
                                        bail!("MQTT task exit")
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Fetch MQTT message error: {e:#}");
                        return Err(e).context("Fetch MQTT message error");
                    }
                }
            }
        }
        .instrument(tracing::info_span!("mqtt_message_poller"))
    });

    // dump
    if let Some(dump_rx) = dump_rx {
        tasks.spawn_blocking({
            let config = config.dump.unwrap();
            let metrics = mqtt_metrics.clone();
            let span = tracing::info_span!("mqtt_dump");
            move || {
                let _entered_span = span.enter();
                let mut writer = {
                    let path = config
                        .path
                        .or_else(|| {
                            task_id.map(|id| {
                                get_data_dir()
                                    .join("tasks")
                                    .join(format!("{id}"))
                                    .join("rawdata")
                            })
                        })
                        .context("Dump path is required")?;
                    let writer = dump::RollingFileAppender::new(
                        path,
                        config.keep as i64,
                        chrono::Local::now,
                    )?;
                    csv::WriterBuilder::new().from_writer(writer)
                };
                while let Ok(message) = dump_rx.recv() {
                    if let Err(e) = writer.write_record([
                        message.ts.to_string().as_bytes(),
                        message.topic.as_bytes(),
                        message.qos.to_string().as_bytes(),
                        &message.payload,
                    ]) {
                        tracing::error!("Dump MQTT message error: {e}");
                        continue;
                    }
                    if let Err(e) = writer.flush() {
                        tracing::error!("Dump MQTT message flush error: {e}");
                        continue;
                    }
                    metrics.clone().add_dumped_messages();
                }
                writer.flush().ok();

                Ok(())
            }
        });
    }

    // build batch
    let get_parallel = || {
        std::thread::available_parallelism()
            .ok()
            .map(std::num::NonZero::<usize>::get)
    };
    let parallel = std::env::var("TAOSX_MQTT_BUILD_BATCH_PARRALLEL")
        .ok()
        .and_then(|v| v.parse().ok())
        .or_else(get_parallel)
        .unwrap_or(10);
    let (tx, rx) = flume::bounded(parallel);

    for _ in 0..parallel {
        let rx = rx.clone();
        let schema = schema.clone();
        let codec_processor = config.codec_processor;
        let topic_pattern = config.topic_pattern.clone();
        let cancel_token = cancel_token.clone();
        let batch_tx = batch_tx.clone();
        tasks.spawn({
            async move {
                let _guard = cancel_token.clone().drop_guard();
                loop {
                    tokio::select! {
                        res = rx.recv_async() => {
                            let Ok(chunk) = res else {
                                break;
                            };
                            let mut builder = RecordBatchBuilder::new(
                                schema.clone(),
                                codec_processor,
                                topic_pattern.clone(),
                                config.task.batch_size.max(1024),
                            );
                            let batch = tokio::task::spawn_blocking(move || {
                                builder.build(chunk)
                            }).await??;
                            tokio::select! {
                                res = batch_tx.send_async(batch) => match res {
                                    Ok(_) => continue,
                                    Err(_) => {
                                        tracing::warn!("ipc writer loop exited");
                                        break
                                    }
                                },
                                _ = cancel_token.cancelled() => break,
                            }
                        },
                        _ = cancel_token.cancelled() => break,
                    }
                }
                Ok(())
            }
        });
    }
    tasks.spawn(
        async move {
            let _guard = cancel_token.clone().drop_guard();
            let chunk_stream = message_rx.into_stream().chunks_timeout(
                config.task.batch_size,
                Duration::from_millis(config.task.batch_timeout as u64),
            );

            pin_mut!(chunk_stream);
            loop {
                tokio::select! {
                    res = chunk_stream.next() => {
                        let Some(chunk) = res else {
                            break
                        };
                        mqtt_metrics.sub_unprocessed_messages(chunk.len() as u64);
                        tokio::select! {
                            res = tx.send_async(chunk) => match res {
                                Ok(_) => continue,
                                Err(_) => {
                                    tracing::warn!("build batch loop exited");
                                    break
                                }
                            },
                            _ = cancel_token.cancelled() => break,
                        }
                    }
                    _ = cancel_token.cancelled() => break,
                }
            }

            Ok(())
        }
        .instrument(tracing::info_span!("mqtt_batch_stream")),
    );

    Ok(tasks)
}

/// Check the connectivity of the mqtt server
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match TryInto::<MqttConnectConfig>::try_into(dsn) {
        Err(err) => DataSourceValidation::invalid(
            "mqtt".to_string(),
            format!("invalid mqtt dsn: {}, cause: {}", dsn, err),
        ),
        Ok(mut config) => {
            // generate a unique client if for validate operation
            config
                .client_id
                .push_str(&format!("_validate_{}", uuid::Uuid::new_v4().simple()));
            match GenericMessagePoller::try_connect(&config).await {
                Ok(_) => DataSourceValidation::valid("mqtt", None),
                Err(e) => DataSourceValidation::invalid(
                    "mqtt",
                    format!(
                        "failed to connect to dsn: {}, {:#}",
                        dsn,
                        anyhow::Error::new(e)
                    ),
                ),
            }
        }
    }
}

/// get sample data from mqtt server
pub async fn get_sample(dsn: &Dsn, limit: usize, timeout: Duration) -> anyhow::Result<DsSampleIn> {
    let samples = get_sample_message(dsn, limit, timeout).await?;

    let sample_json = json!({
        "input": samples,
        "parser": {}
    });

    let sample =
        serde_json::from_value(sample_json).context("failed to parse mqtt sample data to json")?;

    Ok(sample)
}

async fn get_sample_message(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<HashMap<String, String>>> {
    let mut config: MqttConfig = dsn.try_into()?;
    // generate a unique client if for get sample operation
    config
        .mqtt
        .client_id
        .push_str(&format!("_sample_{}", uuid::Uuid::new_v4().simple()));
    let mut poller = GenericMessagePoller::from_config(&config.mqtt, config.topics)
        .await
        .context("build MQTT poller from config error")?;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut count = 0;
    let mut res = Vec::with_capacity(limit);
    loop {
        if count >= limit {
            return Ok(res);
        }

        let message = match tokio::time::timeout_at(deadline, poller.poll()).await {
            Ok(res) => res.context("No MQTT message found")?,
            Err(_elapsed) => return Ok(res),
        };

        let mut map = HashMap::with_capacity(limit);
        if let Some(pattern) = config.topic_pattern.as_mut() {
            map.extend(pattern.parse_topic(&message.topic)?);
        }

        map.insert(
            "payload".to_string(),
            config
                .codec_processor
                .process(message.payload.to_vec())
                .and_then(|s| {
                    String::from_utf8(s).context("parse mqtt string message from bytes error")
                })?,
        );

        res.push(map);
        count += 1;
    }
}
