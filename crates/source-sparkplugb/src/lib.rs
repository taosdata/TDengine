use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use arrow::{array::RecordBatch, ipc::writer::StreamWriter};
use arrow_schema::Schema;
use batch::{BatchBuilder, BatchEntry, BatchPayload};
use config::{Config, MessageType};
use faststr::FastStr;
use futures::pin_mut;
use metrics::Metrics;
use taos::Dsn;
use taosx_core::{TaskNotifySender, core_metrics};
use taosx_ipc::ack::{AckReaderBuilder, AckType};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use taosx_core::{
    build_ipc, core_metrics::get_metrics_arc_from_i64, plugins::Parser,
    utils::futs_helper::select_cancel,
};

use source_mqtt::{
    client::{GenericMessagePoller, MessagePoller},
    config::MqttConnectConfig,
};

use taosx_core::runners::set_tcp_keepalive;

mod batch;
pub mod config;
mod metrics;
mod pb;
mod proto;
pub mod sample;
mod topic;
pub mod validate;
mod variables;

pub const SPARKPLUGB_ID: &str = "sparkplugb";

pub async fn sparkplugb_to_taos(
    from: &Dsn,
    to: &Dsn,
    with_agent: Option<(i64, String, String)>,
    parser: Option<Parser>,
    task_id: Option<i64>,
    notify: TaskNotifySender,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let cancel_token = cancel.child_token();
    let _guard = cancel_token.clone().drop_guard();
    tracing::info!(task_id, ?from, ?to, "SparkplugB task start");

    if with_agent.is_some() {
        let task_id = task_id.context("task id not found for agent runner")?;
        core_metrics::init_task_metrics(from, to, task_id, None)
            .await
            .context("init task metrics error")?;
    }
    let metrics = Arc::new(Metrics::new(get_metrics_arc_from_i64(task_id).await));
    metrics.reset();

    let config: Config = from.try_into()?;

    let (mut ipc_server_handle, socket) = build_ipc(
        None,
        parser,
        to,
        Some(SPARKPLUGB_ID),
        None,
        None,
        cancel,
        with_agent,
        None,
        task_id,
        notify,
        None,
    )
    .await
    .context("run ipc error")?;

    let mut tasks = match execute(socket, config, metrics.clone(), &cancel_token).await {
        Ok(tasks) => tasks,
        Err(e) => {
            tracing::error!("start execute SparkplugB tasks error: {e:#}");
            cancel_token.cancel();
            ipc_server_handle.close().await.ok();
            return Err(e).context("start execute SparkplugB tasks error");
        }
    };

    macro_rules! safe_exit {
        () => {
            cancel_token.cancel();
            ipc_server_handle.close().await.ok();
            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok(_) => {}
                    Err(err) if err.is_cancelled() => {}
                    Err(e) if e.is_panic() => {
                        tracing::error!("SparkplugB task paniced: {e}");
                    }
                    Err(e) => {
                        tracing::error!("SparkplugB task exit with error: {e:#}");
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
                        tracing::error!("SparkplugB task exit with error: {e:#}");
                        return Err(e);
                    }
                    Some(Err(e)) => {
                        safe_exit!();
                        tracing::error!("SparkplugB task paniced: {e:#}");
                        return Err(e).context("MQTT task paniced");
                    }
                    None => break,
                }
            },

            err = ipc_server_handle.recv_error() => {
                if let Some(e) = err {
                    tracing::info!("SparkplugB received worker thread panicked message, terminate child process: {e:#}");
                    safe_exit!();
                    anyhow::bail!("SparkplugB IPC error: {e}");
                }
            }

            _ = cancel_token.cancelled() => {
                tracing::info!("MQTT task received shutdown signal");
                break
            }
        }
    }

    safe_exit!();
    tracing::info!(task_id, "MQTT task finished");
    Ok(())
}

#[tracing::instrument(skip_all, fields(task_name = SPARKPLUGB_ID))]
async fn execute(
    socket: SocketAddr,
    config: Config,
    metrics: Arc<Metrics>,
    cancel: &CancellationToken,
) -> anyhow::Result<JoinSet<anyhow::Result<()>>> {
    let mqtt_configs = config
        .mqtt
        .mqtt_config()
        .context("parse mqtt config error")?;
    let mut tasks = JoinSet::new();

    let parallel = env_parallel("TAOSX_SPARKPLUGB_TASK_PARRALLEL")
        .or_else(system_parallel)
        .unwrap_or(10);
    for mqtt_config in mqtt_configs {
        execute_broker(
            parallel,
            socket,
            &config,
            mqtt_config,
            metrics.clone(),
            &cancel.child_token(),
            &mut tasks,
        )
        .await?;
    }
    Ok(tasks)
}

async fn execute_broker(
    parallel: usize,
    socket: SocketAddr,
    config: &Config,
    mqtt_config: MqttConnectConfig,
    metrics: Arc<Metrics>,
    cancel: &CancellationToken,
    tasks: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    // 创建 client
    let poller = GenericMessagePoller::from_config(&mqtt_config, config.subscribe.subscriptions())
        .await
        .context("create mqtt poller error")?;
    let client = poller.client();

    // 接收 MQTT 消息，发送给下游
    let (schema_tx, schema_rx) = flume::bounded::<(Arc<Schema>, BatchPayload)>(10000);
    tasks.spawn(
        process_message(
            poller,
            schema_tx,
            config.subscribe.send_rebirth_cmd(),
            Some(metrics.clone()),
            cancel.clone(),
        )
        .in_current_span(),
    );

    tasks.spawn({
        let cancel = cancel.clone();
        async move {
            let _guard = taosx_core::utils::defer::defer(|| {
                if !cancel.is_cancelled() {
                    tracing::warn!("execute_broker schema task exit");
                    cancel.cancel();
                } else {
                    tracing::debug!("execute_broker schema task exit")
                }
            });
            let mut schema_map = HashMap::<Arc<Schema>, flume::Sender<BatchPayload>>::new();
            let mut tasks = JoinSet::<anyhow::Result<()>>::new();
            loop {
                let Some(Ok((schema, payload))) =
                    select_cancel(schema_rx.recv_async(), &cancel).await
                else {
                    break;
                };
                // 已存在处理任务，直接发送给下游
                let payload = match schema_map.get(&schema) {
                    Some(sender) => {
                        match select_cancel(sender.send_async(payload), &cancel).await {
                            Some(Ok(_)) => continue,
                            Some(Err(e)) => {
                                tracing::info!("ipc task exit, rebuild...");
                                e.0
                            }
                            None => break,
                        }
                    }
                    None => payload,
                };
                // 创建处理任务
                let entry_tx = start_task(
                    socket,
                    schema.clone(),
                    parallel,
                    &mut tasks,
                    metrics.clone(),
                    &cancel.child_token(),
                )
                .await?;
                schema_map.insert(schema.clone(), entry_tx.clone());
                if select_cancel(entry_tx.send_async(payload), &cancel)
                    .await
                    .is_none_or(|v| v.is_err())
                {
                    break;
                }
            }
            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::error!("sparkplugb task exit with error: {e:#}")
                    }
                    Err(e) => {
                        tracing::error!("sparkplugb task paniced: {e}")
                    }
                }
            }
            Ok(())
        }
        .in_current_span()
    });

    // 如果指定了设备名，先下发 rebirth 命令
    if config.subscribe.send_rebirth_cmd() {
        if let Some(topics) = config.subscribe.rebirth_topics() {
            let payload = pb::rebirth_payload();
            for topic in topics {
                if !client.publish(&topic, 1, payload.clone()).await? {
                    anyhow::bail!("mqtt poller dropped when send rebirth cmd")
                }
            }
        }
    }

    Ok(())
}

async fn process_message(
    mut poller: GenericMessagePoller,
    schema_tx: flume::Sender<(Arc<Schema>, BatchPayload)>,
    send_rebirth_cmd: bool,
    metrics: Option<Arc<Metrics>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let _guard = taosx_core::utils::defer::defer(|| {
        if !cancel.is_cancelled() {
            tracing::warn!("process message task exit");
            cancel.cancel();
        } else {
            tracing::debug!("process message task exit")
        }
    });
    let client = poller.client();
    let mut node_alias_map = HashMap::<FastStr, HashMap<u64, FastStr>>::new();
    'OUTER: loop {
        let Some(message) = select_cancel(poller.poll(), &cancel)
            .await
            .transpose()
            .context("poll mqtt message error")?
        else {
            break;
        };
        if let Some(metrics) = metrics.as_ref() {
            metrics.add_received_messages();
        }

        let entry: BatchEntry = message.try_into()?;
        let payloads = entry.payloads().context("parse payload error")?;
        for (schema, mut payload) in payloads {
            if let BatchPayload::Metric(metric) = &mut payload {
                let node_id = metric.id();
                let alias_map = node_alias_map.entry(node_id).or_default();
                match metric.fill_metric_name(alias_map) {
                    Ok(true) => {}
                    Ok(false) => {
                        // 下发 rebirth 命令
                        if send_rebirth_cmd {
                            let topic = metric.rebirth_topic();
                            let payload = pb::rebirth_payload();
                            if select_cancel(client.publish(&topic, 1, payload), &cancel)
                                .await
                                .is_none_or(|v| v.is_err())
                            {
                                break 'OUTER;
                            }
                        }
                        continue 'OUTER;
                    }
                    Err(e) => {
                        tracing::error!("fill metric name error: {:#}", anyhow::Error::new(e));
                        continue;
                    }
                }
                if matches!(
                    metric.message_type,
                    MessageType::NBirth | MessageType::DBirth
                ) {
                    if let Some((name, alias)) = metric.name_alias() {
                        alias_map.insert(alias, name);
                    }
                }
            }
            if select_cancel(schema_tx.send_async((schema, payload)), &cancel)
                .await
                .is_none_or(|v| v.is_err())
            {
                break 'OUTER;
            }
            if let Some(metrics) = metrics.as_ref() {
                metrics.add_received_metrics();
            }
        }
    }
    Ok(())
}

async fn start_task(
    socket: SocketAddr,
    schema: Arc<Schema>,
    parallel: usize,
    tasks: &mut JoinSet<anyhow::Result<()>>,
    metrics: Arc<Metrics>,
    cancel: &CancellationToken,
) -> anyhow::Result<flume::Sender<BatchPayload>> {
    let (entry_tx, entry_rx) = flume::bounded::<BatchPayload>(1000);
    let (batch_tx, batch_rx) = flume::bounded::<RecordBatch>(10000);

    let build_batch = {
        let schema = schema.clone();
        move |chunks: Vec<BatchPayload>| {
            let batch = BatchBuilder::new(schema.clone())
                .context("build schema error")?
                .build(&chunks)
                .context("build batch error")?;
            Ok(vec![batch])
        }
    };
    parallel_exec(parallel, entry_rx, build_batch, batch_tx, tasks, cancel);

    let stream = tokio::net::TcpStream::connect(socket)
        .await
        .with_context(|| format!("connect to {socket} error"))?
        .into_std()
        .context("convert tokio stream to std error")?;
    stream
        .set_nonblocking(false)
        .context("set tcp stream blocking error")?;
    set_tcp_keepalive(&stream).context("set tcp stream keepalive error")?;
    stream
        .set_read_timeout(None)
        .context("set tcp stream read timeout error")?;
    let (permit_tx, permit_rx) = flume::bounded::<()>(200);
    tasks.spawn_blocking({
        let ack_stream = stream.try_clone().context("clone stream error")?;
        let metrics = metrics.clone();
        move || {
            let ack_reader =
                AckReaderBuilder::new(AckType::Lush).open(ack_stream)
                    .context("Open ACK reader failed")?;
            for ack in ack_reader {
                permit_rx.try_recv().ok();
                metrics.add_fetched_acks();
                if !ack.success() {
                    tracing::error!(code = %ack.code(), message = ack.message(), context = ack.context(), "ack error");
                    anyhow::bail!("ipc ack error: {}, message: {:?}", ack.code(), ack.message());
                }
            }

            anyhow::Ok(())
        }
    });
    tasks.spawn_blocking({
        let schema = schema.clone();
        move || {
            let mut writer =
                StreamWriter::try_new(stream, &schema).context("build ipc batch writer error")?;
            loop {
                if permit_tx.send(()).is_err() {
                    break;
                }
                let Ok(batch) = batch_rx.recv() else { break };
                if let Err(e) = writer.write(&batch) {
                    writer.finish().ok();
                    return Err(e).context("write batch error");
                };
                metrics.add_sent_batches();
            }
            writer.finish().ok();
            Ok(())
        }
    });

    Ok(entry_tx)
}

fn parallel_exec<I, F, O>(
    parallel: usize,
    input_rx: flume::Receiver<I>,
    f: F,
    output_tx: flume::Sender<O>,
    tasks: &mut JoinSet<anyhow::Result<()>>,
    cancel: &CancellationToken,
) where
    F: FnOnce(Vec<I>) -> anyhow::Result<Vec<O>> + Clone + Send + 'static,
    O: Send + 'static,
    I: Send + 'static,
{
    for _ in 0..parallel {
        tasks.spawn({
            let cancel = cancel.clone();
            let output_tx = output_tx.clone();
            let input_rx = input_rx.clone();
            let f = f.clone();
            async move {
                let _guard = taosx_core::utils::defer::defer(|| {
                    if !cancel.is_cancelled() {
                        tracing::warn!("parallel execute task exit");
                        cancel.cancel();
                    } else {
                        tracing::debug!("parallel execute task exit")
                    }
                });
                let stream = {
                    use tokio_stream::StreamExt;
                    input_rx
                        .into_stream()
                        .chunks_timeout(100, Duration::from_millis(10))
                };
                pin_mut!(stream);
                use futures::stream::StreamExt;
                'OUTER: loop {
                    let Some(Some(chunks)) = select_cancel(stream.next(), &cancel).await else {
                        break;
                    };
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    rayon::spawn({
                        let f = f.clone();
                        move || {
                            tx.send(f(chunks)).ok();
                        }
                    });
                    match select_cancel(rx, &cancel).await {
                        Some(Ok(Ok(outputs))) => {
                            for output in outputs {
                                if select_cancel(output_tx.send_async(output), &cancel)
                                    .await
                                    .is_none_or(|v| v.is_err())
                                {
                                    break 'OUTER;
                                }
                            }
                        }
                        Some(Ok(Err(e))) => return Err(e)?,
                        Some(_) => {}
                        None => break,
                    }
                }
                Ok(())
            }
            .in_current_span()
        });
    }
}

fn system_parallel() -> Option<usize> {
    std::thread::available_parallelism()
        .ok()
        .map(std::num::NonZero::<usize>::get)
}

fn env_parallel(env: &str) -> Option<usize> {
    std::env::var(env).ok().and_then(|v| v.parse().ok())
}
