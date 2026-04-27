use std::time::Duration;

use actix_web::{
    HttpRequest, HttpResponse,
    web::{Data, Path, Payload},
};
use actix_ws::{CloseCode, CloseReason, Message, MessageStream, Session};
use anyhow::Context;
use arrow_flight::error::FlightError;
use futures::StreamExt;
use futures_ext::select::{Select3, select3};
use ha_core::{
    activity::Activity,
    batch::BatchIter,
    consts::{AGENT_ACTIVITIES_STABLE, TASK_ACTIVITIES_STABLE, TASK_METRICS, XNODE_ACTIVITIES},
    types::TaskMetrics,
};
use ha_rpc_client::client::HaRpcClient;
use taos::Dsn;
use tokio_util::sync::CancellationToken;

use crate::{
    Args,
    sql::query,
    x_api::{
        FlightResult, Result, get_client,
        tasks::get_all_task_job_metrics,
        types::{ActivityLog, Xnode},
    },
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn get_ws_tasks_activities(
    args: Data<Args>,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse> {
    let dsn = super::get_dsn(&args, &req).await?;

    let xnodes = get_xnode_ids(&dsn).await?;

    let (resp, mut session, msg_stream) =
        actix_ws::handle(&req, stream).map_err(|e| anyhow::anyhow!("handle ws error: {e}"))?;

    // 先查询 10 条数据
    let sql = format!(
        "select \
        `task_id` as `id`, `ts` as `at`, `level`, `status`, `activity` \
        from log.{TASK_ACTIVITIES_STABLE} \
        where status != '-' \
        order by ts desc limit 10;"
    );
    let activities = query::<ActivityLog>(&dsn, &sql).await?;
    for activity in activities {
        let Ok(text) = serde_json::to_string(&activity) else {
            continue;
        };
        if session.text(text).await.is_err() {
            break;
        }
    }

    handle_ws(
        args.as_ref(),
        &dsn,
        &xnodes,
        session,
        msg_stream,
        process_tasks_activity_batch,
    )
    .await?;

    Ok(resp)
}

pub async fn get_ws_agents_activities(
    args: Data<Args>,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse> {
    let dsn = super::get_dsn(&args, &req).await?;

    let xnodes = get_xnode_ids(&dsn).await?;

    let (resp, mut session, msg_stream) =
        actix_ws::handle(&req, stream).map_err(|e| anyhow::anyhow!("handle ws error: {e}"))?;

    // 先查询 10 条数据
    let sql = format!(
        "select \
        `agent_id` as `id`, `ts` as `at`, `level`, `status`, `activity` \
        from log.{AGENT_ACTIVITIES_STABLE} \
        order by ts desc limit 10;"
    );
    let activities = query::<ActivityLog>(&dsn, &sql).await?;
    for activity in activities {
        let Ok(text) = serde_json::to_string(&activity) else {
            continue;
        };
        if session.text(text).await.is_err() {
            break;
        }
    }

    handle_ws(
        args.as_ref(),
        &dsn,
        &xnodes,
        session,
        msg_stream,
        process_agents_activities_batch,
    )
    .await?;

    Ok(resp)
}

pub async fn get_ws_metrics(
    args: Data<Args>,
    task_id: Path<i64>,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse> {
    let task_id = *task_id;
    let dsn = super::get_dsn(&args, &req).await?;

    let xnodes = get_xnode_ids(&dsn).await?;

    let (resp, mut session, msg_stream) =
        actix_ws::handle(&req, stream).map_err(|e| anyhow::anyhow!("handle ws error: {e}"))?;

    // Send initial metrics data
    let initial_metrics = get_all_task_job_metrics(dsn.clone(), task_id).await?;
    if session.text(initial_metrics).await.is_err() {
        return Ok(resp);
    }

    handle_ws_metrics(args.as_ref(), &dsn, &xnodes, session, msg_stream, task_id).await?;

    Ok(resp)
}

async fn process_tasks_activity_batch(event: FlightResult) -> anyhow::Result<Option<String>> {
    let batch = event.context("Failed to receive task metrics")?;
    let mut iter = BatchIter::new(&batch).context("Failed to iterate over task metrics batch")?;

    let Some(record) = iter.next() else {
        return Ok(None);
    };
    if record.action != XNODE_ACTIVITIES {
        return Ok(None);
    }

    let activity =
        serde_json::from_str::<Activity>(record.context).context("Failed to parse task metrics")?;
    if activity.agent_id > 0 {
        return Ok(None);
    }

    Ok(Some(record.context.to_string()))
}

async fn process_agents_activities_batch(event: FlightResult) -> anyhow::Result<Option<String>> {
    let batch = event.context("Failed to receive agent metrics")?;
    let mut iter = BatchIter::new(&batch).context("Failed to iterate over agent metrics batch")?;

    let Some(record) = iter.next() else {
        return Ok(None);
    };
    if record.action != XNODE_ACTIVITIES {
        return Ok(None);
    }

    let activity =
        serde_json::from_str::<Activity>(record.context).context("Failed to parse task metrics")?;

    if activity.agent_id < 0 {
        return Ok(None);
    }

    Ok(Some(record.context.to_string()))
}

fn is_target_task_metrics(event: &FlightResult, task_id: i64) -> bool {
    let Ok(batch) = event.as_ref() else {
        return false;
    };
    let Ok(mut iter) = BatchIter::new(batch) else {
        return false;
    };
    let Some(record) = iter.next() else {
        return false;
    };
    if record.action != TASK_METRICS {
        return false;
    }
    serde_json::from_str::<TaskMetrics>(record.context)
        .map(|m| m.task_id == task_id)
        .unwrap_or(false)
}

/// Listens for metric events; when no event arrives within 2s, falls back to polling.
async fn send_metrics_message(
    mut session: Session,
    event_rx: flume::Receiver<FlightResult>,
    cancel: CancellationToken,
    task_id: i64,
    dsn: Dsn,
) {
    tracing::info!("ws send metrics message loop start");
    let _guard = cancel.drop_guard_ref();
    let _exit_guard = taosx_core::utils::defer::defer(|| {
        tracing::info!("ws send metrics message loop exit");
    });

    loop {
        let should_send = tokio::select! {
            result = cancel.run_until_cancelled(event_rx.recv_async()) => {
                match result {
                    Some(Ok(event)) => is_target_task_metrics(&event, task_id),
                    // channel closed or cancelled
                    _ => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                // Timeout with no event, poll actively
                true
            }
        };

        if !should_send {
            continue;
        }

        match get_all_task_job_metrics(dsn.clone(), task_id).await {
            Ok(text) => {
                if cancel
                    .run_until_cancelled(session.text(text))
                    .await
                    .is_none_or(|r| r.is_err())
                {
                    break;
                }
            }
            Err(e) => {
                tracing::error!("Failed to get metrics: {e:#}");
            }
        }
    }
    session.close(None).await.ok();
}

async fn handle_ws<F, Fut>(
    args: &Args,
    dsn: &Dsn,
    xnode_ids: &[i32],
    session: Session,
    msg_stream: MessageStream,
    processor: F,
) -> Result<()>
where
    F: Fn(FlightResult) -> Fut + Clone + 'static + Send,
    Fut: Future<Output = anyhow::Result<Option<String>>> + Send,
{
    let cancel = CancellationToken::new();

    for xnode_id in xnode_ids {
        start_event_processor(
            args,
            *xnode_id,
            dsn,
            session.clone(),
            processor.clone(),
            cancel.clone(),
        )
        .await?;
    }

    tokio::task::spawn_local(heartbeat_ws(session, msg_stream, cancel));

    Ok(())
}

async fn handle_ws_metrics(
    args: &Args,
    dsn: &Dsn,
    xnode_ids: &[i32],
    session: Session,
    msg_stream: MessageStream,
    task_id: i64,
) -> Result<()> {
    let cancel = CancellationToken::new();
    let (event_tx, event_rx) = flume::bounded(100);

    for xnode_id in xnode_ids {
        let client = get_client(
            args,
            Some(*xnode_id),
            dsn,
            None,
            Some(event_tx.clone()),
            cancel.child_token(),
        )
        .await?
        .context("no available xnode found")?;
        tokio::spawn(ha_client_hb(client, cancel.clone()));
    }
    drop(event_tx);

    tokio::spawn({
        let session = session.clone();
        let cancel = cancel.clone();
        let dsn = dsn.clone();
        async move {
            send_metrics_message(session, event_rx, cancel, task_id, dsn).await;
        }
    });

    tokio::task::spawn_local(heartbeat_ws(session, msg_stream, cancel));

    Ok(())
}

async fn start_event_processor<F, Fut>(
    args: &Args,
    xnode_id: i32,
    dsn: &Dsn,
    session: Session,
    processor: F,
    cancel: CancellationToken,
) -> Result<()>
where
    F: Fn(FlightResult) -> Fut + Clone + 'static + Send,
    Fut: Future<Output = anyhow::Result<Option<String>>> + Send,
{
    let (event_tx, event_rx) = flume::bounded(100);
    let client = get_client(
        args,
        Some(xnode_id),
        dsn,
        None,
        Some(event_tx),
        cancel.child_token(),
    )
    .await?
    .context("no available xnode found")?;
    tokio::spawn({
        let cancel = cancel.clone();
        let session = session.clone();
        let processor = processor.clone();
        async move { send_message(session, event_rx, cancel, processor).await }
    });
    tokio::spawn(ha_client_hb(client, cancel));

    Ok(())
}

async fn ha_client_hb(client: HaRpcClient, cancel: CancellationToken) {
    tracing::info!("ha client heartbeat start");
    let _guard = cancel.drop_guard_ref();
    let _exit_guard = taosx_core::utils::defer::defer(|| {
        tracing::info!("ha client heartbeat exit");
    });
    loop {
        if cancel
            .run_until_cancelled(tokio::time::sleep(Duration::from_secs(5)))
            .await
            .is_none()
        {
            break;
        }
        let Some(res) = cancel.run_until_cancelled(client.guest_heartbeat()).await else {
            break;
        };
        if let Err(e) = res {
            tracing::error!("Failed to send heartbeat: {e}");
            if let ha_rpc_client::error::Error::Flight {
                source: FlightError::Tonic(status),
            } = e
                && matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::Unknown
                )
            {
                tracing::warn!("Connection lost, stopping heartbeat.");
                break;
            }
        };
    }
}

async fn send_message<F, Fut>(
    mut session: Session,
    event_rx: flume::Receiver<FlightResult>,
    cancel: CancellationToken,
    processor: F,
) where
    F: Fn(FlightResult) -> Fut + Clone + 'static + Send,
    Fut: Future<Output = anyhow::Result<Option<String>>> + Send,
{
    tracing::info!("ws send message loop start");
    let _guard = cancel.drop_guard_ref();
    let _exit_guard = taosx_core::utils::defer::defer(|| {
        tracing::info!("ws send message loop exit");
    });

    loop {
        let Some(Ok(event)) = cancel.run_until_cancelled(event_rx.recv_async()).await else {
            break;
        };

        match processor(event).await {
            Ok(Some(text)) => {
                if cancel
                    .run_until_cancelled(session.text(text))
                    .await
                    .is_none_or(|r| r.is_err())
                {
                    break;
                }
            }
            Err(e) => {
                tracing::error!("Failed to process event: {e:#}");
            }
            Ok(None) => {}
        }
    }
    session.close(None).await.ok();
}

async fn get_xnode_ids(dsn: &Dsn) -> Result<Vec<i32>> {
    let xnodes = query::<Xnode>(dsn, "SHOW XNODES WHERE STATUS = 'online'")
        .await?
        .into_iter()
        .map(|v| v.id)
        .collect::<Vec<_>>();
    Ok(xnodes)
}

pub async fn heartbeat_ws(
    mut session: actix_ws::Session,
    mut msg_stream: actix_ws::MessageStream,
    cancel: CancellationToken,
) {
    let _cancel_guard = cancel.drop_guard_ref();
    let _guard = taosx_core::utils::defer::defer(|| {
        tracing::info!("ws disconnected");
    });

    let mut last_heartbeat = std::time::Instant::now();
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);

    let reason = loop {
        match select3(msg_stream.next(), interval.tick(), cancel.cancelled()).await {
            Select3::T1(Some(Ok(msg))) => {
                tracing::debug!("msg: {msg:?}");

                match msg {
                    Message::Close(reason) => {
                        break reason;
                    }
                    Message::Ping(bytes) => {
                        last_heartbeat = std::time::Instant::now();
                        if session.pong(&bytes).await.is_err() {
                            break None;
                        }
                    }
                    Message::Pong(_) => {
                        last_heartbeat = std::time::Instant::now();
                    }
                    Message::Binary(bytes) => {
                        if session.binary(bytes).await.is_err() {
                            break None;
                        }
                    }
                    Message::Text(bytes) => {
                        if session.text(bytes).await.is_err() {
                            break None;
                        }
                    }
                    _ => {}
                };
            }
            Select3::T1(Some(Err(e))) => {
                tracing::error!("ws received error: {e}");
                break Some(CloseReason {
                    code: CloseCode::Error,
                    description: Some(e.to_string()),
                });
            }
            Select3::T1(None) => {
                tracing::error!("ws received none message");
                break None;
            }
            Select3::T2(_) => {
                if last_heartbeat.elapsed() > CLIENT_TIMEOUT {
                    tracing::warn!("ws client has not sent heartbeat for long time; disconnecting");
                    break Some(CloseReason::from(CloseCode::Away));
                }
                if session.ping(b"").await.is_err() {
                    break None;
                };
            }
            Select3::T3(_) => {
                tracing::error!("ws stream cancelled");
                break None;
            }
        }
    };

    session.close(reason).await.ok();
}
