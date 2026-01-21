use std::{sync::atomic::Ordering, time::Duration};

use anyhow::Context;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tokio::{sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};
use uuid::Uuid;

use crate::serve::{
    controller::{Task, load_breakpoints},
    rpc::utils::build_task_job_finish_batch,
    scheduler::runner::{AgentId, GlobalState, LastState, Operator, TaskState},
};
use ha_core::activity::Activity;
use taosx_core::{
    ConnectorLicense, TaskNotify, TaskNotifyReceiver,
    core_metrics::get_metrics_arc_from_i64,
    get_data_dir, plugins,
    task_set::prelude::{EventLevel, health_checker},
    utils::{get_main_version_from_server_version, get_server_version, sql::get_timestamp_range},
};
use taosx_task::TaskOpts;
use taosx_utils::dsn::json_to_dsn;

pub async fn spawn_task(
    task_id: i64,
    job_id: i64,
    opts: TaskState,
    sid: Uuid,
    global: GlobalState,
    tx: oneshot::Sender<bool>,
) {
    let runs = opts.runs.load(Ordering::Relaxed);
    tracing::debug!(
        "spawned new run_task, task.id={} task.rid={}",
        task_id,
        runs
    );
    let span = tracing::info_span!("run_task", task.rid = runs);

    let cancellation = opts.cancellation.child_token();

    let task_fut = run_task(&global, &opts, &sid, cancellation.clone()).instrument(span.clone());
    tokio::pin!(task_fut);

    let stop_condition = opts.stop_condition.clone();
    let last_state = opts.last_state.clone();
    let span_handler = span.clone();
    let check_should_stop = |result| async {
        if let Err(err) = &result {
            tracing::error!(error = %err, backtrace = ?err, "task error");
        } else {
            tracing::info!("task finished");
        }
        let should_stop = stop_condition.should_stop_with(&result);
        match result {
            Ok(_) => {
                last_state.write().await.replace(LastState::Done);
            }
            Err(err) => {
                last_state.write().await.replace(LastState::Error(err));
            }
        }
        if should_stop {
            tracing::info!(
                should_stop,
                ?stop_condition,
                ?opts,
                "stop condition reached"
            );
        }
        should_stop
    };

    let mut should_stop = match cancellation.run_until_cancelled(&mut task_fut).await {
        Some(result) => {
            if opts.cancellation.is_cancelled() {
                tracing::info!("task cancelled");
                opts.last_state.write().await.replace(LastState::Stopped);
                true
            } else {
                check_should_stop(result).await
            }
        }
        None => {
            tracing::info!("task cancelled");
            opts.last_state.write().await.replace(LastState::Stopped);
            (&mut task_fut).await;
            true
        }
    };

    if !should_stop {
        should_stop = opts.stop_condition.should_stop();
    }
    let state_guard = opts.last_state.read().await;
    let last_state = state_guard.as_ref().expect("task should have a last state");
    let (task_id, job_id) = (opts.task.id, opts.task.job_id);
    match last_state {
        LastState::Done => match opts.operator.operator() {
            Operator::Suspend | Operator::Stop => {
                global.send_task_activity(Activity::stopped(task_id, job_id));
                opts.state.write().await.stopped();
            }
            Operator::Run => {
                global.send_task_activity(Activity::completed(task_id, job_id));
                opts.state.write().await.completed();
            }
        },
        LastState::Stopped => {
            global.send_task_activity(Activity::stopped(task_id, job_id));
            opts.state.write().await.stopped();
        }
        LastState::Error(err) => {
            global.send_task_activity(Activity::failed(task_id, job_id, format!("{err:#}")));
            opts.state.write().await.fail(err);
        }
    }
    opts.runs.fetch_add(1, Ordering::Release);
    tx.send(should_stop).ok();
}

async fn run_task(
    global: &GlobalState,
    state: &TaskState,
    job_id: &Uuid,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    debug_assert!(state.task.via.is_none());
    let task = &state.task;
    let task_id = task.id;
    let job_id = task.job_id;
    let (notify_sender, notify_receiver) = flume::bounded(64);
    let opts = task_opts_init(task, notify_sender, cancel.clone()).await?;
    tracing::info!("start worker");

    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let mut logging_tasks = JoinSet::new();

    let health_opts = state.task.trigger.as_ref().map(|v| v.health);
    let (health_tx, health_rx) = health_opts.is_some().then(|| flume::bounded(64)).unzip();
    if let (Some(health_opts), Some(health_rx)) = (health_opts, health_rx) {
        let metrics = get_metrics_arc_from_i64(Some((task_id, job_id)));
        let (_handle, mut rx) = health_checker(health_opts, health_rx, metrics);
        let global_sender = global.clone();

        let cancel = cancel.clone();
        logging_tasks.spawn(async move {
            while let Some(Ok(item)) = cancel.run_until_cancelled(rx.recv()).await {
                tracing::debug!("health state: {:?}", item);
                global_sender.send_task_activity(Activity::health_state(
                    task_id,
                    job_id,
                    item.at,
                    item.state.into(),
                ));
            }
        });
    }

    logging_tasks.spawn({
        let global_sender = global.clone();
        let cancel = cancel.clone();
        async move {
            while let Some(Ok(notify)) = cancel
                .run_until_cancelled(notify_receiver.recv_async())
                .await
            {
                let activity = match notify.level {
                    EventLevel::Error => Activity::error(task_id, job_id, notify.message.clone()),
                    EventLevel::Warn => Activity::warn(task_id, job_id, notify.message.clone()),
                    EventLevel::Info => Activity::info(task_id, job_id, notify.message.clone()),
                    _ => break,
                };
                if let Some(health_tx) = health_tx.as_ref() {
                    health_tx.send_async(notify).await.ok();
                }
                global_sender.send_task_activity(activity);
            }
        }
    });
    global.send_task_activity(Activity::running(
        task_id,
        job_id,
        format!("Start to run task ({task_id},{job_id})"),
    ));
    let instant = std::time::Instant::now();
    let res = opts.run(&global.port_pool).in_current_span().await;
    tracing::info!(task.elapsed = ?instant.elapsed(), "task finished");
    state
        .xnoded_tx
        .try_send(build_task_job_finish_batch(task_id, job_id, &res))
        .ok();
    if let Err(e) = &res {
        tracing::error!("Task exit with error: {e:#}");
    }

    res
}

#[instrument(skip_all)]
#[async_backtrace::framed]
async fn task_opts_init(
    task: &Task,
    notify_sender: flume::Sender<TaskNotify>,
    cancel: CancellationToken,
) -> anyhow::Result<TaskOpts> {
    let task_id = task.id;
    let job_id = task.job_id;
    let from = if let Some(topic) = task.oneshot_topic.as_deref() {
        // let mut from: Dsn = task.from.parse()?;
        let mut from = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
        from.set("use.topic.name", topic);
        tracing::info!("Set task from: {from}");
        from
    } else {
        // task.from.parse()?
        json_to_dsn(&serde_json::Value::String(task.from.clone()))?
    };

    let breakpoints = load_breakpoints(task_id, job_id);

    let parser: Option<plugins::Parser> = task
        .parser
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .context("deserialize parser error")?;
    let to_dsn: Dsn = task.to.parse().context("target not valid dsn")?;
    let parser = if let Some(parser) = parser {
        let pool = {
            let builder = taos::TaosBuilder::from_dsn(&to_dsn)?;
            let mut pool_config = builder.default_pool_config();
            let timeout = parser
                .global()
                .process_on_abnormal
                .connection_timeout_in_second_value;
            pool_config.timeouts.wait = Some(Duration::from_secs(timeout as u64));
            builder.with_pool_config(pool_config)?
        };
        let (_, minimum_timestamp, maximum_timestamp) =
            get_timestamp_range(&pool, &mut None, 3, &cancel).await?;
        let metrics = get_metrics_arc_from_i64(Some((task_id, job_id)));
        let parser = match parser {
            plugins::Parser::Inner(parser) => {
                let mut parser = parser;
                parser.set_maximum_timestamp(maximum_timestamp);
                if let Some(minimum_timestamp) = minimum_timestamp {
                    parser.set_minimum_timestamp(minimum_timestamp);
                }
                parser.organize_archive(task.id, job_id);
                parser.organize_cache(task.id, job_id);
                plugins::Parser::Inner(parser)
            }
            plugins::Parser::WithSample { parser, input } => {
                let mut parser = parser;
                parser.set_maximum_timestamp(maximum_timestamp);
                if let Some(minimum_timestamp) = minimum_timestamp {
                    parser.set_minimum_timestamp(minimum_timestamp);
                }
                parser.organize_archive(task.id, job_id);
                parser.organize_cache(task.id, job_id);
                plugins::Parser::WithSample { parser, input }
            }
        };
        Some(parser)
    } else {
        None
    };

    Ok(TaskOpts {
        transform: vec![],
        from: from.clone(),
        to: to_dsn.clone(),
        parser,
        health: task.trigger.as_ref().map(|v| v.health),
        cancel,
        with_agent: None,
        breakpoints,
        task_job_id: Some((task_id, job_id)),
        notify: notify_sender,
    })
}
