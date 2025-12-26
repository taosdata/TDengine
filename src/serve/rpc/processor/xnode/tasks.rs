use std::{sync::Arc, time::Duration};

use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use futures::stream::FuturesUnordered;
use ha_core::{batch::build_batch, consts::HEARTBEAT_REQ, utils::next_req_id};
use taosx_core::core_metrics::{
    CoreMetrics, MetricsEvent, subscribe_all_task_metrics_watcher, subscribe_metrics_watcher,
    subscribe_task_metrics_watcher,
};
use tokio::{sync::watch, task::JoinSet};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::serve::{
    controller::TaskControllerRef,
    rpc::utils::{build_activity_batch, build_metrics_batch},
    scheduler::SchedulerNotify,
};

pub fn spawn_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    cancel: CancellationToken,
    controller: TaskControllerRef,
    flight_tx: flume::Sender<Result<RecordBatch, FlightError>>,
) {
    // 主动发送心跳
    tasks.spawn({
        let tx = flight_tx.clone();
        let cancel = cancel.clone();
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            while cancel.run_until_cancelled(ticker.tick()).await.is_some() {
                let batch =
                    build_batch(HEARTBEAT_REQ, "", next_req_id()).map_err(FlightError::Arrow);
                if cancel
                    .run_until_cancelled(tx.send_async(batch))
                    .await
                    .is_none_or(|v| v.is_err())
                {
                    break;
                }
            }
            Ok(())
        }
    });
    // 将全局活动日志发送给 xnoded
    tasks.spawn({
        let tx = flight_tx.clone();
        let cancel = cancel.clone();
        let mut activity_receiver = controller.scheduler.notify_channel();
        async move {
            while let Some(Ok(notify)) = cancel.run_until_cancelled(activity_receiver.recv()).await
            {
                let activity = match notify {
                    SchedulerNotify::TaskActivity(activity) => activity,
                    SchedulerNotify::AgentActivity(activity) => activity,
                };
                let batch = build_activity_batch(activity);
                if cancel
                    .run_until_cancelled(tx.send_async(batch))
                    .await
                    .is_none_or(|v| v.is_err())
                {
                    break;
                }
            }
            Ok(())
        }
    });
    // 将 metrics 发送给 xnoded
    tasks.spawn({
        let cancel = cancel.clone();
        async move {
            let mut futs = FuturesUnordered::new();
            let (metrics_tx, metrics_rx) = flume::bounded(1000);
            let task_metrics_receivers = subscribe_all_task_metrics_watcher();
            for receiver in task_metrics_receivers {
                futs.push(recv_metrics_task(metrics_tx.clone(), receiver, cancel.clone()));
            }
            let mut new_metrics_receiver = subscribe_metrics_watcher();
            loop {
                tokio::select! {
                    biased;
                    _ = futs.next(), if !futs.is_empty() => {},
                    res = new_metrics_receiver.changed() => {
                        if res.is_err() {
                            break
                        }
                        let event = {
                            new_metrics_receiver.borrow_and_update().clone()
                        };
                        match event {
                            Some(MetricsEvent::Insert(task_id, job_id, core_metrics)) => {
                                if cancel.run_until_cancelled(metrics_tx.send_async(core_metrics.clone())).await.is_none_or(|v| v.is_err()) {
                                    break
                                }
                                let Some(receiver) = subscribe_task_metrics_watcher(task_id, job_id) else {
                                    break
                                };
                                let metrics_tx = metrics_tx.clone();
                                let cancel = cancel.child_token();
                                futs.push(recv_metrics_task(metrics_tx, receiver, cancel));
                            },
                            Some(MetricsEvent::Delete(_, _)) | Some(MetricsEvent::Update(_, _, _)) | None => {},
                        }
                    },
                    res = metrics_rx.recv_async() => {
                        let Ok(metrics) = res else {
                            break;
                        };
                        let batch = build_metrics_batch(metrics);
                        if cancel.run_until_cancelled(flight_tx.send_async(batch)).await.is_none_or(|v| v.is_err()) {
                            break;
                        }
                    }
                    _ = cancel.cancelled() => break
                }
            }
            Ok(())
        }
    });
}

async fn recv_metrics_task(
    metrics_tx: flume::Sender<Arc<CoreMetrics>>,
    mut receiver: watch::Receiver<MetricsEvent>,
    cancel: CancellationToken,
) {
    static UPDATE_METRICS_DURATION: Duration = Duration::from_millis(500);
    let mut ticker = tokio::time::interval(UPDATE_METRICS_DURATION);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
            break;
        }
        if cancel
            .run_until_cancelled(receiver.changed())
            .await
            .is_none_or(|v| v.is_err())
        {
            break;
        }
        let event = { receiver.borrow_and_update().clone() };
        match event {
            MetricsEvent::Insert(_, _, _) | MetricsEvent::Delete(_, _) => {}
            MetricsEvent::Update(_, _, core_metrics) => {
                if cancel
                    .run_until_cancelled(metrics_tx.send_async(core_metrics.clone()))
                    .await
                    .is_none_or(|v| v.is_err())
                {
                    break;
                }
            }
        }
    }
}
