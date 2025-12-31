use anyhow::Context;
use arrow::array::timezone::Tz;
use arrow_flight::error::FlightError;
use chrono::{DateTime, Utc};
use ha_core::batch::build_batch;
use metrics::{IntoLabels, counter, gauge, histogram};
use taosx_core::{
    CheckResponse, HeartbeatResponse, ListResponse, PutFileResp, QueryDataSourceResp,
    SampleResponse, TaskMetricItem, core_metrics::get_metrics,
};
use taosx_metrics::MetricsEvents;

use crate::serve::{
    controller::activity::Activity,
    rpc::{DataSetsSenders, DsvSenders, StringSenders, utils::internal_err},
    scheduler::agent::{AgentNotify, AgentNotifySender},
};

pub fn list_response(agent_id: i64, context: &str, datasets_senders: DataSetsSenders) {
    let resp = match serde_json::from_str::<ListResponse>(context) {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(
                agent = agent_id,
                "List data sets response parse failed: {e:#}"
            );
            return;
        }
    };

    tokio::spawn(async move {
        let req_id = resp.req_id;
        if let Some(sender) = { datasets_senders.write().remove(&req_id) } {
            if let Err(err) = sender.send_async(resp.res).await {
                tracing::warn!(
                    agent = agent_id,
                    req_id = req_id,
                    "List data sets response send failed: {err:#}"
                );
            }
        } else {
            tracing::warn!(
                agent = agent_id,
                req_id = req_id,
                "List data sets request id has no receiver"
            );
        }
    });
}

pub fn check_response(agent_id: i64, context: &str, dsv_senders: DsvSenders) {
    let resp: CheckResponse = match serde_json::from_str(context) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(agent = agent_id, "Failed to parse check response: {err:#}");
            return;
        }
    };

    let dsv_senders = dsv_senders.clone();
    tokio::spawn(async move {
        let req_id = resp.req_id;
        if let Some(sender) = { dsv_senders.write().remove(&req_id) } {
            if let Err(err) = sender.send_async(resp.res).await {
                tracing::warn!(
                    agent = agent_id,
                    req_id = req_id,
                    "List data sets response send failed: {err:#}"
                );
            }
        } else {
            tracing::warn!(
                agent = agent_id,
                req_id = req_id,
                "List data sets request id has no receiver"
            );
        }
    });
}

pub fn sample_response(agent_id: i64, context: &str, string_senders: StringSenders) {
    let resp: SampleResponse = match serde_json::from_str(context) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(agent = agent_id, "Failed to parse sample response: {err:#}");
            return;
        }
    };
    let string_senders = string_senders.clone();
    tokio::spawn(async move {
        let req_id = resp.req_id;
        if let Some(sender) = { string_senders.write().remove(&req_id) } {
            if let Err(err) = sender.send_async(resp.res).await {
                tracing::warn!(
                    agent = agent_id,
                    req_id = req_id,
                    "get sample response send failed: {err:#}"
                );
            }
        } else {
            tracing::warn!(
                agent = agent_id,
                req_id = req_id,
                "get sample request id has no receiver"
            );
        }
    });
}

pub fn put_file_response(agent_id: i64, context: &str, string_senders: StringSenders) {
    let resp: PutFileResp = match serde_json::from_str(context) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(
                agent = agent_id,
                "Failed to parse put file response: {err:#}"
            );
            return;
        }
    };
    let string_senders = string_senders.clone();
    tokio::spawn(async move {
        let req_id = resp.req_id;
        if let Some(sender) = { string_senders.write().remove(&req_id) } {
            if let Err(err) = sender.send_async(resp.res).await {
                tracing::error!(
                    agent = agent_id,
                    req_id = req_id,
                    "Send PutFileResp failed: {err:#}"
                );
            }
        } else {
            tracing::error!(
                agent = agent_id,
                req_id = req_id,
                "PutFileResp has no receiver"
            );
        }
    });
}

pub fn query_datasource_response(agent_id: i64, context: &str, string_senders: StringSenders) {
    let resp: QueryDataSourceResp = match serde_json::from_str(context) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!(
                agent = agent_id,
                "Invalid QueryDataSourceResp `{context}`: {err:#}"
            );
            return;
        }
    };
    let string_senders = string_senders.clone();
    tokio::spawn(async move {
        let req_id = resp.req_id;
        if let Some(sender) = { string_senders.write().remove(&req_id) } {
            if let Err(err) = sender.send_async(resp.output).await {
                tracing::error!(
                    agent = agent_id,
                    req_id = req_id,
                    "Send QueryDataSourceResp failed: {err:#}"
                );
            }
        } else {
            tracing::error!(
                agent = agent_id,
                req_id = req_id,
                "QueryDataSourceResp has no receiver"
            );
        }
    });
}

pub fn agent_activity(agent_id: i64, context: &str, notify_sender: AgentNotifySender) {
    let activity: Activity = match serde_json::from_str(context) {
        Ok(activity) => activity,
        Err(err) => {
            tracing::error!(
                agent = agent_id,
                "Invalid agent activity `{context}`: {err:#}"
            );
            return;
        }
    };
    tracing::info!(?activity, "agent activity");
    notify_sender
        .send(AgentNotify::AgentActivity(agent_id, activity))
        .ok();
}

pub fn task_activity(agent_id: i64, context: &str, notify_sender: AgentNotifySender) {
    let activity: Activity = match serde_json::from_str(context) {
        Ok(activity) => activity,
        Err(err) => {
            tracing::error!("Invalid task activity `{context}`: {err:#}");
            return;
        }
    };
    tracing::info!(?activity, "task activity");
    notify_sender
        .send(AgentNotify::TaskActivity(agent_id, activity))
        .ok();
}

pub fn heartbeat_ok(agent_id: i64, context: &str) {
    let resp: HeartbeatResponse = match serde_json::from_str(context) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!("Invalid heartbeat response `{context}`: {err:#}");
            return;
        }
    };
    let delay = resp.duration();
    if delay.num_seconds() > 5 {
        tracing::info!(
            agent = agent_id,
            "Agent maybe not health, delay {:?}",
            delay
        );
    } else {
        tracing::info!(agent = agent_id, "Agent is alive, delay: {:?}", delay);
    }
}

pub fn heartbeat(ts: DateTime<Tz>, req_id: u64) -> Result<arrow::array::RecordBatch, FlightError> {
    tracing::trace!("Received heartbeat");
    let req = ts.naive_utc().and_utc();
    let resp = HeartbeatResponse {
        req,
        res: Utc::now(),
    };
    let context = serde_json::to_string(&resp)
        .context("serialize heartbeat resp error")
        .map_err(internal_err)?;
    build_batch("heartbeat-ok", &context, req_id)
        .context("build heartheat-ok batch error")
        .map_err(internal_err)
}

pub fn task_metrics(context: &str) {
    match serde_json::from_str::<Vec<TaskMetricItem>>(context) {
        Ok(events) => {
            tokio::spawn(async move {
                tracing::trace!("Received source metrics events, total: {}", events.len());
                replay_task_metrics_from_agent(events).await;
            });
        }
        Err(err) => {
            tracing::warn!(?err, "Invalid metrics events");
        }
    }
}

async fn replay_task_metrics_from_agent(events: Vec<TaskMetricItem>) {
    for TaskMetricItem {
        task_id,
        job_id,
        key,
        var,
        value,
    } in events
    {
        if let Some(metrics) = get_metrics(task_id, job_id) {
            use taosx_ipc::types::TaskMetricsVariant;
            match var {
                TaskMetricsVariant::Set => {
                    metrics.ipc().set_extra_metric(&key, value);
                }
                TaskMetricsVariant::Inc => {
                    metrics.ipc().add_extra_metric(&key, value);
                }
                TaskMetricsVariant::Dec => {
                    metrics.ipc().sub_extra_metric(&key, value);
                }
            }
        }
    }
}

pub fn metrics_events(context: &str) {
    match serde_json::from_str::<MetricsEvents>(context) {
        Ok(events) => {
            tokio::spawn(async move {
                tracing::trace!("Received metrics events, total: {}", events.len());
                replay_metrics_events_from_agent(events);
            });
        }
        Err(err) => {
            tracing::warn!(?err, "Invalid metrics events");
        }
    }
}

fn replay_metrics_events_from_agent(metrics_events: MetricsEvents) {
    for event in metrics_events.events().iter().cloned() {
        let labels = event.labels.into_labels();
        match event.operation {
            taosx_metrics::MetricOperation::IncrementCounter(value) => {
                counter!(event.key, labels).increment(value);
            }
            taosx_metrics::MetricOperation::SetCounter(value) => {
                counter!(event.key, labels).absolute(value);
            }
            taosx_metrics::MetricOperation::IncrementGauge(value) => {
                gauge!(event.key, labels).increment(value);
            }
            taosx_metrics::MetricOperation::DecrementGauge(value) => {
                gauge!(event.key, labels).decrement(value);
            }
            taosx_metrics::MetricOperation::SetGauge(value) => {
                gauge!(event.key, labels).set(value);
            }
            taosx_metrics::MetricOperation::RecordHistogram(value) => {
                histogram!(event.key, labels).record(value);
            }
        }
    }
}
