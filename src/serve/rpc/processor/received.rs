use std::sync::atomic::AtomicU64;

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_flight::{decode::DecodedFlightData, error::FlightError};
use ha_core::{
    batch::BatchIter,
    consts::*,
    types::{RpcRecord, XnodedId},
};

use crate::serve::rpc::{
    DataSetsSenders,
    processor::{agent, xnode},
    utils::{build_rpc_failed_batch, build_rpc_ok_batch, internal_err},
};
use crate::serve::{
    controller::TaskControllerRef,
    rpc::{DsvSenders, StringSenders},
    scheduler::agent::AgentNotifySender,
};

pub async fn process(
    xnoded_id: Option<&XnodedId>,
    controller: &TaskControllerRef,
    data: DecodedFlightData,
    agent_id: Option<i64>,
    datasets_senders: &DataSetsSenders,
    dsv_senders: &DsvSenders,
    string_senders: &StringSenders,
    notify_sender: &AgentNotifySender,
    xnoded_tx: &flume::Sender<Result<RecordBatch, FlightError>>,
    last_heart_ms: &AtomicU64,
) -> Result<(), FlightError> {
    let arrow_flight::decode::DecodedPayload::RecordBatch(batch) = data.payload else {
        return Ok(());
    };
    debug_assert!(batch.num_rows() == 1);

    let batch_iter = BatchIter::new(&batch)
        .context("Invalid rpc RecordBatch")
        .map_err(internal_err)?;

    for RpcRecord {
        ts,
        action,
        context,
        req_id,
    } in batch_iter
    {
        macro_rules! process {
            ($result: expr, $action: expr, $tx: expr) => {{
                let batch = match $result {
                    Ok(res) => build_rpc_ok_batch($action, res, req_id)?,
                    Err(e) => build_rpc_failed_batch($action, e, req_id)?,
                };
                $tx.send_async(Ok(batch))
                    .await
                    .map_err(|_| send_resp_error($action))?;
            }};
        }
        match (action, agent_id, xnoded_tx) {
            ("list", Some(agent_id), _) => {
                agent::response::list_response(agent_id, context, datasets_senders.clone());
            }
            ("check", Some(agent_id), _) => {
                agent::response::check_response(agent_id, context, dsv_senders.clone());
            }
            ("sample", Some(agent_id), _) => {
                agent::response::sample_response(agent_id, context, string_senders.clone());
            }
            ("put-file", Some(agent_id), _) => {
                agent::response::put_file_response(agent_id, context, string_senders.clone());
            }
            ("query-data-source", Some(agent_id), _) => {
                agent::response::query_datasource_response(
                    agent_id,
                    context,
                    string_senders.clone(),
                );
            }
            ("agent-activity", Some(agent_id), _) => {
                agent::response::agent_activity(agent_id, context, notify_sender.clone());
            }
            ("task-activity", Some(agent_id), _) => {
                agent::response::task_activity(agent_id, context, notify_sender.clone());
            }
            ("heartbeat-ok", Some(agent_id), _) => {
                agent::response::heartbeat_ok(agent_id, context);
            }
            ("heartbeat", _, tx) => {
                let item = agent::response::heartbeat(ts, req_id, last_heart_ms);
                tx.send_async(item).await.map_err(|_| {
                    FlightError::ProtocolError(
                        "Failed to send heartbeat response, stream dropped".to_string(),
                    )
                })?;
            }
            ("task-metrics", _, _) => {
                agent::response::task_metrics(context);
            }
            ("metrics-events", _, _) => {
                agent::response::metrics_events(context);
            }
            (HEARTBEAT_REQ, _, tx) => {
                let Some(xnoded_id) = xnoded_id else {
                    return Err(FlightError::ProtocolError(
                        "Received heartbeat before handshake".to_string(),
                    ));
                };
                process!(
                    xnode::api::heartbeat(xnoded_id, context),
                    HEARTBEAT_RESP,
                    tx
                );
            }
            (PLAN_TASK_REQ, _, tx) => {
                tracing::info!("Received plan task request");
                process!(xnode::api::plan_task(context).await, PLAN_TASK_RESP, tx);
            }
            (START_TASK_JOB_REQ, _, tx) => {
                tracing::info!("Received start task job request");
                process!(
                    xnode::api::start_task_job(controller, context, xnoded_tx.clone()).await,
                    START_TASK_JOB_RESP,
                    tx
                );
            }
            (STOP_TASK_JOB_REQ, _, tx) => {
                tracing::info!("Received stop task job request");
                process!(
                    xnode::api::stop_task_job(controller, context).await,
                    STOP_TASK_JOB_RESP,
                    tx
                );
            }
            (LIST_TASK_JOB_STATES_REQ, _, tx) => {
                tracing::info!("Received list task job states request");
                process!(
                    xnode::api::list_task_states(controller).await,
                    LIST_TASK_JOB_STATES_RESP,
                    tx
                );
            }
            (ADD_AGENTS_REQ, _, tx) => {
                tracing::info!("Received add agents request");
                process!(
                    xnode::api::add_agents(controller, context).await,
                    ADD_AGENTS_RESP,
                    tx
                );
            }
            (DEL_AGENTS_REQ, _, tx) => {
                tracing::info!("Received delete agents request");
                process!(
                    xnode::api::del_agents(controller, context).await,
                    DEL_AGENTS_RESP,
                    tx
                );
            }
            (LIST_AGENTS_REQ, _, tx) => {
                tracing::info!("Received list agents request");
                process!(
                    xnode::api::list_agents(controller).await,
                    LIST_AGENTS_RESP,
                    tx
                );
            }
            (CHECK_VALID_REQ, _, tx) => {
                tracing::info!("Received check valid request");
                process!(xnode::api::check_valid(context).await, CHECK_VALID_RESP, tx);
            }
            (GET_SAMPLES_REQ, _, tx) => {
                tracing::info!("Received get samples request");
                process!(xnode::api::get_samples(context).await, GET_SAMPLES_RESP, tx);
            }
            (TASK_PREVIEW_REQ, _, tx) => {
                tracing::info!("Received task preview request");
                process!(
                    xnode::api::task_preview(context).await,
                    TASK_PREVIEW_RESP,
                    tx
                )
            }
            (TASK_JOB_DRAIN_REQ, _, tx) => {
                tracing::info!("Received task job drain request");
                process!(xnode::api::drain(controller).await, TASK_JOB_DRAIN_RESP, tx);
            }
            (action, agent, _) => {
                tracing::warn!(
                    "Invalid RPC request: action={action}, agent={agent:?}, xnoded_id={xnoded_id:?}"
                );
            }
        }
    }
    Ok(())
}

fn send_resp_error(action: &str) -> FlightError {
    FlightError::ProtocolError(format!(
        "Failed to send {action} response, receiver dropped"
    ))
}
