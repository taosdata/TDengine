use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_flight::{decode::DecodedFlightData, error::FlightError};
use ha_core::{
    batch::{BatchIter, build_batch},
    consts::*,
    types::{RpcClientType, RpcRecord, XnodedId},
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
    tx: &flume::Sender<Result<RecordBatch, FlightError>>,
    client_type: RpcClientType,
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
            ($result: expr, $action: expr) => {{
                let batch = match $result {
                    Ok(res) => build_rpc_ok_batch($action, res, req_id)?,
                    Err(e) => build_rpc_failed_batch($action, e, req_id)?,
                };
                tx.send_async(Ok(batch))
                    .await
                    .map_err(|_| send_resp_error($action))?;
            }};
        }
        match (action, agent_id, client_type) {
            ("list", Some(agent_id), RpcClientType::Agent) => {
                agent::response::list_response(agent_id, context, datasets_senders.clone());
            }
            ("check", Some(agent_id), RpcClientType::Agent) => {
                agent::response::check_response(agent_id, context, dsv_senders.clone());
            }
            ("sample", Some(agent_id), RpcClientType::Agent) => {
                agent::response::sample_response(agent_id, context, string_senders.clone());
            }
            ("put-file", Some(agent_id), RpcClientType::Agent) => {
                agent::response::put_file_response(agent_id, context, string_senders.clone());
            }
            ("query-data-source", Some(agent_id), RpcClientType::Agent) => {
                agent::response::query_datasource_response(
                    agent_id,
                    context,
                    string_senders.clone(),
                );
            }
            ("agent-activity", Some(agent_id), RpcClientType::Agent) => {
                agent::response::agent_activity(agent_id, context, notify_sender.clone());
            }
            ("task-activity", Some(agent_id), RpcClientType::Agent) => {
                agent::response::task_activity(agent_id, context, notify_sender.clone());
            }
            ("heartbeat-ok", Some(agent_id), RpcClientType::Agent) => {
                agent::response::heartbeat_ok(agent_id, context);
            }
            ("heartbeat", _, RpcClientType::Agent) => {
                let item = agent::response::heartbeat(ts, req_id);
                tx.send_async(item).await.map_err(|_| {
                    FlightError::ProtocolError(
                        "Failed to send heartbeat response, stream dropped".to_string(),
                    )
                })?;
            }
            ("task-metrics", _, RpcClientType::Agent) => {
                agent::response::task_metrics(context);
            }
            ("metrics-events", _, RpcClientType::Agent) => {
                agent::response::metrics_events(context);
            }
            (HEARTBEAT_REQ, _, RpcClientType::Xnoded) => {
                let Some(xnoded_id) = xnoded_id else {
                    return Err(FlightError::ProtocolError(
                        "Received heartbeat before handshake".to_string(),
                    ));
                };
                process!(xnode::api::heartbeat(xnoded_id, context), HEARTBEAT_RESP);
            }
            (HEARTBEAT_REQ, _, RpcClientType::Guest) => {
                let batch = build_batch(HEARTBEAT_RESP, "", req_id).map_err(FlightError::Arrow);
                tx.send_async(batch)
                    .await
                    .map_err(|_| send_resp_error(HEARTBEAT_RESP))?;
            }
            (PLAN_TASK_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                tracing::info!("Received plan task request");
                process!(xnode::api::plan_task(context).await, PLAN_TASK_RESP);
            }
            (START_TASK_JOB_REQ, _, RpcClientType::Xnoded) => {
                tracing::info!("Received start task job request");
                process!(
                    xnode::api::start_task_job(controller, context, tx.clone()).await,
                    START_TASK_JOB_RESP
                );
            }
            (STOP_TASK_JOB_REQ, _, RpcClientType::Xnoded) => {
                tracing::info!("Received stop task job request");
                process!(
                    xnode::api::stop_task_job(controller, context).await,
                    STOP_TASK_JOB_RESP
                );
            }
            (LIST_TASK_JOB_STATES_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                tracing::info!("Received list task job states request");
                process!(
                    xnode::api::list_task_states(controller).await,
                    LIST_TASK_JOB_STATES_RESP
                );
            }
            (ADD_AGENTS_REQ, _, RpcClientType::Xnoded) => {
                tracing::info!("Received add agents request");
                process!(
                    xnode::api::add_agents(controller, context).await,
                    ADD_AGENTS_RESP
                );
            }
            (DEL_AGENTS_REQ, _, RpcClientType::Xnoded) => {
                tracing::info!("Received delete agents request");
                process!(
                    xnode::api::del_agents(controller, context).await,
                    DEL_AGENTS_RESP
                );
            }
            (LIST_AGENTS_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                tracing::info!("Received list agents request");
                process!(xnode::api::list_agents(controller).await, LIST_AGENTS_RESP);
            }
            (CHECK_VALID_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                tracing::info!("Received check valid request");
                process!(xnode::api::check_valid(context).await, CHECK_VALID_RESP);
            }
            (GET_SAMPLES_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                tracing::info!("Received get samples request");
                process!(xnode::api::get_samples(context).await, GET_SAMPLES_RESP);
            }
            (GET_X_HTTP_PORT_REQ, _, RpcClientType::Guest) => {
                tracing::info!("Received get x http port request");
                process!(xnode::api::get_x_http_port(), GET_X_HTTP_PORT_RESP);
            }
            (TASK_PREVIEW_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                tracing::info!("Received task preview request");
                process!(xnode::api::task_preview(context).await, TASK_PREVIEW_RESP);
            }
            (TASK_JOB_DRAIN_REQ, _, RpcClientType::Xnoded) => {
                tracing::info!("Received task job drain request");
                process!(xnode::api::drain(controller).await, TASK_JOB_DRAIN_RESP);
            }
            (action, agent, client_type) => {
                tracing::warn!(
                    action,agent,?xnoded_id,%client_type,
                    "Invalid RPC request "
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
