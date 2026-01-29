use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_flight::{decode::DecodedFlightData, error::FlightError};
use ha_core::{
    batch::BatchIter,
    consts::*,
    types::{RpcClientType, RpcRecord, XnodedId},
};

use crate::serve::rpc::{
    DataSetsSenders, SplitTaskSenders,
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
    split_task_senders: &SplitTaskSenders,
    notify_sender: &AgentNotifySender,
    tx: &flume::Sender<Result<RecordBatch, FlightError>>,
    client_type: RpcClientType,
) -> Result<(), FlightError> {
    let arrow_flight::decode::DecodedPayload::RecordBatch(batch) = data.payload else {
        return Ok(());
    };
    debug_assert!(batch.num_rows() == 1);
    let activity_sender = controller.scheduler.notify_sender();

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
            ($req: expr, $result: expr, $action: expr) => {
                process!(info, $req, $result, $action)
            };
            ($level: ident, $req: expr, $result: expr, $action: expr) => {
                tracing::$level!("Received {} request", $req);
                let _process_guard =
                    taosx_core::utils::defer::defer(|| tracing::$level!("Request {} done", $req));
                process!($result, $action)
            };
            ($result: expr, $action: expr) => {
                let batch = match $result {
                    Ok(res) => build_rpc_ok_batch($action, res, req_id)?,
                    Err(e) => build_rpc_failed_batch($action, e, req_id)?,
                };
                tx.send_async(Ok(batch))
                    .await
                    .map_err(|_| send_resp_error($action))?;
            };
        }
        match (action, agent_id, client_type) {
            (ACTION_LIST_DATA_SETS, Some(agent_id), RpcClientType::Agent) => {
                agent::response::list_response(agent_id, context, datasets_senders).await;
            }
            (ACTION_CHECK, Some(agent_id), RpcClientType::Agent) => {
                agent::response::check_response(agent_id, context, dsv_senders).await;
            }
            (ACTION_GET_SAMPLE, Some(agent_id), RpcClientType::Agent) => {
                agent::response::sample_response(agent_id, context, string_senders).await;
            }
            (ACTION_SPLIT_TASK, Some(agent_id), RpcClientType::Agent) => {
                agent::response::split_task_response(agent_id, context, split_task_senders).await;
            }
            (ACTION_PUT_FILE, Some(agent_id), RpcClientType::Agent) => {
                agent::response::put_file_response(agent_id, context, string_senders).await;
            }
            (ACTION_QUERY_DATA_SOURCE, Some(agent_id), RpcClientType::Agent) => {
                agent::response::query_datasource_response(agent_id, context, string_senders).await;
            }
            (MESSAGE_AGENT_ACTIVITY, Some(agent_id), RpcClientType::Agent) => {
                agent::response::agent_activity(
                    agent_id,
                    context,
                    notify_sender,
                    activity_sender.as_deref(),
                );
            }
            (MESSAGE_TASK_ACTIVITY, Some(agent_id), RpcClientType::Agent) => {
                agent::response::task_activity(
                    agent_id,
                    context,
                    notify_sender,
                    activity_sender.as_deref(),
                );
            }
            (MESSAGE_HEARTBEAT_OK, Some(agent_id), RpcClientType::Agent) => {
                agent::response::heartbeat_ok(agent_id, context);
            }
            (MESSAGE_HEARTBEAT, _, RpcClientType::Agent) => {
                let item = agent::response::heartbeat(ts, req_id);
                tx.send_async(item).await.map_err(|_| {
                    FlightError::ProtocolError(
                        "Failed to send heartbeat response, stream dropped".to_string(),
                    )
                })?;
            }
            (MESSAGE_TASK_METRICS, _, RpcClientType::Agent) => {
                agent::response::task_metrics(context);
            }
            (MESSAGE_METRICS_EVENTS, _, RpcClientType::Agent) => {
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
                process!(Ok(()), HEARTBEAT_RESP);
            }
            (PLAN_TASK_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                process!(
                    PLAN_TASK_REQ,
                    xnode::api::plan_task(controller, context).await,
                    PLAN_TASK_RESP
                );
            }
            (START_TASK_JOB_REQ, _, RpcClientType::Xnoded) => {
                process!(
                    START_TASK_JOB_REQ,
                    xnode::api::start_task_job(controller, context, tx.clone()).await,
                    START_TASK_JOB_RESP
                );
            }
            (STOP_TASK_JOB_REQ, _, RpcClientType::Xnoded) => {
                process!(
                    STOP_TASK_JOB_REQ,
                    xnode::api::stop_task_job(controller, context).await,
                    STOP_TASK_JOB_RESP
                );
            }
            (LIST_TASK_JOB_STATES_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                process!(
                    debug,
                    LIST_TASK_JOB_STATES_REQ,
                    xnode::api::list_task_states(controller).await,
                    LIST_TASK_JOB_STATES_RESP
                );
            }
            (ADD_AGENTS_REQ, _, RpcClientType::Xnoded) => {
                process!(
                    ADD_AGENTS_REQ,
                    xnode::api::add_agents(controller, context).await,
                    ADD_AGENTS_RESP
                );
            }
            (DEL_AGENTS_REQ, _, RpcClientType::Xnoded) => {
                process!(
                    DEL_AGENTS_REQ,
                    xnode::api::del_agents(controller, context).await,
                    DEL_AGENTS_RESP
                );
            }
            (LIST_AGENTS_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                process!(
                    debug,
                    LIST_AGENTS_REQ,
                    xnode::api::list_agents(controller).await,
                    LIST_AGENTS_RESP
                );
            }
            (CHECK_VALID_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                process!(
                    CHECK_VALID_REQ,
                    xnode::api::check_valid(controller, context).await,
                    CHECK_VALID_RESP
                );
            }
            (GET_SAMPLES_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                process!(
                    GET_SAMPLES_REQ,
                    xnode::api::get_samples(controller, context).await,
                    GET_SAMPLES_RESP
                );
            }
            (GET_X_HTTP_PORT_REQ, _, RpcClientType::Guest) => {
                process!(
                    GET_X_HTTP_PORT_REQ,
                    xnode::api::get_x_http_port(),
                    GET_X_HTTP_PORT_RESP
                );
            }
            (TASK_PREVIEW_REQ, _, RpcClientType::Xnoded | RpcClientType::Guest) => {
                process!(
                    TASK_PREVIEW_REQ,
                    xnode::api::task_preview(context).await,
                    TASK_PREVIEW_RESP
                );
            }
            (TASK_JOB_DRAIN_REQ, _, RpcClientType::Xnoded) => {
                process!(
                    TASK_JOB_DRAIN_REQ,
                    xnode::api::drain(controller).await,
                    TASK_JOB_DRAIN_RESP
                );
            }
            (HEARTBEAT_RESP, _, RpcClientType::Xnoded | RpcClientType::Guest) => {}
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
