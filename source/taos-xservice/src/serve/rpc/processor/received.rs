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
    scheduler::{NotifySender, agent::AgentNotifySender},
};
use std::future::Future;

#[derive(Debug, PartialEq, Eq)]
struct ReceivedLogFields<'a> {
    action: &'a str,
    req_id: u64,
    client_type: RpcClientType,
}

fn build_received_log_fields(
    action: &str,
    req_id: u64,
    client_type: RpcClientType,
) -> ReceivedLogFields<'_> {
    ReceivedLogFields {
        action,
        req_id,
        client_type,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HandledLogFields<'a> {
    action: &'a str,
    req_id: u64,
    outcome: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandledOutcome {
    Success,
    Failure,
}

fn build_handled_log_fields(
    action: &str,
    req_id: u64,
    outcome: HandledOutcome,
) -> HandledLogFields<'_> {
    HandledLogFields {
        action,
        req_id,
        outcome: match outcome {
            HandledOutcome::Success => "success",
            HandledOutcome::Failure => "failure",
        },
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SendRespErrorLogFields<'a> {
    action: &'a str,
    req_id: u64,
}

fn build_send_resp_error_log_fields(action: &str, req_id: u64) -> SendRespErrorLogFields<'_> {
    SendRespErrorLogFields { action, req_id }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiveLogLevel {
    Info,
    Debug,
}

fn log_received_request(
    level: ReceiveLogLevel,
    fields: &ReceivedLogFields<'_>,
    xnoded_id: Option<&XnodedId>,
    agent_id: Option<i64>,
) {
    match level {
        ReceiveLogLevel::Info => tracing::info!(
            action = fields.action,
            req_id = fields.req_id,
            client_type = %fields.client_type,
            ?xnoded_id,
            agent_id,
            "Received RPC request"
        ),
        ReceiveLogLevel::Debug => tracing::debug!(
            action = fields.action,
            req_id = fields.req_id,
            client_type = %fields.client_type,
            ?xnoded_id,
            agent_id,
            "Received RPC request"
        ),
    }
}

fn log_handled_request(
    level: ReceiveLogLevel,
    handled_fields: &HandledLogFields<'_>,
    latency_ms: u64,
) {
    match (level, handled_fields.outcome) {
        (ReceiveLogLevel::Info, _) => tracing::info!(
            action = handled_fields.action,
            req_id = handled_fields.req_id,
            outcome = handled_fields.outcome,
            latency_ms,
            "Handled RPC request"
        ),
        (ReceiveLogLevel::Debug, "success") => tracing::debug!(
            action = handled_fields.action,
            req_id = handled_fields.req_id,
            outcome = handled_fields.outcome,
            latency_ms,
            "Handled RPC request"
        ),
        (ReceiveLogLevel::Debug, _) => tracing::warn!(
            action = handled_fields.action,
            req_id = handled_fields.req_id,
            outcome = handled_fields.outcome,
            latency_ms,
            "Handled RPC request"
        ),
    }
}

async fn run_with_receive_lifecycle<Fut>(
    level: ReceiveLogLevel,
    fields: &ReceivedLogFields<'_>,
    xnoded_id: Option<&XnodedId>,
    agent_id: Option<i64>,
    operation: Fut,
) -> Result<(), FlightError>
where
    Fut: Future<Output = Result<HandledOutcome, FlightError>>,
{
    log_received_request(level, fields, xnoded_id, agent_id);
    let started = std::time::Instant::now();
    let handled_outcome = match operation.await {
        Ok(outcome) => outcome,
        Err(source) => {
            let handled_fields =
                build_handled_log_fields(fields.action, fields.req_id, HandledOutcome::Failure);
            log_handled_request(level, &handled_fields, started.elapsed().as_millis() as u64);
            return Err(source);
        }
    };

    let handled_fields = build_handled_log_fields(fields.action, fields.req_id, handled_outcome);
    log_handled_request(level, &handled_fields, started.elapsed().as_millis() as u64);
    Ok(())
}

fn log_invalid_request(
    action: &str,
    req_id: u64,
    xnoded_id: Option<&XnodedId>,
    agent_id: Option<i64>,
    client_type: RpcClientType,
) {
    tracing::warn!(
        action,
        req_id,
        agent_id,
        ?xnoded_id,
        %client_type,
        "Received invalid RPC request"
    );
}

async fn send_rpc_response<T>(
    tx: &flume::Sender<Result<RecordBatch, FlightError>>,
    action: &str,
    req_id: u64,
    result: Result<T, FlightError>,
) -> Result<HandledOutcome, FlightError>
where
    T: serde::Serialize,
{
    let (batch, outcome) = match result {
        Ok(response) => (
            build_rpc_ok_batch(action, response, req_id)?,
            HandledOutcome::Success,
        ),
        Err(error) => (
            build_rpc_failed_batch(action, error, req_id)?,
            HandledOutcome::Failure,
        ),
    };
    send_prebuilt_response(tx, action, req_id, Ok(batch), outcome).await
}

async fn send_prebuilt_response(
    tx: &flume::Sender<Result<RecordBatch, FlightError>>,
    action: &str,
    req_id: u64,
    batch: Result<RecordBatch, FlightError>,
    outcome: HandledOutcome,
) -> Result<HandledOutcome, FlightError> {
    tx.send_async(batch)
        .await
        .map_err(|_| send_resp_error(action, req_id))?;
    Ok(outcome)
}

#[derive(Clone, Copy)]
struct AgentReceiveDependencies<'a> {
    datasets_senders: &'a DataSetsSenders,
    dsv_senders: &'a DsvSenders,
    string_senders: &'a StringSenders,
    split_task_senders: &'a SplitTaskSenders,
    notify_sender: &'a AgentNotifySender,
    activity_sender: &'a NotifySender,
}

async fn run_agent_receive_lifecycle<Fut>(
    fields: &ReceivedLogFields<'_>,
    xnoded_id: Option<&XnodedId>,
    agent_id: Option<i64>,
    operation: Fut,
) -> Result<(), FlightError>
where
    Fut: Future<Output = Result<HandledOutcome, FlightError>>,
{
    run_with_receive_lifecycle(
        ReceiveLogLevel::Info,
        fields,
        xnoded_id,
        agent_id,
        operation,
    )
    .await
}

async fn process_agent_receive_action(
    action: &str,
    context: &str,
    fields: &ReceivedLogFields<'_>,
    xnoded_id: Option<&XnodedId>,
    agent_id: Option<i64>,
    dependencies: AgentReceiveDependencies<'_>,
) -> Result<bool, FlightError> {
    match (action, agent_id, fields.client_type) {
        (ACTION_LIST_DATA_SETS, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::list_response(agent_id, context, dependencies.datasets_senders)
                    .await;
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (ACTION_CHECK, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::check_response(agent_id, context, dependencies.dsv_senders).await;
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (ACTION_GET_SAMPLE, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::sample_response(agent_id, context, dependencies.string_senders)
                    .await;
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (ACTION_SPLIT_TASK, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::split_task_response(
                    agent_id,
                    context,
                    dependencies.split_task_senders,
                )
                .await;
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (ACTION_PUT_FILE, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::put_file_response(agent_id, context, dependencies.string_senders)
                    .await;
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (ACTION_QUERY_DATA_SOURCE, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::query_datasource_response(
                    agent_id,
                    context,
                    dependencies.string_senders,
                )
                .await;
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (MESSAGE_AGENT_ACTIVITY, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::agent_activity(
                    agent_id,
                    context,
                    dependencies.notify_sender,
                    dependencies.activity_sender,
                );
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (MESSAGE_TASK_ACTIVITY, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::task_activity(
                    agent_id,
                    context,
                    dependencies.notify_sender,
                    dependencies.activity_sender,
                );
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (MESSAGE_HEARTBEAT_OK, Some(agent_id), RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, Some(agent_id), async {
                agent::response::heartbeat_ok(agent_id, context);
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (MESSAGE_TASK_METRICS, _, RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, agent_id, async {
                agent::response::task_metrics(context);
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        (MESSAGE_METRICS_EVENTS, _, RpcClientType::Agent) => {
            run_agent_receive_lifecycle(fields, xnoded_id, agent_id, async {
                agent::response::metrics_events(context);
                Ok(HandledOutcome::Success)
            })
            .await?;
        }
        _ => return Ok(false),
    }

    Ok(true)
}

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
    let dependencies = AgentReceiveDependencies {
        datasets_senders,
        dsv_senders,
        string_senders,
        split_task_senders,
        notify_sender,
        activity_sender,
    };

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
        let fields = build_received_log_fields(action, req_id, client_type);

        if process_agent_receive_action(action, context, &fields, xnoded_id, agent_id, dependencies)
            .await?
        {
            continue;
        }

        macro_rules! process {
            ($req: expr, $result: expr, $action: expr) => {
                process!(info, $req, $result, $action)
            };
            ($level: ident, $req: expr, $result: expr, $action: expr) => {
                run_with_receive_lifecycle(
                    receive_log_level!($level),
                    &fields,
                    xnoded_id,
                    agent_id,
                    async { send_rpc_response(tx, $action, req_id, $result).await },
                )
                .await?;
            };
        }
        macro_rules! receive_log_level {
            (info) => {
                ReceiveLogLevel::Info
            };
            (debug) => {
                ReceiveLogLevel::Debug
            };
        }
        match (action, agent_id, client_type) {
            (MESSAGE_HEARTBEAT, _, RpcClientType::Agent) => {
                run_with_receive_lifecycle(
                    ReceiveLogLevel::Info,
                    &fields,
                    xnoded_id,
                    agent_id,
                    async {
                        let batch = agent::response::heartbeat(ts, req_id);
                        let outcome = if batch.is_ok() {
                            HandledOutcome::Success
                        } else {
                            HandledOutcome::Failure
                        };
                        send_prebuilt_response(tx, MESSAGE_HEARTBEAT, req_id, batch, outcome).await
                    },
                )
                .await?;
            }
            (HEARTBEAT_REQ, _, RpcClientType::Xnoded) => {
                let Some(xnoded_id) = xnoded_id else {
                    return Err(FlightError::ProtocolError(
                        "Received heartbeat before handshake".to_string(),
                    ));
                };
                process!(
                    debug,
                    HEARTBEAT_REQ,
                    xnode::api::heartbeat(xnoded_id, context),
                    HEARTBEAT_RESP
                );
            }
            (HEARTBEAT_REQ, _, RpcClientType::Guest) => {
                process!(
                    debug,
                    HEARTBEAT_REQ,
                    Ok::<_, FlightError>(()),
                    HEARTBEAT_RESP
                );
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
                log_invalid_request(action, req_id, xnoded_id, agent, client_type);
            }
        }
    }
    Ok(())
}

fn send_resp_error(action: &str, req_id: u64) -> FlightError {
    let fields = build_send_resp_error_log_fields(action, req_id);
    tracing::error!(
        action = fields.action,
        req_id = fields.req_id,
        "rpc response send failed"
    );
    FlightError::ProtocolError(format!(
        "Failed to send {action} response for req_id={req_id}, receiver dropped"
    ))
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use arrow::array::RecordBatch;
    use arrow_flight::error::FlightError;
    use ha_core::{
        batch::build_batch,
        consts::{
            ACTION_CHECK, ACTION_GET_SAMPLE, ACTION_LIST_DATA_SETS, ACTION_PUT_FILE,
            ACTION_QUERY_DATA_SOURCE, ACTION_SPLIT_TASK, HEARTBEAT_REQ, MESSAGE_AGENT_ACTIVITY,
            MESSAGE_HEARTBEAT, MESSAGE_HEARTBEAT_OK, MESSAGE_METRICS_EVENTS, MESSAGE_TASK_ACTIVITY,
            MESSAGE_TASK_METRICS, PLAN_TASK_REQ,
        },
        types::{RpcClientType, XnodedId},
    };
    use linked_hash_map::LinkedHashMap;
    use parking_lot::RwLock;
    use tracing_subscriber::fmt::MakeWriter;

    const AGENT_LIFECYCLE_ACTIONS: &[&str] = &[
        ACTION_LIST_DATA_SETS,
        ACTION_CHECK,
        ACTION_GET_SAMPLE,
        ACTION_SPLIT_TASK,
        ACTION_PUT_FILE,
        ACTION_QUERY_DATA_SOURCE,
        MESSAGE_AGENT_ACTIVITY,
        MESSAGE_TASK_ACTIVITY,
        MESSAGE_HEARTBEAT_OK,
        MESSAGE_TASK_METRICS,
        MESSAGE_METRICS_EVENTS,
    ];
    static TEST_GUARD: Mutex<()> = Mutex::new(());

    #[test]
    fn received_log_fields_include_action_req_id_and_client_type() {
        let fields = super::build_received_log_fields("PLAN_TASK_REQ", 9, RpcClientType::Xnoded);
        assert_eq!(fields.action, "PLAN_TASK_REQ");
        assert_eq!(fields.req_id, 9);
        assert_eq!(fields.client_type, RpcClientType::Xnoded);
    }

    #[test]
    fn handled_log_fields_include_success_outcome() {
        let fields =
            super::build_handled_log_fields("PLAN_TASK_REQ", 9, super::HandledOutcome::Success);
        assert_eq!(fields.action, "PLAN_TASK_REQ");
        assert_eq!(fields.req_id, 9);
        assert_eq!(fields.outcome, "success");
    }

    #[test]
    fn handled_log_fields_include_failure_outcome() {
        let fields =
            super::build_handled_log_fields("PLAN_TASK_REQ", 9, super::HandledOutcome::Failure);
        assert_eq!(fields.action, "PLAN_TASK_REQ");
        assert_eq!(fields.req_id, 9);
        assert_eq!(fields.outcome, "failure");
    }

    #[test]
    fn invalid_request_logs_use_agent_id_field_name() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        super::log_invalid_request("UNKNOWN_REQ", 77, None, Some(7), RpcClientType::Agent);

        let logs = log_buffer.contents();
        assert!(logs.contains("Received invalid RPC request"));
        assert!(logs.contains("action=\"UNKNOWN_REQ\""));
        assert!(logs.contains("req_id=77"));
        assert!(logs.contains("agent_id=7"));
        assert!(!logs.contains("agent=7"));
    }

    #[test]
    fn response_send_error_includes_req_id_in_metadata_and_error() {
        let fields = super::build_send_resp_error_log_fields("PLAN_TASK_RESP", 21);
        assert_eq!(fields.action, "PLAN_TASK_RESP");
        assert_eq!(fields.req_id, 21);

        let err = super::send_resp_error("PLAN_TASK_RESP", 21);
        match err {
            FlightError::ProtocolError(message) => {
                assert_eq!(
                    message,
                    "Failed to send PLAN_TASK_RESP response for req_id=21, receiver dropped"
                );
            }
            other => panic!("unexpected response send error: {other:?}"),
        }
    }

    #[test]
    fn heartbeat_success_logs_receive_lifecycle_fields() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let fields =
                super::build_received_log_fields(MESSAGE_HEARTBEAT, 41, RpcClientType::Agent);
            let (tx, rx) = flume::bounded::<Result<RecordBatch, FlightError>>(1);
            let xnoded_id = XnodedId {
                cluster_id: "cluster-a".to_string(),
                leader_ep: "127.0.0.1:6050".to_string(),
            };

            super::run_with_receive_lifecycle(
                super::ReceiveLogLevel::Info,
                &fields,
                Some(&xnoded_id),
                Some(7),
                async {
                    let batch = build_batch(MESSAGE_HEARTBEAT_OK, "{}", 41)
                        .map_err(|err| FlightError::DecodeError(err.to_string()))?;
                    tx.send_async(Ok(batch))
                        .await
                        .map_err(|_| super::send_resp_error(MESSAGE_HEARTBEAT, 41))?;
                    Ok(super::HandledOutcome::Success)
                },
            )
            .await
            .expect("heartbeat processing succeeds");

            let _ = rx.recv_async().await.expect("heartbeat response is sent");
        });

        let logs = log_buffer.contents();
        assert!(logs.contains("Received RPC request"));
        assert!(logs.contains("Handled RPC request"));
        assert!(logs.contains("action=\"agent_heartbeat\""));
        assert!(logs.contains("req_id=41"));
        assert!(logs.contains("client_type=agent"));
        assert!(logs.contains("agent_id=7"));
        assert!(logs.contains("cluster_id: \"cluster-a\""));
        assert!(logs.contains("outcome=\"success\""));
        assert!(logs.contains("latency_ms="));
    }

    #[test]
    fn heartbeat_failed_batch_logs_failure_outcome() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let fields =
                super::build_received_log_fields(MESSAGE_HEARTBEAT, 42, RpcClientType::Agent);
            let (tx, rx) = flume::bounded::<Result<RecordBatch, FlightError>>(1);

            super::run_with_receive_lifecycle(
                super::ReceiveLogLevel::Info,
                &fields,
                None,
                Some(7),
                async {
                    super::send_prebuilt_response(
                        &tx,
                        MESSAGE_HEARTBEAT,
                        42,
                        Err(FlightError::ProtocolError(
                            "heartbeat batch build failed".to_string(),
                        )),
                        super::HandledOutcome::Failure,
                    )
                    .await
                },
            )
            .await
            .expect("transport succeeds even when heartbeat batch reports failure");

            let err = rx.recv_async().await.expect("heartbeat response is sent");
            match err {
                Err(FlightError::ProtocolError(message)) => {
                    assert_eq!(message, "heartbeat batch build failed");
                }
                other => panic!("unexpected heartbeat response: {other:?}"),
            }
        });

        let logs = log_buffer.contents();
        assert!(logs.contains("Handled RPC request"));
        assert!(logs.contains("action=\"agent_heartbeat\""));
        assert!(logs.contains("req_id=42"));
        assert!(logs.contains("outcome=\"failure\""));
    }

    #[test]
    fn xnoded_heartbeat_lifecycle_logs_are_suppressed() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let fields = super::build_received_log_fields(HEARTBEAT_REQ, 43, RpcClientType::Xnoded);
            let xnoded_id = XnodedId {
                cluster_id: "cluster-a".to_string(),
                leader_ep: "127.0.0.1:6050".to_string(),
            };

            super::run_with_receive_lifecycle(
                super::ReceiveLogLevel::Debug,
                &fields,
                Some(&xnoded_id),
                None,
                async { Ok(super::HandledOutcome::Success) },
            )
            .await
            .expect("xnoded heartbeat should succeed");
        });

        let logs = log_buffer.contents();
        assert!(!logs.contains("Received RPC request"));
        assert!(!logs.contains("Handled RPC request"));
        assert!(!logs.contains(&format!("action=\"{HEARTBEAT_REQ}\"")));
    }

    #[test]
    fn guest_heartbeat_lifecycle_logs_are_suppressed() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let fields = super::build_received_log_fields(HEARTBEAT_REQ, 44, RpcClientType::Guest);

            super::run_with_receive_lifecycle(
                super::ReceiveLogLevel::Debug,
                &fields,
                None,
                None,
                async { Ok(super::HandledOutcome::Success) },
            )
            .await
            .expect("guest heartbeat should succeed");
        });

        let logs = log_buffer.contents();
        assert!(!logs.contains("Received RPC request"));
        assert!(!logs.contains("Handled RPC request"));
        assert!(!logs.contains(&format!("action=\"{HEARTBEAT_REQ}\"")));
    }

    #[test]
    fn debug_level_failures_still_log_lifecycle_events() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let fields = super::build_received_log_fields(HEARTBEAT_REQ, 45, RpcClientType::Guest);

            super::run_with_receive_lifecycle(
                super::ReceiveLogLevel::Debug,
                &fields,
                None,
                None,
                async { Ok(super::HandledOutcome::Failure) },
            )
            .await
            .expect("guest heartbeat failure should complete");
        });

        let logs = log_buffer.contents();
        assert!(logs.contains("Handled RPC request"));
        assert!(logs.contains(&format!("action=\"{HEARTBEAT_REQ}\"")));
        assert!(logs.contains("outcome=\"failure\""));
    }

    #[test]
    fn receive_lifecycle_logs_failure_when_operation_reports_failed_batch() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let fields = super::build_received_log_fields(PLAN_TASK_REQ, 52, RpcClientType::Xnoded);

            super::run_with_receive_lifecycle(
                super::ReceiveLogLevel::Info,
                &fields,
                None,
                None,
                async { Ok(super::HandledOutcome::Failure) },
            )
            .await
            .expect("transport succeeds even when rpc batch reports failure");
        });

        let logs = log_buffer.contents();
        assert!(logs.contains("Received RPC request"));
        assert!(logs.contains("Handled RPC request"));
        assert!(logs.contains(&format!("action=\"{PLAN_TASK_REQ}\"")));
        assert!(logs.contains("req_id=52"));
        assert!(logs.contains("outcome=\"failure\""));
    }

    #[test]
    fn agent_receive_actions_use_shared_lifecycle_logging() {
        let _test_guard = TEST_GUARD.lock().expect("lock test guard");
        let log_buffer = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(log_buffer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build test runtime");
        runtime.block_on(async {
            let datasets_senders = Arc::new(RwLock::new(LinkedHashMap::new()));
            let dsv_senders = Arc::new(RwLock::new(LinkedHashMap::new()));
            let string_senders = Arc::new(RwLock::new(LinkedHashMap::new()));
            let split_task_senders = Arc::new(RwLock::new(LinkedHashMap::new()));
            let (notify_sender, _notify_rx) = tokio::sync::broadcast::channel(8);
            let (activity_sender, _activity_rx) = tokio::sync::broadcast::channel(8);
            let activity_sender = Arc::new(activity_sender);
            let dependencies = super::AgentReceiveDependencies {
                datasets_senders: &datasets_senders,
                dsv_senders: &dsv_senders,
                string_senders: &string_senders,
                split_task_senders: &split_task_senders,
                notify_sender: &notify_sender,
                activity_sender: &activity_sender,
            };

            for (index, action) in AGENT_LIFECYCLE_ACTIONS.iter().copied().enumerate() {
                let req_id = 100 + index as u64;
                let fields = super::build_received_log_fields(action, req_id, RpcClientType::Agent);
                let handled = super::process_agent_receive_action(
                    action,
                    "{}",
                    &fields,
                    None,
                    Some(7),
                    dependencies,
                )
                .await
                .expect("agent receive action should not fail");
                assert!(
                    handled,
                    "{action} should be handled by agent receive processing"
                );
            }
        });

        let logs = log_buffer.contents();
        for (index, action) in AGENT_LIFECYCLE_ACTIONS.iter().copied().enumerate() {
            let req_id = 100 + index as u64;
            let req_logs = logs
                .lines()
                .filter(|line| line.contains(&format!("req_id={req_id}")))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                req_logs.contains("Received RPC request"),
                "missing received lifecycle log for {action}: {req_logs}"
            );
            assert!(
                req_logs.contains("Handled RPC request"),
                "missing handled lifecycle log for {action}: {req_logs}"
            );
            assert!(
                req_logs.contains(&format!("action=\"{action}\"")),
                "missing action field for {action}: {req_logs}"
            );
            assert!(
                req_logs.contains("outcome=\"success\""),
                "missing success outcome for {action}: {req_logs}"
            );
            assert!(
                req_logs.contains("latency_ms="),
                "missing latency for {action}: {req_logs}"
            );
        }
        assert!(logs.contains("agent_id=7"));
    }

    #[derive(Clone, Default)]
    struct SharedLogBuffer(Arc<Mutex<Vec<u8>>>);

    impl SharedLogBuffer {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().expect("lock log buffer").clone())
                .expect("log output is utf8")
        }
    }

    impl<'a> MakeWriter<'a> for SharedLogBuffer {
        type Writer = SharedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedLogWriter(self.0.clone())
        }
    }

    struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("lock log writer")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
