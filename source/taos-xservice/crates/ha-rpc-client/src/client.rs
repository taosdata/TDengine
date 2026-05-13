use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use ha_core::{
    batch::{BatchIter, build_batch},
    consts::*,
    types::*,
    utils::next_req_id,
};
use snafu::{OptionExt, ResultExt};
use tokio::sync::oneshot;

use crate::{RpcRequest, error::*};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RpcLogFields<'a> {
    action: &'a str,
    req_id: u64,
}

fn build_rpc_log_fields(action: &str, req_id: u64) -> RpcLogFields<'_> {
    RpcLogFields { action, req_id }
}

fn should_log_rpc_request(action: &str) -> bool {
    action != HEARTBEAT_REQ
}

fn log_rpc_request(fields: &RpcLogFields<'_>) {
    match fields.action {
        LIST_TASK_JOB_STATES_REQ => tracing::debug!(
            action = fields.action,
            req_id = fields.req_id,
            direction = "xnoded_to_taosx",
            "Received RPC request from xnoded to taosx"
        ),
        _ if should_log_rpc_request(fields.action) => tracing::info!(
            action = fields.action,
            req_id = fields.req_id,
            direction = "xnoded_to_taosx",
            "Received RPC request from xnoded to taosx"
        ),
        _ => {}
    }
}

fn log_rpc_request_error(fields: &RpcLogFields<'_>, error: &Error) {
    if fields.action == LIST_TASK_JOB_STATES_REQ {
        tracing::error!(
            action = fields.action,
            req_id = fields.req_id,
            direction = "xnoded_to_taosx",
            error = %error,
            "RPC request from xnoded to taosx failed"
        );
    }
}

fn serialize_request<Q>(param: &Q) -> Result<String>
where
    Q: serde::Serialize,
{
    serde_json::to_string(param).context(SerializeReqSnafu)
}

fn build_request_batch(fields: &RpcLogFields<'_>, param: &str) -> Result<RecordBatch> {
    build_request_batch_with(fields, param, build_batch)
}

fn build_request_batch_with<F>(
    fields: &RpcLogFields<'_>,
    param: &str,
    batch_builder: F,
) -> Result<RecordBatch>
where
    F: FnOnce(&str, &str, u64) -> std::result::Result<RecordBatch, ArrowError>,
{
    batch_builder(fields.action, param, fields.req_id).context(BuildReqBatchSnafu)
}

#[derive(Clone)]
pub struct HaRpcClient {
    message_tx: flume::Sender<RpcRequest>,
}

impl HaRpcClient {
    pub(crate) fn new(message_tx: flume::Sender<RpcRequest>) -> Self {
        Self { message_tx }
    }

    pub async fn send_no_reply_batch(&self, batch: RecordBatch) -> Result<()> {
        self.message_tx
            .send_async((batch, None))
            .await
            .map_err(|_| EventLoopDroppedSnafu.build())
    }

    pub async fn heartbeat(&self, xnoded_id: &XnodedId) -> Result<HeartbeatMetrics> {
        self.send_recv(HEARTBEAT_REQ, xnoded_id).await
    }

    pub async fn guest_heartbeat(&self) -> Result<()> {
        self.send_recv(HEARTBEAT_REQ, &()).await
    }

    pub async fn plan_task(&self, task: &HaTask) -> Result<SplitJobResult> {
        self.send_recv(PLAN_TASK_REQ, task).await
    }

    pub async fn start_task_job(&self, param: &StartTaskJobParam) -> Result<()> {
        self.send_recv(START_TASK_JOB_REQ, param).await
    }

    pub async fn stop_task_job(&self, param: &StopTaskJobParam) -> Result<()> {
        self.send_recv(STOP_TASK_JOB_REQ, param).await
    }

    pub async fn list_task_job_states(&self) -> Result<ListTaskJobStatesResult> {
        self.send_recv(LIST_TASK_JOB_STATES_REQ, &()).await
    }

    pub async fn task_preview(&self, param: &TaskPreviewParam) -> Result<serde_json::Value> {
        self.send_recv(TASK_PREVIEW_REQ, param).await
    }

    pub async fn check_valid(&self, param: &CheckValidParam) -> Result<serde_json::Value> {
        self.send_recv(CHECK_VALID_REQ, param).await
    }

    pub async fn get_samples(&self, param: &GetSamplesParam) -> Result<GetSamplesResult> {
        self.send_recv(GET_SAMPLES_REQ, param).await
    }

    pub async fn add_agents(&self, param: AddAgentsParam<'_>) -> Result<()> {
        self.send_recv(ADD_AGENTS_REQ, param).await
    }

    pub async fn del_agents(&self, param: DelAgentsParam<'_>) -> Result<()> {
        self.send_recv(DEL_AGENTS_REQ, param).await
    }

    pub async fn list_agents(&self) -> Result<ListAgentsResult> {
        self.send_recv(LIST_AGENTS_REQ, ()).await
    }

    pub async fn drain_task_job(&self) -> Result<()> {
        self.send_recv(TASK_JOB_DRAIN_REQ, ()).await
    }

    pub async fn get_x_http_port(&self) -> Result<Option<Vec<u16>>> {
        self.send_recv(GET_X_HTTP_PORT_REQ, ()).await
    }

    async fn send_recv<Q, R>(&self, action: &str, param: Q) -> Result<R>
    where
        Q: serde::Serialize,
        R: for<'a> serde::Deserialize<'a>,
    {
        let fields = build_rpc_log_fields(action, next_req_id());
        let param = serialize_request(&param)?;
        self.send_recv_with_fields(fields, &param).await
    }

    #[cfg(test)]
    async fn send_recv_raw_context<R>(&self, action: &str, param: &str) -> Result<R>
    where
        R: for<'a> serde::Deserialize<'a>,
    {
        let fields = build_rpc_log_fields(action, next_req_id());
        self.send_recv_with_fields(fields, param).await
    }

    async fn send_recv_with_fields<R>(&self, fields: RpcLogFields<'_>, param: &str) -> Result<R>
    where
        R: for<'a> serde::Deserialize<'a>,
    {
        log_rpc_request(&fields);
        let result = async {
            let batch = build_request_batch(&fields, param)?;
            let (tx, rx) = oneshot::channel();
            self.message_tx
                .send_async((batch, Some((fields.req_id, tx))))
                .await
                .map_err(|_| EventLoopDroppedSnafu.build())?;
            let batch = rx
                .await
                .map_err(|_| AckWaiterDroppedUnexpectedlySnafu.build())?;
            let batch = batch?;
            let mut iter = BatchIter::new(&batch).context(BuildBatchIterSnafu)?;
            let record = iter.next().context(ResponseNoContextSnafu)?;
            let response = serde_json::from_str::<Response<R>>(record.context)
                .context(DeserializeRespSnafu)?;
            match response {
                Response::Data(data) => Ok(data),
                Response::Fail(error) => ResponseFailSnafu { error }.fail(),
            }
        }
        .await;
        if let Err(error) = &result {
            log_rpc_request_error(&fields, error);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use arrow::{
        array::{RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array},
        datatypes::{DataType, Field as ArrowField, Schema},
        error::ArrowError,
    };
    use ha_core::batch::{BatchIter, SCHEMA, build_batch, build_failed_batch, build_ok_batch};
    use tracing::{
        Event, Id, Level, Metadata, Subscriber,
        field::{Field as TracingField, Visit},
        span::{Attributes, Record},
    };

    use crate::{RpcRequest, error::Error};

    #[test]
    fn rpc_log_fields_include_action_and_req_id() {
        let fields = super::build_rpc_log_fields("PLAN_TASK_REQ", 42);
        assert_eq!(fields.action, "PLAN_TASK_REQ");
        assert_eq!(fields.req_id, 42);
    }

    #[test]
    fn heartbeat_request_logs_are_suppressed() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_ok_batch("xnode_heartbeat_resp", (), req_id)
                        .expect("response batch should build")))
                    .expect("response should be delivered");
            });

            let result = client
                .send_recv_raw_context::<()>("xnode_heartbeat", "{}")
                .await;
            send.await.expect("send task should finish");
            result
        });

        result.expect("heartbeat should succeed");
        assert_event_absent(&events, "Received RPC request from xnoded to taosx");
    }

    #[test]
    fn rpc_log_build_batch_failure_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let fields = super::build_rpc_log_fields("xnode_plan_task", 42);
            super::build_request_batch_with(&fields, "{}", |_action, _context, _req_id| {
                Err(ArrowError::InvalidArgumentError("batch boom".into()))
            })
        });

        assert!(matches!(result, Err(Error::BuildReqBatch { .. })));
        assert_event_absent(&events, "rpc request build batch failed");
    }

    #[test]
    fn rpc_log_success_response_keeps_single_request_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_ok_batch("xnode_plan_task_resp", 7_u64, req_id)
                        .expect("response batch should build")))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert_eq!(result.expect("request should succeed"), 7);
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_field(
            &events,
            "Received RPC request from xnoded to taosx",
            "direction",
            "xnoded_to_taosx",
        );
        assert_event_absent(&events, "rpc response");
    }

    #[test]
    fn list_task_job_states_success_request_log_is_debug() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_ok_batch(
                        "xnode_list_task_job_states_resp",
                        7_u64,
                        req_id,
                    )
                    .expect("response batch should build")))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_list_task_job_states", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert_eq!(result.expect("request should succeed"), 7);
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_list_task_job_states"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_level(
            &events,
            "Received RPC request from xnoded to taosx",
            "DEBUG",
        );
    }

    #[test]
    fn rpc_log_failed_response_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_failed_batch(
                        "xnode_plan_task_resp",
                        "boom",
                        req_id,
                    )
                    .expect("failed response batch should build")))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::ResponseFail { .. })));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_absent(&events, "rpc response");
    }

    #[test]
    fn list_task_job_states_failure_emits_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_failed_batch(
                        "xnode_list_task_job_states_resp",
                        "boom",
                        req_id,
                    )
                    .expect("failed response batch should build")))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_list_task_job_states", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::ResponseFail { .. })));
        assert_event_has_fields(
            &events,
            "RPC request from xnoded to taosx failed",
            &[
                ("action", "xnode_list_task_job_states"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_level(&events, "RPC request from xnoded to taosx failed", "ERROR");
    }

    #[test]
    fn rpc_log_ack_failure_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Err(Error::EventLoopDropped))
                    .expect("error should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::EventLoopDropped)));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_absent(&events, "rpc ack failed");
    }

    #[test]
    fn rpc_log_decode_failure_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_batch("xnode_plan_task_resp", "not-json", req_id)
                        .expect("invalid json batch should build")))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::DeserializeResp { .. })));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_absent(&events, "rpc response decode failed");
    }

    #[test]
    fn rpc_log_ack_waiter_drop_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                drop(ack.expect("ack sender should exist").1);
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::AckWaiterDroppedUnexpectedly)));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_absent(&events, "rpc ack waiter dropped");
    }

    #[test]
    fn rpc_log_send_failure_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (tx, rx) = flume::bounded(1);
            drop(rx);
            let client = super::HaRpcClient::new(tx);
            client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await
        });

        assert!(matches!(result, Err(Error::EventLoopDropped)));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[("action", "xnode_plan_task")],
            &["req_id", "direction"],
        );
        assert_event_absent(&events, "rpc send failed");
    }

    #[test]
    fn rpc_log_batch_iter_failure_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_invalid_response_batch()))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::BuildBatchIter { .. })));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_absent(&events, "rpc response batch decode failed");
    }

    #[test]
    fn rpc_log_response_no_context_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, rx) = test_client();
            let send = tokio::spawn(async move {
                let (request, ack) = rx.recv_async().await.expect("request should be sent");
                let req_id = extract_req_id(&request);
                ack.expect("ack sender should exist")
                    .1
                    .send(Ok(build_empty_response_batch()))
                    .expect("response should be delivered");
                req_id
            });

            let result = client
                .send_recv_raw_context::<u64>("xnode_plan_task", "{}")
                .await;
            let req_id = send.await.expect("send task should finish");
            (result, req_id)
        });

        let (result, req_id) = result;
        assert!(matches!(result, Err(Error::ResponseNoContext)));
        assert_event_has_fields(
            &events,
            "Received RPC request from xnoded to taosx",
            &[
                ("action", "xnode_plan_task"),
                ("req_id", &req_id.to_string()),
            ],
            &["direction"],
        );
        assert_event_absent(&events, "rpc response missing context");
    }

    #[test]
    fn rpc_log_serialize_failure_does_not_emit_error_log() {
        let (result, events) = run_with_recorded_events(async move {
            let (client, _rx) = test_client();
            client
                .send_recv::<FailingSerialize, u64>("xnode_plan_task", FailingSerialize)
                .await
        });

        assert!(matches!(result, Err(Error::SerializeReq { .. })));
        assert_event_absent(&events, "rpc request serialize failed");
    }

    fn test_client() -> (super::HaRpcClient, flume::Receiver<RpcRequest>) {
        let (tx, rx) = flume::unbounded();
        (super::HaRpcClient::new(tx), rx)
    }

    fn extract_req_id(batch: &RecordBatch) -> u64 {
        BatchIter::new(batch)
            .expect("request batch should decode")
            .next()
            .expect("request record should exist")
            .req_id
    }

    fn build_invalid_response_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("ts", DataType::Utf8, false),
            ArrowField::new("action", DataType::Utf8, false),
            ArrowField::new("context", DataType::Utf8, false),
            ArrowField::new("req_id", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["bad-ts"])),
                Arc::new(StringArray::from(vec!["xnode_plan_task_resp"])),
                Arc::new(StringArray::from(vec![r#"{"Data":7}"#])),
                Arc::new(StringArray::from(vec!["42"])),
            ],
        )
        .expect("invalid response batch should build")
    }

    fn build_empty_response_batch() -> RecordBatch {
        RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
            ],
        )
        .expect("empty response batch should build")
    }

    fn run_with_recorded_events<Fut, T>(future: Fut) -> (T, Vec<CapturedEvent>)
    where
        Fut: std::future::Future<Output = T>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let subscriber = RecordingSubscriber::default();
        let events = subscriber.events.clone();
        let dispatch = tracing::Dispatch::new(subscriber);
        let result = tracing::dispatcher::with_default(&dispatch, || runtime.block_on(future));
        let captured = events.lock().expect("events mutex should lock").clone();
        (result, captured)
    }

    fn assert_event_field(events: &[CapturedEvent], message: &str, field: &str, expected: &str) {
        let event = find_event(events, message);
        assert_eq!(
            event.fields.get(field).map(String::as_str),
            Some(expected),
            "expected field {field} on {message}, got {:?}",
            event.fields
        );
    }

    fn assert_event_has_fields(
        events: &[CapturedEvent],
        message: &str,
        expected_fields: &[(&str, &str)],
        required_fields: &[&str],
    ) {
        let event = find_event(events, message);
        for (field, expected) in expected_fields {
            assert_eq!(
                event.fields.get(*field).map(String::as_str),
                Some(*expected),
                "expected field {field} on {message}, got {:?}",
                event.fields
            );
        }
        for field in required_fields {
            assert!(
                event.fields.contains_key(*field),
                "expected field {field} on {message}, got {:?}",
                event.fields
            );
        }
    }

    fn assert_event_level(events: &[CapturedEvent], message: &str, expected: &str) {
        let event = find_event(events, message);
        assert_eq!(
            event.level, expected,
            "expected level {expected} on {message}, got {}",
            event.level
        );
    }

    fn find_event<'a>(events: &'a [CapturedEvent], message: &str) -> &'a CapturedEvent {
        events
            .iter()
            .find(|event| event.fields.get("message").map(String::as_str) == Some(message))
            .unwrap_or_else(|| panic!("missing event {message}, got {events:?}"))
    }

    fn assert_event_absent(events: &[CapturedEvent], message: &str) {
        assert!(
            events
                .iter()
                .all(|event| event.fields.get("message").map(String::as_str) != Some(message)),
            "unexpected event {message}, got {events:?}"
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedEvent {
        level: String,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct RecordingSubscriber {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
        next_id: Arc<AtomicU64>,
    }

    impl Subscriber for RecordingSubscriber {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= &Level::TRACE
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = BTreeMap::new();
            event.record(&mut EventVisitor {
                fields: &mut fields,
            });
            self.events
                .lock()
                .expect("events mutex should lock")
                .push(CapturedEvent {
                    level: event.metadata().level().to_string(),
                    fields,
                });
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    struct EventVisitor<'a> {
        fields: &'a mut BTreeMap<String, String>,
    }

    impl Visit for EventVisitor<'_> {
        fn record_debug(&mut self, field: &TracingField, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_i64(&mut self, field: &TracingField, value: i64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &TracingField, value: u64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }

        fn record_bool(&mut self, field: &TracingField, value: bool) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }

        fn record_str(&mut self, field: &TracingField, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    struct FailingSerialize;

    impl serde::Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("serialize boom"))
        }
    }
}
