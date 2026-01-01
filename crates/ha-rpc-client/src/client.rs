use arrow::array::RecordBatch;
use ha_core::{
    batch::{BatchIter, build_batch},
    consts::*,
    types::*,
    utils::next_req_id,
};
use snafu::{OptionExt, ResultExt};
use tokio::sync::oneshot;

use crate::error::*;

#[derive(Clone)]
pub struct HaRpcClient {
    message_tx: flume::Sender<(u64, RecordBatch, oneshot::Sender<Result<RecordBatch>>)>,
}

impl HaRpcClient {
    pub(crate) fn new(
        message_tx: flume::Sender<(u64, RecordBatch, oneshot::Sender<Result<RecordBatch>>)>,
    ) -> Self {
        Self { message_tx }
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

    pub async fn check_valid(&self, param: &CheckValidParam) -> Result<()> {
        self.send_recv(CHECK_VALID_REQ, param).await
    }

    pub async fn get_samples(&self, from: &str) -> Result<GetSamplesResult> {
        self.send_recv_raw_context(GET_SAMPLES_REQ, from).await
    }

    pub async fn add_agents(&self, param: &AddAgentsParam) -> Result<AddAgentsResult> {
        self.send_recv(ADD_AGENTS_REQ, param).await
    }

    pub async fn del_agents(&self, param: &DelAgentsParam) -> Result<DelAgentsResult> {
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
        let param = serde_json::to_string(&param).context(SerializeReqSnafu)?;
        self.send_recv_raw_context(action, &param).await
    }

    async fn send_recv_raw_context<R>(&self, action: &str, param: &str) -> Result<R>
    where
        R: for<'a> serde::Deserialize<'a>,
    {
        let req_id = next_req_id();
        let batch = build_batch(action, param, req_id).context(BuildReqBatchSnafu)?;
        let (tx, rx) = oneshot::channel();
        if self
            .message_tx
            .send_async((req_id, batch, tx))
            .await
            .is_err()
        {
            return EventLoopDroppedSnafu.fail();
        }
        let Ok(batch) = rx.await else {
            return AckWaiterDroppedUnexpectedlySnafu.fail();
        };
        let batch = batch?;
        let mut iter = BatchIter::new(&batch).context(BuildBatchIterSnafu)?;
        let record = iter.next().context(ResponseNoContextSnafu)?;
        match serde_json::from_str::<Response<R>>(record.context).context(DeserializeRespSnafu)? {
            Response::Data(data) => Ok(data),
            Response::Fail(error) => ResponseFailSnafu { error }.fail(),
        }
    }
}
