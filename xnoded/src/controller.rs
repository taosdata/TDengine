pub mod agents;
mod alloc_jobs;
mod event;
mod heartbeat;
mod reconnect;
mod sql_types;
pub mod tasks;
pub mod updaters;
pub mod xnodes;

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use axum::http::{self, StatusCode};
use snafu::{OptionExt, ResultExt};
use taos::Dsn;
use taosx_utils::sql::sql_value_escaped_fmt;
use taosx_utils::taos_conn::{self, TaosConn};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

use ha_core::{
    activity::TaskStatus,
    jwt,
    types::{
        CheckValidParam, HaTask, ListTaskJobStatesResult, StartTaskJobParam, StopTaskJobParam,
        XnodedId,
    },
};
use tracing::{Instrument, instrument};

use crate::{
    Args,
    api::{agent::AgentStatusResult, task::TaskConfigParam},
    controller::{
        agents::Agents,
        alloc_jobs::{AllocatedJobs, alloc_jobs},
        event::event_loop,
        heartbeat::heartbeat_loop,
        reconnect::reconnect_loop,
        tasks::{TaskJobInfo, Tasks, is_oneshot},
        updaters::{remove_cached_agent_state, update_agent_status, update_task_status},
        xnodes::{XNodeStatus, XNodes},
    },
};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Invalid URI: {url}"))]
    InvalidUri {
        url: String,
        source: http::uri::InvalidUri,
    },
    #[snafu(display("Failed to build db connection"))]
    BuildTaosConn { source: taos_conn::Error },
    #[snafu(display("No available xnode"))]
    NoAvailableXnode,
    #[snafu(display("Failed to plan task on xnode {xnode_id}"))]
    PlanTask {
        xnode_id: i32,
        source: ha_rpc_client::error::Error,
    },
    #[snafu(display("Failed to show jobs"))]
    ShowJobsSql { source: taos_conn::Error },
    #[snafu(display("Failed to drop task {task_id} jobs"))]
    DropJobsSql {
        task_id: i64,
        source: taos_conn::Error,
    },
    #[snafu(display("Failed to create job"))]
    CreateJobSql { source: taos_conn::Error },
    #[snafu(display("Failed to split job"))]
    SplitJob { source: alloc_jobs::Error },
    #[snafu(display("Failed to serialize job config"))]
    SerializeJobConfig { source: serde_json::Error },
    #[snafu(display("Xnode {xnode_id} not available"))]
    XnodeNotAvailable { xnode_id: i32 },
    #[snafu(display("Failed to deserialize job ({task_id},{job_id}) config"))]
    DeserializeJobConfig {
        task_id: i64,
        job_id: i64,
        source: serde_json::Error,
    },
    #[snafu(display("Failed to deserialize task {task_id} parser"))]
    DeserializeTaskParser {
        task_id: i64,
        source: serde_json::Error,
    },
    #[snafu(display("Failed to deserialize task {task_id} labels"))]
    DeserializeTaskLabels {
        task_id: i64,
        source: serde_json::Error,
    },
    #[snafu(display("Failed to start task {task_id} job {job_id} on xnode {xnode_id}"))]
    StartTaskJob {
        xnode_id: i32,
        task_id: i64,
        job_id: i64,
        source: ha_rpc_client::error::Error,
    },
    #[snafu(display("Failed to stop task {task_id} job {job_id} on xnode {xnode_id}"))]
    StopTaskJob {
        xnode_id: i32,
        task_id: i64,
        job_id: i64,
        source: ha_rpc_client::error::Error,
    },
    #[snafu(display("Failed to alter task {task_id} job {job_id} on xnode id {xnode_id}"))]
    AlterJobXnodeId {
        task_id: i64,
        job_id: i64,
        xnode_id: i32,
        source: taos_conn::Error,
    },
    #[snafu(display("Failed to alter task {task_id} job {job_id} status {status}"))]
    AlterJobStatus {
        task_id: i64,
        job_id: i64,
        status: TaskStatus,
        source: Box<dyn snafu::Error + Send + Sync + 'static>,
    },
    #[snafu(display("Failed to alter task {task_id} xnode id {xnode_id}"))]
    AlterTaskXnodeId {
        task_id: i64,
        xnode_id: i32,
        source: taos_conn::Error,
    },
    #[snafu(display("Failed to alter task {task_id} status {status}"))]
    AlterTaskStatus {
        task_id: i64,
        status: TaskStatus,
        source: taos_conn::Error,
    },
    #[snafu(display("Failed to show xnodes"))]
    ShowXnodesSql { source: taos_conn::Error },
    #[snafu(display("Failed to show tasks"))]
    ShowTasksSql { source: taos_conn::Error },
    #[snafu(display("Failed to list task job states on xnode id {xnode_id}"))]
    ListTaskJobStates {
        xnode_id: i32,
        source: ha_rpc_client::error::Error,
    },
    #[snafu(display("Failed to drain task job on xnode id {xnode_id}"))]
    DrainTaskJob {
        xnode_id: i32,
        source: ha_rpc_client::error::Error,
    },
    #[snafu(display("Task {task_id} job {job_id} not exists"))]
    TaskJobNotExists { task_id: i64, job_id: i64 },
    #[snafu(display("Failed to check valid, from {from} to {to} on xnode id {xnode_id}"))]
    CheckValid {
        xnode_id: i32,
        from: String,
        to: String,
        source: ha_rpc_client::error::Error,
    },
    #[snafu(display("Failed to build batch iterator"))]
    BuildBatchIter { source: anyhow::Error },
    #[snafu(display("Invalid task {task_id} job {job_id} dsn"))]
    InvalidTaskDsn {
        task_id: i64,
        job_id: i64,
        source: taos::DsnError,
    },
    #[snafu(display("Xnode {id} already exists"))]
    XnodeAlreadyExists { id: i32 },
    #[snafu(display("Invalid task parser"))]
    InvalidTaskParser { source: serde_json::Error },
    #[snafu(display("Invalid dsn {dsn}"))]
    InvalidDsn { dsn: String, source: taos::DsnError },
    #[snafu(display("Failed to create task activity stable"))]
    CreateTaskActivityTable { source: taos_conn::Error },
    #[snafu(display("Failed to create metrics stable"))]
    CreateMetricsTable { source: taos_conn::Error },
    #[snafu(display("Failed to create database `log`"))]
    CreateLogDatabase { source: taos_conn::Error },
    #[snafu(display("Failed to show agents"))]
    ShowAgents { source: taos_conn::Error },
    #[snafu(display("Failed to create agent activity stable"))]
    CreateAgentActivityTable { source: taos_conn::Error },
    #[snafu(display("Agent not found"))]
    AgentNotFound { id: i64 },
    #[snafu(display("Failed to decode agent jwt"))]
    AgentJwtDecode { source: jsonwebtoken::errors::Error },
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidUri { .. } | Error::XnodeAlreadyExists { .. } => StatusCode::BAD_REQUEST,
            Error::NoAvailableXnode => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct Controller {
    cluster_id: String,
    leader_ep: String,
    taos_dsn: Dsn,
    taos_conn: TaosConn,
    cancel: CancellationToken,

    xnodes: XNodes,
    tasks: Tasks,
    agents: Agents,

    rebalance_tx: flume::Sender<i32>,
}

impl Controller {
    #[instrument(skip_all)]
    pub async fn create(
        args: &Args,
        dsn: &str,
        rebalance_tx: flume::Sender<i32>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let dsn = Dsn::from_str(dsn).context(InvalidDsnSnafu { dsn })?;
        let this = Self {
            cluster_id: args.cluster_id.clone(),
            leader_ep: args.leader_ep.clone(),
            taos_dsn: dsn.clone(),
            taos_conn: TaosConn::create(dsn, 5).await.context(BuildTaosConnSnafu)?,
            cancel: cancel.clone(),
            xnodes: XNodes::new(),
            tasks: Tasks::new(),
            agents: Agents::new(),
            rebalance_tx,
        };
        cancel.run_until_cancelled(this.init()).await.transpose()?;
        Ok(this)
    }

    #[instrument(skip_all)]
    async fn init(&self) -> Result<()> {
        self.init_xnodes().await?;
        self.init_task_jobs().await?;
        self.init_agents().await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn init_xnodes(&self) -> Result<()> {
        let xnodes = self
            .taos_conn
            .query::<sql_types::XnodeId>("SHOW XNODES")
            .await
            .context(ShowXnodesSqlSnafu)?;
        for xnode in xnodes {
            let xnode_id = xnode.id;
            if let Err(e) = self.create_xnode(xnode_id, &xnode.url).await {
                tracing::error!(id = xnode_id, url = xnode.url, error = %e, "failed to create xnode");
                continue;
            };
            tracing::info!(id = xnode_id, url = xnode.url, "created xnode");
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn init_task_jobs(&self) -> Result<()> {
        let tasks = self
            .taos_conn
            .query::<sql_types::TaskRecord>("SHOW XNODE TASKS")
            .await
            .context(ShowTasksSqlSnafu)?;

        let jobs = self
            .taos_conn
            .query::<sql_types::JobRecord>("SHOW XNODE JOBS")
            .await
            .context(ShowJobsSqlSnafu)?;
        // taosd 原子任务的状态
        let mut db_status_configs = HashMap::with_capacity(tasks.len() + jobs.len());
        // taosd 任务状态
        let db_tasks_status = tasks
            .iter()
            .map(|v| (v.id, v.status))
            .collect::<HashMap<_, _>>();
        for task in tasks {
            let task_id = task.id;
            let Some(xnode_id) = task.xnode_id else {
                continue;
            };
            let parser = task
                .parser
                .as_ref()
                .map(|v| serde_json::from_str(v))
                .transpose()
                .context(DeserializeTaskParserSnafu { task_id })?;
            let labels = task
                .labels
                .as_ref()
                .map(|v| serde_json::from_str(v))
                .transpose()
                .context(DeserializeTaskLabelsSnafu { task_id })?;
            let config = HaTask {
                from: task.from,
                to: task.to,
                parser,
                via: task.via,
                labels,
            };
            db_status_configs.insert((task_id, -1), (xnode_id, task.status, config.clone()));
        }
        for job in jobs {
            let (task_id, job_id) = (job.task_id, job.id);
            let config: HaTask = serde_json::from_str(&job.config)
                .context(DeserializeJobConfigSnafu { task_id, job_id })?;
            db_status_configs.insert((task_id, job_id), (job.xnode_id, job.status, config));
        }
        let xnodes = self.xnodes.availables();
        for xnode_id in xnodes {
            let Some(client) = self.xnodes.get_client(xnode_id) else {
                tracing::error!("xnode {xnode_id} not available, skip list task states");
                continue;
            };

            // taosx 已有的任务状态，同步到 db
            let states = client
                .list_task_job_states()
                .await
                .context(ListTaskJobStatesSnafu { xnode_id })?;
            let x_tasks = states
                .iter()
                .map(|state| (state.task_id, state.job_id))
                .collect::<HashSet<_>>();
            for state in states {
                let (task_id, job_id) = (state.task_id, state.job_id);
                let status = &state.state;
                let Some((_, db_task_status, config)) =
                    db_status_configs.get_mut(&(task_id, job_id))
                else {
                    // db 不存在的任务，需要停掉
                    let param = StopTaskJobParam { task_id, job_id };
                    if let Err(e) = client.stop_task_job(&param).await {
                        tracing::error!("failed to stop task job: {:#}", anyhow::Error::new(e));
                    }
                    continue;
                };
                if db_task_status.as_ref().is_none_or(|v| v != status) {
                    let mut sql = if job_id < 0 {
                        format!(
                            "ALTER XNODE TASK {task_id} WITH XNODE_ID {xnode_id} STATUS '{status}'"
                        )
                    } else {
                        format!(
                            "ALTER XNODE JOB {job_id} WITH XNODE_ID {xnode_id} STATUS '{status}'"
                        )
                    };
                    if status != &TaskStatus::Failed {
                        sql = format!("{sql} REASON ''");
                    }
                    let res = self.taos_conn.exec(&sql).await;
                    if job_id < 0 {
                        res.context(AlterTaskStatusSnafu { task_id, status })?;
                    } else {
                        res.boxed().context(AlterJobStatusSnafu {
                            task_id,
                            job_id,
                            status,
                        })?;
                    }
                    tracing::info!(task_id, job_id, %status, "updated task job status");
                    *db_task_status = Some(*status);
                }
                self.tasks
                    .add(task_id, job_id, xnode_id, config.clone(), Some(state.state))
                    .context(InvalidTaskDsnSnafu { task_id, job_id })?;
                tracing::info!(task_id, job_id, xnode_id, "add task job");
            }

            // db 的任务，tasox 没有的，分派任务
            for ((task_id, job_id), (task_xnode_id, db_status, config)) in &db_status_configs {
                let (task_id, job_id, task_xnode_id) = (*task_id, *job_id, *task_xnode_id);
                if x_tasks.contains(&(task_id, job_id)) {
                    continue;
                }
                if xnode_id != task_xnode_id {
                    continue;
                }
                // 总任务的状态是 运行中，则子任务需要放到内存中，用户可以手动调度
                if db_tasks_status
                    .get(&task_id)
                    .is_some_and(|v| v.is_some_and(|v| v.is_running()))
                {
                    self.tasks
                        .add(task_id, job_id, xnode_id, config.clone(), None)
                        .context(InvalidTaskDsnSnafu { task_id, job_id })?;
                }
                if db_status
                    .as_ref()
                    .is_none_or(|v| !matches!(v, TaskStatus::Queued | TaskStatus::Running))
                {
                    continue;
                }

                if let Err(e) = self
                    .start_task_job(xnode_id, task_id, job_id, config.clone())
                    .await
                {
                    tracing::error!(
                        task_id,
                        job_id,
                        xnode_id,
                        "failed to start job: {:#}",
                        anyhow::Error::new(e)
                    );
                }
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn init_agents(&self) -> Result<()> {
        let agents = self
            .taos_conn
            .query::<sql_types::AgentRecord>("SHOW XNODE AGENTS")
            .await
            .context(ShowAgentsSnafu)?;
        for agent in &agents {
            self.agents.add(agent.id, &agent.token);
        }

        let tokens = agents.into_iter().map(|v| v.token).collect::<Vec<_>>();
        let xnodes = self.xnodes.availables();
        for xnode_id in xnodes {
            let Some(client) = self.xnodes.get_client(xnode_id) else {
                continue;
            };
            tracing::info!(xnode_id, "add agents on xnode");
            if let Err(e) = client.add_agents(&tokens).await {
                tracing::error!(
                    xnode_id,
                    "failed to init agents on xnode: {:#}",
                    anyhow::Error::new(e)
                );
            }
        }
        Ok(())
    }

    pub fn cancel(&self) -> &CancellationToken {
        &self.cancel
    }

    pub fn tasks(&self) -> Tasks {
        self.tasks.clone()
    }

    pub fn xnodes(&self) -> XNodes {
        self.xnodes.clone()
    }

    #[instrument(skip_all)]
    pub async fn create_xnode(&self, id: i32, url: &str) -> Result<()> {
        if self.xnodes.is_online(id) {
            return XnodeAlreadyExistsSnafu { id }.fail();
        }
        if self.xnodes.is_cancelled(id) {
            wait_xnode_complete(self.xnodes(), id).await;
        }
        let xnoded_id = XnodedId {
            cluster_id: self.cluster_id.clone(),
            leader_ep: self.leader_ep.clone(),
        };
        let addr = if url.starts_with("http") {
            url.to_string()
        } else {
            format!("http://{url}")
        };
        let endpoint: Endpoint =
            Channel::from_shared(addr.clone()).context(InvalidUriSnafu { url })?;
        match endpoint.connect().await {
            Ok(channel) => {
                let (event_tx, event_rx) = flume::bounded(1000);
                let cancel = self.cancel.child_token();
                match ha_rpc_client::create_client(channel, &xnoded_id, event_tx, cancel).await {
                    Ok(client) => {
                        self.xnodes.add_online(id, client, event_rx);
                    }
                    Err(e) => {
                        tracing::error!(
                            addr,
                            "create rpc client error: {:#}",
                            anyhow::Error::new(e)
                        );
                        self.xnodes.add_offline(id);
                    }
                }
            }
            Err(e) => {
                tracing::error!(addr, "build rpc channel error: {e:#}");
                self.xnodes.add_offline(id);
            }
        }
        let (reconnect_tx, reconnect_rx) = flume::bounded(1);
        let cancel = self.cancel.child_token();
        let mut handle = JoinSet::new();
        handle.spawn(
            heartbeat_loop(
                id,
                xnoded_id.clone(),
                self.xnodes.clone(),
                reconnect_tx.clone(),
                self.rebalance_tx.clone(),
                cancel.clone(),
            )
            .in_current_span(),
        );
        handle.spawn(
            event_loop(
                id,
                self.taos_dsn.clone(),
                self.xnodes.clone(),
                self.agents.clone(),
                self.tasks.clone(),
                reconnect_tx,
                self.rebalance_tx.clone(),
                cancel.clone(),
            )
            .in_current_span(),
        );
        handle.spawn(
            reconnect_loop(
                id,
                xnoded_id,
                addr,
                endpoint,
                self.xnodes.clone(),
                self.agents.clone(),
                self.tasks.clone(),
                self.taos_dsn.clone(),
                reconnect_rx,
                cancel.clone(),
            )
            .in_current_span(),
        );
        self.xnodes.set_handle(id, handle, cancel);
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn delete_xnode(&self, id: i32, force: bool) -> Result<()> {
        match self.drain_xnode(id).await {
            Ok(_) => {
                self.tasks.del_xnode_jobs(id);
            }
            Err(e) if force => {
                self.tasks.del_xnode_jobs(id);
                tracing::error!("delete xnode error: {}", anyhow::Error::new(e));
            }
            res => {
                return res;
            }
        }

        wait_xnode_complete(self.xnodes(), id).await;

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn drain_xnode(&self, id: i32) -> Result<()> {
        let Some(client) = self.xnodes.get_client(id) else {
            tracing::error!(
                xnode_id = id,
                "xnoded not found, may be offline or not created"
            );
            self.tasks.del_xnode_jobs(id);
            return Ok(());
        };

        self.xnodes.set_drain(id);
        let _guard = taosx_utils::defer::defer(|| {
            self.xnodes.unset_drain(id);
        });
        client
            .drain_task_job()
            .await
            .context(DrainTaskJobSnafu { xnode_id: id })?;

        if self.xnodes.len() == 0 {
            return Ok(());
        }
        let tasks = self.tasks.del_xnode_jobs(id);

        for ((task_id, job_id), config) in tasks {
            let via = config.config.via;
            let xnode_id = self.xnodes.best_xnode(via).context(NoAvailableXnodeSnafu)?;
            self.start_task_job(xnode_id, task_id, job_id, config.config)
                .await?;
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn start_task_job(
        &self,
        xnode_id: i32,
        task_id: i64,
        job_id: i64,
        config: HaTask,
    ) -> Result<()> {
        start_task_job(
            xnode_id,
            task_id,
            job_id,
            &self.xnodes(),
            &self.tasks,
            &self.taos_conn,
            config,
        )
        .await
    }

    #[instrument(skip_all)]
    pub fn xnode_status(&self, xnode_id: i32) -> Result<XNodeStatus> {
        if self.xnodes.is_cancelled(xnode_id) {
            let xnodes = self.xnodes();
            tokio::spawn(wait_xnode_complete(xnodes, xnode_id));
        }
        self.xnodes
            .status(xnode_id)
            .context(XnodeNotAvailableSnafu { xnode_id })
    }

    #[instrument(skip_all)]
    pub async fn task_status(&self, tid: i64) -> Result<ListTaskJobStatesResult> {
        Ok(update_task_status(&self.taos_conn, &self.xnodes, &self.tasks, Some(tid)).await)
    }

    #[instrument(skip_all)]
    pub async fn plan_start_task(&self, task_id: i64, task: &TaskConfigParam) -> Result<()> {
        let mut parser = task
            .parser
            .as_ref()
            .map(|v| serde_json::from_str::<serde_json::Value>(v))
            .transpose()
            .context(InvalidTaskParserSnafu)?;
        if let Some(inner_parser) = parser
            .as_ref()
            .and_then(|v| v.as_object())
            .and_then(|v| v.get("parser"))
        {
            parser = Some(inner_parser.clone());
        }
        let labels = task
            .labels
            .as_ref()
            .map(|v| serde_json::from_str(v))
            .transpose()
            .context(DeserializeTaskLabelsSnafu { task_id })?;
        let config = HaTask {
            from: task.from.clone(),
            to: task.to.clone(),
            parser: parser.clone(),
            via: task.via,
            labels: labels.clone(),
        };
        match task.xnode_id {
            Some(xnode_id) => {
                // 停掉旧任务
                let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
                let db_task = self
                    .taos_conn
                    .query_one::<sql_types::TaskRecord>(&sql)
                    .await
                    .context(ShowJobsSqlSnafu)?;
                if let Some(task) = db_task
                    && let Some(xnode_id) = task.xnode_id
                {
                    self.stop_job(task_id, -1, xnode_id).await?;
                }
                // 启动新任务
                let config = task.try_into().context(InvalidTaskParserSnafu)?;
                start_task(
                    xnode_id,
                    task_id,
                    &self.xnodes,
                    &self.tasks,
                    &self.taos_conn,
                    config,
                )
                .await?;
            }
            None => {
                // delete all jobs first
                let sql = format!("SHOW XNODE JOBS WHERE TASK_ID = {task_id}");
                let job_ids = self
                    .taos_conn
                    .query::<sql_types::JobId>(&sql)
                    .await
                    .context(ShowJobsSqlSnafu)?;
                for job_id in job_ids {
                    let jid = job_id.id;
                    let tid = job_id.task_id;
                    let xid = job_id.xnode_id;
                    if task_id != tid {
                        continue;
                    }
                    self.taos_conn
                        .exec(&format!("DROP XNODE JOB {jid}"))
                        .await
                        .context(DropJobsSqlSnafu { task_id })?;
                    // stop task job
                    let Some(client) = self.xnodes.get_client(xid) else {
                        continue;
                    };
                    let param = StopTaskJobParam {
                        task_id: tid,
                        job_id: jid,
                    };
                    if let Err(e) = client.stop_task_job(&param).await {
                        tracing::error!(
                            task_id = tid,
                            job_id = jid,
                            xnode_id = xid,
                            "Failed to stop task job: {:#}",
                            anyhow::Error::new(e)
                        );
                    }
                    self.tasks.del_task_job(tid, jid);
                }
                let Some((plan_xnode_id, client)) = self.xnodes.get_one_client() else {
                    return NoAvailableXnodeSnafu.fail();
                };
                let split_config = client.plan_task(&config).await.context(PlanTaskSnafu {
                    xnode_id: plan_xnode_id,
                })?;
                let jobs =
                    alloc_jobs(split_config, &self.xnodes, task.via).context(SplitJobSnafu)?;
                tracing::debug!(?jobs, "alloc jobs result");
                match jobs {
                    AllocatedJobs::Task(xnode_id, mut task) => {
                        task.labels = config.labels.clone();
                        self.start_task(task_id, xnode_id, task).await?;
                    }
                    AllocatedJobs::Jobs(jobs) => {
                        let mut created_jobs = Vec::with_capacity(jobs.len());
                        for (xnode_id, config) in jobs {
                            let job_config =
                                serde_json::to_string(&config).context(SerializeJobConfigSnafu)?;
                            let mut sql = format!(
                                "CREATE XNODE JOB ON {task_id} WITH CONFIG {} XNODE_ID {xnode_id}",
                                sql_value_escaped_fmt(&job_config)
                            );
                            if let Some(labels) = task.labels.as_ref() {
                                sql.push_str(&format!(" LABELS {}", sql_value_escaped_fmt(labels)));
                            }
                            if let Some(via) = config.via {
                                sql.push_str(&format!(" VIA {via}"));
                            }
                            self.taos_conn.exec(&sql).await.context(CreateJobSqlSnafu)?;
                            created_jobs.push((xnode_id, config));
                        }
                        tracing::debug!(task_id, "created db jobs");
                        // get job id
                        let sql = format!("SHOW XNODE JOBS WHERE TASK_ID = {task_id}");
                        let db_jobs = self
                            .taos_conn
                            .query::<sql_types::JobRecord>(&sql)
                            .await
                            .context(ShowJobsSqlSnafu)?;
                        for job in db_jobs {
                            if job.task_id != task_id {
                                continue;
                            }
                            let job_id = job.id;
                            let xnode_id = job.xnode_id;

                            let job_config = job
                                .try_into()
                                .context(DeserializeJobConfigSnafu { task_id, job_id })?;
                            if let Err(e) =
                                self.start_job(task_id, job_id, xnode_id, job_config).await
                            {
                                tracing::error!(
                                    task_id,
                                    job_id,
                                    xnode_id,
                                    "start job error: {:#}",
                                    anyhow::Error::new(e)
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn start_task(&self, task_id: i64, xnode_id: i32, task: HaTask) -> Result<()> {
        start_task(
            xnode_id,
            task_id,
            &self.xnodes,
            &self.tasks,
            &self.taos_conn,
            task,
        )
        .await
    }

    #[instrument(skip_all)]
    pub async fn stop_by_task(&self, task_id: i64, del_task: bool) -> Result<()> {
        let jobs = if del_task {
            self.tasks.del_task(task_id)
        } else {
            self.tasks.task_jobs(task_id)
        };
        for ((task_id, job_id), task) in jobs {
            let xnode_id = task.xnode_id;
            tracing::info!(task_id, job_id, xnode_id, "stop task");
            if let Err(e) = self.stop_job(task_id, job_id, xnode_id).await {
                tracing::error!(
                    task_id,
                    job_id,
                    xnode_id,
                    "stop task failed: {:#}",
                    anyhow::Error::new(e)
                );
            }
            self.tasks.set_manually_stopped(task_id, job_id);
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn drop_task(&self, task_id: i64) -> Result<()> {
        let sql = format!("DROP XNODE JOB WHERE TASK_ID = {task_id}");
        self.taos_conn
            .exec(&sql)
            .await
            .context(DropJobsSqlSnafu { task_id })?;
        self.stop_by_task(task_id, true).await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn start_job(
        &self,
        task_id: i64,
        job_id: i64,
        xnode_id: i32,
        job: HaTask,
    ) -> Result<()> {
        start_job(
            xnode_id,
            task_id,
            job_id,
            &self.xnodes,
            &self.tasks,
            &self.taos_conn,
            job,
        )
        .await
    }

    #[instrument(skip_all)]
    pub async fn stop_job(&self, task_id: i64, job_id: i64, xnode_id: i32) -> Result<()> {
        let client = self
            .xnodes
            .get_client(xnode_id)
            .context(XnodeNotAvailableSnafu { xnode_id })?;
        let param = StopTaskJobParam { task_id, job_id };
        client
            .stop_task_job(&param)
            .await
            .context(StopTaskJobSnafu {
                xnode_id,
                task_id,
                job_id,
            })?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn check_task(&self, xnode_id: Option<i32>, task: TaskConfigParam) -> Result<()> {
        let (xnode_id, client) = match xnode_id {
            Some(xnode_id) => (
                xnode_id,
                self.xnodes
                    .get_client(xnode_id)
                    .context(XnodeNotAvailableSnafu { xnode_id })?,
            ),
            None => self
                .xnodes
                .get_one_client()
                .context(NoAvailableXnodeSnafu)?,
        };
        tracing::info!(xnode_id, "checking task");
        let (from, to) = (task.from, task.to);
        let param = CheckValidParam {
            from: from.clone(),
            to: to.clone(),
            via: task.via,
        };
        client
            .check_valid(&param)
            .await
            .context(CheckValidSnafu { from, to, xnode_id })?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn rebalance_manually(&self, task_id: i64, job_id: i64, xnode_id: i32) -> Result<()> {
        let config = match self.tasks.job(task_id, job_id) {
            Some(config) => config,
            None => {
                tracing::warn!(
                    task_id,
                    job_id,
                    "rebalance manually but job not found, load from db"
                );
                let sql = format!("SHOW XNODE JOBS WHERE ID = {job_id} AND TASK_ID = {task_id}");
                let Some(job) = self
                    .taos_conn
                    .query_one::<sql_types::JobRecord>(&sql)
                    .await
                    .context(ShowJobsSqlSnafu)?
                else {
                    return TaskJobNotExistsSnafu { task_id, job_id }.fail();
                };
                let config: HaTask = serde_json::from_str(&job.config)
                    .context(DeserializeJobConfigSnafu { task_id, job_id })?;
                TaskJobInfo {
                    xnode_id: job.xnode_id,
                    manually_rebalance: false,
                    manually_stopped: false,
                    oneshot: is_oneshot(&config.from)
                        .context(InvalidTaskDsnSnafu { task_id, job_id })?,
                    status: None,
                    config,
                }
            }
        };
        if config.xnode_id == xnode_id {
            tracing::warn!(
                task_id,
                job_id,
                "rebalance manually to xnode that job already running on"
            );
            return Ok(());
        }
        self.stop_job(task_id, job_id, config.xnode_id).await?;

        self.start_task_job(xnode_id, task_id, job_id, config.config)
            .await?;

        self.tasks.set_manually_rebalance(task_id, job_id);

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn rebalance_auto(&self, task_job_ids: &[(i64, Option<i64>)]) -> Result<()> {
        for (task_id, job_id) in task_job_ids {
            let task_id = *task_id;

            match job_id {
                Some(job_id) => {
                    let job_id = *job_id;
                    let job = self
                        .tasks
                        .job(task_id, job_id)
                        .context(TaskJobNotExistsSnafu { task_id, job_id })?;
                    let best_xnode_id = self
                        .xnodes
                        .best_xnode(job.config.via)
                        .context(NoAvailableXnodeSnafu)?;
                    if job.xnode_id == best_xnode_id || job.manually_rebalance {
                        continue;
                    }
                    self.rebalance_manually(task_id, job_id, best_xnode_id)
                        .await?;
                }
                None => {
                    let sql = format!("SHOW XNODE TASKS WHERE ID = {task_id}");
                    let Some(task) = self
                        .taos_conn
                        .query_one::<sql_types::TaskRecord>(&sql)
                        .await
                        .context(ShowTasksSqlSnafu)?
                    else {
                        continue;
                    };
                    let param = TaskConfigParam {
                        xnode_id: None,
                        from: task.from,
                        to: task.to,
                        parser: task.parser,
                        via: task.via,
                        labels: task.labels,
                    };
                    self.plan_start_task(task_id, &param).await?;
                }
            }
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn add_agent(&self, token: &str) -> Result<()> {
        let claims = jwt::agent::jwt_decode(token).context(AgentJwtDecodeSnafu)?;
        self.agents.add(claims.sub, token);

        let xnodes = self.xnodes.availables();
        let param = vec![token.into()];

        for xnode_id in xnodes {
            let Some(client) = self.xnodes.get_client(xnode_id) else {
                continue;
            };
            tracing::info!("adding agent to xnode {}", xnode_id);
            if let Err(e) = client.add_agents(&param).await {
                tracing::error!(
                    xnode_id,
                    "Failed to add agent to xnode: {:#}",
                    anyhow::Error::new(e)
                );
                continue;
            };
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn del_agent(&self, id: i64) -> Result<()> {
        self.agents.del(id);
        remove_cached_agent_state(id);

        let xnodes = self.xnodes.availables();
        for xnode_id in xnodes {
            self.xnodes.del_agent(xnode_id, id);
            let Some(client) = self.xnodes.get_client(xnode_id) else {
                continue;
            };
            tracing::info!("deleting agent from xnode {}", xnode_id);
            if let Err(e) = client.del_agents(&[id]).await {
                tracing::error!(
                    xnode_id,
                    "Failed to delete agent from xnode: {:#}",
                    anyhow::Error::new(e)
                );
            }
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn agent_status(&self) -> Result<HashMap<i64, Vec<AgentStatusResult>>> {
        let xnodes = self.xnodes.availables();
        let mut res: HashMap<i64, Vec<AgentStatusResult>> = HashMap::new();
        for xnode_id in xnodes {
            let Some(client) = self.xnodes.get_client(xnode_id) else {
                continue;
            };
            let agents = match client.list_agents().await {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!(
                        xnode_id,
                        "Failed to list agents from xnode: {:#}",
                        anyhow::Error::new(e)
                    );
                    continue;
                }
            };
            self.xnodes.clear_xnode_agents(xnode_id);
            for agent in agents {
                self.xnodes
                    .set_agent_status(xnode_id, agent.id, agent.status);
                let item = AgentStatusResult {
                    xnode_id,
                    status: agent.status,
                };
                res.entry(agent.id)
                    .and_modify(|v| v.push(item.clone()))
                    .or_insert(vec![item]);
            }
        }
        // update db
        for agent_id in res.keys() {
            if !self.agents.has(*agent_id) {
                continue;
            }
            update_agent_status(&self.taos_conn, &self.xnodes, *agent_id).await;
        }
        Ok(res)
    }
}

#[instrument(skip_all)]
pub async fn start_task_job(
    xnode_id: i32,
    task_id: i64,
    job_id: i64,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &TaosConn,
    config: HaTask,
) -> Result<()> {
    if job_id < 0 {
        start_task(xnode_id, task_id, xnodes, tasks, conn, config).await
    } else {
        start_job(xnode_id, task_id, job_id, xnodes, tasks, conn, config).await
    }
}

#[instrument(skip_all)]
pub async fn start_task(
    xnode_id: i32,
    task_id: i64,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &TaosConn,
    config: HaTask,
) -> Result<()> {
    tasks
        .add(task_id, -1, xnode_id, config.clone(), None)
        .context(InvalidTaskDsnSnafu {
            task_id,
            job_id: -1,
        })?;
    let client = xnodes
        .get_client(xnode_id)
        .context(XnodeNotAvailableSnafu { xnode_id })?;
    let param = StartTaskJobParam {
        task_id,
        job_id: -1,
        from: config.from,
        to: config.to,
        parser: config.parser,
        via: config.via,
        labels: config.labels,
    };
    client
        .start_task_job(&param)
        .await
        .context(StartTaskJobSnafu {
            xnode_id,
            task_id,
            job_id: -1,
        })?;
    conn.exec(&format!(
        "ALTER XNODE TASK {task_id} WITH xnode_id {xnode_id}"
    ))
    .await
    .context(AlterTaskXnodeIdSnafu { task_id, xnode_id })?;
    tracing::info!(task_id, xnode_id, "task started");
    Ok(())
}

#[instrument(skip_all)]
pub async fn start_job(
    xnode_id: i32,
    task_id: i64,
    job_id: i64,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &TaosConn,
    config: HaTask,
) -> Result<()> {
    tasks
        .add(task_id, job_id, xnode_id, config.clone(), None)
        .context(InvalidTaskDsnSnafu { task_id, job_id })?;
    let client = xnodes
        .get_client(xnode_id)
        .context(XnodeNotAvailableSnafu { xnode_id })?;
    let param = StartTaskJobParam {
        task_id,
        job_id,
        from: config.from,
        to: config.to,
        parser: config.parser,
        via: config.via,
        labels: config.labels,
    };
    client
        .start_task_job(&param)
        .await
        .context(StartTaskJobSnafu {
            xnode_id,
            task_id,
            job_id,
        })?;
    conn.exec(&format!(
        "ALTER XNODE JOB {job_id} WITH XNODE_ID {xnode_id}"
    ))
    .await
    .context(AlterJobXnodeIdSnafu {
        task_id,
        job_id,
        xnode_id,
    })?;
    tracing::info!(task_id, job_id, xnode_id, "job started");
    Ok(())
}

#[instrument(skip_all)]
async fn wait_xnode_complete(xnodes: XNodes, id: i32) {
    let Some((mut handle, cancel)) = xnodes.remove(id) else {
        return;
    };
    cancel.cancel();
    while let Some(result) = handle.join_next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("xnode task error: {:#}", anyhow::Error::new(e));
            }
            Err(e) => {
                tracing::error!("xnode task panic: {:#}", anyhow::Error::new(e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_status_code_mapping() {
        let err = Error::XnodeAlreadyExists { id: 1 };
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);

        let err = Error::NoAvailableXnode;
        assert_eq!(err.status_code(), StatusCode::NOT_FOUND);

        let err = Error::AgentNotFound { id: 1 };
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
