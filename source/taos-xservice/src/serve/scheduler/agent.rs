use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};

use anyhow::{Context, bail};
use ha_core::types::{HaTask, SplitJobResult};
use multi_index_map::MultiIndexMap;
use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::transform::sample::{DsSampleIn, DsSamples};
use taosx_core::{DataSet, PutFileReq};
use tokio::sync::{RwLock, oneshot};
use tokio::{runtime::Handle, sync::broadcast::error::RecvError};
use tracing::Instrument;
use utoipa::openapi::path;
use uuid::Uuid;

use crate::serve::controller::AgentAction;
use crate::serve::scheduler::NotifySenderExt;
use ha_core::activity::Activity;

use super::NotifySender;

pub type TaskId = i64;
pub type AgentId = i64;
pub type AgentActionsSender = tokio::sync::broadcast::Sender<(AgentId, AgentAction)>;
pub type AgentActionsReceiver = tokio::sync::broadcast::Receiver<(AgentId, AgentAction)>;
pub type AgentNotifySender = tokio::sync::broadcast::Sender<AgentNotify>;
pub type AgentNotifyReceiver = tokio::sync::broadcast::Receiver<AgentNotify>;
type SpawnedFuture =
    Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'static>>;
pub type AgentSpawnSender = flume::Sender<(
    SpawnedFuture,
    tokio::sync::oneshot::Sender<anyhow::Result<()>>,
)>;
pub type AgentSpawnReceiver = flume::Receiver<(
    SpawnedFuture,
    tokio::sync::oneshot::Sender<anyhow::Result<()>>,
)>;

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    #[default]
    Idle,
    Wait,
    Connected,
    Disconnected,
}

impl AgentState {
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }
}

impl From<AgentState> for ha_core::activity::AgentStatus {
    fn from(value: AgentState) -> Self {
        match value {
            AgentState::Idle => Self::Idle,
            AgentState::Wait => Self::Waiting,
            AgentState::Connected => Self::Connected,
            AgentState::Disconnected => Self::Disconnected,
        }
    }
}

#[derive(MultiIndexMap, Debug)]
pub struct AgentTask {
    #[multi_index(hashed_non_unique)]
    pub agent_id: i64,
    #[multi_index(ordered_unique)]
    pub task_job_id: (i64, i64),
    pub agent_state: Arc<RwLock<AgentState>>,
    pub sender: flume::Sender<Activity>,
    pub stop_sender: Arc<tokio::sync::oneshot::Sender<anyhow::Result<()>>>,
}

impl Debug for MultiIndexAgentTaskMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_list();
        for task in self.iter_by_task_job_id() {
            debug.entry(task);
        }
        debug.finish()
    }
}

#[derive(Debug, Clone)]
pub enum AgentNotify {
    /// Agent connected to RPC server.
    AgentConnected(AgentId),
    /// Agent has been unexpectedly disconnected from RPC server.
    AgentDisconnected(AgentId),

    /// Put stream writer error.
    ///
    /// This error is sent by agent when it encounters an error while writing data to TDengine.
    WriterError(AgentId, i64, i64, String),
    /// Agent task activity.
    TaskActivity(AgentId, Activity),
    /// Agent activity.
    AgentActivity(AgentId, Activity),
}

#[derive(Debug, Clone)]
pub struct AgentWorker {
    agent_states: Arc<RwLock<HashMap<AgentId, AgentState>>>,
    agent_tasks: Arc<RwLock<MultiIndexAgentTaskMap>>,
    agent_action_sender: AgentActionsSender,
    weak_spawn_receiver: Weak<AgentSpawnReceiver>,
    task_set: Arc<tokio::task::JoinSet<()>>,
}

macro_rules! check_agent_exists {
    ($self:expr, $agent_id:expr) => {{
        let agent_states = $self.agent_states.read().await;
        if !agent_states.contains_key(&$agent_id) {
            return Err(anyhow::anyhow!("Agent not found: {}", $agent_id));
        }
    }};
}

impl AgentWorker {
    pub async fn new(
        agent_action_sender: AgentActionsSender,
        agent_notify_receiver: AgentNotifyReceiver,
        scheduler_notify_sender: NotifySender,
        agent_spawn_receiver: AgentSpawnReceiver,
    ) -> Self {
        let agent_tasks_sender = Arc::new(RwLock::new(MultiIndexAgentTaskMap::default()));

        let agent_spawn_receiver = Arc::new(agent_spawn_receiver);
        let weak_spawn_receiver = Arc::downgrade(&agent_spawn_receiver);

        let mut task_set = tokio::task::JoinSet::new();
        task_set.spawn(async move {
            tokio::pin!(agent_spawn_receiver);
            loop {
                match agent_spawn_receiver.recv_async().await {
                    Ok((future, notifier)) => {
                        tokio::spawn(async move {
                            let res = future.await;
                            if let Err(err) = notifier.send(res) {
                                tracing::warn!("Error sending spawn result: {:?}", err);
                            }
                        });
                    }
                    Err(_) => {
                        tracing::info!("Agent spawned task receiver closed");
                        break;
                    }
                }
            }
        });

        let agent_states: Arc<RwLock<HashMap<AgentId, AgentState>>> = Default::default();

        task_set.spawn({
            let agent_states = agent_states.clone();
            let agent_tasks_sender = agent_tasks_sender.clone();
            let scheduler_notify_sender = scheduler_notify_sender.clone();
            let agent_action_sender = agent_action_sender.clone();
            async move {
                tokio::pin!(agent_notify_receiver);
                loop {
                    match agent_notify_receiver.recv().await {
                        Ok(item) => {
                            tracing::debug!("Received agent notify: {:?}", item);
                            match item {
                                AgentNotify::AgentConnected(agent_id) => {
                                    tracing::info!(
                                        agent_id,
                                        "Agent worker received `connected` notify"
                                    );
                                    {
                                        let mut states = agent_states.write().await;
                                        if let std::collections::hash_map::Entry::Vacant(e) =
                                            states.entry(agent_id)
                                        {
                                            e.insert(AgentState::Connected);
                                        } else {
                                            states
                                                .get_mut(&agent_id)
                                                .unwrap()
                                                .clone_from(&AgentState::Connected);
                                        }
                                    }
                                    let mut agent_tasks = agent_tasks_sender.write().await;
                                    for task in agent_tasks.get_by_agent_id(&agent_id) {
                                        let (task_id, job_id) = task.task_job_id;
                                        *task.agent_state.write().await = AgentState::Connected;
                                        if task.sender
                                            .send_async(Activity::agent_resumed(
                                                task_id, job_id, agent_id,
                                            ))
                                            .await
                                            .is_err()
                                        {
                                            tracing::warn!(
                                                task.id = task_id,
                                                job.id = job_id,
                                                "Error sending agent_resumed activity, receiver dropped"
                                            );
                                        }
                                        agent_action_sender
                                            .send((agent_id, AgentAction::Run(task_id, job_id)))
                                            .ok();
                                    }
                                }
                                AgentNotify::AgentDisconnected(agent_id) => {
                                    tracing::info!(
                                        agent_id,
                                        "Agent worker received `disconnected` notify"
                                    );
                                    {
                                        let mut states = agent_states.write().await;
                                        if let Some(state) = states.get_mut(&agent_id) {
                                            state.clone_from(&AgentState::Disconnected)
                                        };
                                    }
                                    let mut agent_tasks = agent_tasks_sender.write().await;
                                    for task in agent_tasks.get_by_agent_id(&agent_id) {
                                        *task.agent_state.write().await = AgentState::Disconnected;
                                        let (task_id, job_id) = task.task_job_id;
                                        if task.sender
                                            .send_async(Activity::agent_waiting(
                                                task_id,
                                                job_id,
                                                agent_id,
                                                format!("Agent {agent_id} is disconnected"),
                                            ))
                                            .await
                                            .is_err()
                                        {
                                            tracing::warn!(
                                                task.id = task_id,
                                                job.id = job_id,
                                                "Error sending agent_waiting activity, receiver dropped"
                                            );
                                        }
                                    }
                                }
                                AgentNotify::TaskActivity(aid, activity) => {
                                    let (agent_id, task_id, job_id) =
                                        (aid, activity.task_id, activity.job_id);
                                    tracing::info!(
                                        agent.id = agent_id,
                                        task.id = task_id,
                                        job.id = job_id,
                                        "Task activity: {:?}",
                                        activity
                                    );
                                    let agent_tasks = agent_tasks_sender.read().await;
                                    if let Some(task) =
                                        agent_tasks.get_by_task_job_id(&(task_id, job_id))
                                    {
                                        if task.sender.send_async(activity).await.is_err() {
                                            tracing::warn!(
                                                agent.id = agent_id,
                                                task.id = task_id,
                                                job.id = job_id,
                                                "Error sending task activity, receiver dropped"
                                            );
                                        }
                                    } else {
                                        // This is expected: the activity arrived after the task was
                                        // already cleaned up from agent_tasks (e.g. a late-arriving
                                        // "stop task via agent" broadcast after JobNotification::Done
                                        // already called remove_task). The activity has already been
                                        // forwarded directly to the scheduler notify channel, so
                                        // nothing is lost.
                                        tracing::debug!(
                                            agent.id = agent_id,
                                            task.id = task_id,
                                            job.id = job_id,
                                            task.activity = activity.activity,
                                            "Task activity arrived after task worker was cleaned up (expected after task completion)",
                                        );
                                    }
                                }
                                AgentNotify::AgentActivity(aid, activity) => {
                                    tracing::info!(
                                        agent.id = aid,
                                        "Agent activity: {:?}",
                                        activity
                                    );
                                    scheduler_notify_sender.push_agent_activity(activity);
                                }
                                AgentNotify::WriterError(agent_id, task_id, job_id, message) => {
                                    tracing::warn!(
                                        agent_id = agent_id,
                                        task_id = task_id,
                                        message = message.as_str(),
                                        "Writer error: {}",
                                        message
                                    );
                                }
                            }
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => {
                            tokio::spawn(async move {
                                let mut agent_tasks = agent_tasks_sender.write().await;
                                agent_tasks.clear();
                            });
                            break;
                        }
                    }
                }
            }
            .in_current_span()
        });
        Self {
            agent_states,
            agent_tasks: agent_tasks_sender,
            agent_action_sender,
            weak_spawn_receiver,
            task_set: Arc::new(task_set),
            // agent_notify_receiver,
        }
    }

    pub async fn insert(&self, task: AgentTask) {
        let mut agent_tasks = self.agent_tasks.write().await;
        // Ensure task id is unique.
        agent_tasks.remove_by_task_job_id(&task.task_job_id);
        agent_tasks.insert(task);
    }

    pub async fn remove(&self, task_id: i64, job_id: i64) {
        let mut agent_tasks = self.agent_tasks.write().await;
        if let Some(task) = agent_tasks.remove_by_task_job_id(&(task_id, job_id)) {
            task.sender
                .send_async(Activity::stopped(task_id, job_id))
                .await
                .ok();
            if let Err(err) = self
                .agent_action_sender
                .send((task.agent_id, AgentAction::Cancel(task_id, job_id)))
            {
                tracing::warn!("Error sending cancel task: {:?}", err);
            }
        }
    }

    pub async fn stop(&self, task_id: i64, job_id: i64) {
        let agent_tasks = self.agent_tasks.read().await;
        if let Some(task) = agent_tasks.get_by_task_job_id(&(task_id, job_id))
            && let Err(err) = self
                .agent_action_sender
                .send((task.agent_id, AgentAction::Stop(task_id, job_id)))
        {
            tracing::warn!("Error sending cancel task: {:?}", err);
        }
    }

    pub(crate) async fn agent_is_alive(&self, agent_id: i64) -> bool {
        let states = self.agent_states.read().await;
        if let Some(state) = states.get(&agent_id) {
            return state.is_connected();
        }
        false
    }

    pub(crate) async fn agent_tasks(&self, agent_id: i64) -> Vec<(i64, i64)> {
        self.agent_tasks
            .read()
            .await
            .get_by_agent_id(&agent_id)
            .iter()
            .map(|task| task.task_job_id)
            .collect()
    }

    pub async fn list_agent_states(&self) -> HashMap<i64, AgentState> {
        self.agent_states.read().await.clone()
    }

    pub(crate) async fn push_action(
        &self,
        agent_id: i64,
        action: AgentAction,
    ) -> anyhow::Result<()> {
        self.agent_action_sender.send((agent_id, action))?;
        Ok(())
    }

    pub(crate) async fn list_data_sets(
        &self,
        agent_id: i64,
        req: taosx_core::DataSetsReq,
    ) -> anyhow::Result<Vec<DataSet>> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        if let Err(err) = self
            .agent_action_sender
            .send((agent_id, AgentAction::ListDataSets(req, sender)))
        {
            tracing::warn!("Error sending list data sets: {:?}", err);
            bail!("Error sending list data sets: {:?}", err);
        }
        let Ok(res) = receiver.recv_async().await else {
            bail!("failed to receive ListDataSets action, request sender dropped");
        };
        match res {
            Ok(data_sets) => Ok(data_sets),
            Err(err) => Err(anyhow::anyhow!("Error listing data sets: {:#}", err)),
        }
    }

    pub(crate) async fn check(
        &self,
        agent_id: i64,
        dsn: String,
    ) -> anyhow::Result<DataSourceValidation> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        if let Err(err) = self
            .agent_action_sender
            .send((agent_id, AgentAction::Check(dsn, sender)))
        {
            tracing::warn!("Error sending data source validation: {:?}", err);
            bail!("Error sending data source validation: {:?}", err);
        }
        let Ok(res) = receiver.recv_async().await else {
            bail!("failed to receive CheckValid action, request sender dropped");
        };
        Ok(res)
    }

    pub(crate) async fn get_sample(&self, agent_id: i64, dsn: String) -> anyhow::Result<DsSamples> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        if let Err(err) = self
            .agent_action_sender
            .send((agent_id, AgentAction::GetSample(dsn, sender)))
        {
            tracing::warn!("failed to send GetSample action, cause: {:?}", err);
            bail!("failed to send GetSample action, cause: {:?}", err);
        }
        let Ok(res) = receiver.recv_async().await else {
            bail!("failed to receive GetSample action, request sender dropped");
        };

        match res {
            Ok(sample) => {
                let sample = serde_json::from_str(&sample).context("deserialize sample result")?;
                Ok(sample)
            }
            Err(err) => Err(anyhow::anyhow!("failed to get sample, cause: {:#}", err)),
        }
    }

    pub(crate) async fn split_task(
        &self,
        agent_id: i64,
        task: HaTask,
    ) -> anyhow::Result<SplitJobResult> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        if let Err(err) = self
            .agent_action_sender
            .send((agent_id, AgentAction::SplitTask(task, sender)))
        {
            tracing::warn!("failed to send SplitTask action, cause: {:?}", err);
            bail!("failed to send SplitTask action, cause: {:?}", err);
        }
        let Ok(res) = receiver.recv_async().await else {
            bail!("failed to receive SplitTask action, request sender dropped");
        };

        match res {
            Ok(res) => Ok(res),
            Err(err) => bail!("failed to split task, cause: {err:#}"),
        }
    }

    pub(crate) async fn remove_task(&self, task_id: i64, job_id: i64) {
        self.agent_tasks
            .write()
            .await
            .remove_by_task_job_id(&(task_id, job_id));
    }

    /// Put file to agent.
    ///
    /// Arguments:
    /// - `agent_id`: Agent id.
    /// - `path`: File path including file name relative to $DATA_HOME.
    /// - `data`: File data.
    pub(crate) async fn put_file_to_agent(
        &self,
        agent_id: i64,
        path: &str,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        let decompress = path.ends_with(".gz");
        let req = PutFileReq {
            path: path.to_string(),
            data,
            decompress,
        };
        if let Err(err) = self
            .agent_action_sender
            .send((agent_id, AgentAction::PutFile(req, sender)))
        {
            bail!("failed to send PutFile action, cause: {:?}", err);
        }
        let timeout = Duration::from_secs(20);
        match tokio::time::timeout(timeout, receiver.recv_async()).await {
            Ok(result) => match result {
                Ok(res) => match res {
                    Ok(path) => {
                        tracing::info!("PutFile success: {}", path);
                        Ok(())
                    }
                    Err(err) => Err(anyhow::anyhow!("failed to PutFile, cause: {:#}", err)),
                },
                Err(err) => Err(anyhow::anyhow!(
                    "failed to get PutFile response, cause: {:#}",
                    err
                )),
            },
            Err(_) => Err(anyhow::anyhow!("PutFile timed out 20s")),
        }
    }

    pub(crate) async fn query_data_source(
        &self,
        agent_id: i64,
        req: taosx_core::QueryDataSourceReq,
    ) -> anyhow::Result<String> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        if let Err(err) = self
            .agent_action_sender
            .send((agent_id, AgentAction::QueryDataSource(req, sender)))
        {
            bail!("failed to send PutFile action, cause: {:?}", err);
        }
        match tokio::time::timeout(Duration::from_secs(5 * 60), receiver.recv_async()).await {
            Ok(result) => match result {
                Ok(res) => match res {
                    Ok(output) => Ok(output),
                    Err(err) => Err(anyhow::anyhow!(
                        "failed to QueryDataSource, cause: {:#}",
                        err
                    )),
                },
                Err(err) => Err(anyhow::anyhow!(
                    "failed to get QueryDataSource response, cause: {:#}",
                    err
                )),
            },
            Err(_) => Err(anyhow::anyhow!("QueryDataSource timed out 3m")),
        }
    }
}

impl Drop for AgentWorker {
    fn drop(&mut self) {
        if let Some(receiver) = self.weak_spawn_receiver.upgrade() {
            tracing::info!(
                spawn.sender = receiver.sender_count(),
                spawn.len = receiver.len(),
                "Dropping agent worker"
            );
            let _ = receiver.drain();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_state_is_connected_only_for_connected() {
        assert!(AgentState::Connected.is_connected());
        for state in [AgentState::Wait, AgentState::Disconnected] {
            assert!(!state.is_connected());
        }
    }
}
