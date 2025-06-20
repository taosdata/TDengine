use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{collections::HashMap, fmt::Debug};

use anyhow::bail;
use multi_index_map::MultiIndexMap;
use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::transform::sample::{DsSampleIn, DsSamples};
use taosx_core::{DataSet, PutFileReq};
use tokio::{
    runtime::Handle,
    sync::{RwLock, broadcast::error::RecvError},
};
use tracing::Instrument;
use utoipa::openapi::path;
use uuid::Uuid;

use crate::serve::controller::{Activity, AgentAction};
use crate::serve::scheduler::NotifySenderExt;

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

#[derive(Debug, Clone, Default)]
pub enum AgentState {
    #[default]
    Wait,
    Connected,
    Disconnected,
    Closed,
}

impl AgentState {
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }
}

#[derive(MultiIndexMap, Debug)]
pub struct AgentTask {
    #[multi_index(hashed_non_unique)]
    pub agent_id: i64,
    #[multi_index(ordered_unique)]
    pub task_id: TaskId,
    pub agent_state: Arc<RwLock<AgentState>>,
    pub sender: tokio::sync::mpsc::Sender<Activity>,
    pub stop_sender: Arc<tokio::sync::oneshot::Sender<anyhow::Result<()>>>,
}

impl Debug for MultiIndexAgentTaskMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_list();
        for task in self.iter_by_task_id() {
            debug.entry(task);
        }
        debug.finish()
    }
}

#[derive(Debug, Clone)]
pub enum AgentNotify {
    /// RPC server stopped.
    ServerStopped,
    /// Agent connected to RPC server.
    AgentConnected(AgentId),
    /// Agent has been unexpectedly disconnected from RPC server.
    AgentDisconnected(AgentId),
    /// Agent closed by ctrl-c.
    AgentClosed(AgentId),

    /// Put stream writer error.
    ///
    /// This error is sent by agent when it encounters an error while writing data to TDengine.
    WriterError(AgentId, TaskId, String),
    /// Agent task activity.
    TaskActivity(AgentId, Activity),
    /// Agent activity.
    AgentActivity(AgentId, Activity),
}

#[derive(Debug, Clone)]
pub struct AgentWorker {
    agent_states: Arc<RwLock<HashMap<AgentId, AgentState>>>,
    agent_tasks_sender: Arc<RwLock<MultiIndexAgentTaskMap>>,
    agent_activity_sender: AgentActionsSender,
    weak_spawn_receiver: Weak<AgentSpawnReceiver>,
    task_set: Arc<tokio::task::JoinSet<()>>,
    // agent_notify_receiver: tokio::sync::mpsc::Receiver<(AgentId, AgentAction)>,
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
        agent_activity_sender: AgentActionsSender,
        agent_notify_receiver: AgentNotifyReceiver,
        scheduler_notify_sender: NotifySender,
        agent_spawn_receiver: AgentSpawnReceiver,
    ) -> Self {
        let agent_tasks_sender = Arc::new(RwLock::new(MultiIndexAgentTaskMap::default()));
        let agent_tasks_sender_clone = agent_tasks_sender.clone();

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
        let agent_states_cloned = agent_states.clone();

        let agent_activity_sender_clone = agent_activity_sender.clone();

        task_set.spawn(
            async move {
                tokio::pin!(agent_notify_receiver);
                loop {
                    match agent_notify_receiver.recv().await {
                        Ok(item) => {
                            tracing::debug!("Received agent notify: {:?}", item);
                            let agent_tasks_sender_clone = agent_tasks_sender_clone.clone();
                            let agent_states_cloned = agent_states_cloned.clone();
                            let scheduler_notify_sender = scheduler_notify_sender.clone();
                            let agent_activity_sender_clone = agent_activity_sender_clone.clone();
                            tokio::spawn(
                                async move {
                                    match item {
                                        AgentNotify::ServerStopped => {
                                            let mut agent_tasks =
                                                agent_tasks_sender_clone.write().await;
                                            agent_tasks.clear();
                                        }
                                        AgentNotify::AgentConnected(agent_id) => {
                                            tracing::info!("Agent connected: {}", agent_id);
                                            {
                                                let mut states = agent_states_cloned.write().await;
                                                if let std::collections::hash_map::Entry::Vacant(
                                                    e,
                                                ) = states.entry(agent_id)
                                                {
                                                    e.insert(AgentState::Connected);
                                                } else {
                                                    states
                                                        .get_mut(&agent_id)
                                                        .unwrap()
                                                        .clone_from(&AgentState::Connected);
                                                }
                                            }
                                            let mut agent_tasks =
                                                agent_tasks_sender_clone.write().await;
                                            agent_tasks.modify_by_agent_id(&agent_id, |t| {
                                                tokio::task::block_in_place(|| {
                                                    Handle::current().block_on(async {
                                                        *t.agent_state.write().await =
                                                            AgentState::Connected;
                                                        t.sender
                                                            .send(Activity::agent_resumed(
                                                                t.task_id, agent_id,
                                                            ))
                                                            .await;
                                                    });
                                                });
                                            });
                                        }
                                        AgentNotify::AgentDisconnected(agent_id) => {
                                            tracing::info!("Agent disconnected: {}", agent_id);
                                            {
                                                let mut states = agent_states_cloned.write().await;
                                                if states.contains_key(&agent_id) {
                                                    states
                                                        .get_mut(&agent_id)
                                                        .unwrap()
                                                        .clone_from(&AgentState::Disconnected);
                                                }
                                            }
                                            let mut agent_tasks =
                                                agent_tasks_sender_clone.write().await;
                                            agent_tasks.modify_by_agent_id(&agent_id, |t| {
                                                tokio::task::block_in_place(|| {
                                                    Handle::current().block_on(async {
                                                        *t.agent_state.write().await =
                                                            AgentState::Disconnected;

                                                        t.sender
                                                            .send(Activity::waiting(
                                                                t.task_id,
                                                                format!(
                                                                "Agent {agent_id} is disconnected"
                                                            ),
                                                            ))
                                                            .await;
                                                    });
                                                });
                                            });
                                        }
                                        AgentNotify::AgentClosed(agent_id) => {
                                            tracing::info!("Agent closed: {}", agent_id);
                                            {
                                                let mut states = agent_states_cloned.write().await;
                                                if let std::collections::hash_map::Entry::Vacant(
                                                    e,
                                                ) = states.entry(agent_id)
                                                {
                                                    e.insert(AgentState::Closed);
                                                } else {
                                                    states
                                                        .get_mut(&agent_id)
                                                        .unwrap()
                                                        .clone_from(&AgentState::Closed);
                                                }
                                            }
                                            let mut agent_tasks =
                                                agent_tasks_sender_clone.write().await;
                                            agent_tasks.modify_by_agent_id(&agent_id, |t| {
                                                tokio::task::block_in_place(|| {
                                                    Handle::current().block_on(async {
                                                        *t.agent_state.write().await =
                                                            AgentState::Closed;
                                                    });
                                                });
                                            });
                                        }
                                        AgentNotify::TaskActivity(aid, activity) => {
                                            tracing::info!(
                                                agent.id = aid,
                                                task.id = activity.id,
                                                "Task activity: {:?}",
                                                activity
                                            );
                                            let agent_tasks = agent_tasks_sender_clone.read().await;
                                            // dbg!(&agent_tasks);
                                            if let Some(task) =
                                                agent_tasks.get_by_task_id(&activity.id)
                                            {
                                                if let Err(err) = task.sender.send(activity).await {
                                                    tracing::warn!(
                                                        "Error sending task activity {:?}",
                                                        err
                                                    );
                                                }
                                                // scheduler_notify_sender.push_task_activity(activity);
                                            } else {
                                                tracing::warn!(
                                                    agent.id = aid,
                                                    task.id = activity.id,
                                                    task.activity = activity.activity,
                                                    "Task worker not found: {:?}",
                                                    activity.id
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
                                        AgentNotify::WriterError(agent_id, task_id, message) => {
                                            tracing::warn!(
                                                agent_id = agent_id,
                                                task_id = task_id,
                                                message = message.as_str(),
                                                "Writer error: {}",
                                                message
                                            );
                                            let mut agent_tasks =
                                                agent_tasks_sender_clone.write().await;
                                            // let agent_activity_sender = agent_activity_sender_clone.clone();
                                            agent_tasks.modify_by_task_id(&task_id, |t| {
                                                tokio::task::block_in_place(|| {
                                                    Handle::current().block_on(async {
                                                        t.sender
                                                            .send(Activity::interrupt(
                                                                t.task_id,
                                                                format!(
                                                                    "Writer error: {}",
                                                                    message
                                                                ),
                                                            ))
                                                            .await;
                                                    });
                                                });
                                            });
                                        }
                                    }
                                }
                                .in_current_span(),
                            );
                        }
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => {
                            tokio::spawn(async move {
                                let mut agent_tasks = agent_tasks_sender_clone.write().await;
                                agent_tasks.clear();
                            });
                            break;
                        }
                    }
                }
            }
            .in_current_span(),
        );
        Self {
            agent_states,
            agent_tasks_sender,
            agent_activity_sender,
            weak_spawn_receiver,
            task_set: Arc::new(task_set),
            // agent_notify_receiver,
        }
    }

    pub async fn insert(&self, task: AgentTask) {
        let mut agent_tasks = self.agent_tasks_sender.write().await;
        // Ensure task id is unique.
        agent_tasks.remove_by_task_id(&task.task_id);
        agent_tasks.insert(task);
    }

    pub async fn remove(&self, task_id: TaskId) {
        let mut agent_tasks = self.agent_tasks_sender.write().await;
        if let Some(task) = agent_tasks.remove_by_task_id(&task_id) {
            task.sender
                .send(Activity::suspended(task_id, uuid::Uuid::nil()))
                .await
                .ok();
            if let Err(err) = self
                .agent_activity_sender
                .send((task.agent_id, AgentAction::Cancel(task_id)))
            {
                tracing::warn!("Error sending cancel task: {:?}", err);
            }
        }
    }

    pub async fn stop(&self, task_id: TaskId) {
        let agent_tasks = self.agent_tasks_sender.read().await;
        if let Some(task) = agent_tasks.get_by_task_id(&task_id) {
            if let Err(err) = self
                .agent_activity_sender
                .send((task.agent_id, AgentAction::Stop(task_id)))
            {
                tracing::warn!("Error sending cancel task: {:?}", err);
            }
        }
    }

    pub async fn suspend(&self, task_id: TaskId) {
        let agent_tasks = self.agent_tasks_sender.read().await;
        if let Some(task) = agent_tasks.get_by_task_id(&task_id) {
            if let Err(err) = self
                .agent_activity_sender
                .send((task.agent_id, AgentAction::Cancel(task_id)))
            {
                tracing::warn!("Error sending cancel task: {:?}", err);
            }
        }
    }

    pub(crate) async fn agent_is_alive(&self, agent_id: i64) -> bool {
        let states = self.agent_states.read().await;
        if let Some(state) = states.get(&agent_id) {
            return state.is_connected();
        }
        false
    }

    pub(crate) async fn push_action(
        &self,
        agent_id: i64,
        action: AgentAction,
    ) -> anyhow::Result<()> {
        self.agent_activity_sender.send((agent_id, action))?;
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
            .agent_activity_sender
            .send((agent_id, AgentAction::ListDataSets(req, sender)))
        {
            tracing::warn!("Error sending list data sets: {:?}", err);
            bail!("Error sending list data sets: {:?}", err);
        }
        let res = receiver
            .recv_async()
            .await
            .map_err(|err| anyhow::anyhow!("Receiving agent list datasets error: {:#}", err))?;
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
            .agent_activity_sender
            .send((agent_id, AgentAction::Check(dsn, sender)))
        {
            tracing::warn!("Error sending data source validation: {:?}", err);
            bail!("Error sending data source validation: {:?}", err);
        }
        let res = receiver
            .recv_async()
            .await
            .map_err(|err| anyhow::anyhow!("Receiving data source validation error: {:#}", err))?;
        Ok(res)
    }

    pub(crate) async fn get_sample(&self, agent_id: i64, dsn: String) -> anyhow::Result<DsSamples> {
        check_agent_exists!(self, agent_id);
        let (sender, receiver) = flume::bounded(1);
        if let Err(err) = self
            .agent_activity_sender
            .send((agent_id, AgentAction::GetSample(dsn, sender)))
        {
            tracing::warn!("failed to send GetSample action, cause: {:?}", err);
            bail!("failed to send GetSample action, cause: {:?}", err);
        }
        let res = receiver.recv_async().await.map_err(|err| {
            anyhow::anyhow!("failed to receive GetSample action, cause: {:#}", err)
        })?;

        match res {
            Ok(sample) => {
                let sample = serde_json::from_str(&sample).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse GetSample action, response: {}, cause: {:#}",
                        sample,
                        err
                    )
                })?;

                Ok(sample)
            }
            Err(err) => Err(anyhow::anyhow!("failed to get sample, cause: {:#}", err)),
        }
    }

    pub(crate) async fn remove_task(&self, task_id: TaskId) {
        self.agent_tasks_sender
            .write()
            .await
            .remove_by_task_id(&task_id);
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
            .agent_activity_sender
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
            .agent_activity_sender
            .send((agent_id, AgentAction::QueryDataSource(req, sender)))
        {
            bail!("failed to send PutFile action, cause: {:?}", err);
        }
        let timeout = Duration::from_secs(5 * 60);
        match tokio::time::timeout(timeout, receiver.recv_async()).await {
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
        // self.task_set.shutdown();
    }
}
