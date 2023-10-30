use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

use anyhow::bail;
use multi_index_map::MultiIndexMap;
use taosx_core::DataSet;
use tokio::{
    runtime::Handle,
    sync::{broadcast::error::RecvError, RwLock},
};

use crate::serve::controller::agent::Activity;
use crate::serve::controller::{AgentAction, TaskActivity};
use crate::serve::scheduler::NotifySenderExt;

use super::NotifySender;

pub type TaskId = i64;
pub type AgentId = i64;
pub type AgentActionsSender = tokio::sync::broadcast::Sender<(AgentId, AgentAction)>;
pub type AgentActionsReceiver = tokio::sync::broadcast::Receiver<(AgentId, AgentAction)>;
pub type AgentNotifySender = tokio::sync::broadcast::Sender<AgentNotify>;
pub type AgentNotifyReceiver = tokio::sync::broadcast::Receiver<AgentNotify>;

#[derive(Debug, Clone, Default)]
pub enum AgentState {
    #[default]
    Wait,
    Connected,
    Disconnected,
    Closed,
}
#[derive(MultiIndexMap, Debug)]
pub struct AgentTask {
    #[multi_index(hashed_non_unique)]
    pub agent_id: i64,
    #[multi_index(ordered_unique)]
    pub task_id: TaskId,

    pub agent_state: Arc<RwLock<AgentState>>,
    pub sender: tokio::sync::mpsc::Sender<TaskActivity>,
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
    /// Agent task activity.
    TaskActivity(AgentId, TaskActivity),
    /// Agent activity.
    AgentActivity(AgentId, Activity),
}

#[derive(Debug, Clone)]
pub struct AgentWorker {
    agent_states: Arc<RwLock<HashMap<AgentId, AgentState>>>,
    agent_tasks_sender: Arc<RwLock<MultiIndexAgentTaskMap>>,
    agent_activity_sender: AgentActionsSender,
    // agent_notify_receiver: tokio::sync::mpsc::Receiver<(AgentId, AgentAction)>,
}

impl AgentWorker {
    pub async fn new(
        agent_activity_sender: AgentActionsSender,
        agent_notify_receiver: AgentNotifyReceiver,
        scheduler_notify_sender: NotifySender,
    ) -> Self {
        let agent_tasks_sender = Arc::new(RwLock::new(MultiIndexAgentTaskMap::default()));
        let agent_tasks_sender_clone = agent_tasks_sender.clone();

        let agent_states: Arc<RwLock<HashMap<AgentId, AgentState>>> = Default::default();
        let agent_states_cloned = agent_states.clone();

        tokio::spawn(async move {
            tokio::pin!(agent_notify_receiver);
            loop {
                match agent_notify_receiver.recv().await {
                    Ok(item) => {
                        let agent_tasks_sender_clone = agent_tasks_sender_clone.clone();
                        let agent_states_cloned = agent_states_cloned.clone();
                        let scheduler_notify_sender = scheduler_notify_sender.clone();
                        tokio::spawn(async move {
                            match item {
                                AgentNotify::ServerStopped => {
                                    let mut agent_tasks = agent_tasks_sender_clone.write().await;
                                    agent_tasks.clear();
                                }
                                AgentNotify::AgentConnected(agent_id) => {
                                    tracing::info!("Agent connected: {}", agent_id);
                                    {
                                        let mut states = agent_states_cloned.write().await;
                                        if states.contains_key(&agent_id) {
                                            states
                                                .get_mut(&agent_id)
                                                .unwrap()
                                                .clone_from(&AgentState::Connected);
                                        } else {
                                            states.insert(agent_id, AgentState::Connected);
                                        }
                                    }
                                    let mut agent_tasks = agent_tasks_sender_clone.write().await;
                                    agent_tasks.modify_by_agent_id(&agent_id, |t| {
                                        tokio::task::block_in_place(|| {
                                            Handle::current().block_on(async {
                                                *t.agent_state.write().await =
                                                    AgentState::Connected;
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
                                        } else {
                                            states.insert(agent_id, AgentState::Disconnected);
                                        }
                                    }
                                    let mut agent_tasks = agent_tasks_sender_clone.write().await;
                                    agent_tasks.modify_by_agent_id(&agent_id, |t| {
                                        tokio::task::block_in_place(|| {
                                            Handle::current().block_on(async {
                                                *t.agent_state.write().await =
                                                    AgentState::Disconnected;
                                            });
                                        });
                                    });
                                }
                                AgentNotify::AgentClosed(agent_id) => {
                                    tracing::info!("Agent closed: {}", agent_id);
                                    {
                                        let mut states = agent_states_cloned.write().await;
                                        if states.contains_key(&agent_id) {
                                            states
                                                .get_mut(&agent_id)
                                                .unwrap()
                                                .clone_from(&AgentState::Closed);
                                        } else {
                                            states.insert(agent_id, AgentState::Closed);
                                        }
                                    }
                                    let mut agent_tasks = agent_tasks_sender_clone.write().await;
                                    agent_tasks.modify_by_agent_id(&agent_id, |t| {
                                        tokio::task::block_in_place(|| {
                                            Handle::current().block_on(async {
                                                *t.agent_state.write().await = AgentState::Closed;
                                            });
                                        });
                                    });
                                }
                                AgentNotify::TaskActivity(_, activity) => {
                                    let agent_tasks = agent_tasks_sender_clone.read().await;
                                    if let Some(task) = agent_tasks.get_by_task_id(&activity.id) {
                                        if let Err(err) = task.sender.send(activity.clone()).await {
                                            tracing::warn!("Error sending task activity {:?}", err);
                                        }
                                    } else {
                                        tracing::warn!(
                                            task.id = activity.id,
                                            task.activity = activity.activity,
                                            "Task not found: {:?}",
                                            activity.id
                                        );
                                    }
                                    scheduler_notify_sender.push_task_activity(activity);
                                }
                                AgentNotify::AgentActivity(_, activity) => {
                                    tracing::info!("Agent activity: {:?}", activity);
                                    scheduler_notify_sender.push_agent_activity(activity);
                                }
                            }
                        });
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
        });
        Self {
            agent_states,
            agent_tasks_sender,
            agent_activity_sender,
            // agent_notify_receiver,
        }
    }

    pub async fn insert(&self, task: AgentTask) {
        let mut agent_tasks = self.agent_tasks_sender.write().await;
        agent_tasks.insert(task);
    }

    pub async fn remove(&self, task_id: TaskId) {
        let mut agent_tasks = self.agent_tasks_sender.write().await;
        if let Some(task) = agent_tasks.remove_by_task_id(&task_id) {
            task.sender
                .send(TaskActivity::suspended(task_id, uuid::Uuid::nil()))
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

    pub async fn cancel(&self, task_id: TaskId) {
        let agent_tasks = self.agent_tasks_sender.read().await;
        if let Some(task) = agent_tasks.get_by_task_id(&task_id) {
            task.sender
                .send(TaskActivity::suspended(task_id, uuid::Uuid::nil()))
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

    pub async fn is_alive(&self, agent_id: AgentId) -> bool {
        let states = self.agent_states.read().await;
        states.contains_key(&agent_id)
    }

    pub(crate) async fn agent_is_alive(&self, agent_id: i64) -> bool {
        let states = self.agent_states.read().await;
        states.contains_key(&agent_id)
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
        {
            let states = self.agent_states.read().await;
            if !states.contains_key(&agent_id) {
                return Err(anyhow::anyhow!("Agent not found: {}", agent_id));
            }
        }
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
}
