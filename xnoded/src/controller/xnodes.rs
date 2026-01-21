use std::{cmp, collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use ha_core::{activity::AgentStatus, types::HeartbeatMetrics};
use ha_rpc_client::client::HaRpcClient;
use parking_lot::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::Result;

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XNodeStatus {
    Online,
    #[default]
    Offline,
    Drain,
}

impl std::fmt::Display for XNodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XNodeStatus::Online => write!(f, "online"),
            XNodeStatus::Offline => write!(f, "offline"),
            XNodeStatus::Drain => write!(f, "drain"),
        }
    }
}

type FlightResult = std::result::Result<RecordBatch, FlightError>;

#[derive(Default)]
pub struct XNode {
    client: Option<HaRpcClient>,
    status: XNodeStatus,
    metrics: Option<HeartbeatMetrics>,
    event_rx: Option<flume::Receiver<FlightResult>>,
    agents: HashMap<i64, AgentStatus>,
}

#[derive(Default)]
pub struct XnodeHandle {
    xnode: XNode,
    handle: Option<(JoinSet<Result<()>>, CancellationToken)>,
}

#[derive(Clone)]
pub struct XNodes(Arc<RwLock<HashMap<i32, RwLock<XnodeHandle>>>>);

impl XNodes {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn all(&self) -> Vec<i32> {
        self.0.read().keys().copied().collect()
    }

    pub fn availables(&self) -> Vec<i32> {
        self.0
            .read()
            .iter()
            .filter_map(|(id, xnode)| {
                matches!(xnode.read().xnode.status, XNodeStatus::Online).then_some(*id)
            })
            .collect()
    }

    pub fn add_online(
        &self,
        id: i32,
        client: HaRpcClient,
        event_rx: flume::Receiver<FlightResult>,
    ) {
        self.0.write().insert(
            id,
            RwLock::new(XnodeHandle {
                xnode: XNode {
                    client: Some(client),
                    status: XNodeStatus::Online,
                    metrics: None,
                    event_rx: Some(event_rx),
                    agents: HashMap::new(),
                },
                handle: None,
            }),
        );
    }

    pub fn set_handle(&self, id: i32, handle: JoinSet<Result<()>>, cancel: CancellationToken) {
        if let Some(xnode) = self.0.read().get(&id) {
            let mut xnode = xnode.write();
            xnode.handle = Some((handle, cancel));
        }
    }

    pub fn set_online(
        &self,
        id: i32,
        client: HaRpcClient,
        event_rx: flume::Receiver<FlightResult>,
    ) {
        if let Some(xnode) = self.0.read().get(&id) {
            let mut xnode = xnode.write();
            xnode.xnode.client = Some(client);
            xnode.xnode.status = XNodeStatus::Online;
            xnode.xnode.event_rx = Some(event_rx);
        }
    }

    pub fn add_offline(&self, id: i32) {
        self.0.write().insert(
            id,
            RwLock::new(XnodeHandle {
                xnode: XNode {
                    client: None,
                    status: XNodeStatus::Offline,
                    metrics: None,
                    event_rx: None,
                    agents: HashMap::new(),
                },
                handle: None,
            }),
        );
    }

    pub fn set_offline(&self, id: i32) {
        if let Some(xnode) = self.0.read().get(&id) {
            let mut xnode = xnode.write();
            xnode.xnode.client.take();
            xnode.xnode.status = XNodeStatus::Offline;
        }
    }

    pub fn set_drain(&self, id: i32) {
        if let Some(xnode) = self.0.read().get(&id) {
            let mut xnode = xnode.write();
            xnode.xnode.status = XNodeStatus::Drain;
        }
    }

    pub fn unset_drain(&self, id: i32) {
        if let Some(xnode) = self.0.read().get(&id) {
            let mut xnode = xnode.write();
            xnode.xnode.status = XNodeStatus::Online;
        }
    }

    pub fn remove(&self, id: i32) -> Option<(JoinSet<Result<()>>, CancellationToken)> {
        self.0
            .write()
            .remove(&id)
            .and_then(|v| v.write().handle.take())
    }

    pub fn is_online(&self, id: i32) -> bool {
        self.0.read().get(&id).is_some_and(|xnode| {
            let xnode = xnode.read();
            matches!(xnode.xnode.status, XNodeStatus::Online)
        })
    }

    pub fn is_offline(&self, id: i32) -> bool {
        self.0.read().get(&id).is_none_or(|xnode| {
            let xnode = xnode.read();
            matches!(xnode.xnode.status, XNodeStatus::Offline)
        })
    }

    pub fn status(&self, id: i32) -> Option<XNodeStatus> {
        self.0.read().get(&id).map(|xnode| {
            let xnode = xnode.read();
            xnode.xnode.status
        })
    }

    pub fn update_metrics(&self, id: i32, metrics: HeartbeatMetrics) {
        if let Some(xnode) = self.0.read().get(&id) {
            let mut xnode = xnode.write();
            xnode.xnode.metrics = Some(metrics);
        }
    }

    pub fn get_client(&self, id: i32) -> Option<HaRpcClient> {
        self.0.read().get(&id).and_then(|xnode| {
            let xnode = xnode.read();
            if !matches!(xnode.xnode.status, XNodeStatus::Online) {
                return None;
            }
            xnode.xnode.client.clone()
        })
    }

    pub fn get_event_rx(&self, id: i32) -> Option<flume::Receiver<FlightResult>> {
        self.0
            .read()
            .get(&id)
            .and_then(|xnode| xnode.read().xnode.event_rx.clone())
    }

    pub fn get_one_client(&self) -> Option<(i32, HaRpcClient)> {
        let xnodes = self.0.read();
        for (id, xnode) in xnodes.iter() {
            let xnode = xnode.read();
            if !matches!(xnode.xnode.status, XNodeStatus::Online) {
                continue;
            }
            if let Some(client) = &xnode.xnode.client {
                return Some((*id, client.clone()));
            }
        }
        None
    }

    pub fn available_xnodes_memory(&self, via: Option<i64>) -> Vec<(i32, u64)> {
        let xnodes = self.0.read();
        let mut xnodes_memory = Vec::with_capacity(xnodes.len());
        for (id, xnode) in xnodes.iter() {
            let xnode = xnode.read();
            if via.is_some_and(|v| !xnode.xnode.agents.contains_key(&v)) {
                continue;
            }
            if !matches!(xnode.xnode.status, XNodeStatus::Online) {
                continue;
            }
            let Some(metrics) = xnode.xnode.metrics.as_ref() else {
                continue;
            };
            let available_memory = metrics.memory - metrics.used_memory;
            xnodes_memory.push((*id, available_memory));
        }

        xnodes_memory.sort_by(|a, b| match a.1.cmp(&b.1) {
            cmp::Ordering::Equal => a.0.cmp(&b.0),
            c => c,
        });
        xnodes_memory
    }

    pub fn cpu_cores(&self, id: i32) -> Option<usize> {
        let xnodes = self.0.read();
        let xnode = xnodes.get(&id)?.read();
        let metrics = xnode.xnode.metrics.as_ref()?;
        Some(metrics.cpu_cores)
    }

    pub fn alloc_concurrency(
        &self,
        mut total_concurrency: usize,
        via: Option<i64>,
    ) -> Vec<(i32, usize)> {
        if total_concurrency == 0 {
            return vec![];
        }
        let xnode_memory = self.available_xnodes_memory(via);
        if xnode_memory.len() == total_concurrency {
            return xnode_memory.into_iter().map(|(id, _)| (id, 1)).collect();
        }
        let mut total_memory: u64 = xnode_memory.iter().map(|(_, memory)| *memory).sum();

        let mut xnode_concurrency = Vec::with_capacity(xnode_memory.len());
        for (id, memory) in xnode_memory {
            let tasks = (memory as f64 / total_memory as f64 * total_concurrency as f64) as usize;
            xnode_concurrency.push((id, tasks));
            total_concurrency -= tasks;
            total_memory -= memory;
        }
        if total_concurrency > 0
            && let Some((_, concurrency)) = xnode_concurrency.last_mut()
        {
            *concurrency += total_concurrency;
        }
        xnode_concurrency
    }

    pub fn len(&self) -> usize {
        self.0
            .read()
            .iter()
            .filter(|(_, xnode)| matches!(xnode.read().xnode.status, XNodeStatus::Online))
            .count()
    }

    pub fn best_xnode(&self, via: Option<i64>) -> Option<i32> {
        self.0
            .read()
            .iter()
            .filter_map(|(id, xnode)| {
                let xnode = xnode.read();
                if !matches!(xnode.xnode.status, XNodeStatus::Online) {
                    return None;
                }
                if via.is_some_and(|v| !xnode.xnode.agents.contains_key(&v)) {
                    return None;
                }
                xnode
                    .xnode
                    .metrics
                    .as_ref()
                    .map(|v| (id, v.memory - v.used_memory))
            })
            .max_by_key(|(_, xnode)| *xnode)
            .map(|(id, _)| *id)
    }

    pub fn is_cancelled(&self, id: i32) -> bool {
        self.0.read().get(&id).is_none_or(|v| {
            v.read()
                .handle
                .as_ref()
                .is_none_or(|(_, cancel)| cancel.is_cancelled())
        })
    }

    pub fn del_agent(&self, id: i32, agent_id: i64) {
        if let Some(v) = self.0.read().get(&id) {
            v.write().xnode.agents.remove(&agent_id);
        }
    }

    pub fn set_agent_status(&self, id: i32, agent_id: i64, status: AgentStatus) {
        if let Some(v) = self.0.read().get(&id) {
            if matches!(status, AgentStatus::Disconnected) {
                v.write().xnode.agents.remove(&agent_id);
                return;
            }
            v.write().xnode.agents.insert(agent_id, status);
        }
    }

    pub fn clear_xnode_agents(&self, id: i32) {
        if let Some(v) = self.0.read().get(&id) {
            v.write().xnode.agents.clear();
        }
    }

    pub fn agent_status(&self, agent_id: i64) -> AgentStatus {
        let xnodes = self.0.read();
        if xnodes.values().any(|v| {
            v.read()
                .xnode
                .agents
                .get(&agent_id)
                .is_some_and(|v| v.is_transferring())
        }) {
            return AgentStatus::Transferring;
        }
        if xnodes.values().any(|v| {
            v.read()
                .xnode
                .agents
                .get(&agent_id)
                .is_some_and(|v| v.is_waiting())
        }) {
            return AgentStatus::Waiting;
        }
        if xnodes.values().any(|v| {
            v.read()
                .xnode
                .agents
                .get(&agent_id)
                .is_some_and(|v| v.is_idle())
        }) {
            return AgentStatus::Idle;
        }
        if xnodes.values().any(|v| {
            v.read()
                .xnode
                .agents
                .get(&agent_id)
                .is_some_and(|v| v.is_connected())
        }) {
            return AgentStatus::Connected;
        }

        AgentStatus::Disconnected
    }

    pub fn agents(&self, xnode_id: i32) -> Vec<i64> {
        self.0
            .read()
            .get(&xnode_id)
            .map(|xnode| xnode.read().xnode.agents.keys().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_xnodes(online: &[u64], offline: usize) -> XNodes {
        let mut xnodes = HashMap::with_capacity(online.len() + offline);
        let mut id = 0;
        for mem in online {
            let xnode = XnodeHandle {
                xnode: XNode {
                    status: XNodeStatus::Online,
                    metrics: Some(HeartbeatMetrics {
                        memory: *mem,
                        used_memory: 0,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            };
            xnodes.insert(id, RwLock::new(xnode));
            id += 1;
        }

        for _ in 0..offline {
            xnodes.insert(id, RwLock::new(XnodeHandle::default()));
            id += 1;
        }

        XNodes(Arc::new(RwLock::new(xnodes)))
    }

    #[test]
    fn available_memory_test() {
        let xnodes = build_xnodes(&[1024, 2048, 2048], 2);
        assert_eq!(
            xnodes.available_xnodes_memory(None),
            vec![(0, 1024), (1, 2048), (2, 2048)]
        );
    }

    #[test]
    fn alloc_concurrency_test() {
        let xnodes = build_xnodes(&[1024, 2048, 2048], 2);
        assert_eq!(
            xnodes.alloc_concurrency(5, None),
            vec![(0, 1), (1, 2), (2, 2)]
        );

        let xnodes = build_xnodes(&[1024, 1024, 1024], 2);
        assert_eq!(
            xnodes.alloc_concurrency(5, None),
            vec![(0, 1), (1, 2), (2, 2)]
        );

        let xnodes = build_xnodes(&[1024, 1024, 2048], 2);
        assert_eq!(
            xnodes.alloc_concurrency(5, None),
            vec![(0, 1), (1, 1), (2, 3)]
        );
    }
}
