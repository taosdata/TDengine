use std::{collections::HashMap, sync::Arc};

use ha_core::activity::AgentStatus;
use parking_lot::RwLock;

struct AgentsInner {
    tokens: RwLock<HashMap<i64, String>>,
    /// Cached agent status shared between the periodic updater and the
    /// real-time event loop. See `Tasks::job_status_cache` for race notes.
    status_cache: RwLock<HashMap<i64, AgentStatus>>,
}

#[derive(Clone)]
pub struct Agents(Arc<AgentsInner>);

impl Agents {
    pub fn new() -> Self {
        Self(Arc::new(AgentsInner {
            tokens: RwLock::new(HashMap::new()),
            status_cache: RwLock::new(HashMap::new()),
        }))
    }

    // ---- status cache methods ----

    pub fn get_cached_agent_state(&self, agent_id: i64) -> Option<AgentStatus> {
        self.0.status_cache.read().get(&agent_id).copied()
    }

    pub fn set_cached_agent_state(&self, agent_id: i64, state: AgentStatus) {
        self.0.status_cache.write().insert(agent_id, state);
    }

    pub fn remove_cached_agent_state(&self, agent_id: i64) {
        self.0.status_cache.write().remove(&agent_id);
    }

    // ---- token methods ----

    pub fn add(&self, id: i64, token: &str) {
        if self.0.tokens.read().contains_key(&id) {
            return;
        }
        self.0.tokens.write().insert(id, token.to_string());
    }

    pub fn del(&self, id: i64) {
        self.0.tokens.write().remove(&id);
    }

    pub fn has(&self, id: i64) -> bool {
        self.0.tokens.read().contains_key(&id)
    }

    pub fn all_tokens(&self) -> Vec<String> {
        self.0.tokens.read().values().cloned().collect()
    }
}
