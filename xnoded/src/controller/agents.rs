use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct Agents(Arc<RwLock<HashMap<i64, String>>>);

impl Agents {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn add(&self, id: i64, token: &str) {
        if self.0.read().contains_key(&id) {
            return;
        }
        self.0.write().insert(id, token.to_string());
    }

    pub fn del(&self, id: i64) {
        self.0.write().remove(&id);
    }

    pub fn has(&self, id: i64) -> bool {
        self.0.read().contains_key(&id)
    }

    pub fn all_tokens(&self) -> Vec<String> {
        self.0.read().values().cloned().collect()
    }
}
