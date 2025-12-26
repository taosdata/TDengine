use std::{collections::HashMap, str::FromStr, sync::Arc};

use ha_core::types::{HaTask, TaskStatus};
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct TaskJobInfo {
    pub xnode_id: i32,
    pub manually_rebalance: bool,
    pub manually_stopped: bool,
    pub oneshot: bool,
    pub status: Option<TaskStatus>,
    pub config: HaTask,
}

#[derive(Clone)]
pub struct Tasks(Arc<RwLock<HashMap<(i64, i64), TaskJobInfo>>>);

impl Tasks {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn contains(&self, tid: i64, jid: i64) -> bool {
        self.0.read().contains_key(&(tid, jid))
    }

    pub fn is_stopped(&self, tid: i64) -> bool {
        self.0
            .read()
            .iter()
            .filter_map(|((task_id, _), info)| (task_id == &tid).then_some(info))
            .all(|info| info.status.as_ref().is_none_or(|v| v.is_stopped()))
    }

    pub fn set_status(&self, tid: i64, jid: i64, status: TaskStatus) {
        self.0
            .write()
            .entry((tid, jid))
            .and_modify(|info| info.status = Some(status));
    }

    pub fn set_manually_stopped(&self, tid: i64, jid: i64) {
        self.0
            .write()
            .entry((tid, jid))
            .and_modify(|info| info.manually_stopped = true);
    }

    pub fn set_manually_rebalance(&self, tid: i64, jid: i64) {
        self.0
            .write()
            .entry((tid, jid))
            .and_modify(|info| info.manually_rebalance = true);
    }

    pub fn is_manually_stopped(&self, tid: i64) -> bool {
        self.0
            .read()
            .iter()
            .filter_map(|((task_id, _), info)| (task_id == &tid).then_some(info))
            .any(|info| info.manually_stopped)
    }

    pub fn add(
        &self,
        tid: i64,
        jid: i64,
        xid: i32,
        config: HaTask,
        status: Option<TaskStatus>,
    ) -> Result<(), taos::DsnError> {
        let oneshot = is_oneshot(&config.from)?;
        self.0.write().insert(
            (tid, jid),
            TaskJobInfo {
                xnode_id: xid,
                manually_rebalance: false,
                manually_stopped: false,
                config,
                oneshot,
                status,
            },
        );
        Ok(())
    }

    pub fn del_task(&self, tid: i64) -> Vec<((i64, i64), TaskJobInfo)> {
        let mut jobs = Vec::new();
        self.0.write().retain(|id, job| {
            if id.0 == tid {
                jobs.push((*id, job.clone()));
                false
            } else {
                true
            }
        });
        jobs
    }

    pub fn del_task_job(&self, tid: i64, jid: i64) {
        self.0.write().remove(&(tid, jid));
    }

    pub fn del_xnode_jobs(&self, xid: i32) -> Vec<((i64, i64), TaskJobInfo)> {
        let mut jobs = Vec::new();
        self.0.write().retain(|id, job| {
            if job.xnode_id == xid {
                jobs.push((*id, job.clone()));
                false
            } else {
                true
            }
        });
        jobs
    }

    pub fn xnode_jobs(&self, xid: i32) -> Vec<(i64, i64)> {
        self.0
            .read()
            .iter()
            .filter(|(_, state)| state.xnode_id == xid)
            .map(|(key, _)| *key)
            .collect()
    }

    pub fn task_jobs(&self, tid: i64) -> Vec<((i64, i64), TaskJobInfo)> {
        self.0
            .read()
            .iter()
            .filter(|(key, _)| key.0 == tid)
            .map(|(key, task)| (*key, task.clone()))
            .collect()
    }

    pub fn task_has_jobs(&self, tid: i64) -> bool {
        self.0
            .read()
            .iter()
            .any(|((task_id, job_id), _)| task_id == &tid && *job_id >= 0)
    }

    pub fn job(&self, tid: i64, jid: i64) -> Option<TaskJobInfo> {
        self.0.read().get(&(tid, jid)).cloned()
    }

    pub fn is_oneshot(&self, tid: i64) -> bool {
        self.0
            .read()
            .iter()
            .any(|((task_id, _), job)| task_id == &tid && job.oneshot)
    }
}

pub fn is_oneshot(from: &str) -> Result<bool, taos::DsnError> {
    let dsn = taos::Dsn::from_str(from)?;
    Ok(matches!(dsn.driver.as_str(), "csv" | "orc"))
}
