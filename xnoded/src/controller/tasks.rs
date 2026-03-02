use std::{collections::HashMap, str::FromStr, sync::Arc};

use ha_core::{activity::TaskStatus, types::HaTask};
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

impl TaskJobInfo {
    pub fn should_skip_rebalance(&self) -> bool {
        self.manually_rebalance
            || self.manually_stopped
            || self.status.is_none_or(|v| !v.is_running())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    use ha_core::activity::TaskStatus;
    use ha_core::types::HaTask;

    fn make_task(from: &str) -> HaTask {
        HaTask {
            from: from.to_string(),
            to: "taos://localhost:6030".to_string(),
            parser: None,
            via: None,
            labels: None,
        }
    }

    #[test]
    fn task_job_info_should_skip_rebalance_respects_flags_and_status() {
        let config = make_task("taos://localhost:6030");

        let info = TaskJobInfo {
            xnode_id: 1,
            manually_rebalance: false,
            manually_stopped: false,
            oneshot: false,
            status: Some(TaskStatus::Running),
            config: config.clone(),
        };
        assert!(!info.should_skip_rebalance());

        let info = TaskJobInfo {
            manually_rebalance: true,
            ..info.clone()
        };
        assert!(info.should_skip_rebalance());

        let info = TaskJobInfo {
            manually_rebalance: false,
            manually_stopped: true,
            ..info.clone()
        };
        assert!(info.should_skip_rebalance());

        let info = TaskJobInfo {
            manually_rebalance: false,
            manually_stopped: false,
            status: None,
            ..info.clone()
        };
        assert!(info.should_skip_rebalance());

        let info = TaskJobInfo {
            status: Some(TaskStatus::Stopped),
            ..info
        };
        assert!(info.should_skip_rebalance());
    }

    #[test]
    fn tasks_add_and_query_basic() {
        let tasks = Tasks::new();
        let task = make_task("taos://localhost:6030");
        tasks
            .add(1, 10, 100, task.clone(), Some(TaskStatus::Running))
            .unwrap();

        assert!(tasks.contains(1, 10));
        assert!(!tasks.contains(1, 11));

        let info = tasks.job(1, 10).expect("job");
        assert_eq!(info.xnode_id, 100);
        assert_eq!(info.status, Some(TaskStatus::Running));
        assert_eq!(info.config, task);
        assert!(!info.oneshot);
        assert!(!tasks.is_stopped(1));
        assert!(!tasks.is_manually_stopped(1));
        assert!(tasks.task_has_jobs(1));
        assert!(tasks.is_oneshot(1) == info.oneshot);
    }

    #[test]
    fn tasks_status_and_delete_helpers_work() {
        let tasks = Tasks::new();
        let task = make_task("taos://localhost:6030");
        tasks
            .add(1, 10, 1, task.clone(), Some(TaskStatus::Running))
            .unwrap();
        tasks
            .add(1, 11, 2, task.clone(), Some(TaskStatus::Stopped))
            .unwrap();

        assert!(!tasks.is_stopped(1));
        tasks.set_status(1, 10, TaskStatus::Stopped);
        assert!(tasks.is_stopped(1));

        assert!(!tasks.is_manually_stopped(1));
        tasks.set_manually_stopped(1, 10);
        assert!(tasks.is_manually_stopped(1));

        let jobs = tasks.task_jobs(1);
        assert_eq!(jobs.len(), 2);

        let xnode_jobs = tasks.xnode_jobs(2);
        assert_eq!(xnode_jobs, vec![(1, 11)]);

        let removed = tasks.del_xnode_jobs(2);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, (1, 11));

        tasks.del_task_job(1, 10);
        assert!(!tasks.task_has_jobs(1));
    }

    #[test]
    fn is_oneshot_dsn_helper_matches_driver() {
        assert!(is_oneshot("csv:///tmp/demo.csv").unwrap());
        assert!(is_oneshot("orc:///tmp/demo.orc").unwrap());
        assert!(!is_oneshot("taos://localhost:6030").unwrap());
    }
}
