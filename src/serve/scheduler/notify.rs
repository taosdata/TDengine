use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::serve::{controller::TaskActivity, scheduler::SchedulerNotify};
use itertools::Itertools;
use taosx_core::{
    sink::lush::{self, TableTagCache},
    utils::breakpoints::BreakpointDb,
    DataSet,
};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock};

use super::{
    runner::{GlobalState, MultiIndexTaskJobMapRef},
    NotifySender,
};

use anyhow::Result;
use tokio_cron_scheduler::JobNotification;
use tracing::{info, Instrument};
use uuid::Uuid;

pub async fn notify_by_job_id(
    tasks: &MultiIndexTaskJobMapRef,
    global: &GlobalState,
    job_id: &Uuid,
    job_state: &JobNotification,
    lush_table_cache: &Arc<RwLock<HashMap<i64, Arc<TableTagCache>>>>,
    task_breakpoint_db: &Arc<RwLock<HashMap<i64, BreakpointDb>>>,
) -> Option<Result<()>> {
    let task_id = { tasks.read().await.get_by_job_id(job_id).map(|j| j.task_id) }?;

    match job_state {
        JobNotification::Stop => {
            info!("Stopping task {:?}", task_id);
            global.send_agent_activity(TaskActivity::stop(task_id));
        }
        JobNotification::Scheduled => {
            info!("Scheduling task {:?}", task_id);
        }
        JobNotification::Started => {
            info!("Starting task {:?}", task_id);
        }
        JobNotification::Done => {
            info!("Done task {:?}", task_id);
            let tasks = tasks.clone();
            let global = global.clone();
            let job_id = *job_id;
            tokio::task::spawn(async move {
                let mut tasks = tasks.write().await;
                let to_remove = {
                    if let Some(task) = tasks.get_by_job_id(&job_id) {
                        task.is_finished().await
                    } else {
                        false
                    }
                };

                if to_remove {
                    if let Some(task) = tasks.remove_by_job_id(&job_id) {
                        if task.task.task.via.is_some() {
                            global.agent_runtime.remove_task(task_id).await;
                        }
                    }
                }
            });
            // TODO： 只对 PI 任务执行下面的代码
            let lush_table_cache = lush_table_cache.clone();
            let task_breakpoint_db = task_breakpoint_db.clone();
            tokio::task::spawn(
                async move {
                    let mut lush_table_cache = lush_table_cache.write().await;
                    if let Some(cache) = lush_table_cache.remove(&task_id) {
                        info!("Removed lush_table_cache task.id={:?}", task_id);
                    }
                    let mut task_breakpoint_db = task_breakpoint_db.write().await;
                    if let Some(cache) = task_breakpoint_db.remove(&task_id) {
                        info!("Removed task_breakpoint_db task.id={:?}", task_id);
                    }
                }
                .in_current_span(),
            );
        }
        JobNotification::Removed => {
            info!("Removed task {:?}", task_id);
        }
    }
    Some(Ok(()))
}
