use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
    time::Duration,
};

use crate::serve::{controller::activity::Activity, scheduler::SchedulerNotify};
use itertools::Itertools;
use taosx_core::{
    DataSet,
    sink::lush::{self, TableTagCache},
    utils::breakpoints::BreakpointDb,
};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock};

use super::{
    NotifySender,
    runner::{GlobalState, MultiIndexTaskJobMapRef},
};

use anyhow::Result;
use tokio_cron_scheduler::JobNotification;
use tracing::{Instrument, info};
use uuid::Uuid;

#[allow(clippy::type_complexity)]
pub async fn notify_by_job_id(
    tasks: &MultiIndexTaskJobMapRef,
    global: &GlobalState,
    sched_id: &Uuid,
    job_state: &JobNotification,
    lush_table_cache: &Arc<RwLock<HashMap<(i64, i64), Arc<TableTagCache>>>>,
    task_breakpoint_db: &Arc<RwLock<HashMap<(i64, i64), BreakpointDb>>>,
) -> Option<Result<()>> {
    let task_job_id = {
        tasks
            .read()
            .await
            .get_by_schedule_id(sched_id)
            .map(|j| j.task_job_id)
    }?;
    let (task_id, job_id) = task_job_id;
    let span = tracing::info_span!("notify_by_job_id", task.id = task_id, job.id = job_id, sched.id = %sched_id);
    let _enter = span.enter();

    match job_state {
        JobNotification::Stop => {
            info!("Stopping task {:?}", task_job_id);
        }
        JobNotification::Scheduled => {
            info!("Scheduling task {:?}", task_job_id);
        }
        JobNotification::Started => {
            info!("Starting task {:?}", task_job_id);
        }
        JobNotification::Done => {
            info!("Done task {:?}", task_job_id);
            let sched_id = *sched_id;
            let tasks = tasks.clone();
            let global = global.clone();
            tokio::task::spawn(
                async move {
                    let mut tasks = tasks.write().await;
                    let to_remove = {
                        if let Some(task) = tasks.get_by_schedule_id(&sched_id) {
                            task.is_finished().await
                        } else {
                            false
                        }
                    };

                    let (task_id, job_id) = task_job_id;
                    if to_remove {
                        tracing::info!("Removing task {:?}", job_id);
                        if let Some(task) = tasks.remove_by_schedule_id(&sched_id)
                            && task.task.task.via.is_some()
                        {
                            global.agent_worker.remove_task(task_id, job_id).await;
                        }
                    }
                }
                .in_current_span(),
            );
            // TODO： 只对 PI 任务执行下面的代码
            let lush_table_cache = lush_table_cache.clone();
            let task_breakpoint_db = task_breakpoint_db.clone();
            tokio::task::spawn(
                async move {
                    let mut lush_table_cache = lush_table_cache.write().await;
                    if let Some(cache) = lush_table_cache.remove(&task_job_id) {
                        info!("Removed lush_table_cache task.id={:?}", task_job_id);
                    }
                    let mut task_breakpoint_db = task_breakpoint_db.write().await;
                    if let Some(cache) = task_breakpoint_db.remove(&task_job_id) {
                        info!("Removed task_breakpoint_db task.id={:?}", task_job_id);
                    }
                }
                .in_current_span(),
            );
        }
        JobNotification::Removed => {
            info!("Removed task {:?}", task_job_id);
        }
    }
    Some(Ok(()))
}
