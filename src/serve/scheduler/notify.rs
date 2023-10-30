use crate::serve::{controller::TaskActivity, scheduler::SchedulerNotify};

use super::{
    runner::{GlobalState, MultiIndexTaskJobMapRef},
    NotifySender,
};

use anyhow::Result;
use tokio_cron_scheduler::JobNotification;
use tracing::info;
use uuid::Uuid;

pub async fn notify_by_job_id(
    tasks: &MultiIndexTaskJobMapRef,
    global: &GlobalState,
    job_id: &Uuid,
    job_state: &JobNotification,
) -> Option<Result<()>> {
    let task_id = { tasks.read().await.get_by_job_id(&job_id).map(|j| j.task_id) }?;

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
            let job_id = job_id.clone();
            tokio::task::spawn(async move {
                if let Some(task) = tasks.write().await.remove_by_job_id(&job_id) {
                    if task.task.task.via.is_some() {
                        global.agent_runtime.remove_task(task_id).await;
                    }
                }
            });
        }
        JobNotification::Removed => {
            info!("Removed task {:?}", task_id);
        }
    }
    Some(Ok(()))
}
