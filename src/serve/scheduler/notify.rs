use crate::serve::controller::TaskActivity;

use super::{runner::MultiIndexTaskJobMapRef, NotifySender};

use anyhow::Result;
use tokio_cron_scheduler::JobNotification;
use tracing::info;
use uuid::Uuid;

pub async fn notify_by_job_id(
    tasks: &MultiIndexTaskJobMapRef,
    sender: &NotifySender,
    job_id: &Uuid,
    job_state: &JobNotification,
) -> Option<Result<()>> {
    let task_id = { tasks.read().await.get_by_job_id(&job_id).map(|j| j.task_id) }?;

    match job_state {
        JobNotification::Stop => {
            info!("Stopping task {:?}", task_id);
            if let Err(err) = sender
                .upgrade()
                .map(|tx| tx.send(TaskActivity::stop(task_id)))
                .transpose()
            {
                tracing::warn!("Error sending task activity {:?}", err);
            }
        }
        JobNotification::Scheduled => {
            info!("Scheduling task {:?}", task_id);
            // if let Err(err) = sender
            //     .upgrade()
            //     .map(|tx| tx.send(TaskActivity::scheduled(task_id)))
            //     .transpose()
            // {
            //     tracing::warn!("Error sending task activity {:?}", err);
            // }
        }
        JobNotification::Started => {
            info!("Starting task {:?}", task_id);
        }
        JobNotification::Done => {
            info!("Done task {:?}", task_id);
            tasks.write().await.remove_by_job_id(&job_id);
        }
        JobNotification::Removed => {
            info!("Removed task {:?}", task_id);
        }
    }
    Some(Ok(()))
}
