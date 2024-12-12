mod creator;
mod deleter;
mod runner;

#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::NotificationData;
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::NotificationData;
use crate::job::{JobId, NotificationId};
pub use creator::NotificationCreator;
pub use deleter::NotificationDeleter;
pub use runner::NotificationRunner;
use uuid::Uuid;

impl NotificationData {
    pub fn job_id_and_notification_id_from_data(&self) -> Option<(JobId, NotificationId)> {
        match self.job_id.as_ref() {
            Some(j) => match (j.job_id.as_ref(), j.notification_id.as_ref()) {
                (Some(job_id), Some(notification_id)) => {
                    let job_id: Uuid = job_id.into();
                    let notification_id: Uuid = notification_id.into();
                    Some((job_id, notification_id))
                }
                _ => None,
            },
            None => None,
        }
    }
}
