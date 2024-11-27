#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobState, NotificationData};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobState, NotificationData};
use crate::job::{JobId, NotificationId};
use crate::store::{CodeGet, DataStore, InitStore};
use crate::{JobSchedulerError, OnJobNotification};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

pub trait NotificationStore: DataStore<NotificationData> + InitStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: JobId,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NotificationId>, JobSchedulerError>> + Send>>;

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>>;

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>>;

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>>;
}

pub trait NotificationRunnableCodeGet: CodeGet<Box<OnJobNotification>> {}
