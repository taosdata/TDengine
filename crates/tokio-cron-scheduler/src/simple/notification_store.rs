#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobIdAndNotification, JobState, NotificationData};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobIdAndNotification, JobState, NotificationData};
use crate::job::{JobId, NotificationId};
use crate::store::{DataStore, InitStore, NotificationStore};
use crate::JobSchedulerError;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct SimpleNotificationStore {
    pub data: Arc<RwLock<HashMap<Uuid, HashMap<Uuid, NotificationData>>>>,
    pub notification_vs_job: Arc<RwLock<HashMap<Uuid, Uuid>>>,
    pub inited: bool,
}

impl Default for SimpleNotificationStore {
    fn default() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            notification_vs_job: Arc::new(RwLock::new(HashMap::new())),
            inited: false,
        }
    }
}

impl InitStore for SimpleNotificationStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        self.inited = true;
        Box::pin(std::future::ready(Ok(())))
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let val = self.inited;
        Box::pin(std::future::ready(Ok(val)))
    }
}

impl DataStore<NotificationData> for SimpleNotificationStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<NotificationData>, JobSchedulerError>> + Send>>
    {
        let data = self.data.clone();
        let job = self.notification_vs_job.clone();
        Box::pin(async move {
            let job = job.read().await;
            let job = job.get(&id);
            match job {
                Some(job) => {
                    let val = data.read().await;
                    let val = val.get(job);
                    match val {
                        Some(job) => {
                            let val = job.get(&id).cloned();
                            Ok(val)
                        }
                        None => Err(JobSchedulerError::GetJobData),
                    }
                }
                None => Err(JobSchedulerError::GetJobData),
            }
        })
    }

    fn add_or_update(
        &mut self,
        data: NotificationData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::pin(async move {
            let id = data.job_id.as_ref();
            match id {
                Some(val) => {
                    let JobIdAndNotification {
                        job_id,
                        notification_id,
                    } = val;
                    match (job_id, notification_id) {
                        (Some(job_id), Some(notification_id)) => {
                            let job_id: Uuid = job_id.into();
                            let notification_id: Uuid = notification_id.into();

                            let mut jobs = jobs.write().await;
                            jobs.insert(notification_id, job_id);

                            let mut notifications = notifications.write().await;
                            notifications.entry(job_id).or_insert_with(HashMap::new);
                            let job = notifications.get_mut(&job_id);
                            if let Some(job) = job {
                                job.insert(notification_id, data);
                            }

                            Ok(())
                        }
                        _ => Err(JobSchedulerError::UpdateJobData),
                    }
                }
                None => Err(JobSchedulerError::UpdateJobData),
            }
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::pin(async move {
            let job_id = {
                let r = jobs.read().await;
                r.get(&guid).cloned()
            };
            let mut jobs = jobs.write().await;
            match job_id {
                Some(job_id) => {
                    jobs.remove(&guid);
                    let mut notifications = notifications.write().await;
                    let job = notifications.get_mut(&job_id);
                    match job {
                        Some(job) => {
                            job.remove(&guid);
                            if job.is_empty() {
                                notifications.remove(&job_id);
                            }
                            Ok(())
                        }
                        None => Err(JobSchedulerError::CantRemove),
                    }
                }
                None => Err(JobSchedulerError::CantRemove),
            }
        })
    }
}

impl NotificationStore for SimpleNotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job_id: JobId,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NotificationId>, JobSchedulerError>> + Send>> {
        let state: i32 = state.into();
        let notifications = self.data.clone();
        Box::pin(async move {
            let notifications = notifications.read().await;
            let job = notifications.get(&job_id);
            match job {
                Some(job) => Ok(job
                    .iter()
                    .filter_map(|(k, v)| {
                        if v.job_states.contains(&state) {
                            Some(*k)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()),
                None => Ok(vec![]),
            }
        })
    }

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        let notifications = self.data.clone();
        Box::pin(async move {
            let notifications = notifications.read().await;
            let job = notifications.get(&job_id);
            match job {
                Some(job) => Ok(job.iter().map(|(k, _v)| *k).collect::<Vec<_>>()),
                None => Ok(vec![]),
            }
        })
    }

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let state: i32 = state.into();

        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::pin(async move {
            let mut ret = false;
            let job_id = {
                let r = jobs.read().await;
                r.get(&notification_id).cloned()
            };
            let mut jobs = jobs.write().await;
            match job_id {
                Some(job_id) => {
                    let mut notifications = notifications.write().await;
                    let job = notifications.get_mut(&job_id);
                    match job {
                        Some(job) => {
                            if job.contains_key(&notification_id) {
                                let notification = job.get_mut(&notification_id).unwrap();
                                if notification.job_states.contains(&state) {
                                    ret = true;
                                }
                                notification.job_states.retain(|v| *v != state);
                                if notification.job_states.is_empty() {
                                    job.remove(&notification_id);
                                    jobs.remove(&notification_id);
                                }
                            }
                            if job.is_empty() {
                                notifications.remove(&job_id);
                            }
                            Ok(ret)
                        }
                        None => Err(JobSchedulerError::CantRemove),
                    }
                }
                None => Err(JobSchedulerError::CantRemove),
            }
        })
    }

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::pin(async move {
            let mut jobs = jobs.write().await;

            jobs.retain(|_k, v| *v != job_id);

            let mut notifications = notifications.write().await;
            notifications.remove(&job_id);
            Ok(())
        })
    }
}
