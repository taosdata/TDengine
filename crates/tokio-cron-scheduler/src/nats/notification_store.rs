use crate::job::job_data_prost::{
    JobAndNotifications, JobState, ListOfJobsAndNotifications, NotificationData,
};
use crate::job::{JobId, NotificationId};
use crate::nats::{sanitize_nats_key, NatsStore};
use crate::store::{DataStore, InitStore, NotificationStore};
use crate::{JobSchedulerError, JobUuid};
use nats::kv::Store;
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::RwLockReadGuard;
use tracing::error;
use uuid::Uuid;

const LIST_NAME: &str = "TCS_NOTIFICATION_LIST";
const NOTIFICATION_PRE: &str = "NOTIF_";

#[derive(Clone, Default)]
pub struct NatsNotificationStore {
    pub store: NatsStore,
}

fn uuid_to_nats_id(uuid: Uuid) -> String {
    let uuid = NOTIFICATION_PRE.to_string() + &*uuid.to_string();
    sanitize_nats_key(&*uuid)
}

impl DataStore<NotificationData> for NatsNotificationStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<NotificationData>, JobSchedulerError>> + Send>>
    {
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let r = bucket.read().await;
            let id = uuid_to_nats_id(id);
            r.get(&*id)
                .map_err(|e| {
                    error!("Error getting data {:?}", e);
                    JobSchedulerError::GetJobData
                })
                .map(|v| v.and_then(|v| NotificationData::decode(v.as_slice()).ok()))
        })
    }

    fn add_or_update(
        &mut self,
        data: NotificationData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
        let notification_id: Uuid = data
            .job_id
            .as_ref()
            .and_then(|j| j.notification_id.as_ref())
            .unwrap()
            .into();

        let job_id: Uuid = data
            .job_id
            .as_ref()
            .and_then(|j| j.job_id.as_ref())
            .unwrap()
            .into();

        let get = self.get(notification_id);
        let add_to_list = self.add_to_list_of_guids(job_id, notification_id);
        Box::pin(async move {
            let bucket = bucket.read().await;
            let bytes = data.encode_to_vec();
            let prev = get.await;
            let uuid = uuid_to_nats_id(notification_id);
            let done = match prev {
                Ok(Some(_)) => bucket.put(&*uuid, bytes),
                Ok(None) => bucket.create(&*uuid, bytes),
                Err(e) => {
                    error!("Error getting existing value {:?}, assuming does not exist and hope for the best", e);
                    bucket.create(&*uuid, bytes)
                }
            };
            let added = add_to_list.await;
            match (done, added) {
                (Ok(_), Ok(_)) => Ok(()),
                _ => Err(JobSchedulerError::CantAdd),
            }
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
        let removed_from_list = self.remove_from_list(guid);
        Box::pin(async move {
            let bucket = bucket.read().await;
            let guid = uuid_to_nats_id(guid);

            let deleted = bucket.delete(&*guid);
            let removed_from_list = removed_from_list.await;

            match (deleted, removed_from_list) {
                (Ok(_), Ok(_)) => Ok(()),
                _ => Err(JobSchedulerError::CantRemove),
            }
        })
    }
}

impl InitStore for NatsNotificationStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        Box::pin(async move {
            // Nop
            // That being said. Would've been better to do the connection startup here.
            Ok(())
        })
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let inited = self.store.inited;
        Box::pin(async move { Ok(inited) })
    }
}

impl NotificationStore for NatsNotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: JobId,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NotificationId>, JobSchedulerError>> + Send>> {
        let list_of_notification_guids = self.list_notification_guids_for_job_id(job);
        let bucket = self.store.bucket.clone();
        let state = state as i32;
        Box::pin(async move {
            let list_of_notification_guids = list_of_notification_guids.await;
            if let Err(e) = list_of_notification_guids {
                error!("Could not get list of guids {:?}", e);
                return Err(e);
            }
            let list_of_notification_guids = list_of_notification_guids.unwrap();
            let bucket = bucket.read().await;
            let notification_ids = list_of_notification_guids
                .iter()
                .filter_map(|s| {
                    let notification_id = *s;
                    bucket
                        .get(&*uuid_to_nats_id(notification_id))
                        .ok()
                        .flatten()
                        .and_then(|b| NotificationData::decode(b.as_slice()).ok())
                        .filter(|nd| nd.job_states.contains(&state))
                        .map(|_| notification_id)
                })
                .collect::<Vec<_>>();
            Ok(notification_ids)
        })
    }

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        let list_guids = self.list_guids();
        Box::pin(async move {
            let list_guids = list_guids.await;
            if let Err(e) = list_guids {
                error!("Error getting {:?}", e);
                return Err(e);
            }
            let list_guids = list_guids.unwrap();
            let list = list_guids
                .job_and_notifications
                .iter()
                .flat_map(|j| {
                    j.job_id
                        .as_ref()
                        .filter(|id| {
                            let id: Uuid = JobUuid {
                                id1: id.id1,
                                id2: id.id2,
                            }
                            .into();
                            id == job_id
                        })
                        .map(|_i| {
                            j.notification_ids
                                .iter()
                                .map(|n| {
                                    let n: Uuid = n.into();
                                    n
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(list)
        })
    }

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let get = self.get(notification_id);
        let mut self_clone = self.clone();

        Box::pin(async move {
            let data = get.await;
            let mut data = match data {
                Ok(Some(get)) => get,
                Ok(None) => {
                    error!("Notification not found {:?}", notification_id);
                    return Err(JobSchedulerError::CantRemove);
                }
                Err(e) => {
                    error!("Error getting notification {:?}", e);
                    return Err(e);
                }
            };
            let state = state as i32;

            let mut deleted = false;
            data.job_states.retain(|s| {
                let ret = *s != state;
                deleted |= !ret;
                ret
            });

            if data.job_states.is_empty() {
                // Need to delete
                let delete = self_clone.delete(notification_id).await;
                if let Err(e) = delete {
                    error!("Could not delete notification {:?}", e);
                    return Err(e);
                }
                let delete = self_clone.remove_from_list(notification_id).await;
                delete.map(|_| true)
            } else {
                // Need to update
                self_clone.add_or_update(data).await.map(|_| deleted)
            }
        })
    }

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let list_guids = self.list_guids();
        let mut self_clone = self.clone();
        Box::pin(async move {
            let list_guids = list_guids.await;
            if let Err(e) = list_guids {
                error!("Error getting list of guids {:?}", e);
                return Err(e);
            }
            let list_guids = list_guids.unwrap();
            let notifications = list_guids
                .job_and_notifications
                .into_iter()
                .filter(|l| {
                    l.job_id
                        .as_ref()
                        .map(|u| {
                            let u: Uuid = u.into();
                            u == job_id
                        })
                        .is_some()
                })
                .flat_map(|l: JobAndNotifications| l.notification_ids)
                .map(|u| {
                    let u: Uuid = u.into();
                    u
                });
            for notification_id in notifications {
                let deleted = self_clone.delete(notification_id).await;
                if let Err(e) = deleted {
                    error!("Error deleting notification {:?}", notification_id);
                    return Err(e);
                }
            }
            Ok(())
        })
    }
}

impl NatsNotificationStore {
    fn list_guids(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ListOfJobsAndNotifications, JobSchedulerError>> + Send>>
    {
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let r = bucket.read().await;
            let list = r.get(&*sanitize_nats_key(LIST_NAME));
            match list {
                Ok(Some(list)) => {
                    let list = list.as_slice();
                    ListOfJobsAndNotifications::decode(list).map_err(|e| {
                        error!("Error decoding list value {:?}", e);
                        JobSchedulerError::CantListGuids
                    })
                }
                Ok(None) => Ok(ListOfJobsAndNotifications::default()),
                Err(e) => {
                    error!("Error getting list of guids {:?}", e);
                    Err(JobSchedulerError::CantListGuids)
                }
            }
        })
    }

    fn add_to_list_of_guids(
        &self,
        job_id: JobId,
        notification_id: NotificationId,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let list = self.list_guids();
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let list = list.await;
            if let Err(e) = list {
                error!("Could not get list of guids {:?}", e);
                return Err(JobSchedulerError::ErrorLoadingGuidList);
            }
            let mut list = list.unwrap();
            let mut job_found = false;
            for job in list.job_and_notifications.iter_mut() {
                let this_job = job
                    .job_id
                    .as_ref()
                    .filter(|j| {
                        let j: Uuid = JobUuid {
                            id1: j.id1,
                            id2: j.id2,
                        }
                        .into();
                        j == job_id
                    })
                    .is_some();
                if this_job {
                    job_found = true;
                    let contains = job.notification_ids.iter().any(|u| {
                        let u: Uuid = JobUuid {
                            id1: u.id1,
                            id2: u.id2,
                        }
                        .into();
                        u == notification_id
                    });
                    if !contains {
                        let notification: JobUuid = notification_id.into();
                        job.notification_ids.push(notification)
                    }
                }
            }
            if !job_found {
                let job_id: JobUuid = job_id.into();
                let notification_id: JobUuid = notification_id.into();
                list.job_and_notifications.push(JobAndNotifications {
                    job_id: Some(job_id),
                    notification_ids: vec![notification_id],
                });
            }

            let bucket = bucket.read().await;
            NatsNotificationStore::update_list(bucket, list)
        })
    }

    fn update_list(
        bucket: RwLockReadGuard<Store>,
        list: ListOfJobsAndNotifications,
    ) -> Result<(), JobSchedulerError> {
        let has_list_already = bucket
            .get(&*sanitize_nats_key(LIST_NAME))
            .ok()
            .flatten()
            .is_some();
        if has_list_already {
            bucket.put(&*sanitize_nats_key(LIST_NAME), list.encode_to_vec())
        } else {
            bucket.create(&*sanitize_nats_key(LIST_NAME), list.encode_to_vec())
        }
        .map(|_| ())
        .map_err(|e| {
            error!("Error saving list of guids {:?}", e);
            JobSchedulerError::CantAdd
        })
    }

    fn remove_from_list(
        &self,
        uuid: NotificationId,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let list = self.list_guids();
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let list = list.await;
            if let Err(e) = list {
                error!("Could not get list of guids {:?}", e);
                return Err(JobSchedulerError::ErrorLoadingGuidList);
            }
            let mut list = list.unwrap();
            let mut exists = false;
            for job_and_notifications in list.job_and_notifications.iter_mut() {
                job_and_notifications.notification_ids.retain(|n| {
                    let n: Uuid = n.into();
                    let retain = n != uuid;
                    exists |= !retain;
                    retain
                });
            }
            if !exists {
                return Ok(());
            }
            let bucket = bucket.read().await;
            NatsNotificationStore::update_list(bucket, list)
        })
    }
}
