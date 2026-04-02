use crate::job::job_data_prost::ListOfUuids;
use crate::nats::{sanitize_nats_key, NatsStore};
use crate::store::{DataStore, InitStore, MetaDataStorage};
use crate::{JobAndNextTick, JobSchedulerError, JobStoredData, JobUuid};
use chrono::{DateTime, Utc};
use nats::kv::Store;
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::RwLockReadGuard;
use tracing::error;
use uuid::Uuid;

const LIST_NAME: &str = "TCS_JOB_LIST";
const METADATA_PRE: &str = "META_";

///
/// A Nats KV store backed metadata store
#[derive(Clone, Default)]
pub struct NatsMetadataStore {
    pub store: NatsStore,
}

fn uuid_to_nats_id(uuid: Uuid) -> String {
    let uuid = METADATA_PRE.to_string() + &*uuid.to_string();
    sanitize_nats_key(&*uuid)
}

impl DataStore<JobStoredData> for NatsMetadataStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobStoredData>, JobSchedulerError>> + Send>>
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
                .map(|v| v.and_then(|v| JobStoredData::decode(v.as_slice()).ok()))
        })
    }

    fn add_or_update(
        &mut self,
        data: JobStoredData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
        let uuid: Uuid = data.id.as_ref().unwrap().into();
        let get = self.get(uuid);
        let add_to_list = self.add_to_list_of_guids(uuid);
        Box::pin(async move {
            let bucket = bucket.read().await;
            let bytes = data.encode_to_vec();
            let prev = get.await;
            let uuid = uuid_to_nats_id(uuid);
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

impl InitStore for NatsMetadataStore {
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

impl MetaDataStorage for NatsMetadataStore {
    fn list_next_ticks(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>> + Send>> {
        let list_guids = self.list_guids();
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let list = list_guids.await;
            if let Err(e) = list {
                error!("Error getting list of guids {:?}", e);
                return Err(e);
            }
            let list = list.unwrap();
            let bucket = bucket.read().await;
            let list = list
                .uuids
                .iter()
                .map(|uuid| {
                    let uuid: Uuid = uuid.into();
                    uuid
                })
                .flat_map(|uuid| bucket.get(&*uuid_to_nats_id(uuid)))
                .flatten()
                .flat_map(|buf| JobStoredData::decode(buf.as_slice()))
                .map(|jd| JobAndNextTick {
                    id: jd.id,
                    job_type: jd.job_type,
                    next_tick: jd.next_tick,
                    last_tick: jd.last_tick,
                })
                .collect::<Vec<_>>();
            Ok(list)
        })
    }

    fn set_next_and_last_tick(
        &mut self,
        guid: Uuid,
        next_tick: Option<DateTime<Utc>>,
        last_tick: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let get = self.get(guid);
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let get = get.await;
            match get {
                Ok(Some(mut val)) => {
                    val.next_tick = match next_tick {
                        Some(next_tick) => next_tick.timestamp(),
                        None => 0,
                    } as u64;
                    val.last_tick = last_tick.map(|lt| lt.timestamp() as u64);
                    let bytes = val.encode_to_vec();
                    let bucket = bucket.read().await;
                    bucket
                        .put(&*uuid_to_nats_id(guid), bytes)
                        .map(|_| ())
                        .map_err(|e| {
                            error!("Error updating value {:?}", e);
                            JobSchedulerError::UpdateJobData
                        })
                }
                Ok(None) => {
                    error!("Could not get value to update");
                    Err(JobSchedulerError::UpdateJobData)
                }
                Err(e) => {
                    error!("Could not get value to update {:?}", e);
                    Err(JobSchedulerError::UpdateJobData)
                }
            }
        })
    }

    fn time_till_next_job(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Duration>, JobSchedulerError>> + Send>> {
        let list = self.list_guids();
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let list = list.await;
            if let Err(e) = list {
                error!("Could not get list of guids {:?}", e);
                return Err(JobSchedulerError::CantGetTimeUntil);
            }
            let list = list.unwrap();
            let bucket = bucket.read().await;
            let now = Utc::now();
            let now = now.timestamp() as u64;
            let ret = list
                .uuids
                .iter()
                .map(|uuid| {
                    let uuid: Uuid = uuid.into();
                    uuid
                })
                .flat_map(|uuid| bucket.get(&*uuid_to_nats_id(uuid)))
                .flatten()
                .flat_map(|b| JobStoredData::decode(b.as_slice()))
                .filter_map(|jd| match jd.next_tick {
                    0 => None,
                    i => {
                        if i > now {
                            Some(i)
                        } else {
                            None
                        }
                    }
                })
                .min()
                .map(|t| t - now)
                .map(std::time::Duration::from_secs);
            Ok(ret)
        })
    }
}

impl NatsMetadataStore {
    fn list_guids(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ListOfUuids, JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let r = bucket.read().await;
            let list = r.get(&*sanitize_nats_key(LIST_NAME));
            match list {
                Ok(Some(list)) => ListOfUuids::decode(list.as_slice()).map_err(|e| {
                    error!("Error decoding list value {:?}", e);
                    JobSchedulerError::CantListGuids
                }),
                Ok(None) => Ok(ListOfUuids::default()),
                Err(e) => {
                    error!("Error getting list of guids {:?}", e);
                    Err(JobSchedulerError::CantListGuids)
                }
            }
        })
    }

    fn add_to_list_of_guids(
        &self,
        uuid: Uuid,
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
            let exists = list.uuid_in_list(uuid);
            if exists {
                return Ok(());
            }
            let uuid: JobUuid = uuid.into();
            list.uuids.push(uuid);

            let bucket = bucket.read().await;
            NatsMetadataStore::update_list(bucket, list)
        })
    }

    fn update_list(
        bucket: RwLockReadGuard<Store>,
        list: ListOfUuids,
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
        uuid: Uuid,
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
            let exists = list.uuid_in_list(uuid);
            if !exists {
                return Ok(());
            }
            list.uuids.retain(|v| {
                let v: Uuid = v.into();
                v != uuid
            });
            let bucket = bucket.read().await;
            NatsMetadataStore::update_list(bucket, list)
        })
    }
}
