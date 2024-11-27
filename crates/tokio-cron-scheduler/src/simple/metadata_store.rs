#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobAndNextTick, JobStoredData};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobAndNextTick, JobStoredData};
use crate::store::{DataStore, InitStore, MetaDataStorage};
use crate::JobSchedulerError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct SimpleMetadataStore {
    pub data: Arc<RwLock<HashMap<Uuid, JobStoredData>>>,
    pub inited: bool,
}

impl Default for SimpleMetadataStore {
    fn default() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            inited: false,
        }
    }
}

impl DataStore<JobStoredData> for SimpleMetadataStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobStoredData>, JobSchedulerError>> + Send>>
    {
        let data = self.data.clone();
        Box::pin(async move {
            let r = data.write().await;
            let val = r.get(&id).cloned();
            Ok(val)
        })
    }

    fn add_or_update(
        &mut self,
        data: JobStoredData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let id: Uuid = data.id.as_ref().unwrap().into();
        let job_data = self.data.clone();
        Box::pin(async move {
            let mut w = job_data.write().await;
            w.insert(id, data);
            Ok(())
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let job_data = self.data.clone();
        Box::pin(async move {
            let mut w = job_data.write().await;
            w.remove(&guid);
            Ok(())
        })
    }
}

impl InitStore for SimpleMetadataStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        self.inited = true;
        Box::pin(std::future::ready(Ok(())))
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let val = self.inited;
        Box::pin(std::future::ready(Ok(val)))
    }
}

impl MetaDataStorage for SimpleMetadataStore {
    fn list_next_ticks(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>> + Send>> {
        let data = self.data.clone();
        Box::pin(async move {
            let r = data.read().await;
            let ret = r
                .iter()
                .map(|(_, v)| (v.id.clone(), v.next_tick, v.last_tick, v.job_type))
                .map(|(id, next_tick, last_tick, job_type)| JobAndNextTick {
                    id,
                    next_tick,
                    last_tick,
                    job_type,
                })
                .collect::<Vec<_>>();
            Ok(ret)
        })
    }

    fn set_next_and_last_tick(
        &mut self,
        guid: Uuid,
        next_tick: Option<DateTime<Utc>>,
        last_tick: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let data = self.data.clone();
        Box::pin(async move {
            let mut w = data.write().await;
            let val = w.get_mut(&guid);
            match val {
                Some(val) => {
                    val.set_next_tick(next_tick);
                    val.set_last_tick(last_tick);
                    Ok(())
                }
                None => Err(JobSchedulerError::UpdateJobData),
            }
        })
    }

    fn time_till_next_job(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<std::time::Duration>, JobSchedulerError>> + Send>>
    {
        let data = self.data.clone();
        Box::pin(async move {
            let r = data.read().await;
            let now = Utc::now();
            let now = now.timestamp() as u64;
            let val = r
                .iter()
                .filter_map(|(_, jd)| match jd.next_tick {
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
            Ok(val)
        })
    }
}
