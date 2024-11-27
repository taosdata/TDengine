#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobAndNextTick, JobStoredData};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobAndNextTick, JobStoredData};
use crate::job::JobToRunAsync;
use crate::store::{CodeGet, DataStore, InitStore};
use crate::JobSchedulerError;
use chrono::{DateTime, Utc};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

pub trait MetaDataStorage: DataStore<JobStoredData> + InitStore {
    fn list_next_ticks(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>> + Send>>;
    fn set_next_and_last_tick(
        &mut self,
        guid: Uuid,
        next_tick: Option<DateTime<Utc>>,
        last_tick: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>>;
    fn time_till_next_job(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<std::time::Duration>, JobSchedulerError>> + Send>>;
}

pub trait JobCodeGet: CodeGet<Box<JobToRunAsync>> {}
