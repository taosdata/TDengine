#[cfg(not(feature = "has_bytes"))]
#[macro_use]
extern crate num_derive;
extern crate core;

mod context;
mod error;
mod job;
mod job_scheduler;
#[cfg(feature = "nats_storage")]
mod nats;
mod notification;
#[cfg(feature = "postgres_storage")]
mod postgres;
mod scheduler;
mod simple;
mod store;

use std::ops::Add;
use std::time::{Duration, SystemTime};

#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::ListOfUuids;
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::ListOfUuids;
use chrono::{DateTime, Utc};
use croner::Cron;
#[cfg(not(feature = "has_bytes"))]
use job::job_data::{JobAndNextTick, JobStoredData, Uuid as JobUuid};
#[cfg(feature = "has_bytes")]
use job::job_data_prost::{JobAndNextTick, JobStoredData, Uuid as JobUuid};
use uuid::Uuid;

#[cfg(feature = "nats_storage")]
pub use crate::nats::{NatsMetadataStore, NatsNotificationStore, NatsStore, NatsStoreBuilder};

#[cfg(feature = "postgres_storage")]
pub use crate::postgres::{PostgresMetadataStore, PostgresNotificationStore, PostgresStore};

pub use context::Context;
pub use error::JobSchedulerError;
#[cfg(not(feature = "has_bytes"))]
pub use job::job_data::JobState as JobNotification;
#[cfg(feature = "has_bytes")]
pub use job::job_data_prost::JobState as JobNotification;
pub use job::to_code::{JobCode, NotificationCode, PinnedGetFuture, ToCode};
pub use job::JobLocked as Job;
pub use job::OnJobNotification;
pub use job::{JobBuilder, JobToRun, JobToRunAsync};
pub use job_scheduler::JobsSchedulerLocked as JobScheduler;
pub use store::{MetaDataStorage, NotificationStore};

pub use simple::{
    SimpleJobCode, SimpleMetadataStore, SimpleNotificationCode, SimpleNotificationStore,
};

impl JobUuid {
    pub fn from_u128(uuid: u128) -> Self {
        let id1 = (uuid >> 64) as u64;
        let id2 = (uuid & 0xFFFF_FFFF_FFFF_FFFF) as u64;
        Self { id1, id2 }
    }

    pub fn as_u128(&self) -> u128 {
        ((self.id1 as u128) << 64) + (self.id2 as u128)
    }
}

impl From<Uuid> for JobUuid {
    fn from(uuid: Uuid) -> Self {
        JobUuid::from_u128(uuid.as_u128())
    }
}

impl From<&Uuid> for JobUuid {
    fn from(uuid: &Uuid) -> Self {
        JobUuid::from_u128(uuid.as_u128())
    }
}

impl From<JobUuid> for Uuid {
    fn from(uuid: JobUuid) -> Self {
        Uuid::from_u128(uuid.as_u128())
    }
}

impl From<&JobUuid> for Uuid {
    fn from(uuid: &JobUuid) -> Self {
        Uuid::from_u128(uuid.as_u128())
    }
}

impl JobAndNextTick {
    pub fn utc(lt: u64) -> DateTime<Utc> {
        let dt = SystemTime::UNIX_EPOCH.add(Duration::from_secs(lt));
        let dt: DateTime<Utc> = DateTime::from(dt);
        dt
    }

    fn next_tick_utc(&self) -> Option<DateTime<Utc>> {
        match self.next_tick {
            0 => None,
            val => Some(JobAndNextTick::utc(val)),
        }
    }

    fn last_tick_utc(&self) -> Option<DateTime<Utc>> {
        self.last_tick.map(JobAndNextTick::utc)
    }
}

impl JobStoredData {
    pub fn schedule(&self) -> Option<Cron> {
        self.job
            .as_ref()
            .and_then(|j| match j {
                #[cfg(feature = "has_bytes")]
                job::job_data_prost::job_stored_data::Job::CronJob(cj) => Some(&*cj.schedule),
                #[cfg(not(feature = "has_bytes"))]
                job::job_data::job_stored_data::Job::CronJob(cj) => Some(&*cj.schedule),
                _ => None,
            })
            .and_then(|s| {
                Cron::new(s)
                    .with_seconds_required()
                    .with_dom_and_dow()
                    .parse()
                    .ok()
            })
    }

    pub fn next_tick_utc(&self) -> Option<DateTime<Utc>> {
        match self.next_tick {
            0 => None,
            val => Some(JobAndNextTick::utc(val)),
        }
    }

    pub fn last_tick_utc(&self) -> Option<DateTime<Utc>> {
        self.last_tick.map(JobAndNextTick::utc)
    }

    pub fn repeated_every(&self) -> Option<u64> {
        self.job.as_ref().and_then(|jt| match jt {
            #[cfg(feature = "has_bytes")]
            job::job_data_prost::job_stored_data::Job::CronJob(_) => None,
            #[cfg(not(feature = "has_bytes"))]
            job::job_data::job_stored_data::Job::CronJob(_) => None,
            #[cfg(feature = "has_bytes")]
            job::job_data_prost::job_stored_data::Job::NonCronJob(ncj) => Some(ncj.repeated_every),
            #[cfg(not(feature = "has_bytes"))]
            job::job_data::job_stored_data::Job::NonCronJob(ncj) => Some(ncj.repeated_every),
        })
    }

    pub fn set_next_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.next_tick = match tick {
            Some(t) => t.timestamp() as u64,
            None => 0,
        }
    }

    pub fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.last_tick = tick.map(|t| t.timestamp() as u64);
    }
}

impl ListOfUuids {
    // Allowing dead code for non-Nats library users.
    #[allow(dead_code)]
    pub fn uuid_in_list(&self, uuid: Uuid) -> bool {
        self.uuids
            .iter()
            .map(|uuid| {
                let uuid: Uuid = uuid.into();
                uuid
            })
            .any(|val| val == uuid)
    }
}
