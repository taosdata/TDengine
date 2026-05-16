use crate::job::cron_job::CronJob;
#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data;
#[cfg(not(feature = "has_bytes"))]
pub use crate::job::job_data::{JobStoredData, JobType, Uuid};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost;
#[cfg(feature = "has_bytes")]
pub use crate::job::job_data_prost::{JobStoredData, JobType, Uuid};
use crate::job::non_cron_job::NonCronJob;
use crate::job::{nop, nop_async, JobLocked};
use crate::{JobSchedulerError, JobToRun, JobToRunAsync};
use chrono::{DateTime, Offset, TimeZone, Utc};
use core::time::Duration;
use croner::Cron;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use uuid::Uuid as UuidUuid;

pub struct JobBuilder<T>
where
    T: TimeZone,
{
    pub job_id: Option<Uuid>,
    pub timezone: Option<T>,
    pub job_type: Option<JobType>,
    pub schedule: Option<Cron>,
    pub run: Option<Box<JobToRun>>,
    pub run_async: Option<Box<JobToRunAsync>>,
    pub duration: Option<Duration>,
    pub repeating: Option<bool>,
    pub instant: Option<Instant>,
    pub start_at: Option<DateTime<T>>,
}

impl JobBuilder<Utc> {
    pub fn new() -> Self {
        Self {
            job_id: None,
            timezone: None,
            job_type: None,
            schedule: None,
            run: None,
            run_async: None,
            duration: None,
            repeating: None,
            instant: None,
            start_at: None,
        }
    }
}

impl Default for JobBuilder<Utc> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: TimeZone> JobBuilder<T> {
    pub fn with_timezone<U: TimeZone>(self, timezone: U) -> JobBuilder<U> {
        JobBuilder {
            timezone: Some(timezone),
            job_id: self.job_id,
            job_type: self.job_type,
            schedule: self.schedule,
            run: self.run,
            run_async: self.run_async,
            duration: self.duration,
            repeating: self.repeating,
            instant: self.instant,
            start_at: None,
        }
    }

    pub fn start_at(self, dt: DateTime<T>) -> Self {
        Self {
            start_at: Some(dt),
            ..self
        }
    }

    pub fn with_job_id(self, job_id: Uuid) -> Self {
        Self {
            job_id: Some(job_id),
            ..self
        }
    }

    pub fn with_job_type(self, job_type: JobType) -> Self {
        Self {
            job_type: Some(job_type),
            ..self
        }
    }

    pub fn with_cron_job_type(self) -> Self {
        Self {
            job_type: Some(JobType::Cron),
            ..self
        }
    }

    pub fn with_repeated_job_type(self) -> Self {
        Self {
            job_type: Some(JobType::Repeated),
            ..self
        }
    }

    pub fn with_one_shot_job_type(self) -> Self {
        Self {
            job_type: Some(JobType::OneShot),
            ..self
        }
    }

    pub fn with_schedule<TS>(self, schedule: TS) -> Result<Self, JobSchedulerError>
    where
        TS: ToString,
    {
        let schedule = JobLocked::schedule_to_cron(schedule)?;
        let schedule = Cron::new(&schedule)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
            .map_err(|_| JobSchedulerError::ParseSchedule)?;
        Ok(Self {
            schedule: Some(schedule),
            ..self
        })
    }

    pub fn with_run_sync(self, job: Box<JobToRun>) -> Self {
        Self {
            run: Some(Box::new(job)),
            ..self
        }
    }

    pub fn with_run_async(self, job: Box<JobToRunAsync>) -> Self {
        Self {
            run_async: Some(Box::new(job)),
            ..self
        }
    }

    pub fn every_seconds(self, seconds: u64) -> Self {
        Self {
            duration: Some(Duration::from_secs(seconds)),
            repeating: Some(true),
            ..self
        }
    }

    pub fn after_seconds(self, seconds: u64) -> Self {
        Self {
            duration: Some(Duration::from_secs(seconds)),
            repeating: Some(false),
            ..self
        }
    }

    pub fn at_instant(self, instant: Instant) -> Self {
        Self {
            instant: Some(instant),
            ..self
        }
    }

    pub fn build(self) -> Result<JobLocked, JobSchedulerError> {
        if self.job_type.is_none() {
            return Err(JobSchedulerError::JobTypeNotSet);
        }
        let job_type = self.job_type.unwrap();
        let (run, run_async) = (self.run, self.run_async);
        if run.is_none() && run_async.is_none() {
            return Err(JobSchedulerError::RunOrRunAsyncNotSet);
        }
        let async_job = run_async.is_some();

        let time_offset_seconds = if let Some(tz) = self.timezone.as_ref() {
            tz.offset_from_utc_datetime(&Utc::now().naive_local())
                .fix()
                .local_minus_utc()
        } else {
            0
        };

        match job_type {
            JobType::Cron => {
                if self.schedule.is_none() {
                    return Err(JobSchedulerError::ScheduleNotSet);
                }
                let schedule = self.schedule.unwrap();

                Ok(JobLocked(Arc::new(RwLock::new(Box::new(CronJob {
                    data: JobStoredData {
                        id: self.job_id.or(Some(UuidUuid::new_v4().into())),
                        last_updated: None,
                        last_tick: None,
                        next_tick: match &self.timezone {
                            Some(timezone) => schedule
                                .iter_from(Utc::now().with_timezone(&timezone.clone()))
                                .next()
                                .map(|t| t.timestamp() as u64)
                                .unwrap_or(0),
                            None => schedule
                                .iter_from(Utc::now())
                                .next()
                                .map(|t| t.timestamp() as u64)
                                .unwrap_or(0),
                        },
                        job_type: JobType::Cron.into(),
                        count: 0,
                        extra: vec![],
                        ran: false,
                        stopped: false,
                        #[cfg(feature = "has_bytes")]
                        job: Some(job_data_prost::job_stored_data::Job::CronJob(
                            job_data_prost::CronJob {
                                schedule: schedule.pattern.to_string(),
                            },
                        )),
                        #[cfg(not(feature = "has_bytes"))]
                        job: Some(job_data::job_stored_data::Job::CronJob(job_data::CronJob {
                            schedule: schedule.pattern.to_string(),
                        })),
                        time_offset_seconds,
                    },
                    run: run.unwrap_or(Box::new(nop)),
                    run_async: run_async.unwrap_or(Box::new(nop_async)),
                    async_job,
                })))))
            }
            JobType::Repeated => {
                if self.duration.is_none() {
                    return Err(JobSchedulerError::DurationNotSet);
                }
                let duration = self.duration.unwrap();

                let next_tick = match self.start_at {
                    Some(start_at) => {
                        // 如果当前时间已经超过了 start_at 时间，则直接执行
                        if Utc::now() > start_at {
                            Utc::now().timestamp() as u64
                        } else {
                            start_at.timestamp() as u64
                        }
                    }
                    None => now_plus_duration(&duration),
                };

                let job = NonCronJob {
                    data: JobStoredData {
                        id: self.job_id.or(Some(UuidUuid::new_v4().into())),
                        last_updated: None,
                        last_tick: None,
                        next_tick,
                        job_type: JobType::Repeated.into(),
                        count: 0,
                        extra: vec![],
                        ran: false,
                        stopped: false,
                        #[cfg(feature = "has_bytes")]
                        job: Some(job_data_prost::job_stored_data::Job::NonCronJob(
                            job_data_prost::NonCronJob {
                                repeating: true,
                                repeated_every: duration.as_secs(),
                            },
                        )),
                        #[cfg(not(feature = "has_bytes"))]
                        job: Some(job_data::job_stored_data::Job::NonCronJob(
                            job_data::NonCronJob {
                                repeating: true,
                                repeated_every: duration.as_secs(),
                            },
                        )),
                        time_offset_seconds: 0,
                    },
                    run: run.unwrap_or(Box::new(nop)),
                    run_async: run_async.unwrap_or(Box::new(nop_async)),
                    async_job,
                };

                Ok(JobLocked(Arc::new(RwLock::new(Box::new(job)))))
            }
            JobType::OneShot => Err(JobSchedulerError::NoNextTick),
        }
    }
}

fn now_plus_duration(duration: &Duration) -> u64 {
    Utc::now()
        .checked_add_signed(chrono::Duration::seconds(duration.as_secs() as i64))
        .map(|t| t.timestamp() as u64)
        .unwrap_or(0)
}
