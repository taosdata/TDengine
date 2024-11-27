#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobState, JobType};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobState, JobType};
use crate::job_scheduler::JobsSchedulerLocked;
use crate::{JobScheduler, JobSchedulerError, JobStoredData};
use chrono::{DateTime, Offset, TimeZone, Utc};
use cron_job::CronJob;
use croner::Cron;
use non_cron_job::NonCronJob;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::oneshot::Receiver;
use tracing::error;
use uuid::Uuid;

mod builder;
mod creator;
mod cron_job;
mod deleter;
#[cfg(not(feature = "has_bytes"))]
pub mod job_data;
#[cfg(feature = "has_bytes")]
pub mod job_data_prost;
mod non_cron_job;
mod runner;
pub mod to_code;

use crate::notification::{NotificationCreator, NotificationDeleter};
use crate::JobSchedulerError::ParseSchedule;
pub use builder::JobBuilder;
pub use creator::JobCreator;
pub use deleter::JobDeleter;
pub use runner::JobRunner;

pub type JobId = Uuid;
pub type NotificationId = Uuid;

pub type JobToRun = dyn FnMut(JobId, JobsSchedulerLocked) + Send + Sync;
pub type JobToRunAsync =
    dyn FnMut(JobId, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

pub type OnJobNotification = dyn FnMut(JobId, NotificationId, JobState) -> Pin<Box<dyn Future<Output = ()> + Send>>
    + Send
    + Sync;

fn nop(_uuid: Uuid, _jobs: JobsSchedulerLocked) {
    // Do nothing
}

fn nop_async(_uuid: Uuid, _jobs: JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {})
}

///
/// A schedulable Job
#[derive(Clone)]
pub struct JobLocked(pub(crate) Arc<RwLock<Box<dyn Job + Send + Sync>>>);

pub trait Job {
    fn is_cron_job(&self) -> bool;
    fn schedule(&self) -> Option<Cron>;
    fn repeated_every(&self) -> Option<u64>;
    fn last_tick(&self) -> Option<DateTime<Utc>>;
    fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>);
    fn next_tick(&self) -> Option<DateTime<Utc>>;
    fn set_next_tick(&mut self, tick: Option<DateTime<Utc>>);
    fn set_count(&mut self, count: u32);
    fn count(&self) -> u32;
    fn increment_count(&mut self);
    fn job_id(&self) -> Uuid;
    fn job_type(&self) -> JobType;
    fn ran(&self) -> bool;
    fn set_ran(&mut self, ran: bool);
    fn stop(&self) -> bool;
    fn set_stopped(&mut self);
    fn set_started(&mut self);
    fn job_data_from_job(&mut self) -> Result<Option<JobStoredData>, JobSchedulerError>;
    fn set_job_data(&mut self, job_data: JobStoredData) -> Result<(), JobSchedulerError>;
    fn run(&mut self, jobs: JobScheduler) -> Receiver<bool>;
    fn fixed_offset_west(&self) -> i32;
}

impl JobLocked {
    /// Create a new cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         });
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new<S, T>(schedule: S, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
        S: ToString,
    {
        Self::new_tz(schedule, Utc, run)
    }

    /// Create a new cron job at a timezone.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         });
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_tz<S, T, TZ>(schedule: S, timezone: TZ, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
        S: ToString,
        TZ: TimeZone,
    {
        let schedule = Self::schedule_to_cron(schedule)?;
        let time_offset_seconds = timezone
            .offset_from_utc_datetime(&Utc::now().naive_local())
            .fix()
            .local_minus_utc();
        let schedule = Cron::new(&schedule)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
            .map_err(|_| ParseSchedule)?;
        let job_id = Uuid::new_v4();
        Ok(Self(Arc::new(RwLock::new(Box::new(CronJob {
            data: JobStoredData {
                id: Some(job_id.into()),
                last_updated: None,
                last_tick: None,
                next_tick: schedule
                    .iter_from(Utc::now().with_timezone(&timezone))
                    .next()
                    .map(|t| t.timestamp() as u64)
                    .unwrap_or(0),
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
            run: Box::new(run),
            run_async: Box::new(nop_async),
            async_job: false,
        })))))
    }

    /// Create a new async cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new_async("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| Box::pin( async move {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_async<S, T>(schedule: S, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
        S: ToString,
    {
        Self::new_async_tz(schedule, Utc, run)
    }

    /// Create a new async cron job at a timezone.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new_async_tz("0 15 6,8,10 * Mar,Jun Fri 2017", Utc, |_uuid, _lock| Box::pin( async move {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_async_tz<S, T, TZ>(
        schedule: S,
        timezone: TZ,
        run: T,
    ) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
        S: ToString,
        TZ: TimeZone,
    {
        let schedule = Self::schedule_to_cron(schedule)?;
        let time_offset_seconds = timezone
            .offset_from_utc_datetime(&Utc::now().naive_local())
            .fix()
            .local_minus_utc();
        let schedule = Cron::new(&schedule)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
            .map_err(|_| ParseSchedule)?;
        let job_id = Uuid::new_v4();
        Ok(Self(Arc::new(RwLock::new(Box::new(CronJob {
            data: JobStoredData {
                id: Some(job_id.into()),
                last_updated: None,
                last_tick: None,
                next_tick: schedule
                    .iter_from(Utc::now().with_timezone(&timezone))
                    .next()
                    .map(|t| t.timestamp() as u64)
                    .unwrap_or(0),
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
            run: Box::new(nop),
            run_async: Box::new(run),
            async_job: true,
        })))))
    }

    /// Create a new cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new_cron_job("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         });
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_cron_job<S, T, E>(schedule: S, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
        S: ToString,
    {
        JobLocked::new(schedule, run)
    }

    /// Create a new async cron job using UTC as timezone.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| Box::pin( async move {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_cron_job_async<S, T>(schedule: S, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
        S: ToString,
    {
        JobLocked::new_async_tz(schedule, Utc, run)
    }

    /// Create a new async cron job using a specified timezone.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", Utc, |_uuid, _lock| Box::pin( async move {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_cron_job_async_tz<S, T, TZ>(
        schedule: S,
        timezone: TZ,
        run: T,
    ) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
        S: ToString,
        TZ: TimeZone,
    {
        JobLocked::new_async_tz(schedule, timezone, run)
    }

    fn make_one_shot_job(
        duration: Duration,
        run: Box<JobToRun>,
        run_async: Box<JobToRunAsync>,
        async_job: bool,
    ) -> Result<Self, JobSchedulerError> {
        let id = Uuid::new_v4();
        let job = NonCronJob {
            run,
            run_async,
            async_job,
            data: JobStoredData {
                id: Some(id.into()),
                last_updated: None,
                last_tick: None,
                next_tick: SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|p| p.as_secs())
                    .unwrap_or(0)
                    + duration.as_secs(),
                job_type: JobType::OneShot.into(),
                count: 0,
                extra: vec![],
                ran: false,
                stopped: false,
                #[cfg(feature = "has_bytes")]
                job: Some(job_data_prost::job_stored_data::Job::NonCronJob(
                    job_data_prost::NonCronJob {
                        repeating: false,
                        repeated_every: duration.as_secs(),
                    },
                )),
                #[cfg(not(feature = "has_bytes"))]
                job: Some(job_data::job_stored_data::Job::NonCronJob(
                    job_data::NonCronJob {
                        repeating: false,
                        repeated_every: duration.as_secs(),
                    },
                )),
                time_offset_seconds: 0,
            },
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> =
            Arc::new(RwLock::new(Box::new(job)));

        Ok(Self(job))
    }

    /// Create a new one shot job.
    ///
    /// This will schedule a job that is only run once after the duration has passed.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_one_shot(Duration::from_secs(18), |_uuid, _l| {
    ///            println!("{:?} I'm only run once", chrono::Utc::now());
    ///        }
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    /// Above will run the code after 18 seconds, only once
    pub fn new_one_shot<T>(duration: Duration, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::make_one_shot_job(duration, Box::new(run), Box::new(nop_async), false)
    }

    /// Create a new async one shot job.
    ///
    /// This will schedule a job that is only run once after the duration has passed.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    ///
    ///  let job = Job::new_one_shot_async(Duration::from_secs(16), |_uuid, _l| {
    ///             Box::pin(async move {
    ///                 info!("I'm only run once async");
    ///             })
    ///         })
    ///         .unwrap();
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    /// Above will run the code after 18 seconds, only once
    pub fn new_one_shot_async<T>(duration: Duration, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
    {
        JobLocked::make_one_shot_job(duration, Box::new(nop), Box::new(run), true)
    }

    fn make_new_one_shot_at_an_instant(
        instant: Instant,
        run: Box<JobToRun>,
        run_async: Box<JobToRunAsync>,
        async_job: bool,
    ) -> Result<Self, JobSchedulerError> {
        let id = Uuid::new_v4();

        let job = NonCronJob {
            run,
            run_async,
            async_job,
            data: JobStoredData {
                id: Some(id.into()),
                last_updated: None,
                last_tick: None,
                next_tick: chrono::Utc::now()
                    .checked_add_signed(chrono::Duration::seconds(
                        instant.duration_since(Instant::now()).as_secs() as i64,
                    ))
                    .map(|t| t.timestamp() as u64)
                    .unwrap_or(0),
                job_type: JobType::OneShot.into(),
                count: 0,
                extra: vec![],
                ran: false,
                stopped: false,
                #[cfg(feature = "has_bytes")]
                job: Some(job_data_prost::job_stored_data::Job::NonCronJob(
                    job_data_prost::NonCronJob {
                        repeating: false,
                        repeated_every: instant.duration_since(Instant::now()).as_secs(),
                    },
                )),
                #[cfg(not(feature = "has_bytes"))]
                job: Some(job_data::job_stored_data::Job::NonCronJob(
                    job_data::NonCronJob {
                        repeating: false,
                        repeated_every: instant.duration_since(Instant::now()).as_secs(),
                    },
                )),
                time_offset_seconds: 0,
            },
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> =
            Arc::new(RwLock::new(Box::new(job)));

        Ok(Self(job))
    }

    /// Create a new one shot job that runs at an instant
    ///
    /// ```rust,ignore
    /// // Run after 20 seconds
    /// let mut sched = JobScheduler::new();
    /// let instant = std::time::Instant::now().checked_add(std::time::Duration::from_secs(20));
    /// let job = Job::new_one_shot_at_instant(instant, |_uuid, _lock| println!("I run once after 20 seconds") );
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_one_shot_at_instant<T>(instant: Instant, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::make_new_one_shot_at_an_instant(
            instant,
            Box::new(run),
            Box::new(nop_async),
            false,
        )
    }

    /// Create a new async one shot job that runs at an instant
    ///
    /// ```rust,ignore
    /// // Run after 20 seconds
    /// let mut sched = JobScheduler::new();
    /// let instant = std::time::Instant::now().checked_add(std::time::Duration::from_secs(20));
    /// let job = Job::new_one_shot_at_instant(instant, |_uuid, _lock| Box::pin(async move {println!("I run once after 20 seconds")}) );
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_one_shot_at_instant_async<T>(
        instant: Instant,
        run: T,
    ) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
    {
        JobLocked::make_new_one_shot_at_an_instant(instant, Box::new(nop), Box::new(run), true)
    }

    fn make_new_repeated(
        duration: Duration,
        run: Box<JobToRun>,
        run_async: Box<JobToRunAsync>,
        async_job: bool,
    ) -> Result<Self, JobSchedulerError> {
        let id = Uuid::new_v4();
        let job = NonCronJob {
            run,
            run_async,
            async_job,
            data: JobStoredData {
                id: Some(id.into()),
                last_updated: None,
                last_tick: None,
                next_tick: chrono::Utc::now()
                    .checked_add_signed(chrono::Duration::seconds(duration.as_secs() as i64))
                    .map(|t| t.timestamp() as u64)
                    .unwrap_or(0),
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
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> =
            Arc::new(RwLock::new(Box::new(job)));

        Ok(Self(job))
    }

    /// Create a new repeated job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_repeated(Duration::from_secs(8), |_uuid, _lock| {
    ///     println!("{:?} I'm repeated every 8 seconds", chrono::Utc::now());
    /// }
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_repeated<T>(duration: Duration, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::make_new_repeated(duration, Box::new(run), Box::new(nop_async), false)
    }

    /// Create a new async repeated job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_repeated_async(Duration::from_secs(8), |_uuid, _lock| Box::pin(async move {
    ///     println!("{:?} I'm repeated every 8 seconds", chrono::Utc::now());
    /// }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_repeated_async<T>(duration: Duration, run: T) -> Result<Self, JobSchedulerError>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
    {
        JobLocked::make_new_repeated(duration, Box::new(nop), Box::new(run), true)
    }

    ///
    /// The `tick` method returns a true if there was an invocation needed after it was last called
    /// This method will also change the last tick on itself
    pub fn tick(&mut self) -> Result<bool, JobSchedulerError> {
        let now = Utc::now();
        let (job_type, last_tick, next_tick, schedule, repeated_every, ran, count) = {
            let r = self.0.read().map_err(|_| JobSchedulerError::TickError)?;
            (
                r.job_type(),
                r.last_tick(),
                r.next_tick(),
                r.schedule(),
                r.repeated_every(),
                r.ran(),
                r.count(),
            )
        };

        // Don't bother processing a cancelled job
        if next_tick.is_none() {
            return Err(JobSchedulerError::NoNextTick);
        }

        let must_run = match (last_tick.as_ref(), next_tick.as_ref(), job_type) {
            (None, Some(next_tick), JobType::OneShot) => {
                let now_to_next = now.cmp(next_tick);
                matches!(now_to_next, std::cmp::Ordering::Greater)
                    || matches!(now_to_next, std::cmp::Ordering::Equal)
            }
            (None, Some(next_tick), JobType::Repeated) => {
                let now_to_next = now.cmp(next_tick);
                matches!(now_to_next, std::cmp::Ordering::Greater)
                    || matches!(now_to_next, std::cmp::Ordering::Equal)
            }
            (Some(last_tick), Some(next_tick), _) => {
                let now_to_next = now.cmp(next_tick);
                let last_to_next = last_tick.cmp(next_tick);

                (matches!(now_to_next, std::cmp::Ordering::Greater)
                    || matches!(now_to_next, std::cmp::Ordering::Equal))
                    && (matches!(last_to_next, std::cmp::Ordering::Less)
                        || matches!(last_to_next, std::cmp::Ordering::Equal))
            }
            _ => false,
        };

        let next_tick = if must_run {
            match job_type {
                JobType::Cron => schedule.and_then(|s| s.iter_after(now).next()),
                JobType::OneShot => None,
                JobType::Repeated => repeated_every.and_then(|r| {
                    next_tick
                        .and_then(|nt| nt.checked_add_signed(chrono::Duration::seconds(r as i64)))
                }),
            }
        } else {
            next_tick
        };
        let last_tick = Some(now);

        let job_data = self.job_data();
        if let Err(e) = job_data {
            error!("Could not get job data");
            return Err(e);
        }

        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::JobTick)?;
            w.set_next_tick(next_tick);
            w.set_last_tick(last_tick);
            w.set_ran(ran || must_run);
            let count = if must_run {
                if count == u32::MAX {
                    0
                } else {
                    count + 1
                }
            } else {
                count
            };
            w.set_count(count);
        }

        Ok(must_run)
    }

    ///
    /// Get the GUID for the job
    ///
    pub fn guid(&self) -> Uuid {
        let r = self.0.read().unwrap();
        r.job_id()
    }

    ///
    /// Add a notification to run on a list of state notifications
    pub async fn on_notifications_add(
        &self,
        job_scheduler: &JobsSchedulerLocked,
        run: Box<OnJobNotification>,
        states: Vec<JobState>,
    ) -> Result<Uuid, JobSchedulerError> {
        if !job_scheduler.inited().await {
            let mut job_scheduler = job_scheduler.clone();
            job_scheduler.init().await?;
        }
        let job_id = self.guid();
        let context = job_scheduler.context();
        NotificationCreator::add(&context, run, states, &job_id).await
    }

    ///
    /// Run something when the task is started. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_start_notification_remove`.
    pub async fn on_start_notification_add(
        &self,
        job_scheduler: &JobsSchedulerLocked,
        on_start: Box<OnJobNotification>,
    ) -> Result<Uuid, JobSchedulerError> {
        self.on_notifications_add(job_scheduler, on_start, vec![JobState::Started])
            .await
    }

    ///
    /// Remove a notification optionally for a certain type of states
    pub async fn on_notification_removal(
        &self,
        job_scheduler: &JobsSchedulerLocked,
        notification_id: &Uuid,
        states: Option<Vec<JobState>>,
    ) -> Result<(NotificationId, bool), JobSchedulerError> {
        if !job_scheduler.inited().await {
            let mut job_scheduler = job_scheduler.clone();
            job_scheduler.init().await?;
        }
        let context = job_scheduler.context();
        NotificationDeleter::remove(&context, notification_id, states)
    }

    ///
    /// Remove the notification when the task was started. Uses the same UUID that was returned by
    /// `on_start_notification_add`
    pub async fn on_start_notification_remove(
        &self,
        job_scheduler: &JobsSchedulerLocked,
        notification_id: &Uuid,
    ) -> Result<bool, JobSchedulerError> {
        self.on_notification_removal(
            job_scheduler,
            notification_id,
            Some(vec![JobState::Started]),
        )
        .await
        .map(|(_, deleted)| deleted)
    }

    ///
    /// Run something when the task is stopped. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_stop_notification_remove`.
    pub async fn on_done_notification_add(
        &mut self,
        job_scheduler: &JobsSchedulerLocked,
        on_stop: Box<OnJobNotification>,
    ) -> Result<Uuid, JobSchedulerError> {
        self.on_notifications_add(job_scheduler, on_stop, vec![JobState::Done])
            .await
    }

    ///
    /// Remove the notification when the task was stopped. Uses the same UUID that was returned by
    /// `on_done_notification_add`
    pub async fn on_done_notification_remove(
        &mut self,
        job_scheduler: &JobsSchedulerLocked,
        notification_id: &Uuid,
    ) -> Result<bool, JobSchedulerError> {
        self.on_notification_removal(job_scheduler, notification_id, Some(vec![JobState::Done]))
            .await
            .map(|(_, deleted)| deleted)
    }

    ///
    /// Run something when the task was removed. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_removed_notification_remove`.
    pub async fn on_removed_notification_add(
        &mut self,
        job_scheduler: &JobsSchedulerLocked,
        on_removed: Box<OnJobNotification>,
    ) -> Result<Uuid, JobSchedulerError> {
        self.on_notifications_add(job_scheduler, on_removed, vec![JobState::Removed])
            .await
    }

    ///
    /// Remove the notification when the task was removed. Uses the same UUID that was returned by
    /// `on_removed_notification_add`
    pub async fn on_removed_notification_remove(
        &mut self,
        job_scheduler: &JobsSchedulerLocked,
        notification_id: &Uuid,
    ) -> Result<bool, JobSchedulerError> {
        self.on_notification_removal(
            job_scheduler,
            notification_id,
            Some(vec![JobState::Removed]),
        )
        .await
        .map(|(_, deleted)| deleted)
    }

    ///
    /// Run something when the task was removed. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_removed_notification_remove`.
    pub async fn on_stop_notification_add(
        &mut self,
        job_scheduler: &JobsSchedulerLocked,
        on_removed: Box<OnJobNotification>,
    ) -> Result<Uuid, JobSchedulerError> {
        self.on_notifications_add(job_scheduler, on_removed, vec![JobState::Stop])
            .await
    }

    ///
    /// Remove the notification when the task was removed. Uses the same UUID that was returned by
    /// `on_removed_notification_add`
    pub async fn on_stop_notification_remove(
        &mut self,
        job_scheduler: &JobsSchedulerLocked,
        notification_id: &Uuid,
    ) -> Result<bool, JobSchedulerError> {
        self.on_notification_removal(job_scheduler, notification_id, Some(vec![JobState::Stop]))
            .await
            .map(|(_, deleted)| deleted)
    }

    ///
    /// Override the job's data for use in data storage
    pub fn set_job_data(&mut self, job_data: JobStoredData) -> Result<(), JobSchedulerError> {
        let mut w = self
            .0
            .write()
            .map_err(|_| JobSchedulerError::UpdateJobData)?;
        w.set_job_data(job_data)
    }

    ///
    /// Set whether this job has been stopped
    pub fn set_stop(&mut self, stop: bool) -> Result<(), JobSchedulerError> {
        let mut w = self
            .0
            .write()
            .map_err(|_| JobSchedulerError::UpdateJobData)?;
        if stop {
            w.set_stopped();
        } else {
            w.set_started();
        }
        Ok(())
    }

    ///
    /// Get the job data
    pub fn job_data(&mut self) -> Result<JobStoredData, JobSchedulerError> {
        let mut w = self.0.write().map_err(|_| JobSchedulerError::GetJobData)?;
        match w.job_data_from_job() {
            Ok(Some(job_data)) => Ok(job_data),
            _ => Err(JobSchedulerError::GetJobData),
        }
    }

    #[cfg(not(feature = "english"))]
    pub fn schedule_to_cron<T: ToString>(schedule: T) -> Result<String, JobSchedulerError> {
        Ok(schedule.to_string())
    }

    #[cfg(feature = "english")]
    pub fn schedule_to_cron<T: ToString>(schedule: T) -> Result<String, JobSchedulerError> {
        let schedule = schedule.to_string();
        match Cron::new(&schedule)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
        {
            Ok(_) => Ok(schedule),
            Err(_) => match english_to_cron::str_cron_syntax(&schedule) {
                Ok(english_to_cron) => {
                    if english_to_cron != schedule {
                        if english_to_cron == "0 * * * * ? *" {
                            Err(ParseSchedule)
                        } else {
                            // english-to-cron adds the year field which we can't put off (currently)
                            let cron = english_to_cron
                                .split(' ')
                                .take(6)
                                .collect::<Vec<_>>()
                                .join(" ");
                            Ok(cron)
                        }
                    } else {
                        Ok(schedule)
                    }
                }
                Err(_) => Err(ParseSchedule),
            },
        }
    }
}
