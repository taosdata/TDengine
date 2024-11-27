use crate::context::Context;
use crate::error::JobSchedulerError;
use crate::job::to_code::{JobCode, NotificationCode};
use crate::job::{JobCreator, JobDeleter, JobLocked, JobRunner};
use crate::notification::{NotificationCreator, NotificationDeleter, NotificationRunner};
use crate::scheduler::Scheduler;
use crate::simple::{
    SimpleJobCode, SimpleMetadataStore, SimpleNotificationCode, SimpleNotificationStore,
};
use crate::store::{MetaDataStorage, NotificationStore};
use chrono::{DateTime, Utc};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(all(unix, feature = "signal"))]
use tokio::signal::unix::SignalKind;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

pub type ShutdownNotification = Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>;

/// The JobScheduler contains and executes the scheduled jobs.
pub struct JobsSchedulerLocked {
    pub context: Arc<Context>,
    pub inited: Arc<AtomicBool>,
    pub job_creator: Arc<RwLock<JobCreator>>,
    pub job_deleter: Arc<RwLock<JobDeleter>>,
    pub job_runner: Arc<RwLock<JobRunner>>,
    pub notification_creator: Arc<RwLock<NotificationCreator>>,
    pub notification_deleter: Arc<RwLock<NotificationDeleter>>,
    pub notification_runner: Arc<RwLock<NotificationRunner>>,
    pub scheduler: Arc<RwLock<Scheduler>>,
    pub shutdown_notifier: Arc<RwLock<Option<ShutdownNotification>>>,
}

impl Clone for JobsSchedulerLocked {
    fn clone(&self) -> Self {
        JobsSchedulerLocked {
            context: self.context.clone(),
            inited: self.inited.clone(),
            job_creator: self.job_creator.clone(),
            job_deleter: self.job_deleter.clone(),
            job_runner: self.job_runner.clone(),
            notification_creator: self.notification_creator.clone(),
            notification_deleter: self.notification_deleter.clone(),
            notification_runner: self.notification_runner.clone(),
            scheduler: self.scheduler.clone(),
            shutdown_notifier: self.shutdown_notifier.clone(),
        }
    }
}

impl JobsSchedulerLocked {
    async fn init_context(
        metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
        notify_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
        channel_size: usize,
    ) -> Result<Arc<Context>, JobSchedulerError> {
        {
            let mut metadata_storage = metadata_storage.write().await;
            metadata_storage.init().await?;
        }
        {
            let mut notification_storage = notification_storage.write().await;
            notification_storage.init().await?;
        }
        let context = Context::new_with_channel_size(
            metadata_storage,
            notification_storage,
            job_code.clone(),
            notify_code.clone(),
            channel_size,
        );
        {
            let mut job_code = job_code.write().await;
            job_code.init(&context).await?;
        }
        {
            let mut notification_code = notify_code.write().await;
            notification_code.init(&context).await?;
        }
        Ok(Arc::new(context))
    }

    async fn init_actors(self) -> Result<(), JobSchedulerError> {
        let for_job_runner = self.clone();
        let Self {
            context,
            job_creator,
            job_deleter,
            job_runner,
            notification_creator,
            notification_deleter,
            notification_runner,
            scheduler,
            ..
        } = self;

        {
            let job_creator = job_creator.write().await;
            job_creator.init(&context).await?;
        }

        {
            let mut job_deleter = job_deleter.write().await;
            job_deleter.init(&context).await?;
        }

        {
            let mut notification_creator = notification_creator.write().await;
            notification_creator.init(&context).await?;
        }

        {
            let mut notification_deleter = notification_deleter.write().await;
            notification_deleter.init(&context).await?;
        }

        {
            let mut notification_runner = notification_runner.write().await;
            notification_runner.init(&context).await?;
        }

        {
            let mut runner = job_runner.write().await;
            runner.init(&context, for_job_runner).await?;
        }

        {
            let mut scheduler = scheduler.write().await;
            scheduler.init(&context).await;
        }

        Ok(())
    }

    ///
    /// Get whether the scheduler is initialized
    pub async fn inited(&self) -> bool {
        self.inited.load(Ordering::Relaxed)
    }

    ///
    /// Initialize the actors
    pub async fn init(&mut self) -> Result<(), JobSchedulerError> {
        if self.inited().await {
            return Ok(());
        }
        self.inited.swap(true, Ordering::Relaxed);
        self.clone()
            .init_actors()
            .await
            .map_err(|_| JobSchedulerError::CantInit)
    }

    ///
    /// Create a new `MetaDataStorage` and `NotificationStore` using the `SimpleMetadataStore`, `SimpleNotificationStore`,
    /// `SimpleJobCode` and `SimpleNotificationCode` implementation with channel size of 200
    pub async fn new() -> Result<Self, JobSchedulerError> {
        Self::new_with_channel_size(200).await
    }

    ///
    /// Create a new `MetaDataStorage` and `NotificationStore` using the `SimpleMetadataStore`, `SimpleNotificationStore`,
    /// `SimpleJobCode` and `SimpleNotificationCode` implementation
    ///
    /// The channel_size parameter is used to set the size of the channels used to communicate between the actors.
    /// The amount in short affects how many messages can be buffered before the sender is blocked.
    /// When the sender is blocked, the processing is lagged.
    pub async fn new_with_channel_size(channel_size: usize) -> Result<Self, JobSchedulerError> {
        let metadata_storage = SimpleMetadataStore::default();
        let metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(metadata_storage)));

        let notification_storage = SimpleNotificationStore::default();
        let notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(notification_storage)));

        let job_code = SimpleJobCode::default();
        let job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(job_code)));

        let notify_code = SimpleNotificationCode::default();
        let notify_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(notify_code)));

        let context = JobsSchedulerLocked::init_context(
            metadata_storage,
            notification_storage,
            job_code,
            notify_code,
            channel_size,
        )
        .await
        .map_err(|_| JobSchedulerError::CantInit)?;

        let val = JobsSchedulerLocked {
            context,
            inited: Arc::new(AtomicBool::new(false)),
            job_creator: Arc::new(Default::default()),
            job_deleter: Arc::new(Default::default()),
            job_runner: Arc::new(Default::default()),
            notification_creator: Arc::new(Default::default()),
            notification_deleter: Arc::new(Default::default()),
            notification_runner: Arc::new(Default::default()),
            scheduler: Arc::new(Default::default()),
            shutdown_notifier: Default::default(),
        };

        Ok(val)
    }

    ///
    /// Create a new `JobsSchedulerLocked` using custom metadata and notification runners, job and notification
    /// code providers
    pub async fn new_with_storage_and_code(
        metadata_storage: Box<dyn MetaDataStorage + Send + Sync>,
        notification_storage: Box<dyn NotificationStore + Send + Sync>,
        job_code: Box<dyn JobCode + Send + Sync>,
        notification_code: Box<dyn NotificationCode + Send + Sync>,
        channel_size: usize,
    ) -> Result<Self, JobSchedulerError> {
        let metadata_storage = Arc::new(RwLock::new(metadata_storage));
        let notification_storage = Arc::new(RwLock::new(notification_storage));
        let job_code = Arc::new(RwLock::new(job_code));
        let notification_code = Arc::new(RwLock::new(notification_code));

        let context = JobsSchedulerLocked::init_context(
            metadata_storage,
            notification_storage,
            job_code,
            notification_code,
            channel_size,
        )
        .await?;

        let val = JobsSchedulerLocked {
            context,
            inited: Arc::new(AtomicBool::new(false)),
            job_creator: Arc::new(Default::default()),
            job_deleter: Arc::new(Default::default()),
            job_runner: Arc::new(Default::default()),
            notification_creator: Arc::new(Default::default()),
            notification_deleter: Arc::new(Default::default()),
            notification_runner: Arc::new(Default::default()),
            scheduler: Arc::new(Default::default()),
            shutdown_notifier: Default::default(),
        };

        Ok(val)
    }

    /// Add a job to the `JobScheduler`
    ///
    /// ```rust,ignore
    /// use tokio_cron_scheduler::{Job, JobScheduler, JobToRun};
    /// let mut sched = JobScheduler::new();
    /// sched.add(Job::new("1/10 * * * * *".parse().unwrap(), || {
    ///     println!("I get executed every 10 seconds!");
    /// })).await;
    /// ```
    pub async fn add(&self, job: JobLocked) -> Result<Uuid, JobSchedulerError> {
        let guid = job.guid();
        if !self.inited().await {
            info!("Uninited");
            let mut s = self.clone();
            s.init().await?;
        }

        let context = self.context.clone();
        JobCreator::add(&context, job).await?;
        info!("Job creator created");

        Ok(guid)
    }

    /// Remove a job from the `JobScheduler`
    ///
    /// ```rust,ignore
    /// use tokio_cron_scheduler::{Job, JobScheduler, JobToRun};
    /// let mut sched = JobScheduler::new();
    /// let job_id = sched.add(Job::new("1/10 * * * * *".parse().unwrap(), || {
    ///     println!("I get executed every 10 seconds!");
    /// }))?.await;
    /// sched.remove(job_id).await;
    /// ```
    ///
    /// Note, the UUID of the job can be fetched calling .guid() on a Job.
    ///
    pub async fn remove(&self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError> {
        if !self.inited().await {
            let mut s = self.clone();
            s.init().await?;
        }

        let context = self.context();
        JobDeleter::remove(&context, to_be_removed).await
    }

    pub async fn is_running(&self, job_id: Uuid) -> Result<bool, JobSchedulerError> {
        if !self.inited().await {
            let mut s = self.clone();
            s.init().await?;
        }

        let mut metadata = self.context.metadata_storage.write().await;
        let jm = metadata.get(job_id).await?;
        match jm {
            Some(job_metadata) => {
                if job_metadata.stopped {
                    return Ok(false);
                }
                if job_metadata.ran {
                    return Ok(true);
                }
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    pub(crate) async fn set_ran(&self, job_id: Uuid, ran: bool) -> Result<(), JobSchedulerError> {
        if !self.inited().await {
            let mut s = self.clone();
            s.init().await?;
        }

        let mut metadata = self.context.metadata_storage.write().await;
        let jm = metadata.get(job_id).await?;
        if let Some(mut job_metadata) = jm {
            job_metadata
                .last_updated
                .replace(Utc::now().timestamp() as u64);
            job_metadata.ran = ran;
            metadata.add_or_update(job_metadata).await?;
        }
        Ok(())
    }

    /// The `start` spawns a Tokio task where it loops. Every 500ms it
    /// runs the tick method to increment any pending jobs.
    ///
    /// ```rust,ignore
    /// if let Err(e) = sched.start().await {
    ///         eprintln!("Error on scheduler {:?}", e);
    ///     }
    /// ```
    pub async fn start(&self) -> Result<(), JobSchedulerError> {
        if !self.inited().await {
            let mut s = self.clone();
            s.init().await?;
        }
        let mut scheduler = self.scheduler.write().await;
        let ret = scheduler.start().await;

        match ret {
            Ok(ret) => Ok(ret),
            Err(e) => {
                error!("Error receiving start result {:?}", e);
                Err(JobSchedulerError::StartScheduler)
            }
        }
    }

    /// The `time_till_next_job` method returns the duration till the next job
    /// is supposed to run. This can be used to sleep until then without waking
    /// up at a fixed interval.AsMut
    /// ```
    pub async fn time_till_next_job(
        &mut self,
    ) -> Result<Option<std::time::Duration>, JobSchedulerError> {
        if !self.inited().await {
            let mut s = self.clone();
            s.init().await?;
        }
        let metadata = self.context.metadata_storage.clone();

        let mut metadata = metadata.write().await;
        let ret = metadata.time_till_next_job().await;

        match ret {
            Ok(ret) => Ok(ret),
            Err(e) => {
                error!("Error getting return of time till next job {:?}", e);
                Err(JobSchedulerError::CantGetTimeUntil)
            }
        }
    }

    /// `next_tick_for_job` returns the date/time for when the next tick will
    /// be for a job
    pub async fn next_tick_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, JobSchedulerError> {
        if !self.inited().await {
            let mut s = self.clone();
            s.init().await?;
        }
        let mut r = self.context.metadata_storage.write().await;
        r.get(job_id).await.map(|v| {
            if let Some(vv) = v {
                if vv.next_tick == 0 {
                    return None;
                }
                DateTime::from_timestamp(vv.next_tick as i64, 0)
            } else {
                None
            }
        })
    }

    ///
    /// Shut the scheduler down
    pub async fn shutdown(&mut self) -> Result<(), JobSchedulerError> {
        let mut scheduler = self.scheduler.write().await;
        scheduler.shutdown().await;
        let notify = { self.shutdown_notifier.write().await.take() };
        if let Some(notify) = notify {
            notify.await;
        }
        Ok(())
    }

    ///
    /// Wait for a signal to shut the runtime down with
    #[cfg(all(unix, feature = "signal"))]
    pub fn shutdown_on_signal(&self, signal: SignalKind) {
        let mut l = self.clone();
        tokio::spawn(async move {
            if let Some(_k) = tokio::signal::unix::signal(signal)
                .expect("Can't wait for signal")
                .recv()
                .await
            {
                l.shutdown().await.expect("Problem shutting down");
            }
        });
    }

    ///
    /// Wait for a signal to shut the runtime down with
    #[cfg(feature = "signal")]
    pub fn shutdown_on_ctrl_c(&self) {
        let mut l = self.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Could not await ctrl-c");

            if let Err(err) = l.shutdown().await {
                error!("{:?}", err);
            }
        });
    }

    ///
    /// Code that is run after the shutdown was run
    pub async fn set_shutdown_handler(
        &mut self,
        job: impl Future<Output = ()> + Send + Sync + 'static,
    ) {
        self.shutdown_notifier.write().await.replace(Box::pin(job));
    }

    ///
    /// Remove the shutdown handler
    pub async fn remove_shutdown_handler(&mut self) {
        self.shutdown_notifier.write().await.take();
    }

    ///
    /// Get the context
    pub fn context(&self) -> Arc<Context> {
        self.context.clone()
    }
}
