#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobState, NotificationData};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobState, NotificationData};
use crate::job::to_code::{JobCode, NotificationCode};
use crate::job::{JobToRunAsync, NotificationId};
use crate::store::{MetaDataStorage, NotificationStore};
use crate::{JobSchedulerError, JobStoredData, OnJobNotification};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use uuid::Uuid;

pub type NotificationDeletedResult =
    Result<(Uuid, bool, Option<Vec<JobState>>), (JobSchedulerError, Option<NotificationId>)>;

pub struct Context {
    pub job_activation_tx: Sender<Uuid>,
    pub notify_tx: Sender<(Uuid, JobState)>,
    pub job_create_tx: Sender<(JobStoredData, Arc<RwLock<Box<JobToRunAsync>>>)>,
    pub job_created_tx: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    pub job_delete_tx: Sender<Uuid>,
    pub job_deleted_tx: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    pub notify_create_tx: Sender<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,
    pub notify_created_tx: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    pub notify_delete_tx: Sender<(Uuid, Option<Vec<JobState>>)>,
    pub notify_deleted_tx: Sender<NotificationDeletedResult>,
    // TODO need to add when notification was deleted and there's no more references to it
    pub metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
    pub notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
    pub job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
    pub notification_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
}

impl Context {
    pub fn new(
        metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
        notification_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
    ) -> Self {
        let (job_activation_tx, _job_activation_rx) = tokio::sync::broadcast::channel(200);
        let (notify_tx, _notify_rx) = tokio::sync::broadcast::channel(200);
        let (job_create_tx, _job_create_rx) = tokio::sync::broadcast::channel(200);
        let (job_created_tx, _job_created_rx) = tokio::sync::broadcast::channel(200);
        let (job_delete_tx, _job_delete_rx) = tokio::sync::broadcast::channel(200);
        let (job_deleted_tx, _job_deleted_rx) = tokio::sync::broadcast::channel(200);
        let (notify_create_tx, _notify_create_rx) = tokio::sync::broadcast::channel(200);
        let (notify_created_tx, _notify_created_rx) = tokio::sync::broadcast::channel(200);
        let (notify_delete_tx, _notify_delete_rx) = tokio::sync::broadcast::channel(200);
        let (notify_deleted_tx, _notify_deleted_rx) = tokio::sync::broadcast::channel(200);

        Self {
            job_activation_tx,
            notify_tx,
            job_create_tx,
            job_created_tx,
            job_delete_tx,
            job_deleted_tx,
            notify_create_tx,
            notify_created_tx,
            notify_delete_tx,
            notify_deleted_tx,
            metadata_storage,
            notification_storage,
            job_code,
            notification_code,
        }
    }

    pub fn new_with_channel_size(
        metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
        notification_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
        channel_size: usize,
    ) -> Self {
        let (job_activation_tx, _job_activation_rx) = tokio::sync::broadcast::channel(channel_size);
        let (notify_tx, _notify_rx) = tokio::sync::broadcast::channel(channel_size);
        let (job_create_tx, _job_create_rx) = tokio::sync::broadcast::channel(channel_size);
        let (job_created_tx, _job_created_rx) = tokio::sync::broadcast::channel(channel_size);
        let (job_delete_tx, _job_delete_rx) = tokio::sync::broadcast::channel(channel_size);
        let (job_deleted_tx, _job_deleted_rx) = tokio::sync::broadcast::channel(channel_size);
        let (notify_create_tx, _notify_create_rx) = tokio::sync::broadcast::channel(channel_size);
        let (notify_created_tx, _notify_created_rx) = tokio::sync::broadcast::channel(channel_size);
        let (notify_delete_tx, _notify_delete_rx) = tokio::sync::broadcast::channel(channel_size);
        let (notify_deleted_tx, _notify_deleted_rx) = tokio::sync::broadcast::channel(channel_size);

        Self {
            job_activation_tx,
            notify_tx,
            job_create_tx,
            job_created_tx,
            job_delete_tx,
            job_deleted_tx,
            notify_create_tx,
            notify_created_tx,
            notify_delete_tx,
            notify_deleted_tx,
            metadata_storage,
            notification_storage,
            job_code,
            notification_code,
        }
    }
}

impl Clone for Context {
    fn clone(&self) -> Self {
        Self {
            job_activation_tx: self.job_activation_tx.clone(),
            notify_tx: self.notify_tx.clone(),
            job_create_tx: self.job_create_tx.clone(),
            job_created_tx: self.job_created_tx.clone(),
            job_delete_tx: self.job_delete_tx.clone(),
            job_deleted_tx: self.job_deleted_tx.clone(),
            notify_create_tx: self.notify_create_tx.clone(),
            notify_created_tx: self.notify_created_tx.clone(),
            notify_delete_tx: self.notify_delete_tx.clone(),
            notify_deleted_tx: self.notify_deleted_tx.clone(),
            metadata_storage: self.metadata_storage.clone(),
            notification_storage: self.notification_storage.clone(),
            job_code: self.job_code.clone(),
            notification_code: self.notification_code.clone(),
        }
    }
}
