use crate::context::{Context, NotificationDeletedResult};
#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobIdAndNotification, JobState, NotificationData};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobIdAndNotification, JobState, NotificationData};
use crate::job::to_code::{JobCode, NotificationCode, ToCode};
use crate::job::JobToRunAsync;
use crate::{JobSchedulerError, JobStoredData, OnJobNotification};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::{error, warn};
use uuid::Uuid;

pub type LockedJobToRunMap = Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<JobToRunAsync>>>>>>;
pub type LockedNotificationToRunMap =
    Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<OnJobNotification>>>>>>;

pub struct SimpleJobCode {
    pub job_code: LockedJobToRunMap,
}

impl Default for SimpleJobCode {
    fn default() -> Self {
        SimpleJobCode {
            job_code: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SimpleJobCode {
    async fn listen_for_additions(
        data: LockedJobToRunMap,
        mut rx: Receiver<(JobStoredData, Arc<RwLock<Box<JobToRunAsync>>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving {:?}", e);
                break;
            }
            let (JobStoredData { id: job_id, .. }, val) = val.unwrap();
            let uuid: Uuid = job_id.as_ref().unwrap().into();
            let mut w = data.write().await;
            w.insert(uuid, val);
        }
    }

    async fn listen_for_removals(
        data: LockedJobToRunMap,
        mut rx: Receiver<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving job removal {:?}", e);
                break;
            }
            let uuid = val.unwrap();
            if let Ok(uuid) = uuid {
                let mut w = data.write().await;
                w.remove(&uuid);
            }
        }
    }
}

impl ToCode<Box<JobToRunAsync>> for SimpleJobCode {
    fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let data = self.job_code.clone();
        let job_create = context.job_create_tx.subscribe();
        let job_deleted = context.job_deleted_tx.subscribe();

        Box::pin(async move {
            tokio::spawn(SimpleJobCode::listen_for_additions(
                data.clone(),
                job_create,
            ));
            tokio::spawn(SimpleJobCode::listen_for_removals(data, job_deleted));
            Ok(())
        })
    }

    fn get(
        &mut self,
        uuid: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<Arc<RwLock<Box<JobToRunAsync>>>>, JobSchedulerError>>
                + Send,
        >,
    > {
        let data = self.job_code.clone();
        Box::pin(async move {
            let r = data.read().await;
            Ok(r.get(&uuid).cloned())
        })
    }
}

impl JobCode for SimpleJobCode {}

pub struct SimpleNotificationCode {
    pub data: LockedNotificationToRunMap,
}

impl Default for SimpleNotificationCode {
    fn default() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SimpleNotificationCode {
    async fn listen_for_additions(
        data: LockedNotificationToRunMap,
        mut rx: Receiver<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,
        tx: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving {:?}", e);
                break;
            }
            let (uuid, val) = val.unwrap();
            let uuid: Uuid = {
                match uuid {
                    NotificationData {
                        job_id:
                            Some(JobIdAndNotification {
                                notification_id: Some(job_id),
                                ..
                            }),
                        ..
                    } => job_id.into(),
                    _ => continue,
                }
            };
            {
                let mut w = data.write().await;
                w.insert(uuid, val);
            }
            if let Err(e) = tx.send(Ok(uuid)) {
                warn!("Error sending notification created {:?} {:?}", e, uuid);
            }
        }
    }

    // TODO check for elsewhere
    async fn listen_for_removals(
        data: LockedNotificationToRunMap,
        mut rx: Receiver<(Uuid, Option<Vec<JobState>>)>,
        tx: Sender<NotificationDeletedResult>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving job removal {:?}", e);
                break;
            }
            let (uuid, states) = val.unwrap();
            error!(
                "Removing notification uuid {:?} and not caring about states!",
                uuid
            );
            {
                let mut w = data.write().await;
                w.remove(&uuid);
            }
            if let Err(e) = tx.send(Ok((uuid, true, states))) {
                error!("Error sending notification removed {:?} {:?}", e, uuid)
            }
        }
    }
}

impl ToCode<Box<OnJobNotification>> for SimpleNotificationCode {
    fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let data = self.data.clone();
        let rx_create = context.notify_create_tx.subscribe();
        let tx_created = context.notify_created_tx.clone();
        let rx_delete = context.notify_delete_tx.subscribe();
        let tx_deleted = context.notify_deleted_tx.clone();

        Box::pin(async move {
            tokio::spawn(SimpleNotificationCode::listen_for_additions(
                data.clone(),
                rx_create,
                tx_created,
            ));
            tokio::spawn(SimpleNotificationCode::listen_for_removals(
                data, rx_delete, tx_deleted,
            ));
            Ok(())
        })
    }

    fn get(
        &mut self,
        uuid: Uuid,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<Arc<RwLock<Box<OnJobNotification>>>>, JobSchedulerError>,
                > + Send,
        >,
    > {
        let data = self.data.clone();
        Box::pin(async move {
            let r = data.read().await;
            Ok(r.get(&uuid).cloned())
        })
    }
}

impl NotificationCode for SimpleNotificationCode {}
