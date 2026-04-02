use crate::context::{Context, NotificationDeletedResult};
#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::JobState;
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::JobState;
use crate::job::{JobId, NotificationId};
use crate::store::NotificationStore;
use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::error;

#[derive(Default)]
pub struct NotificationDeleter {}

impl NotificationDeleter {
    async fn listen_to_job_removals(
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        mut rx_job_delete: Receiver<JobId>,
        tx_notification_deleted: Sender<NotificationDeletedResult>,
    ) {
        loop {
            let val = rx_job_delete.recv().await;
            if let Err(e) = val {
                error!("Error receiving delete jobs {:?}", e);
                break;
            }
            let job_id = val.unwrap();
            let mut storage = storage.write().await;
            let guids = storage.list_notification_guids_for_job_id(job_id).await;
            if let Err(e) = guids {
                error!("Error with getting guids for job id {:?}", e);
                continue;
            }
            let guids = guids.unwrap();
            // TODO first check for removal callback
            for notification_id in guids {
                if let Err(e) = storage.delete(notification_id).await {
                    error!("Error deleting notification {:?}", e);
                    continue;
                }
                if let Err(e) = tx_notification_deleted.send(Ok((notification_id, true, None))) {
                    error!("Error sending deletion {:?}", e);
                    continue;
                }
            }
        }
    }

    async fn listen_for_notification_removals(
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        mut rx: Receiver<(NotificationId, Option<Vec<JobState>>)>,
        tx_deleted: Sender<NotificationDeletedResult>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving notification removals {:?}", e);
                break;
            }
            let (uuid, states) = val.unwrap();
            {
                let mut storage = storage.write().await;
                if let Some(states) = states {
                    for state in states {
                        let delete = storage.delete_notification_for_state(uuid, state).await;
                        if let Err(e) = delete {
                            error!("Error deleting notification for state {:?}", e);
                            continue;
                        }
                        let delete = delete.unwrap();
                        if let Err(e) = tx_deleted.send(Ok((uuid, delete, Some(vec![state])))) {
                            error!("Error sending notification deleted state {:?}", e);
                        }
                    }
                } else {
                    let w = storage.delete(uuid).await;
                    if let Err(e) = w {
                        error!("Error deleting notification for all states {:?}", e);
                        continue;
                    }
                    if let Err(e) = tx_deleted.send(Ok((uuid, true, None))) {
                        error!("Error sending {:?}", e);
                    }
                }
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let rx_job_delete = context.job_delete_tx.subscribe();
        let rx_notification_delete = context.notify_delete_tx.subscribe();
        let tx_notification_deleted = context.notify_deleted_tx.clone();
        let storage = context.notification_storage.clone();

        Box::pin(async move {
            tokio::spawn(NotificationDeleter::listen_to_job_removals(
                storage.clone(),
                rx_job_delete,
                tx_notification_deleted.clone(),
            ));
            tokio::spawn(NotificationDeleter::listen_for_notification_removals(
                storage,
                rx_notification_delete,
                tx_notification_deleted,
            ));
            Ok(())
        })
    }

    pub fn remove(
        context: &Context,
        notification_id: &NotificationId,
        states: Option<Vec<JobState>>,
    ) -> Result<(NotificationId, bool), JobSchedulerError> {
        let notification_id = *notification_id;
        let delete_tx = context.notify_delete_tx.clone();
        let mut deleted_rx = context.notify_deleted_tx.subscribe();
        let (tx, rx) = std::sync::mpsc::channel();

        tokio::spawn(async move {
            tokio::spawn(async move {
                if let Err(e) = delete_tx.send((notification_id, states)) {
                    error!("Error sending notification removal {:?}", e);
                }
            });
            while let Ok(val) = deleted_rx.recv().await {
                match val {
                    Ok((uuid, deleted, _)) => {
                        if uuid == notification_id {
                            if let Err(e) = tx.send(Ok((uuid, deleted))) {
                                error!("Error sending notification removal success {:?}", e);
                            }
                            break;
                        }
                    }
                    Err((e, Some(uuid))) => {
                        if uuid == notification_id {
                            if let Err(e) = tx.send(Err(e)) {
                                error!("Error sending removal error {:?}", e);
                            }
                            break;
                        }
                    }
                    _ => {}
                }
            }
        });
        let ret = rx.recv();
        match ret {
            Ok(ret) => ret,
            Err(e) => {
                error!("Error getting result from notification removal {:?}", e);
                Err(JobSchedulerError::CantRemove)
            }
        }
    }
}
