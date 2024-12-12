use crate::context::Context;
#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::JobState;
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::JobState;
use crate::job::to_code::NotificationCode;
use crate::store::NotificationStore;
use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

#[derive(Default)]
pub struct NotificationRunner {}

impl NotificationRunner {
    async fn listen_for_activations(
        code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
        mut rx: Receiver<(Uuid, JobState)>,
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving value {:?}", e);
                if matches!(e, RecvError::Closed) {
                    break;
                }
                continue;
            }
            let (job_id, state) = val.unwrap();
            let mut storage = storage.write().await;
            let notifications = storage
                .list_notification_guids_for_job_and_state(job_id, state)
                .await;
            if let Err(_e) = notifications {
                error!(
                    "Error getting the list of notifications guids for job {:?} and state {:?}",
                    job_id, state
                );
                continue;
            }
            let notifications = notifications.unwrap();
            let mut code = code.write().await;
            for notification_id in notifications {
                let code = code.get(notification_id).await;
                match code {
                    Ok(Some(code)) => {
                        let code = code.clone();
                        tokio::spawn(async move {
                            let mut code = code.write().await;
                            (code)(job_id, notification_id, state).await;
                        });
                    }
                    _ => {
                        error!(
                            " nCould not get notification code for {:?}",
                            notification_id
                        );
                        continue;
                    }
                }
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let code = context.notification_code.clone();
        let rx = context.notify_tx.subscribe();
        let storage = context.notification_storage.clone();

        Box::pin(async move {
            tokio::spawn(NotificationRunner::listen_for_activations(
                code, rx, storage,
            ));
            Ok(())
        })
    }
}
