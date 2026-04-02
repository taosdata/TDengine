use crate::context::Context;
use crate::store::MetaDataStorage;
use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

#[derive(Default)]
pub struct JobDeleter {}

impl JobDeleter {
    async fn listen_to_removals(
        storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        mut rx: Receiver<Uuid>,
        tx_deleted: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                error!("Error receiving value {:?}", e);
                break;
            }
            let uuid = val.unwrap();
            {
                let mut storage = storage.write().await;
                let delete = storage.delete(uuid).await;
                if let Err(e) = delete {
                    error!("Error deleting {:?}", e);
                    if let Err(e) = tx_deleted.send(Err((e, Some(uuid)))) {
                        error!("Error sending delete error {:?}", e);
                    }
                    continue;
                }
            }
            if let Err(e) = tx_deleted.send(Ok(uuid)) {
                error!("Error sending error {:?}", e);
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send + Sync>> {
        let rx = context.job_delete_tx.subscribe();
        let tx_deleted = context.job_deleted_tx.clone();
        let storage = context.metadata_storage.clone();

        Box::pin(async move {
            tokio::spawn(JobDeleter::listen_to_removals(storage, rx, tx_deleted));
            Ok(())
        })
    }

    pub async fn remove(context: &Context, job_id: &Uuid) -> Result<(), JobSchedulerError> {
        let delete = context.job_delete_tx.clone();
        let mut deleted = context.job_deleted_tx.subscribe();

        let job_id = *job_id;
        tokio::spawn(async move {
            if let Err(e) = delete.send(job_id) {
                error!("Error sending delete id {:?}", e);
            }
        });
        while let Ok(deleted) = deleted.recv().await {
            match deleted {
                Ok(uuid) => {
                    if uuid == job_id {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                Err((e, Some(uuid))) => {
                    if uuid == job_id {
                        return Err(e);
                    } else {
                        continue;
                    }
                }
                _ => continue,
            }
        }
        Err(JobSchedulerError::RemoveShutdownNotifier)
    }
}
