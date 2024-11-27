use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;
mod metadata_store;
mod notification_store;

pub use metadata_store::MetaDataStorage;
pub use notification_store::NotificationStore;

pub trait InitStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>>;
    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>>;
}

pub trait DataStore<DATA>
where
    DATA: Sized,
{
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DATA>, JobSchedulerError>> + Send>>;

    fn add_or_update(
        &mut self,
        data: DATA,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>>;

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>>;
}

pub trait CodeGet<CODE>
where
    CODE: Sized,
{
    fn get(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<Pin<Box<CODE>>, JobSchedulerError>>>;
    fn notify_on_add(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
    fn notify_on_delete(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}
