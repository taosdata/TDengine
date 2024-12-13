use crate::job::job_data_prost::{JobIdAndNotification, JobState, NotificationData};
use crate::job::{JobId, NotificationId};
use crate::store::{DataStore, InitStore, NotificationStore};
use crate::{JobSchedulerError, PostgresStore};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

const MAIN_TABLE: &str = "notification";
const STATES_TABLE: &str = "notification_state";

#[derive(Clone)]
pub struct PostgresNotificationStore {
    pub store: Arc<RwLock<PostgresStore>>,
    pub init_tables: bool,
    pub table: String,
    pub states_table: String,
}

impl Default for PostgresNotificationStore {
    fn default() -> Self {
        let init_tables = std::env::var("POSTGRES_INIT_NOTIFICATIONS")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or_default();
        let table = std::env::var("POSTGRES_NOTIFICATION_TABLE")
            .unwrap_or_else(|_| MAIN_TABLE.to_lowercase());
        let states_table = std::env::var("POSTGRES_NOTIFICATION_STATES_TABLE")
            .unwrap_or_else(|_| STATES_TABLE.to_lowercase());
        let store = Arc::new(RwLock::new(PostgresStore::default()));
        Self {
            init_tables,
            table,
            states_table,
            store,
        }
    }
}

impl DataStore<NotificationData> for PostgresNotificationStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<NotificationData>, JobSchedulerError>> + Send>>
    {
        let store = self.store.clone();
        let table = self.table.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::GetJobData),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let sql =
                        "SELECT id, job_id, extra from ".to_string() + &*table + " where id = $1";
                    let row = store.query(&*sql, &[&id]).await;
                    if let Err(e) = row {
                        error!("Error fetching notification data {:?}", e);
                        return Err(JobSchedulerError::GetJobData);
                    }
                    let row = row.unwrap();
                    let row = row.get(0);
                    if matches!(row, None) {
                        return Ok(None);
                    }
                    let row = row.unwrap();
                    let notification_id: Uuid = row.get(0);

                    let job_states = {
                        let sql =
                            "SELECT state from ".to_string() + &*states_table + " where id = $1";
                        let row = store.query(&*sql, &[&notification_id]).await;
                        match row {
                            Ok(rows) => rows
                                .iter()
                                .map(|row| {
                                    let val: i32 = row.get(0);
                                    val
                                })
                                .collect::<Vec<_>>(),
                            Err(e) => {
                                error!("Error getting states {:?}", e);
                                vec![]
                            }
                        }
                    };

                    let job_id: Uuid = row.get(1);

                    let job_id = JobIdAndNotification {
                        job_id: Some(job_id.into()),
                        notification_id: Some(notification_id.into()),
                    };

                    let extra = row.get(2);
                    let job_id = Some(job_id);
                    Ok(Some(NotificationData {
                        job_id,
                        job_states,
                        extra,
                    }))
                }
            }
        })
    }

    fn add_or_update(
        &mut self,
        data: NotificationData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::UpdateJobData),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let (job_id, notification_id) =
                        match data.job_id_and_notification_id_from_data() {
                            Some((job_id, notification_id)) => (job_id, notification_id),
                            None => return Err(JobSchedulerError::UpdateJobData),
                        };
                    let sql = "DELETE FROM ".to_string() + &*states_table + " WHERE id = $1";
                    let result = store.query(&*sql, &[&notification_id]).await;
                    if let Err(e) = result {
                        error!("Error deleting {:?}", e);
                    }

                    let sql = "INSERT INTO ".to_string()
                        + &*table
                        + " (id, job_id, extra) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT (id) \
                    DO \
                        UPDATE \
                        SET \
                            job_id = $2, extra = $3";
                    let extra = data.extra;
                    let result = store
                        .query(&*sql, &[&notification_id, &job_id, &extra])
                        .await;

                    if let Err(e) = result {
                        error!("Error doing the upsert {:?}", e);
                    }

                    if !data.job_states.is_empty() {
                        let sql = "INSERT INTO ".to_string()
                            + &*states_table
                            + " (id, state) VALUES "
                            + &*data
                                .job_states
                                .iter()
                                .map(|s| format!("($1, {})", s))
                                .collect::<Vec<_>>()
                                .join(",");
                        let result = store.query(&sql, &[&notification_id]).await;
                        if let Err(e) = result {
                            error!("Error inserting state vals {:?}", e);
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::CantRemove),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let sql = "DELETE FROM ".to_string() + &*table + " WHERE id = $1";
                    store.query(&*sql, &[&guid]).await.map(|_| ()).map_err(|e| {
                        error!("Error deleting notification {:?}", e);
                        JobSchedulerError::CantRemove
                    })
                }
            }
        })
    }
}

impl InitStore for PostgresNotificationStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let inited = self.inited();
        let store = self.store.clone();
        let init_tables = self.init_tables;
        let table = self.table.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let inited = inited.await;
            if matches!(inited, Ok(false)) || matches!(inited, Err(_)) {
                let mut w = store.write().await;
                let val = w.clone();
                let val = val.init().await;
                match val {
                    Ok(v) => {
                        if init_tables {
                            if let PostgresStore::Inited(client) = &v {
                                let v = client.read().await;

                                let sql = "CREATE TABLE IF NOT EXISTS ".to_string()
                                    + &*table
                                    + " ( \
                                    id UUID, \
                                    job_id UUID, \
                                    extra BYTEA, \
                                    CONSTRAINT pk_notification_id PRIMARY KEY (id)
                                )";
                                let create = v.query(&*sql, &[]).await;
                                if let Err(e) = create {
                                    error!("Error creating notification table {:?}", e);
                                    return Err(JobSchedulerError::CantInit);
                                }
                                let sql = "CREATE TABLE IF NOT EXISTS ".to_string()
                                    + &*states_table
                                    + " (\
                                    id UUID NOT NULL,
                                    state INTEGER NOT NULL,
                                    CONSTRAINT pk_notification_states PRIMARY KEY (id, state),
                                    CONSTRAINT fk_notification_id FOREIGN KEY (id) REFERENCES "
                                    + &*table
                                    + " (id) ON DELETE CASCADE
                                )";
                                let create = v.query(&*sql, &[]).await;
                                if let Err(e) = create {
                                    error!("Error creating notification states table {:?}", e);
                                    return Err(JobSchedulerError::CantInit);
                                }
                            }
                        }
                        *w = v;
                        Ok(())
                    }
                    Err(e) => {
                        error!("Error initialising {:?}", e);
                        Err(e)
                    }
                }
            } else {
                Ok(())
            }
        })
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        Box::pin(async move {
            let store = store.read().await;
            Ok(store.inited())
        })
    }
}

impl NotificationStore for PostgresNotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: JobId,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NotificationId>, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        let states_table = self.states_table.clone();
        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::CantListGuids),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let state = state as i32;
                    let sql = "SELECT DISTINCT states.id \
                    FROM \
                     "
                    .to_string()
                        + &*table
                        + " as states \
                     RIGHT JOIN "
                        + &*states_table
                        + " as st ON st.id = states.id \
                    WHERE \
                         job_id = $1 \
                     AND state = $2";
                    let result = store.query(&*sql, &[&job, &state]).await;
                    match result {
                        Ok(rows) => Ok(rows
                            .iter()
                            .map(|r| {
                                let uuid: Uuid = r.get(0);
                                uuid
                            })
                            .collect::<Vec<_>>()),
                        Err(e) => {
                            error!("Error listing notification guids for job and state {:?}", e);
                            Err(JobSchedulerError::CantListGuids)
                        }
                    }
                }
            }
        })
    }

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::CantListGuids),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let sql =
                        "SELECT DISTINCT id FROM ".to_string() + &*table + " WHERE job_id = $1";
                    let result = store.query(&*sql, &[&job_id]).await;
                    match result {
                        Ok(rows) => Ok(rows
                            .iter()
                            .map(|g| {
                                let uuid: Uuid = g.get(0);
                                uuid
                            })
                            .collect::<Vec<_>>()),
                        Err(e) => {
                            error!(
                                "Error getting list of notifications guids for job id{:?}",
                                e
                            );
                            Err(JobSchedulerError::CantListGuids)
                        }
                    }
                }
            }
        })
    }

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::CantRemove),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let state = state as i32;
                    let sql = "DELETE FROM ".to_string()
                        + &*states_table
                        + " \
                    WHERE \
                            id = $1 \
                        AND state = $2 \
                    RETURNING state";
                    let result = store.query(&*sql, &[&notification_id, &state]).await;
                    match result {
                        Ok(row) => Ok(!row.is_empty()),
                        Err(e) => {
                            error!("Error deleting notification for state {:?}", e);
                            Err(JobSchedulerError::CantRemove)
                        }
                    }
                }
            }
        })
    }

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                PostgresStore::Created(_) => Err(JobSchedulerError::CantRemove),
                PostgresStore::Inited(store) => {
                    let store = store.read().await;
                    let sql = "DELETE FROM ".to_string() + &*table + " WHERE job_id = $1";
                    store
                        .query(&*sql, &[&job_id])
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            error!("Error deleting for job {:?}", e);
                            JobSchedulerError::CantRemove
                        })
                }
            }
        })
    }
}
