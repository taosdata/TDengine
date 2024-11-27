mod metadata_store;
mod notification_store;

use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};
use tracing::error;

pub use metadata_store::PostgresMetadataStore;
pub use notification_store::PostgresNotificationStore;

#[derive(Clone)]
pub enum PostgresStore {
    Created(String),
    Inited(Arc<RwLock<Client>>),
}

impl PostgresStore {
    pub fn inited(&self) -> bool {
        matches!(self, PostgresStore::Inited(_))
    }
}

impl Default for PostgresStore {
    fn default() -> Self {
        let url = std::env::var("POSTGRES_URL")
            .map(Some)
            .unwrap_or_default()
            .unwrap_or_else(|| {
                let db_host =
                    std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
                let port = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
                let dbname =
                    std::env::var("POSTGRES_DB").unwrap_or_else(|_| "postgres".to_string());
                let username =
                    std::env::var("POSTGRES_USERNAME").unwrap_or_else(|_| "postgres".to_string());
                let password = std::env::var("POSTGRES_PASSWORD")
                    .map(Some)
                    .unwrap_or_default();
                let application_name = std::env::var("POSTGRES_APP_NAME")
                    .map(Some)
                    .unwrap_or_default();

                "".to_string()
                    + "host="
                    + &*db_host
                    + " port="
                    + &*port
                    + " dbname="
                    + &*dbname
                    + " user="
                    + &*username
                    + &*match password {
                        Some(password) => " password=".to_string() + &*password,
                        None => "".to_string(),
                    }
                    + &*match application_name {
                        Some(application_name) => {
                            " application_name=".to_string() + &*application_name
                        }
                        None => "".to_string(),
                    }
            });
        Self::Created(url)
    }
}

impl PostgresStore {
    pub fn init(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<PostgresStore, JobSchedulerError>> + Send>> {
        Box::pin(async move {
            match self {
                PostgresStore::Created(url) => {
                    #[cfg(feature = "postgres-openssl")]
                    let tls = postgres_openssl::TlsConnector;
                    #[cfg(feature = "postgres-native-tls")]
                    let tls = postgres_native_tls::TlsConnector;
                    #[cfg(not(any(
                        feature = "postgres-native-tls",
                        feature = "postgres-openssl"
                    )))]
                    let tls = NoTls;
                    let connect = tokio_postgres::connect(&*url, tls).await;
                    if let Err(e) = connect {
                        error!("Error connecting to postgres {:?}", e);
                        return Err(JobSchedulerError::CantInit);
                    }
                    let (client, connection) = connect.unwrap();
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            error!("Error with Postgres Connection {:?}", e);
                        }
                    });
                    Ok(PostgresStore::Inited(Arc::new(RwLock::new(client))))
                }
                PostgresStore::Inited(client) => Ok(PostgresStore::Inited(client)),
            }
        })
    }
}
