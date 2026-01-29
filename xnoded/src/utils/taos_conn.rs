use std::{sync::Arc, time::Duration};

use parking_lot::RwLock;
use snafu::ResultExt;
use taos::{
    AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, ResultSet, Taos, TaosBuilder,
    TryStreamExt,
};
use tracing::instrument;

use crate::utils;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Failed to query sql {sql}"))]
    Taos { sql: String, source: taos::RawError },
    #[snafu(display("Failed to deserialize query result"))]
    TaosDeserialize { source: taos::RawError },
    #[snafu(display("Invalid dsn"))]
    InvalidDsn { source: taos::RawError },
    #[snafu(display("Failed to build db connection"))]
    BuildTaos { source: taos::RawError },
    #[snafu(display("Task job not exists"))]
    TaskJobNotExists,
}

type Result<T> = std::result::Result<T, Error>;

pub struct TaosConn {
    builder: TaosBuilder,
    conn: RwLock<Option<Arc<Taos>>>,
    max_tries: usize,
}

impl TaosConn {
    #[instrument(skip_all)]
    pub async fn create(dsn: impl IntoDsn, max_tries: usize) -> Result<Self> {
        let builder = taos::TaosBuilder::from_dsn(dsn).context(InvalidDsnSnafu)?;
        let conn = match builder.build().await {
            Ok(conn) => Some(Arc::new(conn)),
            Err(e) => {
                tracing::error!("Failed to build db connection: {:#}", anyhow::Error::new(e));
                None
            }
        };
        Ok(Self {
            builder,
            conn: RwLock::new(conn),
            max_tries,
        })
    }

    #[instrument(skip_all)]
    async fn get_conn(&self) -> Result<Arc<Taos>> {
        if let Some(conn) = self.conn.read().as_ref() {
            Ok(conn.clone())
        } else {
            use taos::AsyncTBuilder;
            let conn = Arc::new(self.builder.build().await.context(BuildTaosSnafu)?);
            {
                *self.conn.write() = Some(conn.clone());
            }
            Ok(conn)
        }
    }

    #[instrument(skip_all)]
    pub async fn query<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        let query = async |conn: &Taos, sql: &str| conn.query(sql).await;
        let on_success = async |mut rs: ResultSet| {
            rs.deserialize::<T>()
                .try_collect()
                .await
                .context(TaosDeserializeSnafu)
        };
        self.with_retry(sql, query, on_success).await
    }

    #[instrument(skip_all)]
    pub async fn query_one<T>(&self, sql: &str) -> Result<Option<T>>
    where
        T: for<'a> serde::Deserialize<'a> + Send,
    {
        let query = async |conn: &Taos, sql: &str| conn.query_one(sql).await;
        let on_success = async |res: Option<T>| Ok(res);
        self.with_retry(sql, query, on_success).await
    }

    #[instrument(skip_all)]
    pub async fn exec(&self, sql: &str) -> Result<usize> {
        let exec = async |conn: &Taos, sql: &str| conn.exec(sql).await;
        let on_success = async |n: usize| Ok(n);
        self.with_retry(sql, exec, on_success).await
    }

    #[instrument(skip_all)]
    async fn with_retry<F1, F2, R1, R2>(&self, sql: &str, method: F1, on_success: F2) -> Result<R2>
    where
        F1: AsyncFn(&Taos, &str) -> std::result::Result<R1, taos::RawError>,
        F2: AsyncFn(R1) -> Result<R2>,
    {
        tracing::debug!(sql, "executing SQL");
        let mut try_count = 0;
        let mut last_err = None;
        let mut backoff =
            utils::backoff::RetryBackoff::new(Duration::from_millis(200), Duration::from_secs(5));
        loop {
            if try_count > self.max_tries
                && let Some(e) = last_err
            {
                return Err(e).context(TaosSnafu { sql })?;
            }
            let conn = match self.get_conn().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!("Failed to get connection: {:#}", anyhow::Error::new(e));
                    backoff.wait().await;
                    continue;
                }
            };
            backoff.reset();
            match method(&conn, sql).await {
                Ok(rs) => return on_success(rs).await,
                Err(e) if should_reconnect(e.code().into()) => {
                    tracing::error!("connection error: {e:#}");
                    self.reset();
                    try_count += 1;
                    last_err = Some(e);
                    continue;
                }
                Err(e) if should_retry(e.code().into()) => {
                    tracing::error!("database error: {e:#}");
                    try_count += 1;
                    last_err = Some(e);
                    continue;
                }
                Err(e) if matches!(e.code().into(), 0x8015 | 0x8010) => {
                    return TaskJobNotExistsSnafu.fail();
                }
                Err(e) => return Err(e).context(TaosSnafu { sql }),
            }
        }
    }

    pub fn reset(&self) {
        *self.conn.write() = None;
    }
}

/// 0xE002: connection closed
/// 0xE003: send timeout
/// 0xE004: receive timeout
/// 0x000B: unable to establish connection
fn should_reconnect(code: i32) -> bool {
    matches!(code, 0xE002 | 0xE003 | 0xE004 | 0x000B)
}

/// 0x0334: Out of dnodes
/// 0x2603: the table does not exist
/// 0x03D3: conflict transaction not completed
/// 0x03C7: stable uid not match
/// 0x032C: object is creating
/// 0x0115: invalid msg
fn should_retry(code: i32) -> bool {
    matches!(code, 0x2603 | 0x0334 | 0x03D3 | 0x03C7 | 0x032C | 0x0115)
}
