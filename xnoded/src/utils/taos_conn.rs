use std::{sync::Arc, time::Duration};

use parking_lot::RwLock;
use snafu::ResultExt;
use taos::{
    AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, ResultSet, Taos, TaosBuilder,
    TryStreamExt,
};

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
    pub async fn create(dsn: impl IntoDsn, max_tries: usize) -> Result<Self> {
        let builder = taos::TaosBuilder::from_dsn(dsn).context(InvalidDsnSnafu)?;
        let conn = builder.build().await.context(BuildTaosSnafu)?;
        Ok(Self {
            builder,
            conn: RwLock::new(Some(Arc::new(conn))),
            max_tries,
        })
    }

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

    pub async fn exec(&self, sql: &str) -> Result<usize> {
        let exec = async |conn: &Taos, sql: &str| conn.exec(sql).await;
        let on_success = async |n: usize| Ok(n);
        self.with_retry(sql, exec, on_success).await
    }

    async fn with_retry<F1, F2, R1, R2>(&self, sql: &str, method: F1, on_success: F2) -> Result<R2>
    where
        F1: AsyncFn(&Taos, &str) -> std::result::Result<R1, taos::RawError>,
        F2: AsyncFn(R1) -> Result<R2>,
    {
        tracing::trace!(sql, "executing SQL");
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
                Err(e) if matches!(e.code().into(), 0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B) => {
                    // 0xE001: internal error
                    // 0xE002: connection closed
                    // 0xE003: send timeout
                    // 0xE004: receive timeout
                    // 0x000B: unable to establish connection
                    tracing::error!("connection error: {e:#}");
                    self.reset();
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
