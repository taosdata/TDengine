use std::{sync::Arc, time::Duration};

use parking_lot::RwLock;
use snafu::ResultExt;
use taos::{
    AsyncFetchable, AsyncQueryable, AsyncTBuilder, Code, IntoDsn, ResultSet, Taos, TaosBuilder,
    TryStreamExt,
};
use tracing::instrument;

use crate::backoff::RetryBackoff;

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

impl Error {
    pub fn code(&self) -> Option<Code> {
        match self {
            Error::Taos { source, .. }
            | Error::TaosDeserialize { source }
            | Error::InvalidDsn { source }
            | Error::BuildTaos { source } => Some(source.code()),
            _ => None,
        }
    }
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
        let mut backoff = RetryBackoff::new(Duration::from_millis(200), Duration::from_secs(5));
        loop {
            let conn = match self.get_conn().await {
                Ok(conn) => conn,
                Err(e) => {
                    if let Some(code) = e.code()
                        && should_exit(code)
                    {
                        tracing::info!(
                            code = i32::from(code),
                            "received exit code from db, exit..."
                        );
                        std::process::exit(0);
                    }
                    if e.code().is_some_and(should_reconnect) {
                        tracing::error!("Failed to get connection: {e:#}");
                        self.wait_retry_or_return(&mut backoff, e).await?;
                        continue;
                    }
                    tracing::error!("Failed to get connection: {e:#}");
                    return Err(e);
                }
            };
            match method(&conn, sql).await {
                Ok(rs) => return on_success(rs).await,
                Err(e) if should_reconnect(e.code()) => {
                    tracing::error!("connection error: {e:#}");
                    self.reset();
                    self.wait_retry_or_return(
                        &mut backoff,
                        Error::Taos {
                            sql: sql.to_string(),
                            source: e,
                        },
                    )
                    .await?;
                    continue;
                }
                Err(e) if should_retry(e.code()) => {
                    tracing::error!("database error: {e:#}");
                    self.wait_retry_or_return(
                        &mut backoff,
                        Error::Taos {
                            sql: sql.to_string(),
                            source: e,
                        },
                    )
                    .await?;
                    continue;
                }
                Err(e) if should_exit(e.code()) => {
                    tracing::info!(
                        code = i32::from(e.code()),
                        "received exit code from db, exit..."
                    );
                    std::process::exit(0);
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

    async fn wait_retry_or_return(&self, backoff: &mut RetryBackoff, err: Error) -> Result<()> {
        if !can_retry(backoff, self.max_tries) {
            return Err(err);
        }

        backoff.wait().await;
        Ok(())
    }
}

/// 0xE002: connection closed
/// 0xE003: send timeout
/// 0xE004: receive timeout
/// 0x000B: unable to establish connection
fn should_reconnect(code: Code) -> bool {
    matches!(code.into(), 0xE002 | 0xE003 | 0xE004 | 0x000B)
}

/// 0x0334: Out of dnodes
/// 0x2603: the table does not exist
/// 0x03D3: conflict transaction not completed
/// 0x03C7: stable uid not match
/// 0x032C: object is creating
/// 0x0115: invalid msg
fn should_retry(code: Code) -> bool {
    matches!(
        code.into(),
        0x2603 | 0x0334 | 0x03D3 | 0x03C7 | 0x032C | 0x0115
    )
}

/// 0x0131: Dnode is closing down
fn should_exit(code: Code) -> bool {
    matches!(code.into(), 0x0131)
}

fn can_retry(backoff: &RetryBackoff, max_tries: usize) -> bool {
    backoff.retries() < max_tries
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Error, can_retry};
    use crate::backoff::RetryBackoff;

    #[test]
    fn build_taos_error_exposes_underlying_error_code() {
        let err = Error::BuildTaos {
            source: taos::RawError::new(0x000B, "Unable to establish connection"),
        };

        assert_eq!(err.code().map(i32::from), Some(0x000B));
    }

    #[test]
    fn can_retry_before_backoff_reaches_max_tries() {
        let mut backoff = RetryBackoff::new(Duration::from_millis(1), Duration::from_millis(1));

        assert!(can_retry(&backoff, 3));
        backoff.reset();
        assert!(can_retry(&backoff, 3));
    }

    #[tokio::test]
    async fn stops_retrying_when_backoff_reaches_max_tries() {
        let mut backoff = RetryBackoff::new(Duration::from_millis(1), Duration::from_millis(1));

        for _ in 0..3 {
            backoff.wait().await;
        }

        assert!(!can_retry(&backoff, 3));
    }
}
