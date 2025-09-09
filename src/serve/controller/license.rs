use std::{sync::LazyLock, time::Duration};

use anyhow::{Result, anyhow};
use async_backtrace::framed;
use cached::{Cached, TimedCache};
use taos::{Dsn, Itertools};

use taosx_core::utils::{
    license::{LicenseKind, LicenseKindGood},
    mask_dsn,
};
use tokio::sync::Mutex;
use tracing::{Instrument, instrument};

static CACHE: LazyLock<Mutex<TimedCache<String, LicenseKindGood>>> = LazyLock::new(|| {
    const DEFAULT_CACHE_TTL: u64 = 60 * 60; // 1 hour
    Mutex::new(TimedCache::with_lifespan(Duration::from_secs(
        std::env::var("TAOSX_LICENSE_CACHE_TTL").map_or(DEFAULT_CACHE_TTL, |f| {
            f.parse::<u64>().unwrap_or(DEFAULT_CACHE_TTL)
        }),
    )))
});

/// LicenseValidator is used to validate the license of the source and target data sources.
pub struct LicenseValidator<'a> {
    from: &'a Dsn,
    to: &'a Dsn,
    pool: Option<&'a sqlx::SqlitePool>,
}

impl<'a> LicenseValidator<'a> {
    /// Create a new LicenseValidator.
    pub fn new(from: &'a Dsn, to: &'a Dsn) -> Self {
        Self {
            from,
            to,
            pool: None,
        }
    }

    /// Create a new LicenseValidator with a sqlite pool.
    pub fn new_with_sqlite(from: &'a Dsn, to: &'a Dsn, pool: &'a sqlx::SqlitePool) -> Self {
        Self {
            from,
            to,
            pool: Some(pool),
        }
    }

    /// Validate the connector license and the enterprise license.
    pub async fn validate_connector(&self) -> Result<LicenseKind> {
        #[cfg(not(feature = "disable-enterprise-only-validation"))]
        {
            let mut masked_to = mask_dsn(self.to);
            let key = if masked_to.driver == "taos" {
                masked_to.subject.take();
                format!("{}-{}", self.from.driver, masked_to)
            } else {
                format!("{}-{}", self.from.driver, masked_to)
            };

            let mut cached = false;
            let kind = if let Some(kind) = CACHE.lock().await.cache_get(&key).cloned() {
                tracing::info!("validating license from cache hit");
                cached = true;
                LicenseKind::Good(kind)
            } else {
                tracing::info!("validating license from cache miss");
                taosx_core::utils::license::validate_enterprise_license(self.from, self.to).await?
            };
            let pool = match self.pool {
                Some(pool) => pool,
                None => {
                    if !cached {
                        if let Some(kind) = kind.as_good() {
                            CACHE.lock().await.cache_set(key, kind.clone());
                        }
                    }
                    return Ok(kind);
                }
            };

            match &kind {
                LicenseKind::Good(good) => {
                    let cluster_id = match good.cluster_id {
                        Some(id) => id,
                        None => {
                            if !cached {
                                CACHE.lock().await.cache_set(key, good.clone());
                            }
                            return Ok(LicenseKind::Good(good.clone()));
                        }
                    };
                    let license = match &good.connector {
                        Some(connector) => connector,
                        None => {
                            if !cached {
                                CACHE.lock().await.cache_set(key, good.clone());
                            }
                            return Ok(LicenseKind::Good(good.clone()));
                        }
                    };
                    let used: Vec<String> = sqlx::query_scalar(&format!("select `from` from tasks join labels where key='cluster-id' and `value` = '{}' and deleted = false and `from` like '{}%';", cluster_id, self.from.driver))
                                    .fetch_all(pool)
                                    .await?;
                    let mut used = used
                        .iter()
                        .map(|s| {
                            if s.starts_with('"') {
                                s.trim_matches('"').replace(r#"\""#, r#"""#)
                            } else {
                                s.to_string()
                            }
                        })
                        .collect_vec();

                    used.push(self.from.to_string());
                    let used = used
                        .into_iter()
                        .map(|s| {
                            s.parse::<Dsn>()
                                .unwrap()
                                .addresses
                                .first()
                                .map(|addr| addr.to_string())
                                .unwrap_or_default()
                        })
                        .collect::<std::collections::HashSet<_>>()
                        .len();

                    return Ok(match license.number {
                        0 => LicenseKind::Connector(anyhow!(
                            "Number of {:?} has reached the licensed upper limit.",
                            license.r#type
                        )),
                        n if n > 0 => {
                            if used > n as usize {
                                LicenseKind::Connector(anyhow!(
                                    "Number of {:?} has reached the licensed upper limit.",
                                    license.r#type
                                ))
                            } else {
                                kind
                            }
                        }
                        _ => kind,
                    });
                }
                LicenseKind::Connector(_) => {
                    return Ok(kind);
                }
                LicenseKind::Edition(_) => {
                    return Ok(kind);
                }
                _ => {}
            }
        }
        Ok(LicenseKind::good())
    }
}

#[framed]
#[instrument(skip_all, fields(source = %mask_dsn(from), sink = %mask_dsn(to)))]
pub async fn validate_task(
    from: &Dsn,
    to: &Dsn,
    pool: Option<&sqlx::SqlitePool>,
) -> anyhow::Result<()> {
    if let Some(pool) = pool {
        LicenseValidator::new_with_sqlite(from, to, pool)
    } else {
        LicenseValidator::new(from, to)
    }
    .validate_connector()
    .in_current_span()
    .await?
    .ok()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use taos::{AsyncQueryable, AsyncTBuilder, Dsn};

    #[tokio::test]
    async fn test_validate_task_with_taos() {
        let _ = tracing_subscriber::fmt().try_init();

        let to = "taos://localhost:6030".parse::<Dsn>().unwrap();
        let conn = taos::TaosBuilder::from_dsn(&to)
            .unwrap()
            .build()
            .await
            .unwrap();
        let _ = conn
            .exec("create database if not exists test_validate_task_with_taos")
            .await;
        for from in [
            "taos://localhost:6030/test",
            "csv://localhost:6030/test",
            "tmq://localhost:6030/test",
            "influxdb://localhost:6030/test",
            "influxdb://localhost:6030/test",
            "pi://localhost:6030/test",
            "pibackfill://localhost:6030/test",
            "kafka://localhost:6030/test",
            "mqtt://localhost:6030/test",
            "mongodb://localhost:6030/test",
            "oracle://localhost:6030/test",
            "mysql://localhost:6030/test",
            "mssql://localhost:6030/test",
            "opentsdb://localhost:6030/test",
        ] {
            let from = from.parse::<Dsn>().unwrap();
            let to = "taos://localhost:6030/test_validate_task_with_taos"
                .parse::<Dsn>()
                .unwrap();
            let instance = Instant::now();
            let _res = validate_task(&from, &to, None).await;
            println!("[{from:40}]: cache miss cost {:?}", instance.elapsed());
            let instance = Instant::now();
            let _res2 = validate_task(&from, &to, None).await;
            println!("[{from:40}]: cache hit cost {:?}", instance.elapsed());

            let mut guard = CACHE.lock().await;
            let store = guard.get_store();
            assert_eq!(store.len(), 1);
            guard.cache_clear();
        }
    }
}
