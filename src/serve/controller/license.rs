use anyhow::{anyhow, Result};
use async_backtrace::framed;
use taos::Dsn;

use taosx_core::utils::{license::LicenseKind, mask_dsn};
use tracing::{instrument, Instrument};

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
            let kind =
                taosx_core::utils::license::validate_enterprise_license(&self.from, &self.to)
                    .await?;
            let pool = match self.pool {
                Some(pool) => pool,
                None => {
                    return Ok(kind);
                }
            };

            match &kind {
                LicenseKind::Good {
                    cluster_id,
                    connector,
                } => {
                    let cluster_id = match cluster_id {
                        Some(id) => id,
                        None => {
                            return Ok(LicenseKind::good());
                        }
                    };
                    let license = match connector {
                        Some(connector) => connector,
                        None => {
                            return Ok(LicenseKind::good());
                        }
                    };
                    let mut used: Vec<String> = sqlx::query_scalar(&format!("select `from` from tasks join labels where key='cluster-id' and `value` = '{}' and deleted = false and `from` like '{}%';", cluster_id, self.from.driver))
                                    .fetch_all(pool)
                                    .await?;
                    used.push(self.from.to_string());
                    let used = used
                        .into_iter()
                        .map(|s| {
                            s.parse::<Dsn>()
                                .unwrap()
                                .addresses
                                .first()
                                .map(|addr| addr.to_string())
                                .unwrap_or_else(|| "".to_string())
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
                                LicenseKind::Connector(anyhow!("Number of {:?} has reached the licensed upper limit.", license.r#type))
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
