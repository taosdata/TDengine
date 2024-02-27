use std::{ops::Deref, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taosx_core::{
    utils::{get_main_version_from_server_version, get_server_version},
    ConnectorLicense,
};

/// LicenseValidator is used to validate the license of the source and target data sources.
pub struct LicenseValidator<'a> {
    from: &'a Dsn,
    to: &'a Dsn,
    pool: Option<&'a sqlx::SqlitePool>,
}

pub enum LicenseKind {
    Good,
    Edition(anyhow::Error),
    Connector(anyhow::Error),
}

impl LicenseKind {
    pub fn ok(self) -> Result<()> {
        match self {
            LicenseKind::Good => Ok(()),
            LicenseKind::Edition(err) => Err(err),
            LicenseKind::Connector(err) => Err(err),
        }
    }

    pub fn is_err(&self) -> bool {
        match self {
            LicenseKind::Good => false,
            LicenseKind::Edition(_) => true,
            LicenseKind::Connector(_) => true,
        }
    }
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
        let edition = validate_enterprise_license(self.from, self.to).await?;

        if edition.is_err() {
            return Ok(edition);
        }

        match self.from.driver.as_str() {
            "opc"
            | "opcua"
            | "opcda"
            | "pi"
            | "pibackfill"
            | "mqtt"
            | "influxdb"
            | "opentsdb"
            | "tmq"
            | "taos"
            | taosx_core::runners::kafka::KAFKA_ID
            | taosx_core::runners::historian::AVEVA_HISTORIAN_ID => {
                validate_connector_license(self.from, self.to, self.pool).await
            }
            _ => Ok(LicenseKind::Good),
        }
    }
}

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
    .await?
    .ok()?;
    Ok(())
}

async fn validate_enterprise_license(from: &Dsn, to: &Dsn) -> Result<LicenseKind> {
    // Check if enterprise available
    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    match (from.driver.as_str(), to.driver.as_str()) {
        ("tmq" | "taos", "tmq" | "taos") => {
            let mut from = from.clone();
            from.subject.take();
            let from = TaosBuilder::from_dsn(from)?;
            let _ = from.build().await?;
            // to.subject.take();
            let to_builder = TaosBuilder::from_dsn(to)?;
            let mut conn = to_builder.build().await?;
            if let Err(err) = to_builder.ping(&mut conn).await {
                if *err.code().deref() == 0x0388 {
                    let subject = to.subject.as_deref().unwrap_or("unknown");
                    Err(err.context(format!("Target database {subject}")))?
                } else {
                    bail!("Failed to connect target server: {err}");
                }
            };
            let edition = to_builder
                .get_edition()
                .await
                .context("Failed to check destination edition")?
                .assert_enterprise_edition();

            if let Err(err) = edition {
                return Ok(LicenseKind::Edition(anyhow!("The destination is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.")));
            }
        }
        ("tmq" | "taos", _) => {
            let mut from = from.clone();
            from.subject.take();
            let builder = TaosBuilder::from_dsn(from)?;
            let _ = builder.build().await.context("Source connection error")?;
            let edition = tokio::time::timeout(Duration::from_secs(30), builder.get_edition())
                .await
                .context("Checking source edition timeout")?
                .context("Failed to check source edition")?
                .assert_enterprise_edition();

            if let Err(err) = edition {
                return Ok(LicenseKind::Edition(anyhow!("The source is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.")));
            }
        }
        ("local", "tmq" | "taos") => {
            let mut to = to.clone();
            to.subject.take();
            // to.subject.take();
            let builder = TaosBuilder::from_dsn(&to)?;
            let mut conn = builder.build().await.context("Target connection error")?;
            builder.ping(&mut conn).await?;
            let edition = builder
                .get_edition()
                .await
                .context("Failed to check destination edition")?
                .assert_enterprise_edition();

            if edition.is_err() {
                let err = edition.unwrap_err().to_string();
                bail!("The destination is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.");
            }
        }
        (_, "tmq" | "taos") => {
            let to = to.clone();
            // to.subject.take();
            let builder = TaosBuilder::from_dsn(&to)?;
            let mut conn = builder.build().await.context("Target connection error")?;
            if let Err(err) = builder.ping(&mut conn).await {
                if *err.code().deref() == 0x0388 {
                    let subject = to.subject.as_deref().unwrap_or("unknown");
                    Err(err.context(format!("Target database {subject}")))?
                } else {
                    bail!("Failed to connect target server: {err}");
                }
            };
            let edition = builder
                .get_edition()
                .await
                .context("Failed to check destination edition")?
                .assert_enterprise_edition();

            if edition.is_err() {
                let err = edition.unwrap_err().to_string();
                bail!("The destination is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.");
            }
        }
        _ => (),
    };
    Ok(LicenseKind::Good)
}

async fn validate_connector_license(
    from: &Dsn,
    to: &Dsn,
    pool: Option<&sqlx::SqlitePool>,
) -> Result<LicenseKind> {
    let builder = TaosBuilder::from_dsn(to)?;
    let taos = builder.build().await?;
    // let is_enterprise = builder.is_enterprise_edition().await?;

    let assert_enterprise = builder.assert_enterprise_edition().await;

    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    if let Err(_) = assert_enterprise {
        /* anyhow::bail!(format!(
            "{err:?}. A non-expired enterprise edition is required in most of steps."
        )) */
        anyhow::bail!("Your TDengine Enterprise edition has bean expired, please contact the TDengine customer success team to get the activation code.")
    }
    // is cloud?
    if to
        .protocol
        .as_ref()
        .map(|p| match p.as_str() {
            "http" | "https" | "ws" | "wss" => true,
            _ => false,
        })
        .unwrap_or(false)
        && to.get("token").is_some()
    {
        return Ok(LicenseKind::Good);
    }

    let endpoint = match (
        from.addresses[0].host.as_deref(),
        from.addresses[0].port.as_ref(),
    ) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => format!("{host}"),
        (None, Some(port)) => format!(":{port}"),
        (None, None) => format!(""),
    };
    let cluster_id: i64 = taos
        .query_one("select id from information_schema.ins_cluster")
        .await
        .map_err(|err| anyhow::format_err!("Cannot retrieve cluster id: {err}"))?
        .unwrap();

    // These lines disable the connector license check.
    let _ = endpoint;
    // let license = taos.query_one(sql)
    let connector = match from.driver.as_str() {
        "opcua" => "opc_ua",
        "opcda" => "opc_da",
        "influxdb" => "influxdb",
        "opentsdb" => "opentsdb",
        "pi" => "pi",
        "pibackfill" => "pi",
        taosx_core::runners::kafka::KAFKA_ID => taosx_core::runners::kafka::KAFKA_ID,
        taosx_core::runners::historian::AVEVA_HISTORIAN_ID => {
            taosx_core::runners::historian::AVEVA_HISTORIAN_ID
        }
        "mqtt" => "mqtt",
        "tmq" => "td3.0",
        "taos" => "td2.6",
        connector => bail!("The current connector {connector} is not supported by license."),
    };

    // get tdengine server version and handle compatibility
    let server_version = get_server_version(&taos).await?;
    let (a, b, c) = get_main_version_from_server_version(&server_version).unwrap();
    // skip license check for newly-added connectors in old version
    let connectors_old = vec!["opc_da", "opc_ua", "pi", "kafka", "influxdb", "mqtt"];
    if !(a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3))
        && !connectors_old.contains(&connector)
    {
        return Ok(LicenseKind::Good);
    }
    let grants_sql = if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
        format!("select `limits` from information_schema.ins_grants_full where grant_name='{connector}'")
    } else {
        format!("select `{connector}` from information_schema.ins_grants")
    };
    let license: ConnectorLicense = taos
        .query_one::<_, String>(grants_sql)
        .await
        .context("Cannot retrieve license")?
        .ok_or_else(|| {
            anyhow::anyhow!("The current connector {connector} is not supported by license.")
        })
        .and_then(|s| {
            serde_json::from_str(&s).with_context(|| format!("Cannot parse license from str: {s}"))
        })?;

    // since 3.2.3.0, the expired time is in seconds
    let expired_days = if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
        license.expired_seconds().map(|s| (s / 86400) as u32)
    } else {
        license.expired_days()
    };
    if let Some(days) = expired_days {
        let err = anyhow!("The current connector {} has been expired for {} days, please contact the TDengine customer success team to get the activation code.", connector, days);
        return Ok(LicenseKind::Connector(err));
    }

    if let Some(pool) = pool {
        let mut used: Vec<String> = sqlx::query_scalar(&format!("select `from` from tasks join labels where key='cluster-id' and `value` = '{}' and deleted = false and `from` like '{}%';",cluster_id, from.driver))
                        .fetch_all(pool)
                        .await?;
        used.push(from.to_string());
        let used = used
            .into_iter()
            .map(|s| s.parse::<Dsn>().unwrap().addresses[0].to_string())
            .collect::<std::collections::HashSet<_>>()
            .len();

        return Ok(match license.number {
            0 => LicenseKind::Connector(anyhow!(
                "The current connector {connector} is disabled by license."
            )),
            n if n > 0 => {
                if used > n as usize {
                    LicenseKind::Connector(anyhow!("The current connector {connector} reaches connection number limit({n}) by license"))
                } else {
                    LicenseKind::Good
                }
            }
            _ => LicenseKind::Good,
        });
    }
    Ok(LicenseKind::Good)
}
