use anyhow::{anyhow, bail, Context, Result};
use async_backtrace::framed;
use itertools::Itertools;
use semver::Version;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, RawResult, TaosBuilder, TaosPool};
use tracing::{instrument, Instrument};

use crate::{
    utils::{constants::*, mask_dsn},
    ConnectorLicense,
};

/// Check if an TDengine dsn is in cloud env
pub fn is_cloud(to: &taos::Dsn) -> bool {
    debug_assert!(
        matches!(to.driver.as_str(), "tmq" | "taos"),
        "Invalid driver: {}",
        to.driver
    );
    to.protocol
        .as_ref()
        .map(|p| match p.as_str() {
            "http" | "https" | "ws" | "wss" => true,
            _ => false,
        })
        .unwrap_or(false)
        && to.get("token").is_some()
}

#[derive(Debug)]
pub enum LicenseKind {
    Good,
    Edition(anyhow::Error),
    Feature(anyhow::Error),
    Connector(anyhow::Error),
}

impl LicenseKind {
    pub fn ok(self) -> Result<()> {
        match self {
            LicenseKind::Good => Ok(()),
            LicenseKind::Edition(err) => Err(err),
            LicenseKind::Feature(err) => Err(err),
            LicenseKind::Connector(err) => Err(err),
        }
    }

    pub fn is_err(&self) -> bool {
        match self {
            LicenseKind::Good => false,
            LicenseKind::Edition(_) => true,
            LicenseKind::Feature(_) => true,
            LicenseKind::Connector(_) => true,
        }
    }
}

static mut INFORMATION_GRANTS_FULL: &'static str = "information_schema.ins_grants_full";

async fn check_grant_of(
    builder: &TaosBuilder,
    version: &Version,
    grant: &str,
) -> Result<LicenseKind> {
    // Check enterprise license
    let edition = builder
        .get_edition()
        .await
        .with_context(|| format!("Failed to check {grant} license"))?
        .assert_enterprise_edition();
    if let Err(err) = edition {
        tracing::warn!(err = %err, "{grant} feature requires enterprise edition");
        return Ok(LicenseKind::Edition(anyhow!("{grant} feature requires enterprise edition, cause: {err:#}, please contact the TDengine customer success team for further assistance.")));
    }

    let conn = builder.build().await?;
    if *version < VERSION_3_3_0 {
        // Not check advanced feature grants in old version.
        return Ok(LicenseKind::Good);
    }
    // Check features license
    let (ok, expire) = conn
        .query_one::<_, (bool, String)>(format!(
            "select `expire` > now as `ok`, `expire` from {} where grant_name='{grant}'",
            unsafe { INFORMATION_GRANTS_FULL }
        ))
        .await
        .with_context(|| format!("Failed to check {grant} license"))?
        .ok_or_else(|| anyhow!("You enterprise edition has no {grant} license"))?;
    tracing::debug!(ok, expire, "active-active license check");
    if !ok {
        Ok(LicenseKind::Edition(anyhow!("Active-Active expired at {expire}, please contact the TDengine customer success team for further assistance.")))
    } else {
        Ok(LicenseKind::Good)
    }
}

async fn check_connector_grant_of(
    builder: &TaosBuilder,
    version: &semver::Version,
    connector: &str,
) -> Result<LicenseKind> {
    // get tdengine server version and handle compatibility
    // skip license check for newly-added connectors in old version
    let connectors_old = vec!["opc_da", "opc_ua", "pi", "kafka", "influxdb", "mqtt"];

    if version < &VERSION_3_2_3 && connectors_old.contains(&connector) {
        return Ok(LicenseKind::Good);
    }
    let grants_sql = if version >= &VERSION_3_2_3 {
        format!(
            "select `limits` from {} where grant_name='{connector}'",
            unsafe { INFORMATION_GRANTS_FULL }
        )
    } else {
        format!("select `{connector}` from information_schema.ins_grants")
    };
    let conn = builder.build().await?;
    let license: ConnectorLicense = conn
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
    let expired_duration = if version >= &VERSION_3_2_3 {
        license.expired_seconds()
    } else {
        license.expired_days()
    };
    if let Some(duration) = expired_duration {
        let err = anyhow!(
            "The current connector {} has been expired for {}, \
            please contact the TDengine customer success team to get the activation code.",
            connector,
            humantime::format_duration(duration.to_std().unwrap())
        );
        return Ok(LicenseKind::Connector(err));
    }
    Ok(LicenseKind::Good)
}

#[allow(dead_code)]
struct LicenseOf<'a> {
    dsn: &'a Dsn,
    pool: TaosPool,
    version: Version,
    edition: RawResult<()>,
}

#[allow(dead_code)]
async fn enterprise_edition_of(dsn: &Dsn) -> anyhow::Result<LicenseOf> {
    let subject_taken = {
        let mut v = dsn.clone();
        v.subject.take();
        v
    };
    let builder = TaosBuilder::from_dsn(&subject_taken)?;
    semver::Version::parse(
        &builder
            .server_version()
            .await
            .with_context(|| mask_dsn(dsn))?
            .split('.')
            .take(3)
            .join("."),
    )?;
    let edition = builder
        .get_edition()
        .await
        .context("Failed to check destination edition")?
        .assert_enterprise_edition();

    let version = semver::Version::parse(
        &builder
            .server_version()
            .await
            .with_context(|| mask_dsn(dsn))?
            .split('.')
            .take(3)
            .join("."),
    )?;
    let pool = builder.pool()?;
    Ok(LicenseOf {
        dsn,
        pool,
        version,
        edition,
    })
}

#[framed]
#[instrument(skip_all, fields(source = %mask_dsn(from), sink = %mask_dsn(to)))]
pub async fn validate_enterprise_license(from: &Dsn, to: &Dsn) -> Result<LicenseKind> {
    let source_dsn_context = || format!("License error in source: {}", mask_dsn(&from));
    let sink_dsn_context = || format!("License error in sink: {}", mask_dsn(&to));
    // Check if enterprise available
    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    match (from.driver.as_str(), to.driver.as_str()) {
        ("tmq", "taos") => {
            let mut from = from.clone();
            from.subject.take();
            let source_builder = TaosBuilder::from_dsn(&from)?;
            let sink_builder = TaosBuilder::from_dsn(to)?;

            let source_version = semver::Version::parse(
                &source_builder
                    .server_version()
                    .await
                    .with_context(source_dsn_context)?
                    .split('.')
                    .take(3)
                    .join("."),
            )?;

            let sink_version = semver::Version::parse(
                &sink_builder
                    .server_version()
                    .await
                    .with_context(sink_dsn_context)?
                    .split('.')
                    .take(3)
                    .join("."),
            )?;

            if source_version >= VERSION_3_3_0 && sink_version < VERSION_3_3_0 {
                bail!("Source version is 3.3.0 or later, but sink version is earlier than 3.3.0, which is not supported.");
            }

            if from.get("replica").is_some() {
                if source_version < VERSION_3_3_0 {
                    return Ok(LicenseKind::Edition(anyhow!(
                        "Active-Active feature requires source version 3.3.0 or later"
                    )));
                }
                // active-active grant validation
                if !is_cloud(&from) {
                    let kind = check_grant_of(&source_builder, &source_version, "active_active")
                        .in_current_span()
                        .await
                        .with_context(source_dsn_context)?;
                    if kind.is_err() {
                        return Ok(kind);
                    }
                }
                if !is_cloud(&to) {
                    let kind = check_grant_of(&sink_builder, &sink_version, "active_active")
                        .in_current_span()
                        .await
                        .with_context(sink_dsn_context)?;
                    if kind.is_err() {
                        return Ok(kind);
                    }
                }
                return check_connector_grant_of(&sink_builder, &sink_version, "td3.0")
                    .in_current_span()
                    .await
                    .with_context(sink_dsn_context);
            } else {
                // plain tmq to taos task.

                // Check source enterprise license
                let mut conn = sink_builder
                    .build()
                    .await
                    .context("sink connection failed")?;
                if let Err(err) = sink_builder.ping(&mut conn).await {
                    if *err.code() == 0x0388 {
                        let subject = to.subject.as_deref().unwrap_or("unknown");
                        Err(err.context(format!("sink database {subject}")))?
                    } else {
                        bail!("Failed to connect sink server: {err}");
                    }
                };

                if is_cloud(&to) {
                    return Ok(LicenseKind::Good);
                }
                let edition = sink_builder
                    .get_edition()
                    .await
                    .context("Failed to check destination edition")?
                    .assert_enterprise_edition();

                if let Err(err) = edition {
                    return Ok(LicenseKind::Edition(anyhow!("The destination is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.")));
                }

                let sink_version = semver::Version::parse(
                    &sink_builder
                        .server_version()
                        .await
                        .with_context(sink_dsn_context)?
                        .split('.')
                        .take(3)
                        .join("."),
                )?;
                return check_connector_grant_of(&sink_builder, &sink_version, "td3.0")
                    .in_current_span()
                    .await
                    .with_context(sink_dsn_context);
            }
        }
        ("taos", "tmq" | "taos") => {
            let mut from = from.clone();
            from.subject.take();
            // let source_builder = TaosBuilder::from_dsn(&from)?;

            // to.subject.take();
            let sink_builder = TaosBuilder::from_dsn(to)?;
            let mut conn = sink_builder
                .build()
                .await
                .context("sink connection failed")?;
            if let Err(err) = sink_builder.ping(&mut conn).await {
                if *err.code() == 0x0388 {
                    let subject = to.subject.as_deref().unwrap_or("unknown");
                    Err(err.context(format!("sink database {subject}")))?
                } else {
                    bail!("Failed to connect sink server: {err}");
                }
            };

            if is_cloud(&to) {
                return Ok(LicenseKind::Good);
            }
            let edition = sink_builder
                .get_edition()
                .await
                .context("Failed to check destination edition")?
                .assert_enterprise_edition();

            if let Err(err) = edition {
                return Ok(LicenseKind::Edition(anyhow!("The destination is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.")));
            }
            let sink_version = semver::Version::parse(
                &sink_builder
                    .server_version()
                    .await
                    .with_context(sink_dsn_context)?
                    .split('.')
                    .take(3)
                    .join("."),
            )?;
            return check_connector_grant_of(&sink_builder, &sink_version, "td2.0")
                .await
                .with_context(sink_dsn_context);
        }
        ("tmq" | "taos", _) => {
            let mut from = from.clone();
            from.subject.take();
            let builder = TaosBuilder::from_dsn(&from)?;
            let _ = builder.build().await.context("Source connection error")?;

            if is_cloud(&from) {
                return Ok(LicenseKind::Good);
            }
            let edition =
                tokio::time::timeout(std::time::Duration::from_secs(30), builder.get_edition())
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
            let mut conn = builder.build().await.context("sink connection error")?;
            builder.ping(&mut conn).await?;

            if is_cloud(&to) {
                return Ok(LicenseKind::Good);
            }
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
            // to.subject.take();
            let sink_builder = TaosBuilder::from_dsn(to)?;
            let sink_version = semver::Version::parse(
                &sink_builder
                    .server_version()
                    .await
                    .with_context(sink_dsn_context)?
                    .split('.')
                    .take(3)
                    .join("."),
            )?;
            let mut conn = sink_builder
                .build()
                .await
                .context("sink connection error")?;
            if let Err(err) = sink_builder.ping(&mut conn).await {
                if *err.code() == 0x0388 {
                    let subject = to.subject.as_deref().unwrap_or("unknown");
                    Err(err.context(format!("sink database {subject}")))?
                } else {
                    bail!("Failed to connect sink server: {err}");
                }
            };
            if is_cloud(&to) {
                return Ok(LicenseKind::Good);
            }
            let edition = sink_builder
                .get_edition()
                .await
                .context("Failed to check destination edition")?
                .assert_enterprise_edition();

            if edition.is_err() {
                let err = edition.unwrap_err().to_string();
                bail!("The destination is not a valid TDengine enterprise edition, cause: {err}, please contact the TDengine customer success team for further assistance.");
            }

            let connector = match from.driver.as_str() {
                "opcua" => "opc_ua",
                "opcda" => "opc_da",
                "influxdb" => "influxdb",
                "opentsdb" => "opentsdb",
                "pi" => "pi",
                "pibackfill" => "pi",
                crate::runners::kafka::KAFKA_ID => "kafka",
                crate::runners::historian::AVEVA_HISTORIAN_ID => "avevahistorian",
                "mqtt" => "mqtt",
                "tmq" => "td3.0",
                "taos" => "td2.6",
                crate::runners::mysql::MYSQL_ID => "mysql",
                crate::runners::postgres::POSTGRES_ID => "postgres",
                crate::runners::oracle::ORACLE_ID => "oracle",
                connector => {
                    bail!("The current connector {connector} is not supported by license.")
                }
            };
            return Ok(
                check_connector_grant_of(&sink_builder, &sink_version, connector)
                    .in_current_span()
                    .await
                    .with_context(sink_dsn_context)?,
            );
        }
        _ => (),
    };
    Ok(LicenseKind::Good)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_validate_enterprise_license() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let now = chrono::Local::now();
        let expired = chrono::Local::now() - (chrono::Duration::days(10));
        let future = chrono::Local::now() + (chrono::Duration::days(100));

        let dsn = Dsn::from_str("taos://").unwrap();
        let taos = TaosBuilder::from_dsn(&dsn).unwrap();
        let conn = taos.build().await.unwrap();
        conn.exec("create database if not exists test")
            .await
            .unwrap();

        conn.exec_many([
            "create table if not exists test.test_grants_full (\
            ts timestamp, grant_name varchar(100), display_name varchar(100),\
            expire varchar(100), limits varchar(100))",
            "delete from test.test_grants_full",
        ])
        .await
        .unwrap();
        unsafe { INFORMATION_GRANTS_FULL = "test.test_grants_full" };

        // 1. tmq + active-active
        let from = Dsn::from_str("tmq:///test?replica").unwrap();
        let to = Dsn::from_str("taos:///test").unwrap();
        let res = validate_enterprise_license(&from, &to).await;
        assert!(res.is_err(), "{:#?}", res);
        assert!(dbg!(format!("{:#}", res.unwrap_err()))
            .contains("You enterprise edition has no active_active license"));

        conn.exec("insert into test.test_grants_full values(now, 'active_active', 'Active-Active', '2022-01-01 00:00:00', NULL)")
            .await
            .unwrap();
        let res = validate_enterprise_license(&from, &to).await.unwrap().ok();
        assert!(res.is_err(), "{:#?}", res);
        assert!(dbg!(format!("{:#}", res.unwrap_err()))
            .contains("Active-Active expired at 2022-01-01 00:00:00"));

        conn.exec_many([
            "delete from test.test_grants_full".to_string(),
            format!(
                "insert into test.test_grants_full values(now, 'active_active', 'Active-Active', '{}', NULL)",
                future.format("%Y-%m-%d %H:%M:%S")
            ),
        ])
        .await
        .unwrap();
        let err = validate_enterprise_license(&from, &to).await.unwrap_err();
        assert!(dbg!(format!("{:#}", err))
            .contains("The current connector td3.0 is not supported by license."));

        let (grant, display) = ("td3.0", "TDengine 3.0");
        conn.exec(format!(
            r#"insert into test.test_grants_full values(now, '{grant}', '{display}', '{time}','{{"number":1, "speed":-1, "expire":"{seconds}", "expireTime":"{time}" }}')"#,
            time = expired.format("%Y-%m-%d %H:%M:%S"),
            seconds = expired.timestamp()
        ))
        .await
        .unwrap();
        let res = validate_enterprise_license(&from, &to).await.unwrap().ok();
        assert!(res.is_err(), "{:#?}", res);
        assert!(dbg!(format!("{:#}", res.unwrap_err()))
            .contains("The current connector td3.0 has been expired for"));
        conn.exec_many([
            "delete from test.test_grants_full".to_string(),
            format!(
                "insert into test.test_grants_full values({}, 'active_active', 'Active-Active', '{}', NULL)",
                now.timestamp_millis(),
                future.format("%Y-%m-%d %H:%M:%S")
            ),
            format!(
            r#"insert into test.test_grants_full values({}, '{grant}', '{display}', '{time}','{{"number":1, "speed":-1, "expire":"{seconds}", "expireTime":"{time}" }}')"#,
            now.timestamp_millis() + 1000,
            time = future.format("%Y-%m-%d %H:%M:%S"),
            seconds =future.timestamp()
        )])
        .await
        .unwrap();
        let res = validate_enterprise_license(&from, &to).await.unwrap().ok();
        assert!(res.is_ok(), "{:#?}", res);

        let connectors = &[
            // id, grant, display
            ("tmq", "td3.0", "TDengine 3.0"),
            ("taos", "td2.0", "TDengine 2.0"),
            ("opcua", "opc_ua", "OPCUA"),
            ("opcda", "opc_da", "OPCDA"),
            ("pi", "pi", "PI"),
            ("pibackfill", "pi", "PI"),
            ("kafka", "kafka", "Kafka"),
            ("influxdb", "influxdb", "InfluxDB"),
            ("opentsdb", "opentsdb", "OpenTSDB"),
            ("avevaHistorian", "avevahistorian", "Aveva Historian"),
            ("mysql", "mysql", "MySQL"),
            ("postgres", "postgres", "PostgreSQL"),
            ("oracle", "oracle", "Oracle"),
            ("mqtt", "mqtt", "MQTT"),
        ];

        for (id, grant, display) in connectors {
            let from = Dsn::from_str(&format!("{}:///test", id)).unwrap();
            let to = Dsn::from_str("taos:///test").unwrap();

            // c.1 no license item
            conn.exec("delete from test.test_grants_full")
                .await
                .unwrap();
            let res = validate_enterprise_license(&from, &to).await;
            assert!(res.is_err(), "{:#?}", res);
            assert!(dbg!(format!("{:#}", res.unwrap_err())).contains(&format!(
                "The current connector {grant} is not supported by license."
            )));

            // c.2 expired
            conn.exec_many([format!(
                r#"insert into test.test_grants_full values(now, '{grant}', '{display}', '{time}',
                    '{{"number":1, "speed":-1, "expire":"{seconds}", "expireTime":"{time}" }}')"#,
                time = expired.format("%Y-%m-%d %H:%M:%S"),
                seconds = expired.timestamp()
            )])
            .await
            .unwrap();
            let err = validate_enterprise_license(&from, &to).await.unwrap().ok();
            dbg!(&err);
            assert!(err.is_err());
            assert!(dbg!(format!("{:#}", err.unwrap_err())).contains(&format!(
                "The current connector {grant} has been expired for"
            )));

            // c.3 good
            conn.exec_many([
                format!("delete from test.test_grants_full"),
                format!(
                    r#"insert into test.test_grants_full values(now, '{grant}', '{display}', '{time}',
                    '{{"number":1, "speed":-1, "expire":"{seconds}", "expireTime":"{time}" }}')"#,
                    time = future.format("%Y-%m-%d %H:%M:%S"),
                    seconds = future.timestamp()
                ),
            ])
            .await
            .unwrap();
            validate_enterprise_license(&from, &to).await.unwrap();
        }
        conn.exec("drop table test.test_grants_full").await.unwrap();
    }
}
