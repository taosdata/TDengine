use std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};

use anyhow::Context;
use chrono::{NaiveDate, Utc};
use dashmap::DashMap;
use serde::Deserialize;
use serde_with::serde_as;
use taos::sync::Queryable;
use taos::taos_query::tmq::Assignment;
use taos::{AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

pub use csv::*;
pub use legacy::*;
pub use local_to_taos::local_to_taos;
pub use parquets::*;
pub use plugins::*;
pub use tmq_to_local::tmq_to_local;
pub use tmq_to_td::tmq_to_td;
pub use transform::Action;
use utils::port_pool::PortPool;

use crate::tmq_to_kafka::clean_task;
pub use crate::tmq_to_kafka::tmq_to_kafka;
use crate::validation::DataSourceValidation;

mod csv;
mod legacy;
mod local_to_taos;
mod parquets;
mod taoz;
mod tmq;
mod tmq_to_local;
mod tmq_to_td;
pub mod types;

mod transform;
pub mod utils;

mod plugins;
mod tmq_to_kafka;

mod extensions;

shadow_rs::shadow!(build);

#[derive(clap::ValueEnum, Clone, Debug)]
enum Compression {
    None,
    Brotli,
    Bzip2,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}

#[derive(Debug, Default)]
pub struct Transferred {
    pub stables: AtomicU32,
    pub tables: AtomicU32,
    pub records: AtomicU64,
    pub points: AtomicU64,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct ConnectorLicense {
    pub r#type: String,
    pub number: i64,
    pub speed: i64,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub expire: u16,
}

impl ConnectorLicense {
    pub fn is_expired(&self) -> bool {
        (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days()
            > self.expire as i64
    }
}

#[derive(Debug, Clone)]
pub struct TaskOpts {
    pub from: Dsn,
    pub transform: Vec<Action>,
    pub to: Dsn,
    pub parser: Option<plugins::Parser>,
    pub jobs: usize,
    pub compression_level: Option<usize>,
    pub force: bool,
    pub cancel: CancellationToken,
    pub with_agent: Option<(i64, String, String)>,
    // pub port_pool: OnceCell<PortPool>
    pub offsets: Arc<DashMap<String, Vec<Assignment>>>,
    pub transferred: Option<Arc<Transferred>>,
    pub span: tracing::Span,
    pub task_id: Option<String>,
}

impl Drop for TaskOpts {
    fn drop(&mut self) {
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
        }
    }
}

pub const METRICS_TIME_START: &str = "metrics.time_started_timestamp";
pub const METRICS_TIME_START_DATE: &str = "metrics.time_started_date";
pub const METRICS_TIME_COST: &str = "metrics.time_cost";
pub const METRICS_TIME_RECORDS_PER_SECOND: &str = "metrics.records_per_second";

impl TaskOpts {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    #[instrument(skip_all, name = "run_task")]
    pub async fn run(&self, port_pool: &PortPool) -> Result<(), anyhow::Error> {
        let Self {
            from,
            transform,
            to,
            parser,
            jobs,
            compression_level: _,
            force,
            cancel,
            with_agent,
            // port_pool,
            offsets,
            transferred,
            span,
            task_id,
            ..
        } = self;
        // dbg!(task_id);

        if with_agent.is_none() {
            // Check if enterprise available
            #[cfg(not(feature = "disable-enterprise-only-validation"))]
            match (from.driver.as_str(), to.driver.as_str()) {
                ("tmq" | "taos", "tmq" | "taos") => {
                    let mut from = from.clone();
                    from.subject.take();
                    let from = TaosBuilder::from_dsn(from)?;
                    let mut to = to.clone();
                    to.subject.take();
                    let to = TaosBuilder::from_dsn(to)?;

                    if !from
                        .is_enterprise_edition()
                        .await
                        .context("Failed to check source edition")?
                        && !to
                            .is_enterprise_edition()
                            .await
                            .context("Failed to check target edition")?
                    {
                        anyhow::bail!(
                        "Source or target should be enterprise edition. If it's not your case, please contact us."
                    )
                    }
                }
                ("tmq" | "taos", _) => {
                    let mut from = from.clone();
                    from.subject.take();
                    let builder = TaosBuilder::from_dsn(from)?;
                    if !builder
                        .is_enterprise_edition()
                        .await
                        .context("Failed to check source edition")?
                    {
                        anyhow::bail!(
                        "Only enterprise edition is supported. If it's not your case, please contact us."
                    )
                    }
                }
                (_, "tmq" | "taos") => {
                    let mut to = to.clone();
                    to.subject.take();
                    let builder = TaosBuilder::from_dsn(to)?;
                    if !builder
                        .is_enterprise_edition()
                        .await
                        .context("Failed to check target edition")?
                    {
                        anyhow::bail!(
                        "Only enterprise edition is supported. If it's not your case, please contact us."
                    )
                    }
                }
                _ => (),
            }
        }

        // Run task
        {
            metrics::gauge!(METRICS_TIME_START, Utc::now().timestamp_millis() as f64);
            match (from.driver.as_str(), to.driver.as_str()) {
                ("tmq", "taos") => {
                    tmq_to_td(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        cancel.clone(),
                        offsets.clone(),
                    )
                    .in_current_span()
                    .await?;
                }
                ("tmq", "local") => {
                    tmq_to_local(
                        from.clone(),
                        to.clone(),
                        *jobs,
                        *force,
                        cancel.clone(),
                        offsets.clone(),
                    )
                    .await?;
                }
                ("local", "taos") => {
                    local_to_taos(from.clone(), to.clone(), *jobs, *force)
                        .in_current_span()
                        .await?;
                }
                ("taos", "taos") => {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            tracing::info!("csv transfer cancelled");
                            return Ok(())
                        }
                        rs = legacy_to_taos(from.clone(), transform.clone(), to.clone(), *jobs, cancel.clone(), task_id.clone())
                        // .in_current_span()
                        .instrument(tracing::info_span!("legacy_to_taos")) => {
                            rs?;
                        }
                    }
                }
                ("taos", "csv") => {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            tracing::info!("csv transfer cancelled");
                            return Ok(())
                        }
                        rs = query_to_csv(from.clone(), to.clone()) => {
                            rs?;
                        }
                    }
                }
                ("taos", "parquet") => {
                    query_to_parquet(from.clone(), to.clone(), *force).await?;
                }
                ("pi" | "pibackfill", "taos") => {
                    pi_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("opc" | "opcda" | "opcua", "taos") => {
                    opc_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("mqtt", "taos") => {
                    mqtt_to_taos(
                        from.clone(),
                        parser.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("influxdb", "taos") => {
                    influxdb_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("opentsdb", "taos") => {
                    opentsdb_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("csv", "taos") => {
                    csv_to_taos(
                        from.clone(),
                        parser.clone(),
                        to.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("tmq", "kafka") => {
                    let mut from = from.clone();
                    if let Some(task_id) = self.task_id.clone() {
                        from.params.insert("topic_suffix".parse()?, task_id);
                    }
                    tmq_to_kafka(from, to.clone(), cancel.clone()).await?;
                }
                ("kafka", "taos") => {
                    kafka_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                ("historian", "taos") => {
                    historian_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                    )
                    .await?;
                }
                (_, _) => anyhow::bail!("unsupported source or target: from {} to {}", from, to),
            }
        }
        Ok(())
    }

    pub async fn delete_task(&self) -> Result<(), anyhow::Error> {
        let Self { from, to, .. } = &self;
        match (from.driver.as_str(), to.driver.as_str()) {
            ("tmq", "kafka") => {
                let mut from = from.clone();
                if let Some(task_id) = self.task_id.clone() {
                    from.params.insert("topic_suffix".parse()?, task_id);
                }
                clean_task(from.clone()).await?;
            }
            (_, _) => {}
        }
        Ok(())
    }
}

pub fn validate_dsn(dsn: impl IntoDsn) -> DataSourceValidation {
    let dsn = dsn.into_dsn();
    match dsn {
        Err(err) => {
            DataSourceValidation::invalid("unknown".to_string(), format!("DSN error: {err:#}"))
        }
        Ok(d) => {
            match d.driver.as_str() {
                // TODO: clickhouse
                "historian" => runners::historian::is_valid(&d),
                "influxdb" => futures::executor::block_on(runners::influxdb::is_valid(&d)),
                "kafka" => runners::kafka::is_valid(&d),
                "mqtt" => runners::mqtt::is_valid(&d),
                "opc" | "opcda" | "opcua" => runners::opc::is_valid(&d),
                "opentsdb" => futures::executor::block_on(runners::opentsdb::is_valid(&d)),
                "pi" | "pibackfill" => runners::pi::is_valid(&d),
                "taos" | "tmq" => futures::executor::block_on(is_valid(&d)),
                &_ => DataSourceValidation::unknown(),
            }
        }
    }
}

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let builder = TaosBuilder::from_dsn(dsn);
    match builder {
        Err(err) => DataSourceValidation::invalid(
            "taos".to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(b) => {
            let conn = b.build().await;
            match conn {
                Err(err) => DataSourceValidation::invalid(
                    "taos".to_string(),
                    format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn.to_string(),
                        err.to_string()
                    ),
                ),
                Ok(c) => {
                    let version = c.server_version();
                    match version {
                        Err(err) => DataSourceValidation::invalid(
                            "taos".to_string(),
                            format!(
                                "failed to get server version from dsn: {}, cause: {}",
                                dsn.to_string(),
                                err.to_string()
                            ),
                        ),
                        Ok(v) => DataSourceValidation {
                            valid: true,
                            support: true,
                            data_source: "taos".to_string(),
                            version: Some(v.to_string()),
                            message: None,
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use taos::Dsn;

    use super::*;

    #[test]
    fn test_validate_dsn() {
        // historian
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let dsv = validate_dsn(dsn);
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("historian", dsv.data_source);

        // kafka
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092").unwrap();
        let dsv = validate_dsn(dsn);
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("kafka", dsv.data_source);

        // taos
        let dsn = Dsn::from_str("taos+ws://192.168.1.92:6041").unwrap();
        let dsv = validate_dsn(dsn);
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("taos", dsv.data_source);
        assert_eq!("3.1.1.3", dsv.version.unwrap());

        // tmq
        let dsn = Dsn::from_str("tmq+ws://192.168.1.92:6041").unwrap();
        let dsv = validate_dsn(dsn);
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!("taos", dsv.data_source);
        assert_eq!("3.1.1.3", dsv.version.unwrap());
    }
}
