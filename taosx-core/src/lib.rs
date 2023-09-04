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

use anyhow::Context;
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use taos::{AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tracing::{instrument, Instrument};

mod extensions;

use crate::tmq_to_kafka::clean_task;
pub use crate::tmq_to_kafka::tmq_to_kafka;
pub use csv::*;
use dashmap::DashMap;
pub use legacy::*;
pub use local_to_taos::local_to_taos;
pub use parquets::*;
pub use plugins::*;
use std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};
use taos::taos_query::tmq::Assignment;
pub use tmq_to_local::tmq_to_local;
pub use tmq_to_td::tmq_to_td;
use tokio_util::sync::CancellationToken;
pub use transform::Action;
use utils::port_pool::PortPool;

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

#[derive(Debug, Serialize)]
pub struct ValidatedSource {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    since: Option<String>,
}

pub type ValidatedTarget = ValidatedSource;

impl Default for ValidatedSource {
    fn default() -> Self {
        Self {
            available: true,
            version: None,
            since: None,
        }
    }
}

pub async fn validate_source(dsn: impl IntoDsn) -> ValidatedSource {
    let dsn = dsn.into_dsn();

    match dsn {
        Ok(dsn) if dsn.driver.as_str() == "kafka" => {
            if let Err(err) = is_kafka_available(&dsn).await {
                ValidatedSource {
                    available: false,
                    version: None,
                    since: Some(format!("{err:#}")),
                }
            } else {
                Default::default()
            }
        }
        Ok(_) => Default::default(),
        Err(err) => ValidatedSource {
            available: false,
            version: None,
            since: Some(format!("DSN error: {err:#}")),
        },
    }
}

pub fn validate_target(dsn: impl IntoDsn) -> ValidatedTarget {
    let dsn = dsn.into_dsn();

    match dsn {
        Ok(_) => Default::default(),
        Err(err) => ValidatedSource {
            available: false,
            version: None,
            since: Some(format!("DSN error: {err:#}")),
        },
    }
}

pub const METRICS_TIME_START: &str = "metrics.time_started";
pub const METRICS_TIME_COST: &str = "metrics.time_cost";
pub const METRICS_TIME_RECORDS_PER_SECOND: &str = "metrics.records_per_second";

impl TaskOpts {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    #[instrument(skip_all, parent = &self.span)]
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
            ..
        } = self;

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
                    local_to_taos(from.clone(), to.clone(), *jobs, *force).await?;
                }
                ("taos", "taos") => {
                    legacy_to_taos(from.clone(), transform.clone(), to.clone(), *jobs).await?;
                }
                ("taos", "csv") => {
                    query_to_csv(from.clone(), to.clone()).await?;
                }
                ("taos", "parquet") => {
                    query_to_parquet(from.clone(), to.clone(), *force).await?;
                }
                ("pi" | "pibackfill", "taos") => {
                    plugins::pi_to_taos(
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
                    plugins::opc_to_taos(
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
                    plugins::mqtt_to_taos(
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
                    plugins::influxdb_to_taos(
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
                    plugins::opentsdb_to_taos(
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
                (_, _) => anyhow::bail!("unsupported source or target: from {} to {}", from, to),
            }
            Ok(())
        }
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
