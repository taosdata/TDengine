use std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};

use anyhow::Context;
use chrono::{NaiveDate, Utc};
use dashmap::DashMap;
use serde::Deserialize;
use serde_with::serde_as;
use taos::taos_query::tmq::Assignment;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

pub use csv::*;
pub use legacy::*;
pub use local_to_taos::local_to_taos;
pub use parquets::*;
pub use plugins::*;
pub use tmq_to_local::tmq_to_local;
pub use tmq_to_td::{tmq_offsets, tmq_to_td};
pub use transform::Action;
use utils::port_pool::PortPool;
use crate::tmq_to_kafka::clean_task;
pub use crate::tmq_to_kafka::tmq_to_kafka;

mod csv;
mod fake;
mod legacy;
mod local_to_taos;
mod parquets;
pub mod taoz;
pub mod tmq;
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
        let days = (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();

        days > self.expire as i64
    }

    pub fn expired_days(&self) -> Option<u32> {
        let days = (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();

        if days > self.expire as i64 {
            Some((days - self.expire as i64) as u32)
        } else {
            None
        }
    }
}

#[test]
fn test_connector_license() {
    let s = r#"{"type":"OPC_UA","number":1,"speed":-1,"expire":"19658"}"#;
    let license: ConnectorLicense = serde_json::from_str(s).unwrap();
    dbg!(&license);
    assert!(license.is_expired());
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
    pub breakpoints: Option<String>,
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
            breakpoints,
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
                    let _ = from.build().await?;
                    let mut to = to.clone();
                    to.subject.take();
                    let to = TaosBuilder::from_dsn(to)?;
                    let _ = to.build().await?;

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
                    let _ = builder.build().await.context("Source connection error")?;
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
                    let _ = builder.build().await.context("Target connection error")?;
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
                        Self::append_breakpoints_in_dsn(breakpoints, from),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                    )
                    .await?;
                }
                ("opentsdb", "taos") => {
                    opentsdb_to_taos(
                        Self::append_breakpoints_in_dsn(breakpoints, from),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        span.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
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
                ("fake", "taos") => {
                    fake::fake_to_taos(
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

    fn append_breakpoints_in_dsn(breakpoints: &Option<String>, from: &Dsn) -> Dsn {
        match breakpoints {
            None => from.clone(),
            Some(b) => {
                let mut from = from.clone();
                from.params.insert("breakpoints".to_string(), b.clone());
                from
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
    fn test_append_breakpoints_in_dsn() {
        let dsn = Dsn::from_str("opentsdb://?param1=abc&param2=123").unwrap();
        let dsn = TaskOpts::append_breakpoints_in_dsn(&Some(String::from("abc")), &dsn);
        assert_eq!("abc", dsn.params.get("breakpoints").unwrap());

        let dsn = Dsn::from_str("opentsdb://?param1=abc&param2=123").unwrap();
        let dsn = TaskOpts::append_breakpoints_in_dsn(&None, &dsn);
        assert_eq!(None, dsn.params.get("breakpoints"));
    }
}
