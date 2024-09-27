use std::sync::OnceLock;
use std::sync::{
    atomic::{AtomicU32, AtomicU64},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use serde::Deserialize;
use serde_with::serde_as;
use taos::Dsn;
use taoslog::utils::{QidMetadataGetter, Span};
use taoslog::QidManager;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

pub mod migrations;

pub use csv::*;
pub use legacy::*;
pub use local_to_taos::local_to_taos;
pub use parquets::*;
pub use plugins::*;
pub use tmq_to_local::tmq_to_local;
pub use tmq_to_td::{get_table_progress, tmq_offsets, tmq_to_td};
pub use transform::Action;
use utils::port_pool::PortPool;
use utils::trace::Qid;

// use crate::plugins::transform::*;
use crate::runners::historian::historian_to_taos;
use crate::runners::influxdb::influxdb_to_taos;
use crate::runners::kafka::kafka_to_taos;
use crate::runners::mongodb::mongodb_to_taos;
use crate::runners::mssql::mssql_to_taos;
use crate::runners::mysql::mysql_to_taos;
use crate::runners::oracle::oracle_to_taos;
use crate::runners::postgres::postgres_to_taos;
use crate::tmq_to_kafka::clean_task;
pub use crate::tmq_to_kafka::tmq_to_kafka;

pub mod csv;
mod fake;
mod legacy;
mod local_to_taos;
mod parquets;
pub mod taoz;
pub mod tmq;
mod tmq_to_local;
mod tmq_to_td;
pub mod types;

pub mod transform;
pub mod utils;

pub mod plugins;
mod tmq_to_kafka;

pub mod core_metrics;
mod extensions;

pub mod global;

// 全局定义的是否开启 agent 压缩的标志位
pub static AGENT_COMPRESSION: OnceLock<bool> = OnceLock::new();

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
    pub r#type: Option<String>,
    pub number: i64,
    pub speed: i64,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub expire: i64,
    pub expire_time: Option<String>,
}

impl ConnectorLicense {
    pub fn is_expired_day(&self) -> bool {
        let days = (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();

        days > self.expire && self.expire >= 0
    }

    pub fn expired_days(&self) -> Option<chrono::Duration> {
        let days = (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();

        if days > self.expire && self.expire >= 0 {
            Some(chrono::Duration::days((days - self.expire as i64) as _))
        } else {
            None
        }
    }

    pub fn is_expired_second(&self) -> bool {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        seconds > self.expire as u64 && self.expire >= 0
    }

    pub fn expired_seconds(&self) -> Option<chrono::Duration> {
        let expire_time = chrono::DateTime::from_timestamp(self.expire as _, 0)?;
        let now = chrono::Utc::now();
        if expire_time > now || self.expire < 0 {
            None
        } else {
            Some(now - expire_time)
        }
    }
}

#[test]
fn test_connector_license() {
    let s = r#"{"type":"OPC_UA","number":1,"speed":-1,"expire":"19658"}"#;
    let license: ConnectorLicense = serde_json::from_str(s).unwrap();
    dbg!(&license);
    assert!(license.is_expired_day());
}

#[test]
fn test_is_expired_day() {
    let s = r#"{"type":"OPC_UA","number":1,"speed":-1,"expire":"-1"}"#;
    let license: ConnectorLicense = serde_json::from_str(s).unwrap();
    dbg!(license.is_expired_day());
}

#[test]
fn test_expired_days() {
    let s = r#"{"type":"OPC_UA","number":1,"speed":-1,"expire":"-1"}"#;
    let license: ConnectorLicense = serde_json::from_str(s).unwrap();
    dbg!(license.expired_days());
}

#[test]
fn test_is_expired_second() {
    let s = r#"{"type":"OPC_UA","number":1,"speed":-1,"expire":"-1"}"#;
    let license: ConnectorLicense = serde_json::from_str(s).unwrap();
    dbg!(license.is_expired_second());
}

#[test]
fn test_expired_seconds() {
    let s = r#"{"type":"OPC_UA","number":1,"speed":-1,"expire":"-1"}"#;
    let license: ConnectorLicense = serde_json::from_str(s).unwrap();
    dbg!(license.expired_seconds());
}

pub enum TaskNotify {
    Info(String),
    Warn(String),
    Error(String),
    Done,
}

impl TaskNotify {
    pub fn info(msg: impl Into<String>) -> Self {
        Self::Info(msg.into())
    }
    pub fn warn(msg: impl Into<String>) -> Self {
        Self::Warn(msg.into())
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }
    pub fn done() -> Self {
        Self::Done
    }
}

pub type TaskNotifySender = flume::Sender<TaskNotify>;
pub type TaskNotifyReceiver = flume::Receiver<TaskNotify>;
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
    pub breakpoints: Option<String>,
    pub transferred: Option<Arc<Transferred>>,
    pub task_id: Option<String>,
    pub notify: TaskNotifySender,
}

impl Drop for TaskOpts {
    fn drop(&mut self) {
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
        }
    }
}

impl TaskOpts {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    #[instrument(name = "task::spawned", skip_all, fields(task.id = self.task_id))]
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
            transferred,
            task_id,
            notify,
            ..
        } = self;
        let mut qid = Span.get_qid().unwrap_or_else(Qid::init);
        qid.set_task_id(
            task_id
                .as_ref()
                .and_then(|id| id.parse::<u16>().ok())
                .unwrap_or_default(),
        );
        // debug_assert!(qid.task_id() > 0);
        // Run task
        {
            match (from.driver.as_str(), to.driver.as_str()) {
                ("tmq" | "sync", "taos") => {
                    let mut from = from.clone();
                    from.driver = "tmq".to_string();
                    tmq_to_td(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        cancel.clone(),
                        task_id.clone(),
                        notify.clone(),
                    )
                    .in_current_span()
                    .await?;
                }
                ("tmq" | "sync", "local") => {
                    let mut from = from.clone();
                    from.driver = "tmq".to_string();
                    tmq_to_local(
                        from.clone(),
                        to.clone(),
                        *jobs,
                        *force,
                        cancel.clone(),
                        task_id.clone(),
                    )
                    .await?;
                }
                ("local", "taos" | "tmq") => {
                    let mut to = to.clone();
                    to.driver = "taos".to_string();
                    local_to_taos(from.clone(), to, *jobs, *force)
                        .in_current_span()
                        .await?;
                }
                ("taos", "taos") => {
                    legacy_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        cancel.clone(),
                        task_id.clone(),
                    )
                    .await?;
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
                    )
                    .await?;
                }
                ("tmq", runners::kafka::KAFKA_ID) => {
                    let mut from = from.clone();
                    if let Some(task_id) = task_id.clone() {
                        from.params.insert("topic_suffix".parse()?, task_id);
                    }
                    tmq_to_kafka(from, to.clone(), cancel.clone()).await?;
                }
                (runners::kafka::KAFKA_ID, "taos") => {
                    let mut dsn = from.clone();
                    if !dsn.params.contains_key("group") {
                        let group_id = task_id
                            .clone()
                            .ok_or(anyhow::anyhow!("group id is required for kafka to taos"))?;
                        dsn.params.insert("group".to_string(), group_id);
                    }

                    kafka_to_taos(
                        dsn,
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
                    )
                    .await?;
                }
                (runners::historian::AVEVA_HISTORIAN_ID, "taos") => {
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
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
                        notify.clone(),
                    )
                    .await?;
                }
                (runners::mysql::MYSQL_ID, "taos") => {
                    mysql_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
                    )
                    .await?;
                }
                (runners::postgres::POSTGRES_ID, "taos") => {
                    postgres_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
                    )
                    .await?;
                }
                (runners::oracle::ORACLE_ID, "taos") => {
                    oracle_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
                    )
                    .await?;
                }
                (runners::mssql::MSSQL_ID, "taos") => {
                    mssql_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
                    )
                    .await?;
                }
                (runners::mongodb::MONGODB_ID, "taos") => {
                    mongodb_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        jobs.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        transferred.clone(),
                        task_id.clone().map(|t| t.parse().unwrap()),
                        notify.clone(),
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
            ("tmq", runners::kafka::KAFKA_ID) => {
                let mut from = from.clone();
                if let Some(task_id) = self.task_id.clone() {
                    from.params.insert("topic_suffix".parse()?, task_id);
                }
                clean_task(from.clone()).await?;
            }
            ("csv", _) => {
                let path = from.path.clone();
                tracing::warn!("delete csv task, path: {:?}", path);
                match path {
                    Some(path) => {
                        let path = std::path::Path::new(&path);
                        if path.exists() {
                            if path.is_file() && path.is_relative() {
                                if let Some(parent) = path.parent() {
                                    std::fs::remove_dir_all(parent)?;
                                }
                            } else {
                                // ignore directory or absolute path, since it's created by manual
                            }
                        }
                    }
                    None => {}
                }
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

    use anyhow::Context;
    use taos::{AsyncTBuilder, TaosBuilder};

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

    #[tokio::test]
    async fn test_wrong_taos_in_dsn() -> Result<(), anyhow::Error> {
        dbg!(format!("test start: {}", chrono::Local::now()));
        let to = Dsn::from_str("taos://localhost:6031?test_db_n").unwrap();
        let builder = TaosBuilder::from_dsn(to)?;
        let now = chrono::Local::now();
        let res = builder
            .build()
            .await
            .context(format!("Target connection error: {now}"));

        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(err.to_string(), format!("Target connection error: {now}"));
        }
        dbg!(format!("test end: {}", chrono::Local::now()));
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_taos_in_dsn_pool() -> Result<(), anyhow::Error> {
        dbg!(format!("test start: {}", chrono::Local::now()));
        let to = Dsn::from_str("taos://localhost:6031?test_db_n").unwrap();
        let builder = taos::TaosBuilder::from_dsn(to)?;
        let pool = builder.pool()?;
        let now = chrono::Local::now();
        let res = pool
            .get()
            .await
            .context(format!("Target connection error: {now}"));

        assert!(res.is_err());
        if let Err(err) = res {
            assert_eq!(err.to_string(), format!("Target connection error: {now}"));
        }
        dbg!(format!("test end: {}", chrono::Local::now()));
        Ok(())
    }
}
