pub mod sample;
pub mod validate;

use std::sync::atomic::{AtomicU32, AtomicU64};

use anyhow::Context;
use sink_csv::{csv_to_taos, query_to_csv};
use sink_parquet::query_to_parquet;
use source_mqtt::mqtt_to_taos;
use source_opc::opc_to_taos;
use source_opentsdb::opentsdb_to_taos;
use source_pi::pi_to_taos;
use taos::Dsn;
use taoslog::QidManager;
use taoslog::utils::{QidMetadataGetter, Span};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

pub use taosx_core::transform::Action;
use taosx_core::utils::port_pool::PortPool;
use taosx_core::utils::trace::Qid;

use sink_kafka::{clean_task, tmq_to_kafka};
use sink_mqtt::tmq_to_mqtt;

use source_historian::historian_to_taos;
use source_influxdb::influxdb_to_taos;
use source_kafka::kafka_to_taos;
use source_mongodb::mongodb_to_taos;
use source_mssql::mssql_to_taos;
use source_mysql::mysql_to_taos;
use source_oracle::oracle_to_taos;
use source_postgres::postgres_to_taos;
use source_sparkplugb::sparkplugb_to_taos;

#[derive(Debug, Default)]
pub struct Transferred {
    pub stables: AtomicU32,
    pub tables: AtomicU32,
    pub records: AtomicU64,
    pub points: AtomicU64,
}

// Use public re-exports to avoid breaking changes
pub use taosx_core::task_set::prelude::TaskNotify;

pub type TaskNotifySender = flume::Sender<TaskNotify>;
pub type TaskNotifyReceiver = flume::Receiver<TaskNotify>;
#[derive(Debug, Clone)]
pub struct TaskOpts {
    pub from: Dsn,
    pub transform: Vec<Action>,
    pub to: Dsn,
    pub parser: Option<taosx_core::plugins::Parser>,
    pub health: Option<taosx_core::task_set::prelude::HealthOpts>,
    pub cancel: CancellationToken,
    pub with_agent: Option<(i64, String, String)>,
    // pub port_pool: OnceCell<PortPool>
    pub breakpoints: Option<String>,
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
            cancel,
            with_agent,
            // port_pool,
            breakpoints,
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
            let task_id_number = task_id
                .as_ref()
                .map(|t| t.parse::<i64>().context("parse task id"))
                .transpose()?;
            match (from.driver.as_str(), to.driver.as_str()) {
                ("tmq" | "sync", "taos") => {
                    let mut from = from.clone();
                    from.driver = "tmq".to_string();
                    tmq_to_td::tmq_to_td(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
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
                    tmq_to_local::tmq_to_local(
                        task_id.clone(),
                        from.clone(),
                        to.clone(),
                        cancel.clone(),
                    )
                    .in_current_span()
                    .await?;
                }
                ("local", "taos" | "tmq") => {
                    let mut to = to.clone();
                    to.driver = "taos".to_string();
                    local_to_taos::local_to_taos(task_id.clone(), from.clone(), to, cancel.clone())
                        .in_current_span()
                        .await?;
                }
                ("taos", "taos") => {
                    legacy_to_taos::legacy_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        cancel.clone(),
                        task_id_number,
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
                    query_to_parquet(from.clone(), to.clone()).await?;
                }
                ("pi" | "pibackfill", "taos") => {
                    pi_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                ("opc" | "opcda" | "opcua", "taos") => {
                    opc_to_taos(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                ("mqtt", "taos") => {
                    mqtt_to_taos(
                        from.clone(),
                        parser.clone(),
                        to.clone(),
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                ("tmq", "mqtt") => {
                    tmq_to_mqtt(from, to, task_id_number, cancel).await?;
                }
                (source_sparkplugb::SPARKPLUGB_ID, "taos") => {
                    sparkplugb_to_taos(
                        from,
                        to,
                        with_agent.clone(),
                        parser.clone(),
                        task_id_number,
                        notify.clone(),
                        cancel,
                    )
                    .await?;
                }
                ("influxdb", "taos") => {
                    influxdb_to_taos(
                        Self::append_breakpoints_in_dsn(breakpoints, from),
                        to.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                ("opentsdb", "taos") => {
                    opentsdb_to_taos(
                        Self::append_breakpoints_in_dsn(breakpoints, from),
                        to.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
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
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                ("tmq", source_kafka::KAFKA_ID) => {
                    let mut from = from.clone();
                    if let Some(task_id) = task_id.clone() {
                        from.params.insert("topic_suffix".parse()?, task_id);
                    }
                    tmq_to_kafka(from, to.clone(), cancel.clone()).await?;
                }
                (source_kafka::KAFKA_ID, "taos") => {
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
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                (source_historian::AVEVA_HISTORIAN_ID, "taos") => {
                    historian_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                // ("fake", "taos") => {
                //     fake::fake_to_taos(
                //         from.clone(),
                //         parser.clone(),
                //         transform.clone(),
                //         to.clone(),
                //         0,
                //         port_pool,
                //         cancel.clone(),
                //         with_agent.clone(),
                //         None,
                //         notify.clone(),
                //     )
                //     .await?;
                // }
                (source_mysql::MYSQL_ID, "taos") => {
                    mysql_to_taos(
                        from.clone(),
                        parser.clone(),
                        to.clone(),
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                (source_postgres::POSTGRES_ID, "taos") => {
                    postgres_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                (source_oracle::ORACLE_ID, "taos") => {
                    oracle_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                (source_mssql::MSSQL_ID, "taos") => {
                    mssql_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
                        notify.clone(),
                    )
                    .await?;
                }
                (source_mongodb::MONGODB_ID, "taos") => {
                    mongodb_to_taos(
                        from.clone(),
                        parser.clone(),
                        transform.clone(),
                        to.clone(),
                        0,
                        port_pool,
                        cancel.clone(),
                        with_agent.clone(),
                        None,
                        task_id_number,
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
            ("tmq", source_kafka::KAFKA_ID) => {
                let mut from = from.clone();
                if let Some(task_id) = self.task_id.clone() {
                    from.params.insert("topic_suffix".parse()?, task_id);
                }
                clean_task(from.clone()).await?;
            }
            ("csv", _) => {
                let path = from.path.clone();
                tracing::warn!("delete csv task, path: {:?}", path);
                if let Some(path) = path {
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

    #[ignore]
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

    #[ignore]
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
