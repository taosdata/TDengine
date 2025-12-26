use anyhow::Context;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sink::persist::PersistConfig;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::instrument;

use crate::plugins::sink::point::model::PointModelConfig;
use crate::runners::influxdb::influxdb_datasets;
use crate::runners::opentsdb::opentsdb_datasets;
use crate::utils::mask_dsn;
use runners::opc::opc_datasets;
use taosx_utils::dsn::json_to_dsn;

pub use runners::{
    get_data_dir, get_file_upload_home_dir, get_log_dir, get_log_keep_days, get_plugins_info,
    set_env_data_dir, set_env_log_home_dir, set_env_log_keep_days, set_env_plugins_home_dir,
};
pub use sink::IpcStreamWorker;
pub use taosx_ipc::types::*;
pub use transform::Pipeline;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Parser {
    Inner(transform::Parser),
    WithSample {
        parser: transform::Parser,
        input: Option<Vec<serde_json::Value>>,
    },
}

#[test]
#[ignore]
fn test_parser_serde() {
    let parser = r#"{
  "parse": { "payload": { "json": ["value::double"] } },
  "model": {
    "table": "{topic}",
    "using": "mqtt",
    "tags": ["topic"],
    "columns": ["ts", "value", "qos"]
  }
}"#;
    let parser: Parser = serde_json::from_str(parser).unwrap();
    dbg!(&parser);
    let json = serde_json::to_string(&parser).unwrap();
    assert_eq!(
        json,
        r#"{"parser":{"parse":{"payload":""}},"format":{"a":1}}"#
    );
}

impl std::ops::Deref for Parser {
    type Target = transform::Parser;

    fn deref(&self) -> &Self::Target {
        match self {
            Parser::Inner(parser) => parser,
            Parser::WithSample { parser, .. } => parser,
        }
    }
}

impl std::ops::DerefMut for Parser {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Parser::Inner(parser) => parser,
            Parser::WithSample { parser, .. } => parser,
        }
    }
}

use self::sink::IpcHandler;
use self::sink::lush::LushModelConfig;

pub mod config;
pub mod expr;
pub mod raw_data;
pub mod runners;
mod service;
pub mod sink;
pub mod transform;

/// ipc stream metrics
/// be careful to modify, in case other crate use string value. for now POINTS value used in taosx-ipc.
pub const METRIC_RECORD_BATCHES: &str = "ipc.stream.record_batches";
pub const METRIC_RECEIVED_BATCHES: &str = "ipc.stream.received_batches";
pub const METRIC_BATCH_RECORDS: &str = "ipc.stream.batch_records";
pub const METRIC_INSERT_SQLS: &str = "ipc.stream.insert_sqls";
pub const METRIC_INSERT_SQL_FAILS: &str = "ipc.stream.insert_sql_fails";
pub const METRIC_STABLE_CREATED: &str = "ipc.stream.stable_created";
pub const METRIC_CHILD_TABLE_CREATED: &str = "ipc.stream.child_table_created";
pub const METRIC_RECORDS: &str = "ipc.stream.records";
pub const METRIC_RECORD_FAILS: &str = "ipc.stream.record_fails";
pub const METRIC_POINTS: &str = "ipc.stream.points";
pub const METRIC_POINT_FAILS: &str = "ipc.stream.point_fails";
pub const METRIC_WRITE_RAW_BLOCKS: &str = "ipc.stream.write_raw_blocks";
pub const METRIC_WRITE_RAW_BLOCK_FAILS: &str = "ipc.stream.write_raw_blocks_fails";

// =============== KingHistorian datasets lister hook ===============
// To avoid circular dependency between taosx-core and source-kinghistorian,
// we expose a registration point. The binary (taosx) can depend on
// source-kinghistorian and register its lister at startup.
// Then, list_datasets_from will invoke this hook when driver=="kinghist".

type KinghistDatasetsFn = fn(from: &Dsn, req: &DataSetsReq) -> anyhow::Result<Vec<DataSet>>;

static KINGHIST_DATASETS_HOOK: OnceLock<KinghistDatasetsFn> = OnceLock::new();

/// Register KingHistorian datasets lister.
/// Call this once at process startup (e.g., in taosx main) if KingHistorian is enabled.
pub fn register_kinghist_datasets_lister(f: KinghistDatasetsFn) {
    let _ = KINGHIST_DATASETS_HOOK.set(f);
}

#[derive(Debug, Clone)]
pub struct Via {
    pub task_id: i64,
    pub job_id: i64,
    pub endpoint: String,
    pub token: String,
}

#[instrument(skip_all)]
pub async fn build_ipc(
    socket: Option<&str>,
    parser: Option<Parser>,
    to: &Dsn,
    connector: Option<&'static str>,
    opc_model_config: Option<Arc<PointModelConfig>>,
    lush_model_config: Option<Arc<LushModelConfig>>,
    cancel: &CancellationToken,
    with_agent: Option<Via>,
    task_job_id: Option<(i64, i64)>,
    notify: crate::TaskNotifySender,
    persist_config: Option<PersistConfig>,
) -> anyhow::Result<(IpcHandler, std::net::SocketAddr)> {
    tracing::info!(ipc.target = % mask_dsn(to), "build ipc listener");
    if let Some(with_agent) = with_agent {
        sink::listen_tcp_socket_with_agent(
            socket,
            cancel.clone(),
            with_agent,
            opc_model_config,
            persist_config,
        )
        .in_current_span()
        .await
    } else {
        let pool = {
            let builder = taos::TaosBuilder::from_dsn(to)?;
            let mut pool_config = builder.default_pool_config();
            let timeout = match parser.clone() {
                Some(parser) => {
                    parser
                        .global()
                        .process_on_abnormal
                        .connection_timeout_in_second_value
                }
                None => 30,
            };
            pool_config.timeouts.wait = Some(Duration::from_secs(timeout as u64));
            builder.with_pool_config(pool_config)?
        };
        let _ = pool.get().await.context("Target connection error")?;
        sink::listen_tcp_socket(
            pool,
            socket,
            opc_model_config,
            lush_model_config,
            cancel.clone(),
            with_agent,
            parser,
            connector,
            task_job_id,
            notify,
            persist_config,
        )
        .in_current_span()
        .await
    }
}

pub async fn list_datasets_from(data: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let data_clone = data.clone();
    let from = if let Some(from_json) = data_clone.from_json {
        json_to_dsn(&from_json)?
    } else if let Some(from) = data_clone.from {
        from.into_dsn()?
    } else {
        anyhow::bail!("from is required");
    };
    tracing::debug!(driver = %from.driver, dsn = %from, "list_datasets_from resolved DSN");
    match from.driver.as_str() {
        "tmq" | "sync" => {
            let mut from = from.clone();
            from.driver = "tmq".to_string();
            // get tmq list
            let builder = TaosBuilder::from_dsn(&from)?.build().await?;
            let mut topics: Vec<_> = builder
                .query("show topics")
                .await?
                .deserialize::<String>()
                .map_ok(|id| DataSet {
                    id,
                    name: None,
                    category: Some("topic".to_string()),
                    r#type: None,
                    options: None,
                    format: None,
                })
                .try_collect()
                .await?;
            let databases: Vec<_> = builder
                .query("show topics")
                .await?
                .deserialize::<String>()
                .map_ok(|id| DataSet {
                    id,
                    name: None,
                    category: Some("database".to_string()),
                    r#type: None,
                    options: None,
                    format: None,
                })
                .try_collect()
                .await?;

            topics.extend(databases);
            Ok(topics)
        }
        "opc" | "opcua" | "opcda" => opc_datasets(data).await,
        "influxdb" => influxdb_datasets(from).await,
        "opentsdb" => opentsdb_datasets(from).await,
        "kinghist" => {
            // kinghistorian: delegate to registered hook to avoid circular deps
            tracing::info!(dsn = %from, "kinghist datasets listing requested");
            match KINGHIST_DATASETS_HOOK.get() {
                Some(f) => {
                    tracing::debug!("kinghist datasets lister hook found; delegating");
                    let from = from.clone();
                    let data_clone = data.clone();
                    tokio::task::spawn_blocking(move || f(&from, &data_clone))
                        .await
                        .context("kinghist datasets lister task join error")?
                }
                None => {
                    tracing::warn!("kinghist datasets lister hook not registered; returning error");
                    anyhow::bail!(
                        "KingHistorian datasets lister is not registered; please enable and register it at startup"
                    )
                }
            }
        }
        _ => Ok(vec![]),
    }
}

pub async fn query_data_source(request: QueryDataSourceReq) -> anyhow::Result<String> {
    async fn query_data_source_inner(request: QueryDataSourceReq) -> anyhow::Result<String> {
        let dsn = json_to_dsn(&request.from)?;
        match dsn.driver.as_str() {
            "pi" | "pibackfill" => runners::pi::query_data_source(dsn, request.args).await,
            _ => unimplemented!(),
        }
    }

    let timeout = Duration::from_secs(5 * 59);
    tokio::time::timeout(timeout, query_data_source_inner(request))
        .await
        .map_err(|err| anyhow::anyhow!("query data source timeout, cause: {:?}", err))?
}
