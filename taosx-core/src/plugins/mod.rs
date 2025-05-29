use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sink::persist::PersistConfig;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing::Instrument;
use tracing::Span;
use transform::sample::DsSamples;

use crate::dsv::DataSourceValidation;
use crate::runners::influxdb::influxdb_datasets;
use crate::utils::dsn::json_to_dsn;
use crate::utils::mask_dsn;
use crate::Transferred;
pub use runners::mqtt::mqtt_to_taos;
use runners::opc::model::OpcModelConfig;
use runners::opc::opc_datasets;
pub use runners::opc::opc_to_taos;
pub use runners::opentsdb::opentsdb_datasets;
pub use runners::opentsdb::opentsdb_to_taos;
pub use runners::pi::pi_to_taos;
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

use self::sink::lush::LushModelConfig;
use self::sink::IpcHandler;

mod config;
pub(crate) mod expr;
mod raw_data;
pub mod runners;
mod service;
pub mod sink;
mod source;
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

#[instrument(skip_all)]
pub async fn build_ipc(
    socket: Option<&str>,
    parser: Option<Parser>,
    to: &Dsn,
    connector: Option<&'static str>,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: &CancellationToken,
    with_agent: Option<(i64, String, String)>,
    _transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
    persist_config: Option<PersistConfig>,
) -> anyhow::Result<(IpcHandler, std::net::SocketAddr)> {
    tracing::info!(ipc.target = % mask_dsn(to), "build ipc listener");
    if with_agent.is_none() {
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
            task_id,
            notify,
            persist_config,
        )
        .in_current_span()
        .await
    } else {
        sink::listen_tcp_socket_with_agent(
            socket,
            cancel.clone(),
            with_agent.unwrap(),
            opc_model_config,
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
        "opc" | "opcua" | "opcda" => {
            // opc
            opc_datasets(data).await
        }
        "influxdb" => {
            // influxdb
            influxdb_datasets(from).await
        }
        "opentsdb" => {
            // opentsdb
            opentsdb_datasets(from).await
        }
        _ => Ok(vec![]),
    }
}

pub async fn validate_dsn(dsn: impl IntoDsn) -> DataSourceValidation {
    let dsn = dsn.into_dsn();
    match dsn {
        Err(err) => {
            DataSourceValidation::invalid("unknown".to_string(), format!("invalid dsn: {}", err))
        }
        Ok(dsn) => match dsn.driver.as_str() {
            runners::historian::AVEVA_HISTORIAN_ID => runners::historian::is_valid(&dsn).await,
            "influxdb" => runners::influxdb::is_valid(&dsn).await,
            runners::kafka::KAFKA_ID => runners::kafka::is_valid(&dsn).await,
            runners::mqtt::MQTT_ID => runners::mqtt::is_valid(&dsn).await,
            runners::sparkplugb::SPARKPLUGB_ID => {
                runners::sparkplugb::validate::is_valid(&dsn).await
            }
            "opc" | "opcda" | "opcua" => runners::opc::is_valid(&dsn).await,
            "opentsdb" => runners::opentsdb::is_valid(&dsn).await,
            "pi" | "pibackfill" => runners::pi::is_pi_valid(&dsn).await,
            "taos" => crate::taoz::is_taos_valid(&dsn).await,
            "tmq" | "sync" => {
                let mut dsn = dsn.clone();
                dsn.driver = "tmq".to_string();
                crate::tmq::is_tmq_valid(&dsn).await
            }
            "csv" => crate::csv::is_csv_valid(&dsn).await,
            "local" => crate::local_to_taos::is_local_valid(&dsn).await,
            runners::mysql::MYSQL_ID => runners::mysql::is_valid(&dsn).await,
            runners::postgres::POSTGRES_ID => runners::postgres::is_valid(&dsn).await,
            runners::oracle::ORACLE_ID => runners::oracle::is_valid(&dsn).await,
            runners::mssql::MSSQL_ID => runners::mssql::is_valid(&dsn).await,
            runners::mongodb::MONGODB_ID => runners::mongodb::is_valid(&dsn).await,
            &_ => DataSourceValidation::unknown(),
        },
    }
}

pub async fn get_sample(dsn: impl IntoDsn) -> anyhow::Result<DsSamples> {
    let dsn = dsn
        .into_dsn()
        .map_err(|err| anyhow::format_err!("invalid dsn, cause: {err}"))?;
    match dsn.driver.as_str() {
        runners::historian::AVEVA_HISTORIAN_ID => runners::historian::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        runners::kafka::KAFKA_ID => {
            let limit = parse_sample_limit(&dsn);
            let timeout = parse_sample_timeout(&dsn);
            runners::kafka::get_sample(&dsn, limit, timeout)
                .await
                .map(DsSamples::Simple)
        }
        runners::mqtt::MQTT_ID => {
            let limit = parse_sample_limit(&dsn);
            let timeout = parse_sample_timeout(&dsn);
            runners::mqtt::get_sample(&dsn, limit, timeout)
                .await
                .map(DsSamples::Simple)
        }
        runners::sparkplugb::SPARKPLUGB_ID => {
            let limit = parse_sample_limit(&dsn);
            let timeout = parse_sample_timeout(&dsn);
            runners::sparkplugb::sample::get_sample(&dsn, limit, timeout)
                .await
                .map(DsSamples::MultiSchema)
        }
        runners::mysql::MYSQL_ID => runners::mysql::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        runners::postgres::POSTGRES_ID => runners::postgres::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        runners::oracle::ORACLE_ID => runners::oracle::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        runners::mssql::MSSQL_ID => runners::mssql::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        runners::mongodb::MONGODB_ID => runners::mongodb::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        s => Err(anyhow::anyhow!(
            "get sample from data source {s} is unsupported"
        )),
    }
}

fn parse_sample_limit(dsn: &Dsn) -> usize {
    dsn.params
        .get("get_sample_limit")
        .or(dsn.params.get("sample_data_limit"))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(5)
}

fn parse_sample_timeout(dsn: &Dsn) -> Duration {
    dsn.params
        .get("get_sample_timeout")
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(30))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_sample_limit() {
        let dsn = Dsn::from_str("taos://?get_sample_limit=123").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 123);

        let dsn = Dsn::from_str("taos://?get_sample_limit=").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 5);

        let dsn = Dsn::from_str("taos://").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 5);

        let dsn = Dsn::from_str("taos://?get_sample_limit=abc").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 5);

        let dsn = Dsn::from_str("taos://?sample_data_limit=123").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 123);
    }

    #[test]
    fn test_parse_sample_timeout() {
        let dsn = Dsn::from_str("taos://?get_sample_timeout=123").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(123));

        let dsn = Dsn::from_str("taos://?get_sample_timeout=").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(30));

        let dsn = Dsn::from_str("taos://").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(30));

        let dsn = Dsn::from_str("taos://?get_sample_timeout=abc").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(30));
    }
}
