use std::sync::Arc;

use anyhow::Context;
use futures::TryStreamExt;
use taos::Dsn;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing::Instrument;
use tracing::Span;

use crate::dsv::DataSourceValidation;
use crate::utils::mask_dsn;
use crate::Transferred;
pub use runners::mqtt::mqtt_to_taos;
use runners::opc::opc_datasets;
pub use runners::opc::opc_to_taos;
pub use runners::opc::ColumnConfig;
pub use runners::opc::OPCConfig;
pub use runners::opc::TableConfig;
pub use runners::opentsdb::opentsdb_datasets;
pub use runners::opentsdb::opentsdb_to_taos;
use runners::pi::pi_datasets;
pub use runners::pi::pi_to_taos;
pub use runners::{
    get_data_dir, get_file_upload_home_dir, get_log_dir, get_log_keep_days, get_plugins_info,
    set_env_data_dir, set_env_log_home_dir, set_env_log_keep_days, set_env_plugins_home_dir,
};
pub use sink::IpcStreamWorker;
pub use taosx_ipc::types::*;
pub use transform::Parser;
use crate::runners::influxdb::influxdb_datasets;

use self::runners::opc::OpcTableConfig;
use self::sink::IpcHandler;

mod config;
pub mod runners;
mod service;
pub(crate) mod sink;
mod source;
mod transform;

/// ipc stream metrics
/// be careful to modify, in case other crate use string value. for now POINTS value used in taosx-ipc.
pub const RECORD_BATCHES: &str = "ipc.stream.record_batches";
pub const BATCH_RECORDS: &str = "ipc.stream.batch_records";
pub const INSERT_SQLS: &str = "ipc.stream.insert_sqls";
pub const INSERT_SQL_FAILS: &str = "ipc.stream.insert_sql_fails";
// pub const STABLE_CREATED: &str = "ipc.stream.stable_created";
// pub const CHILD_TABLE_CREATED: &str = "ipc.stream.child_table_created";
pub const RECORDS: &str = "ipc.stream.records";
pub const RECORD_FAILS: &str = "ipc.stream.record_fails";
pub const POINTS: &str = "ipc.stream.points";
pub const POINT_FAILS: &str = "ipc.stream.point_fails";
pub const WRITE_RAW_BLOCKS: &str = "ipc.stream.write_raw_blocks";
pub const WRITE_RAW_BLOCK_FAILS: &str = "ipc.stream.write_raw_blocks_fails";

#[instrument(skip_all, fields(ipc.listen = socket, ipc.target = % mask_dsn(to)))]
pub async fn build_ipc(
    socket: &str,
    parser: Option<Parser>,
    to: &Dsn,
    connector: Option<&'static str>,
    config: Option<OpcTableConfig>,
    cancel: &CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<IpcHandler> {
    let ipc = if with_agent.is_none() {
        let builder = taos::TaosBuilder::from_dsn(to)?;
        let pool = builder.pool()?;
        if with_agent.is_none() {
            let _ = pool.get().await.context("Target connection error")?;
        }
        sink::listen_tcp_socket(
            pool,
            socket,
            // sender,
            config,
            cancel.clone(),
            with_agent,
            parser,
            connector,
            transferred,
            span,
            task_id,
            notify,
        )
        .in_current_span()
        .await?
    } else {
        sink::listen_tcp_socket_with_agent(socket, cancel.clone(), with_agent.unwrap(), config)
            .in_current_span()
            .await?
    };
    Ok(ipc)
}

pub async fn list_datasets_from(data: &DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    let from = data.from.clone().into_dsn()?;
    match from.driver.as_str() {
        "tmq" => {
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
            return Ok(topics);
        }
        "pi" | "pibackfill" => {
            // pi
            return pi_datasets(data).await;
        }
        "opc" | "opcua" | "opcda" => {
            // opc
            return opc_datasets(data).await;
        }
        "influxdb" => {
            // influxdb
            return influxdb_datasets(from).await;
        }
        "opentsdb" => {
            // opentsdb
            return opentsdb_datasets(from).await;
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
        Ok(dsn) => {
            match dsn.driver.as_str() {
                // TODO: clickhouse
                "historian" => runners::historian::is_valid(&dsn).await,
                "influxdb" => runners::influxdb::is_valid(&dsn).await,
                "kafka" => runners::kafka::is_valid(&dsn).await,
                "mqtt" => runners::mqtt::is_valid(&dsn).await,
                "opc" | "opcda" | "opcua" => runners::opc::is_valid(&dsn).await,
                "opentsdb" => runners::opentsdb::is_valid(&dsn).await,
                "pi" => runners::pi::is_pi_valid(&dsn).await,
                "pibackfill" => runners::pi::is_pi_backfill_valid(&dsn).await,
                "taos" => crate::taoz::is_taos_valid(&dsn).await,
                "tmq" => crate::tmq::is_tmq_valid(&dsn).await,
                "csv" => crate::csv::is_csv_valid(&dsn).await,
                &_ => DataSourceValidation::unknown(),
            }
        }
    }
}
