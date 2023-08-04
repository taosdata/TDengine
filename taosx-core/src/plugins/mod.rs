mod config;
mod service;
pub(crate) mod sink;
mod source;
mod transform;

mod runners;

use std::sync::Arc;

use futures::TryStreamExt;
pub use runners::opc::opc_to_taos;
pub use runners::opc::OPCConfig;
pub use sink::IpcStreamWorker;
pub use runners::kafka::kafka_to_taos;
pub use runners::influxdb::influxdb_to_taos;
pub use runners::influxdb::influxdb_datasets;
pub use runners::mqtt::mqtt_to_taos;
use runners::opc::opc_datasets;
pub use runners::pi::pi_to_taos;
use taos::Dsn;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};
use tokio_util::sync::CancellationToken;

use crate::Transferred;
use crate::plugins::runners::pi::pi_datasets;
pub use taosx_ipc::types::*;

pub use transform::Parser;

pub use runners::{
    get_log_dir, get_log_keep_days, get_plugins_info, valid_env_log_keep_days,
    ENV_TAOSX_LOGS_KEEP_DAYS,
};


pub fn build_ipc(
    socket: &str,
    parser: Option<Parser>,
    to: &Dsn,
    cancel: &CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<(
    std::sync::mpsc::Sender<()>,
    tokio::sync::mpsc::Receiver<String>,
)> {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let ipc = if with_agent.is_none() {
        let builder = taos::TaosBuilder::from_dsn(to)?;
        sink::listen_tcp_socket(
            builder.pool()?,
            socket,
            sender,
            None,
            cancel.clone(),
            with_agent,
            parser,
            None,
            transferred,
        )?
    } else {
        sink::listen_tcp_socket_with_agent(
            socket,
            sender,
            None,
            cancel.clone(),
            with_agent.unwrap(),
        )?
    };
    Ok((ipc, receiver))
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
        _ => Ok(vec![]),
    }
}
