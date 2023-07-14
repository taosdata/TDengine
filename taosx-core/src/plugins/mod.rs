mod config;
mod service;
mod sink;
mod source;
mod transform;

mod runners;

use futures::TryStreamExt;
pub use runners::opc::opc_to_taos;
pub use runners::opc::OPCConfig;
pub use sink::IpcStreamWorker;
pub use runners::kafka::kafka_to_taos;
pub use runners::influxdb::influxdb_to_taos;
pub use runners::mqtt::mqtt_to_taos;
use runners::opc::opc_datasets;
pub use runners::pi::pi_to_taos;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};

use crate::plugins::runners::pi::pi_datasets;
pub use taosx_ipc::types::*;

pub use transform::Parser;

pub use runners::get_plugins_info;
pub use runners::get_log_dir;

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
        _ => Ok(vec![]),
    }
}
