mod config;
mod service;
mod sink;
mod source;
mod transform;

mod runners;

use actix_web::web::Json;
use anyhow::bail;
use futures::TryStreamExt;
pub use runners::opc::opc_to_taos;
pub use runners::opc::OPCConfig;
pub use sink::IpcStreamWorker;

pub use runners::influxdb::influxdb_to_taos;
pub use runners::mqtt::mqtt_to_taos;
use runners::opc::opc_datasets;
pub use runners::pi::pi_to_taos;
use serde::{Deserialize, Serialize};
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};

use crate::plugins::runners::pi::pi_datasets;
pub use taosx_ipc::types::*;

pub use transform::Parser;

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub struct DataSet {
//     id: String,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     name: Option<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     category: Option<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     r#type: Option<String>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     options: Option<Vec<OptionSet>>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     format: Option<String>,
// }

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub struct OptionSet {
//     name: String,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     description: Option<String>,
//     required: bool,
// }

// #[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
// pub struct DataSetsReq {
//     from: String,
//     pub via: Option<i64>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pattern: Option<String>,
//     categories: Vec<String>,
//     offset: usize,
//     limit: usize,
// }

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
        "pi" => {
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
