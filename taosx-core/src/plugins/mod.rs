mod config;
mod service;
mod sink;
mod source;
mod transform;

mod runners;

use anyhow::bail;
use futures::TryStreamExt;
pub use runners::opc::opc_to_taos;
use runners::opc::opc_datasets;
pub use runners::pi::pi_to_taos;
pub use runners::mqtt::mqtt_to_taos;
use serde::{Deserialize, Serialize};
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};

use crate::plugins::runners::pi::pi_datasets;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataSet {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    r#type: Option<String>,
}

pub async fn list_datasets_from(from: impl IntoDsn) -> anyhow::Result<Vec<DataSet>> {
    let from = from.into_dsn()?;
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
                })
                .try_collect()
                .await?;

            topics.extend(databases);
            return Ok(topics);
        }
        "pi" => {
            // pi
            return pi_datasets(&from).await;
        }
        "opc" => {
            // opc
            return opc_datasets(&from).await;
        }
        _ => {
            bail!("Unsupported data source: {}", from);
        }
    }
}
