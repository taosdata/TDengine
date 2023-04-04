use std::{path::PathBuf, num::ParseIntError};

use itertools::Itertools;
use taos::{Dsn, TBuilder, TaosBuilder};

use crate::{plugins::service::spawn_rest_service, Action};

mod config;
mod service;
mod sink;
mod source;
mod transform;
mod port_pool;

#[derive(Debug, serde::Serialize)]
struct PiConfig {
    // system
    #[serde(rename = "PIServerName")]
    server_name: String,
    #[serde(rename = "PISystemName")]
    system_name: String,
    #[serde(rename = "AFDatabaseName")]
    database: String,
    #[serde(rename = "PIDataPipesInstances")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pi_data_pipes_instances: Option<u32>,
    #[serde(rename = "AFDataPipesInstances")]
    #[serde(skip_serializing_if = "Option::is_none")]
    af_data_pipes_instances: Option<u32>,
    // runtime
    #[serde(rename = "MaxWaitLen")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_wait_len: Option<u32>,
    #[serde(rename = "UpdateInterval")]
    #[serde(skip_serializing_if = "Option::is_none")]
    update_interval: Option<u32>,
    #[serde(rename = "MaxBackfillRangeDays")]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_backfill_range_days: Option<u32>,

    #[serde(rename = "IPCStream")]
    ipc_stream: String,
    #[serde(rename = "SQLAPI")]
    sql_api: String,
    // data set
    #[serde(rename = "TemplateForPIPoint")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_pi_point: Vec<String>,
    #[serde(rename = "TemplateForAFElement")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    template_for_af_element: Vec<String>,
    #[serde(rename = "Points")]
    #[serde(skip_serializing_if = "Option::is_none")]
    points: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum PiError {
    #[error("Server is required in PI dsn: {0}")]
    ServerIsRequired(Dsn),
    #[error("Database name is required in PI dsn: {0}")]
    DatabaseIsRequired(Dsn),
    #[error("Parse integer error from {1} while parsing parameter {0}: {:?}")]
    ParseNumberError(&'static str, String, ParseIntError),
}

impl PiConfig {
    pub fn new(dsn: Dsn) -> Result<Self, PiError> {
        debug_assert!(dsn.driver == "id");
        let server_name = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host)
            .ok_or_else(|| PiError::ServerIsRequired(dsn))?;
        let system_name = dsn
            .remove("PISystemName")
            .unwrap_or_else(|| server_name.clone());
        let database = dsn
            .subject
            .ok_or_else(|| PiError::DatabaseIsRequired(dsn))?;
        const PI_DATA_PIPES_INSTANCES: &str = "PIDataPipesInstances";
        const AF_DATA_PIPES_INSTANCES: &str = "AFDataPipesInstances";
        const MAX_WAIT_LEN: &str = "MaxWaitLen";

        macro_rules! parse_int_at {
            ($n:expr) => {
                dsn
                    .remove($n)
                    .map(|v| v.parse::<u32>().map_err(|err| PiError::ParseNumberError($n, v, err)))
                    .transpose()?;
            };
        }
        let pi_data_pips_instances = dsn
            .remove(PI_DATA_PIPES_INSTANCES)
            .map(|v| v.parse::<u32>().map_err(|err| PiError::ParseNumberError(PI_DATA_PIPES_INSTANCES, v, err)))
            .transpose()?;

        let af_data_pips_instances = dsn
            .remove(AF_DATA_PIPES_INSTANCES)
            .map(|v| v.parse::<u32>().map_err(|err| PiError::ParseNumberError(AF_DATA_PIPES_INSTANCES, v, err)))
            .transpose()?;

        let max_wait_len = parse_int_at!("MaxWaitLen");
        let update_interval = parse_int_at!("UpdateInterval");
        let max_backfill_range_days = parse_int_at!("MaxBackfillRangeDays");

        let template_for_pi_point = dsn.remove("TemplateForPIPoint").unwrap_or_default().split(',').map(|s| s.trim())
            .filter(|s| !s.is_empty()).map(|s| s.to_string()).collect_vec();
        let template_for_pi_point = dsn.remove("TemplateForAFElement").unwrap_or_default().split(',').map(|s| s.trim())
            .filter(|s| !s.is_empty()).map(|s| s.to_string()).collect_vec();
        let points = dsn.remove("Points").map(|s| Path::new(s).to_path_buf());

        let ipc_stream = format!("0");
        let sql_api = format!("0");
        // dsn.addresses
        Ok(Self {
            server_name,
            system_name,
            database,
            pi_data_pipes_instances,
            af_data_pipes_instances,
            max_wait_len,
            update_interval,
            max_backfill_range_days,
            ipc_stream,
            sql_api,
            template_for_pi_point,
            template_for_af_element,
            points,
        })
    }
}

pub async fn pi_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
) -> anyhow::Result<()> {
    println!("# plugin: PI");
    let target_pool = TaosBuilder::from_dsn(to)?.pool()?;
    // let server = spawn_rest_service(target_pool, 6050).await?;
    // tokio::spawn(future);
    // [6070-16070];

    // let cmd = std::process::Command::new("opc").arg(arg);
    Ok(())
}
