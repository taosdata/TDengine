use std::sync::Arc;
use std::time::Duration;

use chrono::{Local, NaiveDateTime};
use futures_util::TryStreamExt;
use linked_hash_map::LinkedHashMap;
use serde_json::json;
use taos::Dsn;
use tiberius::{ColumnType, Row};
use tokio_util::sync::CancellationToken;

use crate::dsv::DataSourceValidation;
use crate::plugins::raw_data::RawDataLogger;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::historian::appender::column_meta::ColumnMeta;
use crate::runners::historian::config::connect::ConnectConfig;
use crate::runners::historian::config::{HistorianTable, TaskConfig, TaskMode};
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::worker::{migrate_history, sync_history, sync_live};
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

mod appender;
mod config;
mod query;
mod worker;

pub const AVEVA_HISTORIAN_ID: &str = "avevaHistorian";
pub const AVEVA_HISTORIAN_NAME: &str = "AVEVA Historian";

/// check historian dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            AVEVA_HISTORIAN_ID.to_string(),
            format!("invalid dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let client = HistorianQuery::try_new(c).await;
            match client {
                Err(err) => DataSourceValidation::invalid(
                    AVEVA_HISTORIAN_ID.to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(_cli) => DataSourceValidation::valid(AVEVA_HISTORIAN_ID.to_string(), None),
            }
        }
    }
}

/// get sample data from historian
/// # Arguments
/// * `dsn` - historian dsn
/// # Returns
/// * `DsSampleIn` - {
///     "input": [{ "col_name": "xxx", ... }],
///     "parser": {"parse": {
///         "col_name": { "as": col_type }, ...
///     }}
///   }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    let config = TaskConfig::from_dsn(dsn)?;
    let mut client = HistorianQuery::try_new(config.connect).await?;

    // input: get top N record from table
    let mut input_sample: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();

    let tags_condition = config.tags.clone();
    let mut rows = client
        .top_n(
            config.sample_data_limit,
            config.table,
            tags_condition,
            config.begin_datetime,
            config.end_datetime,
        )
        .await?
        .into_row_stream();
    while let Some(row) = rows.try_next().await? {
        let mut sample_map: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();
        for (idx, col) in row.columns().iter().enumerate() {
            let col_name = col.name();
            let col_type = col.column_type();
            let col_val = to_json_value(&row, idx, col_type).map_err(|err| {
                anyhow::anyhow!(
                    "failed to convert column value, index: {}, type: {:?}, cause: {}",
                    idx,
                    col_type,
                    err.to_string(),
                )
            })?;

            sample_map.insert(col_name.to_string(), col_val);
        }

        input_sample.push(sample_map);
    }
    drop(rows);

    // parser.parse: describe table
    let mut rows = client.describe_table(config.table).await?.into_row_stream();
    let mut parse_sample = LinkedHashMap::new();
    while let Some(row) = rows.try_next().await? {
        let column_meta = ColumnMeta::try_new(&row)?;
        let ipc_type = column_meta.get_ipc_type()?;
        parse_sample.insert(column_meta.column_name, json!({"as": ipc_type}));
    }
    drop(rows);

    let sample_json = json!({
        "input": input_sample,
        "parser": {
            "parse": parse_sample,
        }
    });
    let ds_sample_in: DsSampleIn = serde_json::from_value(sample_json.clone()).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse sample data, cause: {}, value: {:?}",
            err.to_string(),
            sample_json
        )
    })?;

    Ok(ds_sample_in)
}

fn to_json_value(row: &Row, idx: usize, col_type: ColumnType) -> anyhow::Result<serde_json::Value> {
    let col_val = match col_type {
        ColumnType::Datetime2 => json!(row.try_get::<NaiveDateTime, _>(idx)?),
        ColumnType::Int1 => json!(row.try_get::<u8, _>(idx)?),
        ColumnType::Int2 => json!(row.try_get::<i16, _>(idx)?),
        ColumnType::Int4 | ColumnType::Intn => json!(row.try_get::<i32, _>(idx)?),
        ColumnType::Int8 => json!(row.try_get::<i64, _>(idx)?),
        ColumnType::Float4 => json!(row.try_get::<f32, _>(idx)?),
        ColumnType::Floatn | ColumnType::Float8 => json!(row.try_get::<f64, _>(idx)?),
        ColumnType::NVarchar => json!(row.try_get::<&str, _>(idx)?),
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported column index: {}, type: {:?}",
                idx,
                col_type,
            ));
        }
    };

    Ok(col_val)
}

/// migrate or synchronize data from historian to taos

pub async fn historian_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    _transform: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let mut config = TaskConfig::from_dsn(&from)?;
    // set task_id
    config.task_id = task_id;
    tracing::info!(
        "{AVEVA_HISTORIAN_NAME} task start, id: {:?}, configuration: {:?}",
        task_id,
        config
    );

    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for connection"))?;
    let socket = format!("127.0.0.1:{}", port);
    // set ipc port
    config.ipc_port = Some(port.get());

    // create ipc handler
    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some(AVEVA_HISTORIAN_ID),
        None,
        None,
        &cancel,
        with_agent,
        transferred,
        task_id,
        notify,
    )
    .await?;

    // create worker
    let worker = tokio::spawn(exec_task(config));

    let abort_handle = worker.abort_handle();
    tokio::spawn(async move {
        tokio::select! {
            status = worker => {
                match status? {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        match ipc.try_recv_error() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                anyhow::bail!("{AVEVA_HISTORIAN_NAME} exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("{AVEVA_HISTORIAN_NAME} done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("{AVEVA_HISTORIAN_NAME} exit with error: {:#}", err);
                    }
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                abort_handle.abort();
                if let Some(err) = err {
                    let _ = ipc.send(());
                    let _ = ipc.close().await;
                    abort_handle.abort();
                    anyhow::bail!("{AVEVA_HISTORIAN_NAME} writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{AVEVA_HISTORIAN_NAME} task cancelled, id: {}", task_id.unwrap_or(-1));
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("{AVEVA_HISTORIAN_NAME} task done, id: {}", task_id.unwrap_or(-1));
        ipc.close().await?;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

async fn exec_task(mut config: TaskConfig) -> anyhow::Result<()> {
    let mut client = HistorianQuery::try_new(config.connect.clone()).await?;

    let conditions = config.tags.clone();
    let mut rows = client
        .get_tags_with_condition(None, conditions)
        .await?
        .into_row_stream();

    let mut tag_name_list = Vec::new();
    while let Some(row) = rows.try_next().await? {
        let tag_name = row
            .try_get::<&str, _>("TagName")?
            .ok_or(anyhow::anyhow!("TagName cannot be None"))?;
        tag_name_list.push(tag_name.to_string());
    }
    drop(rows);

    if tag_name_list.is_empty() {
        anyhow::bail!("valid TagName is None, tags: {:?}", config.tags.clone());
    }

    // keep_raw_data log
    let (logger_tx, logger_rx) = flume::bounded(0);

    let task_id = config.task_id.unwrap_or_else(|| {
        tracing::warn!(
            "task_id is None, this task may be in run mode, use current timestamp as task_id"
        );
        Local::now().timestamp()
    });

    let logger = RawDataLogger::new(
        task_id,
        config.advanced_options.keep_raw_data.unwrap_or(false),
        config
            .advanced_options
            .keep_raw_data_dir
            .clone()
            .unwrap_or(std::env::var(crate::runners::ENV_TAOSX_DATA_DIR).unwrap()),
        config.advanced_options.keep_raw_data_days.unwrap_or(30),
        logger_rx,
    );
    logger.start();

    match (config.mode, config.table) {
        (TaskMode::Migrate, HistorianTable::History) => {
            config.tags = tag_name_list;
            migrate_history(config, logger_tx).await?;
        }
        (TaskMode::Synchronize, HistorianTable::History) => {
            config.tags = tag_name_list;
            sync_history(config, logger_tx).await?;
        }
        (TaskMode::Synchronize, HistorianTable::Live) => {
            sync_live(config, logger_tx).await?;
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::str::FromStr;

    use chrono::{DateTime, Utc};
    use rand::Rng;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("historian://localhost").unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("historian", res.data_source);
        assert_eq!(
            "invalid dsn: historian://localhost, cause: username is required",
            res.message.unwrap()
        );

        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@127.0.0.1").unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("historian", res.data_source);
        assert_eq!("failed to connect to dsn: historian://aaAdmin:aaAdmin@127.0.0.1, cause: Connection refused (os error 61)", res.message.unwrap());
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433/").unwrap();
        let res = is_valid(&dsn).await;
        assert!(res.valid);
        assert!(res.support);
        assert_eq!("historian", res.data_source);
        assert_eq!(None, res.version);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_sample() {
        let dsn = Dsn::from_str(
            "historian://aaAdmin:aaAdmin@192.168.3.40:1433?mode=synchronize&table=Runtime.dbo.History",
        )
            .unwrap();
        let actual: DsSampleIn = get_sample(&dsn).await.unwrap();
        println!("{}", serde_json::to_string_pretty(&actual).unwrap());

        let dsn = Dsn::from_str(
            "historian://aaAdmin:aaAdmin@192.168.3.40:1433?mode=synchronize&table=Runtime.dbo.Live",
        )
        .unwrap();
        let actual: DsSampleIn = get_sample(&dsn).await.unwrap();
        println!("{}", serde_json::to_string_pretty(&actual).unwrap());
    }

    /// generate test data, Only for local test
    #[ignore]
    #[tokio::test]
    async fn generate_historian_tag_csv() -> anyhow::Result<()> {
        let tag_index = 8;
        let total_records = 10359;
        let gap_sec = 2;

        let mut file = File::create(format!(
            "tag{}_{}_{}sec.csv",
            tag_index, total_records, gap_sec
        ))?;
        file.write_all(b"ASCII\n")?;
        file.write_all(b"|\n")?;
        file.write_all(b"Win10-2021XIVKQ|1|Server Local|1|1\n")?;

        let ts = (Utc::now() - chrono::Duration::days(5)).timestamp();
        let mut rng = rand::thread_rng();

        for i in 0..total_records {
            let dt: DateTime<Utc> = DateTime::from_timestamp(ts + (i * gap_sec), 0).unwrap();
            let date_time = dt.format("%Y/%m/%d|%H:%M:%S").to_string();
            let date_value = rng.gen_range(0.0..100.0);
            file.write_all(
                format!("tag{}|0|{}|1|{}|192\n", tag_index, date_time, date_value).as_bytes(),
            )?;
        }
        Ok(())
    }
}
