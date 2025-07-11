use anyhow::bail;
use chrono::{Local, NaiveDateTime};
use futures_util::TryStreamExt;
use linked_hash_map::LinkedHashMap;
use serde_json::json;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use taos::Dsn;
use tiberius::{ColumnType, Row};
use tokio_util::sync::CancellationToken;

use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::raw_data::RawDataLogger;
use taosx_core::plugins::transform::sample::DsSampleIn;
use taosx_core::utils::port_pool::PortPool;
use taosx_core::{Action, Parser, Transferred, build_ipc};

use config::ConnectConfig;
use config::TaskConfig;
use query::HistorianQuery;
use worker::column_meta::ColumnMeta;
use worker::{migrate_history, sync_history, sync_live};

mod config;
mod query;
mod worker;

const AVEVA_HISTORIAN_DRIVER: &str = "historian";
pub const AVEVA_HISTORIAN_ID: &str = "avevaHistorian";
pub const AVEVA_HISTORIAN_NAME: &str = "AVEVA Historian";

fn assert_driver(dsn: &Dsn) -> anyhow::Result<()> {
    if dsn.driver != AVEVA_HISTORIAN_DRIVER && dsn.driver != AVEVA_HISTORIAN_ID {
        bail!(
            "invalid driver of dsn: {}, expect: {}",
            dsn,
            AVEVA_HISTORIAN_DRIVER
        );
    }

    Ok(())
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum TaskMode {
    Synchronize,
    Migrate,
}

impl Display for TaskMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let to_string = match self {
            TaskMode::Synchronize => "synchronize",
            TaskMode::Migrate => "migrate",
        };
        write!(f, "{}", to_string)
    }
}

impl FromStr for TaskMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "synchronize" => Ok(Self::Synchronize),
            "migrate" => Ok(Self::Migrate),
            _ => Err(anyhow::anyhow!(
                "invalid task mode: {}, must be synchronize or migrate",
                s
            )),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum HistorianTable {
    History,
    Live,
}

impl Display for HistorianTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let to_string = match self {
            HistorianTable::Live => "Runtime.dbo.Live",
            HistorianTable::History => "Runtime.dbo.History",
        };
        write!(f, "{}", to_string)
    }
}

impl FromStr for HistorianTable {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Runtime.dbo.History" => Ok(Self::History),
            "Runtime.dbo.Live" => Ok(Self::Live),
            _ => Err(anyhow::anyhow!(
                "invalid historian table: {}, must be Runtime.dbo.History or Runtime.dbo.Live",
                s
            )),
        }
    }
}

/// check historian dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn).await {
        Ok(_) => DataSourceValidation::valid(AVEVA_HISTORIAN_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(AVEVA_HISTORIAN_ID.to_string(), err.to_string()),
    }
}

pub async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    assert_driver(dsn)?;

    let connect = ConnectConfig::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("invalid dsn: {}, cause: {}", dsn, err))?;

    let _client = HistorianQuery::try_connect(connect).await.map_err(|err| {
        anyhow::anyhow!(
            "failed to connect to dsn: {}, cause: {}",
            dsn,
            err.to_string()
        )
    })?;

    Ok(())
}

/// get sample data from historian
/// # Arguments
/// * `dsn` - historian dsn
/// # Returns
/// * `DsSampleIn` - {
///   "input": [{ "col_name": "xxx", ... }],
///   "parser": {"parse": {
///   "col_name": { "as": col_type }, ...
///   }}
///   }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    assert_driver(dsn)?;
    let config = TaskConfig::from_dsn(dsn)?;
    let mut client = HistorianQuery::try_connect(config.connect).await?;

    // input: get top N record from table
    let mut input_sample: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();

    let tags_condition = config.tags.clone();
    let mut rows = client
        .select_top_n(
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
        ColumnType::BigVarBin | ColumnType::BigBinary => json!(row.try_get::<&[u8], _>(idx)?),
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
    notify: taosx_core::TaskNotifySender,
) -> anyhow::Result<()> {
    assert_driver(&from)?;
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
    let (mut ipc, _) = build_ipc(
        Some(&socket),
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
        None,
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
    let mut client = HistorianQuery::try_connect(config.connect.clone()).await?;

    let conditions = config.tags.clone();
    let mut rows = client
        .select_tags_with_condition(None, conditions)
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
        bail!("valid TagName is None, tags: {:?}", config.tags.clone());
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
            .unwrap_or(std::env::var(taosx_core::runners::ENV_TAOSX_DATA_DIR).unwrap()),
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
    use super::*;
    use taos::IntoDsn;

    #[test]
    fn test_assert_driver() {
        let dsn = Dsn::from_str("historian://localhost").unwrap();
        let res = assert_driver(&dsn);
        assert!(res.is_ok());

        let dsn = Dsn::from_str("avevaHistorian://localhost").unwrap();
        let res = assert_driver(&dsn);
        assert!(res.is_ok());

        let dsn = Dsn::from_str("mssql://localhost").unwrap();
        let res = assert_driver(&dsn);
        assert!(res.is_err());
        assert_eq!(
            "invalid driver of dsn: mssql://localhost, expect: historian",
            res.unwrap_err().to_string()
        );
    }
    #[test]
    fn test_from_str_of_task_mode() {
        let config = TaskMode::from_str("synchronize").unwrap();
        assert_eq!(TaskMode::Synchronize, config);

        let config = TaskMode::from_str("migrate").unwrap();
        assert_eq!(TaskMode::Migrate, config);

        let config = TaskMode::from_str("xxx");
        assert!(config.is_err());
        assert_eq!(
            "invalid task mode: xxx, must be synchronize or migrate",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_from_str_of_historian_table() {
        let t = HistorianTable::from_str("Runtime.dbo.History").unwrap();
        assert_eq!(HistorianTable::History, t);

        let t = HistorianTable::from_str("Runtime.dbo.Live").unwrap();
        assert_eq!(HistorianTable::Live, t);

        let t = HistorianTable::from_str("xxx");
        assert!(t.is_err());
        assert_eq!(
            "invalid historian table: xxx, must be Runtime.dbo.History or Runtime.dbo.Live",
            t.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("historian://?mode=migrate&table=Runtime.dbo.History").unwrap();
        let config = TaskConfig::parse_table(&dsn).unwrap();
        assert_eq!(HistorianTable::History, config);
    }

    #[tokio::test]
    async fn test_is_valid() {
        // given
        let dsn = Dsn::from_str("historian://localhost").unwrap();
        // when
        let res = is_valid(&dsn).await;
        // then
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!(AVEVA_HISTORIAN_ID, res.data_source);
        assert_eq!(
            "invalid dsn: historian://localhost, cause: username is required",
            res.message.unwrap()
        );

        // given
        let dsn = Dsn::from_str("historian://aaAdmin@localhost").unwrap();
        // when
        let res = is_valid(&dsn).await;
        // then
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!(AVEVA_HISTORIAN_ID, res.data_source);
        assert_eq!(
            "invalid dsn: historian://aaAdmin@localhost, cause: password is required",
            res.message.unwrap()
        );

        // given
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@127.0.0.1").unwrap();
        // when
        let res = is_valid(&dsn).await;
        // then
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!(AVEVA_HISTORIAN_ID, res.data_source);
        let err_msg = res.message.unwrap();
        assert!(err_msg.starts_with(
            "failed to connect to dsn: historian://aaAdmin:aaAdmin@127.0.0.1, cause:"
        ));
    }

    #[tokio::test]
    async fn test_get_sample() {
        // given
        let dsn = format!(
            "historian://aaAdmin:aaAdmin@localhost:1433?mode={}&table={}&beginDateTime={}",
            TaskMode::Synchronize,
            HistorianTable::History,
            "2021-01-01T00:00:00Z"
        )
        .into_dsn()
        .unwrap();
        // when
        let actual = get_sample(&dsn).await;
        // then
        assert!(actual.is_err());
    }

    #[tokio::test]
    async fn test_historian_to_taos() {
        let (tx, _rx) = flume::bounded(1);
        // when
        let res = historian_to_taos(
            "historian://".into_dsn().unwrap(),
            None,
            vec![],
            "taos+ws://192.168.0.201/".into_dsn().unwrap(),
            1,
            &PortPool::default(),
            CancellationToken::default(),
            None,
            None,
            None,
            tx,
        )
        .await;
        // then
        assert!(res.is_err())
    }
}
