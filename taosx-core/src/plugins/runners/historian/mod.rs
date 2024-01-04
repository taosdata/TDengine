use std::sync::Arc;
use std::time::Duration;

use taos::Dsn;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::dsv::DataSourceValidation;
use crate::plugins::raw_data::RawDataLogger;
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

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            AVEVA_HISTORIAN_ID.to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let client = HistorianQuery::try_new(c).await;
            match client {
                Err(err) => DataSourceValidation::invalid(
                    AVEVA_HISTORIAN_ID.to_string(),
                    format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn.to_string(),
                        err.to_string()
                    ),
                ),
                Ok(_cli) => DataSourceValidation::valid(AVEVA_HISTORIAN_ID.to_string(), None),
            }
        }
    }
}

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
    span: Span,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let mut config = TaskConfig::from_dsn(&from)?;
    tracing::info!(
        "{AVEVA_HISTORIAN_NAME} task start, id: {}, configuration: {:?}",
        task_id.unwrap_or(-1),
        config
    );

    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for connection"))?;
    let socket = format!("127.0.0.1:{}", port);

    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some(AVEVA_HISTORIAN_ID),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        task_id.clone(),
        notify,
    )
    .await?;
    config.ipc_port = Some(port);

    // create worker
    let worker = tokio::spawn(exec_task(task_id, config));

    let port_pool = port_pool.clone();
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
        // put ipc port back to port pool.
        port_pool.put(port).await;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

async fn exec_task(task_id: Option<i64>, mut config: TaskConfig) -> anyhow::Result<()> {
    let mut tags = config.tags.clone();
    if !tags.is_empty() && tags.len() == 1 && tags.get(0).unwrap() == "*" {
        let mut client = HistorianQuery::try_new(config.connect.clone()).await?;
        let tag_meta = client.get_tags().await?;
        tags = tag_meta
            .iter()
            .map(|meta| meta.name.clone())
            .collect::<Vec<_>>();
    }
    if tags.is_empty() {
        anyhow::bail!("tags cannot be empty");
    }

    let (logger_tx, logger_rx) = flume::bounded(0);
    let logger = RawDataLogger::new(
        task_id.unwrap_or(0),
        config.advanced_options.keep_raw_data.unwrap_or(false),
        config
            .advanced_options
            .keep_raw_data_dir
            .clone()
            .unwrap_or(std::env::var(crate::runners::ENV_TAOSX_DATA_DIR).unwrap()),
        config
            .advanced_options
            .keep_raw_data_days
            .clone()
            .unwrap_or(30),
        logger_rx,
    );
    logger.start();

    match (config.mode, config.table) {
        (TaskMode::Migrate, HistorianTable::History) => {
            config.tags = tags;
            migrate_history(config.clone(), logger_tx).await?;
        }
        (TaskMode::Synchronize, HistorianTable::History) => {
            config.tags = tags;
            sync_history(config.clone(), logger_tx).await?;
        }
        (TaskMode::Synchronize, HistorianTable::Live) => {
            sync_live(config.clone(), logger_tx).await?;
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

    use chrono::{DateTime, NaiveDateTime, Utc};
    use rand::Rng;
    use taos::Dsn;

    use super::*;

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("historian://localhost").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("historian", res.data_source);
        assert_eq!(
            "invalid dsn: historian://localhost, cause: username is required",
            res.message.unwrap()
        );

        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@127.0.0.1").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("historian", res.data_source);
        assert_eq!("failed to connect to dsn: historian://aaAdmin:aaAdmin@127.0.0.1, cause: Connection refused (os error 61)", res.message.unwrap());
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433/").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(true, res.valid);
        assert_eq!(true, res.support);
        assert_eq!("historian", res.data_source);
        assert_eq!(None, res.version);
    }

    #[ignore]
    #[tokio::test]
    /// generate test data, Only for local test
    async fn generate_tag_csv() -> anyhow::Result<()> {
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
            let dt: DateTime<Utc> = NaiveDateTime::from_timestamp_opt(ts + (i * gap_sec), 0)
                .unwrap()
                .and_utc();
            let date_time = dt.format("%Y/%m/%d|%H:%M:%S").to_string();
            let date_value = rng.gen_range(0.0..100.0);
            file.write_all(
                format!("tag{}|0|{}|1|{}|192\n", tag_index, date_time, date_value).as_bytes(),
            )?;
        }
        Ok(())
    }
}
