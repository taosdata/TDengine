use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use itertools::Itertools;
use taos::Dsn;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::{Action, build_ipc, Parser, Transferred};
use crate::dsv::DataSourceValidation;
use crate::runners::historian::config::{TaskConfig, TaskMode};
use crate::runners::historian::config::connect::ConnectConfig;
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::worker::{migrate_history, sync_history, sync_live};
use crate::utils::port_pool::PortPool;

mod arrow;
mod config;
mod query;
mod worker;
mod table_type;

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation {
            valid: false,
            support: false,
            data_source: "historian".to_string(),
            version: None,
            message: Some(format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            )),
        },
        Ok(c) => {
            let client = HistorianQuery::new(c).await;
            match client {
                Err(err) => DataSourceValidation {
                    valid: false,
                    support: false,
                    data_source: "historian".to_string(),
                    version: None,
                    message: Some(format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn,
                        err.to_string()
                    )),
                },
                Ok(_cli) => DataSourceValidation {
                    valid: true,
                    support: true,
                    data_source: "historian".to_string(),
                    version: None,
                    message: None,
                },
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
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let config = TaskConfig::from_dsn(&from)?;
    tracing::info!("AVEVA™ Historian task configuration: {:?}", config);

    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for connection"))?;
    let socket = format!("127.0.0.1:{}", port);

    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some("Historian"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        None,
        notify,
    ).await?;

    let port_pool = port_pool.clone();

    // create worker
    let worker = tokio::spawn(exec_task(config, port_pool.clone()));

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
                                anyhow::bail!("AVEVA™ Historian worker exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("AVEVA™ Historian worker done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("AVEVA™ Historian exit with error: {:#}", err);
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
                    anyhow::bail!("AVEVA™ Historian writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("AVEVA™ Historian task cancelled");
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("AVEVA™ Historian task Done");
        ipc.close().await?;
        // put ipc port back to port pool.
        port_pool.put(port).await;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

async fn exec_task(config: TaskConfig, port_pool: PortPool) -> anyhow::Result<()> {
    match (config.mode, config.table.as_str()) {
        (TaskMode::Synchronize, "Runtime.dbo.History") => {
            sync_history(config.clone(), &port_pool).await?;
        }
        (TaskMode::Migrate, "Runtime.dbo.History") => {
            migrate_history(config.clone(), &port_pool).await?;
        }
        (TaskMode::Synchronize, "Runtime.dbo.Live") => {
            sync_live(config.clone(), &port_pool).await?;
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
