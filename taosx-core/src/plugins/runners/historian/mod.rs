use std::sync::Arc;
use std::time::Duration;

use ::arrow::ipc::writer::StreamWriter;
use futures_util::TryStreamExt;
use taos::Dsn;
use tiberius::{AuthMethod, Client, Config, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::dsv::DataSourceValidation;
use crate::plugins::runners::historian::arrow::ArrowDataAppender;
use crate::plugins::runners::historian::config::SourceConfig;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

mod arrow;
mod config;
mod tag;

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = SourceConfig::from_dsn(dsn);
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
            let client = connect(&c.host, c.port, &c.username, &c.password).await;
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
    )
    .await?;

    let port_pool = port_pool.clone();

    // create worker
    let worker = tokio::spawn(historian_worker(from, port));

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

async fn historian_worker(from: Dsn, port: u16) -> anyhow::Result<()> {
    // connect
    let config = SourceConfig::from_dsn(&from)?;
    tracing::info!("AVEVA™ Historian task configuration: {:?}", config);
    let mut client = connect_by_config(&config).await?;

    // filter tags
    let tag_names;
    if config.tags.len() == 1 && config.tags[0] == "*" {
        tag_names = tag::query_tags(&mut client)
            .await?
            .iter()
            .map(|tag| tag.name.clone())
            .collect();
    } else {
        tag_names = config.tags;
    }

    // query and write
    for tag_name in tag_names {
        // sql
        let sql = format!(
            "select * from {} where TagName = '{}' and DateTime >= '{}' and DateTime <= '{}' and wwRetrievalMode = '{}'",
            config.table, tag_name, config.begin_date_time.to_rfc3339(), config.end_date_time.to_rfc3339(), config.retrieve_mode
        );
        tracing::info!("sql: {}", sql);
        // query
        let mut rows = client
            .query(
                &sql,
                &[
                    &(tag_name.as_str()),
                    &config.begin_date_time,
                    &config.end_date_time,
                    &config.retrieve_mode,
                ],
            )
            .await?;
        // metadata
        let columns = rows
            .columns()
            .await?
            .ok_or_else(|| anyhow::anyhow!("No columns returned"))?;
        let mut appender = ArrowDataAppender::new(columns);
        let socket = format!("127.0.0.1:{}", port);
        let stream = std::net::TcpStream::connect(socket)?;
        let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

        while let Some(row) = rows.try_next().await? {
            match row {
                QueryItem::Row(row) => {
                    appender.append_row(row)?;
                }
                QueryItem::Metadata(_) => {
                    continue;
                }
            }
        }
        // write batch
        let batch = appender.finish()?;
        writer.write(&batch)?;
        writer.finish()?;
        tokio::task::yield_now().await;
    }

    Ok(())
}

async fn connect_by_config(
    source_config: &SourceConfig,
) -> anyhow::Result<Client<Compat<TcpStream>>> {
    connect(
        &source_config.host,
        source_config.port,
        &source_config.username,
        &source_config.password,
    )
    .await
}

async fn connect(
    host: &String,
    port: u16,
    username: &String,
    password: &String,
) -> anyhow::Result<Client<Compat<TcpStream>>> {
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.authentication(AuthMethod::sql_server(username, password));
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let client: Client<Compat<TcpStream>> = Client::connect(config, tcp.compat_write()).await?;

    Ok(client)
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
    async fn test_invalid() {
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

    #[tokio::test]
    async fn test_connect() {
        let client = connect(
            &"127.0.0.1".to_string(),
            1433,
            &"aaAdmin".to_string(),
            &"aaAdmin".to_string(),
        )
        .await;
        assert!(client.is_err());
        assert_eq!(
            "Connection refused (os error 61)",
            client.unwrap_err().to_string()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_connect_by_config() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = SourceConfig::from_dsn(&dsn).unwrap();

        let client = connect_by_config(&config).await;

        assert!(client.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_historian_to_taos() {
        let from = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40").unwrap();
        let to = Dsn::from_str("taos://root:taosdata@192.168.1.92:6030/historian_to_taos").unwrap();

        let (notify, _) = flume::unbounded();

        let res = historian_to_taos(
            from,
            None,
            vec![],
            to,
            1,
            &PortPool::default(),
            CancellationToken::new(),
            None,
            None,
            Span::current(),
            notify,
        )
        .await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    #[ignore]
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
