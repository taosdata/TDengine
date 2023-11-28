use arrow::ipc::writer::StreamWriter;
use chrono::Utc;
use futures_util::TryStreamExt;
use tiberius::QueryItem;

use crate::runners::historian::arrow::ArrowDataAppender;
use crate::runners::historian::config::{HistorianTable, TaskConfig};
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::worker::consumer::Consumer;
use crate::runners::historian::worker::producer::Producer;

mod consumer;
mod producer;

pub async fn migrate_history(config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("create history migrate task with config: {:?}", config);
    let (tx, rx) = flume::bounded(config.concurrency);
    // consume task
    let mut consumers = Vec::new();
    for _ in 0..config.concurrency {
        let receiver = rx.clone();

        let connect_config = config.connect.clone();
        let query = HistorianQuery::try_new(connect_config).await?;

        let ipc_port = config
            .ipc_port
            .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;

        let c = tokio::spawn(async move {
            let mut consumer = Consumer::new(query, ipc_port);
            consumer.consume(receiver).await
        });
        consumers.push(c);
    }
    // produce task
    let producer = Producer::new(&config);
    producer.produce(tx).await?;
    // consumer join
    for c in consumers {
        c.await??;
    }

    tracing::info!("migrate history task finished");
    Ok(())
}

pub async fn sync_history(task_config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("create history sync task with config: {:?}", task_config);

    let mut now = Utc::now();
    let mut migrate_task_config = task_config.clone();
    migrate_task_config.end_datetime = Some(now);

    let _ = tokio::spawn(async move { migrate_history(migrate_task_config).await });

    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    let mut appender = ArrowDataAppender::try_new(HistorianTable::History)?;
    let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

    let mut query = HistorianQuery::try_new(task_config.clone().connect).await?;

    tokio::time::sleep(task_config.tolerance.to_std().unwrap()).await;
    loop {
        let window_start = now;
        let window_end = Utc::now();

        let mut rows = query
            .query_history(task_config.tags.clone(), window_start, window_end)
            .await?;

        while let Some(row) = rows.try_next().await? {
            match row {
                QueryItem::Row(row) => {
                    appender.append_history_row(row)?;
                }
                QueryItem::Metadata(_) => {
                    continue;
                }
            }
        }
        // write batch
        let batch = appender.finish()?;
        writer.write(&batch)?;

        now = window_end;
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}

pub async fn sync_live(task_config: TaskConfig) -> anyhow::Result<()> {
    tracing::debug!("create live sync task with config: {:?}", task_config);

    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port can not be None"))?;
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    let mut appender = ArrowDataAppender::try_new(HistorianTable::Live)?;
    let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

    let mut query = HistorianQuery::try_new(task_config.clone().connect).await?;

    loop {
        tracing::debug!("start live sync task {}", Utc::now().to_rfc3339());
        // query
        let mut rows = query.query_live(task_config.tags.clone()).await?;
        while let Some(row) = rows.try_next().await? {
            match row {
                QueryItem::Row(row) => {
                    appender.append_live_row(row)?;
                }
                QueryItem::Metadata(_) => {
                    continue;
                }
            }
        }
        // write batch
        let batch = appender.finish()?;
        writer.write(&batch)?;

        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}
