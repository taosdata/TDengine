use arrow::ipc::writer::StreamWriter;
use chrono::Utc;
use futures_util::TryStreamExt;
use itertools::Itertools;
use tiberius::QueryItem;

use crate::runners::historian::arrow::ArrowDataAppender;
use crate::runners::historian::config::{HistorianTable, TaskConfig};
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::worker::consumer::Consumer;
use crate::runners::historian::worker::producer::Producer;

mod consumer;
mod producer;

pub async fn migrate_history(config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("migrate history start, config: {:?}", config);

    let (tx, rx) = flume::bounded(config.concurrency);
    // consume task
    let mut consumers = Vec::new();
    for _ in 0..config.concurrency {
        let receiver = rx.clone();

        let query = HistorianQuery::try_new(config.connect.clone()).await?;
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

    tracing::info!("migrate history finished");
    Ok(())
}

pub async fn sync_history(task_config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("sync history start, config: {:?}", task_config);

    // migrate task
    let now = Utc::now();
    let mut migrate_task_config = task_config.clone();
    migrate_task_config.end_datetime = Some(now);
    let _ = tokio::spawn(async move { migrate_history(migrate_task_config).await });

    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    stream.set_nonblocking(false)?;
    let mut appender = ArrowDataAppender::try_new(HistorianTable::History)?;
    let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

    let mut query = HistorianQuery::try_new(task_config.connect.clone()).await?;

    tokio::time::sleep(task_config.tolerance.to_std().unwrap()).await;

    let mut window_start = now;
    let mut count: u64 = 1;
    let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);
    loop {
        let window_end = Utc::now();

        tracing::debug!(
            "sync history {}, begin: {}, end: {}",
            count,
            window_start,
            window_end
        );

        for tags in &tags_group {
            tracing::debug!("sync history {} query rows", count);
            let mut rows = query.query_history(tags.clone(), window_start, window_end).await?;

            tracing::debug!("sync history {} rows to batch", count);
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

            tracing::debug!("sync history {} batch finish", count);
            let batch = appender.finish()?;
            tracing::debug!("sync history {} batch to writer", count);
            writer.write(&batch)?;
            count += 1;
        }

        window_start = window_end;
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}

pub async fn sync_live(task_config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("sync live start, config: {:?}", task_config);

    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port can not be None"))?;
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    stream.set_nonblocking(false)?;
    let mut appender = ArrowDataAppender::try_new(HistorianTable::Live)?;
    let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

    let mut query = HistorianQuery::try_new(task_config.clone().connect).await?;
    let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);

    let mut count: u64 = 1;
    loop {
        for tags in &tags_group {
            tracing::debug!("sync live {} query rows", count);
            let mut rows = query.query_live(tags.clone()).await?;

            tracing::debug!("sync live {} rows to batch", count);
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

            tracing::debug!("sync live {} batch finish", count);
            let batch = appender.finish()?;
            tracing::debug!("sync live {} batch to writer", count);
            writer.write(&batch)?;

            count += 1;
        }
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}

fn split_tags(tags: Vec<String>, size: usize) -> Vec<Vec<String>> {
    tags.iter()
        .chunks(size)
        .into_iter()
        .map(|list| list.map(|s| s.to_string()).collect::<Vec<String>>())
        .collect_vec()
}
