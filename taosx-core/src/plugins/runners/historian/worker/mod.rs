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
    tracing::info!("create history migrate task with config: {:?}", config);

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

    tracing::info!("migrate history task finished");
    Ok(())
}

pub async fn sync_history(task_config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("create history sync task with config: {:?}", task_config);

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
    let mut count = 0;
    loop {
        let window_end = Utc::now();

        tracing::debug!(
            "execute history sync task, begin: {}, end: {}",
            window_start,
            window_end
        );

        let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);
        for tags in tags_group {
            tracing::debug!("sync history[{}] query rows", count);
            let mut rows = query.query_history(tags, window_start, window_end).await?;

            tracing::debug!("sync history[{}] rows to batch", count);
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

            tracing::debug!("sync history[{}] batch finish", count);
            let batch = appender.finish()?;
            tracing::debug!("sync history[{}] batch to writer", count);
            writer.write(&batch)?;
            count += 1;
        }

        window_start = window_end;
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}

pub async fn sync_live(task_config: TaskConfig) -> anyhow::Result<()> {
    tracing::info!("create live sync task with config: {:?}", task_config);

    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port can not be None"))?;
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    let mut appender = ArrowDataAppender::try_new(HistorianTable::Live)?;
    let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

    let mut query = HistorianQuery::try_new(task_config.clone().connect).await?;
    loop {
        tracing::debug!("execute live sync task");

        let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);

        for tags in tags_group {
            let mut rows = query.query_live(tags).await?;

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

            let batch = appender.finish()?;
            writer.write(&batch)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runners::historian::config::TaskConfig;
    use chrono::{DateTime, Local, NaiveDateTime};
    use std::str::FromStr;
    use taos::Dsn;

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sync_history() {
        let dsn = Dsn::from_str(
            "historian://aaAdmin:aaAdmin@192.168.3.40:1433?\
            mode=synchronize\
            &table=Runtime.dbo.History\
            &beginDateTime=2023-11-01T00:00:00Z\
            &tolerance=5s",
        )
        .unwrap();
        let task_config = TaskConfig::from_dsn(&dsn).unwrap();

        let t = tokio::spawn(async move {
            let _ = sync_history(task_config.clone()).await;
        });
        t.await.unwrap();
    }

    #[test]
    fn calculate_datetime() {
        let dt_list = vec![
            "2023-12-02 14:58:52.168797",
            "2023-12-02 15:07:58.628199",
            "2023-12-02 15:23:41.143622",
            "2023-12-02 15:43:42.163841",
            "2023-12-02 16:06:07.460913",
            "2023-12-02 16:31:59.603975",
            "2023-12-02 17:01:56.382011",
            "2023-12-02 17:34:35.588757",
            "2023-12-02 18:09:21.415776",
            "2023-12-02 18:47:07.448945",
            "2023-12-02 19:28:43.426139",
            "2023-12-02 20:09:53.043529",
            "2023-12-02 20:23:45.841507",
            "2023-12-02 20:31:07.156327",
            "2023-12-02 20:36:43.809126",
            "2023-12-02 20:42:18.879181",
        ];

        for i in 0..dt_list.len() - 1 {
            let start = dt_list[i];
            let end = dt_list[i + 1];

            let start = NaiveDateTime::parse_from_str(start, "%Y-%m-%d %H:%M:%S%.6f").unwrap();
            let end = NaiveDateTime::parse_from_str(end, "%Y-%m-%d %H:%M:%S%.6f").unwrap();

            let duration = end - start;
            println!("duration: {:?}", duration);
        }
    }
}
