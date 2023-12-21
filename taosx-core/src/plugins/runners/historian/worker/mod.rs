use std::cmp;

use arrow::ipc::writer::StreamWriter;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use flume::Sender;
use futures_util::TryStreamExt;
use itertools::Itertools;

use taosx_ipc::ack::AckReaderBuilder;

use crate::runners::historian::appender::ArrowDataAppender;
use crate::runners::historian::config::{HistorianTable, TaskConfig};
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::set_tcp_keepalive;
use crate::runners::historian::worker::consumer::Consumer;
use crate::runners::historian::worker::producer::Producer;

mod consumer;
mod producer;

pub async fn migrate_history(config: TaskConfig, logger: Sender<String>) -> anyhow::Result<()> {
    tracing::info!("migrate history start, config: {:?}", config);

    let (tx, rx) = flume::bounded(0);
    // consume task
    let mut consumers = Vec::new();

    let concurrency = cmp::max(config.advanced_options.read_concurrency.unwrap_or(1), 1);

    for _ in 1..=concurrency {
        let receiver = rx.clone();

        let query = HistorianQuery::try_new(config.connect.clone()).await?;
        let ipc_port = config
            .ipc_port
            .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;

        let logger_tx = logger.clone();
        let c = tokio::spawn(async move {
            let mut consumer = Consumer::new(query, ipc_port);
            consumer.consume(receiver, logger_tx).await
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

pub async fn sync_history(task_config: TaskConfig, logger: Sender<String>) -> anyhow::Result<()> {
    tracing::info!("sync history start, config: {:?}", task_config);

    // create migrate task
    let now = Utc::now();
    let mut migrate_task_config = task_config.clone();
    migrate_task_config.end_datetime = Some(now);

    let logger_tx = logger.clone();
    let _ = tokio::spawn(async move { migrate_history(migrate_task_config, logger_tx).await });

    // create stream for ipc
    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    set_tcp_keepalive(&stream)?;
    stream.set_nonblocking(false)?;

    // create stream for ack
    let ack_stream = stream.try_clone()?;
    set_tcp_keepalive(&ack_stream)?;
    ack_stream.set_read_timeout(None)?;

    // handle ack from ipc reader
    tokio::task::spawn_blocking(move || {
        let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
        for ack in ack_reader {
            if !ack.success() {
                tracing::error!("sync history write records error: {ack:?}",);
                if let Some(message) = ack.message() {
                    anyhow::bail!("IPC writer error: {message}")
                }
            }
        }
        tracing::info!("sync history ACK reader finished");
        Ok(())
    });

    let mut appender = ArrowDataAppender::try_new(HistorianTable::History)?;
    let schema = appender.schema().clone();

    // write batch to ipc
    let (tx, rx) = flume::bounded(0);
    tokio::task::spawn_blocking(move || {
        let mut writer = StreamWriter::try_new(stream, &schema)?;
        while let Ok(batch) = rx.recv() {
            writer.write(&batch)?;
            tracing::info!("sync history write {} rows to ipc", batch.num_rows());
        }
        let _ = writer.finish()?;
        anyhow::Ok(())
    });

    tokio::time::sleep(task_config.tolerance.to_std().unwrap()).await;
    // query database and send to writer
    let mut query = HistorianQuery::try_new(task_config.connect.clone()).await?;
    let mut window_start = now;
    let mut count: u64 = 1;
    let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);
    loop {
        let window_end = Utc::now();

        tracing::debug!(
            "sync history:{}, window_start: {}, window_end: {}",
            count,
            window_start,
            window_end
        );

        for tags in &tags_group {
            tracing::debug!("sync history:{} query rows", count);
            let mut rows = query
                .query_history(tags.clone(), window_start, window_end)
                .await?
                .into_row_stream();

            tracing::debug!("sync history:{} rows to batch", count);
            while let Some(row) = rows.try_next().await? {
                let raw_data = appender.append_history_row(&row).map_err(|err| {
                    let err_msg = format!(
                        "sync history:{} append row error: {}",
                        count,
                        err.to_string()
                    );
                    tracing::error!(err_msg);
                    anyhow::anyhow!(err_msg)
                })?;
                logger.send_async(raw_data.to_string()).await?;
            }

            tracing::debug!("sync history:{} batch finish", count);
            let batch = appender.finish()?;
            tracing::debug!("sync history:{} send batch to writer", count);
            tx.send_async(batch.clone()).await?;

            count += 1;
        }

        window_start = window_end;
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}

pub async fn sync_live(task_config: TaskConfig, logger: Sender<String>) -> anyhow::Result<()> {
    tracing::info!("sync live start, config: {:?}", task_config);

    let port = task_config
        .ipc_port
        .ok_or(anyhow::anyhow!("ipc_port can not be None"))?;

    // create stream for ipc
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    set_tcp_keepalive(&stream)?; // set tcp keep alive
    stream.set_nonblocking(false)?;

    // create stream for ack
    let ack_stream = stream.try_clone()?;
    set_tcp_keepalive(&ack_stream)?;
    ack_stream.set_read_timeout(None)?;

    let mut appender = ArrowDataAppender::try_new(HistorianTable::Live)?;
    let schema = appender.schema().clone();

    // write batch to ipc
    let (tx, rx) = flume::bounded(0);
    tokio::task::spawn_blocking(move || {
        let mut writer = StreamWriter::try_new(stream, &schema)?;
        while let Ok(batch) = rx.recv() {
            writer.write(&batch)?;
            tracing::info!("sync live write {} rows to ipc", batch.num_rows());
        }
        let _ = writer.finish()?;
        anyhow::Ok(())
    });

    // handle ack from ipc reader
    tokio::task::spawn_blocking(move || {
        let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
        for ack in ack_reader {
            if !ack.success() {
                tracing::error!("sync live write records error: {ack:?}",);
                if let Some(message) = ack.message() {
                    anyhow::bail!("IPC writer error: {message}")
                }
            }
        }
        tracing::info!("sync live ACK reader finished");
        Ok(())
    });

    let mut query = HistorianQuery::try_new(task_config.clone().connect).await?;

    let mut count: u64 = 1;
    loop {
        tracing::debug!(
            "sync live:{} query rows, now: {}",
            count,
            Local::now().to_string()
        );
        let mut rows = query
            .query_live(task_config.tags.clone())
            .await?
            .into_row_stream();

        tracing::debug!("sync live:{} traverse rows to batch", count);
        let mut earliest = i64::MAX;
        let mut latest = 0;
        while let Some(row) = rows.try_next().await? {
            let raw_data = appender.append_live_row(&row)?;
            let ts = raw_data.datetime;
            logger.send_async(raw_data.to_string()).await?;
            earliest = cmp::min(ts, earliest);
            latest = cmp::max(ts, latest);
        }

        tracing::debug!(
            "sync live:{} batch finish, earliest: {}({}), latest: {}({})",
            count,
            to_local_datetime(earliest),
            earliest,
            to_local_datetime(latest),
            latest
        );
        let batch = appender.finish()?;

        tracing::debug!("sync live:{} send batch to writer", count);
        tx.send_async(batch.clone()).await?;

        count += 1;
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

fn to_local_datetime(nanos_sec: i64) -> DateTime<Local> {
    let naive_datetime = NaiveDateTime::from_timestamp_micros(nanos_sec / 1000).unwrap();
    Local::now().timezone().from_utc_datetime(&naive_datetime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_datetime() {
        let ts = 10_0000_0000_123_456_789;
        let datetime = to_local_datetime(ts);

        assert_eq!("2001-09-09 09:46:40.123456 +08:00", datetime.to_string());
    }
}
