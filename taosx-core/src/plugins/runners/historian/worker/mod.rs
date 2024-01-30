use std::cmp;

use arrow::array::RecordBatchWriter;
use arrow::csv::Writer;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{Local, Utc};
use flume::Sender;
use futures_util::TryStreamExt;
use itertools::Itertools;

use taosx_ipc::ack::AckReaderBuilder;

use crate::runners::historian::appender;
use crate::runners::historian::config::{HistorianTable, TaskConfig};
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::worker::consumer::Consumer;
use crate::runners::historian::worker::producer::Producer;
use crate::runners::set_tcp_keepalive;

mod consumer;
mod producer;

/// migrate data
pub async fn migrate_history(config: TaskConfig, logger: Sender<String>) -> anyhow::Result<()> {
    tracing::info!("migrate history start, config: {:?}", config);

    let (tx, rx) = flume::bounded(0);
    let concurrency = cmp::max(config.advanced_options.read_concurrency.unwrap_or(1), 1);
    // consume task
    let mut consumers = Vec::new();
    for _ in 1..=concurrency {
        let receiver = rx.clone();
        let ipc_port = config
            .ipc_port
            .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;
        let logger_tx = logger.clone();
        let connect_config = config.connect.clone();

        let c = tokio::spawn(async move {
            let mut consumer = Consumer::new(connect_config, ipc_port);
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

    let mut client = HistorianQuery::try_new(task_config.connect.clone()).await?;
    let mut rows = client
        .describe_table(HistorianTable::History)
        .await?
        .into_row_stream();
    let mut fields = Vec::new();
    while let Some(row) = rows.try_next().await? {
        let col_meta = appender::column_meta::ColumnMeta::try_new(&row)?;
        fields.push(col_meta);
    }
    drop(rows);
    let schema = appender::column_meta::to_schema(fields)?;

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

    // query database and send to writer
    let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);

    // sync-history start from now + retrieve_interval + tolerance
    tokio::time::sleep(
        (task_config.tolerance + task_config.retrieve_interval)
            .to_std()
            .unwrap(),
    )
    .await;

    let mut count: u64 = 1;
    let mut window_start = now;
    loop {
        let window_end = Utc::now() - task_config.tolerance;

        tracing::debug!(
            "sync history:{}, window_start: {}, window_end: {}",
            count,
            window_start,
            window_end
        );

        for tags in &tags_group {
            tracing::debug!("sync history: {} query rows", count);

            let stream = client
                .select_from_history(tags.clone(), window_start, window_end)
                .await?;
            let batch = appender::to_record_batch(stream).await?;

            let mut output = Vec::new();
            let mut writer = Writer::new(&mut output);
            writer.write(&batch)?;
            let _ = writer.close();

            logger.send_async(String::from_utf8(output)?).await?;
            tracing::debug!("sync history: {} send batch to writer", count);
            tx.send_async(batch).await?;

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

    let mut client = HistorianQuery::try_new(task_config.clone().connect).await?;

    let mut fields = Vec::new();
    let mut rows = client
        .describe_table(HistorianTable::Live)
        .await?
        .into_row_stream();
    while let Some(row) = rows.try_next().await? {
        let col_meta = appender::column_meta::ColumnMeta::try_new(&row)?;
        fields.push(col_meta);
    }
    drop(rows);

    if fields.is_empty() {
        anyhow::bail!("live table cannot be empty")
    }
    let schema = appender::column_meta::to_schema(fields)?;

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

    let mut count: u64 = 1;
    loop {
        tracing::debug!(
            "sync live: {} query rows, now: {}",
            count,
            Local::now().to_string()
        );

        let stream = client.select_from_live(task_config.tags.clone()).await?;
        let batch = appender::to_record_batch(stream).await?;

        logger.send_async(to_csv_string(&batch)?).await?;
        tracing::debug!("sync live: {} send batch to writer", count);
        tx.send_async(batch).await?;

        count += 1;
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}

pub fn to_csv_string(batch: &RecordBatch) -> anyhow::Result<String> {
    let mut output = Vec::new();
    let mut writer = Writer::new(&mut output);
    writer.write(batch)?;
    let _ = writer.close();

    String::from_utf8(output).map_err(|err| {
        anyhow::anyhow!(
            "failed to convert record batch to csv, cause: {}",
            err.to_string()
        )
    })
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
    use chrono::{NaiveDateTime, TimeZone};

    use super::*;

    #[test]
    fn test_convert_datetime() {
        let ts_nano = 10_0000_0000_123_456_789;

        let naive_datetime = NaiveDateTime::from_timestamp_micros(ts_nano / 1000).unwrap();

        let datetime = Local::now().timezone().from_utc_datetime(&naive_datetime);

        assert_eq!("2001-09-09 09:46:40.123456 +08:00", datetime.to_string());
    }
}
