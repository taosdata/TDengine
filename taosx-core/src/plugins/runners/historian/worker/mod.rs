use std::cmp;

use arrow::array::RecordBatchWriter;
use arrow::csv::Writer;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Local, Utc};
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
use crate::utils::breakpoints;

mod consumer;
mod producer;

const MIGRATE_TASK_PREFIX: &str = "mig";
const SYNCHRONIZE_TASK_PREFIX: &str = "syn";

/// migrate data
pub async fn migrate_history(mut config: TaskConfig, logger: Sender<String>) -> anyhow::Result<()> {
    // get break point
    let break_point = get_break_point(config.task_id);
    if break_point.is_some() {
        let begin_date_time = break_point.unwrap();
        tracing::info!(
            "migrate history start from break point: {}",
            begin_date_time.to_rfc3339()
        );
        config.begin_datetime = Some(begin_date_time);
    }

    tracing::info!("migrate history start, config: {:?}", config);
    let (tx, rx) = flume::bounded(0);
    let concurrency = cmp::max(config.advanced_options.read_concurrency.unwrap_or(1), 1);
    // consume task
    let mut consumers = Vec::new();
    for sub_task_index in 1..=concurrency {
        let receiver = rx.clone();
        let ipc_port = config
            .ipc_port
            .ok_or(anyhow::anyhow!("ipc_port cannot be None"))?;
        let logger_tx = logger.clone();
        let connect_config = config.connect.clone();

        let c = tokio::spawn(async move {
            let sub_task_id = Some(format!("{MIGRATE_TASK_PREFIX}-{sub_task_index}"));
            let mut consumer = Consumer::new(sub_task_id, connect_config, ipc_port);
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

fn get_break_point(task_id: Option<i64>) -> Option<DateTime<Utc>> {
    task_id?;

    let task_id = format!("{}", task_id.unwrap());
    let breakpoints_res = breakpoints::breakpoints_get_all(&task_id);
    if breakpoints_res.is_err() {
        return None;
    }

    let break_points = breakpoints_res.unwrap();
    let mut earliest = None;
    for (sub_task_id, bp) in break_points {
        if sub_task_id.starts_with(MIGRATE_TASK_PREFIX) {
            let date_time = DateTime::parse_from_rfc3339(&bp)
                .map(|dt| Some(dt.with_timezone(&Utc)))
                .unwrap_or(None);

            if date_time.is_some() {
                earliest = Some(cmp::min(
                    earliest.unwrap_or(date_time.unwrap()),
                    date_time.unwrap(),
                ));
            }
        }
    }

    earliest
}

pub async fn sync_history(
    mut task_config: TaskConfig,
    logger: Sender<String>,
) -> anyhow::Result<()> {
    // get break point
    let task_id = task_config.task_id;
    let break_pint = get_break_point(task_id);
    if break_pint.is_some() {
        let break_point = break_pint.unwrap();
        tracing::info!(
            "sync history start from break point: {}",
            break_point.to_rfc3339()
        );
        task_config.begin_datetime = Some(break_point);
    }

    tracing::info!("sync history start, config: {:?}", task_config);
    let now = Utc::now();

    // create migrate task
    let mut migrate_task_config = task_config.clone();
    migrate_task_config.end_datetime = Some(now);

    let logger_tx = logger.clone();
    let _ = tokio::spawn(async move { migrate_history(migrate_task_config, logger_tx).await });

    // create synchronize task and set sub task id
    task_config.sub_task_id = Some(format!("{SYNCHRONIZE_TASK_PREFIX}-1"));
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
    // get schema from database
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
        writer.finish()?;
        anyhow::Ok(())
    });

    // sync-history start from now + retrieve_interval + tolerance
    tokio::time::sleep(
        (task_config.tolerance + task_config.retrieve_interval)
            .to_std()
            .unwrap(),
    )
    .await;
    // query database and send to writer
    let mut count: u64 = 1;
    let mut window_start = now;
    let tags_group = split_tags(task_config.tags.clone(), task_config.tag_list_size);
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
        writer.finish()?;
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

pub async fn set_break_point(task: &TaskConfig, break_point: &DateTime<Utc>) -> anyhow::Result<()> {
    let task = task.clone();

    let task_id = format!("{}", task.task_id.unwrap());
    let sub_task_id = task.sub_task_id.unwrap();
    let breakpoint = break_point.to_rfc3339().to_string();

    breakpoints::breakpoints_set(&task_id, &sub_task_id, &breakpoint)
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
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_convert_datetime() {
        let ts_nano = 1_000_000_000_123_456_789;

        let naive_datetime = DateTime::from_timestamp_micros(ts_nano / 1000)
            .unwrap()
            .naive_utc();

        let datetime = Local::now().timezone().from_utc_datetime(&naive_datetime);

        assert_eq!("2001-09-09 09:46:40.123456 +08:00", datetime.to_string());
    }
}
