use anyhow::Context;
use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef, RecordBatchWriter};
use arrow::csv::Writer;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit::Nanosecond;
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use flume::{Receiver, Sender};
use futures_util::TryStreamExt;
use itertools::Itertools;
use std::cmp;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use taosx_ipc::ack::AckReaderBuilder;
use tiberius::{ColumnType, QueryItem, QueryStream};

use crate::runners::historian::config::ConnectConfig;
use crate::runners::historian::config::TaskConfig;
use crate::runners::historian::query::HistorianQuery;
use crate::runners::historian::HistorianTable;
use crate::runners::set_tcp_keepalive;
use crate::utils::breakpoints;
use column_meta::ColumnMeta;

pub mod column_meta;

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
    tokio::spawn(async move { migrate_history(migrate_task_config, logger_tx).await });

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
        let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush)
            .open(&ack_stream)
            .context("failed to open ack stream")?;
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

    let mut client = HistorianQuery::try_connect(task_config.connect.clone()).await?;
    // get schema from database
    let mut rows = client
        .describe_table(HistorianTable::History)
        .await?
        .into_row_stream();
    let mut fields = Vec::new();
    while let Some(row) = rows.try_next().await? {
        let col_meta = ColumnMeta::try_new(&row)?;
        fields.push(col_meta);
    }
    drop(rows);
    let schema = ColumnMeta::build_schema_with_vec(fields)?;

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
            let batch = to_record_batch(stream).await?;

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

    let mut client = HistorianQuery::try_connect(task_config.clone().connect).await?;

    let mut fields = Vec::new();
    let mut rows = client
        .describe_table(HistorianTable::Live)
        .await?
        .into_row_stream();
    while let Some(row) = rows.try_next().await? {
        let col_meta = ColumnMeta::try_new(&row)?;
        fields.push(col_meta);
    }
    drop(rows);

    if fields.is_empty() {
        anyhow::bail!("live table cannot be empty")
    }
    let schema = ColumnMeta::build_schema_with_vec(fields)?;

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
        let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush)
            .open(&ack_stream)
            .context("failed to open ack stream")?;
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
        let batch = to_record_batch(stream).await?;

        logger.send_async(to_csv_string(&batch)?).await?;
        tracing::debug!("sync live: {} send batch to writer", count);
        tx.send_async(batch).await?;

        count += 1;
        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
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

fn to_csv_string(batch: &RecordBatch) -> anyhow::Result<String> {
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

fn split_tags(tags: Vec<String>, chunk_size: usize) -> Vec<Vec<String>> {
    tags.iter()
        .chunks(chunk_size)
        .into_iter()
        .map(|list| list.map(|s| s.to_string()).collect::<Vec<String>>())
        .collect_vec()
}

async fn to_record_batch(stream: QueryStream<'_>) -> anyhow::Result<RecordBatch> {
    to_record_batches(stream, usize::MAX)
        .await
        .map(|batches| batches[0].clone())
}

async fn to_record_batches(
    mut stream: QueryStream<'_>,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut columns = Vec::new();
    let mut builders = Vec::new();
    let mut fields = Vec::new();
    let mut batches = Vec::new();

    let mut row_count = 0;
    while let Some(item) = stream.try_next().await? {
        match item {
            QueryItem::Metadata(meta) => {
                for col in meta.columns() {
                    let col_name = col.name().to_string();
                    let col_type = col.column_type();
                    columns.push((col_name, col_type));
                }

                for (col_name, col_type) in columns.iter() {
                    let arrow_type = to_arrow_data_type(*col_type)?;
                    fields.push(Field::new(col_name, arrow_type.clone(), true));
                    builders.push(array::make_builder(&arrow_type, 10));
                }
            }
            QueryItem::Row(row) => {
                for (idx, (_col_name, col_type)) in columns.iter().enumerate() {
                    match col_type {
                        ColumnType::Null => {
                            builders[idx]
                                .as_any_mut()
                                .downcast_mut::<array::NullBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        ColumnType::Int1 => {
                            let val = row.try_get::<u8, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::UInt8Builder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::UInt8Builder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Intn => {
                            let val = row.try_get::<i32, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Int32Builder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Int32Builder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                        ColumnType::Float4 | ColumnType::Float8 | ColumnType::Floatn => {
                            let val = row.try_get::<f64, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Float64Builder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Float64Builder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                        ColumnType::Datetime2 => {
                            let val = row.try_get::<NaiveDateTime, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::TimestampNanosecondBuilder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    let ts = Local::now()
                                        .fixed_offset()
                                        .timezone()
                                        .from_local_datetime(&val)
                                        .unwrap()
                                        .timestamp_nanos_opt()
                                        .unwrap();

                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::TimestampNanosecondBuilder>()
                                        .unwrap()
                                        .append_value(ts);
                                }
                            }
                        }
                        _ => {
                            let val = row.try_get::<&str, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::StringBuilder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::StringBuilder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                    }
                }

                if row_count == batch_size {
                    let batch = to_batch(fields.clone(), builders).await?;
                    batches.push(batch);

                    builders = Vec::new();
                    for (_col_name, col_type) in columns.iter() {
                        let arrow_type = to_arrow_data_type(*col_type)?;
                        builders.push(array::make_builder(&arrow_type, 10));
                    }
                    row_count = 0;
                }

                row_count += 1;
            }
        }
    }

    let batch = to_batch(fields, builders).await?;
    batches.push(batch);

    Ok(batches)
}

async fn to_batch(
    fields: Vec<Field>,
    mut builders: Vec<Box<dyn ArrayBuilder>>,
) -> anyhow::Result<RecordBatch> {
    // schema
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("lush"));

    let schema = Schema::new(fields).with_metadata(metadata);
    let array_refs = builders
        .iter_mut()
        .map(|builder| Arc::new(builder.finish()) as ArrayRef)
        .collect_vec();

    let batch = RecordBatch::try_new(Arc::new(schema), array_refs)?;
    Ok(batch)
}

fn to_arrow_data_type(col_type: ColumnType) -> anyhow::Result<DataType> {
    let data_type = match col_type {
        ColumnType::Bit => DataType::Boolean,
        ColumnType::Int1 => DataType::UInt8,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 => DataType::Float64,
        ColumnType::Intn => DataType::Int32,
        ColumnType::Floatn => DataType::Float64,
        ColumnType::Datetime2 => DataType::Timestamp(Nanosecond, None),
        ColumnType::NVarchar => DataType::Utf8,
        ColumnType::BigBinary | ColumnType::BigVarBin => DataType::Binary,
        _ => Err(anyhow::anyhow!("Unsupported column type: {:?}", col_type))?,
    };

    Ok(data_type)
}

pub struct Consumer {
    id: Option<String>,
    connect: ConnectConfig,
    ipc_port: u16,
}

impl Consumer {
    pub fn new(id: Option<String>, connect: ConnectConfig, ipc_port: u16) -> Self {
        Self {
            id,
            connect,
            ipc_port,
        }
    }

    pub async fn consume(
        &mut self,
        receiver: Receiver<TaskConfig>,
        logger_tx: Sender<String>,
    ) -> anyhow::Result<()> {
        let mut client = HistorianQuery::try_connect(self.connect.clone()).await?;

        let mut rows = client
            .describe_table(HistorianTable::History)
            .await?
            .into_row_stream();
        let mut column_meta_list = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let col_meta = column_meta::ColumnMeta::try_new(&row)?;
            column_meta_list.push(col_meta);
        }
        drop(rows);
        let schema = ColumnMeta::build_schema_with_vec(column_meta_list)?;

        // IPC Tcp stream
        let socket = format!("127.0.0.1:{}", self.ipc_port);
        let stream = std::net::TcpStream::connect(socket)?;
        set_tcp_keepalive(&stream)?;
        stream.set_nonblocking(false)?;

        // ack reader stream
        let ack_stream = stream.try_clone()?;
        set_tcp_keepalive(&ack_stream)?;
        ack_stream.set_read_timeout(None)?;

        // write batch to IPC
        let (tx, rx) = flume::bounded(100);
        let writer_handler = tokio::task::spawn_blocking(move || {
            let mut writer = StreamWriter::try_new(stream, &schema)?;
            let mut row_count = 0;
            let mut batches = 0;

            while let Ok(batch) = rx.recv() {
                writer.write(&batch)?;
                tracing::debug!("migrate history write {} rows to ipc", batch.num_rows());

                row_count += batch.num_rows();
                batches += 1;
            }

            tracing::debug!(
                send.batches = batches,
                send.records = row_count,
                "sending finished, waiting for persisting"
            );
            writer.finish()?;
            anyhow::Ok(())
        });

        // receive ACK from IPC
        let ack = tokio::task::spawn_blocking(move || {
            let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush)
                .open(&ack_stream)
                .context("failed to open ack stream")?;
            for ack in ack_reader {
                if !ack.success() {
                    tracing::error!("migrate history write records error: {ack:?}",);
                    if let Some(message) = ack.message() {
                        anyhow::bail!("IPC writer error: {message}")
                    }
                }
            }
            tracing::info!("migrate history ACK reader finished");
            Ok(())
        });

        // query database and send to writer
        let mut batch_count: u64 = 1;
        while let Ok(mut task) = receiver.recv_async().await {
            task.sub_task_id = self.id.clone();

            let start = task
                .begin_datetime
                .ok_or(anyhow::anyhow!("beginDateTime cannot be None"))?;
            let end = task
                .end_datetime
                .ok_or(anyhow::anyhow!("endDateTime cannot be None"))?;

            // query
            tracing::debug!(
                "migrate history batch:{}, execute query from: {}, to: {}",
                batch_count,
                start,
                end
            );

            let batch_size = task.advanced_options.batch_size.unwrap_or(10000);
            let stream = client
                .select_from_history(task.tags.clone(), start, end)
                .await?;
            let batches = to_record_batches(stream, batch_size).await?;

            for batch in batches {
                let _ = logger_tx.send_async(to_csv_string(&batch)?).await;
                tx.send_async(batch.clone()).await?;

                batch_count += 1;
            }

            // set break point
            let task_id = task
                .task_id
                .map(|id| format!("{}", id))
                .ok_or(anyhow::anyhow!("task_id cannot be None"))?;
            let sub_task_id = task
                .sub_task_id
                .ok_or(anyhow::anyhow!("sub_task_id cannot be None"))?;
            let breakpoint = end.to_rfc3339().to_string();

            breakpoints::breakpoints_set(&task_id, &sub_task_id, &breakpoint)?;
        }
        drop(tx);

        tracing::debug!("migrate history query finished");
        writer_handler.await??;
        tracing::debug!("migrate history writer finished");
        ack.await??;
        tracing::debug!("migrate history consumer finished");
        Ok(())
    }
}

pub struct Producer {
    config: TaskConfig,
}

impl Producer {
    pub fn new(config: &TaskConfig) -> Self {
        Producer {
            config: config.clone(),
        }
    }

    pub async fn produce(&self, tx: Sender<TaskConfig>) -> anyhow::Result<()> {
        let mut window_start = self
            .config
            .begin_datetime
            .ok_or(anyhow::anyhow!("beginDateTime cannot be None"))?;
        let end = self
            .config
            .end_datetime
            .ok_or(anyhow::anyhow!("endDateTime cannot be None"))?;
        let time_window = self.config.time_window;
        tracing::debug!(
            "produce task, begin: {}, end: {}, timeWindow: {}",
            window_start,
            end,
            time_window
        );

        while window_start < end {
            let window_end = min(window_start + time_window, end);

            let tasks = self
                .config
                .tags
                .iter()
                .chunks(self.config.tag_list_size)
                .into_iter()
                .map(|list| {
                    let mut task = self.config.clone();

                    task.begin_datetime = Some(window_start);
                    task.end_datetime = Some(window_end);
                    task.tags = list.map(|s| s.to_string()).collect::<Vec<_>>();

                    task
                })
                .collect_vec();

            for task in tasks {
                tx.send_async(task).await.unwrap();
            }

            window_start = window_end;
        }

        tracing::debug!("produce task finished");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use taos::IntoDsn;

    use super::*;

    #[test]
    fn test_break_point() {
        let task_id = 99999;
        let break_point = get_break_point(Some(task_id));
        if break_point.is_none() {
            // given
            let id_string = format!("{}", task_id);
            breakpoints::breakpoints_set(id_string.as_str(), "mig-1", "2021-08-01T00:00:00Z")
                .unwrap();
            // when
            let break_point = get_break_point(Some(task_id)).unwrap();
            // then
            assert_eq!("2021-08-01T00:00:00+00:00", break_point.to_rfc3339());

            breakpoints::breakpoints_remove(id_string.as_str(), "mig-1").unwrap();
        }
    }

    #[test]
    fn test_record_batch_to_csv() {
        // given
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();
        // when
        let csv = to_csv_string(&batch).unwrap();
        // then
        assert_eq!(csv, "id,name\n1,a\n2,b\n3,c\n4,d\n5,e\n");
    }

    #[test]
    fn test_column_type_to_arrow_type() {
        assert_eq!(
            to_arrow_data_type(ColumnType::Bit).unwrap(),
            DataType::Boolean
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Int1).unwrap(),
            DataType::UInt8,
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Int4).unwrap(),
            DataType::Int32
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Int8).unwrap(),
            DataType::Int64
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Float4).unwrap(),
            DataType::Float32
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Float8).unwrap(),
            DataType::Float64
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Intn).unwrap(),
            DataType::Int32
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Floatn).unwrap(),
            DataType::Float64
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Datetime2).unwrap(),
            DataType::Timestamp(Nanosecond, None)
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::NVarchar).unwrap(),
            DataType::Utf8
        );

        assert_eq!(
            to_arrow_data_type(ColumnType::Null)
                .unwrap_err()
                .to_string(),
            "Unsupported column type: Null"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_task_producer() {
        let dsn = format!(
            "historian://aaAdmin:aaAdmin@192.168.3.40:1433/?mode={}&table={}&tags={}&tagListSize={}&beginDateTime={}&endDateTime={}&timeWindow={}",
            "migrate",
            "Runtime.dbo.History",
            "tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,tag8,tag9",
            "3",
            "2021-08-01T00:00:00Z",
            "2021-08-04T12:00:00Z",
            "1d"
        ).into_dsn().unwrap();
        let config = TaskConfig::from_dsn(&dsn).unwrap();

        let (tx, rx) = flume::bounded(4);

        let consumer = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for msg in rx.iter() {
                tasks.push(msg);
            }
            tasks
        });

        let producer = Producer::new(&config);
        producer.produce(tx).await.unwrap();

        let tasks = consumer.await.unwrap();

        assert_eq!(16, tasks.len());
        let t = tasks.first().unwrap();
        assert_eq!(
            "2021-08-01T00:00:00+00:00",
            t.begin_datetime.unwrap().to_rfc3339()
        );
        assert_eq!(
            "2021-08-02T00:00:00+00:00",
            t.end_datetime.unwrap().to_rfc3339()
        );
        assert_eq!(3, t.tags.len());
        assert_eq!("tag0", t.tags.first().unwrap());
        assert_eq!("tag1", t.tags.get(1).unwrap());
        assert_eq!("tag2", t.tags.get(2).unwrap());
    }

    #[test]
    fn test_split_tags() {
        let tags = vec![
            String::from("tag1"),
            String::from("tag2"),
            String::from("tag3"),
            String::from("tag4"),
            String::from("tag5"),
            String::from("tag6"),
            String::from("tag7"),
            String::from("tag8"),
            String::from("tag9"),
            String::from("tag10"),
        ];
        // when
        let groups = split_tags(tags, 3);
        // then
        assert_eq!(groups.len(), 4);
        let g1 = groups.first().unwrap();
        assert_eq!(g1.first().unwrap(), "tag1");
        assert_eq!(g1.get(1).unwrap(), "tag2");
        assert_eq!(g1.get(2).unwrap(), "tag3");
        let g2 = groups.get(1).unwrap();
        assert_eq!(g2.first().unwrap(), "tag4");
        assert_eq!(g2.get(1).unwrap(), "tag5");
        assert_eq!(g2.get(2).unwrap(), "tag6");
        let g3 = groups.get(2).unwrap();
        assert_eq!(g3.first().unwrap(), "tag7");
        assert_eq!(g3.get(1).unwrap(), "tag8");
        assert_eq!(g3.get(2).unwrap(), "tag9");
        let g4 = groups.get(3).unwrap();
        assert_eq!(g4.first().unwrap(), "tag10");
    }
}
