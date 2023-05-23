use anyhow::Context;
use arrow::{
    array::{ArrayRef, TimestampMillisecondArray},
    datatypes::{Field, Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::IpcWriteOptions},
    record_batch::RecordBatch,
};
use arrow_flight::{flight_service_client::FlightServiceClient, FlightClient, PutResult};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, Stream, TryFutureExt, TryStreamExt};
use std::{
    any::Any,
    cell::{RefCell, UnsafeCell},
    collections::HashMap,
    error::Error,
    f32::consts::E,
    io::{Read, Write},
    net::SocketAddr,
    panic,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};
use taos::{
    taos_query::common::views::views_to_raw_block, AsyncQueryable, Bindable, ColumnView, Dsn,
    Itertools, RawBlock, Stmt, Taos, TaosPool,
};
use tokio::sync::{mpsc::Sender, Mutex, OnceCell};
use tokio_util::sync::CancellationToken;
use tonic::IntoStreamingRequest;
use tracing::{debug, info, instrument};

use crate::{OPCConfig, Parser};

use super::runners::opc::{opc_config_blocking, opc_config_from, OpcTableConfig};
use taosx_ipc::{
    prelude::*,
    stream::{flat::FlatMessage, point::PointMessage},
};

// mod rpc_client;

// #[derive(Debug)]
// pub enum XMessageBatch {
//     Tables(Vec<(String, Option<String>, Vec<(taos::Field, taos::Value)>)>),
//     Records(Vec<(String, Vec<ColumnView>)>),
// }

#[instrument(skip(stream, cancel))]
async fn ipc_tcp_forward(
    client: String,
    stream: socket2::Socket,
    cancel: CancellationToken,
    remote: String, // "http://127.0.0.1:6051"
    token: String,
    task_id: i64,
) -> anyhow::Result<()> {
    use arrow_flight::{
        encode::{FlightDataEncoder, FlightDataEncoderBuilder},
        error::FlightError,
        FlightData,
    };
    use futures::StreamExt;
    struct FakeStream(SchemaRef, tokio::time::Interval);

    impl futures::Stream for FakeStream {
        type Item = Result<RecordBatch, FlightError>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            // std::thread::sleep(Duration::from_millis(100));
            match self.1.poll_tick(cx) {
                Poll::Ready(_) => (),
                Poll::Pending => return Poll::Pending,
            }
            // fut.poll_unpin(cx);
            let val = Arc::new(TimestampMillisecondArray::from_iter_values(vec![0, 1])) as ArrayRef;
            let item = RecordBatch::try_from_iter(vec![("ts", val)]).map_err(Into::into);
            log::info!("{item:?}");
            std::task::Poll::Ready(Some(item))
        }
    }
    struct Data {
        data: FlightDataEncoder,
    }
    impl futures::Stream for Data {
        type Item = FlightData;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.data
                .try_poll_next_unpin(cx)
                .map(|u| u.transpose().unwrap())
                .map(|u| {
                    u.map(|mut v| {
                        if v.app_metadata.is_empty() {
                            v.app_metadata = Bytes::from("request");
                            dbg!(v)
                        } else {
                            dbg!(v)
                        }
                    })
                })
        }
    }
    let ipc_reader = IpcReader::new(stream.try_clone()?)?;
    let mut ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);

    let schema = ipc_reader.schema.clone();
    dbg!(&schema);
    let (sender, receiver) = flume::bounded(5);

    tokio::spawn(async move {
        let mut batches = futures::stream::iter(ipc_reader.reader);
        while let Some(res) = batches.next().await {
            // dbg!(&res);
            if let Err(err) = sender.send(res.map_err(FlightError::from)) {
                log::warn!("sender send error: {}", err.to_string());
            }
        }
        log::error!("[task:{task_id}] stopped");
    });

    struct IpcStream {
        receiver: flume::Receiver<Result<RecordBatch, FlightError>>,
        marker: AtomicUsize,
    }
    impl IpcStream {
        fn new(receiver: flume::Receiver<Result<RecordBatch, FlightError>>) -> Self {
            Self {
                receiver,
                marker: AtomicUsize::new(0),
            }
        }
    }

    impl futures::Stream for IpcStream {
        type Item = Result<RecordBatch, FlightError>;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let c = self
                .marker
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            log::info!("polled: {c} {cx:?}");

            if c % 2 == 0 {
                // todo: why this is require?
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let recv = self.receiver.recv();
            dbg!(&recv);
            // cx.waker().wake_by_ref();
            Poll::Ready(dbg!(recv.ok()))
        }
    }

    let data = FlightDataEncoderBuilder::new()
        .with_schema(schema.clone())
        .with_options(IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap())
        .build(IpcStream::new(receiver));
    let channel = tonic::transport::Endpoint::try_from(remote)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);
    let res = client
        .handshake(Bytes::from(token.as_bytes().to_vec()))
        .await?;
    dbg!(res);
    client.add_header("x-task-id", &task_id.to_string())?;
    client.add_header("x-token", &token)?;
    let mut stream = client.do_put(data).await.unwrap();

    while let Some(res) = stream.next().await {
        let res: PutResult = dbg!(res?);
        dbg!(res.app_metadata);
        ipc_ack_writer.write_ok()?;
    }

    info!("[{task_id}] Putting stream finished");
    Ok(())
}

// #[instrument(skip_all)]
async fn ipc_tcp_read(
    client: String,
    pool: TaosPool,
    stream: socket2::Socket,
    lock: Arc<Mutex<()>>,
    config: Option<OpcTableConfig>,
    cancel: CancellationToken,
    parser: Option<Parser>,
) -> anyhow::Result<()> {
    // let stream = Arc::new(stream);
    // let reader = stream.clone();
    let ipc_reader = IpcReader::new(&stream)?;
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    let client = client.to_string();
    tokio::select! {
        _ = cancel.cancelled() => {
            log::debug!("cancel IPC worker");
            Ok(())
        },
        done = ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config, parser) => {
            log::info!("IPC stopped");
            done
        }
    }
}

#[cfg(not(target_os = "windows"))]
async fn ipc_unix_read(
    client: String,
    pool: TaosPool,
    stream: std::os::unix::net::UnixStream,
    lock: Arc<Mutex<()>>,
    config: Option<OpcTableConfig>,
) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config, None).await
}

// #[instrument(skip(taos, record, names, marks))]
async fn consume_lush_record(
    taos: &Taos,
    stmt: &mut Stmt,
    record: LushMessage,
    columns: &Vec<String>,
    names: &str,
    marks: &str,
    records: &mut usize,
) -> anyhow::Result<()> {
    match record {
        LushMessage::Tables(tables) => {
            for table in tables {
                let sql = table.to_sql(None).unwrap();
                info!("Tables: {sql}");
                taos.exec(&sql).await?;
            }
        }
        LushMessage::Insert(record) => {
            let sql = format!("insert into ? ({names}) values({marks})");
            info!("prepare with sql: {sql}");
            stmt.prepare(&sql)?;
            info!("prepare");
            for record in record {
                *records += record.num_rows();
                // let data = record.to_column_views();
                // RawBlock
                let map_data = record.to_column_views_group_by_tablename();
                // taos.write_raw_block()
                // dbg!(&map_data);
                for (k, data_vec) in &map_data {
                    let table_name = k.as_deref().or(record.table());
                    if let Some(table_name) = table_name {
                        if let Err(err) = stmt.set_tbname(table_name) {
                            tracing::warn!("table name `{}` error {err}", table_name);
                            if let Some(tb) = record.meta_sql(Some(String::from(table_name))) {
                                info!("sql: {tb}");
                                taos.exec_sync(&tb)?;
                                stmt.set_tbname(table_name)?;
                            }
                        }
                        debug_assert!(columns.len() == data_vec.len());
                        let mut column_value_pairs:Vec<(String, String)> = Vec::new();
                        for (index, v) in data_vec.iter().enumerate() {
                            let mut i = 0;
                            while i < v.len() {
                                let mut temp_column_value_pair= column_value_pairs.get_mut(i);
                                if temp_column_value_pair.is_none() {
                                    let pair = (String::new(), String::new());
                                    column_value_pairs.insert(i, pair);
                                    temp_column_value_pair= column_value_pairs.get_mut(i);
                                }
                                let temp_column_value_pair = temp_column_value_pair.unwrap();
                                if let Some(v) = v.get(i) {
                                    if !v.is_null() {
                                        temp_column_value_pair.0.push('`');
                                        temp_column_value_pair.0.push_str(columns[index].as_str());
                                        temp_column_value_pair.0.push_str("`,");
                                        temp_column_value_pair.1.push('\'');
                                        temp_column_value_pair.1.push_str(v.into_value().to_string().unwrap().as_str());
                                        temp_column_value_pair.1.push('\'');
                                        temp_column_value_pair.1.push_str(",");
                                    } else {
                                        // ignore null columnview
                                        log::debug!("column view {} is null", columns[index]);
                                    }
                                } else {
                                    log::debug!("column view {} is null", columns[index]);
                                }
                                i = i + 1;
                            }   
                        }
                        column_value_pairs.into_iter().for_each(|(mut c, mut v)| {
                            let mut column_names = String::from("(");
                            let mut values = String::from("(");
                            c.pop();
                            column_names.push_str(c.as_str());
                            column_names.push(')');
                            v.pop();
                            values.push_str(v.as_str());
                            values.push(')');
                            let sql = format!("insert into `{table_name}` {column_names} VALUES {values}");
                            log::debug!("sql: {sql}");
                            let res = taos.exec_sync(sql);
                            match res {
                                Ok(num) => {
                                    info!("written [{num}] records for table {table_name}");
                                }
                                Err(err) => {
                                    log::error!("written err for {table_name} cause: {}", err);
                                }
                            }
                        });
                    } else {
                        stmt.bind(data_vec.as_slice())?;
                        stmt.add_batch().unwrap();
                        let n = stmt.execute()?;

                        info!("written : [{n}] records");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn consume_point_record(
    stmt: &mut Stmt,
    record: &PointMessage,
    count: &mut usize,
    config: &OpcTableConfig,
) -> anyhow::Result<()> {
    for message in record.records() {
        let cv_vec = taosx_ipc::stream::reader::record_batch_to_column_view(message.record());
        // process id, name, ts, value, status
        let schema = message.schema();
        let id_index = schema.index_of("id")?;
        let name_index = schema.index_of("name")?;
        let server_ts_index = schema.index_of("ts")?;
        let value_index = schema.index_of("value")?;
        let received_index = schema.index_of("received")?;
        let status_index = schema.index_of("status")?;
        let id_cv = cv_vec.get(id_index).unwrap();
        let name_cv = cv_vec.get(name_index).unwrap();
        let server_ts_cv = cv_vec.get(server_ts_index).unwrap();
        let received_ts_cv = cv_vec.get(received_index).unwrap();
        let value_cv = cv_vec.get(value_index).unwrap();
        let status_cv = cv_vec.get(status_index).unwrap();

        let table_info = &config.table_info;
        let ts_cloumn = &config.ts_cloumn_name;
        for i in 0..id_cv.len() {
            let id = id_cv.get(i).unwrap().into_value().to_string().unwrap();
            let table_info = table_info.get(&id);
            if table_info.is_none() {
                log::warn!("id: {} cannot get table info", id);
                continue;
            }
            let (table, field, _) = table_info.unwrap();
            let (ts_cloumn_name, server_ts_column_name) = ts_cloumn.get(table).unwrap();
            let mut new_cv_vec = Vec::new();
            let sql = if config.use_received_time {
                let server_ts_column_name = server_ts_column_name.clone().unwrap();
                new_cv_vec.push(received_ts_cv.slice(i..i+1).unwrap());
                new_cv_vec.push(value_cv.slice(i..i+1).unwrap());
                new_cv_vec.push(server_ts_cv.slice(i..i+1).unwrap());
                format!("insert into {table} ({ts_cloumn_name}, {field}, {server_ts_column_name}) values (?, ?, ?)")
            } else {
                new_cv_vec.push(server_ts_cv.slice(i..i+1).unwrap());
                new_cv_vec.push(value_cv.slice(i..i+1).unwrap());
                format!("insert into {table} ({ts_cloumn_name}, {field}) values (?, ?)")
            };
            debug!("sql: {}", sql);
            stmt.prepare(&sql).unwrap();
            stmt.bind(&new_cv_vec.as_slice())
                .context("STMT binding error")?;
            stmt.add_batch().context("STMT adding batch error")?;
            let res = stmt.execute();
            match res {
                Ok(n) => *count += n,
                Err(err) => {
                    let block = RawBlock::parse_from_raw_block(
                        views_to_raw_block(&new_cv_vec),
                        taos::Precision::Millisecond,
                    );
                    let block = block.pretty_format();
                    log::error!("execute error, {}, data: {}", err.to_string(), block);
                }
            }
        }
    }
    Ok(())
}

async fn consume_flat_record(
    _taos: &Taos,
    record: &FlatMessage,
    _count: &mut usize,
    parser: Option<&Parser>,
) -> anyhow::Result<()> {
    // let stmt = Stmt::init(_taos)?;
    let mut max_lengths = HashMap::new();

    for message in record.records() {
        let batch = message.record();
        if let Some(parser) = parser {
            let batch = parser.parse_message_from_records(batch)?;
            // dbg!(&batch);
            match batch {
                crate::plugins::transform::Message::Raw(_) => todo!(),
                crate::plugins::transform::Message::Tables(_) => todo!(),
                crate::plugins::transform::Message::ChildTables(_) => todo!(),
                crate::plugins::transform::Message::Records(message) => {
                    for records in message {
                        // dbg!(&records);
                        let views = taosx_ipc::stream::reader::record_batch_to_column_view(
                            &records.records,
                        );
                        let schema = records.records.schema();
                        let columns = schema.fields().iter().map(|f| f.name()).collect_vec();
                        let table_name = records.table.name.as_str();
                        let mut raw = RawBlock::from_views(&views, taos::Precision::Millisecond);
                        raw.with_field_names(&columns).with_table_name(table_name);
                        info!("{}", &raw.pretty_format());

                        loop {
                            let var_views = views
                                .iter()
                                .zip(&columns)
                                .filter(|(v, _)| v.as_ty().is_var_type())
                                .map(|(view, name)| {
                                    (name, view.as_ty(), view.max_variable_length())
                                })
                                .collect_vec();
                            if var_views.len() > 0 {
                                for (name, ty, length) in var_views {
                                    if let Some(max) = max_lengths.get(*name) {
                                        if *max >= length {
                                            continue;
                                        }
                                    }
                                    loop {
                                        let res = _taos.describe(table_name).await;
                                        match res {
                                            Ok(desc) => {
                                                if let Some(col) =
                                                    desc.iter().find(|f| f.field() == name.as_str())
                                                {
                                                    debug_assert!(ty == col.ty());
                                                    if col.length() < length {
                                                        let table = records
                                                            .table
                                                            .using
                                                            .as_deref()
                                                            .unwrap_or(table_name);
                                                        let sql = format!(
                                                        "alter table `{table}` modify column `{}` {}({})",
                                                        name,
                                                        ty,
                                                        length
                                                        );
                                                        _taos.exec(&sql).await.unwrap();
                                                        max_lengths
                                                            .insert(name.to_string(), length);
                                                        continue;
                                                    }
                                                }
                                                break;
                                            }
                                            Err(err) => {
                                                dbg!(&err);
                                                if let Some(sql) = records.stable_sql() {
                                                    dbg!(&sql);
                                                    _taos.exec(&sql).await.unwrap();
                                                    let sql = records.table_sql();

                                                    loop {
                                                        if let Err(err) = _taos.exec(&sql).await {
                                                            if err.to_string().contains("[0x2605]")
                                                            {
                                                                let table = records
                                                                    .table
                                                                    .using
                                                                    .as_deref()
                                                                    .unwrap();
                                                                let desc = _taos
                                                                    .describe(table)
                                                                    .await
                                                                    .unwrap();
                                                                for f in desc.iter().filter(|f| {
                                                                    f.is_tag()
                                                                        && f.ty().is_var_type()
                                                                }) {
                                                                    let sql = format!(
                                                        "alter table `{table}` modify tag `{}` {}({})",
                                                        f.field(),
                                                        f.ty(),
                                                        f.length() * 2
                                                        );
                                                                    _taos.exec(&sql).await.unwrap();
                                                                    continue;
                                                                }
                                                            } else {
                                                                Err(err)?;
                                                            }
                                                        }
                                                        break;
                                                    }
                                                    //.inspect_err(|err| tracing::warn!("{}", err))?
                                                } else {
                                                    let sql = records.table_sql();
                                                    dbg!(&sql);
                                                    _taos.exec(&sql).await.unwrap();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if let Err(err) = _taos.write_raw_block(&raw).await {
                                dbg!(&err);
                                let err_str = err.to_string();
                                if err_str.contains("[0x2603]") {
                                    if let Some(sql) = records.stable_sql() {
                                        dbg!(&sql);
                                        _taos.exec(&sql).await.unwrap();
                                        let sql = records.table_sql();

                                        loop {
                                            if let Err(err) = _taos.exec(&sql).await {
                                                if err.to_string().contains("[0x2605]") {
                                                    let table =
                                                        records.table.using.as_deref().unwrap();
                                                    let desc = _taos.describe(table).await.unwrap();
                                                    for f in desc.iter().filter(|f| {
                                                        f.is_tag() && f.ty().is_var_type()
                                                    }) {
                                                        let sql = format!(
                                                        "alter table `{table}` modify tag `{}` {}({})",
                                                        f.field(),
                                                        f.ty(),
                                                        f.length() * 2
                                                        );
                                                        _taos.exec(&sql).await.unwrap();
                                                        continue;
                                                    }
                                                } else {
                                                    Err(err)?;
                                                }
                                            }
                                            break;
                                        }
                                        //.inspect_err(|err| tracing::warn!("{}", err))?
                                    } else {
                                        let sql = records.table_sql();
                                        dbg!(&sql);
                                        _taos.exec(&sql).await.unwrap();
                                    }

                                    continue;
                                } else if err_str.contains("[0x2605]") {
                                    // container length is too short.
                                    let desc = _taos.describe(table_name).await.unwrap();
                                    let table =
                                        records.table.using.as_deref().unwrap_or(table_name);
                                    for f in
                                        desc.iter().filter(|f| !f.is_tag() && f.ty().is_var_type())
                                    {
                                        let sql = format!(
                                            "alter table `{table}` modify column `{}` {}({})",
                                            f.field(),
                                            f.ty(),
                                            f.length() * 2
                                        );
                                        _taos.exec(&sql).await.unwrap();
                                    }
                                } else {
                                    Err(err)?;
                                    break;
                                }
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            let cv_vec = taosx_ipc::stream::reader::record_batch_to_column_view(batch);
            // let mut stmt = Stmt::init(&taos)?;
            // process id, ts, value
            dbg!(&cv_vec);
            anyhow::bail!("Parser should be set with flat stream");
        }
    }
    Ok(())
}

// #[instrument(skip_all)]
async fn ipc_lush_stream_reader<R: Read, W: Write>(
    taos: &Taos,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    let columns = ipc_reader.columns().into_iter().map(|s| format!("{s}")).collect_vec();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");
    let mut stmt = Stmt::init(taos)?;

    let mut count = 0;

    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_lush_record(&taos, &mut stmt, record, &columns, &names, &marks, &mut count).await?;
            ipc_ack_writer.write_ok()?;
        }
    }
    println!("finished, totally {count} rows");
    Ok(())
}

// #[instrument(skip_all)]
async fn ipc_point_reader<R: Read, W: Write>(
    taos: &Taos,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    config: Option<OpcTableConfig>,
) -> anyhow::Result<()> {
    let mut count = 0;
    let mut stmt = Stmt::init(taos)?;
    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_point_record(&mut stmt, &record, &mut count, config.as_ref().unwrap()).await?;

            ipc_ack_writer.write_ok()?;
        }
    }
    println!("finished, totally {count} rows");
    Ok(())
}

async fn ipc_flat_stream_reader<R: Read, W: Write>(
    taos: &Taos,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    parser: Option<&Parser>,
) -> anyhow::Result<()> {
    let mut count = 0;
    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_flat_record(&taos, &record, &mut count, parser).await?;

            ipc_ack_writer.write_ok()?;
        }
    }
    println!("finished, totally {count} rows");
    Ok(())
}

#[instrument(skip(pool, ipc_reader, ipc_ack_writer, config))]
async fn ipc_process<R: Read, W: Write>(
    client: String,
    pool: TaosPool,
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
    lock: Arc<Mutex<()>>,
    config: Option<OpcTableConfig>,
    parser: Option<Parser>,
) -> anyhow::Result<()> {
    let taos = pool.get().await?;
    let metadata = ipc_reader.metadata();
    let stream_type = *metadata.stream_type();
    if let Some(sql) = ipc_reader.metadata().init_sql_string() {
        let guard = lock.lock().await;
        let max_retries = 10;
        let mut i = 0;
        loop {
            info!("[{client}] {sql}");
            // rt.block_on(taos.exec(&sql))?;
            let res: Result<usize, taos::Error> = taos.exec(&sql).await;
            if let Err(err) = res {
                tracing::error!("Query error with {sql}: {err:?}");
                i += 1;
                if i > max_retries {
                    break;
                }
            } else {
                break;
            }
        }
        drop(guard)
    }
    match stream_type {
        StreamType::Line => todo!(),
        StreamType::Flat => {
            ipc_flat_stream_reader(&taos, ipc_reader, ipc_ack_writer, parser.as_ref()).await?
        }
        StreamType::Lush => ipc_lush_stream_reader(&taos, ipc_reader, ipc_ack_writer).await?,
        StreamType::Point => ipc_point_reader(&taos, ipc_reader, ipc_ack_writer, config).await?,
    }
    Ok(())
}

pub struct IpcStreamWorker<'a> {
    taos: &'a Taos,
    parser: IpcParser,
    lock: Arc<Mutex<()>>,
    task: Option<i64>,
    from: Dsn,
    config: Option<OPCConfig>,
    opc_table_config: OnceCell<OpcTableConfig>,
    // stmt: Arc<UnsafeCell<Stmt>>,
}

unsafe impl<'a> Send for IpcStreamWorker<'a> {}
unsafe impl<'a> Sync for IpcStreamWorker<'a> {}

impl<'a> IpcStreamWorker<'a> {
    pub fn new(
        taos: &'a Taos,
        from: Dsn,
        lock: Arc<Mutex<()>>,
        schema: Arc<Schema>,
    ) -> anyhow::Result<Self> {
        let config = if from.driver.starts_with("opc") {
            Some(opc_config_blocking(taos, &from, 1)?)
        } else {
            None
        };

        // let stmt = Stmt::init(&taos)?;
        Ok(Self {
            taos,
            from,
            parser: IpcParser::new(schema),
            lock: lock,
            task: None,
            config,
            opc_table_config: OnceCell::const_new(),
            // stmt: Arc::new(UnsafeCell::new(stmt)),
        })
    }

    pub fn with_presets(mut self, preset: OPCConfig) -> Self {
        self.config.replace(preset);
        self
    }

    pub async fn process_record(
        &self,
        stmt: &mut Stmt,
        record: RecordBatch,
    ) -> anyhow::Result<usize> {
        if let Some(sql) = self.parser.metadata().init_sql_string() {
            let guard = self.lock.lock().await;
            let max_retries = 10;
            let mut i = 0;
            loop {
                info!("metadata sql: {sql}");
                let res = self.taos.exec(&sql).await;
                if let Err(err) = res {
                    tracing::error!("Query error with {sql}: {err:?}");
                    i += 1;
                    if i > max_retries {
                        break;
                    }
                } else {
                    break;
                }
            }
            drop(guard);
        }
        // let stmt = unsafe { &mut *self.stmt.get() };
        match self.parser.metadata().stream_type() {
            StreamType::Line => {
                todo!()
            }
            StreamType::Flat => {
                let message = self.parser.parse(record)?;
                let mut count = 0;
                let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .unwrap();
                // todo: parser
                consume_flat_record(&self.taos, &record, &mut count, None).await?;
                Ok(count)
            }
            StreamType::Lush => {
                let columns = self.parser.columns().into_iter().map(|s| format!("{s}")).collect_vec();
                let names = columns.iter().map(|n| format!("`{n}`")).join(",");
                let marks = std::iter::repeat('?').take(columns.len()).join(",");

                let message = self.parser.parse(record)?;
                let mut count = 0;

                let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .map_err(|_| anyhow::format_err!("Unable to read lush message"))?;
                // let stmt = unsafe { &mut *self.stmt.get() };
                consume_lush_record(&self.taos, stmt, record, &columns, &names, &marks, &mut count).await?;
                Ok(count)
            }
            StreamType::Point => {
                let message = self.parser.parse(record)?;
                let mut count = 0;
                let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .unwrap();
                let config = self
                    .config
                    .as_ref()
                    .ok_or_else(|| anyhow::format_err!("OPC table config not found"))?;

                let guard = self.lock.lock().await;
                let res = self.opc_table_config.get();
                let config = match res {
                    Some(config) => config,
                    None => {
                        let v = config.parse_tables_with(&self.taos).await?;
                        self.opc_table_config
                            .get_or_try_init(|| async {
                                config.parse_tables_with(&self.taos).await
                            })
                            .await?
                    }
                };
                drop(guard);
                consume_point_record(stmt, &record, &mut count, config).await?;
                Ok(count)
            }
        }
    }

    // pub async fn consume<'b: 'a, E: Error>(
    //     &'a self,
    //     stream: impl 'b + Stream<Item = Result<RecordBatch, E>>,
    // ) -> impl 'a + Stream<Item = anyhow::Result<usize>> {
    //     let metadata = self.parser.metadata();
    //     let stream_type = *metadata.stream_type();
    //     if let Some(sql) = self.parser.metadata().init_sql_string() {
    //         let guard = self.lock.lock().await;
    //         loop {
    //             info!("[] {sql}");
    //             let res = self.taos.exec(&sql).await;
    //             if let Err(err) = res {
    //                 tracing::error!("Query error with {sql}: {err:?}");
    //             } else {
    //                 break;
    //             }
    //         }
    //         drop(guard)
    //     }
    //     use futures::StreamExt;
    //     stream
    //         .map_err(|err| anyhow::format_err!("Parse record error: {err}\n\n {:?}", err.source()))
    //         .try_filter_map(|record| async { self.process_record(record).await.map(Some) })
    // }
}

#[cfg(unix)]
pub fn listen_unix_socket(
    target: TaosPool,
    socket: impl AsRef<Path>,
    config: Option<OpcTableConfig>,
) -> anyhow::Result<()> {
    let path = socket.as_ref();
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .build()?;
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let sql_lock = Arc::new(Mutex::new(()));
    info!("listen on socket address: {}", path.display());
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new unix client!: {:?}", addr);
                let pool = target.clone();
                let lock = sql_lock.clone();
                let config = config.clone();
                runtime.spawn(async move {
                    ipc_unix_read(
                        addr.as_pathname()
                            .map(|path| path.display().to_string())
                            .unwrap_or_default(),
                        pool,
                        stream,
                        lock,
                        config,
                    )
                    .await
                });
            }
            Err(e) => {
                /* connection failed */
                tracing::debug!("IPC stream acceptation error {e}, might be stopped");
            }
        }
    }
}

pub fn listen_tcp_socket_with_agent(
    socket: impl AsRef<str>,
    sender: Sender<String>,
    config: Option<OpcTableConfig>,
    cancel: CancellationToken,
    with_agent: (i64, String, String),
) -> anyhow::Result<std::sync::mpsc::Sender<()>> {
    let addr = socket.as_ref();
    use socket2::{Domain, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    let addr: SocketAddr = addr.parse()?;
    socket.bind(&addr.into())?;
    socket.set_keepalive(true)?;
    socket.set_nonblocking(false)?;
    socket.listen(128)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("ipc-runner-{}", id)
        })
        .build()?;
    // info!("listen on socket address: {tcp_addr}");
    let sql_lock = Arc::new(Mutex::new(()));
    let socket = Arc::new(socket);
    let closer_socket = socket.clone();

    let (closer, receiver) = std::sync::mpsc::channel::<()>();
    let closed = Arc::new(AtomicBool::new(false));
    let closed2 = closed.clone();

    std::thread::spawn(move || {
        let _ = receiver.recv();
        tracing::debug!("shutdown socket");
        closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = closer_socket.shutdown(std::net::Shutdown::Both);

        // runtime.shutdown_background();
    });

    std::thread::spawn(move || {
        loop {
            if closed2.load(std::sync::atomic::Ordering::SeqCst) {
                tracing::debug!("IPC stopped");
                break;
            }
            match socket.accept() {
                Ok((stream, addr)) => {
                    log::info!("new tcp client!: {:?}", addr);
                    let client = addr.as_socket_ipv4().unwrap().to_string();
                    let se = sender.clone();
                    let cancel = cancel.clone();
                    let (id, remote, token) = with_agent.clone();

                    runtime.spawn(async move {
                        let client = addr.as_socket_ipv4().unwrap().to_string();
                        let res =
                            ipc_tcp_forward(client.clone(), stream, cancel, remote, token, id)
                                .await;
                        if let Err(err) = res {
                            // panic!("{err:?}");
                            log::error!("ipc read err: {}", err);
                            let _ = se.send(err.to_string()).await;
                        } else {
                            log::info!("IPC reader stopped for client {client}",);
                        }
                    });
                }
                Err(e) => {
                    /* connection failed */
                    tracing::debug!("IPC stream acceptation error {e}, might be stopped");
                    break;
                }
            }
        }
        log::info!("IPC stream listener stopped");
    });

    Ok(closer)
}

pub fn listen_tcp_socket(
    target: TaosPool,
    socket: impl AsRef<str>,
    sender: Sender<String>,
    config: Option<OpcTableConfig>,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    parser: Option<Parser>,
) -> anyhow::Result<std::sync::mpsc::Sender<()>> {
    let addr = socket.as_ref();
    use socket2::{Domain, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    let addr: SocketAddr = addr.parse()?;
    socket.bind(&addr.into())?;
    socket.set_keepalive(true)?;
    socket.set_nonblocking(false)?;
    socket.listen(128)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .build()?;
    // info!("listen on socket address: {tcp_addr}");
    let sql_lock = Arc::new(Mutex::new(()));
    let socket = Arc::new(socket);
    let closer_socket = socket.clone();

    let (closer, receiver) = std::sync::mpsc::channel::<()>();
    let closed = Arc::new(AtomicBool::new(false));
    let closed2 = closed.clone();

    std::thread::spawn(move || {
        let _ = receiver.recv();
        tracing::debug!("shutdown socket");
        closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = closer_socket.shutdown(std::net::Shutdown::Both);
        // runtime.shutdown_background();
    });

    std::thread::spawn(move || {
        loop {
            if closed2.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            match socket.accept() {
                Ok((stream, addr)) => {
                    log::info!("new tcp client!: {:?}", addr);
                    let client = addr.as_socket_ipv4().unwrap().to_string();
                    let se = sender.clone();
                    let cancel = cancel.clone();

                    if let Some((id, server, token)) = with_agent.clone() {
                        runtime.spawn(async move {
                            let res =
                                ipc_tcp_forward(client, stream, cancel, server, token, id).await;
                            if let Err(err) = res {
                                // panic!("{err:?}");
                                log::error!("ipc read err: {}", err);
                                let _ = se.send(err.to_string()).await;
                            }
                        });
                    } else {
                        let pool = target.clone();
                        let lock = sql_lock.clone();
                        let config = config.clone();
                        let parser = parser.clone();
                        runtime.spawn(async move {
                            let res =
                                ipc_tcp_read(client, pool, stream, lock, config, cancel, parser)
                                    .await;
                            if let Err(err) = res {
                                // panic!("{err:?}");
                                log::error!("ipc read err: {}", err);
                                let _ = se.send(err.to_string()).await;
                            }
                        });
                    }
                }
                Err(e) => {
                    /* connection failed */
                    tracing::debug!("IPC stream acceptation error {e}, might be stopped");
                }
            }
        }
    });

    Ok(closer)
}
