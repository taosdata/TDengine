use anyhow::Context;
use arrow::{
    array::{ArrayRef, TimestampMillisecondArray},
    datatypes::{Schema, SchemaRef},
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{FlightClient, PutResult};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::column;
use std::{
    any::Any,
    collections::HashMap,
    io::{Read, Write},
    net::SocketAddr,
    panic,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration, ops::DerefMut,
};
use taos::{
    taos_query::common::views::views_to_raw_block, AsyncQueryable, Bindable, Dsn, Itertools,
    RawBlock, Stmt, Taos, TaosPool, Ty,
};
use tokio::sync::{mpsc::Sender, Mutex, OnceCell};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};

use crate::{ConnectorLicense, OPCConfig, Parser, Transferred};

use super::runners::opc::{opc_config_blocking, ColumnConfig, OpcTableConfig, TableConfig};
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
    let _ = cancel;
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
                break;
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
    connector: Option<&'static str>,
    transferred: Option<Arc<Transferred>>,
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
        done = ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config, parser, connector, transferred) => {
            log::info!("IPC stopped");
            done
        }
    }
}

// #[cfg(unix)]
// async fn ipc_unix_read(
//     client: String,
//     pool: TaosPool,
//     stream: std::os::unix::net::UnixStream,
//     lock: Arc<Mutex<()>>,
//     config: Option<OpcTableConfig>,
// ) -> anyhow::Result<()> {
//     let ipc_reader = IpcReader::new(&stream).unwrap();
//     let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
//     ipc_process(
//         client,
//         pool,
//         ipc_reader,
//         ipc_ack_writer,
//         lock,
//         config,
//         None,
//         None,
//         None,
//     )
//     .await
// }

// #[instrument(skip(taos, record, names, marks))]
async fn consume_lush_record(
    taos: &Taos,
    stmt: &mut Stmt,
    record: LushMessage,
    columns: &Vec<String>,
    names: &str,
    marks: &str,
    records: &mut usize,
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    if let Some((license, transferred)) = license.zip(transferred) {
        let used = transferred.records.load(Ordering::SeqCst);
        if used > license.number as _ {
            anyhow::bail!(
                "Connector {} out of number: {}/{}",
                license.r#type,
                used,
                license.number
            );
        }
    }
    match record {
        LushMessage::Tables(tables) => {
            for table in tables {
                let sql = table.to_sql(None).unwrap();
                info!("Tables: {sql}");
                taos.exec(&sql).await?;
                if let Some(transferred) = transferred {
                    transferred.tables.fetch_add(1, Ordering::SeqCst);
                }
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
                        let mut column_value_pairs: Vec<(String, String)> = Vec::new();
                        for (index, v) in data_vec.iter().enumerate() {
                            let mut i = 0;
                            while i < v.len() {
                                let mut temp_column_value_pair = column_value_pairs.get_mut(i);
                                if temp_column_value_pair.is_none() {
                                    let pair = (String::new(), String::new());
                                    column_value_pairs.insert(i, pair);
                                    temp_column_value_pair = column_value_pairs.get_mut(i);
                                }
                                let temp_column_value_pair = temp_column_value_pair.unwrap();
                                if let Some(v) = v.get(i) {
                                    if !v.is_null() {
                                        temp_column_value_pair.0.push('`');
                                        temp_column_value_pair.0.push_str(columns[index].as_str());
                                        temp_column_value_pair.0.push_str("`,");
                                        // temp_column_value_pair.1.push('\'');
                                        temp_column_value_pair
                                            .1
                                            .push_str(v.into_value().to_sql_value().as_str());
                                        // temp_column_value_pair.1.push('\'');
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
                        let mut count = 0;
                        for (mut c, mut v) in column_value_pairs {
                            let mut column_names = String::from("(");
                            let mut values = String::from("(");
                            c.pop();
                            column_names.push_str(c.as_str());
                            column_names.push(')');
                            v.pop();
                            values.push_str(v.as_str());
                            values.push(')');
                            let sql = format!(
                                "insert into `{table_name}` {column_names} VALUES {values}"
                            );
                            log::debug!("sql: {sql}");
                            let res = taos.exec(sql).await;
                            match res {
                                Ok(num) => {
                                    count = count + num;
                                }
                                Err(err) => {
                                    log::error!("written err for {table_name} cause: {}", err);
                                }
                            }
                        }
                        info!("written [{count}] records for table {table_name}");
                    } else {
                        stmt.bind(data_vec.as_slice())?;
                        stmt.add_batch().unwrap();
                        let n = stmt.execute()?;

                        info!("written : [{n}] records");
                        if let Some(transferred) = transferred {
                            transferred.records.fetch_add(n as _, Ordering::SeqCst);
                            transferred
                                .points
                                .fetch_add((n * data_vec.len()) as _, Ordering::SeqCst);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn consume_point_record(
    taos: &Taos,
    stmt: &mut Stmt,
    record: &PointMessage,
    count: &mut usize,
    config: &OpcTableConfig,
) -> anyhow::Result<usize> {
    let mut points = 0;
    for message in record.records() {
        let cv_vec = taosx_ipc::stream::reader::record_batch_to_column_view(message.record());
        // process id, name, ts, value, status
        let schema = message.schema();
        let id_index = schema.index_of("id")?;
        let name_index = schema.index_of("name")?;
        let server_ts_index = schema.index_of("ts")?;
        let value_index = schema.index_of("value")?;
        let value_field = schema.field_with_name("value")?;
        let received_index = schema.index_of("received")?;
        let status_index = schema.index_of("status")?;
        let id_cv = cv_vec.get(id_index).unwrap();
        let name_cv = cv_vec.get(name_index).unwrap();
        let server_ts_cv = cv_vec.get(server_ts_index).unwrap();
        let received_ts_cv = cv_vec.get(received_index).unwrap();
        let value_cv = cv_vec.get(value_index).unwrap();
        let status_cv = cv_vec.get(status_index).unwrap();

        let id_code_map = &config.id_code_map;
        let table_config = &config.table_config;
        let value_type = IpcDataType::from(value_field.data_type()).sql_repr();

        let mut stable_prefix = table_config.stable_prefix.clone();
        let stable_name = if value_type.contains("varchar") {
            stable_prefix.push_str("_varchar");
            stable_prefix
        } else if value_type.contains("nchar") {
            stable_prefix.push_str("_nchar");
            stable_prefix
        } else {
            stable_prefix.push_str(format!("_{value_type}").as_str());
            stable_prefix
        };
        let mut columns = String::new();
        let mut columns_insert: Vec<(String, String)> = Vec::new(); // first is primary key info, its type should be timestamp
        for column_config in &table_config.column_configs {
            if column_config.is_primary_key {
                let primary_key_column_name = column_config.column_name.clone();
                let prinmary_key_column_alias = column_config
                    .column_alias
                    .clone()
                    .unwrap_or(primary_key_column_name.clone());
                columns_insert.insert(
                    0,
                    (primary_key_column_name, prinmary_key_column_alias.clone()),
                );
                columns.insert_str(
                    0,
                    format!("{prinmary_key_column_alias} TIMESTAMP,").as_str(),
                );
            } else {
                let primary_key_column_name = column_config.column_name.clone();
                let prinmary_key_column_alias = column_config
                    .column_alias
                    .clone()
                    .unwrap_or(primary_key_column_name.clone());
                columns_insert.push((primary_key_column_name, prinmary_key_column_alias.clone()));
                let column_type = if column_config.column_type.is_some() {
                    column_config.column_type.unwrap().to_string()
                } else {
                    value_type.clone()
                };
                columns
                    .push_str(format!("`{prinmary_key_column_alias}` {},", column_type).as_str());
            }
        }
        // remove last char
        columns.pop();
        let tags = "`point_id` VARCHAR(256), `point_name` VARCHAR(256)";
        let stable_sql = format!(
            "create table if not exists `{}` ({}) tags ({})",
            stable_name, columns, tags
        );
        for i in 0..id_cv.len() {
            let id = id_cv.get(i).unwrap().into_value().to_string().unwrap();
            let code = id_code_map.get(&id);
            if code.is_none() {
                log::warn!("id: {} cannot get code", id);
                continue;
            }
            let mut child_table_name = stable_name.clone();
            child_table_name.push_str(format!("_{}", code.unwrap()).as_str());
            let mut insert_sql = format!("insert into `{child_table_name}` ");
            let mut values = String::new();
            let mut value_cloumn_name = "value";
            let mut value_cloumn_length = 128;
            let mut columns = String::new();
            for (temp_name, temp_alias) in &columns_insert {
                if temp_name == "received_time" {
                    values.push_str(
                        format!(
                            "{},",
                            received_ts_cv
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        )
                        .as_str(),
                    );
                } else if temp_name == "original_time" {
                    values.push_str(
                        format!(
                            "{},",
                            server_ts_cv
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        )
                        .as_str(),
                    );
                } else if temp_name == "value" {
                    let value_column = value_cv
                        .slice(i..i + 1)
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .into_value()
                        .to_sql_value();
                    values.push_str(format!("{value_column},").as_str());
                    value_cloumn_name = temp_alias;
                    value_cloumn_length = value_column.len();
                } else if temp_name == "status" {
                    values.push_str(
                        format!(
                            "{},",
                            status_cv
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        )
                        .as_str(),
                    );
                }
                columns.push_str(format!("`{temp_alias}`,").as_str());
            }
            values.pop();
            columns.pop();
            let point_name = name_cv
                .slice(i..i + 1)
                .unwrap()
                .get(0)
                .unwrap()
                .to_sql_value();
            insert_sql.push_str(
                format!(
                    " USING `{stable_name}` TAGS (\"{id}\", {}) ({columns})",
                    &point_name
                )
                .as_str(),
            );
            insert_sql.push_str(format!(" VALUES ({})", values).as_str());
            debug!("insert sql: {}", insert_sql);
            loop {
                let sql_res = taos.exec(&insert_sql).await;
                match sql_res {
                    Ok(n) => {
                        *count += n;
                        points += n;
                        break;
                    }
                    Err(err) => {
                        let errstr = err.to_string();
                        log::warn!("error: {}", errstr);
                        if errstr.contains("[0x2603]") {
                            // stable not exists
                            log::info!("create stable sql: {}", &stable_sql);
                            taos.exec(&stable_sql).await?;
                        } else if errstr.contains("[0x2602]") || errstr.contains("[0x263F]") {
                            // Illegal number of columns, alter to add columns
                            for column_config in &table_config.column_configs {
                                let mut need_add = true;
                                let column_name = get_real_column_name(column_config);
                                // alter stable column not supported by taosd
                                let desc = taos.describe(&stable_name).await?;
                                desc.into_iter().for_each(|column_meta| {
                                    if column_name == column_meta.field() {
                                        need_add = false;
                                    }
                                });
                                if need_add {
                                    let add_column_sql = format!(
                                        "alter table `{stable_name}` ADD COLUMN {} {}",
                                        get_real_column_name(column_config),
                                        column_config.column_type.unwrap()
                                    );
                                    log::info!("add_column_sql:{}", add_column_sql);
                                    taos.exec(&add_column_sql).await?;
                                }
                            }
                        } else if errstr.contains("[0x2653]") {
                            // column or tag length not enough
                            let desc = taos.describe(&stable_name.as_str()).await?;
                            desc.into_iter().for_each(|column_meta| {
                                let column_type;
                                let length;
                                if column_meta.field() == "point_id"
                                    && id.len() > column_meta.length()
                                {
                                    column_type = "tag";
                                    length = id.len();
                                } else if column_meta.field() == "point_name"
                                    && point_name.len() > column_meta.length()
                                {
                                    column_type = "tag";
                                    length = point_name.len();
                                } else if (column_meta.ty == Ty::VarChar
                                    || column_meta.ty == Ty::NChar)
                                    && column_meta.field() == value_cloumn_name
                                    && value_cloumn_length > column_meta.length()
                                {
                                    column_type = "column";
                                    length = value_cloumn_length;
                                } else {
                                    return;
                                }
                                let sql = format!(
                                    "alter table `{stable_name}` modify {column_type} `{}` {}({})",
                                    column_meta.field(),
                                    column_meta.ty(),
                                    length,
                                );
                                log::info!("add execute sql: {}", &sql);
                                taos.exec_sync(sql).unwrap();
                            });
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(points)
}

#[inline]
fn get_real_column_name(column_config: &ColumnConfig) -> &String {
    &column_config
        .column_alias
        .as_ref()
        .unwrap_or(&column_config.column_name)
}

async fn consume_flat_record(
    _taos: &Taos,
    record: &FlatMessage,
    _count: &mut usize,
    parser: Option<&Parser>,
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    if let Some((_license, transferred)) = license.zip(transferred) {
        let _used = transferred.records.load(Ordering::SeqCst);
        /*if used > license.number as _ {
            anyhow::bail!(
                "Connector {} out of number: {}/{}",
                license.r#type,
                used,
                license.number
            );
        }*/
    }
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
                        if records.records.num_rows() == 0 {
                            continue;
                        }
                        // dbg!(&records);
                        let views = taosx_ipc::stream::reader::record_batch_to_column_view(
                            &records.records,
                        );
                        // dbg!(&views);
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
                                                    if let Some(transferred) = transferred {
                                                        transferred
                                                            .stables
                                                            .fetch_add(1, Ordering::SeqCst);
                                                    }
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
                                                            } else if err.to_string().contains("[0x260D]") {
                                                                // Tags number not matched
                                                                // add Tag
                                                                let table = records
                                                                    .table
                                                                    .using
                                                                    .as_deref()
                                                                    .unwrap();
                                                                let tags = records.tag_meta().unwrap();
                                                                for tag_meta in tags {
                                                                    let mut need_add = true;
                                                                    let res = _taos.describe(table).await.unwrap();
                                                                    res.into_iter().for_each(|tag_added| {
                                                                        if tag_added.is_tag() && tag_added.field() == tag_meta.field() {
                                                                            need_add = false;
                                                                        }
                                                                    });
                                                                    if need_add {
                                                                        let add_tag_sql = format!(
                                                                            "alter table `{table}` add tag `{}` {}",
                                                                            tag_meta.field(),
                                                                            parser.get_ipcdatatype_from_parser(tag_meta.field()).unwrap().sql_repr()
                                                                            );
                                                                        log::info!("table {table} add tag sql: {add_tag_sql}");
                                                                        _taos.exec(add_tag_sql).await.unwrap();
                                                                    }
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
                                                    // dbg!(&sql);
                                                    if let Some(transferred) = transferred {
                                                        transferred
                                                            .tables
                                                            .fetch_add(1, Ordering::SeqCst);
                                                    }
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
                                            if let Some(transferred) = transferred {
                                                transferred.tables.fetch_add(1, Ordering::SeqCst);
                                            }
                                            break;
                                        }
                                        //.inspect_err(|err| tracing::warn!("{}", err))?
                                    } else {
                                        let sql = records.table_sql();
                                        if let Some(transferred) = transferred {
                                            transferred.tables.fetch_add(1, Ordering::SeqCst);
                                        }
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
                                } else if err_str.contains("[0x0118]") {
                                    // Code([0x0118] Unknown or common error)
                                    // column or tag not exists
                                    let mut index = 0;
                                    while index < columns.len() {
                                        // let column_view = views.get(index).unwrap();
                                        let column_name = columns.get(index).unwrap().as_str();
                                        let desc = _taos.describe(table_name).await?;
                                        let mut need_add = true;
                                        desc.into_iter().for_each(|column_meta| {
                                            if column_meta.field() == column_name {
                                                need_add = false;
                                            }
                                        });
                                        if need_add {
                                            let ipc_data_type = parser.get_ipcdatatype_from_parser(column_name);
                                            if ipc_data_type.is_none() {
                                                log::warn!("column name {column_name} not config in parser");
                                                break;
                                            }
                                            let sql = format!(
                                                "alter table `{}` add column `{}` {}",
                                                records.table.using.as_ref().unwrap_or(&table_name.to_string()),
                                                &column_name,
                                                ipc_data_type.unwrap(),
                                            );
                                            log::info!("alter table column sql: {}", sql);
                                            _taos.exec(&sql).await.unwrap();
                                        }
                                        index += 1;
                                    }
                                } else {
                                    Err(err)?;
                                    break;
                                }
                                continue;
                            } else {
                                if let Some(transferred) = transferred {
                                    transferred
                                        .records
                                        .fetch_add(raw.nrows() as _, Ordering::SeqCst);
                                    transferred.points.fetch_add(
                                        (raw.nrows() * raw.ncols()) as _,
                                        Ordering::SeqCst,
                                    );
                                }
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
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    let columns = ipc_reader
        .columns()
        .into_iter()
        .map(|s| format!("{s}"))
        .collect_vec();
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
            consume_lush_record(
                &taos,
                &mut stmt,
                record,
                &columns,
                &names,
                &marks,
                &mut count,
                license,
                transferred,
            )
            .await?;
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
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    let mut count = 0;
    let mut stmt = Stmt::init(taos)?;
    for record in ipc_reader {
        if let Ok(record) = record {
            if let Some((_license, transferred)) = license.zip(transferred) {
                let _used = transferred.points.load(Ordering::SeqCst);
                // if used > license.number as _ {
                //     anyhow::bail!(
                //         "Connector {} out of points: {}/{}",
                //         license.r#type,
                //         used,
                //         license.number
                //     )
                // }
            }
            let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            let n = consume_point_record(
                taos,
                &mut stmt,
                &record,
                &mut count,
                config.as_ref().unwrap(),
            )
            .await?;

            ipc_ack_writer.write_ok()?;

            if let Some(transferred) = transferred {
                transferred.points.fetch_add(n as _, Ordering::SeqCst);
            }
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
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    let mut count = 0;
    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_flat_record(&taos, &record, &mut count, parser, license, transferred).await?;

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
    connector: Option<&str>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    let taos = pool.get().await?;

    let license: Option<ConnectorLicense> = if let Some(connector) = connector {
        #[cfg(feature = "disable-enterprise-connector-validation")]
        let license: Option<ConnectorLicense> = None;
        #[cfg(not(feature = "disable-enterprise-connector-validation"))]
        let license: Option<ConnectorLicense> = taos
            .query_one::<_, String>(&format!(
                "select {connector} from information_schema.ins_grants"
            ))
            .await
            .unwrap_or(None)
            .and_then(|s| serde_json::from_str(&s).ok());

        if let Some(license) = license {
            if license.is_expired() {
                anyhow::bail!(
                    "Connector {connector} expired, please contact the database administrator for license",
                )
            } else {
                None
                // if license.number == -1 {
                //     None
                // } else {
                //     Some(license)
                // }
            }
        } else {
            None
        }
    } else {
        None
    };

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
            ipc_flat_stream_reader(
                &taos,
                ipc_reader,
                ipc_ack_writer,
                parser.as_ref(),
                license.as_ref(),
                transferred.as_deref(),
            )
            .await?
        }
        StreamType::Lush => {
            ipc_lush_stream_reader(
                &taos,
                ipc_reader,
                ipc_ack_writer,
                license.as_ref(),
                transferred.as_deref(),
            )
            .await?
        }
        StreamType::Point => {
            ipc_point_reader(
                &taos,
                ipc_reader,
                ipc_ack_writer,
                config,
                license.as_ref(),
                transferred.as_deref(),
            )
            .await?
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub struct IpcStreamWorker<'a> {
    taos: &'a Taos,
    parser: IpcParser,
    lock: Arc<Mutex<()>>,
    task: Option<i64>,
    from: Dsn,
    config: Option<OPCConfig>,
    opc_table_config: OnceCell<OpcTableConfig>,
    license: Option<&'a ConnectorLicense>,
    transferred: Option<&'a Transferred>,
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
        license: Option<&'a ConnectorLicense>,
        transferred: Option<&'a Transferred>,
        // license: Option<>
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
            license,
            transferred, // stmt: Arc::new(UnsafeCell::new(stmt)),
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
        parser: Option<&Parser>,
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
                consume_flat_record(
                    &self.taos,
                    &record,
                    &mut count,
                    parser, // todo: license
                    self.license,
                    self.transferred,
                )
                .await?;
                Ok(count)
            }
            StreamType::Lush => {
                let columns = self
                    .parser
                    .columns()
                    .into_iter()
                    .map(|s| format!("{s}"))
                    .collect_vec();
                let names = columns.iter().map(|n| format!("`{n}`")).join(",");
                let marks = std::iter::repeat('?').take(columns.len()).join(",");

                let message = self.parser.parse(record)?;
                let mut count = 0;

                let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .map_err(|_| anyhow::format_err!("Unable to read lush message"))?;
                // let stmt = unsafe { &mut *self.stmt.get() };
                consume_lush_record(
                    &self.taos,
                    stmt,
                    record,
                    &columns,
                    &names,
                    &marks,
                    &mut count,
                    self.license,
                    self.transferred,
                )
                .await?;
                Ok(count)
            }
            StreamType::Point => {
                if let Some((_license, transferred)) = self.license.zip(self.transferred) {
                    let _used = transferred.points.load(Ordering::SeqCst);
                    // if used > license.number as _ {
                    //     anyhow::bail!(
                    //         "Connector {} out of points: {}/{}",
                    //         license.r#type,
                    //         used,
                    //         license.number
                    //     )
                    // }
                }
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
                        let _v = config.parse_tables_with(&self.taos).await?;
                        self.opc_table_config
                            .get_or_try_init(|| async {
                                config.parse_tables_with(&self.taos).await
                            })
                            .await?
                    }
                };
                drop(guard);
                let _n =
                    consume_point_record(&self.taos, stmt, &record, &mut count, config).await?;
                if let Some(transferred) = self.transferred {
                    transferred.points.fetch_add(_n as _, Ordering::SeqCst);
                }
                // todo: license
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

// #[cfg(unix)]
// pub fn listen_unix_socket(
//     target: TaosPool,
//     socket: impl AsRef<Path>,
//     config: Option<OpcTableConfig>,
// ) -> anyhow::Result<()> {
//     let path = socket.as_ref();
//     if path.exists() {
//         std::fs::remove_file(path).unwrap();
//     }
//     let runtime = tokio::runtime::Builder::new_multi_thread()
//         .enable_all()
//         .worker_threads(16)
//         .build()?;
//     let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
//     let sql_lock = Arc::new(Mutex::new(()));
//     info!("listen on socket address: {}", path.display());
//     loop {
//         match listener.accept() {
//             Ok((stream, addr)) => {
//                 tracing::info!("new unix client!: {:?}", addr);
//                 let pool = target.clone();
//                 let lock = sql_lock.clone();
//                 let config = config.clone();
//                 runtime.spawn(async move {
//                     ipc_unix_read(
//                         addr.as_pathname()
//                             .map(|path| path.display().to_string())
//                             .unwrap_or_default(),
//                         pool,
//                         stream,
//                         lock,
//                         config,
//                     )
//                     .await
//                 });
//             }
//             Err(e) => {
//                 /* connection failed */
//                 tracing::debug!("IPC stream acceptation error {e}, might be stopped");
//             }
//         }
//     }
// }

pub fn listen_tcp_socket_with_agent(
    socket: impl AsRef<str>,
    sender: Sender<String>,
    _config: Option<OpcTableConfig>,
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
    // let sql_lock = Arc::new(Mutex::new(()));
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
                    // let client = addr.as_socket_ipv4().unwrap().to_string();
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
                            tokio::time::sleep(Duration::from_millis(100)).await;
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
    connector: Option<&'static str>,
    transferred: Option<Arc<Transferred>>,
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
                        let connector = connector.clone();
                        let transferred = transferred.clone();
                        runtime.spawn(async move {
                            let res = ipc_tcp_read(
                                client,
                                pool,
                                stream,
                                lock,
                                config,
                                cancel,
                                parser,
                                connector,
                                transferred,
                            )
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
