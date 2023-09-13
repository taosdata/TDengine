use anyhow::{bail, Context};
use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions, record_batch::RecordBatch};
use arrow_flight::FlightClient;
use async_backtrace::framed;
use bytes::Bytes;
use futures::TryStreamExt;
use serde_json::json;
use std::{
    any::Any,
    collections::HashMap,
    io::{Read, Write},
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use taos::{
    taos_query::{common::Describe, Manager},
    AsyncBindable, AsyncFetchable, AsyncQueryable, Dsn, Itertools, RawBlock, Stmt, Taos, TaosPool,
    Ty, Value,
};
use tokio::sync::{Mutex, Notify, OnceCell};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, instrument, Instrument, Span};

use crate::{ConnectorLicense, OPCConfig, Parser, Transferred};

use super::runners::opc::{opc_config_blocking, ColumnConfig, OpcTableConfig};
use super::*;
use metrics::*;
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

#[instrument(skip(stream, cancel, token))]
async fn ipc_tcp_forward(
    client: String,
    stream: std::net::TcpStream, // socket2::Socket,
    cancel: CancellationToken,
    remote: String, // "http://127.0.0.1:6051"
    token: String,
    task_id: i64,
) -> anyhow::Result<()> {
    use md5;
    tracing::info!("token: {}", format!("{:x}", md5::compute(token.clone())));

    let _ = cancel;
    use arrow_flight::{encode::FlightDataEncoderBuilder, error::FlightError};
    use futures::StreamExt;
    let reader_stream = stream
        .try_clone()
        .context("Try clone IPC stream as reader error")?;
    let ipc_reader = tokio::task::spawn_blocking(move || IpcReader::new(reader_stream))
        .await?
        .context("Build IPC stream reader error")?;
    let ack = ipc_reader.ack();
    let mut ipc_ack_writer =
        tokio::task::spawn_blocking(move || AckWriterBuilder::new(ack).open(stream)).await?;

    let schema = ipc_reader.schema.clone();
    // dbg!(&schema);
    // let (sender, receiver) = flume::bounded(5);

    info!(client, remote, "reading batches");
    // tokio::spawn(async move {
    //     let mut batches = ipc_reader.into_raw_stream();
    //     while let Some(res) = batches.next().await {
    //         dbg!(&res);
    //         if sender
    //             .send_async(res.map_err(FlightError::from))
    //             .await
    //             .is_err()
    //         {
    //             tracing::info!("IPC remote handler has been closed");
    //             break;
    //         }
    //     }
    //     tracing::info!("[task:{task_id}] stopped");
    // });
    // tokio::task::yield_now().await;

    let ipc_stream = ipc_reader.into_raw_stream();

    // let max_retries_in_one_minutes = 3;
    // let last_retry_time = Arc::new(AtomicUsize::new(0));
    'start: loop {
        let data_stream = ipc_stream.clone();
        let data = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_options(
                IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
            )
            .build(data_stream.map_err(FlightError::from));

        const MAX_RETRIES: usize = 3;
        const RETRY_DELAY: Duration = Duration::from_secs(5);

        let mut retries = 0;
        let channel = loop {
            match try_establish_channel(remote.clone()).await {
                Ok(channel) => break channel,
                Err(err) => {
                    retries += 1;
                    tracing::error!("Failed to establish connection: {}. Retrying...", err);
                    if retries >= MAX_RETRIES {
                        tracing::error!("Max retries reached. Exiting...");
                        return Err(err);
                    }
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        };
        let mut client = FlightClient::new(channel);
        let _ = client
            .handshake(Bytes::from(token.as_bytes().to_vec()))
            .await
            .map_err(|err| match err {
                FlightError::Tonic(status) => anyhow::anyhow!("{}", status.message()),
                err => anyhow::anyhow!("Handshake error: {err:#}"),
            })?;
        info!("Handshake done");
        // dbg!(res);
        client.add_header("x-task-id", &task_id.to_string())?;
        client.add_header("x-token", &token)?;
        info!("Do putting");
        let mut stream = client.do_put(data).await.map_err(|err| match dbg!(err) {
            FlightError::Arrow(err) => anyhow::anyhow!("IPC Arrow error: {err:#}"),
            FlightError::Tonic(status) => anyhow::anyhow!("{}", status.message()),
            err => anyhow::anyhow!("Put IPC stream error: {err:#}"),
        })?;
        info!("Get putting stream response");

        while let Some(res) = stream.next().await {
            let rsp = res;
            match rsp {
                Ok(rsp) => {
                    tracing::debug!("Response ok: {:?}", rsp);
                }
                Err(err) => match &err {
                    FlightError::Tonic(status) => {
                        if status
                            .message()
                            .contains("stream closed because of a broken pipe")
                        {
                            tracing::warn!("Disconnected, retry after one second: {err:#}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue 'start;
                        }
                        tracing::error!("Tonic error: {status}");
                        Err(err).context("Got server response with error")?;
                    }
                    _ => {
                        tracing::error!("Other error: {err:#}");
                        Err(err).context("Got server response with error")?;
                    }
                },
            }
            let _ = ipc_ack_writer.write_ok();
        }

        info!("[{task_id}] Putting stream finished");
        break;
    }
    Ok(())
}

async fn try_establish_channel(remote: String) -> anyhow::Result<Channel> {
    let endpoint = tonic::transport::Endpoint::try_from(remote)?
        .keep_alive_timeout(Duration::from_secs(30))
        .http2_keep_alive_interval(Duration::from_secs(13));
    let channel = endpoint.connect().await?;
    Ok(channel)
}

#[framed]
#[instrument(skip_all)]
async fn ipc_tcp_read(
    client: String,
    pool: TaosPool,
    stream: std::net::TcpStream, //socket2::Socket,
    lock: Arc<Mutex<()>>,
    config: Option<OpcTableConfig>,
    _cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    // let stream = Arc::new(stream);
    // let reader = stream.clone();

    info!(client, "Prepare IPC stream reader");
    let reader_stream = stream.try_clone().context("Clone tcp stream error")?;
    let ipc_reader = tokio::task::spawn_blocking(move || {
        IpcReader::new(reader_stream).context("IPC reading error")
    })
    .await??;
    info!(client, "Prepare IPC ACK writer");
    // dbg!(ipc_reader.ack());
    let ack = ipc_reader.ack();
    let ipc_ack_writer =
        tokio::task::spawn_blocking(move || AckWriterBuilder::new(ack).open(stream)).await?;
    // ipc_ack_writer.ack(LushAck);
    let client = client.to_string();
    info!(client, "Processing IPC stream");
    ipc_process(
        client,
        pool,
        ipc_reader,
        ipc_ack_writer,
        lock,
        config,
        parser,
        connector,
        transferred,
    )
    .await?;
    tracing::info!("IPC stream processed");
    Ok(())
    // tokio::select! {
    // _ = cancel.cancelled() => {
    //     tracing::debug!("cancel IPC worker");
    //     Ok(())
    // },
    // done = ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config, parser, connector, transferred) => {
    //     tracing::info!("IPC stopped");
    //     done
    // }
    // }
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

struct LushMessageTagModify {
    sqls: Vec<(String, bool)>,
    tags: Vec<(String, Value)>,
}

// #[instrument(skip(taos, record, names, marks))]
#[instrument(skip_all)]
async fn consume_lush_record(
    pool: &TaosPool,
    taos: &mut Option<deadpool::managed::Object<Manager<TaosBuilder>>>,
    stmt: &mut Stmt,
    record: LushMessage,
    columns: &Vec<String>,
    names: &str,
    marks: &str,
    records: &mut usize,
    _license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    counter!(RECORD_BATCHES, 1);
    match record {
        LushMessage::Tables(tables) => {
            let taos = taos.as_ref().unwrap();
            // let mut sql = format!("CREATE TABLE ");
            // map: <stable_name, (Vec<sql, sql_overflow?>, Vec<tag_name, tag_value>)>
            let mut create_sql_map: HashMap<String, LushMessageTagModify> = HashMap::new();
            for table in tables {
                let table_name = table.table_name();
                let tags = table.tags();
                if tags.is_none() {
                    continue;
                }
                let tags = tags.clone().unwrap();
                let mut query_tags_sql = format!("SELECT ");
                for (tagname, _) in &tags {
                    query_tags_sql.push_str(format!("`{tagname}`,").as_str());
                }
                query_tags_sql.pop();
                query_tags_sql.push_str(format!(" from `{table_name}`").as_str());
                match taos.query(query_tags_sql).await {
                    Ok(mut rs) => {
                        let mut rows = rs.rows();
                        while let Some(mut row) = rows.try_next().await? {
                            let next = row.next().unwrap();
                            for (tagname, tagvalue) in &tags {
                                if tagname == next.0
                                    && tagvalue.to_sql_value() != next.1.to_sql_value()
                                {
                                    tracing::info!(
                                        "table {table_name} tag value not match, new: {}, old:{}",
                                        tagvalue.to_sql_value(),
                                        next.1.to_sql_value()
                                    );
                                    let alter_set_sql = format!(
                                        "alter table `{table_name}` set TAG `{tagname}`={}",
                                        tagvalue.to_sql_value()
                                    );
                                    tracing::info!("alter_set_sql: {alter_set_sql}");
                                    taos.exec(alter_set_sql).await?;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        tracing::trace!("query_tags_sql err: {}", err.to_string());
                        if err.to_string().contains("0x2603") || err.to_string().contains("0x2662")
                        {
                            // table not exists
                            let table_sql = table.to_sql(None);
                            if table_sql.is_some() {
                                let stable_name = table.stable_name().clone().unwrap();
                                let table_sql = table_sql.unwrap();
                                let sql_vec = create_sql_map.get_mut(&stable_name);
                                let mut insert_done = false;
                                if sql_vec.is_some() {
                                    let tag_modify = sql_vec.unwrap();
                                    for index in 0..tag_modify.sqls.len() {
                                        let (create_sql, overflow) =
                                            tag_modify.sqls.get_mut(index).unwrap();
                                        if *overflow {
                                            continue;
                                        } else {
                                            let sql_suffix = table_sql.replace("CREATE TABLE ", "");
                                            if create_sql.len() + sql_suffix.len() > 1000 * 1000 {
                                                *overflow = true;
                                                continue;
                                            } else {
                                                create_sql.push_str(sql_suffix.as_str());
                                                insert_done = true;
                                            }
                                        }
                                    }
                                    if !insert_done {
                                        // init sql shouldn't overflow
                                        // counter!(CHILD_TABLE_CREATED, 1);
                                        tag_modify.sqls.push((table_sql, false));
                                    }
                                } else {
                                    let mut sql_vec = Vec::new();
                                    sql_vec.push((table_sql, false));
                                    let tag_modify_message = LushMessageTagModify {
                                        sqls: sql_vec,
                                        tags: table.tags().clone().unwrap(),
                                    };
                                    // counter!(CHILD_TABLE_CREATED, 1);
                                    create_sql_map.insert(stable_name.clone(), tag_modify_message);
                                }
                            }
                        }
                    }
                }

                if let Some(transferred) = transferred {
                    transferred.tables.fetch_add(1, Ordering::SeqCst);
                }
            }

            for (stable_name, message_modify) in create_sql_map {
                for sql in message_modify.sqls {
                    info!("Tables: {}", sql.0);
                    match taos.exec(&sql.0).await {
                        Ok(_) => (),
                        Err(err) => {
                            let err_str = err.to_string();
                            tracing::warn!("create table error: {err:#}");
                            if err_str.contains("0x2653") {
                                // column or tag length not enough
                                let desc = taos.describe(&stable_name.as_str()).await?;
                                let fields = message_modify
                                    .tags
                                    .iter()
                                    .filter(|(_, value)| {
                                        matches!(value, Value::VarChar(_))
                                            || matches!(value, Value::NChar(_))
                                    })
                                    .map(|(tag_name, value)| match value {
                                        Value::VarChar(v) => {
                                            (tag_name.clone(), IpcDataType::VarChar(v.len() as u32))
                                        }
                                        Value::NChar(v) => {
                                            (tag_name.clone(), IpcDataType::NChar(v.len() as u32))
                                        }
                                        _ => unimplemented!(),
                                    })
                                    .collect_vec();
                                let alter_sqls = generate_alter_sql_diff_desc(
                                    &stable_name,
                                    &desc,
                                    &fields,
                                    true,
                                );
                                if alter_sqls.is_some() {
                                    for alter_sql in alter_sqls.unwrap() {
                                        info!("lush table alter sql: {alter_sql}");
                                        taos.exec(alter_sql).await?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        LushMessage::Insert(record) => {
            // let guard = mutex.lock().await;
            for record in record {
                if record.num_rows() == 0 {
                    continue;
                }
                counter!(BATCH_RECORDS, record.num_rows() as u64);
                *records += record.num_rows();
                let data = record.to_column_views();
                // RawBlock
                // taos.write_raw_block()
                let sqls = record.generate_insert_sql_from_tablename(&data, columns);
                if let Some((sqls, field_map)) = sqls {
                    for sql in sqls {
                        tracing::debug!("insert sql: {sql}");
                        let mut retry = 0;
                        let mut count = 0;
                        loop {
                            let res = taos.as_ref().unwrap().exec(sql.clone()).await;
                            match res {
                                Ok(num) => {
                                    count = count + num;
                                    counter!(INSERT_SQLS, 1);
                                    counter!(RECORDS, num as u64);
                                    break;
                                }
                                Err(err) => {
                                    if retry > 2 {
                                        tracing::warn!("retry 3 faild continue: {err:#}");
                                        counter!(INSERT_SQL_FAILS, 1);
                                        break;
                                    }
                                    tracing::error!("written err cause: {err:#}");
                                    let errstr = err.to_string();
                                    if errstr.contains("[0x2653]") {
                                        // column or tag length not enough
                                        let fields = Vec::from_iter(field_map.clone());
                                        // get stable name
                                        let stable_name = record.stable_name();
                                        if stable_name.is_none() {
                                            tracing::error!("record should contains init message for stable name");
                                            break;
                                        }
                                        let stable_name = stable_name.unwrap();
                                        let desc =
                                            taos.as_ref().unwrap().describe(&stable_name).await?;
                                        let alter_sqls = generate_alter_sql_diff_desc(
                                            &stable_name,
                                            &desc,
                                            &fields.clone(),
                                            false,
                                        );
                                        if alter_sqls.is_some() {
                                            let alter_sqls = alter_sqls.unwrap();
                                            for alter_sql in alter_sqls {
                                                tracing::info!("alter sql: {alter_sql}");
                                                if let Err(err) =
                                                    taos.as_ref().unwrap().exec(alter_sql).await
                                                {
                                                    tracing::info!("alter sql error: {err:#}");
                                                }
                                            }
                                        }
                                    } else if errstr.contains("[0x0E") {
                                        taos.replace(pool.get().await?);
                                    }
                                    retry += 1;
                                }
                            }
                        }
                        info!("written [{count}] records");
                    }
                } else {
                    let sql = format!("insert into ? ({names}) values({marks})");
                    info!("prepare with sql: {sql}");
                    stmt.prepare(&sql).await?;
                    info!("prepare");
                    stmt.bind(data.as_slice()).await?;
                    stmt.add_batch().await?;
                    let n = stmt.execute().await?;
                    info!("written : [{n}] records");
                    if let Some(transferred) = transferred {
                        transferred.records.fetch_add(n as _, Ordering::SeqCst);
                        transferred
                            .points
                            .fetch_add((n * data.len()) as _, Ordering::SeqCst);
                    }
                }
            }
            // drop(guard);
        }
    }
    info!("consume lush record done");
    Ok(())
}

struct ModifyStructForPointMessage {
    id: String,
    point_name: String,
    value_cloumn_name: String,
    value_cloumn_length: usize,
}

#[instrument(skip_all)]
async fn consume_point_record(
    taos: &Taos,
    _: &mut Stmt,
    record: &PointMessage,
    count: &mut usize,
    config: &OpcTableConfig,
) -> anyhow::Result<usize> {
    let mut points = 0;
    metrics::counter!(RECORD_BATCHES, 1);
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

        let stable_prefix = table_config.stable_prefix.clone();
        let stable_name = if stable_prefix.is_some() {
            let mut stable_prefix = stable_prefix.unwrap();
            if value_type.contains("varchar") {
                stable_prefix.push_str("_varchar");
                Some(stable_prefix)
            } else if value_type.contains("nchar") {
                stable_prefix.push_str("_nchar");
                Some(stable_prefix)
            } else {
                stable_prefix.push_str(format!("_{value_type}").as_str());
                Some(stable_prefix)
            }
        } else {
            None
        };
        let mut columns = String::new();
        let mut columns_insert: Vec<(String, String)> = Vec::new(); // first is primary key info, its type should be timestamp
        let mut value_column = None;
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
                    format!("`{prinmary_key_column_alias}` TIMESTAMP,").as_str(),
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
                if column_config.column_name == "value" {
                    value_column = Some(column_config.clone());
                } else {
                    columns.push_str(
                        format!("`{prinmary_key_column_alias}` {},", column_type).as_str(),
                    );
                }
            }
        }
        // remove last char
        columns.pop();
        let mut tags = "`point_id` VARCHAR(256), `point_name` VARCHAR(256)".to_string();
        if table_config.tag_configs.is_some() {
            let tag_configs = table_config.tag_configs.clone().unwrap();
            for tag in tag_configs {
                tags.push_str(
                    format!(" ,`{}` {}", tag.column_name, tag.column_type.sql_repr()).as_str(),
                );
            }
        }
        // stable, Vec<insert_sql, sql length overflow?, value_column_type>
        let mut stable_insert_map: HashMap<
            String,
            Vec<(String, bool, String, ModifyStructForPointMessage)>,
        > = HashMap::new();
        let mut child_table_create_sql_map = HashMap::new();
        for i in 0..id_cv.len() {
            metrics::counter!(BATCH_RECORDS, 1);
            let id = id_cv.get(i).unwrap().into_value().to_string().unwrap();
            let code = id_code_map.get(&id);
            if code.is_none() {
                tracing::warn!("id: {} cannot get code", id);
                continue;
            }
            let point_config = code.unwrap();
            let stable_name = if stable_name.is_some() {
                stable_name.as_ref().unwrap()
            } else if point_config.stable.is_some() {
                point_config.stable.as_ref().unwrap()
            } else {
                anyhow::bail!("id: {id} failded to get stable");
            };
            let child_table_name = if point_config.stable.is_some() {
                format!("{}", point_config.code)
            } else {
                format!("{stable_name}_{}", point_config.code)
            };
            // child_table_name.push_str(format!("_{}", point_config.code).as_str());
            // let mut insert_sql = format!("insert into `{child_table_name}` ");
            let mut values = String::new();
            let mut value_cloumn_name = "value";
            let mut value_cloumn_length = 128;
            let mut columns_in_insert = String::new();
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
                } else if temp_name == "quality" {
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
                columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
            }
            values.pop();
            columns_in_insert.pop();
            let point_name = name_cv
                .slice(i..i + 1)
                .unwrap()
                .get(0)
                .unwrap()
                .to_sql_value();
            let mut tag_names = String::new();
            let mut tag_values = String::new();
            if table_config.tag_configs.is_some() {
                // let mut index = 0;
                for ele in table_config.tag_configs.as_ref().unwrap() {
                    let tag_name = ele.column_name.clone();
                    tag_names.push_str(format!("`{}`,", tag_name).as_str());
                    let value = point_config
                        .tag_values
                        .as_ref()
                        .unwrap()
                        .get(&tag_name)
                        .unwrap();
                    let value = match ele.column_type {
                        IpcDataType::VarChar(_) | IpcDataType::NChar(_) | IpcDataType::Json => {
                            format!("\"{value}\"")
                        }
                        _ => value.to_string(),
                    };
                    tag_values.push_str(format!("{value},",).as_str());
                    // index += 1;
                }
                tag_names.pop();
                tag_values.pop();
            }
            if tag_names.is_empty() {
                child_table_create_sql_map.insert(
                    child_table_name.clone(),
                    format!(
                        "(`point_id`, `point_name`) TAGS (\"{id}\", {})",
                        &point_name
                    ),
                );
            } else {
                child_table_create_sql_map.insert(
                    child_table_name.clone(),
                    format!(
                        "(`point_id`, `point_name`, {tag_names}) TAGS (\"{id}\", {}, {tag_values})",
                        &point_name
                    ),
                );
            }
            let sql_vec = stable_insert_map.get_mut(stable_name);
            let mut insert_done = false;
            if sql_vec.is_some() {
                let sql_vec = sql_vec.unwrap();
                for index in 0..sql_vec.len() {
                    let (insert_sql, overflow, _, _) = sql_vec.get_mut(index).unwrap();
                    if *overflow {
                        continue;
                    } else {
                        let sql_suffix = format!(
                            " `{child_table_name}` ({}) VALUES ({}) ",
                            columns_in_insert.as_str(),
                            values
                        );
                        if insert_sql.len() + sql_suffix.len() > 1000 * 1000 {
                            *overflow = true;
                            continue;
                        } else {
                            insert_sql.push_str(sql_suffix.as_str());
                            insert_done = true;
                        }
                    }
                }
                if !insert_done {
                    // let insert_sql = ;
                    let value_column_type = if point_config.value_type.is_some() {
                        // maybe should replace value column type
                        point_config.value_type.clone().unwrap().sql_repr()
                    } else {
                        value_type.clone()
                    };
                    sql_vec.push((
                        format!(
                            "insert into `{child_table_name}` ({}) VALUES ({})",
                            columns_in_insert.as_str(),
                            values
                        ),
                        false,
                        value_column_type,
                        ModifyStructForPointMessage {
                            id,
                            point_name,
                            value_cloumn_name: value_cloumn_name.to_string(),
                            value_cloumn_length,
                        },
                    ));
                }
            } else {
                let insert_sql = format!(
                    "insert into `{child_table_name}` ({}) VALUES ({})",
                    columns_in_insert.as_str(),
                    values
                );
                let value_column_type = if point_config.value_type.is_some() {
                    // maybe should replace value column type
                    point_config.value_type.clone().unwrap().sql_repr()
                } else {
                    value_type.clone()
                };
                let mut sql_vec = Vec::new();
                sql_vec.push((
                    insert_sql,
                    false,
                    value_column_type,
                    ModifyStructForPointMessage {
                        id,
                        point_name,
                        value_cloumn_name: value_cloumn_name.to_string(),
                        value_cloumn_length,
                    },
                ));
                stable_insert_map.insert(stable_name.clone(), sql_vec);
            }

            // insert_sql.push_str(tag_sql.as_str());
            // insert_sql.push_str(format!(" VALUES ({})", values).as_str());
        }
        for (stable_name, sql_vec) in stable_insert_map {
            for (insert_sql, _, value_column_type, modify_message) in sql_vec {
                debug!("point message insert sql len: {}", insert_sql.len());
                let mut retry = 0;
                'outer: loop {
                    if retry >= 3 {
                        tracing::warn!("sql error cannot be solved, break;");
                        counter!(INSERT_SQL_FAILS, 1);
                        break;
                    }
                    let sql_res = taos.exec(&insert_sql).await;
                    match sql_res {
                        Ok(n) => {
                            *count += n;
                            counter!(INSERT_SQLS, 1);
                            counter!(RECORDS, n as u64);
                            counter!(POINTS, n as u64 * columns_insert.len() as u64);
                            points += n;
                            break;
                        }
                        Err(err) => {
                            let errstr = err.to_string();
                            tracing::warn!("error: {}", errstr);
                            if errstr.contains("[0x2603]") || errstr.contains("0x0200") {
                                // stable not exists
                                // should be some
                                let value_column_config = value_column.as_ref().unwrap();
                                let primary_key_column_name =
                                    value_column_config.column_name.clone();
                                let prinmary_key_column_alias = value_column_config
                                    .column_alias
                                    .clone()
                                    .unwrap_or(primary_key_column_name.clone());
                                let mut temp_conlumns = columns.clone();
                                temp_conlumns.push_str(
                                    format!(",`{prinmary_key_column_alias}` {value_column_type}")
                                        .as_str(),
                                );
                                let stable_sql = format!(
                                    "create stable if not exists `{}` ({}) tags ({})",
                                    stable_name, temp_conlumns, tags
                                );
                                tracing::info!("create stable sql: {}", &stable_sql);
                                match taos.exec(&stable_sql).await {
                                    Ok(_n) => (), //counter!(STABLE_CREATED, n as u64),
                                    Err(err) => {
                                        if err.to_string().contains("0x032C") {
                                            // Object is creating, maybe should ignore
                                            tracing::warn!("create stable sql encounter 0x032C");
                                        } else {
                                            tracing::error!("create stable sql error: {err:#}");
                                        }
                                    }
                                }
                                // batch create child table
                                let mut child_table_create_sqls = Vec::new();
                                let mut sql_prefix = "create table".to_string();
                                for (child_table_name, child_table_create_sql) in
                                    &child_table_create_sql_map
                                {
                                    let suffix_sql = format!(" IF NOT EXISTS `{child_table_name}` USING `{stable_name}` {child_table_create_sql}");
                                    if sql_prefix.len() + suffix_sql.len() > 1024 * 1024 {
                                        child_table_create_sqls.push(sql_prefix);
                                        sql_prefix = "create table".to_string();
                                    }
                                    sql_prefix.push_str(&suffix_sql);
                                }
                                child_table_create_sqls.push(sql_prefix);
                                for create_child_sql in child_table_create_sqls {
                                    tracing::info!("create child sql: {create_child_sql}");
                                    match taos.exec(&create_child_sql).await {
                                        Ok(_n) => (), // counter!(CHILD_TABLE_CREATED, n as u64),
                                        Err(err) => {
                                            if err.to_string().contains("0x032C") {
                                                // Object is creating, maybe should ignore
                                                tracing::warn!("create table sql encounter 0x032C");
                                            } else {
                                                tracing::error!("create table sql error: {err:#}");
                                            }
                                        }
                                    }
                                }
                            } else if errstr.contains("[0x2602]") || errstr.contains("[0x263F]") {
                                // Illegal number of columns or tags, alter to add columns or tag
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
                                        let column_real_name = get_real_column_name(column_config);
                                        if column_config.column_type.is_none() {
                                            // shouldn't happen if normal
                                            // encounter when rename value column
                                            tracing::error!("column {} column_type is error, maybe stable set error", column_real_name);
                                            break 'outer;
                                        }
                                        let add_column_sql = format!(
                                            "alter table `{stable_name}` ADD COLUMN {} {}",
                                            column_real_name,
                                            column_config.column_type.unwrap()
                                        );
                                        tracing::info!("add_column_sql:{}", add_column_sql);
                                        taos.exec(&add_column_sql).await?;
                                    }
                                }

                                if table_config.tag_configs.is_some() {
                                    // let tag_configs = &table_config.tag_configs.clone().unwrap();
                                    let desc = taos.describe(&stable_name).await?;
                                    let fields = table_config
                                        .tag_configs
                                        .as_ref()
                                        .unwrap()
                                        .iter()
                                        .map(|config| {
                                            (config.column_name.clone(), config.column_type.clone())
                                        })
                                        .collect_vec();
                                    let sqls = generate_alter_sql_diff_desc(
                                        &stable_name,
                                        &desc,
                                        &fields,
                                        true,
                                    );
                                    if sqls.is_some() {
                                        let sqls = sqls.unwrap();
                                        for alter_sql in sqls {
                                            tracing::info!("alter table sql: {alter_sql}");
                                            match taos.exec(alter_sql).await {
                                                Ok(_) => (),
                                                Err(err) => {
                                                    if err.to_string().contains("0x0369") {
                                                        // Tag already exists occur when concurrent exec same alter
                                                        tracing::warn!(
                                                            "alter table err: {}, will be ignored",
                                                            err.to_string()
                                                        );
                                                    } else {
                                                        tracing::warn!(
                                                            "alter table err: {}",
                                                            err.to_string()
                                                        );
                                                        break 'outer;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else if errstr.contains("[0x2653]") {
                                // column or tag length not enough
                                let desc = taos.describe(&stable_name.as_str()).await?;
                                let mut tags_for_diff = Vec::new();
                                tags_for_diff.push((
                                    "point_id".to_string(),
                                    IpcDataType::from_str(
                                        format!("varchar({})", modify_message.id.len()).as_str(),
                                    )
                                    .unwrap(),
                                ));
                                tags_for_diff.push((
                                    "point_name".to_string(),
                                    IpcDataType::from_str(
                                        format!("varchar({})", modify_message.point_name.len())
                                            .as_str(),
                                    )
                                    .unwrap(),
                                ));
                                if table_config.tag_configs.is_some() {
                                    for tag_conf in table_config.tag_configs.clone().unwrap() {
                                        tags_for_diff
                                            .push((tag_conf.column_name, tag_conf.column_type));
                                    }
                                }
                                let sqls = generate_alter_sql_diff_desc(
                                    &stable_name,
                                    &desc,
                                    &tags_for_diff,
                                    true,
                                );
                                if sqls.is_some() {
                                    let sqls = sqls.unwrap();
                                    for sql in sqls {
                                        tracing::info!("add execute sql: {}", &sql);
                                        taos.exec(sql)
                                            .await
                                            .context("Writing point stream error")?;
                                    }
                                }
                                for column_meta in desc {
                                    if (column_meta.ty == Ty::VarChar
                                        || column_meta.ty == Ty::NChar)
                                        && column_meta.field() == modify_message.value_cloumn_name
                                        && modify_message.value_cloumn_length > column_meta.length()
                                    {
                                        let sql = format!(
                                            "alter table `{stable_name}` modify column `{}` {}({})",
                                            column_meta.field(),
                                            column_meta.ty(),
                                            modify_message.value_cloumn_length,
                                        );
                                        tracing::info!("add execute sql: {}", &sql);
                                        taos.exec(sql).await.context(
                                            "Modify column length error while writing point stream",
                                        )?;
                                    }
                                }
                            } else {
                                break;
                            }
                            retry += 1;
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

#[instrument(skip_all, fields(writer.count = count, writer.stream = "flat"))]
async fn consume_flat_record(
    _taos: &Taos,
    record: &FlatMessage,
    count: &mut usize,
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
        tokio::task::yield_now().await;
        counter!(RECORD_BATCHES, 1);
        let batch = message.record();
        if let Some(parser) = parser {
            let batch = parser.parse_message_from_records(batch)?;
            match batch {
                crate::plugins::transform::Message::Raw(_) => todo!(),
                crate::plugins::transform::Message::Tables(_) => todo!(),
                crate::plugins::transform::Message::ChildTables(_) => todo!(),
                crate::plugins::transform::Message::Records(message) => {
                    for records in message {
                        if records.records.num_rows() == 0 {
                            continue;
                        }
                        counter!(BATCH_RECORDS, 1);
                        // dbg!(&records);

                        if records.records.column(0).null_count() > 0 {
                            bail!("Timestamp field contains null or invalid values");
                        }
                        tracing::debug!("Write records with rows {}", records.records.num_rows());
                        let views = taosx_ipc::stream::reader::record_batch_to_column_view(
                            &records.records,
                        );
                        // dbg!(&views);
                        let schema = records.records.schema();
                        let columns = schema.fields().iter().map(|f| f.name()).collect_vec();
                        let table_name = records.table.name.as_str();

                        let mut raw = RawBlock::from_views(&views, taos::Precision::Millisecond);
                        raw.with_field_names(&columns).with_table_name(table_name);
                        //debug!("{}", &raw.pretty_format());

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
                                                        _taos.exec(&sql).await?;
                                                        max_lengths
                                                            .insert(name.to_string(), length);
                                                        continue;
                                                    }
                                                }
                                                break;
                                            }
                                            Err(_) => {
                                                // dbg!(&err);
                                                if let Some(sql) = records.stable_sql() {
                                                    tracing::debug!(
                                                        "flat message stable sql : {sql}"
                                                    );
                                                    if let Some(transferred) = transferred {
                                                        transferred
                                                            .stables
                                                            .fetch_add(1, Ordering::SeqCst);
                                                    }
                                                    match _taos.exec(&sql).await {
                                                        Ok(_n) => (), // counter!(STABLE_CREATED, n as u64),
                                                        Err(err) => return Err(err)?,
                                                    }
                                                    let sql = records.table_sql();

                                                    loop {
                                                        match _taos.exec(&sql).await {
                                                            Ok(_n) => (), // counter!(CHILD_TABLE_CREATED,n as u64),
                                                            Err(err) => {
                                                                if err
                                                                    .to_string()
                                                                    .contains("[0x2605]")
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
                                                                    for f in
                                                                        desc.iter().filter(|f| {
                                                                            f.is_tag()
                                                                                && f.ty()
                                                                                    .is_var_type()
                                                                        })
                                                                    {
                                                                        let sql = format!(
                                                                            "alter table `{table}` modify tag `{}` {}({})",
                                                                            f.field(),
                                                                            f.ty(),
                                                                            f.length() * 2
                                                                        );
                                                                        let _ =
                                                                            _taos.exec(&sql).await;
                                                                        continue;
                                                                    }
                                                                } else if err
                                                                    .to_string()
                                                                    .contains("[0x260D]")
                                                                {
                                                                    // Tags number not matched
                                                                    // add Tag
                                                                    let table = records
                                                                        .table
                                                                        .using
                                                                        .as_deref()
                                                                        .unwrap();
                                                                    let tags =
                                                                        records.tag_meta().unwrap();
                                                                    for tag_meta in tags {
                                                                        let mut need_add = true;
                                                                        let res = _taos
                                                                            .describe(table)
                                                                            .await
                                                                            .unwrap();
                                                                        res.into_iter().for_each(
                                                                            |tag_added| {
                                                                                if tag_added
                                                                                    .is_tag()
                                                                                    && tag_added
                                                                                        .field()
                                                                                        == tag_meta
                                                                                            .field()
                                                                                {
                                                                                    need_add =
                                                                                        false;
                                                                                }
                                                                            },
                                                                        );
                                                                        if need_add {
                                                                            let add_tag_sql = format!(
                                                                                "alter table `{table}` add tag `{}` {}",
                                                                                tag_meta.field(),
                                                                                parser.get_ipcdatatype_from_parser(tag_meta.field()).unwrap().sql_repr()
                                                                                );
                                                                            tracing::info!("table {table} add tag sql: {add_tag_sql}");
                                                                            _taos
                                                                                .exec(add_tag_sql)
                                                                                .await
                                                                                .unwrap();
                                                                        }
                                                                    }
                                                                } else {
                                                                    Err(err)?;
                                                                }
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
                                                    _taos.exec(&sql).await?;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if let Err(err) = _taos.write_raw_block(&raw).await {
                                let code = err.code();
                                let err_str = err.to_string();
                                if err_str.contains("[0x2603]") || err_str.contains("[0x0618]") {
                                    if let Some(sql) = records.stable_sql() {
                                        // dbg!(&sql);
                                        match _taos.exec(&sql).await {
                                            Ok(_n) => (), // counter!(STABLE_CREATED, n as u64),
                                            Err(err) => {
                                                if err.to_string().contains("0x032C") {
                                                    // Object is creating
                                                    tracing::warn!(
                                                        "error code [0x032C] encountered, ignore"
                                                    );
                                                    continue;
                                                } else {
                                                    anyhow::bail!(
                                                        "create stable sql err: {}",
                                                        err.to_string()
                                                    );
                                                }
                                            }
                                        }

                                        let sql = records.table_sql();

                                        loop {
                                            match _taos.exec(&sql).await {
                                                Ok(_n) => (), // counter!(CHILD_TABLE_CREATED, n as u64),
                                                Err(err) => {
                                                    if err.to_string().contains("[0x2605]") {
                                                        let table =
                                                            records.table.using.as_deref().unwrap();
                                                        let desc =
                                                            _taos.describe(table).await.unwrap();
                                                        for f in desc.iter().filter(|f| {
                                                            f.is_tag() && f.ty().is_var_type()
                                                        }) {
                                                            let sql = format!(
                                                            "alter table `{table}` modify tag `{}` {}({})",
                                                            f.field(),
                                                            f.ty(),
                                                            f.length() * 2
                                                            );
                                                            _taos.exec(&sql).await?;
                                                            continue;
                                                        }
                                                    } else {
                                                        Err(err)?;
                                                    }
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
                                        _taos.exec(&sql).await?;
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
                                        _taos.exec(&sql).await?;
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
                                            let ipc_data_type =
                                                parser.get_ipcdatatype_from_parser(column_name);
                                            if ipc_data_type.is_none() {
                                                anyhow::bail!("column name {column_name} not config in parser");
                                            }
                                            let sql = format!(
                                                "alter table `{}` add column `{}` {}",
                                                records
                                                    .table
                                                    .using
                                                    .as_ref()
                                                    .unwrap_or(&table_name.to_string()),
                                                &column_name,
                                                ipc_data_type.unwrap(),
                                            );
                                            tracing::info!("alter table column sql: {}", sql);
                                            _taos.exec(&sql).await?;
                                        }
                                        index += 1;
                                    }
                                // } else if err_str.contains("0x022D") {
                                //     info!(table = table_name, code = %code, "write {} records failed: {err:#}, retry", records.records.num_rows());
                                //     // panic!("{}", err);
                                //     Err(err)?;
                                //     break;
                                } else {
                                    error!(table = table_name, code = %code, "write {} records failed: {err:?}", records.records.num_rows());
                                    counter!(WRITE_RAW_BLOCK_FAILS, 1);
                                    counter!(RECORD_FAILS, raw.nrows() as u64);
                                    counter!(
                                        POINT_FAILS,
                                        (raw.nrows() * raw.column_views().len()) as u64
                                    );
                                    Err(err)?;
                                    break;
                                }
                                continue;
                            } else {
                                *count += raw.nrows();
                                counter!(WRITE_RAW_BLOCKS, 1);
                                counter!(RECORDS, raw.nrows() as u64);
                                counter!(POINTS, (raw.nrows() * raw.column_views().len()) as u64);
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
            let _ = taosx_ipc::stream::reader::record_batch_to_column_view(batch);
            // let mut stmt = Stmt::init(&taos)?;
            // process id, ts, value
            // dbg!(&cv_vec);
            anyhow::bail!("Parser should be set with flat stream");
        }
    }
    Ok(())
}

// #[instrument(skip_all)]
#[instrument(skip_all)]
async fn ipc_lush_stream_reader<R: Read + Send + 'static, W: Write>(
    pool: &TaosPool,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    let taos = pool.get().await?;
    let columns = ipc_reader
        .columns()
        .into_iter()
        .map(|s| format!("{s}"))
        .collect_vec();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");
    let mut stmt = Stmt::init(&taos).await?;

    let mut count = 0;
    let mut stream = ipc_reader.into_stream();

    let mut batches = 0;
    static mut ACKS: AtomicUsize = AtomicUsize::new(0);
    let mut taos = Some(taos);
    while let Some(record) = stream.try_next().await.context("next item error")? {
        let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
            std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
        })
        .unwrap();

        let last = count;
        if let Err(err) = consume_lush_record(
            pool,
            &mut taos,
            &mut stmt,
            record,
            &columns,
            &names,
            &marks,
            &mut count,
            license,
            transferred,
        )
        .in_current_span()
        .await
        {
            tracing::error!("write batch {batches} error: {err:#}");
            let written = count - last;
            let _ = ipc_ack_writer.ack(LushAck {
                code: 0xFFFF,
                message: Some(err.to_string()),
                context: Some(
                    json!({
                        "stream": "flat",
                        "written":  written,
                    })
                    .to_string(),
                ),
            });
        } else {
            tracing::info!("ack");
            let _ = ipc_ack_writer
                .ack(LushAck {
                    code: 0,
                    message: None,
                    context: Some(
                        json!({
                            "stream": "flat",
                            "written":  count - last,
                        })
                        .to_string(),
                    ),
                })
                .context("write ack error");
            tracing::info!(acks = unsafe { ACKS.load(Ordering::SeqCst) }, "ack done");
        }
        unsafe { ACKS.fetch_add(1, Ordering::SeqCst) };
        batches += 1;
    }
    println!("finished, totally {count} rows");
    Ok(())
}

#[instrument(skip_all)]
async fn ipc_point_reader<R: Read, W: Write>(
    taos: &Taos,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    config: Option<OpcTableConfig>,
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    let mut count = 0;
    let mut stmt = Stmt::init(taos).await?;
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

const IPC_STREAM_RECORDS: &str = "ipc.stream.records";
#[instrument(skip_all, fields(ipc.stream.item = "flat", ipc.stream.records = 0, ipc.stream.batches = 0))]
async fn ipc_flat_stream_reader<R: Read, W: Write>(
    taos: &Taos,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    parser: Option<&Parser>,
    license: Option<&ConnectorLicense>,
    transferred: Option<&Transferred>,
) -> anyhow::Result<()> {
    let mut count = 0;
    let mut batches = 0;
    let mut stream = futures::stream::iter(ipc_reader).inspect_err(|err| {
        tracing::warn!("Receive IPC item error: {err:#}");
    });
    while let Some(record) = stream.try_next().await? {
        // if let Ok(record) = record {
        batches += 1;
        let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
            std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
        })
        .unwrap();
        let last = count;
        if let Err(err) =
            consume_flat_record(&taos, &record, &mut count, parser, license, transferred).await
        {
            tracing::error!("write batch {batches} error: {err:#}");
            let written = count - last;
            let _ = ipc_ack_writer.ack(LushAck {
                code: 0xFFFF,
                message: Some(err.to_string()),
                context: Some(
                    json!({
                        "stream": "flat",
                        "written":  written,
                    })
                    .to_string(),
                ),
            });
        } else {
            let _ = ipc_ack_writer
                .ack(LushAck {
                    code: 0,
                    message: None,
                    context: Some(
                        json!({
                            "stream": "flat",
                            "written":  count - last,
                        })
                        .to_string(),
                    ),
                })
                .context("write ack error");
        }
    }
    metrics::counter!("ipc.stream.records", count as u64);
    metrics::counter!("ipc.stream.batches", batches as u64);

    tracing::Span::current()
        .record(IPC_STREAM_RECORDS, count)
        .record("ipc.stream.batches", batches)
        .in_scope(|| {
            info!("IPC processing done, written totally {count} records");
        });
    println!("Flat stream writing finished, totally {count} rows");
    Ok(())
}

pub fn generate_alter_sql_diff_desc(
    tablename: &str,
    desc: &Describe,
    fields: &Vec<(String, IpcDataType)>,
    is_tag: bool,
) -> Option<Vec<String>> {
    let mut alter_sql = Vec::new();
    // diff columns and tags
    for (name, ty) in fields {
        if name == "__table_name__" {
            continue;
        }
        let mut should_alter = false;
        let mut should_add = true;
        desc.iter().for_each(|c| {
            if c.field() == name {
                should_add = false;
                let original_ty = c.ty();
                let new_def_ty = ty.ty();
                if original_ty.is_var_type() {
                    match ty {
                        IpcDataType::VarChar(len) | IpcDataType::NChar(len) => {
                            if original_ty.to_string() != new_def_ty.to_string()
                                || len.clone() as usize > c.length()
                            {
                                should_alter = true;
                            }
                        }
                        _ => (),
                    }
                } else if original_ty.to_string() != new_def_ty.to_string() {
                    should_alter = true;
                }
            }
        });
        if should_alter && !is_tag {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` MODIFY COLUMN `{name}` {} ",
                ty.sql_repr()
            ));
        } else if should_alter {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` MODIFY TAG `{name}` {} ",
                ty.sql_repr()
            ));
        }
        if should_add && !is_tag {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` ADD COLUMN `{name}` {} ",
                ty.sql_repr()
            ));
        } else if should_add {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` ADD TAG `{name}` {} ",
                ty.sql_repr()
            ));
        }
    }
    if alter_sql.is_empty() {
        None
    } else {
        Some(alter_sql)
    }
}

#[framed]
#[instrument(skip_all, fields(client, connector))]
async fn ipc_process<R: Read + Send + 'static, W: Write>(
    client: String,
    pool: TaosPool,
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
    _lock: Arc<Mutex<()>>,
    config: Option<OpcTableConfig>,
    parser: Option<Parser>,
    connector: Option<&str>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    info!(client, "IPC stream processing...");
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
        // let guard = lock.lock().await;
        let init = metadata.init().unwrap();
        handle_lush_message_init(init, &taos, &sql).await?;
        // drop(guard)
    }
    info!(?stream_type, "Processing stream");
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
                &pool,
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

async fn handle_lush_message_init(
    init: &LushMessageInit,
    taos: &Taos,
    sql: &str,
) -> anyhow::Result<()> {
    let max_retries = 10;
    let mut i = 0;
    let stable_name = init.name();
    loop {
        // alter table
        let desc = taos.describe(stable_name).await;
        match desc {
            Ok(desc) => {
                tracing::debug!("table {stable_name} exists");
                let sql = generate_alter_sql_diff_desc(
                    stable_name,
                    &desc,
                    init.columns().as_ref(),
                    false,
                );
                if sql.is_some() {
                    for sql in sql.unwrap() {
                        tracing::info!("alter table sql: {}", sql.clone());
                        taos.exec(sql).await?;
                    }
                }
                let sql =
                    generate_alter_sql_diff_desc(stable_name, &desc, init.tags().as_ref(), true);
                if sql.is_some() {
                    for sql in sql.unwrap() {
                        tracing::info!("alter table sql: {}", sql.clone());
                        taos.exec(sql).await?;
                    }
                }
                break;
            }
            Err(err) => {
                tracing::warn!("describe failed: {}", err.to_string());
                // create table
                info!("create sql: {sql}");
                let res: Result<usize, taos::Error> = taos.exec(&sql).await;
                if let Err(err) = res {
                    tracing::error!("Query error with {sql}: {err:?}");
                    i += 1;
                    if i > max_retries {
                        break;
                    }
                } else {
                    // metrics::counter!(STABLE_CREATED, 1);
                    break;
                }
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub struct IpcStreamWorker {
    pool: TaosPool,
    parser: IpcParser,
    lock: Arc<Mutex<()>>,
    task: Option<i64>,
    from: Dsn,
    config: Option<OPCConfig>,
    opc_table_config: OnceCell<OpcTableConfig>,
    license: Option<ConnectorLicense>,
    transferred: Option<Transferred>,
    span: tracing::Span,
    // stmt: Arc<UnsafeCell<Stmt>>,
}
impl IpcStreamWorker {
    pub async fn new(
        pool: TaosPool,
        from: Dsn,
        lock: Arc<Mutex<()>>,
        schema: Arc<Schema>,
        license: Option<ConnectorLicense>,
        transferred: Option<Transferred>,
        span: tracing::Span,
        // license: Option<>
    ) -> anyhow::Result<Self> {
        let config = if from.driver.starts_with("opc") {
            let taos = pool.get().await?;
            Some(opc_config_blocking(&taos, &from, 1).await?)
        } else {
            None
        };

        // let stmt = Stmt::init(&taos)?;
        Ok(Self {
            pool,
            from,
            parser: IpcParser::new(schema),
            lock: lock,
            task: None,
            config,
            opc_table_config: OnceCell::const_new(),
            license,
            transferred, // stmt: Arc::new(UnsafeCell::new(stmt)),
            span,
        })
    }

    pub fn with_presets(mut self, preset: OPCConfig) -> Self {
        self.config.replace(preset);
        self
    }

    #[instrument(skip_all)]
    pub async fn process_record(
        &self,
        stmt: &mut Stmt,
        record: RecordBatch,
        parser: Option<&Parser>,
    ) -> anyhow::Result<usize> {
        let taos = self.pool.get().await?;
        if let Some(sql) = self.parser.metadata().init_sql_string() {
            let guard = self.lock.lock().await;
            let init = self.parser.metadata.init().unwrap();
            handle_lush_message_init(init, &taos, &sql).await?;
            // let max_retries = 10;
            // let mut i = 0;
            // loop {
            //     info!("metadata sql: {sql}");
            //     let res = self.taos.exec(&sql).await;
            //     if let Err(err) = res {
            //         tracing::error!("Query error with {sql}: {err:?}");
            //         i += 1;
            //         if i > max_retries {
            //             break;
            //         }
            //     } else {
            //         break;
            //     }
            // }
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
                    &taos,
                    &record,
                    &mut count,
                    parser, // todo: license
                    self.license.as_ref(),
                    self.transferred.as_ref(),
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
                let mut taos = Some(self.pool.get().await?);
                // let stmt = unsafe { &mut *self.stmt.get() };
                consume_lush_record(
                    &self.pool,
                    &mut taos,
                    stmt,
                    record,
                    &columns,
                    &names,
                    &marks,
                    &mut count,
                    self.license.as_ref(),
                    self.transferred.as_ref(),
                )
                .await?;
                Ok(count)
            }
            StreamType::Point => {
                if let Some((_license, transferred)) =
                    self.license.as_ref().zip(self.transferred.as_ref())
                {
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
                        let _v = config.parse_tables_with(&taos).await?;
                        self.opc_table_config
                            .get_or_try_init(|| async { config.parse_tables_with(&taos).await })
                            .await?
                    }
                };
                drop(guard);
                let _n = consume_point_record(&taos, stmt, &record, &mut count, config).await?;
                if let Some(transferred) = &self.transferred {
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

pub async fn listen_tcp_socket_with_agent(
    socket: impl AsRef<str>,
    cancel: CancellationToken,
    with_agent: (i64, String, String),
) -> anyhow::Result<IpcHandler> {
    let addr = socket.as_ref();

    let (sender, error_receiver) = tokio::sync::mpsc::channel(1);

    let socket = tokio::net::TcpListener::bind(addr).await?;

    // let (closer, mut receiver) = tokio::sync::mpsc::channel::<()>(1);
    // let closed = Arc::new(AtomicBool::new(false));
    // let closed2 = closed.clone();

    let notify = Arc::new(tokio::sync::Notify::new());
    let notified = notify.clone();
    let thread = tokio::spawn(
        async move {
            let mut handlers = vec![];
            let accept_stream = |stream: tokio::net::TcpStream, addr: std::net::SocketAddr| {
                tracing::info!("new tcp client!: {:?}", addr);
                let stream = stream.into_std().unwrap();
                let _ = stream.set_nonblocking(false);
                // let client = addr.as_socket_ipv4().unwrap().to_string();
                let se = sender.clone();
                let cancel = cancel.clone();
                let (id, remote, token) = with_agent.clone();

                tokio::spawn(async move {
                    let client = addr.to_string();
                    let res =
                        ipc_tcp_forward(client.clone(), stream, cancel, remote, token, id).await;
                    if let Err(err) = res {
                        tracing::error!("{:?}", err);
                        let r = se.send(format!("{err:?}")).await;
                        if let Err(send_err) = r {
                            tracing::error!("error <{err:?}> reported to server: {send_err:?}");
                        }
                    } else {
                        tracing::info!("IPC reader stopped for client {client}",);
                    }
                })
            };
            loop {
                tokio::select! {
                    _ = notified.notified() => {
                        break;
                    }
                    accept = socket.accept() => {
                        match accept {
                            Ok((stream, addr)) => {
                                let h = accept_stream(stream, addr);
                                handlers.push(h);
                            }
                            Err(e) => {
                                /* connection failed */
                                tracing::info!("IPC stream acceptation error {e}, might be stopped");
                                break;
                            }
                        }
                    }
                }
            }
            tracing::info!(ipc.handlers = handlers.len(), "IPC stream listener stopped");
            let instant = std::time::Instant::now();

            for h in handlers {
                let _ = h.await;
            }
            tracing::info!("IPC stream handlers finished after {:?}", instant.elapsed());
            anyhow::Ok(())
        }
        .instrument(tracing::info_span!("agent_ipc_listener")),
    );

    let handle = tokio::spawn(async move {
        tracing::debug!("shutdown socket");
        match tokio::time::timeout(Duration::from_secs(60 * 60), thread).await {
            Ok(Ok(_)) => anyhow::Ok(()),
            Ok(Err(err)) => anyhow::bail!("Thread join error: {err}"),
            Err(_) => {
                anyhow::bail!("Task running deadline elapsed(1h), but seems not finished")
            }
        }
    });
    Ok(IpcHandler::new(notify, handle, error_receiver))
}

pub struct IpcHandler {
    closer: Arc<Notify>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    receiver: tokio::sync::mpsc::Receiver<String>,
}

impl IpcHandler {
    fn new(
        closer: Arc<Notify>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
        receiver: tokio::sync::mpsc::Receiver<String>,
    ) -> Self {
        Self {
            closer,
            handle,
            receiver,
        }
    }
    pub async fn send<T>(&self, _: T) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
        // self.closer.send(()).await
        self.closer.notify_waiters();
        Ok(())
    }
    pub async fn wait(mut self) -> anyhow::Result<()> {
        (&mut self.handle).await?
    }

    /// Receive error
    pub async fn recv_error(&mut self) -> Option<String> {
        self.receiver.recv().await
    }

    /// Receive error
    pub fn try_recv_error(&mut self) -> Result<String, tokio::sync::mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Close IPC listener and wait until IPC handler joint.
    pub async fn close(self) -> anyhow::Result<()> {
        // let _ = self.closer.send(()).await;
        self.closer.notify_waiters();
        self.handle.await??;
        Ok(())
    }
}

#[instrument(skip_all, parent = &span)]
pub async fn listen_tcp_socket(
    target: TaosPool,
    socket: impl AsRef<str>,
    // sender: Sender<String>,
    config: Option<OpcTableConfig>,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
) -> anyhow::Result<IpcHandler> {
    let (sender, error_receiver) = tokio::sync::mpsc::channel(1);

    let addr = socket.as_ref();
    let socket = tokio::net::TcpSocket::new_v4()?;
    let addr: SocketAddr = addr.parse()?;
    socket.bind(addr)?;
    let socket = socket.listen(128)?;
    socket.set_ttl(100)?;

    info!("listen on socket address: {addr}");
    let sql_lock = Arc::new(Mutex::new(()));
    let socket = Arc::new(socket);
    let notify = Arc::new(tokio::sync::Notify::new());
    let notified = notify.clone();

    let thread = tokio::task::spawn(
        async move {
            info!("waiting for IPC connections");
            let mut handlers = vec![];
            let accept_stream = |stream: tokio::net::TcpStream, addr: std::net::SocketAddr| {
                tracing::info!("new tcp client!: {:?}", addr);
                // let span = tracing::info_span!("ipc_reader", client.address = %addr);
                let stream = stream.into_std().unwrap();
                let _ = stream.set_nonblocking(false);
                let client = addr.to_string();
                let se = sender.clone();
                let cancel = cancel.clone();

                if let Some((id, server, token)) = with_agent.clone() {
                    tokio::spawn(async move {
                        let res = ipc_tcp_forward(client, stream, cancel, server, token, id).await;
                        if let Err(err) = res {
                            // panic!("{err:?}");
                            tracing::error!("ipc read err: {}", err);
                            let _ = se.send(err.to_string()).await;
                        }
                    })
                } else {
                    let pool = target.clone();
                    let lock = sql_lock.clone();
                    let config = config.clone();
                    let parser = parser.clone();
                    let connector = connector.clone();
                    let transferred = transferred.clone();
                    tokio::spawn(async move {
                        // let dsn: Dsn = "taos:///db2".parse().unwrap();
                        // let pool = TaosBuilder::from_dsn(dsn).unwrap().pool().unwrap();
                        info!("Spawned IPC reader");
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
                        .in_current_span()
                        .await;
                        if let Err(err) = res {
                            // panic!("{err:?}");
                            println!("{err:?}");
                            tracing::error!("ipc read err: {:#}", err);
                            let _ = se.send(err.to_string()).await;
                        } else {
                            tracing::debug!("IPC handler completed");
                        }
                    }.instrument(span.clone()))
                }
            };
            loop {
                tokio::select! {
                    _ = notified.notified() => {
                        tracing::debug!("IPC listener received close signal");
                        break;
                    }
                    accept = socket.accept() => {
                        match accept {
                            Ok((stream, addr)) => {
                                let h = accept_stream(stream, addr);
                                handlers.push(h);
                            }
                            Err(e) => {
                                /* connection failed */
                                tracing::info!("IPC stream acceptation error {e}, might be stopped");
                                break;
                            }
                        }
                    }
                }
            }
            tracing::info!(ipc.handlers = handlers.len(), "IPC stream listener would wait for handlers to finish");

            let _ = tracing::info_span!("wait for ipc handlers to be finished").entered();

            let instant = std::time::Instant::now();
            for h in handlers {
                let _ = h.await;
            }
            tracing::info!("IPC stream handlers finished after {:?}", instant.elapsed());
        }
        .instrument(tracing::info_span!("plain_ipc_listener")),
    );
    let handle = tokio::spawn(
        async move {
            // closed.store(true, std::sync::atomic::Ordering::SeqCst);
            tracing::info!("stop listener");
            match tokio::time::timeout(Duration::from_secs(60 * 60), thread).await {
                Ok(Ok(_)) => anyhow::Ok(()),
                Ok(err) => err.map_err(Into::into),
                Err(_) => {
                    anyhow::bail!("Task running deadline elapsed(1h), but seems not finished")
                }
            }
        }
        .instrument(tracing::info_span!("plain_ipc_listener_abort_handle")),
    );
    Ok(IpcHandler::new(notify, handle, error_receiver))
}
