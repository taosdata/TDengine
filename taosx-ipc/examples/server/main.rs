use std::{
    any::Any,
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
};
use taos::sync::*;
use taosx_ipc::{
    ack::{AckWriter, AckWriterBuilder},
    stream::{flat::FlatMessage, point::PointMessage},
};
use tokio::runtime::Runtime;
use tracing::{info, instrument};

use taosx_ipc::prelude::*;

// shadow_rs::shadow!(build);

#[instrument]
async fn hello() -> &'static str {
    "Hello world!"
}

fn ipc_windows_read(stream: TcpStream) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    ipc_test(ipc_reader, ipc_ack_writer)
}

#[cfg(not(target_os = "windows"))]
fn ipc_unix_read(stream: std::os::unix::net::UnixStream) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    ipc_test(ipc_reader, ipc_ack_writer)
}

fn ipc_test<R: Read, W: Write>(
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    let dsn =
        std::env::var("TAOSX_TARGET").unwrap_or("taos+ws://192.168.0.201:56041/pi".to_string());
    let mut dsn: Dsn = dsn.parse()?;
    let builder = TaosBuilder::from_dsn(&dsn)?;

    let taos = builder.build().unwrap_or_else(|e| {
        info!("connect error: {}", e);
        let subject = dsn.subject.take();
        let new_builder = TaosBuilder::from_dsn(&dsn).unwrap();
        let taos = new_builder.build().unwrap();
        taos.exec(format!("create database `{}`", subject.unwrap()))
            .unwrap();
        builder.build().unwrap()
    });

    // let (reader, writer) = stream.pair();
    // stream.set_nonblocking(true).unwrap();

    let metadata = ipc_reader.metadata();
    dbg!(metadata);
    match metadata.stream_type() {
        StreamType::Lush => handle_lush_message(ipc_reader, taos, ipc_ack_writer)?,
        StreamType::Point => handle_point_message(ipc_reader, taos, ipc_ack_writer)?,
        StreamType::Flat => handle_flat_message(ipc_reader, taos, ipc_ack_writer)?,
        _ => todo!(),
    }
    Ok(())
}

fn handle_flat_message<R: Read, W: Write>(
    ipc_reader: IpcReader<R>,
    taos: Taos,
    mut ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    let mut map = HashMap::new();
    map.insert(
        String::from("topic1"),
        (
            String::from("d1004"),
            String::from("current"),
            IpcDataType::Float32,
        ),
    );
    map.insert(
        String::from("topic2"),
        (
            String::from("d1004"),
            String::from("voltage"),
            IpcDataType::Int32,
        ),
    );
    map.insert(
        String::from("topic3"),
        (
            String::from("d1004"),
            String::from("phase"),
            IpcDataType::Float32,
        ),
    );
    let mut records_count = 0;
    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            // let record = record.as_any().downcast_ref::<FlatMessage>().unwrap();
            // dbg!(record);
            for message in record.records() {
                let mut cv_vec =
                    taosx_ipc::stream::reader::record_batch_to_column_view(message.record());
                let mut stmt = Stmt::init(&taos)?;
                // process ts, topic, qos, payload
                let schema = message.schema();
                let topic_index = schema.index_of("topic").unwrap();
                let ts_index = schema.index_of("ts").unwrap();
                let payload_index = schema.index_of("payload").unwrap();
                let topic_cv = cv_vec.remove(topic_index);
                dbg!(&cv_vec);
                for i in 0..topic_cv.len() {
                    let id = topic_cv.get(i).unwrap().into_value().to_string().unwrap();
                    let (table, field, _) = map.get(&id).unwrap();
                    let sql = if ts_index > payload_index {
                        format!("insert into {table} ({field}, ts) values (?, ?)")
                    } else {
                        format!("insert into {table} (ts, {field}) values (?, ?)")
                    };
                    stmt.prepare(&sql).unwrap();
                    let new_cv_vec = cv_vec
                        .iter()
                        .map(|t_cv| t_cv.slice(i..i + 1).unwrap())
                        .collect_vec();
                    info!(sql);
                    dbg!(&new_cv_vec);
                    stmt.bind(&new_cv_vec.as_slice()).unwrap();
                    stmt.add_batch().unwrap();
                    let n = stmt.execute().unwrap();
                    records_count += n;
                }
            }
            ipc_ack_writer.write_ok().unwrap();
        }
    }
    println!("finished, totally {records_count} rows");
    Ok(())
}

fn handle_lush_message<R: Read, W: Write>(
    ipc_reader: IpcReader<R>,
    taos: Taos,
    mut ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    let _rt = Runtime::new().unwrap();
    if let Some(sql) = ipc_reader.metadata().init_sql_string() {
        info!("{sql}");
        taos.exec(&sql)?;
        // taos.exec_sync(&sql).unwrap();
    }
    let columns = ipc_reader.columns().into_iter().map(|s| format!("{s}")).collect_vec();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");

    let mut records = 0;

    for record in ipc_reader {
        if let Ok(record) = record {
            let record = record.as_any().downcast_ref::<LushMessage>().unwrap();
            // dbg!(&record);
            match record {
                LushMessage::Tables(tables) => {
                    for table in tables {
                        let sql = table.to_sql(None).unwrap();
                        taos.exec(&sql)?;
                    }
                }
                LushMessage::Insert(record) => {
                    for record in record {
                        records += record.num_rows();
                        // let data = record.to_column_views();
                        let map_data = record.to_column_views_group_by_tablename();
                        dbg!(&map_data);
                        for (k, data_vec) in &map_data {
                            let table_name = k.as_deref().or(record.table());
                            let mut stmt = Stmt::init(&taos)?;
                            info!("init stmt");
                            let sql = format!("insert into ? ({names}) values({marks})");
                            info!("prepare with sql: {sql}");
                            stmt.prepare(&sql).unwrap();
                            info!("prepare");
                            if let Some(table_name) = table_name {
                                if let Err(err) = stmt.set_tbname(table_name) {
                                    tracing::warn!("table name `{}` error {err}", table_name);
                                    if let Some(tb) =
                                        record.meta_sql(Some(String::from(table_name)))
                                    {
                                        info!("sql: {tb}");
                                        // taos.exec_sync(&tb).unwrap();
                                        taos.exec(&tb)?;
                                        // stmt.set_tbname(table_name).unwrap();
                                    }
                                }
                                // stmt.bind(data_vec.as_slice()).unwrap();
                                // stmt.add_batch().unwrap();
                                // let n = stmt.execute().unwrap();
                                let mut column_value_pairs:Vec<(String, String)> = Vec::new();
                                debug_assert!(columns.len() == data_vec.len());
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
                                                temp_column_value_pair.0.push_str(columns[index].as_str());
                                                temp_column_value_pair.0.push_str(",");
                                                temp_column_value_pair.1.push('\'');
                                                temp_column_value_pair.1.push_str(v.into_value().to_string().unwrap().as_str());
                                                temp_column_value_pair.1.push('\'');
                                                temp_column_value_pair.1.push_str(",");
                                            } else {
                                                println!("column view {} is null", columns[index]);
                                            }
                                        } else {
                                            println!("column view {} is null", columns[index]);
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
                                    let sql = format!("insert into {table_name} {column_names} VALUES {values}");
                                    println!("sql: {sql}");
                                    let res = taos.exec(sql).unwrap();
                                    info!("written [{res}] records for table {table_name}");
                                });
                            } else {
                                stmt.bind(data_vec.as_slice()).unwrap();
                                stmt.add_batch().unwrap();
                                let n = stmt.execute().unwrap();

                                info!("written : [{n}] records");
                            }
                            drop(stmt);
                        }
                    }
                }
            }

            ipc_ack_writer.write_ok().unwrap();
        }
    }
    println!("finished, totally {records} rows");
    Ok(())
}

fn handle_point_message<R: Read, W: Write>(
    ipc_reader: IpcReader<R>,
    taos: Taos,
    mut ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    // let rt = Runtime::new().unwrap();
    // TODO use the map initialized
    let mut map = HashMap::new();
    map.insert(
        String::from("1"),
        (
            String::from("d1004"),
            String::from("current"),
            IpcDataType::Float32,
        ),
    );
    map.insert(
        String::from("2"),
        (
            String::from("d1004"),
            String::from("voltage"),
            IpcDataType::Int32,
        ),
    );
    map.insert(
        String::from("3"),
        (
            String::from("d1004"),
            String::from("phase"),
            IpcDataType::Float32,
        ),
    );

    let mut records_count = 0;
    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            dbg!(&record);
            // let record = record.as_any().downcast_ref::<PointMessage>().unwrap();
            for message in record.records() {
                dbg!(&message.record());
                let mut cv_vec =
                    taosx_ipc::stream::reader::record_batch_to_column_view(message.record());
                let mut stmt = Stmt::init(&taos)?;
                // process id, ts, value
                let schema = message.schema();
                let id_index = schema.index_of("id").unwrap();
                let ts_index = schema.index_of("ts").unwrap();
                let value_index = schema.index_of("value").unwrap();
                let id_cv = cv_vec.remove(id_index);
                dbg!(&cv_vec);
                for i in 0..id_cv.len() {
                    let id = id_cv.get(i).unwrap().into_value().to_string().unwrap();
                    let (table, field, _) = map.get(&id).unwrap();
                    let sql = if ts_index > value_index {
                        format!("insert into {table} ({field}, ts) values (?, ?)")
                    } else {
                        format!("insert into {table} (ts, {field}) values (?, ?)")
                    };
                    stmt.prepare(&sql).unwrap();
                    let new_cv_vec = cv_vec
                        .iter()
                        .map(|t_cv| t_cv.slice(i..i + 1).unwrap())
                        .collect_vec();
                    info!(sql);
                    dbg!(&new_cv_vec);
                    stmt.bind(&new_cv_vec.as_slice()).unwrap();
                    stmt.add_batch().unwrap();
                    let n = stmt.execute().unwrap();
                    records_count += n;
                }
            }
            ipc_ack_writer.write_ok().unwrap();
        }
    }
    println!("finished, totally {records_count} rows");
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn listen_unix_socket() {
    let path = Path::new("taosx.sock");
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    info!("listen on socket address: {}", path.display());
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new unix client!: {:?}", addr);
                std::thread::spawn(|| ipc_unix_read(stream).unwrap());
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
            }
        }
    }
}

fn listen_tcp() {
    let tcp_addr = "0.0.0.0:6051";
    let listener = TcpListener::bind(tcp_addr).unwrap();
    info!("listen on socket address: {tcp_addr}");
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new tcp client!: {:?}", addr);
                std::thread::spawn(|| ipc_windows_read(stream).unwrap());
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_level(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_max_level(tracing::Level::DEBUG)
        .pretty()
        .init();
    #[cfg(not(target_os = "windows"))]
    let unix_handle = std::thread::spawn(listen_unix_socket);

    let handle = std::thread::spawn(listen_tcp);

    // HttpServer::new(move || {
    //     App::new()
    //         .wrap(TracingLogger::default())
    //         .service(web::resource("/hello").to(hello))
    // })
    // .bind("127.0.0.1:8080")?
    // .run()
    // .await?;

    handle.join().unwrap();
    #[cfg(not(target_os = "windows"))]
    unix_handle.join().unwrap();
    Ok(())
}
