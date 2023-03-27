use std::{
    any::Any,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    sync::Arc,
};
use taos::{AsyncQueryable, Bindable, Dsn, Itertools, Stmt, TBuilder, Taos, TaosBuilder, TaosPool};
// use taosx_ipc::ack::{AckWriter, AckWriterBuilder};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::{info, instrument};

use taosx_ipc::prelude::*;

#[instrument(skip(pool))]
async fn ipc_tcp_read(
    client: SocketAddr,
    pool: TaosPool,
    stream: TcpStream,
    lock: Arc<Mutex<()>>,
) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    let client = client.to_string();
    ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock).await
}

#[cfg(not(target_os = "windows"))]
async fn ipc_unix_read(
    client: String,
    pool: TaosPool,
    stream: std::os::unix::net::UnixStream,
    lock: Arc<Mutex<()>>,
) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock).await
}

#[instrument(skip(taos, record, names, marks))]
async fn consume_lush_record(
    taos: &Taos,
    record: LushMessage,
    names: &str,
    marks: &str,
    records: &mut usize,
) -> anyhow::Result<()> {
    match record {
        LushMessage::Tables(tables) => {
            for table in tables {
                let sql = table.to_sql(None).unwrap();
                info!("Tables: {sql}");
                taos.exec(&sql).await.unwrap();
            }
        }
        LushMessage::Insert(record) => {
            let mut stmt = Stmt::init(&taos).unwrap();
            info!("init stmt");
            let sql = format!("insert into ? ({names}) values({marks})");
            info!("prepare with sql: {sql}");
            stmt.prepare(&sql).unwrap();
            info!("prepare");
            for record in record {
                *records += record.num_rows();
                // let data = record.to_column_views();
                let map_data = record.to_column_views_group_by_tablename();
                // dbg!(&map_data);
                for (k, data_vec) in &map_data {
                    let table_name = k.as_deref().or(record.table());
                    if let Some(table_name) = table_name {
                        if let Err(err) = stmt.set_tbname(table_name) {
                            tracing::warn!("table name `{}` error {err}", table_name);
                            if let Some(tb) = record.meta_sql(Some(String::from(table_name))) {
                                info!("sql: {tb}");
                                taos.exec_sync(&tb).unwrap();
                                // rt.block_on(taos.exec(&tb))?;
                                stmt.set_tbname(table_name).unwrap();
                            }
                        }
                        stmt.bind(data_vec.as_slice()).unwrap();
                        stmt.add_batch().unwrap();
                        let n = stmt.execute().unwrap();

                        info!("written [{n}] records for table {table_name}");
                    } else {
                        stmt.bind(data_vec.as_slice()).unwrap();
                        stmt.add_batch().unwrap();
                        let n = stmt.execute().unwrap();

                        info!("written : [{n}] records");
                    }
                }
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn ipc_lush_stream_reader<R: Read, W: Write>(
    taos: &Taos,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    let columns = ipc_reader.columns();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");

    let mut records = 0;

    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_lush_record(&taos, record, &names, &marks, &mut records)
                .await
                .unwrap();

            ipc_ack_writer.write_ok().unwrap();
        }
    }
    println!("finished, totally {records} rows");
    Ok(())
}

#[instrument(skip(pool, ipc_reader, ipc_ack_writer))]
async fn ipc_process<R: Read, W: Write>(
    client: String,
    pool: TaosPool,
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
    lock: Arc<Mutex<()>>,
) -> anyhow::Result<()> {
    let taos = pool.get()?;
    let metadata = ipc_reader.metadata();
    let stream_type = *metadata.stream_type();
    if let Some(sql) = ipc_reader.metadata().init_sql_string() {
        let guard = lock.lock().await;
        loop {
            info!("[{client}] {sql}");
            // rt.block_on(taos.exec(&sql))?;
            let res = taos.exec(&sql).await;
            if let Err(err) = res {
                tracing::error!("Query error with {sql}: {err:?}");
            } else {
                break;
            }
        }
        drop(guard)
    }
    match stream_type {
        StreamType::Line => todo!(),
        StreamType::Flat => todo!(),
        StreamType::Lush => ipc_lush_stream_reader(&taos, ipc_reader, ipc_ack_writer).await?,
        StreamType::Point => todo!(),
    }
    Ok(())
}

#[cfg(unix)]
pub fn listen_unix_socket(target: TaosPool, socket: impl AsRef<Path>) -> anyhow::Result<()> {
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

    // let pool = TaosBuilder::from_dsn("taos:///test3")?.pool()?;
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new unix client!: {:?}", addr);
                let pool = target.clone();
                let lock = sql_lock.clone();
                runtime.spawn(async move {
                    ipc_unix_read(
                        addr.as_pathname()
                            .map(|path| path.display().to_string())
                            .unwrap_or_default(),
                        pool,
                        stream,
                        lock,
                    )
                });
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
            }
        }
    }
}

pub fn listen_tcp_socket(target: TaosPool, socket: impl AsRef<str>) -> anyhow::Result<()> {
    let tcp_addr = socket.as_ref();
    let listener = TcpListener::bind(tcp_addr)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .build()?;
    info!("listen on socket address: {tcp_addr}");
    let sql_lock = Arc::new(Mutex::new(()));
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log::info!("new tcp client!: {:?}", addr);
                let pool = target.clone();
                let lock = sql_lock.clone();
                runtime.spawn(async move {
                    let res = ipc_tcp_read(addr, pool, stream, lock).await;
                    if let Err(err) = res {
                        panic!("{err:?}");
                    }
                });
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
                panic!("Error: {:?}", e);
            }
        }
    }
    Ok(())
}
