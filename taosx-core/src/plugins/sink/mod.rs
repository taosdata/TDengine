use anyhow::Context;
use std::{
    any::Any,
    io::{Read, Write},
    net::SocketAddr,
    panic,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};
use taos::{AsyncQueryable, Bindable, Itertools, Stmt, Taos, TaosPool};
use tokio::sync::{mpsc::Sender, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, debug};

use super::runners::opc::OpcTableConfig;
use taosx_ipc::{prelude::*, stream::point::PointMessage};

// #[instrument(skip_all)]
async fn ipc_tcp_read(
    client: String,
    pool: TaosPool,
    stream: socket2::Socket,
    lock: Arc<Mutex<()>>,
    config: Option<OpcTableConfig>,
    cancel: CancellationToken,
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
        done = ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config) => {
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
    ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config).await
}

// #[instrument(skip(taos, record, names, marks))]
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
                taos.exec(&sql).await?;
            }
        }
        LushMessage::Insert(record) => {
            let mut stmt = Stmt::init(&taos)?;
            info!("init stmt");
            let sql = format!("insert into ? ({names}) values({marks})");
            info!("prepare with sql: {sql}");
            stmt.prepare(&sql)?;
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
                                taos.exec_sync(&tb)?;
                                // rt.block_on(taos.exec(&tb))?;
                                stmt.set_tbname(table_name)?;
                            }
                        }
                        stmt.bind(data_vec.as_slice())?;
                        stmt.add_batch().unwrap();
                        let n = stmt.execute()?;

                        info!("written [{n}] records for table {table_name}");
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
    taos: &Taos,
    record: &PointMessage,
    count: &mut usize,
    config: &OpcTableConfig,
) -> anyhow::Result<()> {
    for message in record.records() {
        let mut cv_vec = taosx_ipc::stream::reader::record_batch_to_column_view(message.record());
        let mut stmt = Stmt::init(&taos)?;
        // process id, ts, value
        let schema = message.schema();
        let id_index = schema.index_of("id")?;
        let ts_index = schema.index_of("ts")?;
        let value_index = schema.index_of("value")?;
        let id_cv = cv_vec.remove(id_index);
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
            let ts_cloumn_name = ts_cloumn.get(table).unwrap();
            let sql = if ts_index > value_index {
                format!("insert into {table} ({field}, {ts_cloumn_name}) values (?, ?)")
            } else {
                format!("insert into {table} ({ts_cloumn_name}, {field}) values (?, ?)")
            };
            debug!("sql: {}", sql);
            stmt.prepare(&sql).unwrap();
            let new_cv_vec = cv_vec
                .iter()
                .map(|t_cv| t_cv.slice(i..i + 1).unwrap())
                .collect_vec();
            stmt.bind(&new_cv_vec.as_slice())
                .context("STMT binding error")?;
            stmt.add_batch().context("STMT adding batch error")?;
            let n = stmt.execute().context("STMT executing error")?;
            *count += n;
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
    let columns = ipc_reader.columns();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");

    let mut count = 0;

    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_lush_record(&taos, record, &names, &marks, &mut count).await?;
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
    for record in ipc_reader {
        if let Ok(record) = record {
            let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
                std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
            })
            .unwrap();
            consume_point_record(&taos, &record, &mut count, config.as_ref().unwrap()).await?;

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
) -> anyhow::Result<()> {
    let taos = pool.get().await?;
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
        StreamType::Point => ipc_point_reader(&taos, ipc_reader, ipc_ack_writer, config).await?,
    }
    Ok(())
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
                });
            }
            Err(e) => {
                /* connection failed */
                tracing::debug!("IPC stream acceptation error {e}, might be stopped");
            }
        }
    }
}

pub fn listen_tcp_socket(
    target: TaosPool,
    socket: impl AsRef<str>,
    sender: Sender<String>,
    config: Option<OpcTableConfig>,
    cancel: CancellationToken,
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
                    let pool = target.clone();
                    let lock = sql_lock.clone();
                    let config = config.clone();
                    let se = sender.clone();
                    let cancel = cancel.clone();
                    runtime.spawn(async move {
                        let res = ipc_tcp_read(
                            addr.as_socket_ipv4().unwrap().to_string(),
                            pool,
                            stream,
                            lock,
                            config,
                            cancel,
                        )
                        .await;
                        if let Err(err) = res {
                            // panic!("{err:?}");
                            log::error!("ipc read err: {}", err);
                            let _ = se.send(err.to_string()).await;
                        }
                    });
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
