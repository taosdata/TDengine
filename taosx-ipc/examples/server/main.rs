use std::{path::Path, net::{TcpListener, TcpStream}, io::{Write, Read}};
use taos::{AsyncQueryable, Bindable, Dsn, Itertools, Stmt, TBuilder, TaosBuilder};
use taosx_ipc::ack::{AckWriterBuilder, AckWriter};
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

fn ipc_test<R: Read, W: Write>(ipc_reader: IpcReader<R>, mut ipc_ack_writer: AckWriter<W>) -> anyhow::Result<()>
 {
    let dsn = std::env::var("TAOSX_TARGET").unwrap_or("taos:///test".to_string());
    let mut dsn: Dsn = dsn.parse()?;
    let builder = TaosBuilder::from_dsn(&dsn).unwrap();

    let taos = builder.build().unwrap_or_else(|e| {
        info!("connect error: {}", e);
        let subject = dsn.subject.take();
        let new_builder = TaosBuilder::from_dsn(&dsn).unwrap();
        let taos = new_builder.build().unwrap();
        taos.exec_sync(format!("create database `{}`", subject.unwrap()))
            .unwrap();
        builder.build().unwrap()
    });

    // let (reader, writer) = stream.pair();
    // stream.set_nonblocking(true).unwrap();

    
    let metadata = ipc_reader.metadata();
    dbg!(metadata);
    if let Some(sql) = ipc_reader.metadata().init_sql_string() {
        info!("{sql}");
        taos.exec_sync(&sql).unwrap();
    }
    let columns = ipc_reader.columns();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");
    
    let mut records = 0;

    let mut stmt = Stmt::init(&taos)?;
    info!("init stmt");
    let sql = format!("insert into ? ({names}) values({marks})");
    info!("prepare with sql: {sql}");
    stmt.prepare(&sql).unwrap();
    info!("prepare");
    for record in ipc_reader {
        dbg!(&record);
        if let Ok(record) = record {
            for record in record {
                records += record.num_rows();
                let data = record.to_column_views();

                if let Err(err) = stmt.set_tbname(record.table()) {
                    tracing::warn!("table name `{}` error {err}", record.table());
                    if let Some(tb) = record.meta_sql() {
                        info!("sql: {tb}");
                        taos.exec_sync(&tb).unwrap();
                        stmt.set_tbname(record.table()).unwrap();
                    }
                }
                dbg!(&data);
                stmt.bind(data.as_slice()).unwrap();
                stmt.add_batch().unwrap();
                let n = stmt.execute().unwrap();
                info!("written {n} records");
                dbg!(&record);
                dbg!(&records);
            }
            ipc_ack_writer.write_ok().unwrap();
        }
    }
    println!("finished, totally {records} rows");
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
                tracing::info!("new client!: {:?}", addr);
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
    let listener = TcpListener::bind("0.0.0.0:7890").unwrap();
    info!("listen on socket address: 0.0.0.0:7890");
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new client!: {:?}", addr);
                std::thread::spawn(|| ipc_windows_read(stream).unwrap());
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
            }
        }
    }
}

#[tokio::main]
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
    let handle = std::thread::spawn(listen_unix_socket);

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
    Ok(())
}
