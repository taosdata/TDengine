use std::path::Path;
use taos::{AsyncQueryable, Bindable, Itertools, Stmt, TBuilder};
use taosx_ipc::ack::AckWriterBuilder;
use tracing::{info, instrument};

use taosx_ipc::prelude::*;

// shadow_rs::shadow!(build);

#[instrument]
async fn hello() -> &'static str {
    "Hello world!"
}

fn ipc_test(stream: std::os::unix::net::UnixStream) -> anyhow::Result<()> {
    let taos = taos::TaosBuilder::from_dsn("taos:///x-test-arrow")?;
    let taos = taos.build()?;

    // let (reader, writer) = stream.pair();
    // stream.set_nonblocking(true).unwrap();

    let ipc_reader = IpcReader::new(&stream).unwrap();
    let metadata = ipc_reader.metadata();
    dbg!(metadata);
    if let Some(sql) = ipc_reader.metadata().init_sql_string() {
        info!("{sql}");
        taos.exec_sync(&sql).unwrap();
    }
    let columns = ipc_reader.columns();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");
    let mut ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
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
                std::thread::spawn(|| ipc_test(stream));
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

    let handle = std::thread::spawn(listen_unix_socket);

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
