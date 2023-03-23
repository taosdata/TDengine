use std::{
    any::Any,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
};
use taos::{AsyncQueryable, Bindable, Dsn, Itertools, Stmt, TBuilder, Taos, TaosBuilder, TaosPool};
// use taosx_ipc::ack::{AckWriter, AckWriterBuilder};
use tokio::runtime::Runtime;
use tracing::{info, instrument};

use taosx_ipc::prelude::*;

async fn ipc_tcp_read(pool: TaosPool, stream: TcpStream) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    ipc_process(pool, ipc_reader, ipc_ack_writer).await
}

#[cfg(not(target_os = "windows"))]
async fn ipc_unix_read(
    pool: TaosPool,
    stream: std::os::unix::net::UnixStream,
) -> anyhow::Result<()> {
    let ipc_reader = IpcReader::new(&stream).unwrap();
    let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
    ipc_process(pool, ipc_reader, ipc_ack_writer).await
}
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
                taos.exec(&sql).await?;
            }
        }
        LushMessage::Insert(record) => {
            for record in record {
                *records += record.num_rows();
                // let data = record.to_column_views();
                let map_data = record.to_column_views_group_by_tablename();
                // dbg!(&map_data);
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
                    drop(stmt);
                }
            }
        }
    }
    Ok(())
}

async fn ipc_process<R: Read, W: Write>(
    pool: TaosPool,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
) -> anyhow::Result<()> {
    let taos = pool.get()?;
    let metadata = ipc_reader.metadata();
    let stream_type = *metadata.stream_type();
    if let Some(sql) = ipc_reader.metadata().init_sql_string() {
        info!("{sql}");
        // rt.block_on(taos.exec(&sql))?;
        taos.exec(&sql).await?;
    }
    let columns = ipc_reader.columns();
    let names = columns.iter().map(|n| format!("`{n}`")).join(",");
    let marks = std::iter::repeat('?').take(columns.len()).join(",");

    let mut records = 0;

    for record in ipc_reader {
        if let Ok(record) = record {
            match stream_type {
                StreamType::Line => todo!(),
                StreamType::Flat => todo!(),
                StreamType::Lush => consume_lush_record(
                    &taos,
                    *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                        std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
                    })
                    .unwrap(),
                    &names,
                    &marks,
                    &mut records,
                ),
                StreamType::Point => todo!(),
            }
            .await?;

            ipc_ack_writer.write_ok().unwrap();
        }
    }
    println!("finished, totally {records} rows");
    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub async fn listen_unix_socket(target: &TaosPool, socket: impl AsRef<Path>) -> anyhow::Result<()> {
    let path = socket.as_ref();
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    info!("listen on socket address: {}", path.display());

    // let pool = TaosBuilder::from_dsn("taos:///test3")?.pool()?;
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new unix client!: {:?}", addr);
                let pool = target.clone();
                tokio::spawn(async move { ipc_unix_read(pool, stream) });
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
            }
        }
    }
}

pub async fn listen_tcp_socket(target: &TaosPool, socket: impl AsRef<str>) -> anyhow::Result<()> {
    let tcp_addr = socket.as_ref();
    let listener = TcpListener::bind(tcp_addr).unwrap();
    info!("listen on socket address: {tcp_addr}");
    // let pool = TaosBuilder::from_dsn("taos:///test3")?.pool()?;
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                tracing::info!("new unix client!: {:?}", addr);
                let pool = target.clone();
                tokio::spawn(async move { ipc_tcp_read(pool, stream) });
            }
            Err(e) => {
                /* connection failed */
                tracing::error!("Connection error {e}");
            }
        }
    }
}
