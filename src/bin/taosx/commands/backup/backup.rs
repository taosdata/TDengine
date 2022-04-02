use clap::Args;
use parquet::{
    basic::Compression,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use std::{
    fmt::Debug,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};
use taos::Taos;
use taosx::{Database, TaosOpts};
use thread_id;
use tokio::runtime::Builder;

use crate::commands::backup::{
    fetch::{fetch_database_info, fetch_stable_list, fetch_table_list, TableInfo},
    schema::{get_chunk_schema, get_database_schema, get_table_schema},
    serialize::{serialize_dbinfo, serialize_tableinfo},
};

use self::serialize::serialzie_data;

use super::restore::deserialize::deserialize_print_file;
mod fetch;
mod schema;
mod serialize;

#[derive(Debug, Args, Clone)]
/// Backup database or tables to specific files.
///
/// Basically, an alternative command to `taosdump`.
pub(crate) struct App {
    #[clap(short, long)]
    database: Option<String>,
    #[clap(short, long)]
    output: Option<String>,
    #[clap(short, long)]
    thread: Option<u32>,
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        let database = self.database.as_deref().unwrap_or("test");
        let db = String::from(database);
        let threads = self.thread.unwrap_or(1);
        let output = self.output.as_deref().unwrap_or("./");
        let path = PathBuf::from(output);

        log::info!("prepare parquet schema");
        let database_schema = get_database_schema();
        let table_schema = get_table_schema();
        let chunk_schema = get_chunk_schema();

        log::info!("start backup database info");

        let database = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fetch_database_info(db.clone()));

        dbg!(&database);

        Builder::new_multi_thread()
            .worker_threads(threads as usize)
            .enable_all()
            .build()
            .unwrap()
            .block_on(backup_database_info(
                database_schema,
                database,
                path.clone(),
            ));

        // let tableinfo_list = Builder::new_current_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(TableInfo::new(db.clone(), stable_list, common_list));

        let mut table_path = path.clone();
        table_path.push("table.info");
        fs::remove_dir_all(table_path.clone()).unwrap();
        fs::create_dir_all(table_path.clone()).unwrap();

        let (select_list, describe_list, total_table) = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fetch_table_list(db.clone()));
        log::debug!(
            "select list: {:?}; describe list {:?}",
            select_list,
            describe_list
        );

        // Builder::new_multi_thread()
        //     .worker_threads(threads as usize)
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(backup_table_info(
        //         table_schema,
        //         tableinfo_list,
        //         table_path.clone(),
        //     ));

        let mut block_path = path.clone();
        block_path.push("block");
        fs::remove_dir_all(block_path.clone()).unwrap();
        fs::create_dir_all(block_path.clone()).unwrap();

        let tables_per_thread = total_table / threads;
        let runtime = Builder::new_multi_thread()
            .worker_threads(threads as usize)
            .enable_all()
            .build()
            .unwrap();
        let mut handles = Vec::with_capacity(threads as _);

        let chunks: Vec<&[String]> = select_list.chunks(tables_per_thread as _).collect();
        for chunk in chunks {
            handles.push(runtime.spawn(backup_data(
                chunk_schema.clone(),
                chunk.to_owned(),
                block_path.clone(),
                db.clone(),
            )));
        }
        for handle in handles {
            runtime.block_on(handle).unwrap();
        }
    }
}

async fn backup_database_info(schema: Arc<Type>, database: Database, mut path: PathBuf) {
    path.push("db.info");
    let file = File::create(path.as_path()).unwrap();
    serialize_dbinfo(schema, database, file);
    let file = File::open(path.clone()).unwrap();
    deserialize_print_file(file);
}

async fn backup_table_info(schema: Arc<Type>, tableinfo_list: Vec<TableInfo>, mut path: PathBuf) {
    let thread_id = thread_id::get();
    path.push(format!("{}.parquet", thread_id));
    let file = File::create(path.as_path()).unwrap();
    serialize_tableinfo(schema.clone(), tableinfo_list, file).await;
    let file = File::open(path.clone()).unwrap();
    deserialize_print_file(file);
}

async fn backup_data(
    schema: Arc<Type>,
    tbname_list: Vec<String>,
    mut path: PathBuf,
    database: String,
) {
    let thread_id = thread_id::get();
    path.push(format!("{}.parquet", thread_id));
    let file = File::create(path.as_path()).unwrap();
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let taos = Taos::new("10.72.136.169", "root", "taosdata", database, 6030).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    for tbname in tbname_list {
        let res = taos
            .query(format!("select * from {}", tbname).as_str())
            .await
            .unwrap();
        let stream = res.fetch_block_stream();
        writer = serialzie_data(writer, &tbname, stream).await;
    }
    writer.close().unwrap();
    let file = File::open(path.clone()).unwrap();
    deserialize_print_file(file);
}
