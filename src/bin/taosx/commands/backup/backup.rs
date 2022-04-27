use clap::Args;
use futures::Future;
use parquet::{basic::Compression, schema::types::Type};
use std::{
    fmt::Debug,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::Arc,
};
use taos::TaosOptions;
use taosx::{Database, TaosOpts};
use thread_id;
use tokio::runtime::Builder;

use crate::commands::backup::{
    fetch::{fetch_database_info, fetch_table_list},
    schema::TaosParquetSchema,
};

use self::{fetch::fetch_stable_tag_buffer, serialize::Serialize};

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

fn allocate_task<Fut: 'static>(
    table_list: Vec<String>,
    schema: Arc<Type>,
    dir: PathBuf,
    path: &str,
    mut threads: u32,
    db: String,
    f: impl Fn(Arc<Type>, Vec<String>, PathBuf, String) -> Fut,
) where
    Fut: Future<Output = ()> + std::marker::Send,
{
    let mut own_path = dir;
    own_path.push(path);
    fs::create_dir(own_path.clone()).unwrap_or_else(|_| {
        fs::remove_dir_all(own_path.clone()).unwrap_or_else(|_| {
            fs::remove_dir(own_path.clone()).unwrap();
        });
        fs::create_dir(own_path.clone()).unwrap();
    });
    if threads > table_list.len() as _ {
        threads = table_list.len() as _;
    }
    let tables_per_thread = if table_list.len() as u32 % threads == 0 {
        table_list.len() as u32 / threads
    } else {
        table_list.len() as u32 / threads + 1
    };

    let mut handles = Vec::with_capacity(threads as _);
    let chunks: Vec<&[String]> = table_list.chunks(tables_per_thread as _).collect();
    threads = chunks.len() as _;

    log::info!(
        "{} threads each deal at most {} tables",
        threads,
        tables_per_thread
    );

    let runtime = Builder::new_multi_thread()
        .worker_threads(threads as usize)
        .enable_all()
        .build()
        .unwrap();

    for chunk in chunks {
        handles.push(runtime.spawn(f(
            schema.clone(),
            chunk.to_owned(),
            own_path.clone(),
            db.clone(),
        )));
    }
    for handle in handles {
        runtime.block_on(handle).unwrap();
    }
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        log::info!("prepare config options");

        let database = self.database.as_deref().unwrap_or("test");
        let db = String::from(database);
        let threads = self.thread.unwrap_or(1);
        let output = self.output.as_deref().unwrap_or("./");
        let path = PathBuf::from(output);

        log::info!("prepare parquet schema");

        let schema = TaosParquetSchema::default().build();

        log::info!("start backup database info");

        let database = fetch_database_info(db.clone());

        backup_database(database, path.clone());

        log::info!("finish backup database info");

        let (select_list, describe_list, stable_list) = fetch_table_list(db.clone());

        log::info!(
            "select list: {:?}; describe list {:?}; stable_list{:?}",
            select_list,
            describe_list,
            stable_list
        );

        log::info!("start backup table meta info");
        allocate_task(
            describe_list,
            schema.clone(),
            path.clone(),
            "table.info",
            threads,
            db.clone(),
            backup_table_schema,
        );
        log::info!("finish backup table meta info");
        log::info!("start backup table tags");
        allocate_task(
            stable_list,
            schema.clone(),
            path.clone(),
            "tags",
            threads,
            db.clone(),
            backup_stable_tags,
        );
        log::info!("finish backup table tags");
        log::info!("start backup data");
        allocate_task(select_list, schema, path, "chunk", threads, db, backup_data);
        log::info!("finish backup data");
    }
}

fn backup_database(database: Database, mut path: PathBuf) {
    path.push("db.info");
    let mut file = File::create(path.as_path()).unwrap();
    let j = serde_json::to_string_pretty(&database).unwrap();
    file.write_all(j.as_bytes()).unwrap();
}

async fn backup_table_schema(
    schema: Arc<Type>,
    table_list: Vec<String>,
    mut path: PathBuf,
    db: String,
) {
    let thread_id = thread_id::get();
    path.push(format!("{}.parquet", thread_id));
    let file = File::create(path.as_path()).unwrap();
    let taos = TaosOptions::new().database(db.clone()).build().unwrap();
    let mut serialize = Serialize::new(file, Compression::SNAPPY, schema);
    for tbname in table_list {
        let describe = taos.describe(&tbname).await.unwrap();
        serialize.serialze_table_meta(&tbname, describe);
    }
}

async fn backup_stable_tags(
    schema: Arc<Type>,
    table_list: Vec<String>,
    mut path: PathBuf,
    database: String,
) {
    let thread_id = thread_id::get();
    path.push(format!("{}.parquet", thread_id));
    let file = File::create(path.as_path()).unwrap();
    let mut serialize = Serialize::new(file, Compression::SNAPPY, schema);
    let taos = TaosOptions::new()
        .database(database.clone())
        .build()
        .unwrap();
    for tbname in table_list {
        let tag_buffer = fetch_stable_tag_buffer(database.clone(), tbname.clone()).await;
        let res = taos
            .query(format!("select tbname{} from {}", tag_buffer, tbname).as_str())
            .await
            .unwrap();
        let stream = res.fetch_block_stream();
        serialize.serialize_tag(&tbname, stream).await;
    }
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
    let taos = TaosOptions::new().database(database).build().unwrap();
    let mut serialize = Serialize::new(file, Compression::SNAPPY, schema);
    for tbname in tbname_list {
        let res = taos
            .query(format!("select * from {}", tbname).as_str())
            .await
            .unwrap();
        let stream = res.fetch_block_stream();
        serialize.serialize_data(&tbname, stream).await;
    }
}
