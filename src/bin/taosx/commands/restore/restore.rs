use clap::Args;
use futures::Future;
use std::path::PathBuf;
use taos::TaosOptions;
use taosx::TaosOpts;
use tokio::runtime::Builder;
pub(crate) mod deserialize;

use self::deserialize::{
    deserialize_data, deserialize_database, deserialize_table_info, deserialzie_tags,
};
#[derive(Debug, Args)]
/// Restore from a backup output directory.

pub(crate) struct App {
    #[clap(short, long)]
    database: Option<String>,
    #[clap(short, long)]
    input: Option<String>,
    #[clap(short, long)]
    thread: Option<u32>,
}

fn allocate_task<Fut: 'static>(
    db: String,
    mut dir: PathBuf,
    path: &str,
    mut threads: u32,
    f: impl Fn(Vec<String>, String) -> Fut,
) where
    Fut: Future<Output = ()> + std::marker::Send,
{
    dir.push(path);
    let mut filelist = vec![];
    for element in dir.read_dir().unwrap() {
        let filename = element.unwrap().path();
        if let Some(extension) = filename.extension() {
            if extension == "parquet" {
                filelist.push(filename.to_str().unwrap().to_string());
            }
        }
    }
    if threads > filelist.len() as _ {
        threads = filelist.len() as _;
    }
    let file_per_threads = filelist.len() as u32 / threads;
    log::info!(
        "{} threads each deal with at most {} files",
        threads,
        file_per_threads
    );

    let runtime = Builder::new_multi_thread()
        .worker_threads(threads as usize)
        .enable_all()
        .build()
        .unwrap();

    let mut handles = Vec::with_capacity(threads as _);
    let chunks: Vec<&[String]> = filelist.chunks(file_per_threads as _).collect();
    for chunk in chunks {
        handles.push(runtime.spawn(f(chunk.to_owned(), db.clone())))
    }
    for handle in handles {
        runtime.block_on(handle).unwrap();
    }
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        let threads = self.thread.unwrap_or(1);
        let input = self.input.as_deref().unwrap_or("./");
        let path = PathBuf::from(input);

        let db = restore_database(path.clone());

        allocate_task(
            db.clone(),
            path.clone(),
            "table.info",
            threads,
            restore_table_info,
        );

        allocate_task(db.clone(), path.clone(), "tags", threads, restore_tags);

        allocate_task(db.clone(), path.clone(), "chunk", threads, restore_data);
    }
}

fn restore_database(mut path: PathBuf) -> String {
    path.push("db.info");
    let taos = TaosOptions::new().host("10.72.136.169").build().unwrap();
    let database = deserialize_database(path);
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            taos.query(format!("drop database if exists {}", database.name))
                .await
                .unwrap()
        });
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            taos.query(format!("create database if not exists {}", database.name))
                .await
                .unwrap()
        });

    database.name
}

async fn restore_table_info(filelist: Vec<String>, db: String) {
    let taos = TaosOptions::new()
        .database(db)
        .host("10.72.136.169")
        .build()
        .unwrap();
    for file in filelist {
        let create_list = deserialize_table_info(file).await;
        for sql in create_list {
            taos.query(sql).await.unwrap();
        }
    }
}

async fn restore_tags(filelist: Vec<String>, db: String) {
    let taos = TaosOptions::new()
        .database(db)
        .host("10.72.136.169")
        .build()
        .unwrap();
    for file in filelist {
        let create_list = deserialzie_tags(file).await;
        for sql in create_list {
            taos.query(sql).await.unwrap();
        }
    }
}

async fn restore_data(filelist: Vec<String>, db: String) {
    for file in filelist {
        deserialize_data(file, db.clone()).await;
    }
}
