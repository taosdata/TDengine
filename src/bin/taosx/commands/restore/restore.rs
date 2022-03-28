use clap::Args;
use std::path::PathBuf;
use taos::r2d2::TaosPool;
use taos::TaosOptions;
use taosx::TaosOpts;
use tokio::runtime::Builder;
mod restore_parquet;
mod restore_sql;
use self::restore_parquet::{get_parquet_files, restore_parquet};
use self::restore_sql::restore_sql;
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

pub enum SqlLevel {
    Database,
    Stable,
    Table,
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        let database = self.database.as_deref().unwrap_or("");
        let db = String::from(database);
        let threads = self.thread.unwrap_or(1);
        let source = self.input.as_deref().unwrap_or("./");
        log::info!(
            "start restoring from {} to database {} with {} threads",
            source,
            db,
            threads
        );
        let path = PathBuf::from(source);
        let opts = TaosOptions::new();
        let pool = TaosPool::builder().build(opts).unwrap();
        log::info!("start read database create sql");
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(restore_sql(pool.clone(), &path, SqlLevel::Database));
        log::info!("finish create database");
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(
                pool.clone()
                    .get()
                    .unwrap()
                    .query(format!("use {}", db.clone()).as_str()),
            )
            .unwrap();
        log::info!("use database");
        log::info!("start read stable create sql");
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(restore_sql(pool.clone(), &path, SqlLevel::Stable));
        log::info!("finish create stable(s)");
        log::info!("start read table create sql");
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(restore_sql(pool.clone(), &path, SqlLevel::Table));
        log::info!("finish create table(s)");
        let file_list = get_parquet_files(path.clone());
        let opts = TaosOptions::new();
        let pool = TaosPool::builder()
            .max_size(file_list.len() as u32)
            .build(opts)
            .unwrap();
        let runtime = Builder::new_multi_thread()
            .worker_threads(threads as usize)
            .enable_all()
            .build()
            .unwrap();
        let mut handles = Vec::with_capacity(file_list.len());
        log::info!("read {} parquet files", file_list.len());
        for i in 0..file_list.len() {
            handles.push(runtime.spawn(restore_parquet(
                pool.clone(),
                path.clone(),
                file_list[i].clone(),
                db.clone(),
            )));
        }
        for handle in handles {
            runtime.block_on(handle).unwrap();
        }
        log::info!("finish restoring database {}", db);
    }
}
