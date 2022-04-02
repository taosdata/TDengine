use clap::Args;
use std::fs::File;
use std::path::PathBuf;
use taosx::TaosOpts;
pub(crate) mod deserialize;
mod restore_parquet;

use self::deserialize::deserialize_print_file;
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

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        let database = self.database.as_deref().unwrap_or("test");
        let db = String::from(database);
        let threads = self.thread.unwrap_or(1);
        let input = self.input.as_deref().unwrap_or("./");
        let path = PathBuf::from(input);
        let mut db_path = path.clone();
        db_path.push("db.info");
        let file = File::open(db_path).unwrap();
        // deserialize_print_file(file);
        let mut table_path = path.clone();
        table_path.push("table.info");
        for element in table_path.read_dir().unwrap() {
            let filename = element.unwrap().path();
            if let Some(extension) = filename.extension() {
                if extension == "parquet" {
                    let file = File::open(filename).unwrap();
                    // deserialize_print_file(file);
                }
            }
        }
        // log::info!(
        //     "start restoring from {} to database {} with {} threads",
        //     source,
        //     db,
        //     threads
        // );

        // let opts = TaosOptions::new();
        // let pool = TaosPool::builder().build(opts).unwrap();
        // log::info!("start read database create sql");
        // Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(restore_sql(pool.clone(), &path, SqlLevel::Database));
        // log::info!("finish create database");
        // Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(
        //         pool.clone()
        //             .get()
        //             .unwrap()
        //             .query(format!("use {}", db.clone()).as_str()),
        //     )
        //     .unwrap();
        // log::info!("use database");
        // log::info!("start read stable create sql");
        // Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(restore_sql(pool.clone(), &path, SqlLevel::Stable));
        // log::info!("finish create stable(s)");
        // log::info!("start read table create sql");
        // Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(restore_sql(pool.clone(), &path, SqlLevel::Table));
        // log::info!("finish create table(s)");
        // let file_list = get_parquet_files(path.clone());
        // let opts = TaosOptions::new();
        // let pool = TaosPool::builder()
        //     .max_size(file_list.len() as u32)
        //     .build(opts)
        //     .unwrap();
        // let runtime = Builder::new_multi_thread()
        //     .worker_threads(threads as usize)
        //     .enable_all()
        //     .build()
        //     .unwrap();
        // let mut handles = Vec::with_capacity(file_list.len());
        // log::info!("read {} parquet files", file_list.len());
        // for i in 0..file_list.len() {
        //     handles.push(runtime.spawn(restore_parquet(
        //         pool.clone(),
        //         path.clone(),
        //         file_list[i].clone(),
        //         db.clone(),
        //     )));
        // }
        // for handle in handles {
        //     runtime.block_on(handle).unwrap();
        // }
        // log::info!("finish restoring database {}", db);
    }
}
