use std::path::PathBuf;

use clap::Args;
use libtaos::Taos as OldTaos;
use taos::{r2d2::TaosPool, TaosOptions};
use taosx::TaosOpts;
use tokio::runtime::Builder;

use self::{
    backup_parquet::{backup_data_parquet, generate_parquet_schema},
    backup_sql::{backup_database_sql, backup_stable_sql, backup_table_sql},
};

mod backup_parquet;
mod backup_sql;

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
    pub fn run_with_taos_opts(&self, opts: &TaosOpts) {
        let host = opts.host.as_deref().unwrap_or("localhost");
        let user = opts.username.as_deref().unwrap_or("root");
        let pass = opts.password.as_deref().unwrap_or("taosdata");
        let port = opts.port.unwrap_or(6030);
        let target = self.output.as_deref().unwrap_or("./");
        let path = PathBuf::from(target);
        let database = self.database.as_deref().unwrap_or("");
        let db = String::from(database);
        let threads = self.thread.unwrap_or(1);
        log::info!(
            "start backup database {} to {} with {} threads",
            db,
            target,
            threads
        );
        let taos = OldTaos::new(host, user, pass, "", port).unwrap();
        log::info!("start backup database sql");
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(backup_database_sql(&taos, db.clone(), &path));
        log::info!("finish backup database sql");
        log::info!("start backup stable sql");
        let stable_list = Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(backup_stable_sql(&taos, db.clone(), &path));
        log::info!("finish backup stable sql");
        for stb in stable_list {
            log::info!("start backup table sql from stable {}", stb);
            let table_list = Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(backup_table_sql(&taos, db.clone(), &stb, &path));
            log::info!("finish backup table sql from stable {}", stb);
            let opts = TaosOptions::new();
            let pool = TaosPool::builder()
                .max_size(table_list.len() as u32)
                .build(opts)
                .unwrap();
            log::info!("start generate parquet schema for stable {}", stb);
            let schema = Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(generate_parquet_schema(&taos, db.clone(), &stb));
            log::info!("finish generate parquet schema for stable {}", stb);
            let runtime = Builder::new_multi_thread()
                .worker_threads(threads as usize)
                .enable_all()
                .build()
                .unwrap();
            let mut handles = Vec::with_capacity(table_list.len());
            log::info!("start backup table data to parquet");
            for i in 0..table_list.len() {
                handles.push(runtime.spawn(backup_data_parquet(
                    pool.clone(),
                    db.clone(),
                    table_list[i].clone(),
                    schema.clone(),
                    path.clone(),
                )));
            }
            for handle in handles {
                runtime.block_on(handle).unwrap();
            }
            log::info!("finish backup table data to parquet");
        }
    }
}
