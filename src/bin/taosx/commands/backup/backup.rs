use std::path::PathBuf;

use clap::Args;
use libtaos::Taos as OldTaos;
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
    #[clap(short, long, env = "BACKUP_DATABASE", group = "taos-backup")]
    name: Option<String>,
    #[clap(short, long, env = "BACKUP_TARGET", group = "taos-backup")]
    target: Option<String>,
    #[clap(short, long, env = "BACKUP_THREAD", group = "taos-backup")]
    thread: Option<u32>,
}

impl App {
    pub fn run_with_taos_opts(&'static self, opts: &TaosOpts) {
        let host = opts.host.as_deref().unwrap_or("localhost");
        let user = opts.username.as_deref().unwrap_or("root");
        let pass = opts.password.as_deref().unwrap_or("taosdata");
        let db = self.name.as_deref().unwrap_or("");
        let port = opts.port.unwrap_or(6030);
        let target = self.target.as_deref().unwrap_or("./");
        let path = PathBuf::from(target);
        let threads = self.thread.unwrap_or(1);
        let taos = OldTaos::new(host, user, pass, "", port).unwrap();
        async {
            backup_database_sql(&taos, db, &path).await;
        };

        let stable_list = Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(backup_stable_sql(&taos, db, &path));
        for stb in stable_list {
            let table_list = Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(backup_table_sql(&taos, db, &stb, &path));
            let schema = Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(generate_parquet_schema(&taos, db, &stb));
            let runtime = Builder::new_multi_thread()
                .worker_threads(threads as usize)
                .enable_all()
                .build()
                .unwrap();
            let mut handles = Vec::with_capacity(table_list.len());
            for i in 0..table_list.len() {
                handles.push(runtime.spawn(backup_data_parquet(
                    db,
                    table_list[i].clone(),
                    schema.clone(),
                    path.clone(),
                )));
            }
            for handle in handles {
                runtime.block_on(handle).unwrap();
            }
        }
    }
}
