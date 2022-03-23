use std::path::PathBuf;

use clap::Args;
use libtaos::Taos as OldTaos;
use taos::Taos;
use taosx::TaosOpts;
use tokio::runtime::Builder;

use self::{
    backup_parquet::{backup_data_parquet, generate_parquet_schema},
    backup_sql::{backup_database_sql, backup_stable_sql, backup_table_sql},
};

mod backup_parquet;
mod backup_sql;

#[derive(Debug, Args)]
/// Backup database or tables to specific files.
///
/// Basically, an alternative command to `taosdump`.
pub(crate) struct App {
    #[clap(short, long, env = "BACKUP_DATABASE", group = "taos-backup")]
    name: Option<String>,
    #[clap(short, long, env = "BACKUP_TARGET", group = "taos-backup")]
    target: Option<PathBuf>,
    #[clap(short, long, env = "BACKUP_THREAD", group = "taos-backup")]
    thread: Option<u32>,
}
impl App {
    pub async fn run_with_taos_opts(&self, opts: &TaosOpts) {
        let host = opts.host.as_deref().unwrap_or("localhost");
        let user = opts.username.as_deref().unwrap_or("root");
        let pass = opts.password.as_deref().unwrap_or("taosdata");
        let db = self.name.as_deref().unwrap_or("");
        let port = opts.port.unwrap_or(6030);
        let default_path = PathBuf::from(format!("./{}", db));
        let target = self.target.as_ref().unwrap_or(&default_path);
        let threads = self.thread.unwrap_or(1);
        let taos = OldTaos::new(host, user, pass, "", port).unwrap();
        backup_database_sql(&taos, db, target).await;
        let stable_list = backup_stable_sql(&taos, db, target).await;
        for stb in stable_list {
            let table_list = backup_table_sql(&taos, db, &stb, target).await;
            let total_table_num = table_list.len() as u32;
            let new_taos = Taos::new(host, user, pass, db, port).unwrap();
            let schema = generate_parquet_schema(&taos, db, &stb).await;
            let runtime = Builder::new_multi_thread()
                .worker_threads(threads as usize)
                .enable_all()
                .build()
                .unwrap();
            let mut handles = Vec::with_capacity(table_list.len());
            for i in 0..table_list.len() {
                handles.push(runtime.spawn(backup_data_parquet(
                    db,
                    table_list[i].as_str(),
                    schema.clone(),
                    target,
                )));
            }
            for handle in handles {
                runtime.block_on(handle).unwrap();
            }
        }
    }
}
