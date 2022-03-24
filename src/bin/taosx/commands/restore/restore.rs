use clap::Args;
use taos::Taos;
use taosx::TaosOpts;
use tokio::runtime::Builder;
mod restore_parquet;
mod restore_sql;
use self::restore_parquet::{get_parquet_files, restore_parquet};
use self::restore_sql::restore_sql;
#[derive(Debug, Args)]
/// Restore from a backup output directory.

pub(crate) struct App {
    #[clap(short, long, env = "RESTORE_DATABASE", group = "taos-restore")]
    name: Option<String>,
    #[clap(short, long, env = "RESTORE_TARGET", group = "taos-restore")]
    source: Option<String>,
    #[clap(short, long, env = "RESTORE_THREAD", group = "taos-restore")]
    thread: Option<u32>,
}
impl App {
    pub fn run_with_taos_opts(&'static self, opts: &TaosOpts) {
        let host = opts.host.as_deref().unwrap_or("localhost");
        let user = opts.username.as_deref().unwrap_or("root");
        let pass = opts.password.as_deref().unwrap_or("taosdata");
        let db = self.name.as_deref().unwrap_or("");
        let port = opts.port.unwrap_or(6030);
        let taos = Taos::new(host, user, pass, "", port).unwrap();
        let threads = self.thread.unwrap_or(1);
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(restore_sql(&taos, db, "db"));
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(taos.query(format!("use {}", db).as_str()))
            .unwrap();
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(restore_sql(&taos, db, "stb"));
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(restore_sql(&taos, db, "tb"));
        let file_list = get_parquet_files(db);
        let runtime = Builder::new_multi_thread()
            .worker_threads(threads as usize)
            .enable_all()
            .build()
            .unwrap();
        let mut handles = Vec::with_capacity(file_list.len());
        for i in 0..file_list.len() {
            handles.push(runtime.spawn(restore_parquet(db, file_list[i].clone())));
        }
        for handle in handles {
            runtime.block_on(handle).unwrap();
        }
    }
}
