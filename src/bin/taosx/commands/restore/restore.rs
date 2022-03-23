use clap::Args;
use taos::Taos;
use taosx::TaosOpts;
mod restore_parquet;
mod restore_sql;
use self::restore_parquet::{get_parquet_files, restore_parquet};
use self::restore_sql::restore_sql;
#[derive(Debug, Args)]
/// Restore from a backup output directory.

pub(crate) struct App {
    name: Option<String>,
}
impl App {
    pub async fn run_with_taos_opts(&self, opts: &TaosOpts) {
        let host = opts.host.as_deref().unwrap_or("localhost");
        let user = opts.username.as_deref().unwrap_or("root");
        let pass = opts.password.as_deref().unwrap_or("taosdata");
        let db = opts.database.as_deref().unwrap_or("");
        let port = opts.port.unwrap_or(6030);
        let taos = Taos::new(host, user, pass, "", port).unwrap();
        restore_sql(&taos, db, "db").await;
        taos.query(format!("use {}", db).as_str()).await.unwrap();
        restore_sql(&taos, db, "stb").await;
        restore_sql(&taos, db, "tb").await;
        let file_list = get_parquet_files(db).await;
        restore_parquet(&taos, db, &file_list, 0, file_list.len() as u32).await;
    }
}
