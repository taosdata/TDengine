use clap::Args;
use taos::Taos;
use taosx::TaosOpts;
mod restore_parquet;
mod restore_sql;
use self::restore_sql::restore_sql;
#[derive(Debug, Args)]
/// Restore from a backup output directory.

pub(crate) struct App {
    name: Option<String>,
}
impl App {
    pub async fn run_with_taos_opts(&self, opts: &TaosOpts) {
        let host;
        let user;
        let pass;
        let db;
        let port;
        if let Some(h) = opts.host.as_deref() {
            host = h;
        } else {
            host = "localhost";
        }
        if let Some(u) = opts.username.as_deref() {
            user = u;
        } else {
            user = "root";
        }
        if let Some(p) = opts.password.as_deref() {
            pass = p;
        } else {
            pass = "taosdata";
        }
        if let Some(d) = opts.database.as_deref() {
            db = d;
        } else {
            db = "db";
        }
        if let Some(p) = opts.port {
            port = p;
        } else {
            port = 6030;
        }
        let taos = Taos::new(host, user, pass, "", port).unwrap();
        restore_sql(&taos, db, "db").await;
        taos.query(format!("use {}", db).as_str()).await.unwrap();
        restore_sql(&taos, db, "stb").await;
        restore_sql(&taos, db, "tb").await;
    }
}
