use std::fs::{self};

use clap::Args;
use libtaos::Taos as OldTaos;
use taos::Taos;
use taosx::TaosOpts;

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
        let taos = OldTaos::new(host, user, pass, db, port).unwrap();
        fs::create_dir(format!("./{}", db)).unwrap();
        backup_database_sql(&taos, db).await;
        let stable_list = backup_stable_sql(&taos, db).await;
        for stb in stable_list {
            let table_list = backup_table_sql(&taos, db, &stb).await;
            let new_taos = Taos::new(host, user, pass, db, port).unwrap();
            let schema = generate_parquet_schema(&taos, db, &stb).await;
            for tb in table_list {
                backup_data_parquet(&new_taos, db, &tb, schema.clone()).await;
            }
        }
    }
}
