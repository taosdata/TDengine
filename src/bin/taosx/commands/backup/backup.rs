use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use clap::Args;
use libtaos::{Field, Taos as OldTaos};
// use taos::Taos;
use taosx::TaosOpts;

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
        backup_table_sql(&taos, db, true).await;
        backup_table_sql(&taos, db, false).await;
    }
}

async fn backup_database_sql(taos: &OldTaos, db: &str) {
    let res = taos
        .query(format!("show create database {}", db.to_string()).as_str())
        .await
        .unwrap();
    let filename = format!("./{}/db.sql", db);
    let path = Path::new(filename.as_str());
    let mut file = fs::File::create(&path).unwrap();
    for row in res.rows {
        for field in row {
            match field {
                Field::Binary(v) => {
                    if v == db {
                        continue;
                    } else {
                        file.write(&v).unwrap();
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

async fn backup_table_sql(taos: &OldTaos, db: &str, is_stable: bool) {
    let mut tables = vec![];
    taos.use_database(db).await.unwrap();
    let res;
    if is_stable {
        res = taos.query("show stables").await.unwrap();
    } else {
        res = taos.query("show tables").await.unwrap();
    };
    if res.rows.len() == 0 {
        println!("no stable/table found!");
        return;
    } else {
        for row in res.rows {
            assert!(row.len() > 1);
            match row[0].clone() {
                Field::Binary(v) => tables.push(v),
                _ => continue,
            }
        }
    }
    for tb in tables {
        let res = taos
            .query(format!("show create table {}", tb).as_str())
            .await
            .unwrap();
        let filename;
        if is_stable {
            filename = format!("./{}/stb.sql", db);
        } else {
            filename = format!("./{}/tb.sql", db);
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(filename)
            .unwrap();
        for row in res.rows {
            for field in row {
                match field {
                    Field::Binary(v) => {
                        if v == tb {
                            continue;
                        } else {
                            let buf = v.to_string() + ";\n";
                            file.write(&buf.as_bytes()).unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
