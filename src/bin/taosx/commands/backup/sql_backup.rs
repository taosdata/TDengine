use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use libtaos::{Field, Taos};
pub async fn backup_database_sql(taos: &Taos, db: &str) {
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

pub async fn backup_stable_sql(taos: &Taos, db: &str) -> Vec<String> {
    let mut stable_list = vec![];
    taos.use_database(db).await.unwrap();
    let res = taos.query("show stables").await.unwrap();
    if res.rows.len() == 0 {
        unreachable!("no stable found");
    } else {
        for row in res.rows {
            assert!(row.len() > 1);
            match row[0].clone() {
                Field::Binary(v) => stable_list.push(v.to_string()),
                _ => continue,
            }
        }
    }
    let filename = format!("./{}/stb.sql", db);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(filename)
        .unwrap();
    for stb in &stable_list {
        let res = taos
            .query(format!("show create table {}", stb).as_str())
            .await
            .unwrap();
        for row in res.rows {
            for field in row {
                match field {
                    Field::Binary(v) => {
                        if v == *stb {
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
    stable_list
}

pub async fn backup_table_sql(taos: &Taos, db: &str, stb: &str) -> Vec<String> {
    let mut tables_list = vec![];
    let res = taos
        .query(format!("select tbname from {}.{}", db, stb).as_str())
        .await
        .unwrap();
    if res.rows.len() == 0 {
        unreachable!("no table found in {}", stb);
    } else {
        for row in res.rows {
            assert!(row.len() > 0);
            match row[0].clone() {
                Field::Binary(v) => tables_list.push(v.to_string()),
                _ => continue,
            }
        }
    }
    let filename = format!("./{}/tb.sql", db);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(filename)
        .unwrap();
    for tb in &tables_list {
        let res = taos
            .query(format!("show create table {}", tb).as_str())
            .await
            .unwrap();
        for row in res.rows {
            for field in row {
                match field {
                    Field::Binary(v) => {
                        if v == *tb {
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
    tables_list
}
