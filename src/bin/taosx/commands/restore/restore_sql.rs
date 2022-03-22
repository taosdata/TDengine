use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use taos::Taos;

pub async fn restore_sql(taos: &Taos, db: &str, filename: &str) {
    let file = File::open(format!("./{}/{}.sql", db, filename)).unwrap();
    let fin = BufReader::new(file);
    for line in fin.lines() {
        taos.query(&line.unwrap()).await.unwrap();
    }
}
