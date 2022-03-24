use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use taos::r2d2::TaosPool;

pub async fn restore_sql(pool: TaosPool, db: &str, filename: &str) {
    let file = File::open(format!("./{}/{}.sql", db, filename)).unwrap();
    let fin = BufReader::new(file);
    let taos = pool.get().unwrap();
    for line in fin.lines() {
        taos.query(&line.unwrap()).await.unwrap();
    }
}
