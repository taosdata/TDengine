use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use taos::r2d2::TaosPool;

use super::SqlLevel;

pub async fn restore_sql(pool: TaosPool, source: &PathBuf, level: SqlLevel) {
    let mut path = source.clone();
    path.push(match level {
        SqlLevel::Database => "db.sql",
        SqlLevel::Stable => "stb.sql",
        SqlLevel::Table => "tb.sql",
    });
    let file = File::open(path).unwrap();
    let fin = BufReader::new(file);
    let taos = pool.get().unwrap();
    for line in fin.lines() {
        taos.query(&line.unwrap()).await.unwrap();
    }
}
