use criterion::{criterion_group, criterion_main, Criterion};
use taos::TaosBuilder;
use taos_query::prelude::sync::*;

fn criterion_benchmark(c: &mut Criterion) {
    let db = "bench_insert";
    let prepare_db = [
        format!("drop database if exists {db}"),
        format!("create database if not exists {db}"),
        format!("use {db}"),
        "CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)".to_string(),
    ];

    let mut t: i64 = 1688456534091;
    let mut sql = "INSERT INTO".to_string();
    for i in 0..333 {
        let table = format!(" d100{}", i);
        t += 1;
        let sub_sql = format!("{table}_1 USING meters TAGS ('California.SanFrancisco', 2) (ts, current) VALUES ({t}, 10.2)");
        sql += &sub_sql;
        t += 1;
        let sub_sql = format!("{table}_2 USING meters TAGS ('California.SanFrancisco', 2) (ts, voltage) VALUES ({t}, 219)");
        sql += &sub_sql;
        t += 1;
        let sub_sql = format!("{table}_3 USING meters TAGS ('California.SanFrancisco', 2) (ts, phase) VALUES ({t}, 0.32)");
        sql += &sub_sql;
    }
    println!("{}", sql);

    let dsn = "taos://localhost:6030";
    let native = TaosBuilder::from_dsn(dsn).unwrap().build().unwrap();

    native.exec_many(&prepare_db).unwrap();

    c.bench_function("native", |b| b.iter(|| native.exec(&sql).unwrap()));

    let dsn_ws = "taosws://localhost:6041";
    let ws = TaosBuilder::from_dsn(dsn_ws).unwrap().build().unwrap();

    ws.exec_many(&prepare_db).unwrap();

    c.bench_function("ws", |b| b.iter(|| ws.exec(&sql).unwrap()));
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
