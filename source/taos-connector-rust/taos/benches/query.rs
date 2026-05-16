use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use taos::*;
use tokio::runtime::Runtime;

async fn insert(taos: &Taos, tables: &[&str], _ts: i64, records: usize) {
    static mut TS: i64 = 1691726395000;
    for table in tables {
        for val in 0..records {
            let ts = unsafe { TS + val as i64 };
            taos.exec(format!("insert into {table} values ({ts}, {val})"))
                .await
                .unwrap();
        }
    }
    unsafe { TS += records as i64 };
}

fn bench_query(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let f = async move {
        let taos = TaosBuilder::from_dsn("taos:///")
            .unwrap()
            .build()
            .await
            .unwrap();
        let db = "db1";
        taos.exec(format!("create database if not exists {db}"))
            .await
            .unwrap();
        taos.exec("use db1").await.unwrap();
        taos.exec("create stable if not exists stb1 (ts timestamp, val int) tags(t1 int)")
            .await
            .unwrap();
        taos.exec("create table if not exists tb1 using stb1 tags(1) if not exists tb2 using stb1 tags(1)")
            .await.unwrap();
        taos
    };
    let taos = runtime.block_on(f);
    let p = (runtime, taos);
    for records in [1, 10, 20, 30, 40, 50, 75, 100] {
        c.bench_with_input(
            BenchmarkId::new("insert", format!("native-{}", records)),
            &p,
            |b, (rt, taos)| {
                // let now = chro
                b.to_async(rt)
                    .iter(|| insert(taos, &["tb1", "tb2"], 1691726395000, records));
            },
        );
    }

    let runtime = p.0;
    let f = async move {
        let taos = TaosBuilder::from_dsn("taos+ws:///")
            .unwrap()
            .build()
            .await
            .unwrap();
        let db = "db1";
        taos.exec(format!("create database if not exists {db}"))
            .await
            .unwrap();
        taos.exec("use db1").await.unwrap();
        taos.exec("create stable if not exists stb1 (ts timestamp, val int) tags(t1 int)")
            .await
            .unwrap();
        taos.exec("create table if not exists tb1 using stb1 tags(1) if not exists tb2 using stb1 tags(1)")
            .await.unwrap();
        taos
    };
    let taos = runtime.block_on(f);
    let p = (runtime, taos);
    for records in [1, 10, 20, 30, 40, 50, 75, 100] {
        c.bench_with_input(
            BenchmarkId::new("insert", format!("ws-{}", records)),
            &p,
            |b, (rt, taos)| {
                // let now = chro
                b.to_async(rt)
                    .iter(|| insert(taos, &["tb1", "tb2"], 1691726395000, records));
            },
        );
    }
    println!("------------------------------------------------------------------------");
}

criterion_group! {
    name = benches;
    config = Criterion::default().nresamples(20).sample_size(20);
    targets= bench_query
}

criterion_main!(benches);
