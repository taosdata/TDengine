use criterion::{black_box, criterion_group, criterion_main, Criterion};

use taos::Taos;
use taos_sys::taos_free_result;

fn bench_query_sync(taos: &Taos) {
    let _ = taos.query_sync("select * from log.logs");
}

fn bench_query_sync2(taos: &Taos) {
    let _ = taos.query_sync2("select * from log.logs");
}

fn criterion_benchmark(c: &mut Criterion) {
    // Optionally include some setup
    let taos = Taos::new("localhost", "root", "taosdata", "", 0).unwrap();
    let mut group = c.benchmark_group("query - do nothing");
    use criterion::*;
    group.sampling_mode(SamplingMode::Flat);
    use tokio::runtime;

    let rt = runtime::Runtime::new().unwrap();

    group.bench_function("sync", |b| b.iter(|| bench_query_sync(&taos)));
    group.bench_function("sync with futures block_on", |b| {
        b.iter(|| bench_query_sync2(&taos))
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
