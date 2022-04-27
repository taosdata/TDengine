use criterion::{criterion_group, criterion_main, Criterion};

use taos::{Taos, TaosOptions};

fn bench_query_sync0(taos: &Taos) {
    use taos::prelude::AsyncQueryable;
    let _ = taos.query_sync("select * from log.logs");
}

fn bench_query_sync1(taos: &Taos) {
    use taos::prelude::sync::Queryable;
    let _ = taos.query("select * from log.logs");
}

fn criterion_benchmark(c: &mut Criterion) {
    // Optionally include some setup
    let taos = TaosOptions::new().build().unwrap();
    let mut group = c.benchmark_group("query - do nothing");
    use criterion::*;
    group.sampling_mode(SamplingMode::Linear);
    group.measurement_time(std::time::Duration::from_secs(15));
    use tokio::runtime;

    let _rt = runtime::Runtime::new().unwrap();

    group.bench_function("async", |b| b.iter(|| bench_query_sync0(&taos)));
    group.bench_function("sync", |b| {
        b.iter(|| bench_query_sync1(&taos))
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
