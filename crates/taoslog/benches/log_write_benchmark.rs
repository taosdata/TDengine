use criterion::{criterion_group, criterion_main, Criterion};
use taoslog::{layer::TaosLayer, writer::RollingFileAppender, QidManager};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[derive(Clone)]
struct Qid(u64);

impl QidManager for Qid {
    fn init() -> Self {
        Self(9223372036854775807)
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let appender = RollingFileAppender::builder(".", "taosx", 16)
        .compress(false)
        .reserved_disk_size("1GB")
        .rotation_count(10)
        .keep_days(10)
        .rotation_size("1GB")
        .stop_logging_threadhold(50)
        .build()
        .unwrap();

    tracing_subscriber::registry()
        .with(TaosLayer::<Qid>::new(appender).with_filter(LevelFilter::TRACE))
        .try_init()
        .unwrap();
    c.bench_function("log-write", |b| {
        b.iter(|| {
            tracing::info!(
                ip = "127.0.0.1",
                port = 8080,
                batch_id = 100,
                task_id = 1,
                name = "flat write",
                "this is a benchmark log with multiple kvs, this is a benchmark log with multiple kvs, this is a benchmark log with multiple kvs"
            );
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
