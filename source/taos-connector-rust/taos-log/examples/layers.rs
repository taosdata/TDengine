use std::io::BufRead;

use taos_log::layer::TaosLayer;
use taos_log::writer::RollingFileAppender;
use taos_log::QidManager;
use tracing::level_filters::LevelFilter;
use tracing_log::LogTracer;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

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

fn main() {
    let mut layers = Vec::with_capacity(2);
    let appender = RollingFileAppender::builder(".")
        .compress(true)
        .reserved_disk_size("1GB")
        .rotation_size("1GB")
        .build()
        .unwrap();
    layers.push(
        TaosLayer::<Qid>::new(appender)
            .with_filter(LevelFilter::INFO)
            .boxed(),
    );

    if cfg!(debug_assertions) {
        layers.push(
            TaosLayer::<Qid, _, _>::new(std::io::stdout)
                .with_location()
                .with_filter(LevelFilter::INFO)
                .boxed(),
        );
    }

    tracing_subscriber::registry().with(layers).init();
    LogTracer::init().unwrap();

    let stdin = std::io::stdin();
    let mut stdin_lock = stdin.lock();
    loop {
        tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
            tracing::info!(a = "aaa", b = "bbb", "outer example");
            log::info!("this is log info log");

            tracing::info_span!("inner").in_scope(|| {
                tracing::trace!("trace example");
                tracing::warn!("warn example");
                tracing::debug!("inner info log example");
                tracing::error!(c = "ccc", d = "ddd", "inner example");
            });
        });

        stdin_lock.read_line(&mut String::new()).unwrap();
    }
}
