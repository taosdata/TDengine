use std::io::BufRead;
use std::thread;

use crossbeam::sync::WaitGroup;
use rand::Rng;
use taos_log::layer::TaosLayer;
use taos_log::writer::RollingFileAppender;
use taos_log::QidManager;
use tracing::level_filters::LevelFilter;
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

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
    let rand_id: usize = rand::thread_rng().gen_range(100..999);
    let appender = RollingFileAppender::builder(".")
        .compress(true)
        .reserved_disk_size("1GB")
        .keep_days(10)
        .rotation_size("1KB")
        .stop_logging_threadhold(50)
        .build()
        .unwrap();

    tracing_subscriber::registry()
        .with(TaosLayer::<Qid>::new(appender).with_filter(LevelFilter::TRACE))
        .try_init()
        .unwrap();

    LogTracer::init().unwrap();

    let wg = WaitGroup::new();
    for _ in 0..1 {
        let wg = wg.clone();
        thread::spawn(move || {
            for _ in 0..1 {
                tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
                    tracing::trace!("outer trace example");
                    tracing::info!(a = "aaa", b = "bbb", process = rand_id, "outer example");

                    tracing::info_span!("inner").in_scope(|| {
                        tracing::debug!("inner info log example");
                        tracing::error!(c = "ccc", d = "ddd", process = rand_id, "inner example");
                    });
                });
            }
            drop(wg)
        });
    }
    wg.wait();
    println!("finish bench");

    let stdin = std::io::stdin();
    let mut stdin_lock = stdin.lock();
    loop {
        tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
            tracing::info!(a = "aaa", b = "bbb", process = rand_id, "outer example");
            log::info!("this is log info log");

            tracing::info_span!("inner").in_scope(|| {
                tracing::debug!("inner info log example");
                tracing::error!(c = "ccc", d = "ddd", process = rand_id, "inner example");
                tracing::info!(e = "eee");
            });
        });

        stdin_lock.read_line(&mut String::new()).unwrap();
    }
}
