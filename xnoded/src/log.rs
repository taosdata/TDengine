use anyhow::Context;
use taoslog::QidManager;
use taoslog::layer::TaosLayer;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::Args;

bitfield::bitfield! {
    pub struct Qid(u64);
    impl Debug;

    pub u64, session_id, inner_set_session_id: 55, 0;
    pub u8, instance_id, inner_set_instance_id: 63, 56;
}

impl Clone for Qid {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl QidManager for Qid {
    fn init() -> Self {
        let mut this = Self(0);
        this.inner_set_instance_id(80);
        this
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

pub fn init(args: &Args) -> anyhow::Result<()> {
    let mut builder = taoslog::writer::RollingFileAppender::builder(&args.log.path, "xnoded", 80);
    if let Some(compress) = args.log.compress {
        builder = builder.compress(compress);
    }
    if let Some(reserved_disk_size) = &args.log.reserved_disk_size {
        builder = builder.reserved_disk_size(reserved_disk_size);
    }
    if let Some(rotation_count) = args.log.rotation_count {
        builder = builder.rotation_count(rotation_count);
    }
    if let Some(keep_days) = args.log.keep_days {
        builder = builder.keep_days(keep_days);
    }
    if let Some(rotation_size) = &args.log.rotation_size {
        builder = builder.rotation_size(rotation_size);
    }
    let appender = builder.build().context("build log appender error")?;

    let level_filter = EnvFilter::builder()
        .with_default_directive(args.log.level.unwrap_or(LevelFilter::INFO).into())
        .from_env_lossy()
        .add_directive("h2=warn".parse()?);

    use tracing_subscriber::Layer;
    #[allow(unused_mut)]
    let mut layers = vec![taoslog::layer::TaosLayer::<Qid, _, _>::new(appender).boxed()];

    #[cfg(debug_assertions)]
    layers.push(
        TaosLayer::<Qid, _, _>::new(std::io::stdout)
            .with_ansi()
            .with_location()
            .boxed(),
    );

    tracing_subscriber::registry()
        .with(layers)
        .with(level_filter)
        .try_init()
        .context("init logger error")
}
