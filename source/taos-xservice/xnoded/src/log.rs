use anyhow::Context;
use taoslog::QidManager;
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

fn build_env_filter(level: LevelFilter) -> anyhow::Result<EnvFilter> {
    build_env_filter_from_env(level, "RUST_LOG")
}

fn build_env_filter_from_env(level: LevelFilter, env_var: &str) -> anyhow::Result<EnvFilter> {
    Ok(EnvFilter::builder()
        .with_default_directive(level.into())
        .with_env_var(env_var)
        .from_env_lossy()
        .add_directive("h2=warn".parse()?)
        .add_directive("tower::buffer=warn".parse()?)
        .add_directive("tower::load=warn".parse()?)
        .add_directive("typer_util=warn".parse()?))
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

    let level_filter = build_env_filter(args.log.level.unwrap_or(LevelFilter::INFO))?;

    use tracing_subscriber::Layer;
    #[allow(unused_mut)]
    let mut layers = vec![taoslog::layer::TaosLayer::<Qid, _, _>::new(appender).boxed()];

    #[cfg(debug_assertions)]
    layers.push(
        taoslog::layer::TaosLayer::<Qid, _, _>::new(std::io::stdout)
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use tracing::Subscriber;
    use tracing_subscriber::{
        layer::{Context, Layer, SubscriberExt},
        registry::Registry,
    };

    const TEST_LOG_ENV: &str = "XNODED_TEST_RUST_LOG";

    fn build_filter(level: LevelFilter) -> EnvFilter {
        build_env_filter_from_env(level, TEST_LOG_ENV).expect("env filter")
    }

    fn captured_targets(filter: EnvFilter, emit: impl FnOnce()) -> Vec<&'static str> {
        #[derive(Clone, Default)]
        struct RecordingLayer {
            targets: Arc<Mutex<Vec<&'static str>>>,
        }

        impl<S> Layer<S> for RecordingLayer
        where
            S: Subscriber,
        {
            fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
                self.targets
                    .lock()
                    .expect("lock targets")
                    .push(event.metadata().target());
            }
        }

        let layer = RecordingLayer::default();
        let targets = Arc::clone(&layer.targets);
        let subscriber = Registry::default().with(filter).with(layer);

        tracing::subscriber::with_default(subscriber, emit);

        targets.lock().expect("lock targets").clone()
    }

    #[test]
    fn build_env_filter_allows_tower_http_info_logs() {
        let targets = captured_targets(build_filter(LevelFilter::INFO), || {
            tracing::info!(target: "tower_http::trace::on_response", "response log");
        });

        assert_eq!(targets, vec!["tower_http::trace::on_response"]);
    }

    #[test]
    fn build_env_filter_suppresses_tower_buffer_info_logs() {
        let targets = captured_targets(build_filter(LevelFilter::INFO), || {
            tracing::info!(target: "tower::buffer::worker", "buffer log");
        });

        assert!(targets.is_empty());
    }
}
