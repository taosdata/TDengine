use anyhow::Context;
use rdkafka::{
    consumer::{Consumer, ConsumerContext},
    metadata::Metadata,
};
use std::time::Duration;

pub(crate) async fn run_blocking<T, F>(task_name: &'static str, f: F) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .with_context(|| format!("{task_name} task join error"))?
}

pub(crate) async fn fetch_metadata<C, Ctx>(
    consumer: C,
    topic: Option<String>,
    timeout: Duration,
) -> anyhow::Result<(C, anyhow::Result<Metadata>)>
where
    C: Consumer<Ctx> + Send + 'static,
    Ctx: ConsumerContext,
{
    run_blocking("kafka metadata fetch", move || {
        let result = consumer
            .fetch_metadata(topic.as_deref(), timeout)
            .map_err(anyhow::Error::from);
        Ok((consumer, result))
    })
    .await
}

pub(crate) async fn fetch_watermarks<C, Ctx>(
    consumer: C,
    topic: String,
    partition: i32,
    timeout: Duration,
) -> anyhow::Result<(C, anyhow::Result<(i64, i64)>)>
where
    C: Consumer<Ctx> + Send + 'static,
    Ctx: ConsumerContext,
{
    run_blocking("kafka watermarks fetch", move || {
        let result = consumer
            .fetch_watermarks(&topic, partition, timeout)
            .map_err(anyhow::Error::from);
        Ok((consumer, result))
    })
    .await
}
