use std::ops::ControlFlow;

use taosx_core::{task_set::prelude::*, tmq::tmq_metric::TmqMetrics};

#[derive(Debug)]
pub struct ReplicationSpawner;

#[derive(Debug)]
pub struct ReplicationExecutor {
    opts: TaskOpts,
    metrics: Arc<CoreMetrics>,
}

#[async_trait::async_trait]
impl TaskSpawner for ReplicationSpawner {
    fn source_name(&self) -> SourceName {
        SourceName::builder()
            .id("tmq".into())
            .name("TMQ".into())
            .aliases(vec!["sync".into()])
            .build()
    }

    fn sink_name(&self) -> SinkName {
        SinkName::builder()
            .id("taos".into())
            .name("TDengine 3.0".into())
            .build()
    }

    async fn executor(&self, opts: &TaskOpts) -> anyhow::Result<Box<dyn TaskExecutor>> {
        Ok(Box::new(ReplicationExecutor {
            opts: opts.clone(),
            metrics: Arc::new(CoreMetrics::TMQ(TmqMetrics::default())),
        }))
    }
}

#[async_trait::async_trait]
impl TaskExecutor for ReplicationExecutor {
    async fn license(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn sample(&self) -> anyhow::Result<DsSampleIn> {
        anyhow::bail!("Not supported")
    }

    fn metrics(&self) -> &Arc<CoreMetrics> {
        &self.metrics
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        info!("initialize");
        Ok(())
    }

    async fn reset(&self) {
        info!("reset tmq to taos");
    }

    async fn run(&self, context: &Context) -> anyhow::Result<Exit> {
        let cancel = context.child_token();

        let TaskOpts {
            from,
            transform,
            parser: _,
            to,
        } = self.opts.clone();
        let tid = context.env.tid().map(|id| id.to_string());
        let (notify, _receiver) = flume::unbounded();
        tmq_to_td::tmq_to_td(from, transform, to, cancel, tid, notify).await?;

        Ok(Exit::Completed)
    }
    async fn on_stop(&self) {
        info!("stop");
    }

    async fn before_start(&self) -> anyhow::Result<()> {
        info!("pre_start");
        Ok(())
    }

    async fn on_error(&self, error: anyhow::Error) -> ControlFlow<anyhow::Error> {
        info!(%error, "on_error");
        ControlFlow::Continue(())
    }

    async fn on_fatal(&self) {
        error!("on_fatal");
    }

    async fn on_completed(&self) {
        error!("on completed");
    }
}
