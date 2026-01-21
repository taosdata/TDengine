use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::controller::Controller;

#[instrument(skip_all)]
pub async fn start_rebalancer(
    controller: Arc<Controller>,
    rebalance_rx: flume::Receiver<i32>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("start rebalancer");
    let _guard = cancel.drop_guard_ref();
    let (tasks, xnodes) = (controller.tasks(), controller.xnodes());
    while let Some(Ok(xid)) = cancel.run_until_cancelled(rebalance_rx.recv_async()).await {
        if controller.xnodes().is_online(xid) {
            continue;
        }

        let jobs = tasks.xnode_jobs(xid);
        if jobs.is_empty() {
            continue;
        }

        for (task_id, job_id) in jobs {
            let Some(config) = tasks.job(task_id, job_id) else {
                continue;
            };
            if config.should_skip_rebalance() {
                continue;
            }
            let Some(xnode_id) = xnodes.best_xnode(config.config.via) else {
                continue;
            };
            if xid == xnode_id {
                continue;
            }
            tracing::info!(
                task_id,
                job_id,
                "rebalancing task from {} to xnode {}",
                xid,
                xnode_id
            );
            let start_task_fut =
                controller.start_task_job(xnode_id, task_id, job_id, config.config);
            let Some(res) = cancel.run_until_cancelled(start_task_fut).await else {
                return Ok(());
            };
            match res {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(
                        task_id,
                        job_id,
                        xnode_id,
                        "failed to start job: {:#}",
                        anyhow::Error::new(e)
                    );
                }
            }
        }
    }

    Ok(())
}
