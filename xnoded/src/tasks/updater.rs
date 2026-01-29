use std::time::Duration;

use anyhow::Context;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    controller::{
        tasks::Tasks,
        updaters::{update_agent_status, update_task_status},
        xnodes::XNodes,
    },
    utils::taos_conn::TaosConn,
};

#[instrument(skip_all)]
pub async fn start_ticker(
    dsn: String,
    xnodes: XNodes,
    tasks: Tasks,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("start updater");
    let _guard = cancel.drop_guard_ref();
    let conn = TaosConn::create(dsn, 3)
        .await
        .context("create db connection error")?;
    while cancel
        .run_until_cancelled(tokio::time::sleep(Duration::from_secs(5)))
        .await
        .is_some()
    {
        if cancel
            .run_until_cancelled(update(&conn, &xnodes, &tasks))
            .await
            .is_none()
        {
            return Ok(());
        };
    }

    Ok(())
}

async fn update(conn: &TaosConn, xnodes: &XNodes, tasks: &Tasks) {
    for agent_id in xnodes.all_agents() {
        update_agent_status(conn, xnodes, agent_id).await;
    }

    update_task_status(conn, xnodes, tasks, None).await;
}
