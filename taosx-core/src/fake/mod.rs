use std::sync::Arc;

use anyhow::bail;
use taos::*;
use tokio_util::sync::CancellationToken;

use crate::{utils::port_pool::PortPool, Action, Parser, TaskNotifySender, Transferred};

pub async fn fake_to_taos(
    from: Dsn,
    _parser: Option<Parser>,
    _transform: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    _port_pool: &PortPool,
    cancel: CancellationToken,
    _with_agent: Option<(i64, String, String)>,
    _transferred: Option<Arc<Transferred>>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    tracing::info!("fake_to_taos: from: {:?}, to: {:?}", from, to);

    let _ = notify.send(crate::TaskNotify::info("started"));

    let future = async move {
        if let Some(sleep) = from.get("sleep") {
            let sleep = parse_duration::parse(sleep)?;
            tokio::time::sleep(sleep).await;
        }
        if let Some(value) = from.get("bail") {
            bail!("{}", value);
        }
        anyhow::Ok(())
    };
    tokio::pin!(future);

    tokio::select! {
        _ = cancel.cancelled() => {
            tracing::info!("fake to taos cancelled");
            return Ok(());
        }
        result = future => {
            result?;
        }
    }
    tracing::info!("fake to taos finished");
    let _ = notify.send(crate::TaskNotify::info("finished"));

    Ok(())
}
