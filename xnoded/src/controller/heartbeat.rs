use std::time::Duration;

use arrow_flight::error::FlightError;
use ha_core::types::XnodedId;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use taosx_utils::backoff::RetryBackoff;

use crate::controller::{Result, XNodes};

#[instrument(skip_all, fields(xnode_id=id))]
pub async fn heartbeat_loop(
    id: i32,
    xnoded_id: XnodedId,
    xnodes: XNodes,
    reconnect_tx: flume::Sender<oneshot::Sender<bool>>,
    rebalance_tx: flume::Sender<i32>,
    cancel: CancellationToken,
) -> Result<()> {
    let _cleanup = taosx_utils::defer::defer(|| {
        xnodes.set_offline(id);
        tracing::info!(xnode_id = id, "heartbeat loop exited");
    });
    let _guard = cancel.drop_guard_ref();
    let mut backoff = RetryBackoff::new(Duration::from_millis(500), Duration::from_secs(5));
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if xnodes.is_offline(id) {
            if cancel.run_until_cancelled(backoff.wait()).await.is_none() {
                break;
            }
            if backoff.retries() >= 6
                && cancel
                    .run_until_cancelled(rebalance_tx.send_async(id))
                    .await
                    .is_none()
            {
                break;
            }
            let (tx, rx) = oneshot::channel();
            if reconnect_tx.send_async(tx).await.is_err() {
                break;
            };
            let Some(Ok(ok)) = cancel.run_until_cancelled(rx).await else {
                break;
            };
            if !ok {
                continue;
            }
        }
        backoff.reset();
        if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
            break;
        }
        let Some(client) = xnodes.get_client(id) else {
            continue;
        };
        match client.heartbeat(&xnoded_id).await {
            Ok(metrics) => {
                tracing::debug!(xnode_id = id, "heartbeat ok");
                xnodes.update_metrics(id, metrics);
            }
            Err(ha_rpc_client::error::Error::EventLoopDropped) => {
                xnodes.set_offline(id);
                tracing::error!(xnode_id = id, "eventloop dropped");
            }
            Err(ha_rpc_client::error::Error::Timeout) => {
                xnodes.set_offline(id);
                tracing::error!(xnode_id = id, "heartbeat timeout");
            }
            Err(ha_rpc_client::error::Error::Flight {
                source: FlightError::Tonic(e),
            }) if matches!(
                e.code(),
                tonic::Code::Unavailable | tonic::Code::DataLoss | tonic::Code::Cancelled
            ) =>
            {
                xnodes.set_offline(id);
                tracing::error!(
                    xnode_id = id,
                    "heartbeat failed with flight error: {e}, reconnect"
                );
            }
            Err(e) => {
                tracing::error!(
                    xnode_id = id,
                    "heartbeat failed with error: {:#}",
                    anyhow::Error::new(e)
                );
            }
        }
    }

    Ok(())
}
