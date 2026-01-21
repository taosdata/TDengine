use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use ha_core::{batch::build_batch, consts::MESSAGE_HEARTBEAT, utils::next_req_id};
use tokio_util::sync::CancellationToken;

pub async fn spawn_tasks(
    tx: flume::Sender<Result<RecordBatch, FlightError>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    while cancel.run_until_cancelled(interval.tick()).await.is_some() {
        let batch = build_batch(MESSAGE_HEARTBEAT, "", next_req_id()).map_err(FlightError::Arrow);
        if cancel
            .run_until_cancelled(tx.send_async(batch))
            .await
            .is_none_or(|v| v.is_err())
        {
            break;
        }
    }
    Ok(())
}
