use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::utils::taos_conn::{self, TaosConn};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(transparent)]
    BuildTaos { source: taos_conn::Error },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, serde::Deserialize)]
struct MNodeStatus {
    role: String,
    endpoint: String,
    role_time: String,
}

#[instrument(skip_all)]
pub async fn start_monitor(dsn: &str, leader_ep: &str, cancel: CancellationToken) -> Result<()> {
    tracing::info!("start monitor");
    let _guard = cancel.drop_guard_ref();

    let conn = TaosConn::create(dsn, 3).await?;

    let mut role_time: Option<String> = None;
    loop {
        match cancel
            .run_until_cancelled(conn.query::<MNodeStatus>("SHOW MNODES"))
            .await
        {
            Some(Ok(mnodes)) => {
                for status in mnodes {
                    if status.role_time.starts_with("1970") {
                        continue;
                    }
                    if status.role != "leader" {
                        continue;
                    }
                    if status.endpoint != leader_ep {
                        tracing::error!(
                            "fetch leader endpoint {} not eq {}",
                            status.endpoint,
                            leader_ep
                        );
                        return Ok(());
                    }

                    let rt = role_time.get_or_insert(status.role_time.clone());
                    if rt != &status.role_time {
                        tracing::error!(
                            "fetch leader role time {} not eq {}",
                            status.role_time,
                            rt,
                        );
                        return Ok(());
                    }
                    break;
                }
            }
            Some(Err(e)) => {
                tracing::error!("exec `SHOW MNODES` error: {}", anyhow::Error::new(e))
            }
            None => break,
        }

        if cancel
            .run_until_cancelled(tokio::time::sleep(Duration::from_secs(5)))
            .await
            .is_none()
        {
            break;
        }
    }

    Ok(())
}
