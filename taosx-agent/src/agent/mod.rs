pub mod client;

use std::time::Duration;

use anyhow::Result;
use flume::Sender;
use futures::StreamExt;
use taosx_core::task_set::prelude::HealthOpts;
use taosx_core::utils::files::decompress_and_write_file;

use tokio_util::sync::CancellationToken;
use tracing::instrument;

use taosx_core::{Fail, PutFileReq, PutFileResp, RespAction, Response, get_data_dir};

/// A streaming workflow task description.
#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct Task {
    /// Unique id for the task item.
    pub id: i64,

    pub job_id: i64,

    /// The stream data source.
    pub from: serde_json::Value,

    /// The target of the stream.
    pub to: String,

    /// The health check options.
    pub health: Option<HealthOpts>,

    /// Agent Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<i64>,

    /// break points
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakpoints: Option<String>,
}

#[instrument(skip_all)]
pub async fn listen_task_metrics(
    resp_tx: Sender<RespAction>,
    cancel: CancellationToken,
) -> Result<()> {
    use taosx_core::plugins::sink::ipc_metric::AGENT_METRICS_SENDER;

    let (tx, rx) = flume::bounded(100);

    let _ = AGENT_METRICS_SENDER.set(tx);

    let rx = {
        use tokio_stream::StreamExt as _;
        rx.into_stream().chunks_timeout(100, Duration::from_secs(1))
    };
    tokio::pin!(rx);
    while let Some(Some(vec)) = cancel.run_until_cancelled(rx.next()).await {
        if vec.is_empty() {
            continue;
        }
        let resp = RespAction::TaskMetrics(vec);
        if cancel
            .run_until_cancelled(resp_tx.send_async(resp))
            .await
            .is_none_or(|v| v.is_err())
        {
            break;
        }
    }

    Ok(())
}

async fn do_put_file(req: PutFileReq, req_id: u64, resp_tx: Sender<RespAction>) {
    let data_dir = get_data_dir();
    let mut path = data_dir.join(req.path);
    let decompress = req.decompress;
    tracing::info!("[put-file] path={path:?}");
    if decompress {
        let extension = path.extension().unwrap_or_default();
        if extension == "gz" {
            path.set_extension("");
            tracing::info!("[put-file] Decompress file to {}", path.display());
        } else {
            let err_msg = "Decompress is enabled, but file extension is not .gz";
            tracing::error!("[put-file] {}", err_msg);
            let _send_err = resp_tx.send_async(RespAction::PutFileOk(PutFileResp {
                req_id,
                path: path.display().to_string(),
                res: Response::Err(Fail::new(anyhow::anyhow!("{}", err_msg))),
            }));
            return;
        }
    } else {
        tracing::info!("[put-file] Write file to {}", path.display());
    }
    // If parent folders not exists, try to create them
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        match tokio::fs::create_dir_all(&parent).await {
            Ok(_) => tracing::info!("[put-file] Directory created successfully"),
            Err(e) => tracing::error!("[put-file] Failed to create directory: {e}"),
        }
    }
    let result = if decompress {
        decompress_and_write_file(&path, &req.data)
    } else {
        tokio::fs::write(&path, &req.data).await
    };

    match result {
        Ok(_) => {
            let _send_ok = resp_tx
                .send_async(RespAction::PutFileOk(PutFileResp {
                    req_id,
                    path: path.display().to_string(),
                    res: Response::Ok("Ok".to_string()),
                }))
                .await;
        }
        Err(err) => {
            tracing::error!("[put-file] Write file error: {err:#}");
            let _send_ok = resp_tx
                .send_async(RespAction::PutFileOk(PutFileResp {
                    req_id,
                    path: path.display().to_string(),
                    res: Response::Err(Fail::new(err)),
                }))
                .await;
        }
    }
}
