use std::fmt::Display;

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use taosx_core::TaskOpts;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::agent::Task;

pub enum Action {
    Run(Task),
    Cancel(i64),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TaskStatus {
    id: i64,
    at: DateTime<Utc>,
    action: String,
    message: Option<String>,
    context: Option<String>,
}

struct Worker {
    handle: JoinHandle<Result<()>>,
    cancellation: CancellationToken,
}
impl Worker {
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished() || self.cancelled()
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}
pub fn spawn_runner(
    endpoint: impl Display,
    token: impl Display,
) -> (
    JoinHandle<Result<()>>,
    flume::Sender<Action>,
    flume::Receiver<TaskStatus>,
) {
    let (tx, rx) = flume::bounded(1);
    let (status_tx, status_rx) = flume::unbounded();
    let endpoint = endpoint.to_string();
    let token = token.to_string();
    (
        tokio::task::spawn_blocking(move || {
            let port_pool = taosx_core::utils::port_pool::PortPool::default();
            let tasks: DashMap<i64, Worker> = DashMap::new();
            // let stop_notify = tokio::sync::Notify::new();
            // let scheduler = Arc::new()
            loop {
                if let Ok(action) = rx.recv() {
                    match action {
                        Action::Run(task) => {
                            if let Some(running) = tasks.get(&task.id) {
                                if running.value().is_finished() {
                                    running.cancelled();
                                    tasks.remove(&task.id);
                                } else {
                                    info!("[{}] Runner has been started", running.key());
                                    continue;
                                }
                            }
                            let cancellation = CancellationToken::new();
                            let cancel = cancellation.clone();

                            let opts = TaskOpts {
                                transform: vec![],
                                from: task.from.parse().unwrap(),
                                to: task.to.parse()?,
                                jobs: task.jobs as _,
                                compression_level: task.compression_level.map(Into::into),
                                force: task.force,
                                cancel,
                                parser: None,
                                // port_pool: ONCE,
                                with_agent: Some((
                                    task.id,
                                    endpoint.to_string(),
                                    token.to_string(),
                                )),
                                offsets: Default::default(),
                                transferred: None,
                            };
                            let pool = port_pool.clone();
                            let status_tx = status_tx.clone();
                            let handle = tokio::spawn(async move {
                                if let Err(err) = opts.run(&pool).await {
                                    use itertools::Itertools;
                                    let status = TaskStatus {
                                        id: task.id,
                                        at: Utc::now(),
                                        action: "failed".to_string(),
                                        message: Some(err.to_string()),
                                        context: Some(err.chain().join("\n")),
                                    };
                                    let _ = status_tx.send_async(status).await;
                                    Err(err)
                                } else {
                                    Ok(())
                                }
                            });
                            tasks.insert(
                                task.id,
                                Worker {
                                    handle,
                                    cancellation,
                                },
                            );
                        }
                        Action::Cancel(id) => {
                            if let Some(cancellation) = tasks.get(&id) {
                                cancellation.cancel();
                                drop(cancellation);
                                if let Some((id, worker)) = tasks.remove(&id) {
                                    info!(
                                        id = id,
                                        "[{id}] Remove runner for task {id}, wait for finished"
                                    );
                                    worker.handle.abort();
                                    // if let Err(err) =  {
                                    //     warn!(id = id, "[{id}] Task error: {err}");
                                    // }
                                } else {
                                    warn!("[{id}] Runner not found");
                                }
                            }
                        }
                    }
                } else {
                    tracing::info!("Task listener stopped, now cancel all running tasks...");
                    for task in tasks.iter() {
                        task.cancel()
                    }
                    break Ok(());
                }
            }
        }),
        tx,
        status_rx,
    )
}
