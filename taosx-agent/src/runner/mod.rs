use std::fmt::Display;

use anyhow::Result;
use dashmap::DashMap;
use taosx_core::TaskOpts;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::agent::Task;

pub enum Action {
    Run(Task),
    Cancel(i64),
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
) -> (JoinHandle<Result<()>>, flume::Sender<Action>) {
    let (tx, rx) = flume::bounded(1);
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
                            };
                            let pool = port_pool.clone();
                            let handle = tokio::spawn(async move { opts.run(&pool).await });
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
    )
}
