use std::{
    fmt::Debug,
    io::Write,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use flume::{Receiver, Sender};
use futures::FutureExt;
use itertools::Itertools;
use taos::{AsyncQueryable, TaosPool};
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};

use crate::{LegacyMetrics, QueryOpts, TargetOpts, TimeRange};

use super::{sync_normal_table_schema, sync_single_table, sync_super_table_schema_with_subs};

pub enum Todo {
    Meta(
        Option<Arc<String>>,
        Vec<String>,
        Option<oneshot::Sender<anyhow::Result<()>>>,
    ),
    Data(
        Option<Arc<String>>,
        String,
        TimeRange,
        Option<oneshot::Sender<anyhow::Result<()>>>,
    ),
}

/// Legacy table synchronization scheduler.
pub struct Scheduler {
    workers: u32,
    #[allow(dead_code)]
    source: TaosPool,
    #[allow(dead_code)]
    target: TaosPool,
    opts: Arc<TargetOpts>,
    sender: Sender<Todo>,
    receiver: Receiver<Todo>,
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("workers", &self.workers)
            .field("source", &"..")
            .field("target", &"..")
            .field("opts", &self.opts)
            .field("sender", &self.sender)
            .field("receiver", &self.receiver)
            .field("handles", &self.handles)
            .finish()
    }
}

async fn worker(
    worker: u32,
    source: TaosPool,
    target: TaosPool,
    receiver: Receiver<Todo>,
    query: Arc<QueryOpts>,
    opts: Arc<TargetOpts>,
    metrics: Arc<LegacyMetrics>,
    source_is_v3: bool,
    target_is_v3: bool,
) -> anyhow::Result<()> {
    let mut from = source.get().await?;
    let mut to = target.get().await?;
    from.exec("select 1")
        .await
        .map_err(|err| anyhow::format_err!("check source connection error: {err:?}"))?;
    to.exec("select 1")
        .await
        .map_err(|err| anyhow::format_err!("check target connection error: {err:?}"))?;
    loop {
        let todo = receiver.recv_async().await?;
        match todo {
            Todo::Meta(stable, tables, sender) => {
                match stable {
                    Some(stable) => {
                        let mut retries = 1;
                        loop {
                            //todo
                            match sync_super_table_schema_with_subs(
                                &from,
                                &stable,
                                &tables,
                                &to,
                                tables.len(),
                                &opts,
                                source_is_v3,
                                target_is_v3,
                                0,
                                &metrics,
                            )
                            .await
                            {
                                Ok(_) => {
                                    if let Some(sender) = sender {
                                        let _ = sender.send(Ok(()));
                                    }
                                    break;
                                }
                                Err(err) => {
                                    let table_count = tables.len();
                                    log::error!(
                                		"[worker:{worker}] sync stable schema {stable} with {table_count} sub tables error: {err:?}, continue next"
                            		);
                                    let err_string = err.to_string();
                                    if err_string.contains("0xE00") && retries > 0 {
                                        from = source.get().await?;
                                        to = target.get().await?;
                                        retries -= 1;
                                        continue;
                                    }

                                    if let Some(path) = opts.fails_to.as_ref() {
                                        path.lock().unwrap().write_fmt(format_args!(
                                            "meta\t{}:{}\t{}\n",
                                            stable.as_str(),
                                            tables.join(","),
                                            format!("{err:?}").replace("\n", " ")
                                        ))?;
                                    }

                                    if let Some(sender) = sender {
                                        let _ = sender.send(Err(err));
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    None => {
                        //normal
                        let mut errors = String::new();
                        for table in &tables {
                            if let Err(err) = sync_normal_table_schema(&from, &table, &to).await {
                                log::error!("Syncing table `{table}` error: {err:?}");
                                if let Some(path) = opts.fails_to.as_ref() {
                                    path.lock().unwrap().write_fmt(format_args!(
                                        "meta\t{}\t{}\n",
                                        table.as_str(),
                                        format!("{err:?}").replace("\n", " ")
                                    ))?;
                                }
                                errors.extend(format!("- Error of table {table}: {err}\n").chars());
                            } else {
                                metrics.created_tables.fetch_add(1, Ordering::SeqCst);
                            }
                        }

                        if let Some(sender) = sender {
                            if errors.is_empty() {
                                let _ = sender.send(Ok(()));
                            } else {
                                let _ = sender.send(Err(anyhow::format_err!(
                                    "Syncing {} ordinary tables error:\n{errors}",
                                    tables.len()
                                )));
                            }
                        }
                    }
                }
            }
            Todo::Data(stable, table, time_range, sender) => {
                let query = QueryOpts {
                    time_range,
                    unit: query.unit,
                    limit: query.limit,
                    select_from_stable: query.select_from_stable,
                };
                let mut retries = 1;

                loop {
                    match sync_single_table(
                        &from,
                        stable.as_ref().map(|s| s.as_str()),
                        &table,
                        &to,
                        &query,
                        &opts,
                        target_is_v3,
                        &metrics,
                    )
                    .await
                    {
                        Ok(_) => {
                            if let Some(sender) = sender {
                                let _ = sender.send(Ok(()));
                            }
                            break;
                        }
                        Err(err) => {
                            log::error!(
                                "[worker:{worker}] sync table {table} error: {err:?}, continue next"
                            );
                            let err_string = err.to_string();
                            if err_string.contains("0xE00") && retries > 0 {
                                from = source.get().await?;
                                to = target.get().await?;
                                retries -= 1;
                                continue;
                            }
                            if let Some(path) = opts.fails_to.as_ref() {
                                path.lock().unwrap().write_fmt(format_args!(
                                    "data\t{}\t{}\n",
                                    table.as_str(),
                                    format!("{err:?}").replace("\n", " ")
                                ))?;
                            }

                            if let Some(sender) = sender {
                                let _ = sender.send(Err(err));
                            }
                            break;
                        }
                    };
                }
            }
        }
    }
}
impl Scheduler {
    pub async fn new(
        source: TaosPool,
        target: TaosPool,
        query: Arc<QueryOpts>,
        opts: Arc<TargetOpts>,
        workers: u32,
        metrics: Arc<LegacyMetrics>,
        source_is_v3: bool,
        target_is_v3: bool,
    ) -> Self {
        let (sender, receiver) = flume::unbounded();
        let workers = std::cmp::max(1, workers);
        let handles = (0..workers)
            .map(|i| {
                tokio::spawn(worker(
                    i,
                    source.clone(),
                    target.clone(),
                    receiver.clone(),
                    query.clone(),
                    opts.clone(),
                    metrics.clone(),
                    source_is_v3,
                    target_is_v3,
                ))
            })
            .collect_vec();

        Self {
            workers,
            source,
            target,
            opts,
            sender,
            receiver,
            handles,
        }
    }
    pub fn send(&self, todo: Todo) -> Result<(), flume::SendError<Todo>> {
        self.sender.send(todo)
    }

    // pub fn abort(&self) {
    //     for h in self.handles.iter() {
    //         h.abort();
    //     }
    // }

    // pub fn is_empty(&self) -> bool {
    //     self.receiver.is_empty()
    // }
}
impl std::future::Future for Scheduler {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for h in self.handles.iter_mut() {
            match h.poll_unpin(cx) {
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(_)) => {
                    continue;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
