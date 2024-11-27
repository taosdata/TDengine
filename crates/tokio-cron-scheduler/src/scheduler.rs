use crate::context::Context;
#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobState, JobType};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobState, JobType};
use crate::JobSchedulerError;
use chrono::{FixedOffset, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

pub struct Scheduler {
    pub shutdown: Arc<AtomicBool>,
    pub start_tx: Arc<RwLock<Option<Sender<bool>>>>,
    pub start_rx: Arc<RwLock<Option<Receiver<bool>>>>,
    pub ticking: Arc<AtomicBool>,
    pub inited: bool,
}

impl Default for Scheduler {
    fn default() -> Self {
        let (ticker_tx, ticker_rx) = tokio::sync::oneshot::channel();
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            inited: false,
            start_tx: Arc::new(RwLock::new(Some(ticker_tx))),
            start_rx: Arc::new(RwLock::new(Some(ticker_rx))),
            ticking: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Scheduler {
    pub async fn init(&mut self, context: &Context) {
        if self.inited {
            return;
        }

        let job_activation_tx = context.job_activation_tx.clone();
        let notify_tx = context.notify_tx.clone();
        let job_delete_tx = context.job_delete_tx.clone();
        let shutdown = self.shutdown.clone();
        let metadata_storage = context.metadata_storage.clone();

        self.inited = true;

        let start_rx = {
            let mut w = self.start_rx.write().await;
            w.take()
        };

        let ticking = self.ticking.clone();
        tokio::spawn(async move {
            let is_ticking = ticking.load(Ordering::Relaxed);
            if !is_ticking {
                if let Some(start_rx) = start_rx {
                    if let Err(e) = start_rx.await {
                        error!(?e, "Could not subscribe to ticker starter");
                        return;
                    }
                }
                let is_ticking = ticking.load(Ordering::Relaxed);
                if !is_ticking {
                    loop {
                        let is_ticking = ticking.load(Ordering::Relaxed);
                        if is_ticking {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            'next_tick: loop {
                let shutdown = { shutdown.load(Ordering::Relaxed) };
                if shutdown {
                    break 'next_tick;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                let now = Utc::now();
                let next_ticks = {
                    let mut w = metadata_storage.write().await;
                    w.list_next_ticks().await
                };
                if let Err(e) = next_ticks {
                    error!("Error with listing next ticks {:?}", e);
                    continue 'next_tick;
                }
                let mut next_ticks = next_ticks.unwrap();
                let to_be_deleted = next_ticks.iter().filter_map(|v| {
                    v.id.as_ref()?;
                    if v.next_tick == 0 {
                        let id: Uuid = v.id.as_ref().unwrap().into();
                        Some(id)
                    } else {
                        None
                    }
                });
                for uuid in to_be_deleted {
                    let tx = job_delete_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tx.send(uuid) {
                            error!("Error sending deletion {:?}", e);
                        }
                    });
                }

                next_ticks.retain(|n| n.next_tick != 0);

                let must_runs = next_ticks.iter().filter_map(|n| {
                    let next_tick = n.next_tick_utc();
                    let last_tick = n.last_tick_utc();
                    let job_type: JobType = JobType::from_i32(n.job_type).unwrap();

                    let must_run = match (last_tick.as_ref(), next_tick.as_ref(), job_type) {
                        (None, Some(next_tick), JobType::OneShot) => {
                            let now_to_next = now.cmp(next_tick);
                            matches!(now_to_next, std::cmp::Ordering::Greater)
                                || matches!(now_to_next, std::cmp::Ordering::Equal)
                        }
                        (None, Some(next_tick), JobType::Repeated) => {
                            let now_to_next = now.cmp(next_tick);
                            matches!(now_to_next, std::cmp::Ordering::Greater)
                                || matches!(now_to_next, std::cmp::Ordering::Equal)
                        }
                        (None, Some(next_tick), JobType::Cron) => {
                            let now_to_next = now.cmp(next_tick);
                            matches!(now_to_next, std::cmp::Ordering::Greater)
                                || matches!(now_to_next, std::cmp::Ordering::Equal)
                        }
                        (Some(last_tick), Some(next_tick), _) => {
                            let now_to_next = now.cmp(next_tick);
                            let last_to_next = last_tick.cmp(next_tick);

                            (matches!(now_to_next, std::cmp::Ordering::Greater)
                                || matches!(now_to_next, std::cmp::Ordering::Equal))
                                && (matches!(last_to_next, std::cmp::Ordering::Less)
                                    || matches!(last_to_next, std::cmp::Ordering::Equal))
                        }
                        _ => false,
                    };
                    if must_run {
                        let id: Uuid = n.id.as_ref().map(|f| f.into()).unwrap();
                        Some(id)
                    } else {
                        None
                    }
                });

                for uuid in must_runs {
                    {
                        if let Ok(Some(data)) = metadata_storage.write().await.get(uuid).await {
                            if !data.ran {
                                {
                                    let tx = notify_tx.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = tx.send((uuid, JobState::Scheduled)) {
                                            error!("Error sending notification activation {:?}", e);
                                        }
                                    });
                                }
                                {
                                    let tx = job_activation_tx.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = tx.send(uuid) {
                                            error!("Error sending job activation tx {:?}", e);
                                        }
                                    });
                                }
                            }
                        }
                    }

                    let storage = metadata_storage.clone();
                    tokio::spawn(async move {
                        let mut w = storage.write().await;
                        let job = w.get(uuid).await;

                        let next_and_last_tick = match job {
                            Ok(Some(job)) => {
                                let job_type: JobType = JobType::from_i32(job.job_type).unwrap();
                                let schedule = job.schedule();
                                let fixed_offset = FixedOffset::east_opt(job.time_offset_seconds)
                                    .unwrap_or(FixedOffset::east_opt(0).unwrap());
                                let now = now.with_timezone(&fixed_offset);
                                let repeated_every = job.repeated_every();
                                let next_tick = job
                                    .next_tick_utc()
                                    .map(|nt| nt.with_timezone(&fixed_offset));
                                let next_tick = match job_type {
                                    JobType::Cron => {
                                        schedule.and_then(|s| s.iter_after(now).next())
                                    }
                                    JobType::OneShot => None,
                                    JobType::Repeated => repeated_every.and_then(|r| {
                                        next_tick.and_then(|nt| {
                                            nt.checked_add_signed(chrono::Duration::seconds(
                                                r as i64,
                                            ))
                                        })
                                    }),
                                };
                                let last_tick = Some(now);
                                Some((
                                    next_tick.map(|nt| nt.with_timezone(&Utc)),
                                    last_tick.map(|nt| nt.with_timezone(&Utc)),
                                ))
                            }
                            _ => {
                                error!("Could not get job metadata");
                                None
                            }
                        };

                        if let Some((next_tick, last_tick)) = next_and_last_tick {
                            if let Err(e) =
                                w.set_next_and_last_tick(uuid, next_tick, last_tick).await
                            {
                                error!("Could not set next and last tick {:?}", e);
                            }
                        }
                    });
                }
            }
        });
    }

    pub async fn shutdown(&mut self) {
        self.shutdown.swap(true, Ordering::Relaxed);
    }

    pub async fn start(&mut self) -> Result<(), JobSchedulerError> {
        let is_ticking = self.ticking.load(Ordering::Relaxed);
        if is_ticking {
            Err(JobSchedulerError::TickError)
        } else {
            self.ticking.swap(true, Ordering::Relaxed);
            let tx = {
                let mut w = self.start_tx.write().await;
                let mut tx: Option<Sender<bool>> = None;
                std::mem::swap(&mut tx, &mut *w);
                tx
            };

            if let Some(tx) = tx {
                if let Err(e) = tx.send(true) {
                    error!(?e, "Start ticker send error");
                }
            }

            Ok(())
        }
    }
}
