use std::{ops::ControlFlow, time::Duration};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::Entry;

const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
const DEFAULT_VACUUM_DURATION: Duration = Duration::from_secs(30);

pub struct ReaderBuilder<R, P> {
    vacuum_interval: Option<Duration>,
    batch_size: Option<usize>,

    reader: R,
    tx: flume::Sender<Entry<P>>,
}

impl<R, P> ReaderBuilder<R, P>
where
    R: super::RawReader<EntryPosition = P>,
{
    pub fn batch_size(self, max_batch_size: usize) -> Self {
        Self {
            batch_size: Some(max_batch_size),
            ..self
        }
    }

    pub fn vacuum_interval(self, vacuum_interval: Duration) -> Self {
        Self {
            vacuum_interval: Some(vacuum_interval),
            ..self
        }
    }

    pub fn build(self) -> Reader<R, P> {
        Reader {
            reader: self.reader,
            batch_size: self.batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE),
            tx: self.tx,
            vacuum_interval: self.vacuum_interval.unwrap_or(DEFAULT_VACUUM_DURATION),
        }
    }
}

pub struct Reader<R, P> {
    reader: R,
    batch_size: usize,
    tx: flume::Sender<Entry<P>>,
    vacuum_interval: Duration,
}

impl<R, P> Reader<R, P>
where
    R: super::RawReader<EntryPosition = P>,
{
    pub fn builder(reader: R, tx: flume::Sender<Entry<P>>) -> ReaderBuilder<R, P> {
        ReaderBuilder {
            vacuum_interval: None,
            batch_size: None,
            reader,
            tx,
        }
    }

    pub async fn run(mut self, token: CancellationToken) -> super::Result<()> {
        let mut ticker = tokio::time::interval(self.vacuum_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if token.is_cancelled() {
                break;
            }
            if ticker.tick().now_or_never().is_some() {
                self.reader.vacuum().await?;
            }
            let entries = self
                .reader
                .read_util(1, self.batch_size, Some(DEFAULT_VACUUM_DURATION), &token)
                .await?;
            if entries.is_empty() {
                continue;
            }
            for entry in entries {
                if self.send_entry(entry, token.child_token()).await.is_break() {
                    break;
                }
            }
        }

        // 退出时再尝试清理数据
        self.reader.vacuum().await?;

        Ok(())
    }

    async fn send_entry(&self, entry: Entry<P>, token: CancellationToken) -> ControlFlow<()> {
        tokio::select! {
            res = self.tx.send_async(entry) => {
                if res.is_err() {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            },
            _ = token.cancelled() => ControlFlow::Break(()),
        }
    }
}
