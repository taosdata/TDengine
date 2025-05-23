use std::{ops::ControlFlow, time::Duration};

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
        // 初始化时先尝试清理数据
        self.reader.vacuum().await?;

        let mut ticker = tokio::time::interval(self.vacuum_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            #[cfg(unix)]
            {
                tokio::select! {
                    res = self.reader.read_util(1, self.batch_size, Some(self.vacuum_interval)) => {
                        let entries = res?;
                        if entries.is_empty() {
                            self.reader.vacuum().await?;
                            continue;
                        }
                        for entry in entries {
                            if self.send_entry(entry, token.child_token()).await.is_break() {
                                break;
                            }
                        }
                    },
                    _ = ticker.tick() => {
                        self.reader.vacuum().await?;
                    }
                    _ = token.cancelled() => break
                }
            }
            #[cfg(windows)]
            {
                if token.is_cancelled() {
                    break;
                }
                let entries = self.reader.read(self.batch_size).await?;
                if entries.is_empty() {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                        _ = ticker.tick() => {
                            self.reader.vacuum().await?;
                        }
                        _ = token.cancelled() => break,
                    }
                    continue;
                }
                for entry in entries {
                    if self.send_entry(entry, token.child_token()).await.is_break() {
                        break;
                    }
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
