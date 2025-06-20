use std::time::Duration;

use futures::StreamExt;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

const DEFAULT_CHUNK_SIZE: usize = 1000;
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(3);

pub struct WriterBuilder<W, B> {
    chunk_size: Option<usize>,
    sync_interval: Option<Duration>,

    writer: W,
    rx: flume::Receiver<B>,
}

impl<W, B> WriterBuilder<W, B> {
    pub fn chunk_size(self, chunk_size: usize) -> Self {
        Self {
            chunk_size: Some(chunk_size),
            ..self
        }
    }

    pub fn sync_interval(self, sync_interval: Duration) -> Self {
        Self {
            sync_interval: Some(sync_interval),
            ..self
        }
    }

    pub fn build(self) -> Writer<W, B> {
        Writer {
            writer: self.writer,
            rx: self.rx,
            chunk_size: self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
            sync_interval: self.sync_interval.unwrap_or(DEFAULT_SYNC_INTERVAL),
        }
    }
}

pub struct Writer<W, B> {
    writer: W,
    rx: flume::Receiver<B>,
    chunk_size: usize,
    sync_interval: Duration,
}

impl<W, B> Writer<W, B>
where
    W: super::RawWriter<B, EntryPosition = crate::fs::EntryPosition>,
    B: AsRef<[u8]> + Send,
{
    pub fn builder(writer: W, rx: flume::Receiver<B>) -> WriterBuilder<W, B> {
        WriterBuilder {
            chunk_size: None,
            sync_interval: None,
            writer,
            rx,
        }
    }

    pub async fn run(mut self, token: CancellationToken) -> super::Result<()> {
        let mut ticker = tokio::time::interval(self.sync_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut stream = self.rx.stream().ready_chunks(self.chunk_size);
        let mut is_dirty = false;
        loop {
            tokio::select! {
                res = stream.next() => {
                    let Some(entries) = res else {
                        break
                    };
                    self.writer.write(entries).await?;
                    is_dirty = true;
                },
                _ = ticker.tick(), if is_dirty => {
                    self.writer.sync_data().await?;
                }
                _ = token.cancelled() => {
                    break
                },
            }
        }

        // 把剩余的消息消费完
        while let Some(entries) = stream.next().await {
            self.writer.write(entries).await?;
        }
        // 退出前同步数据到磁盘，避免文件损坏
        self.writer.sync_data().await?;

        Ok(())
    }
}
