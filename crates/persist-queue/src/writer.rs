use std::time::Duration;

use futures::StreamExt;
use futures_ext::OptionFuture;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const DEFAULT_CHUNK_SIZE: usize = 1000;
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(3);

pub struct WriterBuilder<W, B, P> {
    chunk_size: Option<usize>,
    sync_interval: Option<Duration>,

    writer: W,
    rx: flume::Receiver<B>,
    request_rx: Option<flume::Receiver<Request<P>>>,
}

impl<W, B, P> WriterBuilder<W, B, P> {
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

    pub fn request_rx(self, request_rx: flume::Receiver<Request<P>>) -> Self {
        Self {
            request_rx: Some(request_rx),
            ..self
        }
    }

    pub fn build(self) -> Writer<W, B, P> {
        Writer {
            writer: self.writer,
            rx: self.rx,
            request_rx: self.request_rx,
            chunk_size: self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE),
            sync_interval: self.sync_interval.unwrap_or(DEFAULT_SYNC_INTERVAL),
        }
    }
}

pub struct Writer<W, B, P> {
    writer: W,
    rx: flume::Receiver<B>,
    chunk_size: usize,
    sync_interval: Duration,
    request_rx: Option<flume::Receiver<Request<P>>>,
}

pub enum Request<P> {
    Position(oneshot::Sender<P>),
}

impl<W, B, P> Writer<W, B, P>
where
    W: super::RawWriter<B, EntryPosition = P>,
    B: AsRef<[u8]> + Send,
{
    pub fn builder(writer: W, rx: flume::Receiver<B>) -> WriterBuilder<W, B, P> {
        WriterBuilder {
            chunk_size: None,
            sync_interval: None,
            request_rx: None,
            writer,
            rx,
        }
    }

    pub async fn run(mut self, token: CancellationToken) -> super::Result<()> {
        let mut stream = self.rx.stream().ready_chunks(self.chunk_size);
        loop {
            tokio::select! {
                res = stream.next() => {
                    let Some(entries) = res else {
                        break
                    };
                    self.writer.write(entries).await?;
                },
                req = OptionFuture::from(self.request_rx.as_ref().map(|v| v.recv_async())) => {
                    let Ok(req) = req else {
                        break
                    };
                    match req {
                        Request::Position(sender) => {
                            let position = self.writer.position();
                            sender.send(position).ok();
                        },
                    }
                },
                _ = tokio::time::sleep(self.sync_interval) => {
                    self.writer.sync_data().await?;
                }
                _ = token.cancelled() => {
                    break
                },
            }
        }

        // 退出前同步数据到磁盘，避免文件损坏
        self.writer.sync_data().await?;

        Ok(())
    }
}
