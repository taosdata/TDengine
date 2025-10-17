use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use flume::Receiver;
use taos::*;
use tokio::{
    fs::OpenOptions,
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
};
use tokio_util::sync::CancellationToken;

use crate::{
    QueryObject,
    config::{Td2LocalConfig, Td2LocalContext},
};

enum WriterMsg {
    Task {
        task_id: usize,
        rx: flume::Receiver<Bytes>,
    },
}

pub struct FileWriter {
    tx: flume::Sender<WriterMsg>,
}

// A writer adapter that counts how many bytes have been written to the underlying writer.
use std::{
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};
struct CountingWriter<W: AsyncWrite + Unpin> {
    inner: W,
    written: Arc<AtomicU64>,
}

impl<W: AsyncWrite + Unpin> CountingWriter<W> {
    fn new(inner: W, written: Arc<AtomicU64>) -> Self {
        Self { inner, written }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for CountingWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let mut inner = Pin::new(&mut this.inner);
        match inner.as_mut().poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.written.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl FileWriter {
    pub async fn spawn(base_path: PathBuf, cfg: Td2LocalConfig) -> anyhow::Result<Arc<Self>> {
        let (tx, rx) = flume::bounded::<WriterMsg>(128);

        // parameters
        let level = cfg.backup_comp_level;
        let max_size = cfg.backup_max_size;

        // 后台写入任务
        tokio::spawn(async move {
            use async_compression::tokio::write::ZstdEncoder;

            // helper to build part file path: <base>.part00001
            let part_path = |base: &PathBuf, idx: u32| -> PathBuf {
                let parent = base
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("."));
                let file_name = base
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "backup.bin".to_string());
                parent.join(format!("{}.part{:05}", file_name, idx))
            };

            // open a new part file and return encoder and counter
            async fn open_encoder(
                path: PathBuf,
                level: async_compression::Level,
            ) -> anyhow::Result<(
                ZstdEncoder<CountingWriter<BufWriter<tokio::fs::File>>>,
                Arc<AtomicU64>,
            )> {
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("open backup file failed: {:?}, path={:?}", e, path)
                    })?;
                let buf = BufWriter::new(file);
                let counter = Arc::new(AtomicU64::new(0));
                let cw = CountingWriter::new(buf, counter.clone());
                let enc = ZstdEncoder::with_quality(cw, level);
                tracing::info!("opened backup part file: {:?}", path);
                Ok((enc, counter))
            }

            // rotation state
            let mut part_idx: u32 = 1;
            let mut current_path = part_path(&base_path, part_idx);
            let (mut encoder, mut counter) = match open_encoder(current_path.clone(), level).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("failed to open first backup file: {:?}", e);
                    return;
                }
            };

            // 连续处理每个任务，保证同一任务的 rawblock 不被打断（文件轮转发生在任务边界）
            while let Ok(msg) = rx.recv_async().await {
                match msg {
                    WriterMsg::Task { task_id, rx } => {
                        // rotate before starting a new task if current file reached max size
                        if counter.load(Ordering::Relaxed) >= max_size {
                            // finalize current file
                            if let Err(e) = encoder.flush().await {
                                tracing::warn!("flush before rotate failed: {:?}", e);
                            }
                            if let Err(e) = encoder.shutdown().await {
                                tracing::warn!(
                                    "zstd encoder shutdown error on {:?}: {:?}",
                                    current_path,
                                    e
                                );
                            }
                            tracing::info!(
                                "rotating backup file: {:?} ({} bytes >= {}), opening next",
                                current_path,
                                counter.load(Ordering::Relaxed),
                                max_size
                            );
                            part_idx += 1;
                            current_path = part_path(&base_path, part_idx);
                            match open_encoder(current_path.clone(), level).await {
                                Ok((enc, cnt)) => {
                                    encoder = enc;
                                    counter = cnt;
                                }
                                Err(e) => {
                                    tracing::error!("failed to open next backup file: {:?}", e);
                                    break;
                                }
                            }
                        }

                        // Write header
                        if let Err(e) = encoder.write_all(b"TASK").await {
                            tracing::error!("write header failed for task {}: {:?}", task_id, e);
                            break;
                        }
                        if let Err(e) = encoder.write_all(&(task_id as u64).to_le_bytes()).await {
                            tracing::error!("write task_id failed for task {}: {:?}", task_id, e);
                            break;
                        }

                        // Consume blocks of this task
                        while let Ok(bytes) = rx.recv_async().await {
                            // simple length-prefixed frame
                            if let Err(e) =
                                encoder.write_all(&(bytes.len() as u32).to_le_bytes()).await
                            {
                                tracing::error!(
                                    "write block len failed for task {}: {:?}",
                                    task_id,
                                    e
                                );
                                break;
                            }
                            if let Err(e) = encoder.write_all(&bytes).await {
                                tracing::error!(
                                    "write block bytes failed for task {}: {:?}",
                                    task_id,
                                    e
                                );
                                break;
                            }
                        }

                        if let Err(e) = encoder.write_all(b"ENDT").await {
                            tracing::error!(
                                "write end marker failed for task {}: {:?}",
                                task_id,
                                e
                            );
                            break;
                        }
                        if let Err(e) = encoder.flush().await {
                            tracing::error!("flush failed for task {}: {:?}", task_id, e);
                            break;
                        }
                        // Do not rotate mid-task. Rotation check will be done before next task.
                    }
                }
            }

            // finalize last part
            if let Err(e) = encoder.shutdown().await {
                tracing::warn!("zstd encoder shutdown error on {:?}: {:?}", current_path, e);
            }
            tracing::info!("file writer shutdown: {:?}", base_path);
        });

        Ok(Arc::new(Self { tx }))
    }

    pub async fn enqueue_task(
        &self,
        task_id: usize,
        rx: flume::Receiver<Bytes>,
    ) -> anyhow::Result<()> {
        self.tx.send_async(WriterMsg::Task { task_id, rx }).await?;
        Ok(())
    }
}

pub struct Worker {
    id: i32,
    context: Td2LocalContext,
    task_rx: Receiver<TaosToLocalTask>,
    file_writer: Arc<FileWriter>,
    cancel: CancellationToken,
}

impl Worker {
    pub fn new(
        id: i32,
        context: Td2LocalContext,
        task_rx: Receiver<TaosToLocalTask>,
        file_writer: Arc<FileWriter>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            id,
            context,
            task_rx,
            file_writer,
            cancel,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("worker: {} start", self.id);

        let taos = match &self.context.pool {
            Some(p) => p.get().await?,
            None => {
                anyhow::bail!("pool must be set in context");
            }
        };

        let mut count = 0;
        loop {
            tokio::select! {
                res = self.task_rx.recv_async() => {
                    match res {
                        Ok(task) => {
                            let task_id = task.id;
                            tracing::info!("worker: {} received task: {}", self.id, task_id);
                            self.run_impl(&taos,task).await?;
                            count += 1;
                            tracing::info!("worker: {} finished task: {}", self.id, task_id);
                        },
                        Err(_) => {
                            tracing::info!("worker: {} task channel closed", self.id);
                            break;
                        },
                    }
                }
                _ = self.cancel.cancelled() => {
                    tracing::info!("worker: {} cancelled", self.id);
                    break;
                }
            }
        }

        tracing::info!("worker: {} shutdown, total: {}", self.id, count);
        Ok(())
    }

    async fn run_impl(&self, taos: &Taos, task: TaosToLocalTask) -> anyhow::Result<()> {
        tracing::info!(
            "worker: {} handle task, {}: {:?}",
            self.id,
            task.id,
            task.sql
        );

        // 为该任务创建一个通道，将查询结果发送到 file writer
        let (tx, rx) = flume::bounded::<Bytes>(16);
        self.file_writer.enqueue_task(task.id, rx).await?;

        // 执行查询
        let mut res = taos.query(&task.sql).await?;
        // 将查询结果写入本地文件
        let mut blocks = res.blocks();
        while let Some(block) = blocks.try_next().await? {
            let raw: &[u8] = block.as_raw_bytes();
            let bytes = Bytes::copy_from_slice(raw);
            // 发送到 file writer
            tx.send_async(bytes).await?;
        }

        // 关闭该任务的发送端，通知 file writer 该任务结束
        drop(tx);

        Ok(())
    }
}

#[derive(Debug)]
pub struct TaosToLocalTask {
    pub id: usize,
    pub sql: String,
}

pub struct TaskProducer {
    context: Td2LocalContext,
    task_tx: flume::Sender<TaosToLocalTask>,
    cancel: CancellationToken,
}

impl TaskProducer {
    pub fn new(
        context: Td2LocalContext,
        task_tx: flume::Sender<TaosToLocalTask>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            task_tx,
            cancel,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tokio::select! {
            _ = self.cancel.cancelled() => {
                tracing::info!("task producer cancelled");
            },
            res = self.run_impl() => {
                if let Err(e) = res {
                    tracing::error!("task producer error: {:?}", e);
                }
            }
        }
        tracing::info!("task producer shutdown");
        Ok(())
    }

    async fn run_impl(&self) -> anyhow::Result<()> {
        let (pool, query_obj, schema) = match (
            &self.context.pool,
            &self.context.query_obj,
            &self.context.schema,
        ) {
            (Some(p), Some(q), Some(s)) => (p, q, s),
            _ => {
                anyhow::bail!("pool, query_obj, schema must be set in context");
            }
        };
        let taos = pool.get().await?;
        const PAGE_SIZE: usize = 10_0000;

        match query_obj {
            QueryObject::Database(db) => {
                let stables = schema.stables();
                let mut id = 1;
                for stb in stables {
                    // 对每个 stble 分片
                    let sum: i64 = schema.stable_count(&taos, db, &stb).await?;
                    let mut offset = 0;
                    while offset < sum {
                        let sql = format!(
                            "SELECT * FROM `{}`.`{}` ORDER BY _c0 ASC LIMIT {} OFFSET {}",
                            db, stb, PAGE_SIZE, offset
                        );
                        let task = TaosToLocalTask { id, sql };
                        tracing::info!("send task: {}", task.id);
                        self.task_tx.send_async(task).await?;

                        offset += PAGE_SIZE as i64;
                        id += 1;
                    }
                }
            }
            QueryObject::SuperTables((db, stables)) => {
                let mut id = 1;
                for stb in stables {
                    // 对每个 stble 分片
                    let sum = schema.stable_count(&taos, db, stb).await?;
                    let mut offset = 0;
                    while offset < sum {
                        let sql = format!(
                            "SELECT * FROM `{}`.`{}` ORDER BY _c0 ASC LIMIT {} OFFSET {}",
                            db, stb, PAGE_SIZE, offset
                        );
                        let task = TaosToLocalTask { id, sql };
                        tracing::info!("send task: {:?}", task);
                        self.task_tx.send_async(task).await?;
                        id += 1;
                        offset += PAGE_SIZE as i64;
                    }
                }
            }
            QueryObject::Select((_db, select)) => {
                let task = TaosToLocalTask {
                    id: 1,
                    sql: select.clone(),
                };
                tracing::info!("send task: {:?}", task);
                self.task_tx.send_async(task).await?;
            }
        }

        tracing::info!("task producer finished");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Td2LocalConfigBuilder;

    use super::*;
    use tokio_util::sync::CancellationToken;

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_producer() {
        let (tx, rx) = flume::bounded::<TaosToLocalTask>(10);
        let cancel = CancellationToken::new();

        // consumer
        let cancel_consumer = cancel.clone();
        let consumer_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_consumer.cancelled() => {
                        break;
                    },
                    res = rx.recv_async() => {
                        match res {
                            Ok(task) => {
                                println!("Received task {}: {}", task.id, task.sql);
                            },
                            Err(_) => {
                                break;
                            },
                        }
                }}
            }
            anyhow::Ok(())
        });

        let from = "taos+ws://192.168.2.139:6041/test".into_dsn().unwrap();
        let to = "local:/Users/yangzy/taosx/backup".into_dsn().unwrap();
        let config = Td2LocalConfigBuilder::new(None, from.clone(), to.clone())
            .build()
            .unwrap();
        let mut ctx = Td2LocalContext::new(None, from.clone(), to.clone(), config);
        // 连接池
        let pool = TaosBuilder::from_dsn(&from).unwrap().pool().unwrap();
        ctx.pool = Some(pool);
        // 备份对象
        let qo = QueryObject::try_from_dsn(&from).unwrap();
        ctx.query_obj = Some(qo.clone());
        // Schema
        let taos = ctx.pool.as_ref().unwrap().get().await.unwrap();
        let meta = qo.fetch_schema(&taos).await.unwrap();
        ctx.schema = Some(meta);

        // producer
        let producer = TaskProducer::new(ctx, tx, cancel.clone());
        let producer_handle = tokio::spawn(async move { producer.run().await });

        let _ = producer_handle.await;
        let _ = consumer_handle.await;
    }
}
