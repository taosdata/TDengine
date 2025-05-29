use std::{
    collections::HashMap,
    ops::{ControlFlow, Deref},
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_compute_ext::RecordBatchExt;
use arrow_schema::{ArrowError, Schema};
use faststr::FastStr;
use futures::{
    pin_mut,
    stream::{FuturesOrdered, StreamExt},
    FutureExt,
};
use parking_lot::Mutex;
use persist_queue::{fs::EntryPosition, writer::Request};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::{
    sync::{oneshot, watch},
    task::JoinSet,
};

use tokio_stream::wrappers::WatchStream;
use tokio_util::sync::CancellationToken;

use taosx_ipc::ack::LushAck;
use tracing::{info_span, Instrument};

use crate::{
    core_metrics::CoreMetrics,
    utils::{self, breakpoints::BreakpointDb, futs_helper::select_cancel},
};

const PERSIST_QUEUE_BREAKPOINT_KEY: &str = "persist_queue_breakpoint";
const DEFAULT_READ_BATCH_SIZE: usize = 1000;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_BATCH_CHUNK_SIZE: usize = 100;

const METRICS_PERSIST_READ_OFFSET: FastStr = FastStr::from_static_str("persist_read_offset");
const METRICS_PERSIST_WRITE_OFFSET: FastStr = FastStr::from_static_str("persist_write_offset");
const METRICS_PERSIST_INFLIGHT_ACKS: FastStr = FastStr::from_static_str("persist_inflight_acks");

#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub schemas: HashMap<Arc<Schema>, PathBuf>,
    pub record_metrics: bool,
    /// number of rows in one RecordBatch
    pub batch_size: Option<usize>,
    /// max timeout wait for rows in one RecordBatch
    pub batch_timeout: Option<Duration>,
    /// max number of RecordBatches processed at a time
    pub batch_chunk_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct PersistComponentConfig {
    pub record_metrics: bool,
    /// number of rows in one RecordBatch
    pub batch_size: Option<usize>,
    /// max timeout wait for rows in one RecordBatch
    pub batch_timeout: Option<Duration>,
    /// max number of RecordBatches processed at a time
    pub batch_chunk_size: Option<usize>,
}

#[derive(Clone)]
pub struct PersistComponents {
    pub config: Arc<PersistComponentConfig>,
    pub components: HashMap<Arc<Schema>, PersistComponent>,
}

#[derive(Clone)]
pub struct PersistComponent {
    pub dir: PathBuf,
    pub schema: Arc<Schema>,
    pub payload_tx: flume::Sender<Vec<u8>>,
    pub request_tx: flume::Sender<persist_queue::writer::Request<EntryPosition>>,
    pub reader_rx: flume::Receiver<persist_queue::Entry<EntryPosition>>,
    pub breakpoint_db: BreakpointDb,
    pub config: Arc<PersistComponentConfig>,
}

impl PersistComponent {
    pub fn new(
        dir: PathBuf,
        schema: Arc<Schema>,
        payload_tx: flume::Sender<Vec<u8>>,
        request_tx: flume::Sender<persist_queue::writer::Request<EntryPosition>>,
        reader_rx: flume::Receiver<persist_queue::Entry<EntryPosition>>,
        breakpoint_db: BreakpointDb,
        config: Arc<PersistComponentConfig>,
    ) -> Self {
        Self {
            dir,
            schema,
            payload_tx,
            request_tx,
            reader_rx,
            breakpoint_db,
            config,
        }
    }
}

#[tracing::instrument(name = "persist_queue_rw_runners", skip_all)]
pub async fn get_persist(
    mut config: PersistConfig,
    token: &CancellationToken,
) -> anyhow::Result<(PersistComponents, JoinSet<anyhow::Result<()>>)> {
    let persist_config = Arc::new(PersistComponentConfig {
        record_metrics: config.record_metrics,
        batch_size: config.batch_size,
        batch_timeout: config.batch_timeout,
        batch_chunk_size: config.batch_chunk_size,
    });
    let mut components = HashMap::with_capacity(config.schemas.len());
    let mut tasks = tokio::task::JoinSet::new();
    for (schema, dir) in std::mem::take(&mut config.schemas) {
        let channel_batch_size = config.batch_size.map(|v| v * 2).unwrap_or(1000);
        // 创建 persist queue
        let mut queue = persist_queue::fs::FsQueue::builder(&dir)
            .build()
            .await
            .context("build persist queue error")?;
        // 获取 breakpoint 数据库
        let path = dir.join("breakpoint");
        let breakpoint_db = tokio::task::spawn_blocking(move || BreakpointDb::open(&path))
            .await?
            .context("open persist breakpoint db error")?;

        // 创建 reader
        let (reader_tx, reader_rx) = flume::bounded(channel_batch_size);
        let breakpoint = match breakpoint_db
            .get(PERSIST_QUEUE_BREAKPOINT_KEY)
            .await
            .context("get persist queue breakpoint error")?
        {
            Some(breakpoint) => {
                let position = serde_json::from_str(&breakpoint)
                    .context("deserialize persist queue positoin error")?;
                persist_queue::fs::ReadFrom::LastPosition(position)
            }
            None => persist_queue::fs::ReadFrom::Earliest,
        };
        let reader = queue
            .new_reader(breakpoint)
            .await
            .context("build persist queue reader error")?;
        let mut reader_builder = persist_queue::reader::Reader::builder(reader, reader_tx);
        if let Some(batch_size) = config.batch_size {
            reader_builder = reader_builder.batch_size(batch_size)
        }
        tasks.spawn(
            reader_builder
                .build()
                .run(token.child_token())
                .map(|res| res.context("persist reader task error"))
                .instrument(info_span!("persist queue reader runner")),
        );

        // 创建 writer
        let w = queue.new_writer::<Vec<u8>>().await?;
        let (payload_tx, payload_rx) = flume::bounded(channel_batch_size);
        let (request_tx, request_rx) = flume::bounded(1);
        let mut writer_builder =
            persist_queue::writer::Writer::builder(w, payload_rx).request_rx(request_rx);
        if let Some(batch_size) = config.batch_size {
            writer_builder = writer_builder.chunk_size(batch_size);
        }
        tasks.spawn(
            writer_builder
                .build()
                .run(token.child_token())
                .map(|res| res.context("persist writer task error"))
                .instrument(info_span!("persist queue writer runner")),
        );
        components.insert(
            schema.clone(),
            PersistComponent::new(
                dir,
                schema,
                payload_tx,
                request_tx,
                reader_rx,
                breakpoint_db,
                persist_config.clone(),
            ),
        );
    }

    Ok((
        PersistComponents {
            config: persist_config,
            components,
        },
        tasks,
    ))
}

type PersistReceiver = flume::Receiver<(
    Result<RecordBatch, ArrowError>,
    Option<oneshot::Sender<LushAck>>,
)>;

/// ack_tx 用来回复数据源
#[tracing::instrument(name = "persist_queue_tasks", skip_all)]
pub fn get_stream<S>(
    persist: PersistComponent,
    stream: S,
    ack_tx: flume::Sender<LushAck>,
    token: &CancellationToken,
    metrics: Option<Arc<CoreMetrics>>,
    tasks: &mut JoinSet<anyhow::Result<()>>,
    wait_for_ack: bool,
) -> anyhow::Result<PersistReceiver>
where
    S: futures::stream::Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
{
    let batch_chunk_size = persist
        .config
        .batch_chunk_size
        .unwrap_or(DEFAULT_BATCH_CHUNK_SIZE);

    let metrics = Arc::new(PersistMetrics::new(metrics));
    metrics.reset();

    // 从 ipc_reader 获取数据写入持久化组件
    // 将接收到的 recordbatch 写入 persist queue
    let (write_tx, write_rx) = flume::bounded::<Vec<Vec<u8>>>(batch_chunk_size);
    tasks.spawn({
        let token = token.clone();
        let payload_tx = persist.payload_tx;
        let ack_tx = ack_tx.clone();
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::debug!("persist queue write records task exit");
            });
            'OUTER: loop {
                let Some(Ok(rows)) = select_cancel(write_rx.recv_async(), &token).await else {
                    break;
                };

                let mut written = 0;
                for row in rows {
                    if select_cancel(payload_tx.send_async(row), &token)
                        .await
                        .is_none_or(|v| v.is_err())
                    {
                        break 'OUTER;
                    }
                    written += 1;
                }
                // 回复上游数据源
                let ack = LushAck {
                    code: 0,
                    message: None,
                    context: Some(
                        serde_json::json!({
                            "stream": "flat",
                            "written":  written
                        })
                        .to_string(),
                    ),
                };
                if select_cancel(ack_tx.send_async(ack), &token)
                    .await
                    .is_none_or(|v| v.is_err())
                {
                    break;
                }
            }

            Ok(())
        }
        .in_current_span()
    });

    tasks.spawn({
        let token = token.clone();
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::debug!("persist queue read stream task exit");
            });
            pin_mut!(stream);
            let mut msg_stream = stream.ready_chunks(batch_chunk_size);
            'OUTER: loop {
                let Some(Some(batches)) = select_cancel(msg_stream.next(), &token).await else {
                    break;
                };
                let batches_rows = tokio::task::spawn_blocking({
                    let ack_tx = ack_tx.clone();
                    move || {
                        batches
                            .into_par_iter()
                            .filter_map(|batch| {
                                let batch = match batch {
                                    Ok(batch) => batch,
                                    Err(e) => {
                                        let ack = LushAck {
                                            code: 0xFFFF,
                                            message: Some(format!("Parse message error: {e:#}")),
                                            context: Some(
                                                serde_json::json!({
                                                    "stream": "flat",
                                                })
                                                .to_string(),
                                            ),
                                        };
                                        ack_tx.send(ack).ok()?;
                                        return None;
                                    }
                                };
                                let rows: Vec<Box<serde_json::value::RawValue>> = {
                                    match batch
                                        .to_json_rows()
                                        .context("serialize recordbatch to json rawvalue error")
                                    {
                                        Ok(rows) => rows,
                                        Err(e) => return Some(Err(e)),
                                    }
                                };
                                let rows = rows
                                    .into_iter()
                                    .map(|v| v.get().as_bytes().to_vec())
                                    .collect::<Vec<_>>();
                                Some(anyhow::Ok(rows))
                            })
                            .collect::<anyhow::Result<Vec<_>>>()
                    }
                })
                .await??;
                if batches_rows.is_empty() {
                    continue;
                }
                for rows in batches_rows {
                    if select_cancel(write_tx.send_async(rows), &token)
                        .await
                        .is_none_or(|v| v.is_err())
                    {
                        break 'OUTER;
                    }
                }
            }

            anyhow::Ok(())
        }
        .in_current_span()
    });

    // 从持久化组件中，读取数据
    let (batch_tx, batch_rx) = flume::bounded(1);
    let (acks_tx, acks_rx) = flume::bounded(0);
    let (read_batch_tx, read_batch_rx) = flume::bounded(batch_chunk_size);
    let (metrics_tx, metrics_rx) = watch::channel(EntryPosition::default());

    tasks.spawn({
        let batch_size = persist.config.batch_size.unwrap_or(DEFAULT_READ_BATCH_SIZE);
        let batch_timeout = persist.config.batch_timeout.unwrap_or(DEFAULT_READ_TIMEOUT);
        let token = token.clone();
        let reader_rx = persist.reader_rx;
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::debug!("persist queue read records task exit");
            });
            let stream = {
                use tokio_stream::StreamExt;
                reader_rx
                    .into_stream()
                    .chunks_timeout(batch_size, batch_timeout)
            };
            pin_mut!(stream);
            loop {
                let Some(Some(entries)) = select_cancel(stream.next(), &token).await else {
                    break;
                };
                let Some(position) = entries.last().map(|entry| entry.position) else {
                    continue;
                };
                let batch = tokio::task::spawn_blocking(move || {
                    entries
                        .into_par_iter()
                        .map(|entry| {
                            serde_json::from_slice(&entry.payload)
                                .context("deserialize entry error")
                        })
                        .collect::<anyhow::Result<Vec<serde_json::Value>>>()
                })
                .await??;

                if select_cancel(read_batch_tx.send_async((batch, position)), &token)
                    .await
                    .is_none_or(|v| v.is_err())
                {
                    break;
                }
            }
            anyhow::Ok(())
        }
        .in_current_span()
    });

    tasks.spawn({
        let schema = persist.schema.clone();
        let token = token.clone();
        let breakpoint_db = persist.breakpoint_db.clone();
        let metrics_tx = metrics_tx.clone();
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::debug!("persist queue deserialize records task exit");
            });
            let mut batch_stream = read_batch_rx.into_stream().ready_chunks(batch_chunk_size);
            'OUTER: loop {
                let Some(Some(batches_rows)) = select_cancel(batch_stream.next(), &token).await
                else {
                    break;
                };
                let schema = schema.clone();
                let batches = tokio::task::spawn_blocking(move || {
                    batches_rows
                        .into_par_iter()
                        .map(|(payloads, position)| {
                            let mut decoder =
                                arrow::json::reader::ReaderBuilder::new(schema.clone())
                                    .with_strict_mode(true)
                                    .build_decoder()
                                    .context("build arrow json reader error")?;
                            decoder
                                .serialize(&payloads)
                                .context("arrow json decoder serialize error")?;
                            let batch = decoder
                                .flush()
                                .context("arrow json decoder flush error")?
                                .context("record batch not found in arrow read decoder")?;

                            let metadata = schema
                                .metadata()
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .chain([(
                                    PERSIST_QUEUE_BREAKPOINT_KEY.to_string(),
                                    serde_json::to_string(&position)?,
                                )]);
                            let mut metadata = HashMap::<_, _, _>::from_iter(metadata);
                            metadata.insert(
                                PERSIST_QUEUE_BREAKPOINT_KEY.to_string(),
                                serde_json::to_string(&position)?,
                            );
                            let schema: Schema = schema.deref().clone();

                            Ok((
                                batch
                                    .with_schema(Arc::new(schema.with_metadata(metadata)))
                                    .context("build batch with new metadata error")?,
                                position,
                            ))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()
                })
                .await??;

                for (batch, position) in batches {
                    if wait_for_ack {
                        let (ack_tx, ack_rx) = oneshot::channel::<LushAck>();
                        // 发送给 FuturesOrdered 用于等待 ack
                        if select_cancel(acks_tx.send_async(ack_rx), &token)
                            .await
                            .is_none_or(|v| v.is_err())
                        {
                            break 'OUTER;
                        }
                        // 把 recordbatch 发送给下游，等待下游的 ack
                        if select_cancel(
                            batch_tx.send_async((Ok::<_, ArrowError>(batch), Some(ack_tx))),
                            &token,
                        )
                        .await
                        .is_none_or(|v| v.is_err())
                        {
                            break 'OUTER;
                        }
                    } else {
                        // 把 recordbatch 发送给下游，不等待下游的 ack
                        breakpoint_db
                            .set(
                                PERSIST_QUEUE_BREAKPOINT_KEY,
                                &serde_json::to_string(&position)?,
                            )
                            .await?;
                        if metrics_tx.send(position).is_err() {
                            break 'OUTER;
                        }
                        if select_cancel(
                            batch_tx.send_async((Ok::<_, ArrowError>(batch), None)),
                            &token,
                        )
                        .await
                        .is_none_or(|v| v.is_err())
                        {
                            break 'OUTER;
                        }
                    }
                }
            }

            Ok(())
        }
        .in_current_span()
    });

    // 读取 ack 数据，将进度写入 breakpoint 数据库
    tasks.spawn({
        let breakpoint_db = persist.breakpoint_db;
        let token = token.clone();
        let metrics = metrics.clone();
        let dir = persist.dir.clone();
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::debug!("persist queue ack task exit");
                metrics.reset();
            });
            use oneshot::error::RecvError;
            // 使用 FuturesOrdered 保证等待 ack 的顺序
            let mut futs = FuturesOrdered::new();

            loop {
                tokio::select! {
                    biased;
                    res = futs.next(), if !futs.is_empty() => {
                        let Some(Ok::<LushAck, RecvError>(ack)) = res else {
                            break
                        };
                        metrics.sub_persist_inflight_acks();

                        // 无论成功与否，都写断点
                        if let Some(context) = ack.context() {
                            let context: HashMap<String, serde_json::Value> =
                                serde_json::from_str(context)
                                .context("deserialize ack context error")?;
                            if let Some(position) = context.get(PERSIST_QUEUE_BREAKPOINT_KEY).and_then(|s| s.as_str()) {
                                breakpoint_db
                                    .set(PERSIST_QUEUE_BREAKPOINT_KEY, position)
                                    .await
                                    .context("set ack breakpoint error")?;
                                // 更新读进度
                                let position: persist_queue::fs::EntryPosition =
                                        serde_json::from_str(position).context("deserialize position error")?;
                                if metrics_tx.send(position).is_err() {
                                    break
                                }
                            }
                        }

                        // 下游处理不了，退出当前任务等待重启
                        if !ack.success() {
                            if let Some(err) = ack.message() {
                                tracing::error!("persist queue received failed ack: {err}");
                            }
                            break
                        }
                    },
                    res = acks_rx.recv_async(), if futs.len() < batch_chunk_size => {
                        let Ok(ack_rx) = res else {
                            break
                        };
                        futs.push_back(ack_rx);
                        metrics.add_persist_inflight_acks();
                        if tracing::enabled!(tracing::Level::TRACE) {
                            tracing::trace!(
                                dir = dir.display().to_string(),
                                inflight_acks = metrics.get_persist_inflight_acks(),
                                "update persist queue inflight acks",
                            );
                        }
                    },
                    _ = token.cancelled() => break
                }
            }
            anyhow::Ok(())
        }.in_current_span()
    });

    // 更新 metrics
    tasks.spawn({
        let token = token.clone();
        let request_tx = persist.request_tx;
        let mut watch_stream = WatchStream::from_changes(metrics_rx);
        let dir = persist.dir.clone();
        async move {
            let _metrics_guard = utils::defer::defer(|| {
                tracing::debug!("persist queue metrics task exit");
                metrics.reset()
            });
            loop {
                tokio::select! {
                    res = watch_stream.next() => {
                        let Some(read_position) = res else {
                            break
                        };
                        if update_rw_position(&dir, &request_tx, Some(read_position), metrics.clone(), token.child_token()).await.is_break() {
                            break
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        if update_rw_position(&dir, &request_tx, None, metrics.clone(), token.child_token()).await.is_break() {
                            break
                        }
                    }
                    _ = token.cancelled() => break
                };
            }
            Ok(())
        }.in_current_span()
    });

    Ok(batch_rx)
}

#[tracing::instrument(skip_all)]
async fn update_rw_position(
    dir: impl AsRef<Path>,
    request_tx: &flume::Sender<Request<EntryPosition>>,
    read_position: Option<EntryPosition>,
    metrics: Arc<PersistMetrics>,
    token: CancellationToken,
) -> ControlFlow<()> {
    use persist_queue::EntryPosition;
    let (tx, rx) = oneshot::channel();
    if select_cancel(request_tx.send_async(Request::Position(tx)), &token)
        .await
        .is_none_or(|v| v.is_err())
    {
        return ControlFlow::Break(());
    }

    let Some(Ok(write_position)) = select_cancel(rx, &token).await else {
        return ControlFlow::Break(());
    };
    metrics.set_persist_write_offset(write_position.offset());

    if let Some(position) = read_position {
        metrics.set_persist_read_offset(position.offset());
    }
    if tracing::enabled!(tracing::Level::TRACE) {
        tracing::trace!(
            dir = dir.as_ref().display().to_string(),
            read_offset = metrics.get_persist_read_offset(),
            write_offset = metrics.get_persist_write_offset(),
            "update read/write offset"
        );
    }

    ControlFlow::Continue(())
}

struct PersistMetrics {
    core_metrics: Option<Arc<CoreMetrics>>,

    pub persist_read_offset: AtomicU64,
    pub persist_write_offset: AtomicU64,
    pub persist_inflight_acks: AtomicU64,

    instant: Mutex<std::time::Instant>,
}

impl PersistMetrics {
    fn new(core_metrics: Option<Arc<CoreMetrics>>) -> Self {
        Self {
            core_metrics,
            persist_read_offset: AtomicU64::default(),
            persist_write_offset: AtomicU64::default(),
            persist_inflight_acks: AtomicU64::default(),
            instant: Mutex::new(std::time::Instant::now()),
        }
    }

    fn set_persist_read_offset(&self, offset: u64) {
        self.persist_read_offset
            .store(offset, std::sync::atomic::Ordering::SeqCst);
        self.update();
    }

    fn get_persist_read_offset(&self) -> u64 {
        self.persist_read_offset
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn set_persist_write_offset(&self, offset: u64) {
        self.persist_write_offset
            .store(offset, std::sync::atomic::Ordering::SeqCst);
        self.update();
    }

    fn get_persist_write_offset(&self) -> u64 {
        self.persist_write_offset
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn add_persist_inflight_acks(&self) {
        self.persist_inflight_acks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.update();
    }

    fn sub_persist_inflight_acks(&self) {
        self.persist_inflight_acks
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.update();
    }

    fn get_persist_inflight_acks(&self) -> u64 {
        self.persist_inflight_acks
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reset(&self) {
        let Some(core_metrics) = self.core_metrics.clone() else {
            return;
        };
        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_READ_OFFSET, 0);
        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_WRITE_OFFSET, 0);
        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_INFLIGHT_ACKS, 0);
    }

    fn update(&self) {
        let Some(core_metrics) = self.core_metrics.clone() else {
            return;
        };
        let Some(mut instant) = self.instant.try_lock() else {
            return;
        };
        if instant.elapsed() <= Duration::from_millis(100) {
            return;
        }

        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_READ_OFFSET, self.get_persist_read_offset());
        core_metrics.ipc().set_extra_metric(
            &METRICS_PERSIST_WRITE_OFFSET,
            self.get_persist_write_offset(),
        );
        core_metrics.ipc().set_extra_metric(
            &METRICS_PERSIST_INFLIGHT_ACKS,
            self.get_persist_inflight_acks(),
        );
        *instant = std::time::Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayRef, RecordBatch, StringBuilder, TimestampNanosecondBuilder, UInt8Builder,
    };
    use arrow_schema::{DataType, Field, TimeUnit};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    /// cargo nextest run --workspace 会启用 serde_json 的 arbitrary_precision feature，导致 arrow_json 反序列化报错
    /// https://github.com/apache/arrow-rs/issues/5069
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn persist_test() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let token = CancellationToken::new();
        let schema = Arc::new(build_schema());

        let (mut components, mut tasks) = get_persist(
            PersistConfig {
                record_metrics: false,
                schemas: HashMap::from_iter([(schema.clone(), dir.into_path())]),
                batch_size: Some(10),
                batch_timeout: Some(Duration::from_millis(100)),
                batch_chunk_size: None,
            },
            &token,
        )
        .await?;

        let breakpoint_db = components
            .components
            .get(&schema.clone())
            .unwrap()
            .breakpoint_db
            .clone();

        let (batch_tx, batch_rx) = flume::bounded(1);
        let (ack_tx, ack_rx) = flume::bounded(1);

        let batch_receiver = get_stream(
            components.components.remove(&schema.clone()).unwrap(),
            batch_rx.into_stream(),
            ack_tx,
            &token,
            None,
            &mut tasks,
            true,
        )?;

        let mut test_tasks = JoinSet::new();
        const TEST_COUNT: usize = 30;

        // 发送 TEST_COUNT 个 batch
        test_tasks.spawn(async move {
            let mut rows_count = 0;
            for _ in 0..TEST_COUNT {
                let mut ts = TimestampNanosecondBuilder::new();
                let mut topic = StringBuilder::new();
                let mut qos = UInt8Builder::new();
                let mut payload = StringBuilder::new();
                // 每个 batch 有 TEST_COUNT 条数据
                for _ in 0..TEST_COUNT {
                    ts.append_value(1);
                    topic.append_value("this/is/a/test/topic");
                    qos.append_value(0);
                    payload.append_value("this is a test payload");
                }
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(ts.finish()),
                    Arc::new(topic.finish()),
                    Arc::new(qos.finish()),
                    Arc::new(payload.finish()),
                ];
                let batch = RecordBatch::try_new(schema.clone(), columns)
                    .context("build recordbatch error")?;
                rows_count += batch.num_rows();
                batch_tx.send_async(Ok(batch)).await?;
            }
            assert_eq!(rows_count, TEST_COUNT * TEST_COUNT);
            anyhow::Ok(())
        });

        // 接收 persist 读出的数据
        test_tasks.spawn(async move {
            let mut rows_count = 0;
            'OUTER: while let Ok((batch, mut ack_wait_tx)) = batch_receiver.recv_async().await {
                // 从 batch 处获取 metadata
                let batch = batch?;
                let rows = batch.num_rows();
                let metadata = batch.schema_ref().metadata();
                let meta = serde_json::Map::from_iter(
                    metadata.iter().map(|(k, v)| (k.clone(), json!(v))).chain([
                        ("stream".to_string(), serde_json::json!("flat")),
                        ("written".to_string(), serde_json::json!(rows)),
                    ]),
                );
                let ctx = serde_json::Value::from(meta).to_string();
                if let Some(ack_tx) = ack_wait_tx.take() {
                    ack_tx
                        .send(LushAck {
                            code: 0,
                            message: None,
                            context: Some(ctx),
                        })
                        .ok();
                }
                rows_count += rows;
                if rows_count >= TEST_COUNT * TEST_COUNT {
                    break 'OUTER;
                }
            }
            Ok(())
        });

        // 接收 ack
        test_tasks.spawn(async move {
            let mut ack_count = 0;
            while let Ok(ack) = ack_rx.recv_async().await {
                assert!(ack.success());
                let written = ack
                    .context()
                    .and_then(|c| {
                        serde_json::from_str::<HashMap<String, serde_json::Value>>(c)
                            .ok()
                            .as_ref()
                            .and_then(|m| m.get("written"))
                            .and_then(|v| v.as_i64())
                    })
                    .unwrap_or_default();
                ack_count += written;
                if ack_count as usize >= TEST_COUNT * TEST_COUNT {
                    break;
                }
            }
            Ok(())
        });

        // tokio::time::sleep(Duration::from_secs(10)).await;

        // while let Some(res) = test_tasks.try_join_next() {
        //     println!("{res:?}")
        // }

        // while let Some(res) = tasks.try_join_next() {
        //     println!("{res:?}")
        // }

        let mut has_error = false;
        for res in test_tasks.join_all().await {
            if let Err(e) = &res {
                println!("{e:#}");
            }
            has_error = false;
        }
        let start = std::time::Instant::now();
        loop {
            let breakpoint = breakpoint_db
                .get(PERSIST_QUEUE_BREAKPOINT_KEY)
                .await?
                .context("breakpoint not found")?;
            if breakpoint == r#"{"segment_id":0,"end_offset":105300}"# {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            if start.elapsed() >= Duration::from_secs(10) {
                panic!("persist breakpoint set timeout: {breakpoint}");
            }
        }
        token.cancel();
        for res in tasks.join_all().await {
            if let Err(e) = &res {
                println!("{e:#}");
            }
            has_error = false;
        }
        assert!(!has_error);

        Ok(())
    }

    pub fn build_schema() -> Schema {
        let fields = vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("topic", DataType::Utf8, false),
            Field::new("qos", DataType::UInt8, false),
            Field::new("payload", DataType::Utf8, false),
        ];

        let meta = HashMap::from_iter([
            ("version".to_string(), "1.0".to_string()),
            ("stream".to_string(), "flat".to_string()),
            ("ack".to_string(), "lush".to_string()),
        ]);

        Schema::new_with_metadata(fields, meta)
    }
}
