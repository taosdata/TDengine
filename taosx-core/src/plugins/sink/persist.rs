use std::{
    collections::HashMap,
    ops::Deref,
    path::PathBuf,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_compute_ext::RecordBatchExt;
use arrow_schema::{ArrowError, Schema};
use faststr::FastStr;
use futures::{
    FutureExt, pin_mut,
    stream::{FuturesOrdered, StreamExt},
};
use parking_lot::Mutex;
use persist_queue::fs::EntryPosition;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::{sync::oneshot, task::JoinSet};

use tokio_util::sync::CancellationToken;

use taosx_ipc::ack::LushAck;
use tracing::{Instrument, info_span};

use crate::{
    core_metrics::CoreMetrics,
    utils::{self, breakpoints::BreakpointDb, futs_helper::select_cancel},
};

const PERSIST_QUEUE_BREAKPOINT_KEY: &str = "persist_queue_breakpoint";
const DEFAULT_READ_BATCH_SIZE: usize = 1000;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_BATCH_CHUNK_SIZE: usize = 100;

const METRICS_PERSIST_READ_MESSAGES: FastStr = FastStr::from_static_str("persist_read_messages");
const METRICS_PERSIST_WRITE_MESSAGES: FastStr = FastStr::from_static_str("persist_write_messages");
const METRICS_PERSIST_RECEIVED_ACKS: FastStr = FastStr::from_static_str("persist_received_acks");
const METRICS_PERSIST_SEND_BATCHES: FastStr = FastStr::from_static_str("persist_send_batches");

static TASK_PERSIST_METRICS: LazyLock<scc::HashIndex<i64, Arc<PersistMetrics>>> =
    LazyLock::new(scc::HashIndex::new);

#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub task_id: i64,
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
    pub task_id: i64,
    pub dir: PathBuf,
    pub schema: Arc<Schema>,
    pub payload_tx: flume::Sender<Vec<u8>>,
    pub reader_rx: flume::Receiver<persist_queue::Entry<EntryPosition>>,
    pub breakpoint_db: BreakpointDb,
    pub config: Arc<PersistComponentConfig>,
}

impl PersistComponent {
    pub fn new(
        task_id: i64,
        dir: PathBuf,
        schema: Arc<Schema>,
        payload_tx: flume::Sender<Vec<u8>>,
        reader_rx: flume::Receiver<persist_queue::Entry<EntryPosition>>,
        breakpoint_db: BreakpointDb,
        config: Arc<PersistComponentConfig>,
    ) -> Self {
        Self {
            task_id,
            dir,
            schema,
            payload_tx,
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
        let breakpoint_db = tokio::task::spawn_blocking({
            let path = path.clone();
            move || BreakpointDb::open(&path)
        })
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
                    .context("deserialize persist queue position error")?;
                persist_queue::fs::ReadFrom::LastPosition(position)
            }
            None => persist_queue::fs::ReadFrom::Earliest,
        };
        tracing::info!(?path, "start persist reader at offset: {breakpoint}");
        let reader = queue
            .new_reader(breakpoint)
            .await
            .context("build persist queue reader error")?;
        let mut reader_builder = persist_queue::reader::Reader::builder(reader, reader_tx);
        if let Some(batch_size) = config.batch_size {
            reader_builder = reader_builder.batch_size(batch_size)
        }
        tasks.spawn({
            let path = path.clone();
            let token = token.child_token();
            async move {
                let _guard = utils::defer::defer(|| {
                    tracing::info!(?path, "perssit queue reader exit");
                });
                reader_builder
                    .build()
                    .run(token)
                    .map(|res| res.context("persist reader task error"))
                    .instrument(info_span!("persist queue reader runner"))
                    .await
            }
        });

        tracing::info!(?path, "start persist writer");
        // 创建 writer
        let w = queue.new_writer::<Vec<u8>>().await?;
        let (payload_tx, payload_rx) = flume::bounded(channel_batch_size);
        let mut writer_builder = persist_queue::writer::Writer::builder(w, payload_rx);
        if let Some(batch_size) = config.batch_size {
            writer_builder = writer_builder.chunk_size(batch_size);
        }
        tasks.spawn({
            let path = path.clone();
            let token = token.child_token();
            async move {
                let _guard = utils::defer::defer(|| {
                    tracing::info!(?path, "perssit queue writer exit");
                });
                writer_builder
                    .build()
                    .run(token)
                    .map(|res| res.context("persist writer task error"))
                    .instrument(info_span!("persist queue writer runner"))
                    .await
            }
        });
        components.insert(
            schema.clone(),
            PersistComponent::new(
                config.task_id,
                dir,
                schema,
                payload_tx,
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

    let metric_entry = TASK_PERSIST_METRICS
        .entry(persist.task_id)
        .or_insert(Arc::new(PersistMetrics::new(
            metrics,
            Duration::from_millis(100),
        )));
    let metrics = metric_entry.get();
    metrics.reset(true);
    tasks.spawn({
        let token = token.clone();
        let metrics = metrics.clone();
        async move {
            token.cancelled().await;
            metrics.reset(false);
            Ok(())
        }
    });

    let path = persist.dir;

    // 从 ipc_reader 获取数据写入持久化组件
    // 将接收到的 recordbatch 写入 persist queue
    let (write_tx, write_rx) = flume::bounded::<Vec<Vec<u8>>>(batch_chunk_size);
    tasks.spawn({
        let token = token.clone();
        let payload_tx = persist.payload_tx;
        let ack_tx = ack_tx.clone();
        let metrics = metrics.clone();
        let path = path.clone();
        tracing::info!(?path, "persist queue write records task start");
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::info!(?path, "persist queue write records task exit");
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
                    metrics.add_persist_write_messages();
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
        let path = path.clone();
        tracing::info!(?path, "persist queue read stream task start");
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::info!(?path, "persist queue read stream task exit");
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

    tasks.spawn({
        let batch_size = persist.config.batch_size.unwrap_or(DEFAULT_READ_BATCH_SIZE);
        let batch_timeout = persist.config.batch_timeout.unwrap_or(DEFAULT_READ_TIMEOUT);
        let token = token.clone();
        let reader_rx = persist.reader_rx;
        let metrics = metrics.clone();
        let path = path.clone();
        tracing::info!(?path, "persist queue read records task start");
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::info!(?path, "persist queue read records task exit");
            });
            let stream = {
                use tokio_stream::StreamExt;
                reader_rx
                    .into_stream()
                    .chunks_timeout(batch_size, batch_timeout)
            };
            pin_mut!(stream);
            loop {
                let Ok(res) = tokio::time::timeout(
                    Duration::from_secs(60),
                    select_cancel(stream.next(), &token),
                )
                .await
                else {
                    tracing::warn!("persist queue fetch no messages for 60s");
                    continue;
                };
                let Some(Some(entries)) = res else {
                    break;
                };
                let Some(position) = entries.last().map(|entry| entry.position) else {
                    continue;
                };
                metrics.add_persist_read_messages(entries.len() as _);
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
        let path = path.clone();
        tracing::info!(?path, "persist queue deserialize records task start");
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::info!(?path, "persist queue deserialize records task exit");
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
        let path = path.clone();
        tracing::info!(?path, "persist queue ack task start");
        async move {
            let _guard = utils::defer::defer(|| {
                tracing::info!(?path, "persist queue ack task exit");
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
                        metrics.add_persist_received_acks();

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
                        metrics.add_persist_send_batches();
                    },
                    _ = token.cancelled() => break
                }
            }
            anyhow::Ok(())
        }.in_current_span()
    });

    Ok(batch_rx)
}

struct PersistMetrics {
    core_metrics: Option<Arc<CoreMetrics>>,

    persist_read_messages: AtomicU64,
    persist_write_messages: AtomicU64,
    persist_received_acks: AtomicU64,
    persist_send_batches: AtomicU64,

    instant: Mutex<std::time::Instant>,
    update_interval: Duration,
    running: AtomicBool,
}

impl PersistMetrics {
    fn new(core_metrics: Option<Arc<CoreMetrics>>, update_interval: Duration) -> Self {
        Self {
            core_metrics,
            persist_read_messages: AtomicU64::default(),
            persist_write_messages: AtomicU64::default(),
            persist_received_acks: AtomicU64::default(),
            persist_send_batches: AtomicU64::default(),
            instant: Mutex::new(std::time::Instant::now()),
            update_interval,
            running: AtomicBool::default(),
        }
    }

    fn add_persist_read_messages(&self, additional: u64) {
        self.persist_read_messages
            .fetch_add(additional, Ordering::SeqCst);
        self.update();
    }

    fn get_persist_read_messages(&self) -> u64 {
        self.persist_read_messages.load(Ordering::SeqCst)
    }

    fn add_persist_write_messages(&self) {
        self.persist_write_messages.fetch_add(1, Ordering::SeqCst);
        self.update();
    }

    fn get_persist_write_messages(&self) -> u64 {
        self.persist_write_messages.load(Ordering::SeqCst)
    }

    fn add_persist_received_acks(&self) {
        self.persist_received_acks.fetch_add(1, Ordering::SeqCst);
        self.update();
    }

    fn get_persist_received_acks(&self) -> u64 {
        self.persist_received_acks.load(Ordering::SeqCst)
    }

    fn add_persist_send_batches(&self) {
        self.persist_send_batches.fetch_add(1, Ordering::SeqCst);
        self.update();
    }

    fn get_persist_send_batches(&self) -> u64 {
        self.persist_send_batches.load(Ordering::SeqCst)
    }

    fn reset(&self, init: bool) {
        if self
            .running
            .compare_exchange(!init, init, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        // 退出时不改变指标值
        if !init {
            return;
        }

        self.persist_read_messages.store(0, Ordering::SeqCst);
        self.persist_write_messages.store(0, Ordering::SeqCst);
        self.persist_received_acks.store(0, Ordering::SeqCst);
        self.persist_send_batches.store(0, Ordering::SeqCst);

        let Some(core_metrics) = self.core_metrics.clone() else {
            return;
        };

        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_READ_MESSAGES, 0);
        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_WRITE_MESSAGES, 0);
        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_RECEIVED_ACKS, 0);
        core_metrics
            .ipc()
            .set_extra_metric(&METRICS_PERSIST_SEND_BATCHES, 0);
    }

    fn update(&self) {
        let Some(core_metrics) = self.core_metrics.clone() else {
            return;
        };
        let Some(mut instant) = self.instant.try_lock() else {
            return;
        };
        if instant.elapsed() <= self.update_interval {
            return;
        }

        core_metrics.ipc().set_extra_metric(
            &METRICS_PERSIST_READ_MESSAGES,
            self.get_persist_read_messages(),
        );
        core_metrics.ipc().set_extra_metric(
            &METRICS_PERSIST_WRITE_MESSAGES,
            self.get_persist_write_messages(),
        );
        core_metrics.ipc().set_extra_metric(
            &METRICS_PERSIST_RECEIVED_ACKS,
            self.get_persist_received_acks(),
        );
        core_metrics.ipc().set_extra_metric(
            &METRICS_PERSIST_SEND_BATCHES,
            self.get_persist_send_batches(),
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
                task_id: 0,
                record_metrics: false,
                schemas: HashMap::from_iter([(schema.clone(), dir.path().to_path_buf())]),
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

    #[test]
    fn metrics_test() -> anyhow::Result<()> {
        let metrics = PersistMetrics::new(None, Duration::ZERO);
        metrics.add_persist_read_messages(1);
        metrics.add_persist_received_acks();
        metrics.add_persist_send_batches();
        metrics.add_persist_write_messages();
        assert_eq!(metrics.get_persist_read_messages(), 1);
        assert_eq!(metrics.get_persist_received_acks(), 1);
        assert_eq!(metrics.get_persist_send_batches(), 1);
        assert_eq!(metrics.get_persist_write_messages(), 1);

        metrics.reset(true);
        assert_eq!(metrics.get_persist_read_messages(), 0);
        assert_eq!(metrics.get_persist_received_acks(), 0);
        assert_eq!(metrics.get_persist_send_batches(), 0);
        assert_eq!(metrics.get_persist_write_messages(), 0);

        metrics.add_persist_read_messages(1);
        metrics.add_persist_received_acks();
        metrics.add_persist_send_batches();
        metrics.add_persist_write_messages();
        assert_eq!(metrics.get_persist_read_messages(), 1);
        assert_eq!(metrics.get_persist_received_acks(), 1);
        assert_eq!(metrics.get_persist_send_batches(), 1);
        assert_eq!(metrics.get_persist_write_messages(), 1);

        // 只能初始化一次
        metrics.reset(true);
        assert_eq!(metrics.get_persist_read_messages(), 1);
        assert_eq!(metrics.get_persist_received_acks(), 1);
        assert_eq!(metrics.get_persist_send_batches(), 1);
        assert_eq!(metrics.get_persist_write_messages(), 1);

        // 退出时不重置更新
        metrics.reset(false);
        assert_eq!(metrics.get_persist_read_messages(), 1);
        assert_eq!(metrics.get_persist_received_acks(), 1);
        assert_eq!(metrics.get_persist_send_batches(), 1);
        assert_eq!(metrics.get_persist_write_messages(), 1);

        // 再次初始化后可以更新
        metrics.reset(true);
        assert_eq!(metrics.get_persist_read_messages(), 0);
        assert_eq!(metrics.get_persist_received_acks(), 0);
        assert_eq!(metrics.get_persist_send_batches(), 0);
        assert_eq!(metrics.get_persist_write_messages(), 0);

        metrics.add_persist_read_messages(1);
        metrics.add_persist_received_acks();
        metrics.add_persist_send_batches();
        metrics.add_persist_write_messages();
        assert_eq!(metrics.get_persist_read_messages(), 1);
        assert_eq!(metrics.get_persist_received_acks(), 1);
        assert_eq!(metrics.get_persist_send_batches(), 1);
        assert_eq!(metrics.get_persist_write_messages(), 1);

        Ok(())
    }
}
