use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use std::vec;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use csv::{ByteRecord, Reader, ReaderBuilder, StringRecord};
use faststr::FastStr;
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt, TryStreamExt};
use notify::{Config, Event, RecursiveMode, Watcher};
use regex::Regex;
use serde::{Deserialize, Serialize};
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Itertools, TaosBuilder};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use taosx_ipc::types::dsv::DataSourceValidation;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn, Instrument, Span};

use crate::core_metrics::{get_metrics_arc_from_i64, CoreMetrics};
use crate::sink::channel_based_transformer;
use crate::sink::ipc_metric::IpcMetrics;
use crate::utils::breakpoints;
use crate::utils::dsn::json_to_dsn;
use crate::utils::port_pool::PortPool;
use crate::{utils, Parser, Transferred};

type MsgSender = flume::Sender<std::result::Result<RecordBatch, ArrowError>>;
trait CsvReaderExt: Send + Sync + std::io::Read {}

impl<T: Send + Sync + std::io::Read> CsvReaderExt for T {}

const TOTAL_CSV_FILES: FastStr = FastStr::from_static_str("total_csv_files");
const TOTAL_CSV_FILES_COMPLETED: FastStr = FastStr::from_static_str("total_csv_files_completed");
const TOTAL_CSV_FILES_COMPLETED_ROWS: FastStr =
    FastStr::from_static_str("total_csv_files_completed_rows");

const CSV_FILES: FastStr = FastStr::from_static_str("csv_files");
const CSV_FILES_COMPLETED: FastStr = FastStr::from_static_str("csv_files_completed");
const CSV_FILES_COMPLETED_ROWS: FastStr = FastStr::from_static_str("csv_files_completed_rows");

pub async fn query_to_csv(mut from: Dsn, to: Dsn) -> Result<()> {
    let sql = from.params.remove("query").unwrap();
    let builder = TaosBuilder::from_dsn(from)?;
    let taos = builder.build().await?;
    let mut rs = taos.query(sql).await?;
    let names = rs.filed_names();

    let file = to.path.expect("csv file not found");
    let file = tokio::fs::File::create(file).await?;
    let mut csv = csv_async::AsyncWriter::from_writer(file);

    csv.write_record(names).await?;

    let mut rows = rs.rows();

    while let Some(row) = rows.try_next().await? {
        csv.write_record(
            row.into_value_iter()
                .map(|v| format!("{}", v))
                .collect_vec(),
        )
        .await?;
    }

    csv.flush().await?;

    Ok::<(), anyhow::Error>(())
}

pub async fn list_csv_file(path: &str) -> Result<Vec<String>> {
    CsvSource::csv_path(path).await
}

pub async fn csv_header(
    paths: Vec<impl AsRef<str>>,
    file_pattern: Option<String>,
    has_header: bool,
    skip: usize,
    delimiter: Option<u8>,
    quote: Option<u8>,
    comment: Option<u8>,
    sample: usize,
    sort: usize,
) -> Result<CsvHeader> {
    let mut header = Vec::new();
    let option = CsvOption {
        has_header,
        skip: Some(skip),
        delimiter: delimiter.unwrap_or(b','),
        quote,
        comment,
        file_pattern: file_pattern.clone(),
        sort,
        ..Default::default()
    };

    for path in &paths {
        let path_header =
            tokio::time::timeout(Duration::from_secs(60), option.read_header(path.as_ref()))
                .await
                .context("Reading CSV header timeout(60s)")?
                .context("Failed to read CSV header")?;
        if !CsvSource::is_same_header(&header, &path_header, has_header) {
            bail!(
                "CSV file \"{}\" format is different from others",
                path.as_ref()
            );
        }
        header = path_header;
    }

    let mut values = Vec::new();

    if sample > 0 {
        if let Some(path) = paths.first() {
            values = CsvSource::sample(
                path.as_ref(),
                file_pattern,
                has_header,
                skip,
                delimiter,
                quote,
                comment,
                sample,
                sort,
            )
            .await?;
        };
    }

    Ok(CsvHeader {
        columns: header.len(),
        headers: if has_header { header } else { vec![] },
        values,
    })
}

// pub const METRIC_CSV_FILES: &str = "metrics.csv.files";
// pub const CSV_READ_RECORDS: &str = "metrics.csv.csv_read_records";
// pub const CSV_READ_RECORD_BATCHES: &str = "metrics.csv.csv_read_record_batches";

async fn csv_to_taos_with_channel(
    mut from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    cancel: CancellationToken,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> Result<()> {
    // load metrics
    let metrics_arc = get_metrics_arc_from_i64(task_id).await;

    tracing::info!("CSV to Taos, from: {from}, to: {to}");

    let builder = taos::TaosBuilder::from_dsn(to)?;
    let pool = builder.pool()?;
    let worker_cancel = cancel.child_token();
    let (msg, ack) =
        channel_based_transformer(pool, worker_cancel, parser, Some("csv"), task_id, notify)
            .await?;

    let ack_handler = tokio::spawn(async move {
        let mut count = 0;
        while let Ok(_ack) = ack.recv_async().await {
            count += 1;
        }
        info!("CSV worker finished, total record batches: {}", count);
    });

    let mut source = CsvSource::new(task_id, &mut from, msg.clone(), metrics_arc.clone()).await?;
    // metrics::counter!(METRIC_CSV_FILES, source.readers.len() as u64);
    info!("spawn CSV worker");
    let worker = tokio::spawn({
        let cancel_clone = cancel.clone();
        let from_clone = from.clone();
        async move {
            info!(
                "Reading CSV with config(concurrent: {}, batch_size: {})",
                source.option.concurrent, source.option.batch_size
            );
            let handlers = source.read(metrics_arc.clone()).await?;
            for handler in handlers {
                tokio::task::yield_now().await;
                handler.await??;
            }
            // notify new files
            if source.option.new_file_notify {
                // processing new files
                tokio::spawn({
                    let cancel_clone = cancel_clone.clone();
                    let from_clone = from_clone.clone();
                    async move {
                        while !cancel_clone.is_cancelled() {
                            // iterate over each path, find the update_time exceeds notify_interval
                            let now = Utc::now();
                            let mut files = Vec::new();
                            if let Some(files_map) =
                                NOTIFY_NEW_FILES.get(&task_id.unwrap_or_default())
                            {
                                files_map.scan(|path, update_time| {
                                    let time_delta = (now - *update_time).num_seconds() as u64;
                                    if Duration::from_secs(time_delta)
                                        > source.option.notify_interval
                                    {
                                        files.push(path.clone());
                                    }
                                });
                            }
                            // process the files
                            for path in files {
                                let mut from_clone = from_clone.clone();
                                from_clone.path = Some(path.clone());
                                let mut source = CsvSource::new(
                                    task_id,
                                    &mut from_clone,
                                    msg.clone(),
                                    metrics_arc.clone(),
                                )
                                .await?;
                                let handlers = source.read(metrics_arc.clone()).await?;
                                for handler in handlers {
                                    tokio::task::yield_now().await;
                                    handler.await??;
                                }
                                // remove the file from notify list
                                let _ = NOTIFY_NEW_FILES
                                    .entry(task_id.unwrap_or_default())
                                    .and_modify(|files_map| {
                                        files_map.remove(&path);
                                    });
                            }
                            sleep(Duration::from_secs(2)).await;
                        }
                        Ok::<(), anyhow::Error>(())
                    }
                    .instrument(Span::current())
                });
                // add a watcher to monitor the new files
                let mut watcher = notify::recommended_watcher({
                    move |event: notify::Result<notify::Event>| match event {
                        Ok(event) => {
                            process_notify_event(task_id, event);
                        }
                        Err(e) => {
                            tracing::error!("CSV source, notify event error: {e}");
                        }
                    }
                })?;
                // set poll interval
                let option = Config::default().with_poll_interval(source.option.notify_interval);
                let _ = watcher.configure(option);
                // start watch
                watcher.watch(
                    Path::new(from.path.unwrap().trim()),
                    RecursiveMode::Recursive,
                )?;
                cancel_clone.cancelled().await;
            }
            Ok::<_, anyhow::Error>(())
        }
        .instrument(Span::current())
    });

    info!("CSV worker spawned");
    let abort_handle = worker.abort_handle();

    info!("Spawn task handler");
    tokio::spawn(async move {
        info!("Spawned task handler");
        tokio::select! {
            // application exit with error code
            status = worker => {
                match status? {
                    Ok(_) => {
                        tracing::info!("CSV worker done, wait for writer");
                        let _ = ack_handler.await;
                    }
                    Err(err) => {
                        ack_handler.abort();
                        anyhow::bail!("CSV exit with error: {:#}", err);
                    }
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("CSV task cancelled");
                abort_handle.abort();
            }
        };
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        // stop the connector
        tracing::info!("CSV task finished");
        Ok(())
    })
    .await??;

    Ok(())
}

static NOTIFY_NEW_FILES: LazyLock<scc::HashMap<i64, scc::HashMap<String, DateTime<Utc>>>> =
    LazyLock::new(scc::HashMap::new);

/// process the notify event
///
/// if the event is access(close) or create(file) or modify(data)
/// or modify(name(to)), continue to process the path.
fn process_notify_event(task_id: Option<i64>, event: Event) {
    match event.kind {
        notify::EventKind::Access(kind) => match kind {
            notify::event::AccessKind::Read => {
                tracing::debug!("notify event(access(read)), ignore: {:?}", event.paths)
            }
            notify::event::AccessKind::Open(_) => {
                tracing::debug!("notify event(access(open)), ignore: {:?}", event.paths);
            }
            notify::event::AccessKind::Close(_) => {
                tracing::info!("notify event(access(close)): {:?}", event.paths);
                process_new_file(task_id, event.paths);
            }
            _ => tracing::debug!(
                "notify event-(access({:?})), ignore: {:?}",
                kind,
                event.paths
            ),
        },
        notify::EventKind::Create(kind) => match kind {
            notify::event::CreateKind::File => {
                tracing::info!("notify event(create(file)): {:?}", event.paths);
                process_new_file(task_id, event.paths);
            }
            notify::event::CreateKind::Folder => {
                tracing::debug!("notify event(create(folder)), ignore: {:?}", event.paths);
            }
            _ => tracing::debug!(
                "notify event-(create({:?})), ignore: {:?}",
                kind,
                event.paths
            ),
        },
        notify::EventKind::Modify(kind) => match kind {
            notify::event::ModifyKind::Data(_) => {
                tracing::info!("notify event(modify(data)): {:?}", event.paths);
                process_new_file(task_id, event.paths);
            }
            notify::event::ModifyKind::Metadata(_) => {
                tracing::debug!("notify event(modify(metadata)), ignore: {:?}", event.paths);
            }
            notify::event::ModifyKind::Name(mode) => match mode {
                notify::event::RenameMode::To => {
                    tracing::info!("notify event(modify(name(to))): {:?}", event.paths);
                    process_new_file(task_id, event.paths);
                }
                _ => tracing::debug!(
                    "notify event(modify(name({:?}))), ignore: {:?}",
                    mode,
                    event.paths
                ),
            },
            _ => tracing::debug!(
                "notify event(modified({:?})), ignore: {:?}",
                kind,
                event.paths
            ),
        },
        notify::EventKind::Remove(kind) => match kind {
            notify::event::RemoveKind::File => {
                tracing::debug!("notify event(remove(file)), ignore: {:?}", event.paths);
            }
            notify::event::RemoveKind::Folder => {
                tracing::debug!("notify event(remove(folder)), ignore: {:?}", event.paths);
            }
            _ => tracing::debug!(
                "notify event(remove({:?})), ignore: {:?}",
                kind,
                event.paths
            ),
        },
        _ => tracing::debug!("notify event({:?}), ignore: {:?}", event.kind, event.paths),
    }
}

/// record the new files
///
/// if the file is already in the list, update the last notify time
/// otherwise, add it.
///
/// ignore temporary files.
fn process_new_file(task_id: Option<i64>, paths: Vec<PathBuf>) {
    let now = Utc::now();
    paths.iter().for_each(|path| {
        let path = path.to_str().unwrap_or_default();
        // ignore temporary files
        if path.ends_with(".swp") || path.ends_with(".swx") || path.ends_with("~") {
            return;
        }
        // record the last notify time
        NOTIFY_NEW_FILES
            .entry(task_id.unwrap_or_default())
            .and_modify(|file_map| {
                file_map
                    .entry(path.to_string())
                    .and_modify(|update_time| {
                        *update_time = now;
                    })
                    .or_insert(now);
            })
            .or_insert({
                let file_map = scc::HashMap::new();
                let _ = file_map.insert(path.to_string(), now);
                file_map
            });
    });
}

#[instrument(skip_all)]
pub async fn csv_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    _: &PortPool,
    cancel: CancellationToken,
    _with_agent: Option<(i64, String, String)>,
    _transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> Result<()> {
    csv_to_taos_with_channel(from, parser, to, cancel, task_id, notify).await

    // let port = port_pool
    //     .get()
    //     .await
    //     .ok_or_else(|| anyhow::format_err!("No available port for CSV connection"))?;
    // let socket = format!("127.0.0.1:{}", port);
    // let mut ipc_handler = build_ipc(
    //     &socket,
    //     parser,
    //     &to,
    //     Some("csv"),
    //     None,
    //     &cancel,
    //     with_agent,
    //     transferred,
    //     span,
    //     task_id.clone(),
    //     notify,
    // )
    // .await?;

    // let mut source = CsvSource::new(&mut from, port)?;
    // // metrics::counter!(METRIC_CSV_FILES, source.readers.len() as u64);
    // info!("spawn CSV worker");
    // let worker = tokio::spawn(
    //     async move {
    //         info!(
    //             "Reading CSV with config(concurrent: {}, batch_size: {})",
    //             source.concurrent, source.batch_size
    //         );
    //         let handlers = source.read().await?;
    //         for handler in handlers {
    //             tokio::task::yield_now().await;
    //             handler.await??;
    //         }
    //         Ok::<_, anyhow::Error>(())
    //     }
    //     .instrument(Span::current()),
    // );
    // info!("CSV worker spawned");
    // let abort_handle = worker.abort_handle();

    // let port_pool = port_pool.clone();

    // info!("Spawn task handler");
    // tokio::spawn(async move {
    //     info!("Spawned task handler");
    //     tokio::select! {
    //         // application exit with error code
    //         status = worker => {
    //             match status? {
    //                 Ok(_) => {
    //                     match ipc_handler.try_recv_error() {
    //                         Ok(res) => {
    //                             tracing::error!("IPC Error: {res}");
    //                             tokio::time::sleep(Duration::from_millis(100)).await;
    //                             port_pool.put(port).await;
    //                             anyhow::bail!("CSV exit with IPC error: {res}");
    //                         }
    //                         Err(err) => {
    //                             tracing::debug!("CSV worker done, cause: {:#}", err);
    //                         }
    //                     }
    //                 }
    //                 Err(err) => {
    //                     let _ = ipc_handler.close().await;
    //                     port_pool.put(port).await;
    //                     anyhow::bail!("CSV exit with error: {:#}", err);
    //                 }
    //             }
    //         },
    //         err = ipc_handler.recv_error() => {
    //             tracing::info!("have received worker thread panicked message, terminate child process");
    //             abort_handle.abort();
    //             if let Some(err) = err {
    //                 let _ = ipc_handler.close().await;
    //                 port_pool.put(port).await;
    //                 anyhow::bail!("CSV writer error: {err:#}");
    //             }
    //         },
    //         _ = cancel.cancelled() => {
    //             tracing::info!("CSV task cancelled");
    //             abort_handle.abort();
    //         }
    //     };
    //     // wait for completion
    //     tokio::time::sleep(Duration::from_millis(100)).await;
    //     // stop the connector
    //     tracing::info!("CSV task finished");
    //     // wait for handler closed
    //     let _ = ipc_handler.close().await;
    //     // put ipc port back to port pool.
    //     port_pool.put(port).await;
    //     Ok(())
    // })
    // .await??;

    // Ok(())
}

pub struct CsvHeader {
    pub columns: usize,
    pub headers: Vec<String>,
    pub values: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct CsvOption {
    pub task_id: Option<i64>,
    pub has_header: bool,
    pub headers: Vec<String>,
    pub skip: Option<usize>,
    pub delimiter: u8,
    pub quote: Option<u8>,
    pub comment: Option<u8>,
    pub batch_size: usize,
    pub skip_error: bool,
    pub null_pattern: Option<Arc<Vec<Bytes>>>,
    pub concurrent: usize,
    pub keep_processed_files: bool,
    pub file_pattern: Option<String>,
    pub new_file_notify: bool,
    pub notify_interval: Duration,
    pub sort: usize,
}

impl Default for CsvOption {
    fn default() -> Self {
        Self {
            task_id: None,
            has_header: true,
            headers: vec![],
            skip: None,
            delimiter: b',',
            quote: None,
            comment: None,
            batch_size: 1000,
            skip_error: false,
            null_pattern: None,
            concurrent: 16,
            keep_processed_files: true,
            file_pattern: None,
            new_file_notify: false,
            notify_interval: Duration::from_secs(60),
            sort: 1,
        }
    }
}

type CsvReader = Reader<Box<dyn CsvReaderExt>>;

impl CsvOption {
    fn from_dsn(dsn: Dsn) -> anyhow::Result<Self> {
        let has_header: bool = dsn
            .get("has_header")
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);
        let headers = dsn
            .get("header")
            .or(dsn.get("headers"))
            .map(|headers| {
                headers
                    .split(',')
                    .map(String::from)
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        let skip = dsn.params.get("skip").and_then(|skip_char| {
            if skip_char.is_empty() {
                None
            } else {
                Some(skip_char.parse().unwrap())
            }
        });
        let delimiter = dsn
            .params
            .get("delimiter")
            .and_then(|value| {
                let value = value.trim();
                match value.len() {
                    0 => None,
                    1 => Some(Ok(value.as_bytes()[0])),
                    _ => Some(Err(anyhow!("CSV delimiter should be a single character"))),
                }
            })
            .transpose()?
            .unwrap_or(b',');
        let quote = dsn
            .params
            .get("quote")
            .and_then(|quote_char| match quote_char.trim().as_bytes() {
                [] => None,
                [quote] if *quote == delimiter => Some(Err(anyhow!(
                    "CSV quote should not be the same as delimiter"
                ))),
                [quote] => Some(Ok(*quote)),
                _ => Some(Err(anyhow!("CSV quote should be a single character"))),
            })
            .transpose()?;
        let comment = dsn
            .params
            .get("comment")
            .and_then(|comment| match comment.trim().as_bytes() {
                [] => None,
                [comment] if *comment == delimiter => Some(Err(anyhow!(
                    "CSV comment should not be the same as delimiter"
                ))),
                [comment] => Some(Ok(*comment)),
                _ => Some(Err(anyhow!("CSV comment should be a single character"))),
            })
            .transpose()?;
        let batch_size: usize = dsn
            .params
            .get("batch_size")
            .unwrap_or(&"1".to_string())
            .parse()
            .context("Invalid batch_size value")?;
        let skip_error = dsn
            .get("skip_error")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(true)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(false);
        let null_pattern = dsn.get("null_pattern").map(|v| {
            Arc::new(
                v.trim()
                    .split(',')
                    .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                    .collect_vec(),
            )
        });
        let concurrent: usize = dsn
            .params
            .get("read_concurrency")
            .unwrap_or(&"2".to_string())
            .parse()
            .context("Invalid concurrent value")?;
        let keep_processed_files = dsn
            .get("keep_processed_files")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(true)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(true);
        let file_pattern = dsn.get("file_pattern").map(|v| v.to_string());
        let new_file_notify = dsn
            .get("new_file_notify")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(false)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(false);
        let notify_interval = dsn
            .get("notify_interval")
            .and_then(|v| {
                let duration = utils::parse_duration(v);
                match duration {
                    Ok(duration) => Some(duration),
                    Err(_) => {
                        tracing::warn!("Invalid notify_interval value {v}, use default value 60s");
                        None
                    }
                }
            })
            .unwrap_or(Duration::from_secs(60));
        let sort = dsn
            .get("sort")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(1)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(1);

        Ok(Self {
            task_id: None,
            has_header,
            headers,
            skip,
            delimiter,
            quote,
            comment,
            batch_size,
            skip_error,
            null_pattern,
            concurrent,
            keep_processed_files,
            file_pattern,
            new_file_notify,
            notify_interval,
            sort,
        })
    }

    fn builder(&self) -> ReaderBuilder {
        let mut builder = ReaderBuilder::new();
        builder
            .delimiter(self.delimiter)
            .quote(match self.quote {
                Some(quote) => quote,
                _ => b'"',
            })
            .comment(self.comment)
            .has_headers(true)
            .flexible(self.skip_error);
        builder
    }
    fn open(&self, path: impl AsRef<Path>) -> anyhow::Result<CsvReader> {
        let path = path.as_ref();
        let builder = self.builder();
        let gz = path.extension().is_some_and(|ext| ext == "gz");
        let mut reader = if gz {
            let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
            tracing::info!(
                decoder = "gz",
                "Open file {} with gz decoder",
                path.display()
            );
            let gz = flate2::read::GzDecoder::new(file);
            let reader = Box::new(gz) as Box<dyn CsvReaderExt>;
            builder.from_reader(reader)
        } else {
            let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
            let reader = Box::new(file) as Box<dyn CsvReaderExt>;
            builder.from_reader(reader)
        };

        if !self.headers.is_empty() {
            reader.set_headers(StringRecord::from(self.headers.as_slice()));
        }
        // should first fetch headers record in case it has headers.
        if !self.has_header && self.headers.is_empty() {
            let mut reader2 = if gz {
                let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
                let gz = flate2::read::GzDecoder::new(file);
                let reader = Box::new(gz) as Box<dyn CsvReaderExt>;
                builder.from_reader(reader)
            } else {
                let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
                let reader = Box::new(file) as Box<dyn CsvReaderExt>;
                builder.from_reader(reader)
            };
            let headers = reader2.byte_headers()?;
            let mut column_names = vec![];
            for n in 0..(headers.len()) {
                column_names.push(format!("c{n}"));
            }
            reader.set_headers(StringRecord::from(column_names));
        }
        // let _ = dbg!(reader.headers());
        let _ = reader.headers()?;

        if let Some(skip) = self.skip {
            let mut record = ByteRecord::new();
            for _ in 0..skip {
                let _ = reader.read_byte_record(&mut record);
            }
            let pos = reader.position();
            info!(
                skip,
                "Start reading csv from line {}, byte: {}",
                pos.line(),
                pos.byte(),
            );
        }
        Ok(reader)
    }

    fn open_many(&self, paths: &[impl AsRef<Path>]) -> anyhow::Result<Vec<CsvReader>> {
        let mut readers = Vec::with_capacity(paths.len());
        for path in paths {
            readers.push(self.open(path)?);
        }
        Ok(readers)
    }

    pub async fn read_header(&self, read_path: &str) -> Result<Vec<String>> {
        let paths = CsvSource::csv_path(read_path).await?;
        // filter by file pattern
        let paths = filter_paths_by_pattern(paths, self.file_pattern.clone())?;
        // sort files
        let paths = sort_paths(paths, self.sort);

        if paths.is_empty() {
            return Err(anyhow!(format!("there are not csv file is {}", read_path)));
        }

        let mut headers: Vec<String> = Vec::new();
        let mut readers = self.open_many(paths.as_slice())?;

        for (i, reader) in readers.iter_mut().enumerate() {
            tokio::task::yield_now().await;
            let file_headers = reader
                .headers()?
                .iter()
                .map(String::from)
                .collect::<Vec<String>>();
            if headers.is_empty() {
                headers = file_headers;
                continue;
            }
            if !CsvSource::is_same_header(&headers, &file_headers, true) {
                return Err(anyhow!(format!(
                    "header of files {} is different with others",
                    &paths[i]
                )));
            }

            headers = file_headers;
        }

        Ok(headers)
    }

    pub fn validate(&self, paths: &[impl AsRef<Path>]) -> Result<()> {
        const MAX_VALIDATE_LINES: usize = 10;
        let mut cols = 0;
        for path in paths {
            let path = path.as_ref();
            let mut reader = self.open(path)?;
            let headers = reader
                .headers()
                .with_context(|| format!("Reading file {} header error", path.display()))?;
            if headers.len() <= 1 {
                bail!("CSV fields number should greater than 1.")
            }

            if self.skip_error {
                continue;
            }

            if cols == 0 {
                cols = headers.len();
            }
            let mut record = StringRecord::new();
            for _ in 0..MAX_VALIDATE_LINES {
                let ok = reader
                    .read_record(&mut record)
                    .with_context(|| format!("Reading file {} record error", path.display()))?;
                if !ok {
                    break;
                }
                let len = record.len();
                let line = reader.position().line();
                if len == 0 {
                    continue;
                }
                if cols != len {
                    bail!(
                        "CSV file {} line {line} expect {cols} columns but has {len}",
                        path.display()
                    );
                }
            }
        }
        Ok(())
    }

    #[allow(unused)]
    #[instrument(skip(self), fields(path = %path.as_ref().display()))]
    fn open_path_into_stream(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<impl Stream<Item = Result<RecordBatch>>> {
        let reader = self.open(path)?;
        Ok(self.open_reader_into_stream(reader))
    }

    fn open_reader_into_stream(
        &self,
        mut reader: CsvReader,
    ) -> impl Stream<Item = Result<RecordBatch>> {
        let headers = reader
            .headers()
            .unwrap()
            .iter()
            .map(String::from)
            .collect::<Vec<String>>();

        info!("CSV stream reading, headers: {headers:?}");

        let batch_size = self.batch_size;
        let skip_error = self.skip_error;
        let null_pattern = self.null_pattern.clone();

        let (tx, rx) = flume::bounded(256);

        tokio::task::spawn_blocking(move || {
            let mut records = vec![Vec::with_capacity(batch_size); headers.len()];
            let mut count = 0usize;
            let mut errors = 0usize;

            for record in reader.byte_records() {
                let record = match record {
                    Ok(record) => record,
                    Err(err) => {
                        if !skip_error {
                            tx.send(Err(err).context("Reading csv records error"))?;
                            break;
                        } else {
                            warn!("skip error is enabled, ignore error: {:#}", err);
                            errors += 1;
                        }
                        continue;
                    }
                };

                if record.is_empty() {
                    continue;
                }
                #[allow(clippy::needless_range_loop)]
                for i in 0..headers.len() {
                    match record.get(i) {
                        Some(s) => {
                            if let Some(null_pattern) = &null_pattern {
                                if null_pattern.iter().any(|p| p == s) {
                                    records[i].push(None);
                                    continue;
                                }
                            }
                            let s = String::from_utf8_lossy(s);
                            let s = s.trim();
                            records[i].push(if !s.is_empty() {
                                Some(s.replace('\0', "").to_string())
                            } else {
                                None
                            });
                        }
                        None => records[i].push(None),
                    }
                }
                if records[0].len() >= batch_size {
                    count += batch_size;
                    let record_batch = RecordBatch::try_from_iter(
                        headers.iter().zip(
                            records
                                .iter()
                                .map(|s| Arc::new(StringArray::from_iter(s)) as ArrayRef),
                        ),
                    )?;
                    tx.send(Ok(record_batch))?;
                    records.iter_mut().for_each(|r| r.clear());
                }
            }

            if !records[0].is_empty() {
                count += records[0].len();
                let record_batch = RecordBatch::try_from_iter(
                    headers.iter().zip(
                        records
                            .iter()
                            .map(|s| Arc::new(StringArray::from_iter(s)) as ArrayRef),
                    ),
                )?;
                tx.send(Ok(record_batch))?; // send last batch
            }
            info!(count, errors, "CSV stream finished");
            anyhow::Ok(())
        });

        rx.into_stream()
    }
}

// CsvSource read csv file and send data to Sender
struct CsvSource {
    option: CsvOption,
    paths: Vec<String>,
    readers: Vec<Reader<Box<dyn CsvReaderExt>>>,
    sender: MsgSender,
}
unsafe impl Send for CsvSource {}
unsafe impl Sync for CsvSource {}
impl std::fmt::Debug for CsvSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSource")
            .field("readers", &self.readers.len())
            .field("paths", &self.paths)
            .field("option", &self.option)
            .finish()
    }
}

pub async fn get_paths_from_dsn_and_breakpoints(
    task_id: Option<i64>,
    dsn: &mut Dsn,
) -> anyhow::Result<Vec<String>> {
    // parse csv options
    let option = CsvOption::from_dsn(dsn.clone())?;

    // get breakpoint
    let breakpoints = get_breakpoint(task_id).unwrap_or_default();

    // dsn: csv:path/to/csv/path_1/or/file_1,path/to/csv/path_2/or/file_2?has_header=&header=&skip=&delimiter=&batch_size=&concurrent=
    let dsn_paths = match &dsn.path {
        Some(path) => {
            if path.trim().is_empty() {
                bail!("CSV path should not be empty");
            }
            path.split(",").collect_vec()
        }
        None => return Err(anyhow!("csv path is null")),
    };
    let mut paths = Vec::new();
    for path in dsn_paths {
        let csv_paths = CsvSource::csv_path(path).await?;
        for csv_path in csv_paths {
            paths.push(csv_path);
        }
    }

    // record the files in breakpoint to file list
    let paths_in_breakpoints: Vec<String> = breakpoints.keys().cloned().collect();
    let paths_in_breakpoints = sort_paths(paths_in_breakpoints, option.sort);
    paths_in_breakpoints.iter().for_each(|path| {
        add_csv_file_to_task(
            task_id,
            path,
            FileStatus::Completed,
            *breakpoints.get(path).unwrap(),
        );
    });

    // filter by breakpoint
    let paths = paths
        .into_iter()
        .filter(|path| {
            if breakpoints.contains_key(path) {
                tracing::info!("file '{path}' is already processed, skip it");
                false
            } else {
                // keep the file
                true
            }
        })
        .collect_vec();

    // filter by file pattern
    let paths = filter_paths_by_pattern(paths, option.file_pattern.clone())?;

    // sort files
    let paths = sort_paths(paths, option.sort);

    // record to file list, the status is not started
    paths.iter().for_each(|path| {
        add_csv_file_to_task(task_id, path, FileStatus::NotStarted, 0);
    });

    Ok(paths)
}

pub fn filter_paths_by_pattern(
    paths: Vec<String>,
    file_pattern: Option<String>,
) -> anyhow::Result<Vec<String>> {
    let paths = if let Some(file_pattern) = file_pattern {
        let re = Regex::new(&file_pattern)?;
        paths
            .into_iter()
            .filter(|path| {
                let file_name = Path::new(path).file_name().unwrap().to_str().unwrap();
                re.is_match(file_name)
            })
            .collect_vec()
    } else {
        paths
    };
    Ok(paths)
}

pub fn sort_paths(paths: Vec<String>, sort: usize) -> Vec<String> {
    paths
        .iter()
        .sorted_by(|a, b| {
            let a_depth = a.split('/').count();
            let b_depth = b.split('/').count();
            let a_len = a.len();
            let b_len = b.len();
            if sort == 2 {
                a_depth
                    .cmp(&b_depth)
                    .then_with(|| b.to_lowercase().cmp(&a.to_lowercase()))
                    .then_with(|| b_len.cmp(&a_len))
            } else {
                // default is 1
                a_depth
                    .cmp(&b_depth)
                    .then_with(|| a.to_lowercase().cmp(&b.to_lowercase()))
                    .then_with(|| a_len.cmp(&b_len))
            }
        })
        .map(|s| s.to_string())
        .collect_vec()
}

impl CsvSource {
    async fn new(
        task_id: Option<i64>,
        dsn: &mut Dsn,
        sender: MsgSender,
        metrics_arc: Arc<CoreMetrics>,
    ) -> Result<CsvSource> {
        // get csv option
        let mut option = CsvOption::from_dsn(dsn.clone())?;
        option.task_id = task_id;

        // get paths
        let paths = get_paths_from_dsn_and_breakpoints(task_id, dsn).await?;

        if option.concurrent == 0 {
            option.concurrent = paths.len();
        }

        // if !has_header && headers.len() == 0 {
        //     return Err(anyhow!("csv header is null"));
        // }

        let skip_validate: bool = dsn
            .get("skip_validate")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(true)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(false);
        if !skip_validate {
            option.validate(&paths)?;
        }

        // get breakpoint
        let breakpoints = get_breakpoint(task_id).unwrap_or_default();

        // extra metrics
        let total_csv_files = breakpoints.len() + paths.len();
        let total_csv_files_completed = breakpoints.len();
        let total_csv_files_completed_rows = breakpoints.values().sum::<usize>();

        let metrics = metrics_arc.ipc();
        metrics.set_extra_metric(&TOTAL_CSV_FILES, total_csv_files as u64);
        metrics.set_extra_metric(&TOTAL_CSV_FILES_COMPLETED, total_csv_files_completed as u64);
        metrics.set_extra_metric(
            &TOTAL_CSV_FILES_COMPLETED_ROWS,
            total_csv_files_completed_rows as u64,
        );
        metrics.set_extra_metric(&CSV_FILES, total_csv_files as u64);
        metrics.add_extra_metric(&CSV_FILES_COMPLETED, 0);
        metrics.add_extra_metric(&CSV_FILES_COMPLETED_ROWS, 0);

        let readers = option.open_many(paths.as_slice())?;
        Ok(CsvSource {
            readers,
            paths,
            sender,
            option: option.clone(),
        })
    }

    async fn sample(
        read_path: &str,
        file_pattern: Option<String>,
        has_header: bool,
        skip: usize,
        delimiter: Option<u8>,
        quote: Option<u8>,
        comment: Option<u8>,
        sample: usize,
        sort: usize,
    ) -> Result<Vec<Vec<String>>> {
        let paths = CsvSource::csv_path(read_path).await?;
        // filter by file pattern
        let paths = filter_paths_by_pattern(paths, file_pattern)?;
        // sort files
        let paths = sort_paths(paths, sort);

        if paths.is_empty() {
            return Err(anyhow!(format!("there are not csv file is {}", read_path)));
        }

        let mut samples: Vec<_> = Vec::new();

        for path in paths {
            let mut reader = CsvSource::csv_reader_of(
                &path,
                has_header,
                &[],
                Some(skip as u64),
                delimiter.unwrap_or(b','),
                quote,
                comment,
                false,
            )?;

            loop {
                let mut record = StringRecord::new();
                let ok = reader
                    .read_record(&mut record)
                    .with_context(|| format!("Reading file {} record error", path))?;
                if ok {
                    samples.push(
                        record
                            .iter()
                            .map(|value| value.trim().replace("\0", "").to_string())
                            .collect::<Vec<String>>(),
                    );
                } else {
                    break;
                }
                if samples.len() >= sample {
                    break;
                }
            }
        }

        Ok(samples)
    }

    fn is_same_header(
        old_header: &Vec<String>,
        new_header: &Vec<String>,
        has_header: bool,
    ) -> bool {
        old_header.is_empty()
            || (has_header && old_header == new_header)
            || (!has_header && old_header.len() == new_header.len())
    }

    async fn read(
        &mut self,
        metrics_arc: Arc<CoreMetrics>,
    ) -> Result<FuturesUnordered<JoinHandle<Result<()>>>> {
        let batch_size = self.option.batch_size;
        // let skip_error = self.skip_error;
        tracing::info!("reading csv files with batch size: {batch_size}");
        let futures = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(self.option.concurrent));
        // let total = Arc::new(AtomicU64::new(0));

        for (reader, path) in self.readers.drain(..).zip(self.paths.drain(..)) {
            let permit = semaphore.clone().acquire_owned().await?;
            let option = self.option.clone();
            let sender = self.sender.clone();
            let task_id = self.option.task_id;
            let keep_processed_files = self.option.keep_processed_files;
            let metrics_arc = metrics_arc.clone();
            // let total = total.clone();
            let future = tokio::spawn(
                async move {
                    info!("Deal with csv reader");

                    // record to file list, the status is processing
                    add_csv_file_to_task(task_id, &path, FileStatus::Processing, 0);

                    // let res =
                    //     CsvSource::deal_file(reader, port, batch_size, skip_error, null_pattern)
                    //         .await?;
                    let stream = option.open_reader_into_stream(reader);
                    tokio::pin!(stream);
                    let mut count = 0;
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        count += batch.num_rows();
                        sender.send_async(Ok(batch)).await?;
                        tracing::debug!(path, count, "send batches to writer");

                        // record to file list, the status is processing
                        add_csv_file_to_task(task_id, &path, FileStatus::Processing, count);
                    }

                    // write to breakpoint file
                    let _ = set_breakpoint(task_id, &path, count).await;

                    // record to file list, the status is completed
                    add_csv_file_to_task(task_id, &path, FileStatus::Completed, count);

                    // record in metrics
                    let metrics = metrics_arc.ipc();
                    metrics.add_extra_metric(&CSV_FILES_COMPLETED, 1);
                    metrics.add_extra_metric(&CSV_FILES_COMPLETED_ROWS, count as u64);
                    metrics.add_extra_metric(&TOTAL_CSV_FILES_COMPLETED, 1);
                    metrics.add_extra_metric(&TOTAL_CSV_FILES_COMPLETED_ROWS, count as u64);

                    // if keep_processed_files is false, delete the processed file
                    if !keep_processed_files {
                        tracing::info!("Delete processed file: {path}");
                        let _ = std::fs::remove_file(path);
                    }
                    drop(permit);
                    Ok(())
                }
                .instrument(Span::current()),
            );

            futures.push(future);
        }

        Ok(futures)
    }

    async fn csv_path(path: &str) -> Result<Vec<String>> {
        let ext = "csv";
        if path.trim().is_empty() {
            bail!("CSV path should not be empty");
        }
        let p = PathBuf::from(path);

        let path_clone = path.to_string();

        tokio::task::spawn_blocking(move || {
            // path is csv file
            if p.is_file() {
                return Ok(vec![path_clone]);
            }
            if p.is_dir() {
                let all_files = utils::files::get_files_in_dir(&path_clone, ext)
                    .with_context(|| format!("Reading CSV path {p:?} error"))?;
                return Ok(all_files);
            }
            match glob::glob(&path_clone) {
                Ok(paths) => {
                    let paths: Vec<_> = paths
                        .into_iter()
                        .map_ok(|path| path.display().to_string())
                        .try_collect()?;
                    if paths.is_empty() {
                        return Ok(vec![path_clone.to_string()]);
                    }
                    Ok(paths)
                }
                Err(err) => {
                    anyhow::bail!("Invalid csv path/glob {path_clone:?}: {err:#}");
                }
            }
        })
        .await?
        .with_context(|| format!("Reading CSV file {path:?} error"))
    }

    fn csv_reader_of(
        path: impl AsRef<Path>,
        has_header: bool,
        headers: &[String],
        skip: Option<u64>,
        delimiter: u8,
        quote: Option<u8>,
        comment: Option<u8>,
        skip_error: bool,
    ) -> Result<Reader<Box<dyn CsvReaderExt>>> {
        let path = path.as_ref();
        let gz = path.extension().is_some_and(|ext| ext == "gz");
        let mut reader = if gz {
            let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
            tracing::info!(
                decoder = "gz",
                "Open file {} with gz decoder",
                path.display()
            );
            let gz = flate2::read::GzDecoder::new(file);
            let reader = Box::new(gz) as Box<dyn CsvReaderExt>;
            ReaderBuilder::new()
                .delimiter(delimiter)
                .quote(match quote {
                    Some(quote) => quote,
                    _ => b'"',
                })
                .comment(comment)
                .has_headers(true)
                .flexible(skip_error)
                .from_reader(reader)
        } else {
            let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
            let reader = Box::new(file) as Box<dyn CsvReaderExt>;
            ReaderBuilder::new()
                .delimiter(delimiter)
                .quote(match quote {
                    Some(quote) => quote,
                    _ => b'"',
                })
                .comment(comment)
                .has_headers(true)
                .flexible(skip_error)
                .from_reader(reader)
        };

        if !headers.is_empty() {
            reader.set_headers(StringRecord::from(headers));
        }
        // should first fetch headers record in case it has headers.
        if !has_header && headers.is_empty() {
            let mut reader2 = if gz {
                let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
                let gz = flate2::read::GzDecoder::new(file);
                let reader = Box::new(gz) as Box<dyn CsvReaderExt>;
                ReaderBuilder::new()
                    .delimiter(delimiter)
                    .quote(match quote {
                        Some(quote) => quote,
                        _ => b'"',
                    })
                    .comment(comment)
                    .has_headers(true)
                    .flexible(skip_error)
                    .from_reader(reader)
            } else {
                let file = File::open(path).with_context(|| format!("Open file {path:?} error"))?;
                let reader = Box::new(file) as Box<dyn CsvReaderExt>;
                ReaderBuilder::new()
                    .delimiter(delimiter)
                    .quote(match quote {
                        Some(quote) => quote,
                        _ => b'"',
                    })
                    .comment(comment)
                    .has_headers(true)
                    .flexible(skip_error)
                    .from_reader(reader)
            };
            let headers = reader2.byte_headers()?;
            let mut column_names = vec![];
            for n in 0..(headers.len()) {
                column_names.push(format!("c{n}"));
            }
            reader.set_headers(StringRecord::from(column_names));
        }
        // let _ = dbg!(reader.headers());
        let _ = reader.headers();

        if let Some(skip) = skip {
            let mut record = ByteRecord::new();
            for _ in 0..skip {
                let _ = reader.read_byte_record(&mut record);
            }
            let pos = reader.position();
            info!(
                skip,
                "Start reading csv from line {}, byte: {}",
                pos.line(),
                pos.byte(),
            );
        }
        Ok(reader)
    }
}

/*
#[tokio::test]
async fn test_csv_source() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    let span = tracing::info_span!("task::spawned", trace_id = tracing::field::Empty);
    use std::str::FromStr;

    let (notify, _) = flume::unbounded();
    csv_to_taos(
        Dsn::from_str("csv:../tests/csv/table-ns/ns.csv?batch_size=1000").unwrap(),
        Some(
            Parser::from_str(
                r#"{
  "parse": {
    "time": { "as": "timestamp(ns)", "alias": "time" },
    "field0": { "as": "int" },
    "field7": { "as": "int" }
  },
  "model": {
    "name": "f_{field0}",
    "using": "stb1",
    "tags": ["field0"],
    "columns": ["time", "field7"]
  }
}"#,
            )
            .unwrap(),
        ),
        Dsn::from_str("taos:///testns").unwrap(),
        &Default::default(),
        Default::default(),
        None,
        None,
        span.clone(),
        notify,
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let taos = TaosBuilder::from_dsn("taos:///testns")?.build().await?;
    let u: usize = taos.query_one("select count(*) from stb1").await?.unwrap();
    assert_eq!(u, 200);
    Ok(())
}
*/

pub async fn is_csv_valid(from: &Dsn) -> DataSourceValidation {
    let (sender, _) = flume::bounded(0);
    if let Err(err) = CsvSource::new(
        None,
        &mut from.clone(),
        sender,
        Arc::new(CoreMetrics::IPC(IpcMetrics::default())),
    )
    .await
    {
        DataSourceValidation::invalid("csv".to_string(), err.to_string())
    } else {
        DataSourceValidation::valid("csv".to_string(), None)
    }
}

pub async fn set_breakpoint(task_id: Option<i64>, path: &str, amount: usize) -> anyhow::Result<()> {
    if task_id.is_none_or(|id| id == -1 || id == 0) {
        return Ok(());
    }
    let task_id = format!("{}", task_id.unwrap_or(0));
    let amount = format!("{}", amount);
    // set breakpoint, if failed, retry after 1s
    let mut result = breakpoints::breakpoints_set(&task_id, path, &amount);
    while let Err(e) = result {
        tracing::error!("set breakpoint for task {task_id} failed, error: {e}, retry after 1s");
        tokio::time::sleep(Duration::from_secs(1)).await;
        result = breakpoints::breakpoints_set(&task_id, path, &amount);
    }
    tracing::info!("set breakpoint for task {task_id} success, '{path}: {amount}'");
    Ok(())
}

pub fn get_breakpoint(task_id: Option<i64>) -> anyhow::Result<HashMap<String, usize>> {
    if task_id.is_none_or(|id| id == -1 || id == 0) {
        return Ok(HashMap::new());
    }
    let task_id = format!("{}", task_id.unwrap_or(0));
    let result = breakpoints::breakpoints_get_all(&task_id);
    match result {
        Ok(records) => {
            let map = records
                .iter()
                .map(|(path, amount)| {
                    let path = path.to_string();
                    let amount = amount.parse::<usize>().unwrap_or(0);
                    (path, amount)
                })
                .collect();
            Ok(map)
        }
        Err(e) => {
            tracing::error!("get breakpoint for task {task_id} failed, error: {e}");
            Err(e)
        }
    }
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, Eq, PartialEq)]
pub enum FileStatus {
    #[default]
    NotStarted,
    Processing,
    Completed,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct TaskFile {
    path: String,
    status: FileStatus,
    amount: usize,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
}

static TASK_FILES: LazyLock<scc::HashMap<String, Vec<TaskFile>>> = LazyLock::new(scc::HashMap::new);

pub async fn get_csv_files_from_task(
    task_id: Option<i64>,
    from: &str,
) -> anyhow::Result<Vec<TaskFile>> {
    let task_id_str = format!("{}", task_id.unwrap_or(0));
    if let Some(files) = TASK_FILES.get_async(&task_id_str).await {
        Ok(files.get().clone())
    } else {
        // 重新生成文件列表
        // let dsn: Dsn = from.parse()?;
        let dsn = json_to_dsn(&serde_json::Value::String(from.to_string()))?;
        let _ = get_paths_from_dsn_and_breakpoints(task_id, &mut dsn.clone()).await?;
        if let Some(files) = TASK_FILES.get_async(&task_id_str).await {
            Ok(files.get().clone())
        } else {
            Ok(vec![])
        }
    }
}

fn add_csv_file_to_task(task_id: Option<i64>, path: &str, status: FileStatus, amount: usize) {
    let task_id = format!("{}", task_id.unwrap_or(0));
    let start_time = Some(Utc::now());
    let end_time = if status.eq(&FileStatus::Completed) {
        Some(Utc::now())
    } else {
        None
    };
    TASK_FILES
        .entry(task_id)
        .and_modify(|files| {
            for file in files.iter_mut() {
                if file.path == path {
                    // update status and amount
                    file.status = status;
                    file.amount = amount;
                    file.end_time = end_time;
                    return;
                }
            }
            files.push(TaskFile {
                path: path.to_string(),
                status,
                amount,
                start_time,
                end_time,
            });
        })
        .or_insert(vec![TaskFile {
            path: path.to_string(),
            status,
            amount,
            start_time,
            end_time,
        }]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_list_csv_file() {
        let path = "test.csv".to_string();
        create_csv_file(&path).await.unwrap();

        let paths = CsvSource::csv_path("./*.csv").await.unwrap();
        dbg!(&paths);

        delete_csv_file(&path).unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_read_header() {
        let path = "test.csv".to_string();
        create_csv_file(&path).await.unwrap();

        let option = CsvOption::default();
        let header = option.read_header(path.as_ref()).await.unwrap();

        assert_eq!(header, vec!["ts".to_string(), "payload".to_string()]);
        delete_csv_file(&path).unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_header_timeout() {
        let path = "/".to_string();

        let option = CsvOption::default();
        let result = tokio::time::timeout(Duration::from_secs(5), async move {
            option.read_header(path.as_ref()).await
        })
        .await
        .context("Reading CSV header timeout(5s)");
        dbg!(&result);
        match result {
            Ok(res) => assert!(res.is_err()),
            Err(err) => assert_eq!(err.to_string(), "Reading CSV header timeout(5s)"),
        }
    }

    #[tokio::test]
    async fn test_sample() {
        let path = tempfile::NamedTempFile::new()
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        create_csv_file(&path).await.unwrap();

        let samples = CsvSource::sample(
            &path,
            None,
            true,
            1,
            Some(b','),
            Some(b'"'),
            Some(b'#'),
            1,
            1,
        )
        .await
        .unwrap();

        assert_eq!(
            samples,
            vec![vec![
                "2001-01-01T00:00:01Z".to_string(),
                "location,1,2,3".to_string()
            ]]
        );
        delete_csv_file(&path).unwrap();
    }

    #[tokio::test]
    async fn test_csv_readers() {
        let paths = vec![tempfile::NamedTempFile::new().unwrap().path().to_path_buf()];
        for path in &paths {
            create_csv_file(path).await.unwrap();
        }

        let option = CsvOption {
            quote: Some(b'"'),
            comment: Some(b'#'),
            batch_size: 1,
            concurrent: 1,
            ..Default::default()
        };

        // has header
        let mut readers = option.open_many(&paths).unwrap();
        while let Some(mut reader) = readers.pop() {
            let headers = reader
                .headers()
                .unwrap()
                .iter()
                .map(String::from)
                .collect::<Vec<String>>();
            assert_eq!(headers, vec!["ts".to_string(), "payload".to_string()]);
            let mut record = StringRecord::new();
            let _ = reader.read_record(&mut record);
            let record = record
                .iter()
                .map(|value| value.trim().replace("\0", "").to_string())
                .collect::<Vec<String>>();
            assert_eq!(
                record,
                vec![
                    "2001-01-01T00:00:00Z".to_string(),
                    "location,1,2,3".to_string()
                ]
            );
        }

        let option = CsvOption {
            has_header: false,
            quote: Some(b'"'),
            comment: Some(b'#'),
            batch_size: 1,
            concurrent: 1,
            ..Default::default()
        };

        let mut readers = option.open_many(&paths).unwrap();
        while let Some(mut reader) = readers.pop() {
            let headers = reader
                .headers()
                .unwrap()
                .iter()
                .map(String::from)
                .collect::<Vec<String>>();
            assert_eq!(headers, vec!["c0".to_string(), "c1".to_string()]);
            let mut record = StringRecord::new();
            let _ = reader.read_record(&mut record);
            assert_eq!(record, vec!["ts".to_string(), "payload".to_string()]);
            let _ = reader.read_record(&mut record);
            let record = record
                .iter()
                .map(|value| value.trim().replace("\0", "").to_string())
                .collect::<Vec<String>>();
            assert_eq!(
                record,
                vec![
                    "2001-01-01T00:00:00Z".to_string(),
                    "location,1,2,3".to_string()
                ]
            );
        }

        let option = CsvOption {
            headers: vec!["ts".to_string(), "payload".to_string()],
            quote: Some(b'"'),
            comment: Some(b'#'),
            batch_size: 1,
            concurrent: 1,
            ..Default::default()
        };

        // does not has header, but with custom headers
        let mut readers = option.open_many(&paths).unwrap();
        while let Some(mut reader) = readers.pop() {
            let headers = reader
                .headers()
                .unwrap()
                .iter()
                .map(String::from)
                .collect::<Vec<String>>();
            assert_eq!(headers, vec!["ts".to_string(), "payload".to_string()]);
            let mut record = StringRecord::new();
            let _ = reader.read_record(&mut record);
            assert_eq!(record, vec!["ts".to_string(), "payload".to_string()]);
            let _ = reader.read_record(&mut record);
            let record = record
                .iter()
                .map(|value| value.trim().replace("\0", "").to_string())
                .collect::<Vec<String>>();
            assert_eq!(
                record,
                vec![
                    "2001-01-01T00:00:00Z".to_string(),
                    "location,1,2,3".to_string()
                ]
            );
        }
        for path in &paths {
            let _ = delete_csv_file(path);
        }
    }

    #[tokio::test]
    async fn test_validate() {
        let paths = vec![tempfile::NamedTempFile::new().unwrap().path().to_path_buf()];
        for path in &paths {
            create_csv_file(path).await.unwrap();
        }

        let option = CsvOption {
            quote: Some(b'"'),
            comment: Some(b'#'),
            batch_size: 1,
            concurrent: 1,
            ..Default::default()
        };

        option.validate(&paths).unwrap();

        for path in &paths {
            let _ = delete_csv_file(path);
        }
    }

    #[tokio::test]
    async fn test_gzipped_csv() {
        let path = "test.csv.gz".to_string();
        create_csv_file(&path).await.unwrap();

        let option = CsvOption {
            quote: Some(b'"'),
            comment: Some(b'#'),
            batch_size: 1,
            concurrent: 1,
            ..Default::default()
        };

        let header = option.read_header(path.as_ref()).await.unwrap();

        assert_eq!(header, vec!["ts".to_string(), "payload".to_string()]);

        let (tx, _) = flume::bounded(0);
        let csv = CsvSource::new(
            None,
            &mut "csv:./test.csv.gz".parse().unwrap(),
            tx,
            Arc::new(CoreMetrics::IPC(IpcMetrics::default())),
        )
        .await;
        assert!(csv.is_ok(), "{csv:?}");
        delete_csv_file(&path).unwrap();
    }

    #[tokio::test]
    async fn test_set_breakpoint() {
        let task_id = Some(1);
        let path = "test.csv";
        let amount = 100;
        let result = set_breakpoint(task_id, path, amount).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_breakpoint_taskid_none() {
        let task_id = None;
        let path = "test.csv";
        let amount = 100;
        let result = set_breakpoint(task_id, path, amount).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_breakpoint() {
        let task_id = Some(1);
        let result = get_breakpoint(task_id);
        dbg!(&result);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_breakpoint_taskid_none() {
        let _ = set_breakpoint(None, "test.csv", 100).await;
        let result = get_breakpoint(None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let _ = set_breakpoint(Some(0), "test.csv", 100).await;
        let result = get_breakpoint(Some(0));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let _ = set_breakpoint(Some(-1), "test.csv", 100).await;
        let result = get_breakpoint(Some(-1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_paths_sort() {
        let paths = vec![
            "test_1.csv".to_string(),
            "test_2.csv".to_string(),
            "test_3.csv".to_string(),
            "./sub1/test_1.csv".to_string(),
            "./sub1/test_2.csv".to_string(),
            "./sub2/test_1.csv".to_string(),
            "./sub2/test_2.csv".to_string(),
            "./sub2/sub1/test_1.csv".to_string(),
            "./sub2/sub2/test_1.csv".to_string(),
        ];
        // sort by name
        let paths = paths.iter().sorted_by(|a, b| {
            let a_depth = a.split('/').count();
            let b_depth = b.split('/').count();
            a_depth
                .cmp(&b_depth)
                .then_with(|| b.to_lowercase().cmp(&a.to_lowercase()))
                .then_with(|| b.len().cmp(&a.len()))
        });
        dbg!(&paths);
    }

    #[test]
    fn test_file_pattern() {
        let paths = vec![
            "/data/test-csv/?-*-[-]-a-c-1-123.csv".to_string(),
            "/data/test-csv/x-*-[-]-a-c-1-123.csv".to_string(),
            "/data/test-csv/?-x-[-]-a-c-1-123.csv".to_string(),
            "/data/test-csv/?-*-x-]-a-c-1-123.csv".to_string(),
            "/data/test-csv/?-*-[-x-a-c-1-123.csv".to_string(),
            "/data/test-csv/?-*-[-]-x-c-1-123.csv".to_string(),
            "/data/test-csv/?-*-[-]-a-e-1-123.csv".to_string(),
            "/data/test-csv/?-*-[-]-a-c-12-123.csv".to_string(),
            "/data/test-csv/xxxx?-*-[-]-a-c-1-123.csv".to_string(),
            "/data/test-csv/?-*-[-]-a-c-1-123.csvxxxx".to_string(),
        ];
        let re = Regex::new(r"^\?\-\*\-\[\-\]\-[ab]\-[^ef]\-.\-.*\.csv$").unwrap();
        let paths = paths
            .into_iter()
            .filter(|path| re.is_match(path))
            .collect_vec();
        dbg!(&paths);
    }

    #[tokio::test]
    async fn test_open_reader_into_stream() {
        let path = tempfile::NamedTempFile::new()
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        create_csv_file(&path).await.unwrap();

        let option = CsvOption {
            quote: Some(b'"'),
            comment: Some(b'#'),
            batch_size: 1,
            concurrent: 1,
            ..Default::default()
        };
        let reader = option.open_many(&[path]).unwrap().pop().unwrap();
        let mut stream = option.open_reader_into_stream(reader);
        while let Some(batch) = stream.next().await {
            dbg!(&batch);
        }
    }

    async fn create_csv_file(path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        // let csv;
        if path.extension().is_some_and(|ext| ext == "gz") {
            let file = std::fs::File::create(path)?;
            let gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut csv = csv::Writer::from_writer(gz);

            csv.write_record(["ts", "payload"])?;
            csv.write_record(["2001-01-01T00:00:00Z", "   location,1,2,3"])?;
            csv.write_record(["2001-01-01T00:00:01Z", "location,1,2,3   "])?;
            csv.flush()?;
        } else {
            let file = tokio::fs::File::create(path).await?;
            let mut csv = csv_async::AsyncWriter::from_writer(file);

            csv.write_record(&["ts", "payload"]).await?;
            csv.write_record(&["2001-01-01T00:00:00Z", "   location,1,2,3"])
                .await?;
            csv.write_record(&["2001-01-01T00:00:01Z", "location,1,2,3   "])
                .await?;
            csv.flush().await?;
        }
        Ok(())
    }

    fn delete_csv_file(path: impl AsRef<Path>) -> Result<(), std::io::Error> {
        std::fs::remove_file(path)
    }
}
