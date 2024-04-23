use std::collections::HashMap;
use std::fs::File;
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use csv_lib::{ByteRecord, Reader, ReaderBuilder, StringRecord};
use futures_util::stream::FuturesUnordered;
use futures_util::TryStreamExt;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Itertools, TaosBuilder};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use taosx_ipc::prelude::{AckReaderBuilder, ArrowDataType};
use taosx_ipc::types::dsv::DataSourceValidation;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn, Instrument, Span};

use crate::utils::port_pool::PortPool;
use crate::{build_ipc, utils, Parser, Transferred};

trait CsvReaderExt: Send + Sync + std::io::Read {}

impl<T: Send + Sync + std::io::Read> CsvReaderExt for T {}

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

    Ok(())
}

pub async fn list_csv_file(path: &str) -> Result<Vec<String>> {
    CsvSource::csv_path(path)
}

pub async fn csv_header(
    paths: Vec<impl AsRef<str>>,
    has_header: bool,
    skip: usize,
    delimiter: Option<u8>,
    quote: Option<u8>,
    comment: Option<u8>,
    sample: usize,
) -> Result<CsvHeader> {
    let mut header = Vec::new();
    for path in &paths {
        let path_header = tokio::time::timeout(
            Duration::from_secs(60),
            CsvSource::read_header(path.as_ref(), has_header, delimiter, quote, comment),
        )
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
                has_header,
                skip,
                delimiter,
                quote,
                comment,
                sample,
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

pub const METRIC_CSV_FILES: &str = "metrics.csv.files";
pub const CSV_READ_RECORDS: &str = "metrics.csv.csv_read_records";
pub const CSV_READ_RECORD_BATCHES: &str = "metrics.csv.csv_read_record_batches";

#[instrument(skip_all)]
pub async fn csv_to_taos(
    mut from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> Result<()> {
    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for CSV connection"))?;
    let socket = format!("127.0.0.1:{}", port);
    let mut ipc_handler = build_ipc(
        &socket,
        parser,
        &to,
        Some("csv"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        task_id.clone(),
        notify,
    )
    .await?;

    let mut source = CsvSource::new(&mut from, port)?;
    // metrics::counter!(METRIC_CSV_FILES, source.readers.len() as u64);
    info!("spawn CSV worker");
    let worker = tokio::spawn(
        async move {
            info!(
                "Reading CSV with config(concurrent: {}, batch_size: {})",
                source.concurrent, source.batch_size
            );
            let handlers = source.read().await?;
            for handler in handlers {
                tokio::task::yield_now().await;
                handler.await??;
            }
            Ok::<_, anyhow::Error>(())
        }
        .instrument(Span::current()),
    );
    info!("CSV worker spawned");
    let abort_handle = worker.abort_handle();

    let port_pool = port_pool.clone();

    info!("Spawn task handler");
    tokio::spawn(async move {
        info!("Spawned task handler");
        tokio::select! {
            // application exit with error code
            status = worker => {
                match status? {
                    Ok(_) => {
                        match ipc_handler.try_recv_error() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                port_pool.put(port).await;
                                anyhow::bail!("CSV exit with IPC error: {res}");
                            }
                            Err(err) => {
                                tracing::debug!("CSV worker done, cause: {:#}", err);
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc_handler.close().await;
                        port_pool.put(port).await;
                        anyhow::bail!("CSV exit with error: {:#}", err);
                    }
                }
            },
            err = ipc_handler.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                abort_handle.abort();
                if let Some(err) = err {
                    let _ = ipc_handler.close().await;
                    port_pool.put(port).await;
                    anyhow::bail!("CSV writer error: {err:#}");
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
        // wait for handler closed
        let _ = ipc_handler.close().await;
        // put ipc port back to port pool.
        port_pool.put(port).await;
        Ok(())
    })
    .await??;

    Ok(())
}

pub struct CsvHeader {
    pub columns: usize,
    pub headers: Vec<String>,
    pub values: Vec<Vec<String>>,
}

// CsvSource read csv file and send data to Sender
struct CsvSource {
    readers: Vec<Reader<Box<dyn CsvReaderExt>>>,
    concurrent: usize,
    batch_size: usize,
    skip_error: bool,
    null_pattern: Option<Arc<Vec<Bytes>>>,
    port: u16,
}
unsafe impl Send for CsvSource {}
unsafe impl Sync for CsvSource {}
impl std::fmt::Debug for CsvSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSource")
            .field("concurrent", &self.concurrent)
            .field("batch_size", &self.batch_size)
            .field("skip_error", &self.skip_error)
            .field("port", &self.port)
            .finish()
    }
}
impl CsvSource {
    fn new(dsn: &mut Dsn, port: u16) -> Result<CsvSource> {
        // dsn: csv:path/to/csv/path_1/or/file_1,path/to/csv/path_2/or/file_2
        //  ?has_header=&header=&skip=&delimiter=&batch_size=&concurrent=
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
            let csv_paths = CsvSource::csv_path(path)?;
            for csv_path in csv_paths {
                paths.push(csv_path);
            }
        }

        let has_header: bool = dsn
            .remove("has_header")
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);
        let headers = dsn
            .remove("header")
            .or(dsn.remove("headers"))
            .map(|headers| {
                headers
                    .split(',')
                    .map(String::from)
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        let skip = dsn.params.remove("skip").and_then(|skip_char| {
            if skip_char.is_empty() {
                None
            } else {
                Some(skip_char.parse().unwrap())
            }
        });

        let delimiter = dsn
            .params
            .remove("delimiter")
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
            .remove("quote")
            .and_then(|quote_char| match quote_char.trim().as_bytes() {
                [] => None,
                [quote] if *quote == delimiter => Some(Err(anyhow!(
                    "CSV quote should not be the same as delimiter"
                ))),
                [quote] => Some(Ok(quote.clone())),
                _ => Some(Err(anyhow!("CSV quote should be a single character"))),
            })
            .transpose()?;

        let comment = dsn
            .params
            .remove("comment")
            .and_then(|comment| match comment.trim().as_bytes() {
                [] => None,
                [comment] if *comment == delimiter => Some(Err(anyhow!(
                    "CSV comment should not be the same as delimiter"
                ))),
                [comment] => Some(Ok(comment.clone())),
                _ => Some(Err(anyhow!("CSV comment should be a single character"))),
            })
            .transpose()?;

        let batch_size: usize = dsn
            .params
            .remove("batch_size")
            .unwrap_or("1".to_string())
            .parse()
            .context("Invalid batch_size value")?;
        let mut concurrent: usize = dsn
            .params
            .remove("read_concurrency")
            .unwrap_or("2".to_string())
            .parse()
            .context("Invalid concurrent value")?;
        if concurrent == 0 {
            concurrent = paths.len();
        }

        let skip_error = dsn
            .remove("skip_error")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(true)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(false);

        let null_pattern = dsn.remove("null_pattern").map(|v| {
            Arc::new(
                v.trim()
                    .split(',')
                    .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                    .collect_vec(),
            )
        });

        // if !has_header && headers.len() == 0 {
        //     return Err(anyhow!("csv header is null"));
        // }
        let skip_validate: bool = dsn
            .remove("skip_validate")
            .and_then(|v| {
                if v.trim().is_empty() {
                    Some(true)
                } else {
                    v.parse().ok()
                }
            })
            .unwrap_or(false);
        if !skip_validate {
            CsvSource::validate(
                &paths, has_header, &headers, skip, delimiter, quote, comment, skip_error,
            )?;
        }

        let readers = CsvSource::csv_readers(
            &paths, has_header, &headers, skip, delimiter, quote, comment, skip_error,
        )?;

        Ok(CsvSource {
            readers,
            concurrent,
            batch_size,
            skip_error,
            null_pattern,
            port,
        })
    }

    async fn read_header(
        read_path: &str,
        has_header: bool,
        delimiter: Option<u8>,
        quote: Option<u8>,
        comment: Option<u8>,
    ) -> Result<Vec<String>> {
        let clone_read_path = read_path.to_string();
        let paths =
            tokio::task::spawn_blocking(move || CsvSource::csv_path(clone_read_path.as_ref()))
                .await?
                .with_context(|| format!("Reading CSV file {read_path:?} error"))?;
        if paths.is_empty() {
            return Err(anyhow!(format!("there are not csv file is {}", read_path)));
        }

        let mut headers: Vec<String> = Vec::new();

        for path in paths {
            tokio::task::yield_now().await;
            let mut reader = CsvSource::csv_reader_of(
                &path,
                has_header,
                &[],
                None,
                delimiter.unwrap_or(b','),
                quote,
                comment,
                false,
            )?;
            let file_headers = reader
                .headers()?
                .iter()
                .map(String::from)
                .collect::<Vec<String>>();
            if !CsvSource::is_same_header(&headers, &file_headers, has_header) {
                return Err(anyhow!(format!(
                    "header of files {} is different with others",
                    &path
                )));
            }

            headers = file_headers;
        }

        Ok(headers)
    }

    async fn sample(
        read_path: &str,
        has_header: bool,
        skip: usize,
        delimiter: Option<u8>,
        quote: Option<u8>,
        comment: Option<u8>,
        sample: usize,
    ) -> Result<Vec<Vec<String>>> {
        let paths = CsvSource::csv_path(read_path)?;
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
                    samples.push(record.iter().map(String::from).collect::<Vec<String>>());
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

    async fn read(&mut self) -> Result<FuturesUnordered<JoinHandle<Result<()>>>> {
        let port = self.port;
        let batch_size = self.batch_size;
        let skip_error = self.skip_error;
        tracing::info!("reading csv files with batch size: {batch_size}");
        let futures = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(self.concurrent));

        while let Some(reader) = self.readers.pop() {
            let permit = semaphore.clone().acquire_owned().await?;
            let null_pattern = self.null_pattern.clone();
            let future = tokio::spawn(
                async move {
                    info!("Deal with csv reader");
                    let res = CsvSource::deal_file(reader, port, batch_size, skip_error, null_pattern).await?;

                    drop(permit);
                    Ok(res)
                }
                .instrument(Span::current()),
            );

            futures.push(future);
        }

        Ok(futures)
    }

    async fn deal_file(
        mut reader: Reader<Box<dyn CsvReaderExt>>,
        port: u16,
        batch_size: usize,
        skip_error: bool,
        null_pattern: Option<Arc<Vec<Bytes>>>,
    ) -> Result<()> {
        debug!("Deal with file by IPC port: {port}");
        let stream = std::net::TcpStream::connect(format!("localhost:{}", port));
        debug!("Connected to IPC stream");
        let stream = stream.unwrap();

        let headers = reader
            .headers()?
            .iter()
            .map(String::from)
            .collect::<Vec<String>>();
        let schema = CsvSource::stream_schema(&headers);
        stream.set_nonblocking(false)?;
        let ack_stream = stream.try_clone()?;

        info!("CSV stream reading...");
        let ack = tokio::task::spawn_blocking(move || {
            let ack_reader =
                AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
            let (mut total, mut ok) = (0usize, 0usize);
            for ack in ack_reader {
                total += 1;
                if !ack.success() {
                    warn!(
                        source = "csv",
                        batch = batch_size,
                        "write {batch_size} records error: {ack:?}",
                    );
                    if let Some(message) = ack.message() {
                        bail!("IPC writer error: {message}")
                    }
                } else {
                    ok += 1;
                }
            }
            info!("ACK reader finished");
            Ok((total, ok))
        });
        let wrt = tokio::task::spawn_blocking(move || {
            let stream = stream;

            let mut writer: StreamWriter<_> = StreamWriter::try_new(&stream, &schema)?;

            let mut records = vec![Vec::with_capacity(batch_size); headers.len()];
            let mut batches = 0usize;
            let mut errors = 0usize;

            for record in reader.byte_records() {
                let record = match record {
                    Ok(record) => record,
                    Err(err) => {
                        if !skip_error {
                            Err(err).context("Reading csv records error")?;
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
                    CsvSource::write_to_stream(&headers, &mut writer, &records)?;
                    records.iter_mut().for_each(Vec::clear);
                    batches += 1;
                    // metrics::counter!(CSV_READ_RECORDS, batch_size as u64);
                    // metrics::counter!(CSV_READ_RECORD_BATCHES, 1);
                }
            }

            if records[0].len() > 0 {
                CsvSource::write_to_stream(&headers, &mut writer, &records)?;
                batches += 1;
                // metrics::counter!(CSV_READ_RECORDS, records[0].len() as u64);
                // metrics::counter!(CSV_READ_RECORD_BATCHES, 1);
            }
            if errors > 0 {
                warn!("There are {} errors while reading csv records", errors);
            }

            info!("CSV stream finished");
            let _ = writer.finish();
            anyhow::Ok(batches)
        });
        let batches = wrt.await?.context("CSV writing error")?;
        let (total, ok) = ack.await??;
        if batches == total {
            if total == ok {
                tracing::info!("Current CSV stream completed");
            } else {
                tracing::info!(
                    "Current CSV stream is finished, but there's some failed batches ({})",
                    total - ok
                );
            }
        } else {
            tracing::error!(
                csv.total = batches,
                csv.ok = ok,
                "Current CSV stream seems finished"
            );
        }

        Ok(())
    }

    fn write_to_stream(
        headers: &Vec<String>,
        writer: &mut StreamWriter<&TcpStream>,
        // ack: &mut StreamReader<&TcpStream>,
        records: &Vec<Vec<Option<String>>>,
    ) -> Result<()> {
        let record_batch = RecordBatch::try_from_iter(
            headers.iter().zip(
                records
                    .iter()
                    .map(|s| Arc::new(StringArray::from_iter(s)) as ArrayRef),
            ),
        )?;
        writer.write(&record_batch)?;
        // let _ = ack.next();
        // dbg!(ack.next());
        Ok(())
    }

    fn stream_schema(headers: &Vec<String>) -> Schema {
        let mut metadata = HashMap::new();
        metadata.insert(String::from("version"), String::from("1.0"));
        metadata.insert(String::from("stream"), String::from("flat"));
        metadata.insert(String::from("ack"), String::from("lush"));

        let columns = headers
            .iter()
            .map(|header| Field::new(header, ArrowDataType::Utf8, true))
            .collect::<Vec<Field>>();

        Schema::new(columns).with_metadata(metadata)
    }

    fn csv_path(path: &str) -> Result<Vec<String>> {
        let ext = "csv";
        if path.trim().is_empty() {
            bail!("CSV path should not be empty");
        }
        let p = Path::new(path);

        // path is csv file
        if p.is_file() {
            return Ok(vec![path.to_string()]);
        }
        if p.is_dir() {
            let all_files = utils::files::get_files_in_dir(path, ext)
                .with_context(|| format!("Reading CSV path {p:?} error"))?;
            return Ok(all_files);
        }
        match glob::glob(path) {
            Ok(paths) => {
                let paths: Vec<_> = paths
                    .into_iter()
                    .map_ok(|path| path.display().to_string())
                    .try_collect()?;
                if paths.is_empty() {
                    return Ok(vec![path.to_string()]);
                }
                return Ok(paths);
            }
            Err(err) => {
                anyhow::bail!("Invalid csv path/glob {path:?}: {err:#}");
            }
        }
    }

    fn csv_readers(
        paths: &[impl AsRef<Path>],
        has_header: bool,
        headers: &[String],
        skip: Option<u64>,
        delimiter: u8,
        quote: Option<u8>,
        comment: Option<u8>,
        skip_error: bool,
    ) -> Result<Vec<Reader<Box<dyn CsvReaderExt>>>> {
        let mut readers = Vec::new();
        for path in paths {
            let reader = CsvSource::csv_reader_of(
                path, has_header, headers, skip, delimiter, quote, comment, skip_error,
            )?;
            readers.push(reader);
        }
        Ok(readers)
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
        let gz = path.extension().map_or(false, |ext| ext == "gz");
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

    fn validate(
        paths: &[impl AsRef<Path>],
        has_header: bool,
        headers: &[String],
        skip: Option<u64>,
        delimiter: u8,
        quote: Option<u8>,
        comment: Option<u8>,
        skip_error: bool,
    ) -> Result<()> {
        const MAX_VALIDATE_LINES: usize = 5;
        let mut cols = 0;
        for path in paths {
            let path = path.as_ref();
            let mut reader = CsvSource::csv_reader_of(
                path, has_header, headers, skip, delimiter, quote, comment, skip_error,
            )?;
            let headers = reader
                .headers()
                .with_context(|| format!("Reading file {} header error", path.display()))?;
            if headers.len() <= 1 {
                bail!("CSV fields number should greater than 1.")
            }

            if skip_error {
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
    return if let Err(err) = CsvSource::new(&mut from.clone(), 0) {
        DataSourceValidation::invalid("csv".to_string(), err.to_string())
    } else {
        DataSourceValidation::valid("csv".to_string(), None)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_header() {
        let path = "test.csv".to_string();
        create_csv_file(&path).await.unwrap();

        let header =
            CsvSource::read_header(path.as_ref(), true, Some(b','), Some(b'"'), Some(b'#'))
                .await
                .unwrap();

        assert_eq!(header, vec!["ts".to_string(), "payload".to_string()]);
        delete_csv_file(&path).unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_header_timeout() {
        let path = "/".to_string();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            CsvSource::read_header(path.as_ref(), true, Some(b','), Some(b'"'), Some(b'#')),
        )
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

        let samples = CsvSource::sample(&path, true, 1, Some(b','), Some(b'"'), Some(b'#'), 1)
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
            let _ = create_csv_file(path).await.unwrap();
        }

        // has header
        let mut readers =
            CsvSource::csv_readers(&paths, true, &[], None, b',', Some(b'"'), Some(b'#'), false)
                .unwrap();
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
            assert_eq!(
                record,
                vec![
                    "2001-01-01T00:00:00Z".to_string(),
                    "location,1,2,3".to_string()
                ]
            );
        }

        // does not has header
        let mut readers = CsvSource::csv_readers(
            &paths,
            false,
            &[],
            None,
            b',',
            Some(b'"'),
            Some(b'#'),
            false,
        )
        .unwrap();
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
            assert_eq!(
                record,
                vec![
                    "2001-01-01T00:00:00Z".to_string(),
                    "location,1,2,3".to_string()
                ]
            );
        }

        // does not has header, but with custom headers
        let mut readers = CsvSource::csv_readers(
            &paths,
            false,
            &["ts".to_string(), "payload".to_string()],
            None,
            b',',
            Some(b'"'),
            Some(b'#'),
            false,
        )
        .unwrap();
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
            let _ = create_csv_file(path).await.unwrap();
        }

        let _ = CsvSource::validate(&paths, true, &[], None, b',', Some(b'"'), Some(b'#'), false)
            .unwrap();

        for path in &paths {
            let _ = delete_csv_file(path);
        }
    }

    #[tokio::test]
    async fn test_gzipped_csv() {
        let path = "test.csv.gz".to_string();
        create_csv_file(&path).await.unwrap();

        let header =
            CsvSource::read_header(path.as_ref(), true, Some(b','), Some(b'"'), Some(b'#'))
                .await
                .unwrap();

        assert_eq!(header, vec!["ts".to_string(), "payload".to_string()]);

        let csv = CsvSource::new(&mut "csv:./test.csv.gz".parse().unwrap(), 0);
        assert!(csv.is_ok(), "{csv:?}");
        delete_csv_file(&path).unwrap();
    }

    async fn create_csv_file(path: impl AsRef<Path>) -> anyhow::Result<()> {
        // let csv;
        if path.ends_with(".gz") {
            let file = std::fs::File::create(path)?;
            let gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            let mut csv = csv_lib::Writer::from_writer(gz);

            csv.write_record(&["ts", "payload"])?;
            csv.write_record(&["2001-01-01T00:00:00Z", "location,1,2,3"])?;
            csv.write_record(&["2001-01-01T00:00:01Z", "location,1,2,3"])?;
            csv.flush()?;
        } else {
            let file = tokio::fs::File::create(path).await?;
            let mut csv = csv_async::AsyncWriter::from_writer(file);

            csv.write_record(&["ts", "payload"]).await?;
            csv.write_record(&["2001-01-01T00:00:00Z", "location,1,2,3"])
                .await?;
            csv.write_record(&["2001-01-01T00:00:01Z", "location,1,2,3"])
                .await?;
            csv.flush().await?;
        }
        Ok(())
    }

    fn delete_csv_file(path: impl AsRef<Path>) -> Result<(), std::io::Error> {
        std::fs::remove_file(path)
    }
}
