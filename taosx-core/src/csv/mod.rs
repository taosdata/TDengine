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
use csv_lib::{Reader, ReaderBuilder, StringRecord};
use futures_util::stream::FuturesUnordered;
use futures_util::TryStreamExt;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Itertools, TaosBuilder};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use taosx_ipc::prelude::ArrowDataType;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::utils::port_pool::PortPool;
use crate::{utils, Parser, Transferred};

pub async fn query_to_csv(mut from: Dsn, to: Dsn) -> Result<()> {
    let sql = from.params.remove("query").unwrap();
    let builder = TaosBuilder::from_dsn(from)?;
    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    if !builder.is_enterprise_edition().await? {
        anyhow::bail!(
            "Only enterprise edition is supported. If it's not your case, please contact us."
        )
    }
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

pub async fn csv_header(paths: Vec<&str>, has_header: bool) -> Result<CsvHeader> {
    let mut header = Vec::new();
    for path in paths {
        let path_header = CsvSource::read_header(path, has_header).await?;
        if !CsvSource::is_same_header(&header, &path_header, has_header) {
            return Err(anyhow!(format!(
                "header of files {} is different with others",
                &path
            )));
        }
        header = path_header;
    }

    Ok(CsvHeader {
        columns: header.len(),
        headers: if has_header { header } else { vec![] },
    })
}

fn build_ipc(
    socket: &str,
    parser: Option<Parser>,
    to: &Dsn,
    cancel: &CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<(
    std::sync::mpsc::Sender<()>,
    tokio::sync::mpsc::Receiver<String>,
)> {
    use crate::plugins::sink;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let ipc = if with_agent.is_none() {
        let builder = taos::TaosBuilder::from_dsn(to)?;
        sink::listen_tcp_socket(
            builder.pool()?,
            socket,
            sender,
            None,
            cancel.clone(),
            with_agent,
            parser,
            None,
            transferred,
        )?
    } else {
        sink::listen_tcp_socket_with_agent(
            socket,
            sender,
            None,
            cancel.clone(),
            with_agent.unwrap(),
        )?
    };
    Ok((ipc, receiver))
}

pub async fn csv_to_taos(
    mut from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> Result<()> {
    let port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for CSV connection"))?;
    let socket = format!("127.0.0.1:{}", port);
    let (abort, mut closed) = build_ipc(&socket, parser, &to, &cancel, with_agent, transferred)?;

    let worker = tokio::spawn(async move {
        let mut source = CsvSource::new(&mut from, port)?;
        let handlers = source.read().await?;
        for handler in handlers {
            tokio::task::yield_now().await;
            handler.await??;
        }
        Ok::<_, anyhow::Error>(())
    });

    let port_pool = port_pool.clone();

    tokio::spawn(async move {
        tokio::select! {
            // application exit with error code
            status = worker => {
                match status? {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        log::info!("CSV worker done successfully");
                        let _ = abort.send(());
                    }
                    Err(err) => {
                        let _ = abort.send(());
                        anyhow::bail!("CSV exit with error: {:#}", err);
                    }
                }
            },
            err = closed.recv() => {
                log::info!("have received worker thread panicked message, terminate child process");
                if let Some(err) = err {
                    let _ = abort.send(());
                    anyhow::bail!("CSV writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                log::info!("CSV task cancelled");
            }
        };
        // send an empty tuple
        let _ = abort.send(());
        // stop the connector
        log::info!("CSV task Done");
        // put ipc port back to port pool.
        port_pool.put(port);
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    })
    .await??;

    Ok(())
}

pub struct CsvHeader {
    pub columns: usize,
    pub headers: Vec<String>,
}

// CsvSource read csv file and send data to Sender
#[derive(Debug)]
struct CsvSource {
    readers: Vec<Reader<File>>,
    concurrent: usize,
    batch_size: usize,
    port: u16,
}

impl CsvSource {
    fn new(dsn: &mut Dsn, port: u16) -> Result<CsvSource> {
        // dsn: csv:path/to/csv/path_1/or/file_1,path/to/csv/path_2/or/file_2
        //  ?has_header=&header=&skip=&sep=&batch_size=&concurrent=
        let dsn_paths = match &dsn.path {
            Some(path) => path.split(",").collect_vec(),
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
            .unwrap_or_default();
        let headers = if !headers.is_empty() {
            headers
                .split(",")
                .map(|i| i.trim().to_string())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        };
        let skip = dsn.params.remove("skip").and_then(|skip_char| {
            if skip_char.is_empty() {
                None
            } else {
                Some(skip_char.parse().unwrap())
            }
        });

        let sep = dsn.params.remove("sep").and_then(|sep_char| {
            if sep_char.is_empty() {
                None
            } else {
                let sep_char = sep_char.as_bytes();
                if sep_char.len() == 1 && sep_char[0] != b',' {
                    Some(sep_char[0])
                } else {
                    None
                }
            }
        });

        let batch_size: usize = dsn
            .params
            .remove("batch_size")
            .unwrap_or("1".to_string())
            .parse()?;
        let concurrent: usize = dsn
            .params
            .remove("concurrent")
            .unwrap_or("1".to_string())
            .parse()?;

        if !has_header && headers.len() == 0 {
            return Err(anyhow!("csv header is null"));
        }

        CsvSource::validate(&paths, has_header, &headers, sep, skip)?;

        let readers = CsvSource::csv_readers(&paths, has_header, &headers, sep, skip)?;

        Ok(CsvSource {
            readers,
            concurrent,
            batch_size,
            port,
        })
    }

    async fn read_header(read_path: &str, has_header: bool) -> Result<Vec<String>> {
        let paths = CsvSource::csv_path(read_path)?;
        if paths.is_empty() {
            return Err(anyhow!(format!("there are not csv file is {}", read_path)));
        }

        let mut headers: Vec<String> = Vec::new();

        for path in paths {
            let mut reader = ReaderBuilder::new().from_path(&path)?;
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
        tracing::info!("reading csv files with batch size: {batch_size}");
        let futures = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(self.concurrent));

        while let Some(mut reader) = self.readers.pop() {
            let permit = semaphore.clone().acquire_owned().await?;

            let future = tokio::spawn(async move {
                let res = CsvSource::deal_file(&mut reader, port, batch_size).await?;

                drop(permit);
                Ok(res)
            });

            futures.push(future);
        }

        Ok(futures)
    }

    async fn deal_file(reader: &mut Reader<File>, port: u16, batch_size: usize) -> Result<()> {
        let stream = TcpStream::connect(format!("localhost:{}", port))?;
        let headers = reader
            .headers()?
            .iter()
            .map(String::from)
            .collect::<Vec<String>>();
        let schema = CsvSource::stream_schema(&headers);
        let mut writer: StreamWriter<&TcpStream> = StreamWriter::try_new(&stream, &schema)?;

        tracing::info!("CSV stream reading...");
        tokio::task::yield_now().await;
        let mut record = StringRecord::new();
        let mut records = vec![Vec::with_capacity(batch_size); headers.len()];
        while reader.read_record(&mut record)? {
            if record.is_empty() {
                continue;
            }
            for (i, s) in record.iter().enumerate() {
                records[i].push(if !s.is_empty() {
                    Some(s.to_string())
                } else {
                    None
                });
            }
            if records[0].len() >= batch_size {
                CsvSource::write_to_stream(&headers, &mut writer, &records)?;
                records.iter_mut().for_each(Vec::clear);
                tokio::task::yield_now().await;
            }
        }

        if records[0].len() > 0 {
            CsvSource::write_to_stream(&headers, &mut writer, &records)?;
        }

        tokio::task::yield_now().await;
        tracing::info!("CSV stream reading finished");

        Ok(())
    }

    fn write_to_stream(
        headers: &Vec<String>,
        writer: &mut StreamWriter<&TcpStream>,
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
        Ok(())
    }

    fn stream_schema(headers: &Vec<String>) -> Schema {
        let mut metadata = HashMap::new();
        metadata.insert(String::from("version"), String::from("1.0"));
        metadata.insert(String::from("stream"), String::from("flat"));
        metadata.insert(String::from("ack"), String::from("none"));

        let columns = headers
            .iter()
            .map(|header| Field::new(header, ArrowDataType::Utf8, false))
            .collect::<Vec<Field>>();

        Schema::new(columns).with_metadata(metadata)
    }

    fn csv_path(path: &str) -> Result<Vec<String>> {
        let ext = "csv";
        let p = Path::new(path);

        // path is csv file
        if p.is_file() {
            return Ok(vec![path.to_string()]);
        }
        if p.is_dir() {
            let all_files = utils::files::get_files_in_dir(path, ext)?;
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
        paths: &Vec<String>,
        has_header: bool,
        headers: &Vec<String>,
        sep: Option<u8>,
        skip: Option<u64>,
    ) -> Result<Vec<Reader<File>>> {
        let mut readers = Vec::new();
        for path in paths {
            let mut reader = ReaderBuilder::new()
                .delimiter(match sep {
                    Some(sep) => sep,
                    _ => b',',
                })
                .has_headers(true)
                .flexible(false)
                .from_path(path)
                .with_context(|| format!("Open file {path:?} error"))?;
            // should first fetch headers record in case it has headers.
            if has_header {
                let _ = reader.headers();
            }
            if !headers.is_empty() {
                reader.set_headers(StringRecord::from(headers.clone()));
            }
            info!(
                path,
                "Using headers: \"{}\"",
                reader.headers()?.iter().join(",")
            );
            if let Some(skip) = skip {
                let mut record = StringRecord::new();
                for _ in 0..skip {
                    let _ = reader.read_record(&mut record);
                }
                info!(
                    skip,
                    "Start reading csv from line {}",
                    reader.position().line()
                );
            }
            readers.push(reader);
        }
        Ok(readers)
    }

    fn validate(
        paths: &[String],
        has_header: bool,
        headers: &Vec<String>,
        sep: Option<u8>,
        skip: Option<u64>,
    ) -> Result<()> {
        const MAX_VALIDATE_LINES: usize = 10;
        let mut cols = 0;
        for path in paths {
            let mut reader = ReaderBuilder::new()
                .delimiter(match sep {
                    Some(sep) => sep,
                    _ => b',',
                })
                .has_headers(true)
                .flexible(false)
                .from_path(path)
                .with_context(|| format!("Open file {path:?} error"))?;
            // should first fetch headers record in case it has headers.
            if has_header {
                let _ = reader.headers();
            }
            if !headers.is_empty() {
                reader.set_headers(StringRecord::from(headers.clone()));
            }
            info!(
                path,
                "Using headers: {}",
                reader.headers()?.iter().join(" ")
            );
            if let Some(skip) = skip {
                let mut record = StringRecord::new();
                for _ in 0..skip {
                    let _ = reader.read_record(&mut record);
                }
                info!(
                    skip,
                    "Start reading csv from line {}",
                    reader.position().line()
                );
            }
            let headers = reader.headers()?;
            if headers.len() <= 1 {
                bail!("CSV fields number should greater than 1.")
            }

            if cols == 0 {
                cols = headers.len();
            }
            let mut record = StringRecord::new();
            for _ in 0..MAX_VALIDATE_LINES {
                reader
                    .read_record(&mut record)
                    .with_context(|| format!("Reading file {path:?} record error"))?;
                let len = record.len();
                let line = reader.position().line();
                if cols != len {
                    bail!("CSV file {path:?} line {line} expect {cols} columns but has {len}");
                }
            }
        }
        Ok(())
    }
}
