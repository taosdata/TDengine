use std::collections::HashMap;
use std::fs::File;
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use csv_lib::{Position, Reader, ReaderBuilder, StringRecord};
use futures_util::{TryStreamExt};
use futures_util::stream::FuturesUnordered;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Itertools, TaosBuilder};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use taosx_ipc::prelude::ArrowDataType;

use crate::utils;

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
            return Err(anyhow!(format!("header of files {} is different with others", &path)));
        }
        header = path_header;
    }

    Ok(CsvHeader {
        columns: header.len(),
        headers: if has_header { header } else { vec![] },
    })
}

pub async fn csv_to_taos(mut from: Dsn) -> Result<()> {
    let mut source = CsvSource::new(&mut from)?;
    let handlers = source.read().await?;

    for handler in handlers {
        handler.await?.unwrap();
    }

    Ok(())
}

pub struct CsvHeader {
    pub columns: usize,
    pub headers: Vec<String>,
}

// CsvSource read csv file and send data to Sender
struct CsvSource {
    readers: Vec<Reader<File>>,
    concurrent: usize,
    batch_size: usize,
    port: u32,
}

impl CsvSource {
    fn new(dsn: &mut Dsn) -> Result<CsvSource> {
        // dsn: csv:path/to/csv/path/or/file?has_header=&header=&skip=&sep=&batch_size=&concurrent=&port=
        let paths = if let Some(path) = &dsn.path {
            CsvSource::csv_path(&path)?
        } else {
            return Err(anyhow!("csv path is null"));
        };

        let has_header: bool = dsn.params.remove("has_header").unwrap_or_else(|| "true".to_string()).parse()?;
        let headers = dsn.params.remove("header").unwrap_or_default();
        let headers = if !headers.is_empty() {
            headers.split(",").map(String::from).collect::<Vec<String>>()
        } else { Vec::new() };
        let skip = dsn.params.remove("skip").and_then(|skip_char| {
            if skip_char.is_empty() { None } else {
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

        let batch_size: usize = dsn.params.remove("batch_size").unwrap_or("10".to_string()).parse()?;
        let concurrent: usize = dsn.params.remove("concurrent").unwrap_or("1".to_string()).parse()?;
        let port: u32 = dsn.params.remove("port").unwrap().parse()?;

        let readers = CsvSource::csv_readers(&paths, has_header, &headers, sep, skip)?;

        Ok(CsvSource { readers, concurrent, batch_size, port })
    }

    async fn read_header(read_path: &str, has_header: bool) -> Result<Vec<String>> {
        let paths = CsvSource::csv_path(read_path)?;
        if paths.is_empty() {
            return Err(anyhow!(format!("there are not csv file is {}", read_path)));
        }

        let mut headers: Vec<String> = Vec::new();

        for path in paths {
            let mut reader = ReaderBuilder::new().from_path(&path)?;
            let file_headers = reader.headers()?
                .iter()
                .map(String::from)
                .collect::<Vec<String>>();
            if !CsvSource::is_same_header(&headers, &file_headers, has_header) {
                return Err(anyhow!(format!("header of files {} is different with others", &path)));
            }

            headers = file_headers;
        }

        Ok(headers)
    }

    fn is_same_header(old_header: &Vec<String>, new_header: &Vec<String>, has_header: bool) -> bool {
        old_header.is_empty()
            || (has_header && old_header == new_header)
            || (!has_header && old_header.len() == new_header.len())
    }

    async fn read(&mut self) -> Result<FuturesUnordered<JoinHandle<Result<()>>>> {
        let port = self.port;
        let batch_size = self.batch_size;
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

    async fn deal_file(reader: &mut Reader<File>, port: u32, batch_size: usize) -> Result<()> {
        let stream = TcpStream::connect(format!("localhost:{}", port))?;
        let headers = reader.headers()?.iter().map(String::from).collect::<Vec<String>>();
        let schema = CsvSource::stream_schema(&headers);
        let mut writer: StreamWriter<&TcpStream> = StreamWriter::try_new(&stream, &schema)?;

        let mut records: Vec<HashMap<String, String>> = Vec::with_capacity(batch_size);

        for result in reader.deserialize() {
            let record: HashMap<String, String> = result?;
            records.push(record);

            if records.len() >= batch_size {
                CsvSource::write_to_stream(&headers, &schema, &mut writer, &records)?;
                records.clear();
            }
        }

        Ok(())
    }

    fn write_to_stream(headers: &Vec<String>, schema: &Schema, writer: &mut StreamWriter<&TcpStream>, records: &Vec<HashMap<String, String>>) -> Result<()> {
        let arrow_columns: Vec<ArrayRef> = headers.iter()
            .map(|col| {
                let cols = records.iter()
                    .map(|record| record[col].clone())
                    .collect::<Vec<String>>();
                Arc::new(StringArray::from(cols)) as ArrayRef
            }).collect();

        let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrow_columns)?;
        let res = writer.write(&record_batch)?;

        Ok(res)
    }

    fn stream_schema(headers: &Vec<String>) -> Schema {
        let mut metadata = HashMap::new();
        metadata.insert(String::from("version"), String::from("1.0"));
        metadata.insert(String::from("stream"), String::from("flat"));
        metadata.insert(String::from("ack"), String::from("none"));

        let columns = headers
            .iter()
            .map(
                |header| Field::new(header, ArrowDataType::Utf8, false)
            )
            .collect::<Vec<Field>>();

        Schema::new(columns).with_metadata(metadata)
    }

    fn csv_path(path: &str) -> Result<Vec<String>> {
        let ext = "csv";
        let p = Path::new(path);

        // path is csv file
        if p.is_file() {
            if !path.ends_with(ext) {
                return Err(anyhow!(format!("not a {} file", ext)));
            }
            return Ok(vec![path.to_string()]);
        }
        let all_files = utils::files::get_files_in_dir(path, ext)?;
        Ok(all_files)
    }

    fn csv_readers(paths: &Vec<String>,
                   has_header: bool,
                   headers: &Vec<String>,
                   sep: Option<u8>,
                   skip: Option<u64>) -> Result<Vec<Reader<File>>> {
        let mut readers = Vec::new();
        let mut first = true;
        for path in paths {
            let mut reader = ReaderBuilder::new()
                .delimiter(match sep {
                    Some(sep) => sep,
                    _ => b','
                })
                .has_headers(has_header)
                .flexible(true).from_path(path)?;
            if !headers.is_empty() {
                reader.set_headers(StringRecord::from(headers.clone()));
            }
            if first {
                if let Some(skip) = skip {
                    let mut position = Position::new();
                    position.set_line(skip);
                    reader.seek(position)?;
                }
                first = false;
            }
            readers.push(reader);
        }
        Ok(readers)
    }
}
