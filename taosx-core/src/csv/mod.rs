use std::collections::{HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use csv_lib::{Reader, ReaderBuilder, StringRecord};
use futures_util::TryStreamExt;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Itertools, TaosBuilder};
use tokio::sync::{mpsc, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
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

pub async fn csv_header(path: &str, has_header: bool) -> Result<CsvHeader> {
    let files = CsvSource::csv_path(path)?;
    let headers = CsvSource::read_header(files[0].as_str(), has_header).await?;
    if has_header {
        Ok(CsvHeader {
            columns: headers.len(),
            headers,
        })
    } else {
        Ok(CsvHeader {
            columns: headers.len(),
            headers: vec![],
        })
    }
}

pub async fn csv_to_taos(mut from: Dsn, mut to: Dsn) -> Result<()> {
    // mpsc from source to transform
    let (csv_tx, csv_rx) = mpsc::channel(1024);
    // mpsc from transform to sink
    let (sql_tx, sql_rx) = async_channel::bounded(1024);
    // let (sql_tx, sql_rx) = mpsc::channel(1024);

    let source = CsvSource::new(&mut from, csv_tx)?;
    let transform = CsvTransform::new(&mut from, &mut to, csv_rx, sql_tx).await?;
    let sink = CsvSink::new(&mut to, sql_rx).await?;

    let sink_handlers = sink.sink().await?;
    let trans_handler = transform.trans().await?;
    let source_handlers = source.read().await?;

    for handler in sink_handlers {
        handler.await?.unwrap();
    }
    trans_handler.await?.unwrap();
    for handler in source_handlers {
        handler.await?.unwrap();
    }

    Ok(())
}

#[derive(Debug)]
pub struct CsvHeader {
    pub columns: usize,
    pub headers: Vec<String>,
}

pub struct Col {
    field: String,
    field_type: String,
    is_tag: bool,
}

// CsvSource read csv file and send data to Sender
struct CsvSource {
    readers: Vec<Reader<File>>,
    sender: Sender<Vec<String>>,
    concurrent: usize,
}

struct CsvTransform {
    table_name: String,
    batch_size: usize,
    is_stable: bool,
    ts_type: TsType,
    table_meta: Vec<Col>,
    receiver: Receiver<Vec<String>>,
    sender: async_channel::Sender<String>,
}

#[derive(PartialEq)]
enum TsType {
    TimeStamp,
    String,
}

fn to_ts_type(ts_type: String) -> TsType {
    let ts_type = ts_type.to_lowercase();
    let ts_type = ts_type.as_str();
    match ts_type {
        "timestamp" => TsType::TimeStamp,
        _ => TsType::String
    }
}

struct CsvSink {
    taos_builder: TaosBuilder,
    receiver: async_channel::Receiver<String>,
    concurrent: usize,
}

impl CsvSource {
    fn new(dsn: &mut Dsn, sender: Sender<Vec<String>>) -> Result<CsvSource> {
        // from: csv:path/to/csv/path/or/file?delimiter=&quotes=&comment=&flexible=&concurrent=
        let paths = match &dsn.path {
            Some(path) => CsvSource::csv_path(&path)?,
            None => return Err(anyhow!("csv path is null")),
        };

        let has_header: bool = dsn.params.remove("has_header").unwrap_or("true".to_string()).parse()?;
        let delimiter = dsn.params.remove("delimiter").unwrap_or_default();
        let delimiter = delimiter.as_bytes();
        let delimiter = if delimiter.len() == 1 && delimiter[0] != b',' {
            delimiter[0]
        } else { b',' };

        let quotes = dsn.params.remove("quotes").unwrap_or_default();
        let quotes = quotes.as_bytes();
        let double_quote = quotes.len() == 1 && quotes[0] != b'"';
        let escape = if quotes.len() == 1 && quotes[0] != b'"' {
            Some(quotes[0])
        } else {
            None
        };

        let comment = dsn.params.remove("comment").unwrap_or_default();
        let comment = comment.as_bytes();
        let comment = if comment.len() == 1 { Some(comment[0]) } else { None };

        let flexible = dsn.params.remove("flexible").unwrap_or("true".to_string()).parse().unwrap();
        let concurrent: usize = dsn.params.remove("concurrent").unwrap_or("1".to_string()).parse()?;

        let readers = CsvSource::csv_readers(
            &paths,
            has_header,
            delimiter,
            double_quote,
            escape,
            comment,
            flexible)?;

        Ok(CsvSource { readers, sender, concurrent })
    }

    async fn read(mut self) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mut futures = Vec::new();
        let semaphore = Arc::new(Semaphore::new(self.concurrent));

        while let Some(mut reader) = self.readers.pop() {
            let permit = semaphore.clone().acquire_owned().await?;
            let sender = self.sender.clone();

            let future = tokio::spawn(async move {
                CsvSource::read_file(&mut reader, &sender).await?;

                drop(permit);
                Ok(())
            });
            futures.push(future);
        }

        Ok(futures)
    }

    async fn read_header(read_path: &str, has_header: bool) -> Result<Vec<String>> {
        let paths = CsvSource::csv_path(read_path)?;
        if paths.len() == 0 {
            return Err(anyhow!(format!("there are not csv file is {}", read_path)));
        }

        let mut headers: Vec<String> = vec![];

        for path in paths {
            let mut reader = ReaderBuilder::new().from_path(&path)?;
            let file_headers: Vec<String> = reader.headers()?
                .iter()
                .map(|h| h.to_string()).collect_vec();
            if has_header && headers.len() > 0 && headers != file_headers {
                return Err(anyhow!(format!("header of files {} is different with others", &path)));
            }
            if !has_header && headers.len() > 0 && headers.len() != file_headers.len() {
                return Err(anyhow!(format!("columns of {} if different with others", &path)));
            }
            headers = file_headers
        }

        Ok(headers)
    }

    async fn read_file(reader: &mut Reader<File>, sender: &Sender<Vec<String>>) -> Result<()> {
        for result in reader.records() {
            let record: Vec<String> = result?.iter().map(|r| r.to_string()).collect();
            sender.send(record).await?;
        }

        Ok(())
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
                   delimiter: u8,
                   double_quote: bool,
                   escape: Option<u8>,
                   comment: Option<u8>,
                   flexible: bool) -> Result<Vec<Reader<File>>> {
        let mut readers = Vec::new();
        for path in paths {
            let mut reader = ReaderBuilder::new()
                .has_headers(has_header)
                .delimiter(delimiter)
                .double_quote(double_quote)
                .escape(escape)
                .comment(comment)
                .flexible(flexible)
                .from_path(path)?;
            if !has_header {
                // let headers = header.split(",").collect_vec();
                // let headers = StringRecord::from(headers);
                // reader.set_headers(headers);
                todo!()
            }
            readers.push(reader);
        }
        Ok(readers)
    }
}

impl CsvTransform {
    async fn new(from: &mut Dsn, to: &mut Dsn, receiver: Receiver<Vec<String>>, sender: async_channel::Sender<String>) -> Result<CsvTransform> {
        // to: taos://username:password@host:port/db?stable=&table=&batch_size=?concurrent=?
        let stable: String = to.params.remove("stable").unwrap_or_default();
        let table: String = to.params.remove("table").unwrap_or_default();
        let batch_size: usize = to.params.remove("batch_size").unwrap_or(String::from("1")).parse()?;
        let is_stable = !stable.is_empty() && stable.len() > 0;
        let ts_type: String = from.params.remove("ts_type").unwrap_or_default();
        let ts_type = to_ts_type(ts_type);

        let taos_builder = TaosBuilder::from_dsn(&*to)?;
        let table_name = if !stable.is_empty() { stable } else { table };
        let table_meta = CsvTransform::table_meta(taos_builder, &table_name).await?;

        Ok(CsvTransform { table_name, batch_size, is_stable, ts_type, table_meta, receiver, sender })
    }

    async fn trans(mut self) -> Result<JoinHandle<Result<()>>> {
        let future = tokio::spawn(async move {
            // let mut records = Vec::new();
            // while let Some(record) = self.receiver.recv().await {
            //     records.push(record);
            //     if records.len() >= self.batch_size {
            //         let sql = self.generate_sql(&records)?;
            //         self.sender.send(sql).await.unwrap();
            //         records.clear();
            //     }
            // }
            // if !records.is_empty() {
            //     let sql = self.generate_sql(&records)?;
            //     self.sender.send(sql).await.unwrap();
            // }
            // Ok(())
            todo!();
        });

        Ok(future)
    }

    fn generate_sql(&self, records: &Vec<HashMap<String, String>>) -> Result<String> {
        if self.is_stable {
            return self.generate_sql_for_stable(records);
        }
        self.generate_sql_for_table(records)
    }

    fn generate_sql_for_stable(&self, records: &Vec<HashMap<String, String>>) -> Result<String> {
        // insert into {} using {} tags() values()
        let mut sql: String = String::from("insert into ");
        for record in records {
            let mut tag_values = Vec::new();
            let mut col_values = Vec::new();

            for col in &self.table_meta {
                let col_value = record.get(&col.field).map(|v| v.to_string()).unwrap_or_default();

                if col.is_tag {
                    let tag_value = if self.is_str_type(&col.field_type) {
                        format!("'{}'", col_value)
                    } else {
                        col_value
                    };
                    tag_values.push(tag_value);
                } else {
                    let col_value = if self.is_str_type(&col.field_type) {
                        format!("'{}'", col_value)
                    } else {
                        col_value
                    };
                    col_values.push(col_value);
                }
            }

            let table_name = format!("{}_{:x}", self.table_name, md5::compute(&tag_values.join("_").as_bytes()));
            sql.push_str(table_name.as_str());
            sql.push_str(" using ");
            sql.push_str(&self.table_name);
            sql.push_str(" tags (");
            sql.push_str(&tag_values.join(", "));
            sql.push_str(" ) values (");
            sql.push_str(&col_values.join(", "));
            sql.push_str(" ) ");
        }

        Ok(sql)
    }

    fn generate_sql_for_table(&self, records: &Vec<HashMap<String, String>>) -> Result<String> {
        // insert into {} values()
        let mut sql: String = format!("insert into {} values ", self.table_name);
        for record in records {
            sql.push_str("(");
            for col in &self.table_meta {
                let col_value = record.get(&col.field).map(|v| v.as_str()).unwrap_or_default();

                if self.is_str_type(&col.field_type) {
                    sql.push_str(format!("'{}'", col_value).as_str());
                } else {
                    sql.push_str(col_value);
                }
            }
            sql.push_str(")");
        }

        Ok(sql)
    }

    fn is_str_type(&self, field_type: &str) -> bool {
        let field_type = field_type.to_lowercase();
        let field_type = field_type.as_str();
        matches!(field_type, "binary"|"nchar"|"json"|"varchar")
            || (self.ts_type == TsType::String && field_type == "timestamp")
    }

    async fn table_meta(taos_builder: TaosBuilder, table: &str) -> Result<Vec<Col>> {
        let taos = taos_builder.build().await?;

        let mut rs = taos.query(format!("desc {}", table)).await?;
        let mut rows = rs.rows();

        let mut fields = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let values = row.into_values();
            fields.push(Col {
                field: values[0].to_string()?,
                field_type: values[1].to_string()?,
                is_tag: values[3].to_string()?.to_lowercase() == "tag",
            });
        }
        Ok(fields)
    }
}

impl CsvSink {
    async fn new(dsn: &mut Dsn, receiver: async_channel::Receiver<String>) -> Result<CsvSink> {
        // to: taos://username:password@host:port/db?stable=&table=&batch_size=?concurrent=?
        let concurrent: usize = dsn.params.remove("concurrent").unwrap_or(String::from("1")).parse()?;
        let taos_builder = TaosBuilder::from_dsn(&*dsn)?;

        Ok(CsvSink { taos_builder, receiver, concurrent })
    }

    async fn sink(&self) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mut handlers = Vec::new();

        for _ in 0..self.concurrent {
            let taos = self.taos_builder.build().await?;
            let receiver = self.receiver.clone();

            let handler = tokio::spawn(async move {
                while let Ok(sql) = receiver.recv().await {
                    taos.exec(sql).await?;
                }
                Ok(())
            });

            handlers.push(handler);
        }

        Ok(handlers)
    }
}