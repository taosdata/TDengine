use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use arrow::array::RecordBatch;
use arrow_schema::ArrowError;
use chardetng::EncodingDetector;
use chrono::{Duration, Utc};
use encoding_rs::Encoding;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use taos::Itertools;

use crate::get_data_dir;

pub fn get_files_in_dir(dir: &str, ext: &str) -> Result<Vec<String>, anyhow::Error> {
    let path = Path::new(dir);
    if !path.is_dir() {
        return Err(anyhow!(format!("path {} is not dir", dir)));
    }

    let mut files = vec![];
    let mut stack = vec![path.to_path_buf()];

    while let Some(p) = stack.pop() {
        let dir_files = fs::read_dir(p)?;
        for entry in dir_files {
            let entry_path = entry?.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }
            if let Some(file) = entry_path
                .to_str()
                .filter(|f| ext.is_empty() || f.ends_with(ext))
            {
                files.push(file.to_owned());
            }
        }
    }

    Ok(files)
}

pub fn get_encode<T: AsRef<Path>>(file_path: T) -> anyhow::Result<&'static Encoding> {
    let file_path = file_path.as_ref();
    let mut file = File::open(file_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to open file: {}, cause: {}",
            file_path.display(),
            e.to_string()
        )
    })?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|e| {
        anyhow::anyhow!(
            "failed to read file: {}, cause: {}",
            file_path.display(),
            e.to_string()
        )
    })?;

    get_encode_from_buffer(buffer.as_slice())
}

pub fn get_encode_from_buffer(buffer: &[u8]) -> anyhow::Result<&'static Encoding> {
    let mut detector = EncodingDetector::new();
    detector.feed(buffer, true);

    let encoding = detector.guess(None, true);

    Ok(encoding)
}

pub fn decompress_and_write_file(
    path: &std::path::PathBuf,
    data: &[u8],
) -> Result<(), std::io::Error> {
    use std::io::Write;
    let decode_buf = Vec::new();
    let mut decoder = flate2::write::GzDecoder::new(decode_buf);
    decoder.write_all(data)?;
    let writer = decoder.finish()?;
    let mut file = File::create(path)?;
    file.write_all(&writer)?;
    Ok(())
}

pub fn write_to_file(task_id: i64, filename: &String, record: &String) -> anyhow::Result<()> {
    let data_dir = get_data_dir();
    let path = data_dir.join("tasks").join(format!("{task_id}"));
    if !path.exists() {
        if let Err(err) = std::fs::create_dir_all(&path) {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
    }
    let path = path.join(format!("{}.{}", filename, Utc::now().format("%Y%m%d")));
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", record)?;
    Ok(())
}

pub fn write_to_parquet_file(
    task_id: i64,
    filename: &String,
    keep_days: usize,
    max_size: usize,
    record: &RecordBatch,
) -> anyhow::Result<()> {
    let data_dir = get_data_dir();
    let path = data_dir.join("tasks").join(format!("{task_id}"));
    if !path.exists() {
        if let Err(err) = std::fs::create_dir_all(&path) {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
    }
    // delete old files
    if keep_days > 0 {
        delete_old_parquet_files_by_date(task_id, filename, keep_days)?;
    }
    if max_size > 0 {
        delete_old_parquet_files_by_size(task_id, filename, max_size)?;
    }

    let path = path.join(format!("{}.{}", filename, Utc::now().format("%Y%m%d")));
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let schema = record.schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(record)?;
    writer.close()?;
    Ok(())
}

pub fn delete_old_parquet_files_by_date(
    task_id: i64,
    filename: &String,
    keep_days: usize,
) -> anyhow::Result<()> {
    let data_dir = get_data_dir();
    let path = data_dir.join("tasks").join(format!("{task_id}"));
    let path = path.join(format!("{}.{}", filename, Utc::now().format("%Y%m%d")));
    let path = path.parent().unwrap();
    if !path.exists() {
        if let Err(err) = std::fs::create_dir_all(path) {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
    }

    let cutoff_date = Utc::now() - Duration::days(keep_days as i64);
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        if file_path.is_file() {
            if let Some(file_name) = file_path.file_name().and_then(|s| s.to_str()) {
                let file_date = file_name.split('.').last().unwrap();
                if let Ok(file_date) = chrono::NaiveDate::parse_from_str(file_date, "%Y%m%d") {
                    if file_date <= cutoff_date.naive_utc().date() {
                        tracing::info!("delete archived file: {:?}, since out of date", file_path);
                        fs::remove_file(file_path)?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn delete_old_parquet_files_by_size(
    task_id: i64,
    filename: &String,
    max_size: usize,
) -> anyhow::Result<()> {
    let data_dir = get_data_dir();
    let path = data_dir.join("tasks").join(format!("{task_id}"));
    let path = path.join(format!("{}.{}", filename, Utc::now().format("%Y%m%d")));
    let path = path.parent().unwrap();
    if !path.exists() {
        if let Err(err) = std::fs::create_dir_all(path) {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
    }

    let max_size = (max_size * 1024 * 1024 * 1024) as u64;
    let mut total_file_size = 0;

    let mut entries = fs::read_dir(path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .map(|entry| {
            let file_size = entry.metadata().unwrap().len();
            total_file_size += file_size;
            (entry, file_size)
        })
        .collect_vec();

    entries.sort_by_key(|(entry, _)| entry.file_name());

    for (entry, file_size) in entries {
        if total_file_size <= max_size {
            break;
        }
        let file_path = entry.path();
        tracing::info!("delete archived file: {:?}, since out of date", file_path);
        fs::remove_file(file_path)?;
        total_file_size -= file_size;
    }
    Ok(())
}

pub fn delete_oldest_parquet_file(task_id: i64, filename: &String) -> anyhow::Result<()> {
    let data_dir = get_data_dir();
    let path = data_dir.join("tasks").join(format!("{task_id}"));
    let path = path.join(format!("{}.{}", filename, Utc::now().format("%Y%m%d")));
    let path = path.parent().unwrap();
    if !path.exists() {
        if let Err(err) = std::fs::create_dir_all(path) {
            tracing::error!("failed to create dir {:?}: {}", path, err);
        }
    }

    let mut entries = fs::read_dir(path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .collect_vec();

    entries.sort_by_key(|entry| entry.file_name());

    if let Some(entry) = entries.first() {
        let file_path = entry.path();
        tracing::info!("delete archived file: {:?}, since out of date", file_path);
        fs::remove_file(file_path)?;
    }
    Ok(())
}

pub fn read_parquet_file(path: PathBuf) -> anyhow::Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    // open the file
    let file =
        File::open(path.clone()).with_context(|| format!("Unable to open file '{:?}'", path))?;
    let buffer = std::io::BufReader::new(file);
    // the flag to identify the split point
    let flag = b"PAR1PAR1";
    // look for the flag and read the file in chunks
    let mut content = Vec::new();
    let mut windows = Vec::new();
    for byte in buffer.bytes() {
        match byte {
            Ok(byte) => {
                content.push(byte);
                windows.push(byte);
                if windows.len() > flag.len() {
                    windows.remove(0);
                }
                if windows == flag {
                    // remove the extra bytes
                    content.truncate(content.len() - 4);
                    // transform to batches
                    match transform_bytes_to_record(content.clone()) {
                        Ok(mut vec) => batches.append(&mut vec),
                        Err(err) => {
                            anyhow::bail!("Error reading file: {err:#}");
                        }
                    }
                    // begin new record
                    content.clear();
                    b"PAR1".iter().for_each(|x| content.push(*x));
                }
            }
            Err(err) => {
                anyhow::bail!("Error reading file: {err:#}");
            }
        }
    }
    // last record
    match transform_bytes_to_record(content.clone()) {
        Ok(mut vec) => batches.append(&mut vec),
        Err(err) => {
            anyhow::bail!("Error reading file: {err:#}");
        }
    }

    Ok(batches)
}

fn transform_bytes_to_record(bytes: Vec<u8>) -> Result<Vec<RecordBatch>, ArrowError> {
    // build the parquet reader
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .expect("Unable to create Parquet reader builder")
        .build()
        .expect("Unable to build Parquet reader");
    // read all batches
    reader
        .next()
        .into_iter()
        .collect::<Result<Vec<RecordBatch>, ArrowError>>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};

    use super::*;

    #[test]
    fn test_get_file_encode() {
        let file_path = "./tests/opc/opcua-utf8.csv";
        let encode = get_encode(file_path).unwrap();
        assert_eq!(encode.name(), "UTF-8");

        let file_path = "./tests/opc/opcua-utf8bom.csv";
        let encode = get_encode(file_path).unwrap();
        assert_eq!(encode.name(), "UTF-8");

        let file_path = "./tests/opc/opcua-gbk.csv";
        let encode = get_encode(file_path).unwrap();
        assert_eq!(encode.name(), "GBK");
    }

    #[test]
    fn test_write_to_file() {
        let task_id = 1;
        let filename = "cache".to_string();
        let record = "hello world".to_string();
        write_to_file(task_id, &filename, &record).unwrap();
    }

    #[test]
    fn test_write_to_parquet_file() {
        let task_id = 1;
        let filename = "archive".to_string();
        let record = RecordBatch::try_from_iter([
            ("str1", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        write_to_parquet_file(task_id, &filename, 5, 2, &record).unwrap();
    }

    #[test]
    fn test_delete_old_parquet_files_by_date() {
        let task_id = 1;
        let filename = "archive/p1/p2/p3/p4/file".to_string();
        let keep_days = 5;
        let res = delete_old_parquet_files_by_date(task_id, &filename, keep_days);
        assert!(res.is_ok());
    }

    #[test]
    fn test_delete_old_parquet_files_by_size() {
        let task_id = 1;
        let filename = "archive/p1/p2/p3/p4/file".to_string();
        let max_size = 8;
        let res = delete_old_parquet_files_by_size(task_id, &filename, max_size);
        assert!(res.is_ok());
    }

    #[test]
    fn test_delete_oldest_parquet_file() {
        let task_id = 1;
        let filename = "archive/p1/p2/p3/p4/file".to_string();
        let res = delete_oldest_parquet_file(task_id, &filename);
        assert!(res.is_ok());
    }

    #[ignore]
    #[test]
    fn test_read_parquet_file() {
        let task_id = 7;
        let filename = "archived.20250226".to_string();

        let data_dir = get_data_dir();
        let path = data_dir
            .join("tasks")
            .join(format!("{task_id}"))
            .join(filename);

        let res = read_parquet_file(path);
        dbg!(&res);
    }
}
