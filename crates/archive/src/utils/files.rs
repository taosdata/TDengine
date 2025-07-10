use std::{fs::File, io::Read, path::PathBuf};

use anyhow::Context;
use arrow::{array::RecordBatch, error::ArrowError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Read parquet file
///
/// Read the parquet file and return the record batches
pub fn read_parquet_file(path: PathBuf) -> anyhow::Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    if !path.exists() {
        return Ok(batches);
    }
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

/// Transform bytes to record
///
/// the bytes are read from the parquet file, and transformed to record batches
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
mod test {
    use std::path::PathBuf;

    use crate::utils::files::read_parquet_file;

    #[ignore]
    #[test]
    fn test_read_parquet_file() {
        let task_id = 7;
        let filename = "archived.20250226".to_string();

        let data_dir = PathBuf::from("/tmp/taosx");
        let path = data_dir
            .join("tasks")
            .join(format!("{task_id}"))
            .join(filename);

        let res = read_parquet_file(path);
        dbg!(&res);
    }
}
