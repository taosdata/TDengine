/// This program reads a Parquet file and optionally writes the content to an output file.
///
/// # Usage
/// ```sh
/// read-parquet <input_file> [-o <output_file>]
/// ```
///
/// # Options
/// - `-h`, `--help`: Print this help message.
///
/// # Arguments
/// - `<input_file>`: The path to the input Parquet file.
/// - `-o <output_file>`: The path to the output file where the content will be written. If not provided, the content will be printed to the console.
///
/// # Example
/// ```sh
/// read-parquet input.parquet -o output.txt
/// ```
///
/// # Panics
/// The program will panic if it is unable to open the input or output files, or if it encounters an error while reading the Parquet file.
///
use arrow::array::RecordBatch;
use arrow_schema::ArrowError;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::Write;

#[derive(Debug, clap::Parser)]
struct Args {
    #[clap()]
    input_file: String,
    #[clap(short = 'o', long)]
    output_file: Option<String>,
}

fn main() {
    let args = Args::parse();

    let file_source = &args.input_file;
    let file_target = &args.output_file;

    let mut batches = Vec::new();
    // open the file
    let file =
        File::open(file_source).unwrap_or_else(|_| panic!("Unable to open file '{}'", file_source));
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
                            eprintln!("Error reading file: {err:#}");
                        }
                    }
                    // begin new record
                    content.clear();
                    b"PAR1".iter().for_each(|x| content.push(*x));
                }
            }
            Err(err) => {
                eprintln!("Error reading file: {err:#}");
                return;
            }
        }
    }
    // last record
    match transform_bytes_to_record(content.clone()) {
        Ok(mut vec) => batches.append(&mut vec),
        Err(err) => {
            eprintln!("Error reading file: {err:#}");
        }
    }

    // output to a file or just print it
    if let Some(file_target) = file_target {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(file_target)
            .unwrap_or_else(|_| panic!("Unable to open file '{}'", file_target));
        let _ = writeln!(file, "{:?}", batches);
    } else {
        println!("{:?}", batches);
    }
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
