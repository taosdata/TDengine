use std::{borrow::BorrowMut, fs::File};

use parquet::{
    column::reader::{ColumnReader, ColumnReaderImpl},
    data_type::{ByteArray, ByteArrayType, DataType},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::Row,
    schema::{printer, types::Type},
};

fn print_json(
    row_count: usize,
    col_count: usize,
    iter: impl Iterator<Item = Row>, // iter: std::iter::Take<parquet::record::reader::RowIter>,
) {
    print!("[");

    for (row_index, row) in iter.enumerate() {
        print!("{{");

        for (col_index, (name, field)) in row.get_column_iter().enumerate() {
            let out = format!("\"{}\": {}", name, field);

            if col_index == col_count - 1 {
                print!("{}", out);
            } else {
                print!("{}, ", out);
            }
        }

        if row_index == row_count - 1 {
            print!("}}");
        } else {
            print!("}},");
        }
    }

    println!("]");
}

pub fn deserialize_print_file(file: File) {
    let parquet_reader = SerializedFileReader::try_from(file).unwrap();
    let schema = parquet_reader.metadata().file_metadata().schema();
    let mut buf = Vec::new();
    printer::print_schema(&mut buf, &schema);

    let string_schema = String::from_utf8(buf).unwrap();

    log::debug!("\n{}", string_schema);

    let md = parquet_reader.metadata().file_metadata();

    log::debug!("MetaData:");
    log::debug!("num_columns: {}", md.schema_descr().columns().len());
    log::debug!("num_rows: {}", md.num_rows());
    log::debug!("num_row_groups: {}", parquet_reader.num_row_groups());

    // for row_group in 0..parquet_reader.num_row_groups() {
    //     dbg!(row_group);
    //     let row_group_reader = parquet_reader.get_row_group(row_group).unwrap();
    //     for col in 0..row_group_reader.num_columns() {
    //         dbg!(col);
    //         let col_reader = row_group_reader.get_column_reader(col).unwrap();
    //         let row_num = row_group_reader.metadata().num_rows() as usize;
    //         dbg!(row_num);
    //         match col_reader {
    //             ColumnReader::BoolColumnReader(v) => {
    //                 dbg!("bool");
    //                 let column = read(v, row_num);
    //                 dbg!(column);
    //             }
    //             ColumnReader::Int32ColumnReader(v) => {
    //                 let column = read(v, row_num);
    //                 dbg!(column);
    //             }
    //             ColumnReader::Int64ColumnReader(v) => {
    //                 let column = read(v, row_num);
    //                 dbg!(column);
    //             }
    //             ColumnReader::FloatColumnReader(v) => {
    //                 let column = read(v, row_num);
    //                 dbg!(column);
    //             }
    //             ColumnReader::DoubleColumnReader(v) => {
    //                 let column = read(v, row_num);
    //                 dbg!(column);
    //             }
    //             ColumnReader::ByteArrayColumnReader(v) => {
    //                 let column = read_str(v, row_num);
    //                 dbg!(column);
    //             }
    //             _ => panic!(),
    //         }
    //     }
    // }

    let row_count = parquet_reader.metadata().file_metadata().num_rows();
    let iter = parquet_reader.get_row_iter(None).unwrap();
    let col_count = parquet_reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .len();
    print_json(row_count as _, col_count, iter);
}

fn read<T>(mut column_reader: ColumnReaderImpl<T>, num_rows: usize) -> Vec<T::T>
where
    T: DataType,
    T::T: Default + Copy,
{
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut values = [T::T::default(); BATCH_SIZE];
    for _ in 0..(num_rows + BATCH_SIZE - 1) / BATCH_SIZE {
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        data.extend(&values[0..num]);
    }
    data
}

fn read_str(mut column_reader: ColumnReaderImpl<ByteArrayType>, num_rows: usize) -> Vec<ByteArray> {
    let mut data = vec![];
    const BATCH_SIZE: usize = 10;
    let mut count = 0;
    for _ in 0..(num_rows + BATCH_SIZE - 1 / BATCH_SIZE) {
        let mut values = vec![];
        for _ in 0..BATCH_SIZE {
            values.push(ByteArray::new());
        }
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        count += num;
        data.extend(values);
    }
    let _ = data.split_off(count);
    data
}
