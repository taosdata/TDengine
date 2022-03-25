use bitvec_simd::BitVec;
use glob::glob;
use parquet::basic::ConvertedType;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType, DoubleType, FloatType, Int32Type, Int64Type,
};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::array;
use std::ops::DerefMut;
use std::path::PathBuf;
use taos::block::serde::Block;
use taos::r2d2::TaosPool;
use tokio::runtime::Builder;

pub fn get_parquet_files<'a>(path: PathBuf) -> Vec<String> {
    let mut file_list = vec![];
    let paths = glob(format!("{}/*.parquet", path.as_os_str().to_str().unwrap()).as_str()).unwrap();
    for entry in paths {
        match entry {
            Ok(path) => {
                let filename = path.to_str().unwrap().split("/").last().unwrap();
                file_list.push(filename.to_string())
            }
            Err(e) => println!("{:?}", e),
        }
    }
    file_list
}

fn read_bool(mut column_reader: ColumnReaderImpl<BoolType>, num_rows: usize) -> (Vec<bool>, usize) {
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 {
        let mut values: [bool; BATCH_SIZE] = [false; BATCH_SIZE];
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        count += num;
        data.extend(values);
    }
    (data, count)
}

fn read_i32(mut column_reader: ColumnReaderImpl<Int32Type>, num_rows: usize) -> (Vec<i32>, usize) {
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 {
        let mut values: [i32; BATCH_SIZE] = [0; BATCH_SIZE];
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        count += num;
        let (l, _) = values.split_at(num);
        data.extend(l);
    }
    (data, count)
}

fn read_f32<T>(mut column_reader: ColumnReaderImpl<T>, num_rows: usize) -> (Vec<T::T>, usize)
where
    T: DataType,
    T::T: Default + Copy,
{
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    let mut values = [T::T::default(); BATCH_SIZE];
    for _ in 0..(num_rows + BATCH_SIZE - 1) / BATCH_SIZE {
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        // let (l, _) = values.split_at(num);
        count += num;
        data.extend(&values[0..num]);
    }
    (data, count)
}

fn read_f64(mut column_reader: ColumnReaderImpl<DoubleType>, num_rows: usize) -> (Vec<f64>, usize) {
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 {
        let mut values: [f64; BATCH_SIZE] = [0.0; BATCH_SIZE];
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        let (l, _) = values.split_at(num);
        count += num;
        data.extend(l);
    }
    (data, count)
}

fn read_i64(mut column_reader: ColumnReaderImpl<Int64Type>, num_rows: usize) -> (Vec<i64>, usize) {
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 {
        let mut values: [i64; BATCH_SIZE] = [0; BATCH_SIZE];
        let (num, _) = column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        let (l, _) = values.split_at(num);
        count += num;
        data.extend(l);
    }
    (data, count)
}

fn read_str(
    mut column_reader: ColumnReaderImpl<ByteArrayType>,
    num_rows: usize,
) -> (Vec<ByteArray>, usize) {
    let mut data = vec![];
    const BATCH_SIZE: usize = 100;
    let mut count = 0;
    for _ in 0..(num_rows as f64 / BATCH_SIZE as f64).ceil() as i64 {
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
    (data, count)
}

pub async fn restore_parquet(pool: TaosPool, path: PathBuf, filename: String, database: String) {
    let taos = pool.get().unwrap();
    let tb = filename.split(".").next().unwrap();
    let parquet_reader =
        SerializedFileReader::try_from(format!("{}/{}.parquet", path.to_str().unwrap(), tb))
            .unwrap();
    let read_schema = parquet_reader.metadata().file_metadata().schema();
    let mut sql = format!("insert into {} values(", tb);
    let fields = read_schema.get_fields();
    let column_num = fields.len();
    for i in 0..column_num {
        if i != column_num - 1 {
            sql += "?,";
        } else {
            sql += "?)";
        }
    }
    dbg!(&sql);
    let mut bind_array = vec![];
    for row_group in 0..parquet_reader.num_row_groups() {
        let row_group_reader = parquet_reader.get_row_group(row_group).unwrap();
        for col_num in 0..row_group_reader.num_columns() {
            let col_reader = row_group_reader.get_column_reader(col_num).unwrap();
            let row_num = row_group_reader.metadata().num_rows() as usize;
            let nulls = BitVec::zeros(row_num);
            let values: Block;
            match col_reader {
                ColumnReader::BoolColumnReader(v) => {
                    let (column, _) = read_bool(v, row_num);
                    values = Block::Bool(nulls, column);
                    bind_array.push(values.to_multi_bind());
                }
                ColumnReader::ByteArrayColumnReader(v) => {
                    let (column, _) = read_str(v, row_num);

                    values = match fields.get(col_num).unwrap().get_basic_info().logical_type() {
                        Some(_) => {
                            Block::NChar(column.into_iter().map(|v| Some(v.to_string())).collect())
                        }
                        None => Block::Binary(
                            column
                                .into_iter()
                                .map(|v| Some(v.data().to_vec()))
                                .collect::<Vec<Option<Vec<u8>>>>()
                                .to_vec(),
                        ),
                    };
                    bind_array.push(values.to_multi_bind());
                }
                ColumnReader::DoubleColumnReader(v) => {
                    let (column, _) = read_f64(v, row_num);
                    values = Block::Double(nulls, column);
                    bind_array.push(values.to_multi_bind());
                }
                ColumnReader::FloatColumnReader(v) => {
                    let (column, _) = read_f32(v, row_num);
                    values = Block::Float(nulls, column);
                    bind_array.push(values.to_multi_bind());
                }
                ColumnReader::Int32ColumnReader(v) => {
                    let (column, _) = read_i32(v, row_num);

                    let values = match fields
                        .get(col_num)
                        .unwrap()
                        .get_basic_info()
                        .converted_type()
                    {
                        ConvertedType::UINT_8 => Block::UTinyInt(
                            nulls,
                            column.into_iter().map(|v| v as u8).collect::<Vec<u8>>(),
                        ),
                        ConvertedType::UINT_16 => Block::USmallInt(
                            nulls,
                            column.into_iter().map(|v| v as u16).collect::<Vec<u16>>(),
                        ),
                        ConvertedType::UINT_32 => Block::UInt(
                            nulls,
                            column.into_iter().map(|v| v as u32).collect::<Vec<u32>>(),
                        ),
                        ConvertedType::INT_8 => Block::TinyInt(
                            nulls,
                            column.into_iter().map(|v| v as i8).collect::<Vec<i8>>(),
                        ),
                        ConvertedType::INT_16 => Block::SmallInt(
                            nulls,
                            column.into_iter().map(|v| v as i16).collect::<Vec<i16>>(),
                        ),
                        ConvertedType::NONE => Block::Int(nulls, column),
                        _ => unreachable!(),
                    };
                    bind_array.push(values.to_multi_bind());
                }
                ColumnReader::Int64ColumnReader(v) => {
                    let (column, _) = read_i64(v, row_num);
                    values = match fields.get(col_num).unwrap().get_basic_info().logical_type() {
                        Some(_) => {
                            dbg!(&column);
                            Block::Timestamp(nulls, column)
                        }
                        None => match fields
                            .get(col_num)
                            .unwrap()
                            .get_basic_info()
                            .converted_type()
                        {
                            ConvertedType::NONE => Block::BigInt(nulls, column),
                            ConvertedType::UINT_64 => Block::UBigInt(
                                nulls,
                                column.into_iter().map(|v| v as u64).collect::<Vec<u64>>(),
                            ),
                            _ => unreachable!(),
                        },
                    };
                    dbg!(&values);
                    bind_array.push(values.to_multi_bind());
                }
                _ => unreachable!(),
            }
        }
    }
    taos.query(format!("use {}", database)).await.unwrap();
    let mut stmt = taos.stmt(sql).unwrap();
    dbg!(&bind_array);
    stmt.multi_bind(&bind_array).unwrap();
    stmt.execute().unwrap();
}
