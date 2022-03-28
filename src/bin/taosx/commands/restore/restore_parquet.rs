use bitvec_simd::BitVec;
use glob::glob;
use parquet::basic::ConvertedType;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArray, ByteArrayType, DataType};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::path::PathBuf;
// use taos::block::serde::Block;
// use taos::block::Block;
use taos::block::Column as Block;
use taos::r2d2::TaosPool;

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
    for _ in 0..(num_rows + BATCH_SIZE - 1 / BATCH_SIZE) {
        let mut values = vec![];
        for _ in 0..BATCH_SIZE {
            values.push(ByteArray::new());
        }
        column_reader
            .read_batch(BATCH_SIZE, None, None, &mut values)
            .unwrap();
        data.extend(values);
    }
    let _ = data.split_off(num_rows);
    log::debug!("data: {:?}", data);
    data
}

pub async fn restore_parquet(pool: TaosPool, path: PathBuf, filename: String, database: String) {
    let taos = pool.get().unwrap();
    taos.query(format!("use {}", database)).await.unwrap();
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

    for row_group in 0..parquet_reader.num_row_groups() {
        let row_group_reader = parquet_reader.get_row_group(row_group).unwrap();
        let blocks: Vec<_> = (0..row_group_reader.num_columns())
            .map(|col_num| {
                let col_reader = row_group_reader.get_column_reader(col_num).unwrap();
                let row_num = row_group_reader.metadata().num_rows() as usize;
                let nulls = BitVec::zeros(row_num);

                match col_reader {
                    ColumnReader::BoolColumnReader(v) => {
                        let column = read(v, row_num);
                        Block::Bool(nulls, column)
                    }
                    ColumnReader::ByteArrayColumnReader(v) => {
                        let column = read_str(v, row_num);
                        match fields.get(col_num).unwrap().get_basic_info().logical_type() {
                            Some(_) => Block::NChar(
                                column
                                    .into_iter()
                                    .map(|v| {
                                        Some(std::str::from_utf8(v.data()).unwrap().to_string())
                                    })
                                    .collect(),
                            ),
                            None => Block::Binary(
                                column
                                    .into_iter()
                                    .map(|v| Some(v.data().to_vec()))
                                    .collect::<Vec<Option<Vec<u8>>>>()
                                    .to_vec(),
                            ),
                        }
                    }
                    ColumnReader::DoubleColumnReader(v) => {
                        let column = read(v, row_num);
                        Block::Double(nulls, column)
                    }
                    ColumnReader::FloatColumnReader(v) => {
                        let column = read(v, row_num);
                        Block::Float(nulls, column)
                    }
                    ColumnReader::Int32ColumnReader(v) => {
                        let column = read(v, row_num);

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
                        values
                    }
                    ColumnReader::Int64ColumnReader(v) => {
                        let column = read(v, row_num);
                        match fields.get(col_num).unwrap().get_basic_info().logical_type() {
                            Some(_) => Block::Timestamp(nulls, column),
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
                        }
                    }
                    _ => unreachable!(),
                }
            })
            .collect();

        let bind: Vec<_> = blocks.iter().map(|b| b.to_multi_bind()).collect();
        let mut stmt = taos.stmt(&sql).unwrap();
        stmt.multi_bind(&bind).unwrap();
        stmt.execute().unwrap();
    }
}
