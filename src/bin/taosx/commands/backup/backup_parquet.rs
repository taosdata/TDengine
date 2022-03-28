use libtaos::Taos;
use parquet::{
    basic::{
        Compression, ConvertedType, LogicalType, Repetition, TimeUnit, TimestampType,
        Type as PhysicalType,
    },
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use std::{fs, path::PathBuf, sync::Arc};
use taos::r2d2::TaosPool;
pub async fn generate_parquet_schema(taos: &Taos, db: String, stb: &str) -> Arc<Type> {
    let mut fields = vec![];
    taos.use_database(&db).await.unwrap();
    let res = taos.describe(stb).await.unwrap();
    for i in 0..res.cols.len() {
        match res.cols[i].type_ {
            libtaos::TaosDataType::Null => todo!(),
            libtaos::TaosDataType::Bool => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::BOOLEAN)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::TinyInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_8)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::SmallInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_16)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::Int => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::BigInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::Float => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::FLOAT)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::Double => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::DOUBLE)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::Binary => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::Timestamp => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::TIMESTAMP(TimestampType {
                        is_adjusted_to_u_t_c: false,
                        unit: TimeUnit::MILLIS(Default::default()),
                    })))
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::NChar => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::STRING(Default::default())))
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::UTinyInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_8)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::USmallInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_16)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::UInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_32)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::UBigInt => fields.push(Arc::new(
                Type::primitive_type_builder(&i.to_string(), PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_64)
                    .build()
                    .unwrap(),
            )),
            libtaos::TaosDataType::Json => todo!(),
            libtaos::TaosDataType::Unknown => todo!(),
        }
    }
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}

pub async fn backup_data_parquet(
    pool: TaosPool,
    db: String,
    tb: String,
    schema: Arc<Type>,
    target: PathBuf,
) {
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let mut path = target.clone();
    path.push(format!("{}.parquet", tb));
    let file = fs::File::create(path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let taos = pool.get().unwrap();
    let res = taos
        .query(format!("select * from {}.{} ", db, tb).as_str())
        .await
        .unwrap();
    let stream = res.fetch_block_stream();
    use futures::future;
    use futures::stream::StreamExt;
    stream
        .enumerate()
        .for_each(|(_, partial)| {
            // log::debug!("num of row in block: {}", partial.num_of_rows());
            let mut row_group_writer = writer.next_row_group().unwrap();
            for col in partial.columns_iter() {
                let data_writer = row_group_writer.next_column().unwrap();
                if let Some(mut writer) = data_writer {
                    use taos::block::BorrowedColumn::*;
                    match writer {
                        parquet::column::writer::ColumnWriter::BoolColumnWriter(ref mut typed) => {
                            match col {
                                Bool(is_nulls, values) => {
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                _ => unreachable!(),
                            }
                        }
                        parquet::column::writer::ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                            // let values;
                            match col {
                                TinyInt(is_nulls, values) => {
                                    let values: Vec<i32> =
                                        values.into_iter().map(|v| *v as _).collect();
                                    typed.write_batch(&values, None, None).unwrap();
                                    // std::mem::transmute::<*const i32>(values.ptr()).

                                    // values = values
                                    //     .into_iter()
                                    //     .map(|v| match v {
                                    //         Some(u) => Some(u as i32),
                                    //         None => None,
                                    //     })
                                    //     .collect::<Option<Vec<i32>>>()
                                    //     .unwrap()
                                }
                                SmallInt(is_nulls, values) => {
                                    let values: Vec<i32> =
                                        values.into_iter().map(|v| *v as _).collect();
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                Int(is_nulls, values) => {
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                UTinyInt(is_nulls, values) => {
                                    let values: Vec<i32> =
                                        values.into_iter().map(|v| *v as _).collect();
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                USmallInt(is_nulls, values) => {
                                    let values: Vec<i32> =
                                        values.into_iter().map(|v| *v as _).collect();
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                UInt(is_nulls, values) => {
                                    // let values: Vec<i32> = values.into_iter().map(|v| *v as _).collect();
                                    let len = values.len();
                                    let ptr: *mut i32 = values.as_ptr() as _;
                                    let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                                    typed.write_batch(&values, None, None).unwrap();
                                }

                                _ => unreachable!(),
                            }
                            // typed.write_batch(&values, None, None).unwrap();
                        }
                        parquet::column::writer::ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                            // let values;
                            match col {
                                BigInt(is_nulls, values) => {
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                UBigInt(is_nulls, values) => {
                                    // let values: Vec<i32> = values.into_iter().map(|v| *v as _).collect();
                                    let len = values.len();
                                    let ptr: *mut i64 = values.as_ptr() as _;
                                    let values = unsafe { std::slice::from_raw_parts(ptr, len) };
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                Timestamp(is_nulls, values) => {
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                _ => unreachable!(),
                            }
                            // typed.write_batch(&values, None, None).unwrap();
                        }
                        parquet::column::writer::ColumnWriter::FloatColumnWriter(ref mut typed) => {
                            // let values;
                            match col {
                                Float(is_nulls, values) => {
                                    typed.write_batch(&values, None, None).unwrap();
                                }
                                _ => unreachable!(),
                            }
                        }
                        parquet::column::writer::ColumnWriter::DoubleColumnWriter(
                            ref mut typed,
                        ) => match col {
                            Double(is_nulls, values) => {
                                typed.write_batch(&values, None, None).unwrap();
                            }
                            _ => unreachable!(),
                        },
                        parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(
                            ref mut typed,
                        ) => {
                            let mut values = vec![];
                            match col {
                                Binary(v) => {
                                    for f in v.into_iter() {
                                        match f {
                                            Some(u) => values.push(
                                                parquet::data_type::ByteArray::from(u.to_vec()),
                                            ),
                                            None => {
                                                values.push(parquet::data_type::ByteArray::from(""))
                                            }
                                        }
                                    }
                                }
                                NChar(v) => {
                                    for f in v.into_iter() {
                                        match f {
                                            Some(u) => values.push(
                                                parquet::data_type::ByteArray::from(u),
                                            ),
                                            None => {
                                                values.push(parquet::data_type::ByteArray::from(""))
                                            }
                                        }
                                    }
                                }
                                _ => unreachable!(),
                            }
                            typed.write_batch(&values, None, None).unwrap();
                        }
                        _ => unreachable!(),
                    }
                    row_group_writer.close_column(writer).unwrap();
                }
            }
            writer.close_row_group(row_group_writer).unwrap();
            future::ready(())
        })
        .await;
    writer.close().unwrap();
}
