use futures::{future, StreamExt};
use parquet::{
    column::writer::*,
    data_type::{BoolType, ByteArray, ByteArrayType, Int32Type},
    file::writer::{FileWriter, ParquetWriter, RowGroupWriter, SerializedFileWriter},
};

use taos::{block::BlockStream, helpers::ColumnMeta};

pub fn serialize_col_meta(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    col_metas: Vec<ColumnMeta>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    let mut name_values = vec![];
    let mut type_values = vec![];
    let mut length_values = vec![];
    let mut is_tags = vec![];
    let def_levels = vec![2; col_metas.len()];
    let mut rep_levels = vec![2; col_metas.len()];
    rep_levels[0] = 0;
    for meta in col_metas {
        match meta {
            ColumnMeta::Column(v) => {
                name_values.push(ByteArray::from(v.field.as_bytes().to_vec()));
                length_values.push(v.length as i32);
                type_values.push(v.r#type as i32);
                is_tags.push(false);
            }
            ColumnMeta::Tag(v) => {
                name_values.push(ByteArray::from(v.field.as_bytes().to_vec()));
                type_values.push(v.r#type as i32);
                length_values.push(v.length as i32);
                is_tags.push(true);
            }
        }
    }

    typed
        .write_batch(&mut name_values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<Int32Type>(&mut data_writer);
    typed
        .write_batch(&mut type_values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<Int32Type>(&mut data_writer);
    typed
        .write_batch(&mut length_values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<BoolType>(&mut data_writer);
    typed
        .write_batch(&mut is_tags, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_tableinfo<W>(
    tbname: &str,
    describe: Vec<ColumnMeta>,
    mut writer: SerializedFileWriter<W>,
) -> SerializedFileWriter<W>
where
    W: ParquetWriter + 'static,
{
    let mut row_group_writer = writer.next_row_group().unwrap();
    row_group_writer = serialzie_tbname(row_group_writer, tbname);
    row_group_writer = serialize_col_meta(row_group_writer, describe);
    writer.close_row_group(row_group_writer).unwrap();
    writer
}

fn serialzie_tbname(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tbname: &str,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    typed
        .write_batch(&[ByteArray::from(tbname)], Some(&[1]), Some(&[0]))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub async fn serialize_tag<W>(
    mut writer: SerializedFileWriter<W>,
    tbname: &str,
    stream: BlockStream<'_>,
) -> SerializedFileWriter<W>
where
    W: ParquetWriter + 'static,
{
    stream
        .enumerate()
        .for_each(|(_, row)| {
            let mut row_group_writer = writer.next_row_group().unwrap();
            row_group_writer = serialzie_tbname(row_group_writer, tbname);
            let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
            let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
            let num_of_rows = row.num_of_rows();
            let num_of_fields = row.num_of_fields();
            let mut values = vec![];
            let mut null_values = vec![];
            let def_levels = vec![3; num_of_rows as usize * num_of_fields as usize];
            let mut rep_levels = vec![];
            let mut rep;
            for v in row.rows_iter() {
                rep = 2;
                for value in v {
                    rep_levels.push(rep);
                    match value.to_owned() {
                        taos::block::BorrowedValue::Null => {
                            values.push(ByteArray::from(""));
                            null_values.push(true);
                        }
                        taos::block::BorrowedValue::Bool(_) => todo!(),
                        taos::block::BorrowedValue::TinyInt(_) => todo!(),
                        taos::block::BorrowedValue::SmallInt(_) => todo!(),
                        taos::block::BorrowedValue::Int(v) => {
                            values.push(ByteArray::from(v.to_string().as_bytes().to_vec()));
                            null_values.push(false);
                        }
                        taos::block::BorrowedValue::BigInt(_) => todo!(),
                        taos::block::BorrowedValue::Float(_) => todo!(),
                        taos::block::BorrowedValue::Double(_) => todo!(),
                        taos::block::BorrowedValue::Binary(v) => {
                            let mut value = Vec::new();
                            value.push(34);
                            value.append(&mut v.to_vec());
                            value.push(34);
                            values.push(ByteArray::from(value));
                            null_values.push(false);
                        }
                        taos::block::BorrowedValue::Timestamp(_) => todo!(),
                        taos::block::BorrowedValue::NChar(_) => todo!(),
                        taos::block::BorrowedValue::UTinyInt(_) => todo!(),
                        taos::block::BorrowedValue::USmallInt(_) => todo!(),
                        taos::block::BorrowedValue::UInt(_) => todo!(),
                        taos::block::BorrowedValue::UBigInt(_) => todo!(),
                        taos::block::BorrowedValue::Json(_) => todo!(),
                        taos::block::BorrowedValue::VarChar(_) => todo!(),
                        taos::block::BorrowedValue::VarBinary(_) => todo!(),
                        taos::block::BorrowedValue::Decimal(_) => todo!(),
                        taos::block::BorrowedValue::Blob(_) => todo!(),
                    }
                    rep = 3;
                }
            }
            rep_levels[0] = 0;
            typed
                .write_batch(&values, Some(&def_levels), Some(&rep_levels))
                .unwrap();
            row_group_writer.close_column(data_writer).unwrap();
            let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
            let typed = get_typed_column_writer_mut::<BoolType>(&mut data_writer);
            typed
                .write_batch(&null_values, Some(&def_levels), Some(&rep_levels))
                .unwrap();
            row_group_writer.close_column(data_writer).unwrap();
            writer.close_row_group(row_group_writer).unwrap();
            future::ready(())
        })
        .await;
    writer
}

pub async fn serialzie_data<W>(
    mut writer: SerializedFileWriter<W>,
    tbname: &str,
    stream: BlockStream<'_>,
) -> SerializedFileWriter<W>
where
    W: ParquetWriter + 'static,
{
    stream
        .enumerate()
        .for_each(|(_, partial)| {
            let mut row_group_writer = writer.next_row_group().unwrap();
            row_group_writer = serialzie_tbname(row_group_writer, tbname);
            let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
            let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
            let values_length = partial.num_of_fields();
            let def_levels = vec![2; values_length];
            let mut rep_levels = vec![2; values_length];
            rep_levels[0] = 0;
            let mut values = vec![];
            let mut null_values = vec![];
            for col in partial.columns_iter() {
                match col.into_owned() {
                    taos::block::Column::Null(_) => todo!(),
                    taos::block::Column::Bool(_, _) => todo!(),
                    taos::block::Column::TinyInt(_, _) => todo!(),
                    taos::block::Column::SmallInt(_, _) => todo!(),
                    taos::block::Column::Int(is_nulls, vs) => {
                        let mut tmp_vec = Vec::new();
                        for v in vs {
                            tmp_vec.extend_from_slice(&v.to_string().as_bytes());
                        }
                        null_values.push(ByteArray::from(
                            is_nulls
                                .into_bools()
                                .into_iter()
                                .map(|f| f as u8)
                                .collect::<Vec<u8>>(),
                        ));
                        values.push(ByteArray::from(tmp_vec));
                        // null_values.append(&mut is_nulls.into_bools());
                    }
                    taos::block::Column::BigInt(_, _) => todo!(),
                    taos::block::Column::Float(is_nulls, vs) => {
                        let mut tmp_vec = Vec::new();
                        for v in vs {
                            tmp_vec.extend_from_slice(&v.to_string().as_bytes());
                        }
                        null_values.push(ByteArray::from(
                            is_nulls
                                .into_bools()
                                .into_iter()
                                .map(|f| f as u8)
                                .collect::<Vec<u8>>(),
                        ));
                        values.push(ByteArray::from(tmp_vec));
                        // null_values.append(&mut is_nulls.into_bools());
                    }
                    taos::block::Column::Double(_, _) => todo!(),
                    taos::block::Column::Binary(_) => todo!(),
                    taos::block::Column::Timestamp(is_nulls, vs) => {
                        let mut tmp_vec = Vec::new();
                        for v in vs {
                            tmp_vec.extend_from_slice(&v.to_string().as_bytes());
                        }
                        values.push(ByteArray::from(tmp_vec));
                        null_values.push(ByteArray::from(
                            is_nulls
                                .into_bools()
                                .into_iter()
                                .map(|f| f as u8)
                                .collect::<Vec<u8>>(),
                        ));
                    }
                    taos::block::Column::NChar(_) => todo!(),
                    taos::block::Column::UTinyInt(_, _) => todo!(),
                    taos::block::Column::USmallInt(_, _) => todo!(),
                    taos::block::Column::UInt(_, _) => todo!(),
                    taos::block::Column::UBigInt(_, _) => todo!(),
                    _ => todo!(),
                }
            }
            typed
                .write_batch(&values, Some(&def_levels), Some(&rep_levels))
                .unwrap();
            row_group_writer.close_column(data_writer).unwrap();
            let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
            let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
            typed
                .write_batch(&null_values, Some(&def_levels), Some(&rep_levels))
                .unwrap();
            row_group_writer.close_column(data_writer).unwrap();
            writer.close_row_group(row_group_writer).unwrap();
            future::ready(())
        })
        .await;
    writer
}
