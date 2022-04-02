use bevy_reflect::Struct;
use futures::{future, StreamExt};
use libtaos::TaosDataType;
use parquet::{
    basic::Compression,
    column::writer::*,
    data_type::{BoolType, ByteArray, ByteArrayType, Int32Type},
    file::{
        properties::WriterProperties,
        writer::{FileWriter, ParquetWriter, RowGroupWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use serde::de::value;
use std::sync::Arc;
use taos::block::BlockStream;
use taos::Taos;
use taosx::Database;

use super::fetch::TableInfo;

pub fn serialize_dbinfo<W>(schema: Arc<Type>, database: Database, target: W)
where
    W: ParquetWriter + 'static,
{
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(target, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let mut rep = 0;
    for (i, value) in database.iter_fields().enumerate() {
        if let Some(value) = value.downcast_ref::<String>() {
            if i == 0 {
                let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
                typed
                    .write_batch(
                        &[ByteArray::from(value.as_bytes().to_owned())],
                        Some(&[0]),
                        Some(&[0]),
                    )
                    .unwrap();
                row_group_writer.close_column(data_writer).unwrap();
                data_writer = row_group_writer.next_column().unwrap().unwrap();
            } else {
                def_levels.push(1);
                rep_levels.push(rep);
                values.push(ByteArray::from(value.as_bytes().to_owned()));
                rep = 1;
            }
            println!("i: {}, values: {}", i, *value);
        }
    }
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    typed
        .write_batch(&values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}

pub fn serialzie_tablename(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    for table in tableinfo_list {
        values.push(ByteArray::from(table.name.as_str()));
        def_levels.push(1);
        rep_levels.push(0);
    }

    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_colname(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let def = 2;
    let mut rep;
    for table in tableinfo_list {
        rep = 0;
        for col in &table.cols {
            values.push(ByteArray::from(col.name.as_str()));
            def_levels.push(def);
            rep_levels.push(rep);
            rep = 2;
        }
    }

    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_tagname(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let def = 2;
    let mut rep;
    for table in tableinfo_list {
        rep = 0;
        if let Some(tags) = &table.tags {
            for tag in tags {
                values.push(ByteArray::from(tag.name.as_str()));
                def_levels.push(def);
                rep_levels.push(rep);
                rep = 2;
            }
        } else {
            rep_levels.push(0);
            def_levels.push(1);
        }
    }

    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_coltype(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<Int32Type>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let def = 2;
    let mut rep;
    for table in tableinfo_list {
        rep = 0;
        for col in &table.cols {
            values.push(col.type_ as i32);
            def_levels.push(def);
            rep_levels.push(rep);
            rep = 2;
        }
    }
    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_tagtype(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<Int32Type>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let def = 2;
    let mut rep;
    for table in tableinfo_list {
        rep = 0;
        if let Some(tags) = &table.tags {
            for tag in tags {
                values.push(tag.type_ as i32);
                def_levels.push(def);
                rep_levels.push(rep);
                rep = 2;
            }
        } else {
            rep_levels.push(0);
            def_levels.push(1);
        }
    }
    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_collength(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<Int32Type>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let mut def;
    let mut rep;
    for table in tableinfo_list {
        rep = 0;
        for col in &table.cols {
            values.push(col.bytes as i32);
            def = match col.type_ {
                TaosDataType::Binary => 3,
                TaosDataType::NChar => 3,
                _ => 2,
            };
            def_levels.push(def);
            rep_levels.push(rep);
            rep = 1;
        }
    }

    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub fn serialize_taglength(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<Int32Type>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let mut def;
    let mut rep;
    for table in tableinfo_list {
        rep = 0;
        if let Some(tags) = &table.tags {
            for tag in tags {
                values.push(tag.bytes as i32);
                def = match tag.type_ {
                    TaosDataType::Binary => 3,
                    TaosDataType::NChar => 3,
                    _ => 2,
                };
                def_levels.push(def);
                rep_levels.push(rep);
                rep = 1;
            }
        } else {
            rep_levels.push(0);
            def_levels.push(1);
        }
    }

    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub async fn serialize_tag_data(
    mut row_group_writer: Box<dyn RowGroupWriter>,
    tableinfo_list: &Vec<TableInfo>,
) -> Box<dyn RowGroupWriter> {
    let taos = Taos::new("10.72.136.169", "root", "taosdata", "", 6030).unwrap();
    let mut data_writer = row_group_writer.next_column().unwrap().unwrap();
    let typed = get_typed_column_writer_mut::<ByteArrayType>(&mut data_writer);
    let mut values = vec![];
    let mut def_levels = vec![];
    let mut rep_levels = vec![];
    let def = 2;
    let mut rep;
    for tableinfo in tableinfo_list {
        rep = 0;
        if let Some(buffer) = &tableinfo.tag_buffer {
            let res = taos
                .query(format! {"select tbname, {} from test.{}", buffer, tableinfo.name})
                .await
                .unwrap();
            let stream = res.fetch_block_stream();
            stream
                .enumerate()
                .for_each(|(_, partial)| {
                    let row_num = partial.num_of_rows();
                    for col in partial.columns_iter() {
                        match col {
                            taos::block::BorrowedColumn::Bool(_, v) => todo!(),
                            taos::block::BorrowedColumn::TinyInt(_, v) => todo!(),
                            taos::block::BorrowedColumn::SmallInt(_, v) => todo!(),
                            taos::block::BorrowedColumn::Int(_, v) => todo!(),
                            taos::block::BorrowedColumn::BigInt(_, v) => todo!(),
                            taos::block::BorrowedColumn::Float(_, v) => todo!(),
                            taos::block::BorrowedColumn::Double(_, v) => todo!(),
                            taos::block::BorrowedColumn::Binary(v) => todo!(),
                            taos::block::BorrowedColumn::Timestamp(_, v) => todo!(),
                            taos::block::BorrowedColumn::NChar(v) => todo!(),
                            taos::block::BorrowedColumn::UTinyInt(_, v) => todo!(),
                            taos::block::BorrowedColumn::USmallInt(_, v) => todo!(),
                            taos::block::BorrowedColumn::UInt(_, v) => todo!(),
                            taos::block::BorrowedColumn::UBigInt(_, v) => todo!(),
                            _ => unreachable!(),
                        }
                    }
                    future::ready(())
                })
                .await;
        } else {
            rep_levels.push(0);
            def_levels.push(1);
        }
    }
    typed
        .write_batch(&mut values, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    row_group_writer.close_column(data_writer).unwrap();
    row_group_writer
}

pub async fn serialize_tableinfo<W>(schema: Arc<Type>, tableinfo_list: Vec<TableInfo>, target: W)
where
    W: ParquetWriter + 'static,
{
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(target, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    row_group_writer = serialzie_tablename(row_group_writer, &tableinfo_list);
    row_group_writer = serialize_colname(row_group_writer, &tableinfo_list);
    row_group_writer = serialize_coltype(row_group_writer, &tableinfo_list);
    row_group_writer = serialize_collength(row_group_writer, &tableinfo_list);
    row_group_writer = serialize_tagname(row_group_writer, &tableinfo_list);
    row_group_writer = serialize_tagtype(row_group_writer, &tableinfo_list);
    row_group_writer = serialize_taglength(row_group_writer, &tableinfo_list);
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
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
                            tmp_vec.extend_from_slice(&v.to_be_bytes());
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
                            tmp_vec.extend_from_slice(&v.to_be_bytes());
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
                            tmp_vec.extend_from_slice(&v.to_be_bytes());
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
