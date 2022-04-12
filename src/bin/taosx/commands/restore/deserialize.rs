use parquet::{
    column::reader::ColumnReader,
    data_type::ByteArray,
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::Field,
    schema::{printer, types::Type},
};
use std::io::BufReader;
use std::{fs::File, path::PathBuf};
use taos_sys::TaosDataType;
use taosx::Database;

pub fn deserialize_database(path: PathBuf) -> Database {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap()
}

fn _print_schema(schema: &Type) {
    let mut buf = Vec::new();
    printer::print_schema(&mut buf, &schema);

    let string_schema = String::from_utf8(buf).unwrap();

    log::info!("\n{}", string_schema);
}

pub async fn deserialize_table_info(file: String) -> Vec<String> {
    let parquet_reader = SerializedFileReader::try_from(file).unwrap();
    let mut create_table_list = vec![];
    // let schema = parquet_reader.metadata().file_metadata().schema();
    // print_schema(schema);

    let iter = parquet_reader.get_row_iter(None).unwrap();
    for row in iter {
        let mut create_sql = String::from("create table ");
        let mut tag_buffer = String::from("tags (");
        for (_, field) in row.get_column_iter() {
            if let Field::ListInternal(v) = field {
                for field in v.elements() {
                    if let Field::Group(g) = field {
                        for (_, field) in g.get_column_iter() {
                            match field {
                                Field::Bytes(v) => {
                                    create_sql += v.as_utf8().unwrap();
                                    create_sql += " (";
                                }
                                Field::ListInternal(l) => {
                                    for field in l.elements() {
                                        let mut tmp = String::from("");

                                        if let Field::Group(g) = field {
                                            let mut col_iter = g.get_column_iter();
                                            let (_, field) = col_iter.next().unwrap();
                                            if let Field::Bytes(b) = field {
                                                tmp += b.as_utf8().unwrap();
                                                tmp += " "
                                            }
                                            let (_, field) = col_iter.next().unwrap();
                                            let t;
                                            match field {
                                                Field::Int(i) => {
                                                    t = TaosDataType::from(*i as u8);
                                                    tmp += TaosDataType::from(t).as_str();
                                                }
                                                _ => unreachable!(),
                                            }

                                            let (_, field) = col_iter.next().unwrap();
                                            if t == TaosDataType::NChar || t == TaosDataType::Binary
                                            {
                                                if let Field::Int(i) = field {
                                                    tmp += format!("({})", *i).as_str();
                                                }
                                            }
                                            tmp += ",";
                                            let (_, field) = col_iter.next().unwrap();
                                            if let Field::Bool(b) = field {
                                                if *b {
                                                    tag_buffer += tmp.as_str();
                                                } else {
                                                    create_sql += tmp.as_str();
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
            }
        }
        create_sql.pop();
        tag_buffer.pop();
        create_sql += ") ";
        tag_buffer += ")";
        create_sql += &tag_buffer;
        create_table_list.push(create_sql);
    }
    create_table_list
}

pub async fn deserialzie_tags(file: String) -> Vec<String> {
    let parquet_reader = SerializedFileReader::try_from(file).unwrap();

    // let schema = parquet_reader.metadata().file_metadata().schema();
    // print_schema(schema);
    let mut sql_list = vec![];
    let iter = parquet_reader.get_row_iter(None).unwrap();
    for row in iter {
        for (_, field) in row.get_column_iter() {
            if let Field::ListInternal(v) = field {
                for field in v.elements() {
                    let mut stable_name = String::new();
                    if let Field::Group(row) = field {
                        for (_, field) in row.get_column_iter() {
                            if let Field::Bytes(b) = field {
                                stable_name = b.as_utf8().unwrap().to_string();
                            }
                            if let Field::ListInternal(l) = field {
                                for field in l.elements() {
                                    if let Field::Group(g) = field {
                                        let mut sql = String::from("create table ");
                                        for (_, field) in g.get_column_iter() {
                                            if let Field::ListInternal(l) = field {
                                                let mut index = 0;
                                                for field in l.elements() {
                                                    if let Field::Group(g) = field {
                                                        let mut iter = g.get_column_iter();
                                                        let (_, field1) = iter.next().unwrap();
                                                        let (_, field2) = iter.next().unwrap();
                                                        if let Field::Bool(b) = field2 {
                                                            if *b {
                                                                sql += "NULL,";
                                                            } else {
                                                                if let Field::Bytes(b) = field1 {
                                                                    if index == 0 {
                                                                        sql += b.as_utf8().unwrap();
                                                                        sql += format!(
                                                                            " using {} tags (",
                                                                            stable_name
                                                                        )
                                                                        .as_str();
                                                                    } else {
                                                                        sql += b.as_utf8().unwrap();
                                                                        // sql += b.data().;
                                                                        sql += ",";
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    index += 1;
                                                }
                                            }
                                        }
                                        sql.pop();
                                        sql += ")";
                                        sql_list.push(sql);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    sql_list
}

pub async fn deserialize_data(file: String, _db: String) {
    let parquet_reader = SerializedFileReader::try_from(file).unwrap();
    for row_group in 0..parquet_reader.num_row_groups() {
        let row_group_reader = parquet_reader.get_row_group(row_group).unwrap();
        for col_num in 0..row_group_reader.num_columns() {
            let mut col_reader = row_group_reader.get_column_reader(col_num).unwrap();
            match col_reader {
                ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                    let mut values = vec![
                        ByteArray::from(vec![0; 10]),
                        ByteArray::from(vec![0; 10]),
                        ByteArray::from(vec![0; 10]),
                        ByteArray::from(vec![0; 10]),
                        ByteArray::from(vec![0; 10]),
                        ByteArray::from(vec![0; 10]),
                    ];
                    // let mut def_levels = vec![2; 10];
                    // let mut rep_levels = vec![2; 10];
                    // rep_levels[0] = 0;
                    let (num, _) = typed_reader
                        .read_batch(
                            10,
                            // Some(&mut def_levels),
                            None,
                            // Some(&mut rep_levels),
                            None,
                            &mut values,
                        )
                        .unwrap();
                    dbg!(values.split_at(num).0);
                }
                _ => todo!(),
            }
        }
    }
    let iter = parquet_reader.get_row_iter(None).unwrap();
    for row in iter {
        // dbg!(row);
    }
    // let schema = parquet_reader.metadata().file_metadata().schema();
    // print_schema(schema);
}
