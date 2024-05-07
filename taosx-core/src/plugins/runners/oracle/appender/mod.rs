use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use oracle::sql_type::OracleType;

pub mod column_meta;

pub fn to_schema(col_map: LinkedHashMap<String, OracleType>) -> anyhow::Result<Schema> {
    let mut fields = Vec::new();
    for (col_name, col_type) in col_map {
        // arrow data type
        let arrow_type = column_meta::to_arrow_data_type(&col_type)?;
        fields.push(Field::new(col_name.to_string(), arrow_type.clone(), true));
    }
    let schema = build_schema(fields)?;
    Ok(schema)
}

#[allow(dead_code)]
pub fn to_record_batch(
    col_map: LinkedHashMap<String, OracleType>,
    rows: Vec<oracle::Row>,
) -> anyhow::Result<RecordBatch> {
    to_record_batches(col_map, rows, usize::MAX).map(|batches| batches[0].clone())
}

pub fn to_record_batches(
    col_map: LinkedHashMap<String, OracleType>,
    rows: Vec<oracle::Row>,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut fields = Vec::new();
    let mut builders = Vec::new();
    let mut batches = Vec::new();

    let mut row_count = 0;

    for (col_name, col_type) in col_map.clone() {
        // arrow data type
        let arrow_type = column_meta::to_arrow_data_type(&col_type)?;
        fields.push(Field::new(col_name.to_string(), arrow_type.clone(), true));
        builders.push(array::make_builder(&arrow_type, 10));
    }

    for row in rows {
        for (col_cidx, col) in row.sql_values().iter().enumerate() {
            let col_type: &OracleType = col.oracle_type()?;
            match col_type {
                // 字符串
                OracleType::Varchar2(_)
                | OracleType::NVarchar2(_)
                | OracleType::Char(_)
                | OracleType::NChar(_)
                | OracleType::Rowid
                | OracleType::Raw(_) => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                // 浮点数
                OracleType::BinaryFloat => {
                    let val = col.get::<f32>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float32Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float32Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::BinaryDouble => {
                    let val = col.get::<f64>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::Number(_, _) | OracleType::Float(_) => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                // 日期时间
                OracleType::Date => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::Timestamp(_)
                | OracleType::TimestampTZ(_)
                | OracleType::TimestampLTZ(_) => {
                    let val = col.get::<NaiveDateTime>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_value(val.and_utc().timestamp_nanos_opt().unwrap() as i64);
                        }
                    }
                }
                OracleType::IntervalDS(_, _) | OracleType::IntervalYM(_) => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                // 大文本
                OracleType::CLOB | OracleType::NCLOB | OracleType::BLOB => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::BFILE | OracleType::RefCursor => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::Boolean => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::Object(_)
                | OracleType::Long
                | OracleType::LongRaw
                | OracleType::Json => {
                    let val = col.get::<String>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                // 整型数
                OracleType::Int64 => {
                    let val = col.get::<i64>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                OracleType::UInt64 => {
                    let val = col.get::<u64>();
                    match val {
                        Err(_) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Ok(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt64Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                } // 其他
                  // _ => anyhow::bail!("unsupported data type: {:?}", col_type),
            }
        }
        // increase row count
        row_count += 1;
        // check batch size
        if row_count == batch_size {
            // build record batch
            let batch = build_record_batch(fields.clone(), builders)?;
            batches.push(batch);
            // reset builders
            builders = Vec::new();
            for (_col_name, col_type) in col_map.clone() {
                // arrow data type
                let arrow_type = column_meta::to_arrow_data_type(&col_type)?;
                builders.push(array::make_builder(&arrow_type, 10));
            }
            // reset row count
            row_count = 0;
        }
    }
    let batch = build_record_batch(fields, builders)?;
    batches.push(batch);

    Ok(batches)
}

fn build_schema(fields: Vec<Field>) -> anyhow::Result<Schema> {
    // metadata
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("lush"));
    // schema
    let schema = Schema::new(fields).with_metadata(metadata);
    Ok(schema)
}

fn build_record_batch(
    fields: Vec<Field>,
    mut builders: Vec<Box<dyn ArrayBuilder>>,
) -> anyhow::Result<RecordBatch> {
    // schema
    let schema = build_schema(fields)?;
    // data array
    let array_refs = builders
        .iter_mut()
        .map(|builder| Arc::new(builder.finish()) as ArrayRef)
        .collect_vec();
    // record batch
    let batch = RecordBatch::try_new(Arc::new(schema), array_refs)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runners::oracle::{config::connect::ConnectConfig, query::OracleQuery};
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test]
    async fn test_to_schema() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let col_map = query.select_for_schema("select * from TEST").unwrap();
        let schema = to_schema(col_map).unwrap();
        dbg!(schema);
    }

    #[tokio::test]
    async fn test_to_record_batch() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let (col_map, rows) = query.select_all("select * from TEST").unwrap();

        let batch = to_record_batch(col_map, rows).unwrap();
        dbg!(batch);
    }

    #[tokio::test]
    async fn test_to_record_batches() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let (col_map, rows) = query.select_all("select * from TEST").unwrap();

        let batches = to_record_batches(col_map, rows, 3).unwrap();
        dbg!(batches);
    }

    #[test]
    fn test_build_schema() {
        let fields = vec![
            Field::new("id".to_string(), arrow::datatypes::DataType::Int32, true),
            Field::new("name".to_string(), arrow::datatypes::DataType::Utf8, true),
            Field::new("age".to_string(), arrow::datatypes::DataType::Int32, true),
        ];
        let schema = build_schema(fields).unwrap();
        dbg!(schema);
    }

    #[test]
    fn test_build_record_batch() {
        let fields = vec![
            Field::new("id".to_string(), arrow::datatypes::DataType::Int32, true),
            Field::new("name".to_string(), arrow::datatypes::DataType::Utf8, true),
            Field::new("age".to_string(), arrow::datatypes::DataType::Int32, true),
        ];
        let mut builders = vec![
            array::make_builder(&arrow_schema::DataType::Int32, 10),
            array::make_builder(&arrow_schema::DataType::Utf8, 10),
            array::make_builder(&arrow_schema::DataType::Int32, 10),
        ];
        builders[0]
            .as_any_mut()
            .downcast_mut::<array::Int32Builder>()
            .unwrap()
            .append_value(1);
        builders[1]
            .as_any_mut()
            .downcast_mut::<array::StringBuilder>()
            .unwrap()
            .append_value("Alice");
        builders[2]
            .as_any_mut()
            .downcast_mut::<array::Int32Builder>()
            .unwrap()
            .append_value(20);
        // build record batch
        let batch = build_record_batch(fields, builders).unwrap();
        dbg!(batch);
    }
}
