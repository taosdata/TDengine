use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, FixedOffset, NaiveDateTime};
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
    time_zone: String,
) -> anyhow::Result<RecordBatch> {
    to_record_batches(col_map, rows, usize::MAX, time_zone).map(|batches| batches[0].clone())
}

pub fn to_record_batches(
    col_map: LinkedHashMap<String, OracleType>,
    rows: Vec<oracle::Row>,
    batch_size: usize,
    time_zone: String,
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
                OracleType::Timestamp(_) => {
                    let val = col.get::<NaiveDateTime>();
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
                                .append_value(format!("{:?}", val));
                        }
                    }
                }
                OracleType::TimestampTZ(_) => {
                    let val = col.get::<DateTime<FixedOffset>>();
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
                                .append_value(val.timestamp_nanos_opt().unwrap() as i64);
                        }
                    }
                }
                OracleType::TimestampLTZ(_) => {
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
                            let time_zone = FixedOffset::from_str(time_zone.as_str()).unwrap();
                            let val_with_tz = val.and_local_timezone(time_zone).unwrap();
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_value(val_with_tz.timestamp_nanos_opt().unwrap() as i64);
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
                OracleType::UInt64 => {
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
    use taos::Dsn;

    fn test_create_table() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                let sql_create_table = "create table t_metric (id NUMBER(10, 0) PRIMARY KEY, name VARCHAR2(255), value NUMBER(10, 2), ts timestamp)";
                let x = conn.execute(sql_create_table, &[]);
                println!("create table: {:?}", x);
                let y = conn.commit();
                println!("commit: {:?}", y);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    fn test_insert_data(len: usize) {
        let _ = test_create_table();

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                for i in 0..len {
                    let sql_insert_data = format!("insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, sysdate)", i);
                    let _ = conn.execute(&sql_insert_data.as_str(), &[]);
                }
                let _ = conn.commit();
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    fn test_clear_data() {
        let _ = test_create_table();

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                let sql = "delete from t_metric where 1 = 1";
                let _ = conn.execute(sql, &[]);
                let _ = conn.commit();
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_to_schema() {
        // prepare data
        let _ = test_clear_data();
        let _ = test_insert_data(1);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.select_for_schema("select * from t_metric");
                match query_result {
                    Ok(col_map) => {
                        let schema = to_schema(col_map).unwrap();
                        dbg!(&schema);
                        assert_eq!(schema.fields().len(), 4);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        let _ = test_clear_data();
    }

    #[tokio::test]
    async fn test_to_record_batch() {
        // prepare data
        let _ = test_clear_data();
        let _ = test_insert_data(3);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric");
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(&col_map);
                        let batch = to_record_batch(col_map, rows, String::from("+08:00")).unwrap();
                        dbg!(&batch);
                        assert_eq!(batch.num_columns(), 4);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        let _ = test_clear_data();
    }

    #[tokio::test]
    async fn test_to_record_batches() {
        // prepare data
        let _ = test_clear_data();
        let _ = test_insert_data(7);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric");
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(&col_map);
                        let batches =
                            to_record_batches(col_map, rows, 3, String::from("+08:00")).unwrap();
                        dbg!(&batches);
                        assert_eq!(batches.len(), 3);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        let _ = test_clear_data();
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
