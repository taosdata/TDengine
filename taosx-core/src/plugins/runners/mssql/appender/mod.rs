use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use tiberius::{ColumnType, Row};

pub mod column_meta;

pub fn to_schema(col_map: LinkedHashMap<String, ColumnType>) -> anyhow::Result<Schema> {
    let mut fields = Vec::new();
    for (col_name, col_type) in col_map {
        // arrow data type
        let arrow_type = column_meta::to_arrow_data_type(&col_type)?;
        fields.push(Field::new(col_name, arrow_type.clone(), true));
    }
    let schema = build_schema(fields)?;
    Ok(schema)
}

#[allow(dead_code)]
pub fn to_record_batch(
    col_map: LinkedHashMap<String, ColumnType>,
    rows: Vec<Row>,
    time_zone: String,
) -> anyhow::Result<RecordBatch> {
    to_record_batches(col_map, rows, usize::MAX, time_zone).map(|batches| batches[0].clone())
}

pub fn to_record_batches(
    col_map: LinkedHashMap<String, ColumnType>,
    rows: Vec<Row>,
    batch_size: usize,
    _time_zone: String,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut fields = Vec::new();
    let mut builders = Vec::new();
    let mut batches = Vec::new();

    let mut row_count = 0;

    for (col_name, col_type) in col_map.clone() {
        // arrow data type
        let arrow_type = column_meta::to_arrow_data_type(&col_type)?;
        fields.push(Field::new(col_name, arrow_type.clone(), true));
        builders.push(array::make_builder(&arrow_type, 10));
    }

    for row in rows {
        for (col_cidx, col) in row.into_iter().enumerate() {
            // column type, data type is not exact
            let col_type = col_map
                .iter()
                .nth(col_cidx)
                .map(|(_, value)| value)
                .unwrap();
            match col {
                tiberius::ColumnData::U8(val) => match col_type {
                    ColumnType::Int1 => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int8Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int8Builder>()
                                .unwrap()
                                .append_value(val as i8);
                        }
                    },
                    ColumnType::Intn => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_value(val as i64);
                        }
                    },
                    _ => anyhow::bail!("mistake data type: {:?}", col_type),
                },
                tiberius::ColumnData::I16(val) => match col_type {
                    ColumnType::Int2 => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int16Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int16Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    },
                    ColumnType::Intn => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_value(val as i64);
                        }
                    },
                    _ => anyhow::bail!("mistake data type: {:?}", col_type),
                },
                tiberius::ColumnData::I32(val) => match col_type {
                    ColumnType::Int4 => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int32Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int32Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    },
                    ColumnType::Intn => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_value(val as i64);
                        }
                    },
                    _ => anyhow::bail!("mistake data type: {:?}", col_type),
                },
                tiberius::ColumnData::I64(val) => match col_type {
                    ColumnType::Int8 | ColumnType::Intn => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    },
                    _ => anyhow::bail!("mistake data type: {:?}", col_type),
                },
                tiberius::ColumnData::F32(val) => match col_type {
                    ColumnType::Float4 => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float32Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float32Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    },
                    ColumnType::Floatn => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_value(val as f64);
                        }
                    },
                    _ => anyhow::bail!("mistake data type: {:?}", col_type),
                },
                tiberius::ColumnData::F64(val) => match col_type {
                    ColumnType::Float8 | ColumnType::Floatn => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    },
                    ColumnType::Money | ColumnType::Money4 => match val {
                        None => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(format!("{:?}", val));
                        }
                    },
                    _ => anyhow::bail!("mistake data type: {:?}", col_type),
                },
                tiberius::ColumnData::Bit(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", val));
                    }
                },
                tiberius::ColumnData::String(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(val);
                    }
                },
                tiberius::ColumnData::Guid(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", val));
                    }
                },
                tiberius::ColumnData::Binary(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", val));
                    }
                },
                tiberius::ColumnData::Numeric(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(val.to_string().replace(".-", "."));
                    }
                },
                tiberius::ColumnData::Xml(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", val));
                    }
                },
                tiberius::ColumnData::DateTime(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        let days = val.days();
                        let secs = val.seconds_fragments();
                        // convert to seconds and nanoseconds(since 1900-01-01 00:00:00, and seconds are actually 1/300 seconds)
                        let secs = (days as u32 - 25567) * 24 * 60 * 60 + secs / 300;
                        // convert to datetime with timezone
                        let datetime = DateTime::from_timestamp(secs as i64, 0_u32).unwrap();

                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", datetime));
                    }
                },
                tiberius::ColumnData::SmallDateTime(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        let days = val.days();
                        let secs = val.seconds_fragments();
                        // convert to seconds and nanoseconds(since 1900-01-01 00:00:00, and seconds are actually minutes)
                        let secs = (days as i64 - 25567) * 24 * 60 * 60 + (secs as i64) * 60;
                        // convert to datetime with timezone
                        let datetime = DateTime::from_timestamp(secs, 0_u32).unwrap();

                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", datetime));
                    }
                },
                tiberius::ColumnData::Time(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        // convert to seconds and nanoseconds
                        let secs = val.increments() / (10_u64.pow(val.scale() as u32));
                        let nsecs = val.increments() % (10_u64.pow(val.scale() as u32))
                            * 10_u64.pow(9 - val.scale() as u32);
                        let time = NaiveTime::from_num_seconds_from_midnight_opt(
                            secs as u32,
                            nsecs as u32,
                        )
                        .unwrap();

                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", time));
                    }
                },
                tiberius::ColumnData::Date(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        // convert to days(since 1st of January, year 1)
                        let days = val.days() + 1;
                        // convert to naivedate
                        let date = NaiveDate::from_num_days_from_ce_opt(days as i32).unwrap();

                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", date));
                    }
                },
                tiberius::ColumnData::DateTime2(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        let date = val.date();
                        let time = val.time();
                        // convert to seconds and nanoseconds(since 1st of January, year 1)
                        let secs = (date.days() as u64 - 719162) * 24 * 60 * 60
                            + time.increments() / (10_u64.pow(time.scale() as u32));
                        let nsecs = time.increments() % (10_u64.pow(time.scale() as u32))
                            * 10_u64.pow(9 - time.scale() as u32);
                        // convert to datetime with timezone
                        let datetime = DateTime::from_timestamp(secs as i64, nsecs as u32).unwrap();
                        // append value
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::StringBuilder>()
                            .unwrap()
                            .append_value(format!("{:?}", datetime));
                    }
                },
                tiberius::ColumnData::DateTimeOffset(val) => match val {
                    None => {
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::TimestampNanosecondBuilder>()
                            .unwrap()
                            .append_null();
                    }
                    Some(val) => {
                        let datetime = val.datetime2();
                        let date = datetime.date();
                        let time = datetime.time();
                        // convert to seconds and nanoseconds(since 1st of January, year 1)
                        let secs = (date.days() as u64 - 719162) * 24 * 60 * 60
                            + time.increments() / (10_u64.pow(time.scale() as u32));
                        let nsecs = time.increments() % (10_u64.pow(time.scale() as u32))
                            * 10_u64.pow(9 - time.scale() as u32);
                        // get timezone
                        let _offset = FixedOffset::east_opt((val.offset() as i32) * 60).unwrap();
                        // convert to datetime(an accurate UTC time)
                        let datetime = DateTime::from_timestamp(secs as i64, nsecs as u32).unwrap();
                        // append value
                        builders[col_cidx]
                            .as_any_mut()
                            .downcast_mut::<array::TimestampNanosecondBuilder>()
                            .unwrap()
                            .append_value(datetime.timestamp_nanos_opt().unwrap());
                    }
                },
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
    use crate::runners::mssql::{config::connect::ConnectConfig, query::MssqlQuery};
    use std::str::FromStr;
    use taos::Dsn;

    async fn test_create_database() {
        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/master?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_database = "create database test_taosx";
                let mut conn = query.pool.get().await.unwrap();
                let _ = conn.execute(sql_create_database, &[]).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_create_table() {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_table = "create table t_metric (id bigint, name char(10), value float, ts datetimeoffset(7))";
                let mut conn = query.pool.get().await.unwrap();
                let x = conn.execute(sql_create_table, &[]).await;
                println!("create table: {:?}", x);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                for i in 0..len {
                    let sql_insert_data = format!("insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, GETDATE())", i);
                    let mut conn = query.pool.get().await.unwrap();
                    let _ = conn.execute(sql_insert_data, &[]).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql = "delete from t_metric where 1 = 1";
                let mut conn = query.pool.get().await.unwrap();
                let _ = conn.execute(sql, &[]).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_to_schema() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(1).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_for_schema("select * from t_metric").await;
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
        let _ = test_clear_data().await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_to_record_batch() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric").await;
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
        let _ = test_clear_data().await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_to_record_batches() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric").await;
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
        let _ = test_clear_data().await;
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
