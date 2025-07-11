use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{FixedOffset, NaiveDateTime};
use itertools::Itertools;
use sqlx::mysql::MySqlRow;
use sqlx::{Column, Row, TypeInfo};

pub mod column_meta;

pub async fn to_schema(row: MySqlRow) -> anyhow::Result<Schema> {
    let mut fields = Vec::new();
    for col in row.columns() {
        let col_name = col.name();
        let col_type = col.type_info().name();
        // arrow data type
        let arrow_type = column_meta::to_arrow_data_type(col_type.to_string())?;
        fields.push(Field::new(col_name.to_string(), arrow_type.clone(), true));
    }
    let schema = build_schema(fields)?;
    Ok(schema)
}

pub async fn to_record_batch(
    rows: Vec<MySqlRow>,
    time_zone: String,
) -> anyhow::Result<RecordBatch> {
    to_record_batches(rows, usize::MAX, time_zone)
        .await
        .map(|batches| batches[0].clone())
}

pub async fn to_record_batches(
    rows: Vec<MySqlRow>,
    batch_size: usize,
    time_zone: String,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut fields = Vec::new();
    let mut builders = Vec::new();
    let mut batches = Vec::new();

    let mut row_count = 0;

    macro_rules! append_null {
        ($builder:expr, $arrow_type:ty) => {
            $builder
                .as_any_mut()
                .downcast_mut::<$arrow_type>()
                .unwrap()
                .append_null();
        };
    }

    for (ridx, row) in rows.iter().enumerate() {
        if ridx == 0 {
            for col in row.columns() {
                let col_name = col.name();
                let col_type = col.type_info().name();
                // arrow data type
                let arrow_type = column_meta::to_arrow_data_type(col_type.to_string())?;
                fields.push(Field::new(col_name.to_string(), arrow_type.clone(), true));
                builders.push(array::make_builder(&arrow_type, 10));
            }
        }
        for col in row.columns() {
            let col_cidx = col.ordinal();
            let col_type = col.type_info().name();
            match col_type {
                // 整型数
                "TINYINT" => {
                    let val = row.try_get::<Option<i8>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::Int8Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::Int8Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'TINYINT' result error: {e:?}");
                            append_null!(builders[col_cidx], array::Int8Builder);
                        }
                    }
                }
                "TINYINT UNSIGNED" => {
                    let val = row.try_get::<Option<u8>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::UInt8Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt8Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'TINYINT UNSIGNED' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::UInt8Builder);
                        }
                    }
                }
                "SMALLINT" => {
                    let val = row.try_get::<Option<i16>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::Int16Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::Int16Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'SMALLINT' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::Int16Builder);
                        }
                    }
                }
                "SMALLINT UNSIGNED" => {
                    let val = row.try_get::<Option<u16>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::UInt16Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt16Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'SMALLINT UNSIGNED' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::UInt16Builder);
                        }
                    }
                }
                "MEDIUMINT" | "INT" => {
                    let val = row.try_get::<Option<i32>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::Int32Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::Int32Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'MEDIUMINT/INT' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::Int32Builder);
                        }
                    }
                }
                "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
                    let val = row.try_get::<Option<u32>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::UInt32Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt32Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'MEDIUMINT UNSIGNED/INT UNSIGNED' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::UInt32Builder);
                        }
                    }
                }
                "BIGINT" => {
                    let val = row.try_get::<Option<i64>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::Int64Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::Int64Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'BIGINT' result error: {e:?}");
                            append_null!(builders[col_cidx], array::Int64Builder);
                        }
                    }
                }
                "BIGINT UNSIGNED" => {
                    let val = row.try_get::<Option<u64>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::UInt64Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt64Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'BIGINT UNSIGNED' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::UInt64Builder);
                        }
                    }
                }
                // 浮点数
                "FLOAT" => {
                    let val = row.try_get::<Option<f32>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::Float32Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::Float32Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'FLOAT' result error: {e:?}");
                            append_null!(builders[col_cidx], array::Float32Builder);
                        }
                    }
                }
                "DOUBLE" => {
                    let val = row.try_get::<Option<f64>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::Float64Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::Float64Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'DOUBLE' result error: {e:?}");
                            append_null!(builders[col_cidx], array::Float64Builder);
                        }
                    }
                }
                "DECIMAL" => {
                    let val = row.try_get::<Option<bigdecimal::BigDecimal>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::StringBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::StringBuilder>()
                                    .unwrap()
                                    .append_value(val.to_string());
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'DECIMAL' result error: {e:?}");
                            append_null!(builders[col_cidx], array::StringBuilder);
                        }
                    }
                }
                // 字符串
                "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDUIMTEXT" | "LONGTEXT" => {
                    let val = row.try_get::<Option<String>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::StringBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::StringBuilder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'CHAR/VARCHAR/...' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::StringBuilder);
                        }
                    }
                }
                // 字节数组
                "BINARY" | "VARBINARY" => {
                    let val = row.try_get::<Option<&[u8]>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::BinaryBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::BinaryBuilder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'BINARY/VARBINARY/...' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::BinaryBuilder);
                        }
                    }
                }
                "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                    let val = row.try_get::<Option<&[u8]>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::StringBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::StringBuilder>()
                                    .unwrap()
                                    .append_value(format!("{:?}", val));
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'BINARY/VARBINARY/...' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::StringBuilder);
                        }
                    }
                }
                // 日期时间
                "DATE" => {
                    let val = row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::StringBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::StringBuilder>()
                                    .unwrap()
                                    .append_value(format!("{:?}", val));
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'DATE' result error: {e:?}");
                            append_null!(builders[col_cidx], array::StringBuilder);
                        }
                    }
                }
                "TIME" => {
                    let val = row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::StringBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::StringBuilder>()
                                    .unwrap()
                                    .append_value(format!("{:?}", val));
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'TIME' result error: {e:?}");
                            append_null!(builders[col_cidx], array::StringBuilder);
                        }
                    }
                }
                "DATETIME" => {
                    let val = row.try_get::<Option<NaiveDateTime>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::StringBuilder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::StringBuilder>()
                                    .unwrap()
                                    .append_value(format!("{:?}", val));
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'DATETIME' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::StringBuilder);
                        }
                    }
                }
                "TIMESTAMP" => {
                    let val = row.try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::TimestampNanosecondBuilder);
                            }
                            Some(val) => {
                                // mysql 的 timestamp 是基于 session 时区的假 UTC 时间，需要转换为真正的 UTC 时间
                                let time_zone = FixedOffset::from_str(time_zone.as_str()).unwrap();
                                let real_timestamp_utc =
                                    val.naive_utc().and_local_timezone(time_zone).unwrap();
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::TimestampNanosecondBuilder>()
                                    .unwrap()
                                    .append_value(
                                        real_timestamp_utc.timestamp_nanos_opt().unwrap(),
                                    );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'TIMESTAMP' result error: {e:?}"
                            );
                            append_null!(builders[col_cidx], array::TimestampNanosecondBuilder);
                        }
                    }
                }
                "YEAR" => {
                    let val = row.try_get::<Option<u16>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::UInt16Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt16Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'YEAR' result error: {e:?}");
                            append_null!(builders[col_cidx], array::UInt16Builder);
                        }
                    }
                }
                // 二进制
                "BIT" => {
                    let val = row.try_get::<Option<u8>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                append_null!(builders[col_cidx], array::UInt8Builder);
                            }
                            Some(val) => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt8Builder>()
                                    .unwrap()
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'BIT' result error: {e:?}");
                            append_null!(builders[col_cidx], array::UInt8Builder);
                        }
                    }
                }
                _ => {
                    tracing::warn!("migrate mysql, unknown column type: {col_type}");
                    append_null!(builders[col_cidx], array::StringBuilder);
                }
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
            for col in row.columns() {
                let col_type = col.type_info().name();
                // arrow data type
                let arrow_type = column_meta::to_arrow_data_type(col_type.to_string())?;
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
    use crate::{config::connect::ConnectConfig, query::MySqlQuery};
    use sqlx::Executor;
    use std::str::FromStr;
    use taos::Dsn;

    async fn test_create_database() {
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.45:3306/information_schema").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_database = "create database if not exists test_taosx";
                let _ = query.pool.execute(sql_create_database).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_create_table(table_name: &str) {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_drop_table = format!("drop table if exists {table_name}");
                let _ = query.pool.execute(sql_drop_table.as_str()).await;
                let sql_create_table = format!(
                    "create table if not exists {table_name} (id int primary key auto_increment, name varchar(255), value double, ts timestamp, v_tinyint tinyint, v_tinyint_unsigned tinyint unsigned, v_smallint smallint, v_smallint_unsigned smallint unsigned, v_mediumint mediumint, v_mediumint_unsigned mediumint unsigned, v_int int, v_int_unsigned int unsigned, v_bigint bigint, v_bigint_unsigned bigint unsigned, v_float float, v_double double, v_decimal decimal(10, 2), v_char char(10), v_varchar varchar(255), v_binary binary(10), v_varbinary varbinary(255), v_date date, v_time time, v_datetime datetime, v_timestamp timestamp, v_year year, v_bit bit(8))"
                );
                let _ = query.pool.execute(sql_create_table.as_str()).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(table_name: &str, len: usize) {
        let _ = test_create_table(table_name).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_insert_data = format!(
                    "insert into {table_name} (name, value, ts, v_tinyint, v_tinyint_unsigned, v_smallint, v_smallint_unsigned, v_mediumint, v_mediumint_unsigned, v_int, v_int_unsigned, v_bigint, v_bigint_unsigned, v_float, v_double, v_decimal, v_char, v_varchar, v_binary, v_varbinary, v_date, v_time, v_datetime, v_timestamp, v_year, v_bit) values ('cpu', 0.8, now(), 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 'a', 'a', 'a', 'a', '2021-01-01', '12:00:00', '2021-01-01 12:00:00', '2021-01-01 12:00:00', 2021, 1)"
                );
                for _ in 0..len {
                    let _ = query.pool.execute(sql_insert_data.as_str()).await;
                }
                // insert null
                let _ = query
                    .pool
                    .execute(
                        format!("insert into {table_name}(name) values ('null_values')").as_str(),
                    )
                    .await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data(table_name: &str) {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql = format!("delete from {table_name} where 1 = 1");
                let _ = query.pool.execute(sql.as_str()).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_to_schema_with_datasource() {
        // prepare data
        let _ = test_clear_data("test_to_schema").await;
        let _ = test_insert_data("test_to_schema", 1).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let row = query
                    .select_one_for_schema("select * from test_to_schema")
                    .await
                    .unwrap();
                match row {
                    Some(row) => {
                        let schema = to_schema(row).await.unwrap();
                        dbg!(&schema.fields().len());
                        // assert_eq!(schema.fields().len(), 27);
                    }
                    None => {
                        println!("no row");
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        let _ = test_clear_data("test_to_schema").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_to_record_batch() {
        // prepare data
        let _ = test_clear_data("test_to_record_batch").await;
        let _ = test_insert_data("test_to_record_batch", 3).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_all("select * from test_to_record_batch")
            .await
            .unwrap();

        dbg!(&rows);

        let batch = to_record_batch(rows, String::from("+08:00")).await.unwrap();
        dbg!(&batch.num_columns());
        // assert_eq!(batch.num_columns(), 27);
        // clear data
        let _ = test_clear_data("test_to_record_batch").await;
    }

    #[tokio::test]
    async fn test_to_record_batches() {
        // prepare data
        let _ = test_clear_data("test_to_record_batch").await;
        let _ = test_insert_data("test_to_record_batch", 7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_all("select * from test_to_record_batch")
            .await
            .unwrap();

        let batches = to_record_batches(rows, 3, String::from("+08:00"))
            .await
            .unwrap();
        dbg!(&batches.len());
        // assert_eq!(batches.len(), 3);
        // clear data
        let _ = test_clear_data("test_to_record_batch").await;
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
