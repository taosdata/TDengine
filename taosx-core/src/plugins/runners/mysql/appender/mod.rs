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
                                    .append_value(val);
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'TINYINT' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int8Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "TINYINT UNSIGNED" => {
                    let val = row.try_get::<Option<u8>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt8Builder>()
                                    .unwrap()
                                    .append_null();
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
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt8Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "SMALLINT" => {
                    let val = row.try_get::<Option<i16>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'SMALLINT' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int16Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "SMALLINT UNSIGNED" => {
                    let val = row.try_get::<Option<u16>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt16Builder>()
                                    .unwrap()
                                    .append_null();
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
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt16Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "MEDIUMINT" | "INT" => {
                    let val = row.try_get::<Option<i32>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'MEDIUMINT/INT' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int32Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
                    let val = row.try_get::<Option<u32>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt32Builder>()
                                    .unwrap()
                                    .append_null();
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
                            tracing::warn!("migrate mysql, decoding 'MEDIUMINT UNSIGNED/INT UNSIGNED' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt32Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "BIGINT" => {
                    let val = row.try_get::<Option<i64>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'BIGINT' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "BIGINT UNSIGNED" => {
                    let val = row.try_get::<Option<u64>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt64Builder>()
                                    .unwrap()
                                    .append_null();
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
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt64Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 浮点数
                "FLOAT" => {
                    let val = row.try_get::<Option<f32>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'FLOAT' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float32Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "DOUBLE" => {
                    let val = row.try_get::<Option<f64>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'DOUBLE' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "DECIMAL" => {
                    let val = row.try_get::<Option<bigdecimal::BigDecimal>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                                    .append_value(val.to_string());
                            }
                        },
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'DECIMAL' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 字符串
                "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDUIMTEXT" | "LONGTEXT" => {
                    let val = row.try_get::<Option<String>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'CHAR/VARCHAR/...' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                    let val = row.try_get::<Option<&[u8]>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'BINARY/VARBINARY/...' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 日期时间
                "DATE" => {
                    let val = row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'DATE' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "TIME" => {
                    let val = row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'TIME' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "DATETIME" => {
                    let val = row.try_get::<Option<NaiveDateTime>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!(
                                "migrate mysql, decoding 'DATETIME' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "TIMESTAMP" => {
                    let val = row.try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::TimestampNanosecondBuilder>()
                                    .unwrap()
                                    .append_null();
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
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "YEAR" => {
                    let val = row.try_get::<Option<u16>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt16Builder>()
                                    .unwrap()
                                    .append_null();
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
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt16Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 二进制
                "BIT" => {
                    let val = row.try_get::<Option<u8>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
                            None => {
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::UInt8Builder>()
                                    .unwrap()
                                    .append_null();
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
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::UInt8Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                _ => {
                    let val = row.try_get::<Option<String>, _>(col_cidx);
                    match val {
                        Ok(val) => match val {
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
                        Err(e) => {
                            tracing::warn!("migrate mysql, decoding 'UNKNOWN' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
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
    use crate::runners::mysql::{config::connect::ConnectConfig, query::MySqlQuery};
    use sqlx::Executor;
    use std::str::FromStr;
    use taos::Dsn;

    async fn test_create_database() {
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/information_schema").unwrap();
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

    async fn test_create_table() {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_table = "create table if not exists t_metric (id int primary key auto_increment, name varchar(255), value double, ts timestamp)";
                let _ = query.pool.execute(sql_create_table).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_insert_data =
                    "insert into t_metric (name, value, ts) values ('cpu', 0.8, now())";
                for _ in 0..len {
                    let _ = query.pool.execute(sql_insert_data).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql = "delete from t_metric where 1 = 1";
                let _ = query.pool.execute(sql).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_to_schema() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(1).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let row = query
                    .select_one_for_schema("select * from t_metric")
                    .await
                    .unwrap();
                match row {
                    Some(row) => {
                        let schema = to_schema(row).await.unwrap();
                        dbg!(&schema);
                        assert_eq!(schema.fields().len(), 4);
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
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_to_record_batch() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query.select_all("select * from t_metric").await.unwrap();

        let batch = to_record_batch(rows, String::from("+08:00")).await.unwrap();
        assert_eq!(batch.num_columns(), 4);
        // clear data
        let _ = test_clear_data().await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_to_record_batches() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query.select_all("select * from t_metric").await.unwrap();

        let batches = to_record_batches(rows, 3, String::from("+08:00"))
            .await
            .unwrap();
        dbg!(&batches);
        assert_eq!(batches.len(), 3);
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
