use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{format, DateTime, Datelike, Timelike, Utc};
use itertools::Itertools;
use sqlx::{Column, Row, TypeInfo};
use sqlx_postgres::types::PgTimeTz;
use sqlx_postgres::PgRow;

pub mod column_meta;

pub async fn to_schema(row: PgRow) -> anyhow::Result<Schema> {
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

pub async fn to_record_batch(rows: Vec<PgRow>) -> anyhow::Result<RecordBatch> {
    to_record_batches(rows, usize::MAX)
        .await
        .map(|batches| batches[0].clone())
}

pub async fn to_record_batches(
    rows: Vec<PgRow>,
    batch_size: usize,
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
                // 布尔值
                "BOOL" => {
                    let val = row.try_get::<Option<bool>, _>(col_cidx);
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
                            tracing::warn!("migrate postgres, decoding 'BOOL' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 字符
                "CHAR" => {
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
                            tracing::warn!("migrate postgres, decoding 'CHAR' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 整型数
                "SMALLINT" | "SMALLSERIAL" | "INT2" => {
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
                            tracing::warn!("migrate postgres, decoding 'SMALLINT/SMALLSERIAL/INT2' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int16Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "INT" | "SERIAL" | "INT4" => {
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
                                "migrate postgres, decoding 'INT/SERIAL/INT4' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int32Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "BIGINT" | "BIGSERIAL" | "INT8" => {
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
                            tracing::warn!("migrate postgres, decoding 'BIGINT/BIGSERIAL/INT8' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Int64Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 浮点数
                "REAL" | "FLOAT4" => {
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
                            tracing::warn!(
                                "migrate postgres, decoding 'REAL/FLOAT4' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float32Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "DOUBLE PRECISION" | "FLOAT8" => {
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
                            tracing::warn!("migrate postgres, decoding 'DOUBLE PRECISION/FLOAT8' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "NUMERIC" => {
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
                            tracing::warn!(
                                "migrate postgres, decoding 'NUMERIC' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // 字符串
                "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" | "CITEXT" => {
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
                            tracing::warn!("migrate postgres, decoding 'VARCHAR/CHAR(N)/...' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "BYTEA" => {
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
                            tracing::warn!(
                                "migrate postgres, decoding 'BYTEA' result error: {e:?}"
                            );
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
                            tracing::warn!("migrate postgres, decoding 'DATE' result error: {e:?}");
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
                            tracing::warn!("migrate postgres, decoding 'TIME' result error: {e:?}");
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "TIMESTAMP" => {
                    let val =
                        row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(col_cidx);
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
                                "migrate postgres, decoding 'TIMESTAMP' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "TIMESTAMPTZ" => {
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
                                builders[col_cidx]
                                    .as_any_mut()
                                    .downcast_mut::<array::TimestampNanosecondBuilder>()
                                    .unwrap()
                                    .append_value(val.timestamp_nanos_opt().unwrap());
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate postgres, decoding 'TIMESTAMPTZ' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // uuid
                "UUID" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
                }
                // 二进制数组
                "BIT" | "VARBIT" => {
                    let val = row.try_get::<Option<bit_vec::BitVec>, _>(col_cidx);
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
                                "migrate postgres, decoding 'BIT/VARBIT' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // json
                "JSON" | "JSONB" => {
                    let val = row.try_get::<Option<serde_json::Value>, _>(col_cidx);
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
                                "migrate postgres, decoding 'JSON/JSONB' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                // Others
                "INTERVAL" => {
                    let val = row.try_get::<Option<sqlx_postgres::types::PgInterval>, _>(col_cidx);
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
                                "migrate postgres, decoding 'INTERVAL' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
                }
                "MONEY" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
                }
                "LTREE" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
                }
                "LQUERY" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
                }
                "TIMETZ" => {
                    let val = row.try_get::<Option<PgTimeTz>, _>(col_cidx);
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
                                    .append_value(format!("{} {}", val.time, val.offset));
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "migrate postgres, decoding 'TIMETZ' result error: {e:?}"
                            );
                            builders[col_cidx]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                    }
                }
                "INET" | "CIDR" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
                }
                "MACADDR" => {
                    // TODO
                    builders[col_cidx]
                        .as_any_mut()
                        .downcast_mut::<array::StringBuilder>()
                        .unwrap()
                        .append_null();
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
                            tracing::warn!(
                                "migrate postgres, decoding 'UNKNOWN' result error: {e:?}"
                            );
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
    use crate::runners::postgres::{config::connect::ConnectConfig, query::PostgresQuery};
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test]
    async fn test_to_schema() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select * from information_schema.tables")
            .await
            .unwrap();

        match row {
            Some(row) => {
                let schema = to_schema(row).await.unwrap();
                dbg!(schema);
            }
            None => {
                println!("no row");
            }
        }
    }

    #[tokio::test]
    async fn test_to_record_batch() {
        let dsn = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_all("select * from public.pg_test1")
            .await
            .unwrap();

        let batch = to_record_batch(rows).await.unwrap();
        dbg!(batch);
    }

    #[tokio::test]
    async fn test_to_record_batches() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_all("select * from information_schema.tables")
            .await
            .unwrap();

        let batches = to_record_batches(rows, 3).await.unwrap();
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
