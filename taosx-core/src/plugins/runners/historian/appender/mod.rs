use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::TimeUnit::Nanosecond;
use chrono::{Local, NaiveDateTime, TimeZone};
use futures_util::TryStreamExt;
use itertools::Itertools;
use tiberius::{ColumnType, QueryItem, QueryStream};

pub mod column_meta;

pub async fn to_record_batch(stream: QueryStream<'_>) -> anyhow::Result<RecordBatch> {
    to_record_batches(stream, usize::MAX)
        .await
        .map(|batches| batches[0].clone())
}

pub async fn to_record_batches(
    mut stream: QueryStream<'_>,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut columns = Vec::new();
    let mut builders = Vec::new();
    let mut fields = Vec::new();
    let mut batches = Vec::new();

    let mut row_count = 0;
    while let Some(item) = stream.try_next().await? {
        match item {
            QueryItem::Metadata(meta) => {
                for col in meta.columns() {
                    let col_name = col.name().to_string();
                    let col_type = col.column_type();
                    columns.push((col_name, col_type));
                }

                for (col_name, col_type) in columns.iter() {
                    let arrow_type = to_arrow_data_type(*col_type)?;
                    fields.push(Field::new(col_name, arrow_type.clone(), true));
                    builders.push(array::make_builder(&arrow_type, 10));
                }
            }
            QueryItem::Row(row) => {
                for (idx, (_col_name, col_type)) in columns.iter().enumerate() {
                    match col_type {
                        ColumnType::Null => {
                            builders[idx]
                                .as_any_mut()
                                .downcast_mut::<array::NullBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        ColumnType::Int1 => {
                            let val = row.try_get::<u8, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::UInt8Builder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::UInt8Builder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Intn => {
                            let val = row.try_get::<i32, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Int32Builder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Int32Builder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                        ColumnType::Float4 | ColumnType::Float8 | ColumnType::Floatn => {
                            let val = row.try_get::<f64, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Float64Builder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::Float64Builder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                        ColumnType::Datetime2 => {
                            let val = row.try_get::<NaiveDateTime, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::TimestampNanosecondBuilder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    let ts = Local::now()
                                        .fixed_offset()
                                        .timezone()
                                        .from_local_datetime(&val)
                                        .unwrap()
                                        .timestamp_nanos_opt()
                                        .unwrap();

                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::TimestampNanosecondBuilder>()
                                        .unwrap()
                                        .append_value(ts);
                                }
                            }
                        }
                        _ => {
                            let val = row.try_get::<&str, _>(idx)?;
                            match val {
                                None => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::StringBuilder>()
                                        .unwrap()
                                        .append_null();
                                }
                                Some(val) => {
                                    builders[idx]
                                        .as_any_mut()
                                        .downcast_mut::<array::StringBuilder>()
                                        .unwrap()
                                        .append_value(val);
                                }
                            }
                        }
                    }
                }

                if row_count == batch_size {
                    let batch = to_batch(fields.clone(), builders).await?;
                    batches.push(batch);

                    builders = Vec::new();
                    for (_col_name, col_type) in columns.iter() {
                        let arrow_type = to_arrow_data_type(*col_type)?;
                        builders.push(array::make_builder(&arrow_type, 10));
                    }
                    row_count = 0;
                }

                row_count += 1;
            }
        }
    }

    let batch = to_batch(fields, builders).await?;
    batches.push(batch);

    Ok(batches)
}

async fn to_batch(
    fields: Vec<Field>,
    mut builders: Vec<Box<dyn ArrayBuilder>>,
) -> anyhow::Result<RecordBatch> {
    // schema
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("lush"));

    let schema = Schema::new(fields).with_metadata(metadata);
    let array_refs = builders
        .iter_mut()
        .map(|builder| Arc::new(builder.finish()) as ArrayRef)
        .collect_vec();

    let batch = RecordBatch::try_new(Arc::new(schema), array_refs)?;
    Ok(batch)
}

fn to_arrow_data_type(col_type: ColumnType) -> anyhow::Result<DataType> {
    let data_type = match col_type {
        ColumnType::Bit => DataType::Boolean,
        ColumnType::Int1 => DataType::UInt8,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 => DataType::Float64,
        ColumnType::Intn => DataType::Int32,
        ColumnType::Floatn => DataType::Float64,
        ColumnType::Datetime2 => DataType::Timestamp(Nanosecond, None),
        ColumnType::NVarchar => DataType::Utf8,
        _ => Err(anyhow::anyhow!("Unsupported column type: {:?}", col_type))?,
    };

    Ok(data_type)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_replace() {
        let s = "h_02324202110001_114.1M";
        // let regex = Regex::new(r"[^0-9a-zA-Z_]+").unwrap();
        // let new_s = regex.replace_all(s, "_").to_string();
        let new_s = s.to_string().replace(".", "_").replace("`", "_");
        assert_eq!(new_s, "h_02324202110001_114_1M");
    }
}
