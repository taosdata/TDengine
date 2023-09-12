use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use itertools::Itertools;
use tiberius::{Column, ColumnType, Row};

use taosx_ipc::prelude::ArrowDataType;

pub struct ArrowDataAppender {
    column_meta: Vec<Column>,
    schema: Schema,
    data_builders: Vec<Box<dyn ArrayBuilder>>,
}

impl ArrowDataAppender {
    pub fn new(columns: &[Column]) -> Self {
        // column meta
        let column_meta = columns.iter().map(|c| c.clone()).collect_vec();

        // schema
        let mut metadata = HashMap::new();
        metadata.insert(String::from("version"), String::from("1.0"));
        metadata.insert(String::from("stream"), String::from("flat"));
        metadata.insert(String::from("ack"), String::from("lush"));
        // fields
        let fields = columns
            .iter()
            .map(|c| Field::new(c.name(), to_arrow_type(c.column_type()), true))
            .collect_vec();

        // data builders
        let data_builders = fields
            .iter()
            .map(|f| array::make_builder(f.data_type(), 10))
            .collect_vec();

        ArrowDataAppender {
            column_meta,
            schema: Schema::new(fields).with_metadata(metadata),
            data_builders,
        }
    }

    pub fn append_row(&mut self, row: Row) -> anyhow::Result<()> {
        for (index, col) in self.column_meta.iter().enumerate() {
            match col.column_type() {
                ColumnType::Datetime2 => {
                    let val = row.try_get::<NaiveDateTime, _>(index)?;
                    match val {
                        None => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::TimestampNanosecondBuilder>()
                                .unwrap()
                                .append_value(val.timestamp_nanos());
                        }
                    }
                }
                ColumnType::NVarchar => {
                    let val = row.try_get::<&str, _>(index)?;
                    match val {
                        None => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::StringBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                ColumnType::Floatn => {
                    let val = row.try_get::<f64, _>(index)?;
                    match val {
                        None => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::Float64Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                ColumnType::Int1 => {
                    let val = row.try_get::<u8, _>(index)?;
                    match val {
                        None => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::UInt8Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::UInt8Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                ColumnType::Intn | ColumnType::Int4 => {
                    let val = row.try_get::<i32, _>(index)?;
                    match val {
                        None => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::Int32Builder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::Int32Builder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
                _ => {
                    let val = row.try_get::<&[u8], _>(index)?;
                    match val {
                        None => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::BinaryBuilder>()
                                .unwrap()
                                .append_null();
                        }
                        Some(val) => {
                            self.data_builders[index]
                                .as_any_mut()
                                .downcast_mut::<array::BinaryBuilder>()
                                .unwrap()
                                .append_value(val);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> anyhow::Result<RecordBatch> {
        let array_refs = self
            .data_builders
            .iter_mut()
            .map(|builder| Arc::new(builder.finish()) as ArrayRef)
            .collect_vec();

        let batch = RecordBatch::try_new(Arc::new(self.schema.clone()), array_refs)?;
        Ok(batch)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn to_arrow_type(column_type: ColumnType) -> ArrowDataType {
    match column_type {
        ColumnType::Datetime2 => ArrowDataType::Timestamp(Nanosecond, None),
        ColumnType::NVarchar => ArrowDataType::Utf8,
        ColumnType::Floatn => ArrowDataType::Float64,
        ColumnType::Int1 => ArrowDataType::UInt8,
        ColumnType::Intn | ColumnType::Int4 => ArrowDataType::Int32,
        _ => ArrowDataType::Binary,
    }
}
