use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use itertools::Itertools;
use tiberius::Row;

use taosx_ipc::prelude::ArrowDataType;

use crate::runners::historian::config::TaskConfig;
use crate::runners::historian::table_type::HistorianTable;

pub struct ArrowDataAppender {
    schema: Schema,
    data_builders: Vec<Box<dyn ArrayBuilder>>,
}

impl ArrowDataAppender {
    pub fn new(task_config: &TaskConfig) -> anyhow::Result<Self> {
        let table = HistorianTable::from_str(&task_config.table)
            .map_err(|err| anyhow::anyhow!("invalid table: {}", err.to_string()))?;
        // fields
        let fields = match table {
            HistorianTable::Live => Self::live_fields(),
            HistorianTable::History => Self::history_fields(),
        };

        // data builders
        let data_builders = fields
            .iter()
            .map(|f| array::make_builder(f.data_type(), 10))
            .collect_vec();

        // schema
        let mut metadata = HashMap::new();
        metadata.insert(String::from("version"), String::from("1.0"));
        metadata.insert(String::from("stream"), String::from("flat"));
        metadata.insert(String::from("ack"), String::from("lush"));

        Ok(Self {
            schema: Schema::new(fields).with_metadata(metadata),
            data_builders,
        })
    }

    fn live_fields() -> Vec<Field> {
        let mut fields = Vec::new();

        fields.push(Field::new(
            "DateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            true,
        ));
        fields.push(Field::new("TagName", ArrowDataType::Utf8, true));
        fields.push(Field::new("Value", ArrowDataType::Float64, true));
        fields.push(Field::new("vValue", ArrowDataType::Utf8, true));
        fields.push(Field::new("Quality", ArrowDataType::Int32, true));
        fields.push(Field::new("QualityDetail", ArrowDataType::Int32, true));
        fields.push(Field::new("OPCQuality", ArrowDataType::Int32, true));
        fields.push(Field::new("wwTagKey", ArrowDataType::Int32, true));
        fields.push(Field::new("SourceTag", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceServer", ArrowDataType::Utf8, true));

        fields
    }

    fn history_fields() -> Vec<Field> {
        let mut fields = Vec::new();

        fields.push(Field::new(
            "DateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            true,
        ));
        fields.push(Field::new("TagName", ArrowDataType::Utf8, true));
        fields.push(Field::new("Value", ArrowDataType::Float64, true));
        fields.push(Field::new("vValue", ArrowDataType::Utf8, true));
        fields.push(Field::new("Quality", ArrowDataType::Int32, true));
        fields.push(Field::new("QualityDetail", ArrowDataType::Int32, true));
        fields.push(Field::new("wwTagKey", ArrowDataType::Int32, true));
        fields.push(Field::new("wwResolution", ArrowDataType::Int32, true));
        fields.push(Field::new(
            "StartDateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            true,
        ));
        fields.push(Field::new("SourceTag", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceServer", ArrowDataType::Utf8, true));

        fields
    }

    pub fn append_history_row(&mut self, row: Row) -> anyhow::Result<()> {
        self.append_timestamp(&row, "DateTime", 0)?;
        self.append_string(&row, "TagName", 1)?;
        self.append_float64(&row, "Value", 2)?;
        self.append_string(&row, "vValue", 3)?;
        self.append_int32(&row, "Quality", 4)?;
        self.append_int32(&row, "QualityDetail", 5)?;
        self.append_int32(&row, "wwTagKey", 6)?;
        self.append_int32(&row, "wwResolution", 7)?;
        self.append_timestamp(&row, "StartDateTime", 8)?;
        self.append_string(&row, "SourceTag", 9)?;
        self.append_string(&row, "SourceServer", 10)?;

        Ok(())
    }

    pub fn append_live_row(&mut self, row: Row) -> anyhow::Result<()> {
        self.append_timestamp(&row, "DateTime", 0)?;
        self.append_string(&row, "TagName", 1)?;
        self.append_float64(&row, "Value", 2)?;
        self.append_string(&row, "vValue", 3)?;
        self.append_int32(&row, "Quality", 4)?;
        self.append_int32(&row, "QualityDetail", 5)?;
        self.append_int32(&row, "OPCQuality", 6)?;
        self.append_int32(&row, "wwTagKey", 7)?;
        self.append_string(&row, "SourceTag", 8)?;
        self.append_string(&row, "SourceServer", 9)?;

        Ok(())
    }

    fn append_timestamp(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<()> {
        let val = row.try_get::<NaiveDateTime, _>(column_name)?;
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
                    .append_value(val.timestamp_nanos_opt().expect(
                        "value can not be represented in a timestamp with nanosecond precision.",
                    ));
            }
        }
        Ok(())
    }

    fn append_string(&mut self, row: &Row, column_name: &str, index: usize) -> anyhow::Result<()> {
        let val = row.try_get::<&str, _>(column_name)?;
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
        Ok(())
    }

    fn append_float64(&mut self, row: &Row, column_name: &str, index: usize) -> anyhow::Result<()> {
        let val = row.try_get::<f64, _>(column_name)?;
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
        Ok(())
    }

    fn append_int32(&mut self, row: &Row, column_name: &str, index: usize) -> anyhow::Result<()> {
        let val = row.try_get::<i32, _>(column_name)?;
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
