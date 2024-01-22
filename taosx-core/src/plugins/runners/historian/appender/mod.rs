use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Local, NaiveDateTime, TimeZone};
use itertools::Itertools;
use serde_json::json;
use tiberius::{ColumnType, Row};

use taosx_ipc::prelude::ArrowDataType;

use crate::runners::historian::appender::history::History;
use crate::runners::historian::appender::live::Live;
use crate::runners::historian::config::HistorianTable;

mod history;
mod live;

pub struct ArrowDataAppender {
    schema: Schema,
    data_builders: Vec<Box<dyn ArrayBuilder>>,
}

impl ArrowDataAppender {
    pub fn try_new(table: HistorianTable) -> anyhow::Result<Self> {
        // fields
        let fields = match table {
            HistorianTable::History => Self::history_fields(),
            HistorianTable::Live => Self::live_fields(),
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

    fn history_fields() -> Vec<Field> {
        let mut fields = Vec::new();

        fields.push(Field::new(
            "DateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            false,
        ));
        fields.push(Field::new("TagName", ArrowDataType::Utf8, false));
        fields.push(Field::new("Value", ArrowDataType::Float64, true));
        fields.push(Field::new("vValue", ArrowDataType::Utf8, true));
        fields.push(Field::new("Quality", ArrowDataType::UInt8, false));
        fields.push(Field::new("QualityDetail", ArrowDataType::Int32, true));
        fields.push(Field::new("wwTagKey", ArrowDataType::Int32, false));
        fields.push(Field::new("wwResolution", ArrowDataType::Int32, true));
        fields.push(Field::new(
            "StartDateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            false,
        ));
        fields.push(Field::new("SourceTag", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceServer", ArrowDataType::Utf8, true));

        fields
    }

    fn live_fields() -> Vec<Field> {
        let mut fields = Vec::new();

        fields.push(Field::new(
            "DateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            false,
        ));
        fields.push(Field::new("TagName", ArrowDataType::Utf8, false));
        fields.push(Field::new("Value", ArrowDataType::Float64, true));
        fields.push(Field::new("vValue", ArrowDataType::Utf8, true));
        fields.push(Field::new("Quality", ArrowDataType::UInt8, false));
        fields.push(Field::new("QualityDetail", ArrowDataType::Int32, true));
        fields.push(Field::new("OPCQuality", ArrowDataType::Int32, true));
        fields.push(Field::new("wwTagKey", ArrowDataType::Int32, false));
        fields.push(Field::new("SourceTag", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceServer", ArrowDataType::Utf8, true));

        fields
    }

    pub fn append_history_row(&mut self, row: &Row) -> anyhow::Result<History> {
        let datetime = self
            .append_timestamp(row, "DateTime", 0)?
            .ok_or(anyhow::anyhow!("DateTime cannot be None"))?;
        let tag_name = self.append_tag_name(row, "TagName", 1)?;
        let value = self.append_float64(row, "Value", 2)?;
        let v_value = self.append_string(row, "vValue", 3)?;
        let quality = self
            .append_uint8(row, "Quality", 4)?
            .ok_or(anyhow::anyhow!("Quality cannot be None"))?;
        let quality_detail = self.append_int32(row, "QualityDetail", 5)?;
        let ww_tag_key = self
            .append_int32(row, "wwTagKey", 6)?
            .ok_or(anyhow::anyhow!("wwTagKey cannot be None"))?;
        let ww_resolution = self.append_int32(row, "wwResolution", 7)?;
        let start_datetime = self
            .append_timestamp(row, "StartDateTime", 8)?
            .ok_or(anyhow::anyhow!("StartDateTime cannot be None"))?;
        let source_tag = self.append_string(row, "SourceTag", 9)?;
        let source_server = self.append_string(row, "SourceServer", 10)?;

        Ok(History {
            datetime,
            tag_name,
            value,
            v_value,
            quality,
            quality_detail,
            ww_tag_key,
            ww_resolution,
            start_datetime,
            source_tag,
            source_server,
        })
    }

    pub fn append_live_row(&mut self, row: &Row) -> anyhow::Result<Live> {
        let datetime = self
            .append_timestamp(row, "DateTime", 0)?
            .ok_or(anyhow::anyhow!("DateTime cannot be None"))?;
        let tag_name = self.append_tag_name(row, "TagName", 1)?;
        let value = self.append_float64(row, "Value", 2)?;
        let v_value = self.append_string(row, "vValue", 3)?;
        let quality = self
            .append_uint8(row, "Quality", 4)?
            .ok_or(anyhow::anyhow!("Quality cannot be None"))?;
        let quality_detail = self.append_int32(row, "QualityDetail", 5)?;
        let opc_quality = self.append_int32(row, "OPCQuality", 6)?;
        let ww_tag_key = self
            .append_int32(row, "wwTagKey", 7)?
            .ok_or(anyhow::anyhow!("wwTagKey cannot be None"))?;
        let source_tag = self.append_string(row, "SourceTag", 8)?;
        let source_server = self.append_string(row, "SourceServer", 9)?;

        Ok(Live {
            datetime,
            tag_name,
            value,
            v_value,
            quality,
            quality_detail,
            opc_quality,
            ww_tag_key,
            source_tag,
            source_server,
        })
    }

    fn append_timestamp(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<Option<i64>> {
        let val = row.try_get::<NaiveDateTime, _>(column_name)?;
        let ts = match val {
            None => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::TimestampNanosecondBuilder>()
                    .unwrap()
                    .append_null();
                None
            }
            Some(val) => {
                let ts = Local::now()
                    .fixed_offset()
                    .timezone()
                    .from_local_datetime(&val)
                    .unwrap()
                    .timestamp_nanos_opt()
                    .unwrap();

                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::TimestampNanosecondBuilder>()
                    .unwrap()
                    .append_value(ts);
                Some(ts)
            }
        };
        Ok(ts)
    }

    fn append_tag_name(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<String> {
        let val = row.try_get::<&str, _>(column_name)?;
        let tag_name = match val {
            None => {
                return Err(anyhow::anyhow!("TagName cannot be None"));
            }
            Some(val) => {
                let new_tag_name = val.replace(".", "_").replace("`", "_");

                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::StringBuilder>()
                    .unwrap()
                    .append_value(new_tag_name.clone());

                new_tag_name
            }
        };

        Ok(tag_name)
    }

    fn append_string(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<Option<String>> {
        let val = row.try_get::<&str, _>(column_name)?;
        let string_value = match val {
            None => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::StringBuilder>()
                    .unwrap()
                    .append_null();

                None
            }
            Some(val) => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::StringBuilder>()
                    .unwrap()
                    .append_value(val);

                Some(val.to_string())
            }
        };
        Ok(string_value)
    }

    fn append_float64(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<Option<f64>> {
        let val = row.try_get::<f64, _>(column_name)?;
        let float_value = match val {
            None => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::Float64Builder>()
                    .unwrap()
                    .append_null();
                None
            }
            Some(val) => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::Float64Builder>()
                    .unwrap()
                    .append_value(val);
                Some(val)
            }
        };

        Ok(float_value)
    }

    fn append_int32(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<Option<i32>> {
        let val = row.try_get::<i32, _>(column_name)?;
        let i32_value = match val {
            None => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::Int32Builder>()
                    .unwrap()
                    .append_null();
                None
            }
            Some(val) => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::Int32Builder>()
                    .unwrap()
                    .append_value(val);
                Some(val)
            }
        };
        Ok(i32_value)
    }

    fn append_uint8(
        &mut self,
        row: &Row,
        column_name: &str,
        index: usize,
    ) -> anyhow::Result<Option<u8>> {
        let val = row.try_get::<u8, _>(column_name)?;
        let u8_value = match val {
            None => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::UInt8Builder>()
                    .unwrap()
                    .append_null();
                None
            }
            Some(val) => {
                self.data_builders[index]
                    .as_any_mut()
                    .downcast_mut::<array::UInt8Builder>()
                    .unwrap()
                    .append_value(val);
                Some(val)
            }
        };
        Ok(u8_value)
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

pub fn to_json_value(
    row: &Row,
    idx: usize,
    col_type: ColumnType,
) -> anyhow::Result<serde_json::Value> {
    let col_val = match col_type {
        ColumnType::Datetime2 => json!(row.try_get::<NaiveDateTime, _>(idx)?),
        ColumnType::Int1 => json!(row.try_get::<u8, _>(idx)?),
        ColumnType::Int4 => json!(row.try_get::<i32, _>(idx)?),
        ColumnType::Intn => json!(row.try_get::<i32, _>(idx)?),
        ColumnType::Floatn => json!(row.try_get::<f64, _>(idx)?),
        ColumnType::NVarchar => json!(row.try_get::<&str, _>(idx)?),
        _ => {
            return Err(anyhow::anyhow!("Unsupported column type: {:?}", col_type));
        }
    };

    Ok(col_val)
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
