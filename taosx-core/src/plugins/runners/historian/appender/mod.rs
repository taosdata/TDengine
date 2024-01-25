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
        fields.push(Field::new("OPCQuality", ArrowDataType::Int32, true));
        fields.push(Field::new("wwTagKey", ArrowDataType::Int32, false));
        fields.push(Field::new("wwRowCount", ArrowDataType::Int32, true));
        fields.push(Field::new("wwResolution", ArrowDataType::Int32, true));
        fields.push(Field::new("wwEdgeDetection", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwRetrievalMode", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwTimeDeadband", ArrowDataType::Int32, true));
        fields.push(Field::new("wwValueDeadband", ArrowDataType::Float64, true));
        fields.push(Field::new("wwTimeZone", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwVersion", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwCycleCount", ArrowDataType::Int32, true));
        fields.push(Field::new("wwTimeStampRule", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwInterpolationType", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwQualityRule", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwStateCalc", ArrowDataType::Utf8, true));
        fields.push(Field::new("StateTime", ArrowDataType::Float64, true));
        fields.push(Field::new("PercentGood", ArrowDataType::Float64, true));
        fields.push(Field::new("wwParameters", ArrowDataType::Utf8, true));
        fields.push(Field::new(
            "StartDateTime",
            ArrowDataType::Timestamp(Nanosecond, None),
            false,
        ));
        fields.push(Field::new("SourceTag", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceServer", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwFilter", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwValueSelector", ArrowDataType::Utf8, false));
        fields.push(Field::new("wwMaxStates", ArrowDataType::Int32, true));
        fields.push(Field::new("wwOption", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwExpression", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwUnit", ArrowDataType::Utf8, true));

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
        fields.push(Field::new("wwRetrievalMode", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwTimeDeadband", ArrowDataType::Int32, true));
        fields.push(Field::new("wwValueDeadband", ArrowDataType::Float64, true));
        fields.push(Field::new("wwTimeZone", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwParameters", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceTag", ArrowDataType::Utf8, true));
        fields.push(Field::new("SourceServer", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwValueSelector", ArrowDataType::Utf8, false));
        fields.push(Field::new("wwExpression", ArrowDataType::Utf8, true));
        fields.push(Field::new("wwUnit", ArrowDataType::Utf8, true));

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
        let opc_quality = self.append_int32(row, "OPCQuality", 6)?;
        let ww_tag_key = self
            .append_int32(row, "wwTagKey", 7)?
            .ok_or(anyhow::anyhow!("wwTagKey cannot be None"))?;
        let ww_row_count = self.append_int32(row, "wwRowCount", 8)?;
        let ww_resolution = self.append_int32(row, "wwResolution", 9)?;
        let ww_edge_detection = self.append_string(row, "wwEdgeDetection", 10)?;

        let ww_retrieval_mode = self.append_string(row, "wwRetrievalMode", 11)?;
        let ww_time_dead_band = self.append_int32(row, "wwTimeDeadband", 12)?;
        let ww_value_dead_band = self.append_float64(row, "wwValueDeadband", 13)?;
        let ww_time_zone = self.append_string(row, "wwTimeZone", 14)?;
        let ww_version = self.append_string(row, "wwVersion", 15)?;
        let ww_cycle_count = self.append_int32(row, "wwCycleCount", 16)?;
        let ww_time_stamp_rule = self.append_string(row, "wwTimeStampRule", 17)?;
        let ww_interpolation_type = self.append_string(row, "wwInterpolationType", 18)?;
        let ww_quality_rule = self.append_string(row, "wwQualityRule", 19)?;
        let ww_state_calc = self.append_string(row, "wwStateCalc", 20)?;
        let state_time = self.append_float64(row, "StateTime", 21)?;
        let percent_good = self.append_float64(row, "PercentGood", 22)?;
        let ww_parameters = self.append_string(row, "wwParameters", 23)?;
        let start_datetime = self
            .append_timestamp(row, "StartDateTime", 24)?
            .ok_or(anyhow::anyhow!("StartDateTime cannot be None"))?;
        let source_tag = self.append_string(row, "SourceTag", 25)?;
        let source_server = self.append_string(row, "SourceServer", 26)?;
        let ww_filter = self.append_string(row, "wwFilter", 27)?;
        let ww_value_selector = self
            .append_string(row, "wwValueSelector", 28)?
            .ok_or(anyhow::anyhow!("wwValueSelector cannot be None"))?;
        let ww_max_states = self.append_int32(row, "wwMaxStates", 29)?;
        let ww_option = self.append_string(row, "wwOption", 30)?;
        let ww_expression = self.append_string(row, "wwExpression", 31)?;
        let ww_unit = self.append_string(row, "wwUnit", 32)?;

        Ok(History {
            datetime,
            tag_name,
            value,
            v_value,
            quality,
            quality_detail,
            opc_quality,
            ww_tag_key,
            ww_row_count,
            ww_resolution,
            ww_edge_detection,
            ww_retrieval_mode,
            ww_time_dead_band,
            ww_value_dead_band,
            ww_time_zone,
            ww_version,
            ww_cycle_count,
            ww_time_stamp_rule,
            ww_interpolation_type,
            ww_quality_rule,
            ww_state_calc,
            state_time,
            percent_good,
            ww_parameters,
            start_datetime,
            source_tag,
            source_server,
            ww_filter,
            ww_value_selector,
            ww_max_states,
            ww_option,
            ww_expression,
            ww_unit,
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
        let ww_retrieval_mode = self.append_string(row, "wwRetrievalMode", 8)?;
        let ww_time_dead_band = self.append_int32(row, "wwTimeDeadband", 9)?;
        let ww_value_dead_band = self.append_float64(row, "wwValueDeadband", 10)?;
        let ww_time_zone = self.append_string(row, "wwTimeZone", 11)?;
        let ww_parameters = self.append_string(row, "wwParameters", 12)?;

        let source_tag = self.append_string(row, "SourceTag", 13)?;
        let source_server = self.append_string(row, "SourceServer", 14)?;
        let ww_value_selector = self
            .append_string(row, "wwValueSelector", 15)?
            .ok_or(anyhow::anyhow!("wwValueSelector cannot be None"))?;
        let ww_expression = self.append_string(row, "wwExpression", 16)?;
        let ww_unit = self.append_string(row, "wwUnit", 17)?;

        Ok(Live {
            datetime,
            tag_name,
            value,
            v_value,
            quality,
            quality_detail,
            opc_quality,
            ww_tag_key,
            ww_retrieval_mode,
            ww_time_dead_band,
            ww_value_dead_band,
            ww_time_zone,
            ww_parameters,
            source_tag,
            source_server,
            ww_value_selector,
            ww_expression,
            ww_unit,
        })
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
