use arrow::array::{
    BinaryArray, BooleanArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::{RecordFilter, RecordFilterError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchRecordFilter {
    r#match: LinkedHashMap<String, JsonValue>,
}

impl MatchRecordFilter {
    pub fn new(r#match: LinkedHashMap<String, JsonValue>) -> Self {
        Self { r#match }
    }
}

impl RecordFilter for MatchRecordFilter {
    fn filter_records(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, RecordFilterError> {
        // 使用 fold 方式遍历 map，将筛选结果作为下一次的输入参数
        let result = self
            .r#match
            .iter()
            .fold(Ok(records.clone()), |result, map| {
                result.and_then(|result| {
                    // 每一行是否符合筛选条件
                    let mut filter: Vec<bool> = vec![];
                    // 根据名称获取列
                    match result.column_by_name(map.0) {
                        Some(column) => {
                            // 匹配规则
                            let value = map.1;
                            // 判断列的数据类型
                            match column.data_type() {
                                arrow::datatypes::DataType::Boolean => {
                                    column
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_boolean() {
                                                if data.unwrap() == value.as_bool().unwrap() {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Int8 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Int8Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as i8 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Int16 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Int16Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as i16 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Int32 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Int32Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as i32 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Int64 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::UInt8 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<UInt8Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as u8 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::UInt16 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<UInt16Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as u16 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::UInt32 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<UInt32Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as u32 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::UInt64 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<UInt64Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_i64() {
                                                if data.unwrap() == value.as_i64().unwrap() as u64 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Float16 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Float16Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_f64() {
                                                if data.unwrap().to_f32()
                                                    == value.as_f64().unwrap() as f32
                                                {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Float32 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Float32Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_f64() {
                                                if data.unwrap() == value.as_f64().unwrap() as f32 {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Float64 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            if value.is_f64() {
                                                if data.unwrap() == value.as_f64().unwrap() {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            } else if data.unwrap().to_string().as_str()
                                                == value.as_str().unwrap()
                                            {
                                                filter.push(true);
                                            } else {
                                                filter.push(false);
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Timestamp(unit, None) => match unit {
                                    arrow::datatypes::TimeUnit::Second => {
                                        column
                                            .as_any()
                                            .downcast_ref::<TimestampSecondArray>()
                                            .unwrap()
                                            .iter()
                                            .for_each(|data| {
                                                if value.is_i64() {
                                                    if data.unwrap() == value.as_i64().unwrap() {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                } else if data.unwrap().to_string().as_str()
                                                    == value.as_str().unwrap()
                                                {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            });
                                    }
                                    arrow::datatypes::TimeUnit::Millisecond => {
                                        column
                                            .as_any()
                                            .downcast_ref::<TimestampMillisecondArray>()
                                            .unwrap()
                                            .iter()
                                            .for_each(|data| {
                                                if value.is_i64() {
                                                    if data.unwrap() == value.as_i64().unwrap() {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                } else if data.unwrap().to_string().as_str()
                                                    == value.as_str().unwrap()
                                                {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            });
                                    }
                                    arrow::datatypes::TimeUnit::Microsecond => {
                                        column
                                            .as_any()
                                            .downcast_ref::<TimestampMicrosecondArray>()
                                            .unwrap()
                                            .iter()
                                            .for_each(|data| {
                                                if value.is_i64() {
                                                    if data.unwrap() == value.as_i64().unwrap() {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                } else if data.unwrap().to_string().as_str()
                                                    == value.as_str().unwrap()
                                                {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            });
                                    }
                                    arrow::datatypes::TimeUnit::Nanosecond => {
                                        column
                                            .as_any()
                                            .downcast_ref::<TimestampNanosecondArray>()
                                            .unwrap()
                                            .iter()
                                            .for_each(|data| {
                                                if value.is_i64() {
                                                    if data.unwrap() == value.as_i64().unwrap() {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                } else if data.unwrap().to_string().as_str()
                                                    == value.as_str().unwrap()
                                                {
                                                    filter.push(true);
                                                } else {
                                                    filter.push(false);
                                                }
                                            });
                                    }
                                },
                                arrow::datatypes::DataType::Binary => {
                                    column
                                        .as_any()
                                        .downcast_ref::<BinaryArray>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            match regex::Regex::new(value.as_str().unwrap()) {
                                                Ok(regex) => {
                                                    if regex.is_match(
                                                        String::from_utf8(data.unwrap().to_vec())
                                                            .unwrap()
                                                            .as_str(),
                                                    ) {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                                Err(_) => {
                                                    if String::from_utf8(data.unwrap().to_vec())
                                                        .unwrap()
                                                        .as_str()
                                                        == value.as_str().unwrap()
                                                    {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::FixedSizeBinary(_) => {
                                    column
                                        .as_any()
                                        .downcast_ref::<FixedSizeBinaryArray>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            match regex::Regex::new(value.as_str().unwrap()) {
                                                Ok(regex) => {
                                                    if regex.is_match(
                                                        String::from_utf8(data.unwrap().to_vec())
                                                            .unwrap()
                                                            .as_str(),
                                                    ) {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                                Err(_) => {
                                                    if String::from_utf8(data.unwrap().to_vec())
                                                        .unwrap()
                                                        .as_str()
                                                        == value.as_str().unwrap()
                                                    {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::LargeBinary => {
                                    column
                                        .as_any()
                                        .downcast_ref::<LargeBinaryArray>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            match regex::Regex::new(value.as_str().unwrap()) {
                                                Ok(regex) => {
                                                    if regex.is_match(
                                                        String::from_utf8(data.unwrap().to_vec())
                                                            .unwrap()
                                                            .as_str(),
                                                    ) {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                                Err(_) => {
                                                    if String::from_utf8(data.unwrap().to_vec())
                                                        .unwrap()
                                                        .as_str()
                                                        == value.as_str().unwrap()
                                                    {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::Utf8 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            match regex::Regex::new(value.as_str().unwrap()) {
                                                Ok(regex) => {
                                                    if regex.is_match(data.unwrap()) {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                                Err(_) => {
                                                    if data.unwrap() == value.as_str().unwrap() {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                            }
                                        });
                                }
                                arrow::datatypes::DataType::LargeUtf8 => {
                                    column
                                        .as_any()
                                        .downcast_ref::<LargeStringArray>()
                                        .unwrap()
                                        .iter()
                                        .for_each(|data| {
                                            match regex::Regex::new(value.as_str().unwrap()) {
                                                Ok(regex) => {
                                                    if regex.is_match(data.unwrap()) {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                                Err(_) => {
                                                    if data.unwrap() == value.as_str().unwrap() {
                                                        filter.push(true);
                                                    } else {
                                                        filter.push(false);
                                                    }
                                                }
                                            }
                                        });
                                }
                                _ => todo!(),
                            }
                        }
                        None => {
                            // 没有符合的列则默认全部保留
                            let num = records.num_rows();
                            for _ in 1..num {
                                filter.push(true);
                            }
                        }
                    };
                    let filter = BooleanArray::from(filter);
                    arrow::compute::filter_record_batch(&result, &filter)
                })
            });
        Ok(result.unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn test_filter_by_value_boolean() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        // fields.insert(String::from("a"), json!(true));
        fields.insert(String::from("a"), json!("true"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 4);
    }

    #[test]
    fn test_filter_by_value_number() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        // fields.insert(String::from("b"), json!(2));
        // fields.insert(String::from("b"), json!("2"));
        // fields.insert(String::from("c"), json!(2));
        // fields.insert(String::from("c"), json!("2"));
        // fields.insert(String::from("d"), json!(2));
        // fields.insert(String::from("d"), json!("2"));
        // fields.insert(String::from("e"), json!(2));
        // fields.insert(String::from("e"), json!("2"));
        // fields.insert(String::from("f"), json!(2));
        // fields.insert(String::from("f"), json!("2"));
        // fields.insert(String::from("g"), json!(2));
        // fields.insert(String::from("g"), json!("2"));
        // fields.insert(String::from("h"), json!(2));
        // fields.insert(String::from("h"), json!("2"));
        // fields.insert(String::from("i"), json!(2));
        // fields.insert(String::from("i"), json!("2"));
        // fields.insert(String::from("j"), json!(2.0));
        // fields.insert(String::from("j"), json!("2")); // 2.0 不过,只能 2
        // fields.insert(String::from("k"), json!(2.1));
        // fields.insert(String::from("k"), json!("2.1"));
        // fields.insert(String::from("l"), json!(2.1));
        fields.insert(String::from("l"), json!("2.1"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 4);
    }

    #[test]
    fn test_filter_by_value_timestamp() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        // fields.insert(String::from("m"), json!(1699847022));
        // fields.insert(String::from("m"), json!("1699847022"));
        // fields.insert(String::from("n"), json!(1699847022000 as i64));
        // fields.insert(String::from("n"), json!("1699847022000"));
        // fields.insert(String::from("o"), json!(1699847022000000 as i64));
        // fields.insert(String::from("o"), json!("1699847022000000"));
        // fields.insert(String::from("p"), json!(1699847022000000000 as i64));
        fields.insert(String::from("p"), json!("1699847022000000000"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_by_value_binary() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        // fields.insert(String::from("q"), json!("a111"));
        // fields.insert(String::from("r"), json!("a111"));
        fields.insert(String::from("s"), json!("a111"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_by_value_utf8() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        // fields.insert(String::from("t"), json!("a111"));
        fields.insert(String::from("u"), json!("a111"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_by_regex() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        fields.insert(String::from("u"), json!(r"^b\d{3}$"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 2);
    }

    #[test]
    fn test_filter_by_multiple() {
        let record_batch = init_record_batch();

        let mut fields = LinkedHashMap::new();
        fields.insert(String::from("b"), json!("1"));
        fields.insert(String::from("u"), json!(r"^b\d{3}$"));
        let filter = MatchRecordFilter::new(fields);

        let new_batch = filter.filter_records(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 1);
    }

    fn init_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::Int16, false),
            Field::new("d", DataType::Int32, false),
            Field::new("e", DataType::Int64, false),
            Field::new("f", DataType::UInt8, false),
            Field::new("g", DataType::UInt16, false),
            Field::new("h", DataType::UInt32, false),
            Field::new("i", DataType::UInt64, false),
            Field::new("j", DataType::Float16, false),
            Field::new("k", DataType::Float32, false),
            Field::new("l", DataType::Float64, false),
            Field::new(
                "m",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            ),
            Field::new(
                "n",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "o",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "p",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("q", DataType::Binary, false),
            Field::new("r", DataType::FixedSizeBinary(4), false),
            Field::new("s", DataType::LargeBinary, false),
            Field::new("t", DataType::Utf8, false),
            Field::new("u", DataType::LargeUtf8, false),
        ]);

        let a = BooleanArray::from(vec![true, true, true, false, false, false, true, false]);
        let b = Int8Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let c = Int16Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let d = Int32Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let e = Int64Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let f = UInt8Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let g = UInt16Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let h = UInt32Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        let i = UInt64Array::from(vec![1, 1, 1, 2, 2, 2, 1, 2]);
        // half::f16::from_f64(1.1) 会丢失精度,所以这一列使用 1.0 与 2.0
        let j = Float16Array::from(vec![
            half::f16::from_f64(1.0),
            half::f16::from_f64(1.0),
            half::f16::from_f64(1.0),
            half::f16::from_f64(2.0),
            half::f16::from_f64(2.0),
            half::f16::from_f64(2.0),
            half::f16::from_f64(1.0),
            half::f16::from_f64(2.0),
        ]);
        let k = Float32Array::from(vec![1.1, 1.1, 1.1, 2.1, 2.1, 2.1, 1.1, 2.1]);
        let l = Float64Array::from(vec![1.1, 1.1, 1.1, 2.1, 2.1, 2.1, 1.1, 2.1]);
        let m = TimestampSecondArray::from(vec![
            1699847021, 1699847022, 1699847023, 1699847024, 1699847025, 1699847026, 1699847027,
            1699847028,
        ]);
        let n = TimestampMillisecondArray::from(vec![
            1699847021000,
            1699847022000,
            1699847023000,
            1699847024000,
            1699847025000,
            1699847026000,
            1699847027000,
            1699847028000,
        ]);
        let o = TimestampMicrosecondArray::from(vec![
            1699847021000000,
            1699847022000000,
            1699847023000000,
            1699847024000000,
            1699847025000000,
            1699847026000000,
            1699847027000000,
            1699847028000000,
        ]);
        let p = TimestampNanosecondArray::from(vec![
            1699847021000000000,
            1699847022000000000,
            1699847023000000000,
            1699847024000000000,
            1699847025000000000,
            1699847026000000000,
            1699847027000000000,
            1699847028000000000,
        ]);
        let q = BinaryArray::from(vec![
            String::from("a111").as_bytes(),
            String::from("a222").as_bytes(),
            String::from("b111").as_bytes(),
            String::from("b222").as_bytes(),
            String::from("c111").as_bytes(),
            String::from("b222").as_bytes(),
            String::from("d111").as_bytes(),
            String::from("d222").as_bytes(),
        ]);
        let r = FixedSizeBinaryArray::from(vec![
            String::from("a111").as_bytes(),
            String::from("a222").as_bytes(),
            String::from("b111").as_bytes(),
            String::from("b222").as_bytes(),
            String::from("c111").as_bytes(),
            String::from("b222").as_bytes(),
            String::from("d111").as_bytes(),
            String::from("d222").as_bytes(),
        ]);
        let s = LargeBinaryArray::from(vec![
            String::from("a111").as_bytes(),
            String::from("a222").as_bytes(),
            String::from("b111").as_bytes(),
            String::from("b222").as_bytes(),
            String::from("c111").as_bytes(),
            String::from("b222").as_bytes(),
            String::from("d111").as_bytes(),
            String::from("d222").as_bytes(),
        ]);
        let t = StringArray::from(vec![
            "a111", "a222", "b111", "b222", "c111", "c222", "d111", "d222",
        ]);
        let u = LargeStringArray::from(vec![
            "a111", "a222", "b111", "b222", "c111", "c222", "d111", "d222",
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(a),
                Arc::new(b),
                Arc::new(c),
                Arc::new(d),
                Arc::new(e),
                Arc::new(f),
                Arc::new(g),
                Arc::new(h),
                Arc::new(i),
                Arc::new(j),
                Arc::new(k),
                Arc::new(l),
                Arc::new(m),
                Arc::new(n),
                Arc::new(o),
                Arc::new(p),
                Arc::new(q),
                Arc::new(r),
                Arc::new(s),
                Arc::new(t),
                Arc::new(u),
            ],
        )
        .unwrap()
    }
}
