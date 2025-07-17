use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use super::TransformExt;

pub mod expr;
mod r#match;

/// TODO(@Yuanpai Zhang): implement map transform.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Filter(Vec<FilterImpl>);

impl Filter {
    pub fn new(filters: Vec<FilterImpl>) -> Self {
        Self(filters)
    }
}

impl TransformExt for Filter {
    fn transform_record_batch(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, super::Error> {
        let result = self.0.iter().try_fold(records.clone(), |result, filter| {
            filter.filter_records(&result)
        });
        Ok(result.unwrap().clone())
    }
}

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum RecordFilterError {
    #[error("invalid record filter")]
    InvalidRecordFilter,
}

trait RecordFilter {
    fn filter_records(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, RecordFilterError>;
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum FilterImpl {
    Expr(expr::ExprRecordFilter),
    Match(r#match::MatchRecordFilter),
}

impl RecordFilter for FilterImpl {
    fn filter_records(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, RecordFilterError> {
        match self {
            FilterImpl::Expr(expr) => expr.filter_records(records),
            FilterImpl::Match(r#match) => r#match.filter_records(records),
        }
    }
}

impl<'de> Deserialize<'de> for FilterImpl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterImplVisitor;
        impl<'de> serde::de::Visitor<'de> for FilterImplVisitor {
            type Value = FilterImpl;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or a map")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FilterImpl::Expr(expr::ExprRecordFilter::new(
                    value.to_string(),
                )))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut fields = LinkedHashMap::new();
                while let Some((key, value)) = map.next_entry::<String, JsonValue>()? {
                    fields.insert(key, value);
                }
                Ok(FilterImpl::Match(r#match::MatchRecordFilter::new(fields)))
            }
        }
        deserializer.deserialize_any(FilterImplVisitor)
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterVisitor;
        impl<'de> serde::de::Visitor<'de> for FilterVisitor {
            type Value = Filter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("array or string or map")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(Filter(vec![FilterImpl::Expr(expr::ExprRecordFilter::new(
                    value.to_string(),
                ))]))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut fields = LinkedHashMap::new();
                while let Some((key, value)) = map.next_entry::<String, JsonValue>()? {
                    fields.insert(key, value);
                }
                Ok(Filter(vec![FilterImpl::Match(
                    r#match::MatchRecordFilter::new(fields),
                )]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut v = Vec::new();
                while let Some(value) = seq.next_element::<FilterImpl>()? {
                    v.push(value)
                }
                Ok(Filter(v))
            }
        }
        deserializer.deserialize_any(FilterVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_impl() {
        let filter = r#""a > b && c != 0""#;
        let filter: FilterImpl = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"{ "d": 5, "e": 6 }"#;
        let filter: FilterImpl = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"{ "d": 5, "f": "^bc" }"#;
        let filter: FilterImpl = serde_json::from_str(filter).unwrap();
        dbg!(filter);
    }
    #[test]
    fn test_single_filter() {
        let filter = r#"["a > b && c != 0"]"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"["a > b && c != 0", "f > g && h != 0", { "d": 5, "e": "/abc/"}]"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#""a > b && c != 0""#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
        let filter = r#"{ "b": "^b\\d{3}$" }"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();
        dbg!(filter);
    }

    use arrow::array::{
        BinaryArray, BooleanArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
        RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_filter_by_match() {
        let record_batch = init_record_batch();

        let filter = r#"{ "b": "2", "u": "^b\\d{3}$" }"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();

        let new_batch = filter.transform_record_batch(&record_batch).unwrap();
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

    #[test]
    fn test_filter_by_expression() {
        let record_batch = init_record_batch_simple();

        let filter = r#"[ {}, "!a && b == 1 && c > 1" ]"#;
        let filter: Filter = serde_json::from_str(filter).unwrap();

        let new_batch = filter.transform_record_batch(&record_batch).unwrap();
        dbg!(&new_batch);

        assert_eq!(new_batch.num_rows(), 1);
    }

    fn init_record_batch_simple() -> RecordBatch {
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
        ]);

        let a = BooleanArray::from(vec![true, false, true, false, true, false, true, false]);
        let b = Int8Array::from(vec![1, 1, 1, 1, 2, 2, 2, 2]);
        let c = Int16Array::from(vec![2, 2, 1, 1, 2, 2, 1, 1]);
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
            ],
        )
        .unwrap()
    }
}
