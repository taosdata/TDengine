use arrow::array::BooleanArray;
use serde::{Deserialize, Serialize};

use crate::plugins::expr::BooleanExpr;

use super::{RecordFilter, RecordFilterError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExprRecordFilter {
    expr: String,
}

impl ExprRecordFilter {
    pub fn new(expr: String) -> Self {
        Self { expr }
    }
}

impl RecordFilter for ExprRecordFilter {
    fn filter_records(
        &self,
        records: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, RecordFilterError> {
        let expr = BooleanExpr::try_new(self.expr.clone());
        let filter = match expr {
            Ok(expr) => expr.eval(records).unwrap(),
            Err(_) => {
                // 没有符合的列则默认全部保留
                vec![true; records.num_rows()]
            }
        };
        let filter = BooleanArray::from(filter);
        Ok(arrow::compute::filter_record_batch(records, &filter).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        RecordBatch, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_filter_by_expression() {
        let record_batch = init_record_batch();

        let filter = ExprRecordFilter::new(String::from("!a && b == 1 && c > 1"));

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
