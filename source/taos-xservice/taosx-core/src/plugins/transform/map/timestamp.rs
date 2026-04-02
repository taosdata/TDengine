//!
//! 解析表示时间戳运算的表达式， 例如： ts - 1ms
//!
use super::{ValueBuilder, ValueBuilderError};
use arrow::array::Array;
use arrow::array::TimestampMillisecondArray;
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
struct TimestampExpr {
    from_col_name: String,
    delta: i64,
}

impl<'de> serde::de::Deserialize<'de> for TimestampExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ExprInner {
            expr: String,
        }
        let inner = ExprInner::deserialize(deserializer)?;
        TimestampExpr::try_new(inner.expr).map_err(serde::de::Error::custom)
    }
}

impl TimestampExpr {
    pub fn try_new(expr: String) -> Result<Self, String> {
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            return Err("timestamp expr is empty".to_string());
        }
        let plus_sigin = trimmed.find('+');
        let minus_sigin = trimmed.find('-');
        let mut delta = 0i64;
        let is_add = match (plus_sigin, minus_sigin) {
            (None, None) => {
                // 假设此时表达式只包含列名
                return Ok(TimestampExpr {
                    from_col_name: trimmed.to_string(),
                    delta,
                });
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            _ => return Err(format!("Invalid expression: {trimmed}")),
        };

        let parts: Vec<&str> = if is_add {
            trimmed.split('+').collect::<Vec<&str>>()
        } else {
            trimmed.split("-").collect::<Vec<&str>>()
        };

        if parts.len() != 2 {
            return Err(format!(
                "Expect 2 parts after splitting by add/minus sign but got {}",
                parts.len()
            ));
        }
        let from_col_name = parts[0].trim();
        let time_expr = parts[1].trim();
        let ms = TimestampExpr::time_expr_to_ms(time_expr)?;
        if is_add {
            delta = ms;
        } else {
            delta = -ms;
        }
        Ok(TimestampExpr {
            from_col_name: from_col_name.to_string(),
            delta,
        })
    }

    /// Convert time expression to milliseconds
    /// Example:
    /// 1h -> 3600000,
    /// 1hour -> 3600000,
    /// 1s -> 1000,
    /// 1min -> 60000,
    /// 1minutes -> 60000,
    /// 1ms -> 1,
    /// 10:00:00 -> 36000000
    /// 10:00 -> 36000000
    fn time_expr_to_ms(time_expr: &str) -> Result<i64, String> {
        let parts = time_expr.split(':').collect::<Vec<&str>>();
        if parts.len() == 3 {
            let h = parts[0].parse::<i64>().map_err(|e| e.to_string())?;
            let m = parts[1].parse::<i64>().map_err(|e| e.to_string())?;
            let s = parts[2].parse::<i64>().map_err(|e| e.to_string())?;
            Ok(h * 3600000 + m * 60000 + s * 1000)
        } else if parts.len() == 2 {
            let h = parts[0].parse::<i64>().map_err(|e| e.to_string())?;
            let m = parts[1].parse::<i64>().map_err(|e| e.to_string())?;
            Ok(h * 3600000 + m * 60000)
        } else if parts.len() == 1 {
            let (num, unit) = TimestampExpr::split_num_and_unit(time_expr)?;
            match unit {
                "h" | "hour" => Ok(num * 3600000),
                "min" | "minutes" => Ok(num * 60000),
                "s" => Ok(num * 1000),
                "ms" => Ok(num),
                _ => Err(format!("Invalid time unit: {}", unit)),
            }
        } else {
            Err(format!(
                "Invalid time expression: {}, get more than 3 parts after split by ':'",
                time_expr
            ))
        }
    }

    fn split_num_and_unit(s: &str) -> Result<(i64, &str), String> {
        let mut split_index: usize = 0;
        for (i, c) in s.chars().enumerate() {
            if i == 0 && !c.is_numeric() {
                return Err(format!(
                    "Invalid time expression: {}, not starts with numeric",
                    s
                ));
            }
            if !c.is_numeric() {
                split_index = i;
                break;
            }
        }
        if split_index == 0 {
            return Err(format!(
                "Invalid time expression: {}, not ends with time unit",
                s
            ));
        }
        let num = s
            .get(0..split_index)
            .unwrap()
            .parse::<i64>()
            .map_err(|e| e.to_string())?;
        let unit = s.get(split_index..).unwrap();
        Ok((num, unit))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampExprValueBuilder(TimestampExpr);

impl ValueBuilder for TimestampExprValueBuilder {
    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        let col_name = self.0.from_col_name.as_str();
        let column = record
            .column_by_name(col_name)
            .ok_or_else(|| ValueBuilderError::Expr(format!("column {col_name} not found")))?;
        if self.0.delta == 0 {
            // 只包含列名，直接返回该列
            return Ok(column.clone());
        }
        let data = column
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                ValueBuilderError::Expr(format!(
                    "column {col_name} type not match, expect TimestampMillisecondArray"
                ))
            })?;
        let mut new_values: Vec<Option<i64>> = Vec::with_capacity(data.len());
        for v in data {
            match v {
                Some(v) => new_values.push(Some(v + self.0.delta)),
                None => new_values.push(None),
            }
        }
        let array: TimestampMillisecondArray = TimestampMillisecondArray::from(new_values);
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[cfg(test)]
mod tests {

    use arrow_schema::{DataType, TimeUnit};

    use super::*;

    #[test]
    fn test_time_expr_to_ms() {
        let time_expr_to_ms: fn(&str) -> Result<i64, String> = TimestampExpr::time_expr_to_ms;
        assert_eq!(time_expr_to_ms("1h").unwrap(), 3600000);
        assert_eq!(time_expr_to_ms("1hour").unwrap(), 3600000);
        assert_eq!(time_expr_to_ms("1s").unwrap(), 1000);
        assert_eq!(time_expr_to_ms("1min").unwrap(), 60000);
        assert_eq!(time_expr_to_ms("1minutes").unwrap(), 60000);
        assert_eq!(time_expr_to_ms("1ms").unwrap(), 1);
        assert_eq!(time_expr_to_ms("10:00:00").unwrap(), 36000000);
        assert_eq!(time_expr_to_ms("10:00").unwrap(), 36000000);
    }

    #[test]
    fn test_time_expr_parser() {
        let expr1 = TimestampExpr::try_new("ts - 1h".to_string()).unwrap();
        assert_eq!(expr1.from_col_name, "ts");
        assert_eq!(expr1.delta, -3600000);
        let expr2 = TimestampExpr::try_new("ts_col+1:00".to_string()).unwrap();
        assert_eq!(expr2.from_col_name, "ts_col");
        assert_eq!(expr2.delta, 3600000);
    }

    #[test]
    fn test_timestamp_expr_value_builder() {
        let builder: TimestampExprValueBuilder =
            serde_json::from_str(r#"{ "expr": "ts + 1h"}"#).unwrap();

        let batch = RecordBatch::try_from_iter([(
            "ts",
            Arc::new(TimestampMillisecondArray::from_value(1700000000000, 3)) as ArrayRef,
        )])
        .unwrap();

        let (field, value) = builder.build_field("ts_transform", &batch, None).unwrap();

        assert_eq!(field.name(), "ts_transform");
        assert_eq!(
            *field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(value.len(), 3);
        let arr = value
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), 1700003600000);
        assert_eq!(arr.value(1), 1700003600000);
        assert_eq!(arr.value(2), 1700003600000);
    }
}
