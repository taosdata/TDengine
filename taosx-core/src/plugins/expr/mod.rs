use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, NullArray};
use rhai::{Dynamic, Engine, EvalAltResult, Scope};

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod functions;

#[derive(Debug, Clone)]
pub struct BooleanExpr(Expr);

impl BooleanExpr {
    pub fn eval(&self, records: &RecordBatch) -> Result<Vec<bool>, EvalError> {
        let values = self.0.eval_as(records, DataType::Boolean)?;

        Ok(values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .values()
            .iter()
            .collect())
    }

    pub fn filter(&self, records: &RecordBatch) -> Result<RecordBatch, EvalError> {
        let values = self.0.eval_as(records, DataType::Boolean)?;
        let predicate = values.as_any().downcast_ref::<BooleanArray>().unwrap();
        Ok(arrow::compute::filter_record_batch(records, predicate)?)
    }

    pub fn try_new(expr: String) -> Result<Self, EvalAltResult> {
        Expr::try_new(expr, true).map(BooleanExpr)
    }
}

impl<'de> serde::de::Deserialize<'de> for BooleanExpr {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let inner = String::deserialize(deserializer)?;

        Self::try_new(inner).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for BooleanExpr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.expr.serialize(serializer)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Expr {
    pub expr: String,
    pub null_if_error: bool,
    #[serde(skip)]
    ast: rhai::AST,
    #[serde(skip)]
    engine: Arc<Engine>,
}

impl<'de> serde::de::Deserialize<'de> for Expr {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct ExprInner {
            expr: String,
            null_if_error: Option<bool>,
        }
        let inner = ExprInner::deserialize(deserializer)?;

        Expr::try_new(inner.expr, inner.null_if_error.unwrap_or(true))
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Error, Debug)]
pub enum EvalError {
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Eval `{0}` error: {1}")]
    RhaiError(String, Box<rhai::EvalAltResult>),
    #[error("Scope error: currently not supported type: {0}")]
    ScopeTypeNotSupported(DataType),
    #[error("Eval error: value type not supported: {0}")]
    ValueTypeNotSupported(DataType),
    #[error("Eval error: value type not match: expect {0} but got {1}")]
    ValueTypeNotMatch(DataType, &'static str),
    #[error("invalid result")]
    InvalidResult,
}

impl Expr {
    pub fn try_new(expr: impl Into<String>, null_if_error: bool) -> Result<Self, EvalAltResult> {
        let expr = expr.into();
        if expr.is_empty() {
            return Err(rhai::EvalAltResult::ErrorParsing(
                rhai::ParseErrorType::ExprExpected("non empty".to_string()),
                rhai::Position::START,
            ));
        }
        // let mut engine = Engine::new();
        let mut engine = Engine::new();
        engine.register_fn("append", functions::append);
        engine.register_fn("replace", functions::replace);
        engine.register_fn("replace", functions::replacen);
        engine.register_fn("truncate", functions::truncate);
        engine.register_fn("add_or_set", functions::add_or_set);
        let engine = Arc::new(engine);
        let ast = engine.compile(&expr)?;
        // let ast = engine.compile_expression(&expr)?;
        Ok(Self {
            expr,
            null_if_error,
            ast,
            engine,
        })
    }

    fn eval_inner(&self, records: &RecordBatch) -> Result<Vec<Dynamic>, EvalError> {
        let (rows, cols) = (records.num_rows(), records.num_columns());
        let schema = records.schema();
        let columns = records.columns();
        if rows == 0 {
            return Ok(vec![]);
        }

        if self.expr.is_empty() {
            return Ok(vec![Dynamic::UNIT; rows]);
        }

        // fn parse_variables_from_expr(expr: &rhai::AST) -> Vec<String> {
        //     let mut variables = vec![];
        //     expr.walk();
        //     variables
        // }

        let mut values = Vec::with_capacity(rows);
        for rix in 0..rows {
            let mut scope = Scope::new();
            for cix in 0..cols {
                let field = schema.field(cix);
                let name = field.name();
                let column = &columns[cix];

                if column.is_null(rix) {
                    scope.set_or_push(name, Dynamic::UNIT);
                    continue;
                }

                macro_rules! set_scope {
                    ($t:ident) => {
                        paste::paste! {
                            let value = column
                                .as_any()
                                .downcast_ref::<arrow::array::[<$t Array>]>()
                                .unwrap()
                                .value(rix);
                            scope.set_or_push(name, value);
                        }
                    };
                    (Float16, $to:ty) => {
                        paste::paste! {
                            let value = column
                                .as_any()
                                .downcast_ref::<arrow::array::Float16Array>()
                                .unwrap()
                                .value(rix);
                            scope.set_or_push(name, value.to_f64());
                        }
                    };
                    ($t:ident, $to:ty) => {
                        paste::paste! {
                            let value = column
                                .as_any()
                                .downcast_ref::<arrow::array::[<$t Array>]>()
                                .unwrap()
                                .value(rix);
                            scope.set_or_push(name, value as $to);
                        }
                    };
                }

                match field.data_type() {
                    DataType::Boolean => {
                        set_scope!(Boolean);
                    }
                    DataType::Int8 => {
                        set_scope!(Int8, i64);
                    }
                    DataType::Int16 => {
                        set_scope!(Int16, i64);
                    }
                    DataType::Int32 => {
                        set_scope!(Int32, i64);
                    }
                    DataType::Int64 => {
                        set_scope!(Int64, i64);
                    }
                    DataType::UInt8 => {
                        set_scope!(UInt8, i64);
                    }
                    DataType::UInt16 => {
                        set_scope!(UInt16, i64);
                    }
                    DataType::UInt32 => {
                        set_scope!(UInt32, i64);
                    }
                    DataType::UInt64 => {
                        set_scope!(UInt64, u64);
                    }
                    DataType::Float16 => {
                        set_scope!(Float16, f64);
                    }
                    DataType::Float32 => {
                        set_scope!(Float32, f64);
                    }
                    DataType::Float64 => {
                        set_scope!(Float64, f64);
                    }
                    DataType::Binary => {
                        let value = column
                            .as_any()
                            .downcast_ref::<arrow::array::BinaryArray>()
                            .unwrap()
                            .value(rix);
                        scope.set_or_push(name, String::from_utf8_lossy(value).to_string());
                    }
                    DataType::Utf8 => {
                        let value = column
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .unwrap()
                            .value(rix);
                        scope.set_value(name, value.to_string());
                    }
                    dt => {
                        let _ = dt; // TODO: support more types

                        // return Err(EvalError::ScopeTypeNotSupported(dt.clone()));
                    }
                }
            }
            let res: rhai::Dynamic = match self.engine.eval_ast_with_scope(&mut scope, &self.ast) {
                Ok(v) => v,
                Err(e) => {
                    if self.null_if_error {
                        rhai::Dynamic::UNIT
                    } else {
                        return Err(EvalError::RhaiError(self.expr.clone(), e));
                    }
                }
            };
            values.push(res);
        }
        Ok(values)
    }
    pub fn eval_as(&self, records: &RecordBatch, r#as: DataType) -> Result<ArrayRef, EvalError> {
        let rows = records.num_rows();
        if rows == 0 {
            return Ok(arrow::array::new_empty_array(&r#as));
        }
        let values = self.eval_inner(records)?;
        let array = array_from_rhai_dynamics(values);
        if array.is_none() {
            return Ok(arrow::array::new_null_array(&r#as, rows));
        }
        let array = array.unwrap();
        arrow::compute::cast(&array, &r#as).map_err(Into::into)
    }

    pub fn eval(
        &self,
        records: &RecordBatch,
        _as: Option<DataType>,
    ) -> Result<ArrayRef, EvalError> {
        match _as {
            None => {
                let values = self.eval_inner(records)?;
                let result = array_from_rhai_dynamics(values);
                if self.null_if_error {
                    Ok(result.unwrap_or_else(|| Arc::new(NullArray::new(records.num_rows())) as _))
                } else {
                    result.ok_or(EvalError::InvalidResult)
                }
            }
            Some(as_type) => self.eval_as(records, as_type),
        }
    }
}

pub fn array_from_rhai_dynamics(values: Vec<Dynamic>) -> Option<ArrayRef> {
    debug_assert!(values
        .iter()
        .filter_map(|v| if v.is_unit() { None } else { Some(v.type_id()) })
        .all_equal());

    if values.is_empty() {
        return None;
    }

    let value = values.iter().find(|v| !v.is_unit())?;
    if value.is_bool() {
        let values: Vec<_> = values
            .into_iter()
            .map(|v| v.as_bool().ok())
            .collect::<Vec<_>>();
        let array = arrow::array::BooleanArray::from(values);
        Some(Arc::new(array))
    } else if value.is_int() {
        let values: Vec<_> = values
            .into_iter()
            .map(|v| v.as_int().ok())
            .collect::<Vec<_>>();
        let array = arrow::array::Int64Array::from(values);
        Some(Arc::new(array))
    } else if value.is_float() {
        let values: Vec<_> = values
            .into_iter()
            .map(|v| v.as_float().ok())
            .collect::<Vec<_>>();
        let array = arrow::array::Float64Array::from(values);
        Some(Arc::new(array))
    } else if value.is_string() {
        let values: Vec<_> = values
            .into_iter()
            .map(|v| v.into_immutable_string().ok().map(|v| v.into_owned()))
            .collect::<Vec<_>>();
        let array = arrow::array::StringArray::from(values);
        Some(Arc::new(array))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::*, datatypes::*};

    use super::*;

    #[test]
    fn test_serde() {
        let expr = Expr::try_new("a + b", true).unwrap();
        let json = serde_json::to_string(&expr).unwrap();
        assert_eq!(json, r#"{"expr":"a + b","null_if_error":true}"#);

        let expr = BooleanExpr::try_new("a < b".to_string()).unwrap();
        let json = serde_json::to_string(&expr).unwrap();
        assert_eq!(json, r#""a < b""#);
    }

    #[test]
    fn test_bool() {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        let expr = BooleanExpr::try_new("id > 3".to_string()).unwrap();

        let values = expr.eval(&batch).unwrap();
        assert_eq!(values, [false, false, false, true, true]);
    }

    #[test]
    fn test_bool_with_empty_expr() {
        let expr = BooleanExpr::try_new("".to_string());
        assert!(expr.is_err());
        assert_eq!(
            expr.unwrap_err().to_string(),
            "Syntax error: Expecting non empty expression (line 1, position 0)"
        );
    }

    #[test]
    fn test_bool_with_nulls() {
        let id_array = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        let expr = BooleanExpr::try_new("id > 3".to_string()).unwrap();

        let values = expr.eval(&batch).unwrap();
        assert_eq!(values, [false, false, false, true, true]);
    }

    #[test]
    fn test_i8() {
        let id_array = Int8Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int8, true)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        let expr = BooleanExpr::try_new("id > 3".to_string()).unwrap();

        let values = expr.eval(&batch).unwrap();
        assert_eq!(values, [false, false, false, true, true]);
    }

    #[test]
    fn test_int_plus_float() {
        let expr = Expr::try_new("a + b", true).unwrap();
        let i8_array = Int8Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let f32_array = Float32Array::from(vec![Some(1.), None, Some(1.), Some(4.), Some(5.)]);

        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(i8_array) as ArrayRef),
            ("b", Arc::new(f32_array) as ArrayRef),
        ])
        .unwrap();

        let values = expr.eval_as(&batch, DataType::Float32).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_primitive::<Float32Type>().iter().collect_vec(),
            [Some(2.), None, None, Some(8.), Some(10.)]
        );
        let values = expr.eval_as(&batch, DataType::UInt64).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_primitive::<UInt64Type>().iter().collect_vec(),
            [Some(2u64), None, None, Some(8), Some(10)]
        );
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("2.0"), None, None, Some("8.0"), Some("10.0")]
        );
    }
    #[test]
    fn test_int_plus_int() {
        let expr = Expr::try_new("a + b", true).unwrap();
        let i8_array = Int8Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let f32_array = Int32Array::from(vec![Some(1), None, Some(1), Some(4), Some(5)]);

        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(i8_array) as ArrayRef),
            ("b", Arc::new(f32_array) as ArrayRef),
        ])
        .unwrap();

        let values = expr.eval_as(&batch, DataType::Float32).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_primitive::<Float32Type>().iter().collect_vec(),
            [Some(2.), None, None, Some(8.), Some(10.)]
        );
        let values = expr.eval_as(&batch, DataType::UInt64).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_primitive::<UInt64Type>().iter().collect_vec(),
            [Some(2u64), None, None, Some(8), Some(10)]
        );
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("2"), None, None, Some("8"), Some("10")]
        );
    }

    #[test]
    fn test_string_plus_string() {
        let expr = Expr::try_new("a + b", true).unwrap();
        let i8_array = StringArray::from(vec![Some("1"), None, None, Some("4"), Some("5")]);
        let f32_array = StringArray::from(vec![Some("1"), None, Some("1"), Some("4"), Some("5")]);

        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(i8_array) as ArrayRef),
            ("b", Arc::new(f32_array) as ArrayRef),
        ])
        .unwrap();

        let values = expr.eval_as(&batch, DataType::Float32).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_primitive::<Float32Type>().iter().collect_vec(),
            [Some(11.), None, Some(1.0), Some(44.), Some(55.)]
        );
        let values = expr.eval_as(&batch, DataType::UInt64).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_primitive::<UInt64Type>().iter().collect_vec(),
            [Some(11u64), None, Some(1), Some(44), Some(55)]
        );
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("11"), None, Some("1"), Some("44"), Some("55")]
        );
    }

    #[test]
    fn test_string_operations() {
        let expr = Expr::try_new(r#"a.starts_with("123")"#, true).unwrap();
        let a = StringArray::from(vec![Some("1234567890")]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(a) as ArrayRef)]).unwrap();

        let values = expr.eval_as(&batch, DataType::Boolean).unwrap();
        dbg!(&values);
        assert_eq!(values.as_boolean().iter().collect_vec(), [Some(true)]);
    }

    #[test]
    fn test_string_operations_ext() {
        let a = StringArray::from(vec![Some("11234567890")]);
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(a) as ArrayRef)]).unwrap();

        let expr = Expr::try_new(r#"a.append("abc")"#, true).unwrap();
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("11234567890abc")]
        );

        let expr = Expr::try_new(r#"a.replace("1","2")"#, true).unwrap();
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("22234567890")]
        );

        let expr = Expr::try_new(r#"a.replace("1","2", 1)"#, true).unwrap();
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("21234567890")]
        );

        let expr = Expr::try_new(r#"a.truncate(4)"#, true).unwrap();
        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("1123")]
        );
    }
    #[test]
    fn test_if_statement() {
        let expr = Expr::try_new(
            r#"if a.starts_with("123") {
            "foo"
        } else if (a.len() > 5) {
            "bar"
        } else {
            "baz"
        }"#,
            true,
        )
        .unwrap();
        let a = StringArray::from(vec![Some("1234567890"), None, Some("abcdefg"), Some("no")]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(a) as ArrayRef)]).unwrap();

        let values = expr.eval_as(&batch, DataType::Utf8).unwrap();
        dbg!(&values);
        assert_eq!(
            values.as_string::<i32>().iter().collect_vec(),
            [Some("foo"), None, Some("bar"), Some("baz")]
        );
    }
}
