use arrow::{
    array::{Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_schema::Fields;
use lazy_static::lazy_static;
use rhai::{Dynamic, EvalAltResult, LexError, ParseError, ParseErrorType, Scope, AST};
use rhai_dylib::module_resolvers::libloading::DylibModuleResolver;
use rhai_dylib::rhai::{config::hashing::set_hashing_seed, Engine};
use serde::{de::Visitor, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt;
use std::{collections::HashMap, sync::Arc};
use tracing::{instrument, warn};

use super::Parse;

lazy_static! {
    static ref ENGINE: Engine = init_engine();
}

fn init_engine() -> Engine {
    let seed_values: [u64; 4] = [2, 0, 2, 7];
    set_hashing_seed(Some(seed_values)).unwrap();

    let mut engine = Engine::new();
    let resolver = DylibModuleResolver::with_path(crate::runners::get_plugin_dir("udt"));
    engine.set_module_resolver(resolver);
    engine
}

fn check_same_type(field_type: &str, value_type: &str) -> bool {
    if field_type != value_type {
        warn!("type mismatch, field type: {field_type}, but value type: {value_type}",);
        return false;
    }
    true
}

#[derive(Debug, Clone)]
enum ArrowDataField {
    I64(Field, Vec<Option<i64>>),
    F64(Field, Vec<Option<f64>>),
    Bool(Field, Vec<Option<bool>>),
    String(Field, Vec<Option<String>>),
    Blob(Field, Vec<Option<Vec<u8>>>),
}

impl ArrowDataField {
    fn from_dynamic(name: &str, v: Dynamic, data_size: usize) -> Option<Self> {
        match v.type_name() {
            "i64" => {
                let values = init_data_array(v.as_int().unwrap(), data_size);
                let field = init_data_field(name, DataType::Int64, "i64");
                Some(ArrowDataField::I64(field, values))
            }
            "f64" => {
                let values = init_data_array(v.as_float().unwrap(), data_size);
                let field = init_data_field(name, DataType::Float64, "f64");
                Some(ArrowDataField::F64(field, values))
            }
            "bool" => {
                let values = init_data_array(v.as_bool().unwrap(), data_size);
                let field = init_data_field(name, DataType::Boolean, "bool");
                Some(ArrowDataField::Bool(field, values))
            }
            "string" => {
                let values = init_data_array(v.into_string().unwrap().to_string(), data_size);
                let field = init_data_field(name, DataType::Utf8, "string");
                Some(ArrowDataField::String(field, values))
            }
            "blob" => {
                let values = init_data_array(v.into_blob().unwrap(), data_size);
                let field = init_data_field(name, DataType::Binary, "blob");
                Some(ArrowDataField::Blob(field, values))
            }
            _ => {
                warn!("udt unknown type: {:?}", v.type_name());
                None
            }
        }
    }

    fn add_value(&mut self, value: Dynamic) -> bool {
        match self {
            ArrowDataField::I64(field, array) => {
                if field
                    .metadata()
                    .get("from_cast")
                    .is_none_or(|name| check_same_type(name, value.type_name()))
                {
                    array.push(Some(value.as_int().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::F64(field, array) => {
                if field
                    .metadata()
                    .get("from_cast")
                    .is_none_or(|name| check_same_type(name, value.type_name()))
                {
                    array.push(Some(value.as_float().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::Bool(field, array) => {
                if field
                    .metadata()
                    .get("from_cast")
                    .is_none_or(|name| check_same_type(name, value.type_name()))
                {
                    array.push(Some(value.as_bool().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::String(field, array) => {
                if field
                    .metadata()
                    .get("from_cast")
                    .is_none_or(|name| check_same_type(name, value.type_name()))
                {
                    array.push(Some(value.into_string().unwrap().to_string()));
                } else {
                    return false;
                }
            }
            ArrowDataField::Blob(field, array) => {
                if field
                    .metadata()
                    .get("from_cast")
                    .is_none_or(|name| check_same_type(name, value.type_name()))
                {
                    array.push(Some(value.into_blob().unwrap()));
                } else {
                    return false;
                }
            }
        };
        true
    }

    fn pad_none(&mut self, size: usize) {
        match self {
            ArrowDataField::I64(_, array) => {
                if array.len() < size {
                    array.push(None);
                }
            }
            ArrowDataField::F64(_, array) => {
                if array.len() < size {
                    array.push(None);
                }
            }
            ArrowDataField::Bool(_, array) => {
                if array.len() < size {
                    array.push(None);
                }
            }
            ArrowDataField::String(_, array) => {
                if array.len() < size {
                    array.push(None);
                }
            }
            ArrowDataField::Blob(_, array) => {
                if array.len() < size {
                    array.push(None);
                }
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            ArrowDataField::I64(_, array) => array.len(),
            ArrowDataField::F64(_, array) => array.len(),
            ArrowDataField::Bool(_, array) => array.len(),
            ArrowDataField::String(_, array) => array.len(),
            ArrowDataField::Blob(_, array) => array.len(),
        }
    }
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct UdtAST {
    #[serde(skip)]
    pub(crate) script: String,
    #[serde(skip)]
    pub(crate) ast: Option<AST>,

    #[serde(skip)]
    pub(crate) error: Option<ParseError>,
}

impl std::cmp::PartialEq for UdtAST {
    fn eq(&self, other: &Self) -> bool {
        self.script == other.script
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq)]
pub struct Udt {
    pub(crate) udt: UdtAST,
}

impl<'de> Deserialize<'de> for UdtAST {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringVisitor;

        impl Visitor<'_> for StringVisitor {
            type Value = String;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string to parse into UdtAST")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(v.to_string())
            }
        }

        let script = deserializer.deserialize_str(StringVisitor)?;
        let udt_ast = match ENGINE.compile(&script) {
            Ok(ast) => UdtAST {
                script,
                ast: Some(ast),
                error: None,
            },
            Err(e) => UdtAST {
                script,
                ast: None,
                error: Some(e),
            },
        };

        Ok(udt_ast)
    }
}

fn init_data_array<T>(value: T, init_capacity: usize) -> Vec<Option<T>> {
    let mut values: Vec<Option<T>> = vec![];
    for _ in 0..init_capacity {
        values.push(None);
    }
    values.push(Some(value));
    values
}

fn init_data_field(name: &str, data_type: DataType, cast_from: &str) -> Field {
    let field = Field::new(name, data_type, true);
    field.with_metadata(HashMap::from([(
        "from_cast".to_string(),
        cast_from.to_string(),
    )]))
}

fn json_value_to_dynamic(value: Value) -> Dynamic {
    match value {
        Value::Null => Dynamic::UNIT,
        Value::Bool(b) => Dynamic::from(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                Dynamic::from(n.to_string())
            }
        }
        Value::String(s) => Dynamic::from(s),
        Value::Array(arr) => Dynamic::from(
            arr.into_iter()
                .map(json_value_to_dynamic)
                .collect::<Vec<_>>(),
        ),
        Value::Object(map) => Dynamic::from(
            map.into_iter()
                .map(|(k, v)| (k.into(), json_value_to_dynamic(v)))
                .collect::<rhai::Map>(),
        ),
    }
}
impl Udt {
    #[instrument(skip_all)]
    fn parse_data(&self, item_raw_data: &str) -> Result<Vec<Dynamic>, super::ParseError> {
        if self.udt.ast.is_none() {
            let parse_error = self.udt.error.as_ref().unwrap();
            return Err(super::ParseError::UdtError(EvalAltResult::ErrorParsing(
                parse_error.err_type().clone(),
                parse_error.position(),
            )));
        }
        let mut scope = Scope::new();
        scope.push("raw", item_raw_data.to_string());
        let json: Value = serde_json::from_str(item_raw_data).map_err(|e| {
            tracing::error!(raw = item_raw_data, "Failed to parse JSON: {e}");
            super::ParseError::UdtError(EvalAltResult::ErrorParsing(
                ParseErrorType::BadInput(LexError::UnexpectedInput(item_raw_data.to_string())),
                rhai::Position::START,
            ))
        })?;
        scope.push("data", json_value_to_dynamic(json));
        // 约定返回的数据为
        ENGINE
            .eval_ast_with_scope::<Dynamic>(
                &mut scope,
                self.udt.ast.as_ref().expect("UdtAST should not None"),
            )
            .map(|v| {
                if v.is_array() {
                    v.into_array().expect("Failed to convert to array")
                } else {
                    vec![v]
                }
            })
            .map_err(|source| {
                tracing::error!(
                    raw = item_raw_data,
                    expr = self.udt.script.as_str(),
                    "Failed to evaluate UDT: {source}"
                );
                super::ParseError::UdtError(*source)
            })
    }
}

impl Parse for Udt {
    /// TODO: 添加解析参数，如：是否 flatten 嵌套数组（`[[{"key": "value"}]]`）
    #[instrument(skip_all)]
    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        let _ = field;
        if array.is_empty() {
            // Return empty record batch.
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }

        let array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array_raw_data = array.as_any().downcast_ref::<StringArray>().unwrap();
        let num_rows = string_array_raw_data.len();

        let mut key_index_map = HashMap::new();
        let mut arrow_fields: Vec<ArrowDataField> = Vec::with_capacity(num_rows * 10);
        let mut indices = Vec::with_capacity(num_rows * 10);

        // 使用 UDT 解析数据
        for i in 0..num_rows {
            if string_array_raw_data.is_null(i) {
                continue;
            }

            let rslt = match self.parse_data(string_array_raw_data.value(i)) {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("udt parse data error: {e:?}");
                    continue;
                }
            };

            for row_value in rslt {
                // 将 row_value 转换为 其表达类型的值
                if row_value.is_unit() {
                    continue;
                }
                let mut data_available = false;
                if row_value.is_map() {
                    for (k, v) in row_value
                        .try_cast::<rhai::Map>()
                        .expect("Expected a map")
                        .into_iter()
                    {
                        let key = k.to_string();

                        // 如果已经有了，就只需要添加数据就可以
                        match key_index_map.get(&key) {
                            Some(index) => {
                                let data_field: &mut ArrowDataField = &mut arrow_fields[*index];
                                if data_field.add_value(v) {
                                    data_available = true;
                                }
                            }
                            None => {
                                let now_data_size = indices.len();
                                if let Some(data_field) =
                                    ArrowDataField::from_dynamic(&key, v, now_data_size)
                                {
                                    key_index_map.insert(key, arrow_fields.len());
                                    arrow_fields.push(data_field);
                                    data_available = true;
                                }
                            }
                        }
                    }
                } else if row_value.is_blob() | row_value.is_int()
                    || row_value.is_float()
                    || row_value.is_bool()
                    || row_value.is_string()
                {
                    if let Some(index) = key_index_map.get(field.name()) {
                        let data_field: &mut ArrowDataField = &mut arrow_fields[*index];
                        if data_field.add_value(row_value.clone()) {
                            data_available = true;
                        }
                    } else {
                        let now_data_size = indices.len();
                        if let Some(data_field) =
                            ArrowDataField::from_dynamic(field.name(), row_value, now_data_size)
                        {
                            key_index_map.insert(field.name().to_string(), arrow_fields.len());
                            arrow_fields.push(data_field);
                            data_available = true;
                        }
                    }
                } else {
                    warn!(
                        "udt parse data error: expected a map, but got {:?}",
                        row_value.type_name()
                    );
                }

                if data_available {
                    // 这一行有有效数据，则需要补齐其他列的空数据，统一长度
                    indices.push(i);
                    for data_field in arrow_fields.iter_mut() {
                        data_field.pad_none(indices.len());
                    }
                }
            }
        }

        if indices.is_empty() {
            return Ok((RecordBatch::new_empty(Arc::new(Schema::empty())), None));
        }

        let mut r_fields = Vec::with_capacity(arrow_fields.len());
        let mut r_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(arrow_fields.len());
        for data_field in arrow_fields {
            match data_field {
                ArrowDataField::I64(field, array) => {
                    r_fields.push(field);
                    r_arrays.push(Arc::new(Int64Array::from(array)));
                }
                ArrowDataField::F64(field, array) => {
                    r_fields.push(field);
                    r_arrays.push(Arc::new(Float64Array::from(array)));
                }
                ArrowDataField::Bool(field, array) => {
                    r_fields.push(field);
                    r_arrays.push(Arc::new(BooleanArray::from(array)));
                }
                ArrowDataField::String(field, array) => {
                    r_fields.push(field);
                    r_arrays.push(Arc::new(StringArray::from(array)));
                }
                ArrowDataField::Blob(field, array) => {
                    r_fields.push(field);
                    r_arrays.push(Arc::new(BinaryArray::from_iter(array)));
                }
            }
        }

        let schema = Schema::new(Fields::from(r_fields));
        let records = RecordBatch::try_new(Arc::new(schema), r_arrays)?;

        Ok((records, Some(indices)))
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;

    #[test]
    fn deserialize_udt_ast() {
        let udt1: Udt = serde_json::from_str(
            r#"{
                "udt": "if data.type_of() == \"array\" {{ data }} else {{ data }}"
            }"#,
        )
        .unwrap();

        let udt2: Udt = serde_json::from_str(
            r#"{
                "udt": "if data.type_of() == \"array\" {{ data }} else {{ data }}"
            }"#,
        )
        .unwrap();

        assert_eq!(udt1, udt2);
        let udt_err = serde_json::from_str::<Udt>(r#"{"udt": 8.8}"#);
        assert!(udt_err.is_err());
        assert!(udt_err
            .unwrap_err()
            .to_string()
            .contains("expected a string to parse into UdtAST"));
    }
    #[test]
    fn eval_with_udt_nested_array() {
        let udt: Udt = serde_json::from_str(
            r#"{
                "udt": "if data.type_of() == \"array\" {{ data }} else {{ data }}"
            }"#,
        )
        .unwrap();

        let result = udt
            .parse_data(r#"[{"a": 1, "b": "v2"},{"a": 2, "b": "v3"}]"#)
            .unwrap();
        assert_eq!(result.len(), 2);

        let result = udt.parse_data(r#"{"a": 1, "b": "v2"}"#).unwrap();
        assert_eq!(result.len(), 1);

        let result = udt.parse_data(r#""string""#).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_string());
        assert_eq!(
            result[0].as_immutable_string_ref().unwrap().deref(),
            "string"
        );
    }

    #[test]
    fn eval_with_udt_non_map() {
        // 0. empty array
        let input = r#"{"udt": "\"string\""}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::new_null(0));
        let (records, _) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 0);
        assert_eq!(records.num_rows(), 0);
        let array: ArrayRef = Arc::new(StringArray::new_null(3));
        let (records, _) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 0);
        assert_eq!(records.num_rows(), 0);

        // 1. string value
        let input = r#"{"udt": "\"string\""}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));
        let (records, indics) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indics.unwrap(), vec![0, 1, 2]);
        let col = records
            .column_by_name("a")
            .expect("Column 'a' should be the name");
        assert_eq!(col.data_type(), &DataType::Utf8);

        // 2. blob value
        let input = r#"{"udt": "blob(16)"}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));
        let (records, indics) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indics.unwrap(), vec![0, 1, 2]);
        let col = records
            .column_by_name("a")
            .expect("Column 'a' should be the name");
        assert_eq!(col.data_type(), &DataType::Binary);

        // 3. float value
        let input = r#"{"udt": "if data.n > 0 { data.n * 9.99 } else { [] }"}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));
        let (records, indics) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 2);
        assert_eq!(indics.unwrap(), vec![0, 2]);
        let col = records
            .column_by_name("a")
            .expect("Column 'a' should be the name");
        assert_eq!(col.data_type(), &DataType::Float64);

        // 4. bool value
        let input = r#"{"udt": "if data.n > 0 { true } else { false }"}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));
        let (records, indics) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indics.unwrap(), vec![0, 1, 2]);
        let col = records
            .column_by_name("a")
            .expect("Column 'a' should be the name");
        assert_eq!(col.data_type(), &DataType::Boolean);

        // unit/null
        let input = r#"{"udt": "if data.n > 0 { true } else { () }"}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));
        let (records, indics) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 2);
        assert_eq!(indics.unwrap(), vec![0, 2]);
        let col = records
            .column_by_name("a")
            .expect("Column 'a' should be the name");
        assert_eq!(col.data_type(), &DataType::Boolean);
    }

    #[test]
    fn udt_usage_with_raw() {
        // unit/null
        let input = r#"{"udt": "if data.n > 0 { raw } else { `${data.n}` }"}"#;
        let udt: Udt = serde_json::from_str(input).unwrap();
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));
        let (records, _) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 3);
        let col = records
            .column_by_name("a")
            .expect("Column 'a' should be the name");
        assert_eq!(col.data_type(), &DataType::Utf8);
    }
    #[test]
    fn eval_with_udt_a() {
        let input = format!(
            "{{\"udt\": \"{}\"}}",
            r#"
        if (data["n"] == 0) { 
            []
        } else if (data["n"] == 1) { 
            [#{"a": 1, "b": "v2"}] 
        } else { 
            [#{"a": 3}, #{"b": "v5"}]
        }"#
            .replace("\"", "\\\"")
            .replace("\n", "")
        );

        let udt: Udt = serde_json::from_str(&input).unwrap();

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));

        let (records, indics) = udt.parse_array(&field, &array).unwrap();
        assert_eq!(records.num_columns(), 2);
        assert_eq!(records.num_rows(), 3);
        assert_eq!(indics.unwrap(), vec![0, 2, 2]);
    }

    #[test]
    fn eval_with_udt_error() {
        let input = format!(
            "{{\"udt\": \"{}\"}}",
            r#"
        if (data"n"] == 0) { 
            []
        } else if (data["n"] == 1) { 
            [#{"a": 1, "b": "v2"}] 
        } else { 
            [#{"a": 3}, #{"b": "v5"}]
        }"#
            .replace("\"", "\\\"")
            .replace("\n", "")
        );

        let udt: Udt = serde_json::from_str(&input).unwrap();

        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));

        let s = array.as_any().downcast_ref::<StringArray>().unwrap();
        let result = udt.parse_data(s.value(0));
        assert!(result.is_err());

        let udt: Udt = serde_json::from_str(r#"{"udt": "xt"}"#).unwrap();
        let result = udt.parse_data(s.value(0));
        assert!(result.is_err());
        dbg!(result.unwrap_err());
    }

    /// Others util function tests.
    ///
    //  TODO: add unit test in upstream to coverage it much use cases.
    #[test]
    fn others() {
        let v = init_data_array(Dynamic::from(1i64), 2);
        assert_eq!(v.len(), 3);

        assert!(!check_same_type("i32", "i64"));
        assert!(check_same_type("i64", "i64"));

        assert!(ArrowDataField::from_dynamic("a", Dynamic::from(|| -> i64 { 42 }), 1).is_none());

        for (unit, t1, t2, v1) in [
            ("bool", DataType::Boolean, "varchar", Dynamic::from(true)),
            ("i64", DataType::Int64, "varchar", Dynamic::from(10i64)),
            ("f64", DataType::Float64, "varchar", Dynamic::from(10f64)),
            ("string", DataType::Utf8, "varchar", Dynamic::from("string")),
            ("blob", DataType::Binary, "varchar", Dynamic::from(b"abc")),
        ] {
            let f = Field::new("a", t1, true)
                .with_metadata(HashMap::from([("from_cast".to_string(), t2.to_string())]));
            let mut field = match unit {
                "bool" => ArrowDataField::Bool(f, vec![]),
                "i64" => ArrowDataField::I64(f, vec![]),
                "f64" => ArrowDataField::F64(f, vec![]),
                "string" => ArrowDataField::String(f, vec![]),
                "blob" => ArrowDataField::Blob(f, vec![]),
                _ => unreachable!(),
            };
            field.add_value(v1);
            assert_eq!(field.len(), 0);
            field.pad_none(5);
            assert_eq!(field.len(), 1); // TODO: pad_none to 5 results 1, is it expected behavior?
        }
    }
}
