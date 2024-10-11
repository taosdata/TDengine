use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_schema::Fields;
use lazy_static::lazy_static;
use rhai::{Dynamic, EvalAltResult, ParseError, Scope, AST};
use rhai_dylib::module_resolvers::libloading::DylibModuleResolver;
use rhai_dylib::rhai::{config::hashing::set_hashing_seed, Engine};
use serde::{de::Visitor, Deserialize, Deserializer, Serialize};
use std::fmt;
use std::{collections::HashMap, sync::Arc};
use tracing::warn;

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
        warn!(
            "type mismatch, field type: {}, but value type: {}",
            field_type, value_type
        );
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
            _ => {
                warn!("udt unknown type: {:?}", v.type_name());
                None
            }
        }
    }

    fn add_value(&mut self, value: Dynamic) -> bool {
        match self {
            ArrowDataField::I64(field, array) => {
                if check_same_type(
                    field.metadata().get("from_cast").unwrap(),
                    value.type_name(),
                ) {
                    array.push(Some(value.as_int().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::F64(field, array) => {
                if check_same_type(
                    field.metadata().get("from_cast").unwrap(),
                    value.type_name(),
                ) {
                    array.push(Some(value.as_float().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::Bool(field, array) => {
                if check_same_type(
                    field.metadata().get("from_cast").unwrap(),
                    value.type_name(),
                ) {
                    array.push(Some(value.as_bool().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::String(field, array) => {
                if check_same_type(
                    field.metadata().get("from_cast").unwrap(),
                    value.type_name(),
                ) {
                    array.push(Some(value.into_string().unwrap().to_string()));
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
        }
    }
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct UdtAST {
    #[serde(skip)]
    pub(crate) ast: Option<AST>,

    #[serde(skip)]
    pub(crate) error: Option<ParseError>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Udt {
    pub(crate) udt: UdtAST,
}

impl<'de> Deserialize<'de> for UdtAST {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringVisitor;

        impl<'de> Visitor<'de> for StringVisitor {
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

        let s = deserializer.deserialize_str(StringVisitor)?;
        let udt_ast = match ENGINE.compile(s) {
            Ok(ast) => UdtAST {
                ast: Some(ast),
                error: None,
            },
            Err(e) => UdtAST {
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
impl Udt {
    fn parse_data(&self, item_raw_data: &str) -> Result<Vec<Dynamic>, super::ParseError> {
        if self.udt.ast.is_none() {
            let parse_error = self.udt.error.as_ref().unwrap();
            return Err(super::ParseError::UdtError(EvalAltResult::ErrorParsing(
                parse_error.err_type().clone(),
                parse_error.position(),
            )));
        }

        let _map = ENGINE
            .parse_json(item_raw_data, true)
            .map_err(|rhai_error| {
                tracing::error!("json parse error, the raw string: {}", item_raw_data);
                super::ParseError::UdtError(*rhai_error)
            })?;

        let mut scope = Scope::new();
        scope.push("data", _map);

        // 约定返回的数据为
        ENGINE
            .eval_ast_with_scope::<rhai::Array>(&mut scope, self.udt.ast.as_ref().unwrap())
            .map_err(|source| super::ParseError::UdtError(*source))
    }
}

impl Parse for Udt {
    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        let _ = field;
        if array.len() == 0 {
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

            let rslt = self.parse_data(string_array_raw_data.value(i))?;

            for row_value in rslt {
                // 将 row_value 转换为 其表达类型的值
                let row_value_as_map = row_value.try_cast::<rhai::Map>();
                if row_value_as_map.is_none() {
                    continue;
                }

                let mut data_available = false;
                for (k, v) in row_value_as_map.unwrap().into_iter() {
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
            }
        }

        let schema = Schema::new(Fields::from(r_fields));
        let records = RecordBatch::try_new(Arc::new(schema), r_arrays)?;

        Ok((records, Some(indices)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_with_udt_simple() {
        let udt: Udt = serde_json::from_str(
            r#"{
                "udt": "[data]"
            }"#,
        )
        .unwrap();

        let result = udt.parse_data(r#"{"a": 1, "b": "v2"}"#);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.len(), 1);
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

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"n": 1}"#,
            r#"{"n": 0}"#,
            r#"{"n": 2}"#,
        ]));

        let result = udt.parse_array(&field, &array);
        assert!(result.is_err());
    }
}
