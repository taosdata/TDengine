use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_schema::Fields;
use lazy_static::lazy_static;
use rhai::{Dynamic, Engine, ParseError, Scope, AST};
use serde::{de::Visitor, Deserialize, Deserializer, Serialize};
use std::fmt;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use thiserror::Error;
use tracing::warn;

use super::Parse;

lazy_static! {
    static ref ENGINE: Engine = Engine::new();
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
    BOOL(Field, Vec<Option<bool>>),
    STRING(Field, Vec<Option<String>>),
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
                Some(ArrowDataField::BOOL(field, values))
            }
            "string" => {
                let values = init_data_array(v.into_string().unwrap().to_string(), data_size);
                let field = init_data_field(name, DataType::Utf8, "string");
                Some(ArrowDataField::STRING(field, values))
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
            ArrowDataField::BOOL(field, array) => {
                if check_same_type(
                    field.metadata().get("from_cast").unwrap(),
                    value.type_name(),
                ) {
                    array.push(Some(value.as_bool().unwrap()));
                } else {
                    return false;
                }
            }
            ArrowDataField::STRING(field, array) => {
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
        return true;
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
            ArrowDataField::BOOL(_, array) => {
                if array.len() < size {
                    array.push(None);
                }
            }
            ArrowDataField::STRING(_, array) => {
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
    pub(crate) ast: AST,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Udt {
    pub(crate) udt: UdtAST,
}

#[derive(Debug, Error)]
#[error("udt error: {source:?}")]
pub struct UdtParserError {
    msg: String,
    source: Option<ParseError>,
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

fn eval_with_udt(item_raw_data: &str, udt: &AST) -> Result<Vec<Dynamic>, super::ParseError> {
    let _map = ENGINE
        .parse_json(item_raw_data, false)
        .map_err(|rhai_error| super::ParseError::UdtError(*rhai_error))?;

    let mut scope = Scope::new();
    scope.push("data", _map);

    // 约定返回的数据为
    ENGINE
        .eval_ast_with_scope::<rhai::Array>(&mut scope, udt)
        .map_err(|source| super::ParseError::UdtError(*source))
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
        let ast = ENGINE.compile(&s).map_err(serde::de::Error::custom)?;
        Ok(UdtAST { ast })
    }
}

impl FromStr for UdtAST {
    type Err = UdtParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        println!("parse udt from str: {}.", s);
        // dbg!(&s);

        if s.is_empty() {
            return Err(UdtParserError {
                msg: "Empty script".to_string(),
                source: None,
            });
        }

        let ast = ENGINE.compile(s).map_err(|source| UdtParserError {
            msg: "rhai compile error".to_string(),
            source: Some(source),
        })?;

        Ok(UdtAST { ast })
    }
}

impl Parse for Udt {
    // 行数可能修改
    fn num_rows_will_be_changed(&self) -> bool {
        true
    }

    // 列数可能修改
    fn num_columns_will_be_changed(&self) -> bool {
        true
    }

    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
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

            let rslt = eval_with_udt(string_array_raw_data.value(i), &self.udt.ast)?;

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
                            match ArrowDataField::from_dynamic(&key, v, now_data_size) {
                                Some(data_field) => {
                                    key_index_map.insert(key, arrow_fields.len());
                                    arrow_fields.push(data_field);
                                    data_available = true;
                                }
                                None => {}
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
                ArrowDataField::BOOL(field, array) => {
                    r_fields.push(field);
                    r_arrays.push(Arc::new(BooleanArray::from(array)));
                }
                ArrowDataField::STRING(field, array) => {
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
    fn test_eval_ast() {
        let udt: Udt = serde_json::from_str("{\"udt\": \"x += 2; x\"}").unwrap();

        // let ast = ENGINE.compile("x += 2; x").unwrap();
        let mut scope = Scope::new();
        scope.push("x", 1_i64);

        match ENGINE.eval_ast_with_scope::<i64>(&mut scope, &(udt.udt.ast)) {
            Ok(result) => {
                println!("{:?}", result);
            }
            Err(err) => {
                println!("{:?}", err);
            }
        }
    }
}
