use std::sync::Arc;
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use arrow::datatypes::Field;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::plugins::transform::Select;

use super::Parse;
use thiserror::Error;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Regex {
    #[serde(with = "serde_regex")]
    regex: regex::Regex,
    #[serde(default)]
    select: Option<Select>,
    #[serde(default)]
    keep: bool,
}

#[derive(Debug, Error)]
#[error("Regex error str: {item:?} info: {info:?} regex: {regex:?}")]
pub struct RegexError {
    info: String,
    item: String,
    regex: String,
}

impl Parse for Regex {
    fn num_rows_will_be_changed(&self) -> bool {
        false
    }

    fn num_columns_will_be_changed(&self) -> bool {
        !self.keep && self.regex.capture_names().filter_map(|v| v).count() == 1
    }

    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(arrow::record_batch::RecordBatch, Option<Vec<usize>>), super::ParseError> {
        let string_array = match array.data_type() {
            DataType::Utf8 => {
                let string_array = array.
                    as_any().
                    downcast_ref::<StringArray>().
                    ok_or_else(|| {
                        RegexError {
                            info: "Failed to downcast ArrayRef to StringArray".to_owned(),
                            item: "".to_string(),
                            regex: self.regex.to_string(),
                        }
                    })?;
                string_array
            }
            _ => {
                 Err(RegexError {
                    info: format!(
                        "Unsupported data type {:?} for parsing as regex",
                        array.data_type(),
                    ),
                    item: "".to_string(),
                    regex: self.regex.to_string(),
                })?
            }
        };
        let num_rows = string_array.len();

        let fields:Vec<_> = self.regex.capture_names().flatten().map(|s|{
           Field::new(s,DataType::Utf8,true)
        }).collect();

        let mut schema=Schema::new(fields);
        if let Some(select) = self.select.as_ref() {
            schema = select.schema(&schema);
        }

        let mut arrays = Vec::new();
        let fields = schema.fields.clone();
        // dbg!(&schema);
        // dbg!(&fields);
        if self.keep{
            arrays.push((field.name(),array.clone()));
        }

        let mut captures_groups = Vec::with_capacity(num_rows);
        for row_index in 0..num_rows {
           if string_array.is_null(row_index){
                captures_groups.push(None)
           }else {
               let value = string_array.value(row_index);
               let caps = self.regex.captures(&value).ok_or_else(||{
                   RegexError {
                       info: format!(
                           "Regex pattern {:?} has no capture groups",
                           self.regex
                       ),
                       item: value.to_string(),
                       regex: self.regex.to_string(),
                   }
               })?;
               captures_groups.push(Some(caps));
           }
        }
        for f in &fields{
            let name = f.name();
            if name ==field.name(){
                continue
            }
            let dt = f.data_type();
            macro_rules! get_values {
                ($ty:ty) => {
                    captures_groups.iter().map(|caps|{
                        if let Some(caps) = caps{
                            caps.name(name).map(|v|{
                                v.as_str().parse::<$ty>().map_err(|err| RegexError{
                                    info: err.to_string(),
                                    item: v.as_str().to_string(),
                                    regex: self.regex.to_string(),
                                })
                            }).transpose()
                        }else {
                            Ok(None)
                        }
                    }).try_collect::<_,Vec<_>,_>()
                }
            }
            match dt{
                DataType::UInt8 =>{
                    let values = get_values!(u8)?;
                    let array: ArrayRef = Arc::new(UInt8Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::UInt16 =>{
                    let values = get_values!(u16)?;
                    let array: ArrayRef = Arc::new(UInt16Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::UInt32 =>{
                    let values = get_values!(u32)?;
                    let array: ArrayRef = Arc::new(UInt32Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::UInt64 =>{
                    let values = get_values!(u64)?;
                    let array: ArrayRef = Arc::new(UInt64Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Int8 =>{
                    let values = get_values!(i8)?;
                    let array: ArrayRef = Arc::new(Int8Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Int16 =>{
                    let values = get_values!(i16)?;
                    let array: ArrayRef = Arc::new(Int16Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Int32 =>{
                    let values = get_values!(i32)?;
                    let array: ArrayRef = Arc::new(Int32Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Int64 =>{
                    let values = get_values!(i64)?;
                    let array: ArrayRef = Arc::new(Int64Array::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let values = captures_groups.iter().map(|caps|{
                        if let Some(caps) = caps{
                            caps.name(name).map(|v|{
                                v.as_str()
                            })
                        }else {
                            None
                        }
                    }).collect_vec();
                    let array: ArrayRef = Arc::new(StringArray::from_iter(values));
                    arrays.push((f.name(), array));
                }
                DataType::Binary | DataType::LargeBinary => {
                    let values = captures_groups.iter().map(|caps|{
                        if let Some(caps) = caps{
                            caps.name(name).map(|v|{
                                v.as_str().as_bytes()
                            })
                        }else {
                            None
                        }
                    }).collect_vec();
                    let array: ArrayRef = Arc::new(BinaryArray::from_iter(values));
                    arrays.push((f.name(), array));
                }
                _ => todo!(),
            }
        }
        let records = RecordBatch::try_from_iter(arrays).unwrap();
        Ok((records,None))
    }
}

#[cfg(test)]
mod tests{
    use arrow::{array::ArrayRef, datatypes::Field};
    use super::*;
    #[test]
    fn regex_test(){
        let re = Regex{
            regex: regex::Regex::new(r"'(?P<title>[^']+)'\s+\((?P<year>\d{4})\)").unwrap(),
            select: Some(serde_json::from_str(&r#"["title::nchar(100)", "year::i32"]"#).unwrap()),
            keep: false,
        };
        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            r#"Not my favorite movie: 'Citizen Kane' (1941)."#,
            r#"Not my favorite movie: 'Movie1' (1942)."#,
            r#"Not my favorite movie: 'Movie2' (1943)."#,
        ]));
        let (records, _) = re.parse_array(&field, &array).unwrap();

        dbg!(&records);
        assert_eq!(records.num_columns(),2);
        assert_eq!(records.num_rows(), 3);
        let values = vec!["Citizen Kane","Movie1","Movie2"];
        let array: ArrayRef = Arc::new(StringArray::from(values));
        assert_eq!(records.column(0),&array);
        let values = vec![1941i32,1942i32,1943i32];
        let array: ArrayRef = Arc::new(Int32Array::from(values));
        assert_eq!(records.column(1),&array);
    }
}
