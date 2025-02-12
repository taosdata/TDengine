// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    schema::Schema,
    types::Value,
    util::{zig_i32, zig_i64},
};
use std::convert::TryInto;

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) {
    encode_ref(value, schema, buffer)
}

fn encode_bytes<B: AsRef<[u8]> + ?Sized>(s: &B, buffer: &mut Vec<u8>) {
    let bytes = s.as_ref();
    encode(&Value::Long(bytes.len() as i64), &Schema::Long, buffer);
    buffer.extend_from_slice(bytes);
}

fn encode_long(i: i64, buffer: &mut Vec<u8>) {
    zig_i64(i, buffer)
}

fn encode_int(i: i32, buffer: &mut Vec<u8>) {
    zig_i32(i, buffer)
}

/// Encode a `Value` into avro format.
///
/// **NOTE** This will not perform schema validation. The value is assumed to
/// be valid with regards to the schema. Schema are needed only to guide the
/// encoding for complex type values.
pub fn encode_ref(value: &Value, schema: &Schema, buffer: &mut Vec<u8>) {
    match value {
        Value::Null => (),
        Value::Boolean(b) => buffer.push(if *b { 1u8 } else { 0u8 }),
        // Pattern | Pattern here to signify that these _must_ have the same encoding.
        Value::Int(i) | Value::Date(i) | Value::TimeMillis(i) => encode_int(*i, buffer),
        Value::Long(i)
        | Value::TimestampMillis(i)
        | Value::TimestampMicros(i)
        | Value::TimeMicros(i) => encode_long(*i, buffer),
        Value::Float(x) => buffer.extend_from_slice(&x.to_le_bytes()),
        Value::Double(x) => buffer.extend_from_slice(&x.to_le_bytes()),
        Value::Decimal(decimal) => match schema {
            Schema::Decimal { inner, .. } => match *inner.clone() {
                Schema::Fixed { size, .. } => {
                    let bytes = decimal.to_sign_extended_bytes_with_len(size).unwrap();
                    let num_bytes = bytes.len();
                    if num_bytes != size {
                        panic!(
                            "signed decimal bytes length {} not equal to fixed schema size {}",
                            num_bytes, size
                        );
                    }
                    encode(&Value::Fixed(size, bytes), inner, buffer)
                }
                Schema::Bytes => encode(&Value::Bytes(decimal.try_into().unwrap()), inner, buffer),
                _ => panic!("invalid inner type for decimal: {:?}", inner),
            },
            _ => panic!("invalid type for decimal: {:?}", schema),
        },
        &Value::Duration(duration) => {
            let slice: [u8; 12] = duration.into();
            buffer.extend_from_slice(&slice);
        }
        Value::Uuid(uuid) => encode_bytes(&uuid.to_string(), buffer),
        Value::Bytes(bytes) => match *schema {
            Schema::Bytes => encode_bytes(bytes, buffer),
            Schema::Fixed { .. } => buffer.extend(bytes),
            _ => (),
        },
        Value::String(s) => match *schema {
            Schema::String => {
                encode_bytes(s, buffer);
            }
            Schema::Enum { ref symbols, .. } => {
                if let Some(index) = symbols.iter().position(|item| item == s) {
                    encode_int(index as i32, buffer);
                }
            }
            _ => (),
        },
        Value::Fixed(_, bytes) => buffer.extend(bytes),
        Value::Enum(i, _) => encode_int(*i, buffer),
        Value::Union(item) => {
            if let Schema::Union(ref inner) = *schema {
                // Find the schema that is matched here. Due to validation, this should always
                // return a value.
                let (idx, inner_schema) = inner
                    .find_schema(item)
                    .expect("Invalid Union validation occurred");
                encode_long(idx as i64, buffer);
                encode_ref(&*item, inner_schema, buffer);
            }
        }
        Value::Array(items) => {
            if let Schema::Array(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for item in items.iter() {
                        encode_ref(item, inner, buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Map(items) => {
            if let Schema::Map(ref inner) = *schema {
                if !items.is_empty() {
                    encode_long(items.len() as i64, buffer);
                    for (key, value) in items {
                        encode_bytes(key, buffer);
                        encode_ref(value, inner, buffer);
                    }
                }
                buffer.push(0u8);
            }
        }
        Value::Record(fields) => {
            if let Schema::Record {
                fields: ref schema_fields,
                ..
            } = *schema
            {
                for (i, &(_, ref value)) in fields.iter().enumerate() {
                    encode_ref(value, &schema_fields[i].schema, buffer);
                }
            }
        }
    }
}

pub fn encode_to_vec(value: &Value, schema: &Schema) -> Vec<u8> {
    let mut buffer = Vec::new();
    encode(value, schema, &mut buffer);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_encode_empty_array() {
        let mut buf = Vec::new();
        let empty: Vec<Value> = Vec::new();
        encode(
            &Value::Array(empty),
            &Schema::Array(Box::new(Schema::Int)),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }

    #[test]
    fn test_encode_empty_map() {
        let mut buf = Vec::new();
        let empty: HashMap<String, Value> = HashMap::new();
        encode(
            &Value::Map(empty),
            &Schema::Map(Box::new(Schema::Int)),
            &mut buf,
        );
        assert_eq!(vec![0u8], buf);
    }
}
