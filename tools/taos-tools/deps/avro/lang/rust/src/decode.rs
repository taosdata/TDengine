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
    decimal::Decimal,
    duration::Duration,
    schema::Schema,
    types::Value,
    util::{safe_len, zag_i32, zag_i64},
    AvroResult, Error,
};
use std::{collections::HashMap, convert::TryFrom, io::Read, str::FromStr};
use uuid::Uuid;

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> AvroResult<Value> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let len = zag_i64(reader)?;
    safe_len(usize::try_from(len).map_err(|e| Error::ConvertI64ToUsize(e, len))?)
}

/// Decode the length of a sequence.
///
/// Maps and arrays are 0-terminated, 0i64 is also encoded as 0 in Avro reading a length of 0 means
/// the end of the map or array.
fn decode_seq_len<R: Read>(reader: &mut R) -> AvroResult<usize> {
    let raw_len = zag_i64(reader)?;
    safe_len(
        usize::try_from(match raw_len.cmp(&0) {
            std::cmp::Ordering::Equal => return Ok(0),
            std::cmp::Ordering::Less => {
                let _size = zag_i64(reader)?;
                -raw_len
            }
            std::cmp::Ordering::Greater => raw_len,
        })
        .map_err(|e| Error::ConvertI64ToUsize(e, raw_len))?,
    )
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> AvroResult<Value> {
    match *schema {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            reader
                .read_exact(&mut buf[..])
                .map_err(Error::ReadBoolean)?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false)),
                1u8 => Ok(Value::Boolean(true)),
                _ => Err(Error::BoolValue(buf[0])),
            }
        }
        Schema::Decimal { ref inner, .. } => match &**inner {
            Schema::Fixed { .. } => match decode(inner, reader)? {
                Value::Fixed(_, bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                value => Err(Error::FixedValue(value.into())),
            },
            Schema::Bytes => match decode(inner, reader)? {
                Value::Bytes(bytes) => Ok(Value::Decimal(Decimal::from(bytes))),
                value => Err(Error::BytesValue(value.into())),
            },
            schema => Err(Error::ResolveDecimalSchema(schema.into())),
        },
        Schema::Uuid => Ok(Value::Uuid(
            Uuid::from_str(match decode(&Schema::String, reader)? {
                Value::String(ref s) => s,
                value => return Err(Error::GetUuidFromStringValue(value.into())),
            })
            .map_err(Error::ConvertStrToUuid)?,
        )),
        Schema::Int => decode_int(reader),
        Schema::Date => zag_i32(reader).map(Value::Date),
        Schema::TimeMillis => zag_i32(reader).map(Value::TimeMillis),
        Schema::Long => decode_long(reader),
        Schema::TimeMicros => zag_i64(reader).map(Value::TimeMicros),
        Schema::TimestampMillis => zag_i64(reader).map(Value::TimestampMillis),
        Schema::TimestampMicros => zag_i64(reader).map(Value::TimestampMicros),
        Schema::Duration => {
            let mut buf = [0u8; 12];
            reader.read_exact(&mut buf).map_err(Error::ReadDuration)?;
            Ok(Value::Duration(Duration::from(buf)))
        }
        Schema::Float => {
            let mut buf = [0u8; std::mem::size_of::<f32>()];
            reader.read_exact(&mut buf[..]).map_err(Error::ReadFloat)?;
            Ok(Value::Float(f32::from_le_bytes(buf)))
        }
        Schema::Double => {
            let mut buf = [0u8; std::mem::size_of::<f64>()];
            reader.read_exact(&mut buf[..]).map_err(Error::ReadDouble)?;
            Ok(Value::Double(f64::from_le_bytes(buf)))
        }
        Schema::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(Error::ReadBytes)?;
            Ok(Value::Bytes(buf))
        }
        Schema::String => {
            let len = decode_len(reader)?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(Error::ReadString)?;

            Ok(Value::String(
                String::from_utf8(buf).map_err(Error::ConvertToUtf8)?,
            ))
        }
        Schema::Fixed { size, .. } => {
            let mut buf = vec![0u8; size];
            reader
                .read_exact(&mut buf)
                .map_err(|e| Error::ReadFixed(e, size))?;
            Ok(Value::Fixed(size, buf))
        }
        Schema::Array(ref inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                items.reserve(len);
                for _ in 0..len {
                    items.push(decode(inner, reader)?);
                }
            }

            Ok(Value::Array(items))
        }
        Schema::Map(ref inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_seq_len(reader)?;
                if len == 0 {
                    break;
                }

                items.reserve(len);
                for _ in 0..len {
                    match decode(&Schema::String, reader)? {
                        Value::String(key) => {
                            let value = decode(inner, reader)?;
                            items.insert(key, value);
                        }
                        value => return Err(Error::MapKeyType(value.into())),
                    }
                }
            }

            Ok(Value::Map(items))
        }
        Schema::Union(ref inner) => {
            let index = zag_i64(reader)?;
            let variants = inner.variants();
            let variant = variants
                .get(usize::try_from(index).map_err(|e| Error::ConvertI64ToUsize(e, index))?)
                .ok_or_else(|| Error::GetUnionVariant {
                    index,
                    num_variants: variants.len(),
                })?;
            let value = decode(variant, reader)?;
            Ok(Value::Union(Box::new(value)))
        }
        Schema::Record { ref fields, .. } => {
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                // TODO: This clone is also expensive. See if we can do away with it...
                items.push((field.name.clone(), decode(&field.schema, reader)?));
            }
            Ok(Value::Record(items))
        }
        Schema::Enum { ref symbols, .. } => {
            Ok(if let Value::Int(raw_index) = decode_int(reader)? {
                let index = usize::try_from(raw_index)
                    .map_err(|e| Error::ConvertI32ToUsize(e, raw_index))?;
                if (0..=symbols.len()).contains(&index) {
                    let symbol = symbols[index].clone();
                    Value::Enum(raw_index, symbol)
                } else {
                    return Err(Error::GetEnumValue {
                        index,
                        nsymbols: symbols.len(),
                    });
                }
            } else {
                return Err(Error::GetEnumSymbol);
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        decode::decode,
        schema::Schema,
        types::{
            Value,
            Value::{Array, Int, Map},
        },
        Decimal,
    };
    use std::collections::HashMap;

    #[test]
    fn test_decode_array_without_size() {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];
        let result = decode(&Schema::Array(Box::new(Schema::Int)), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_array_with_size() {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = decode(&Schema::Array(Box::new(Schema::Int)), &mut input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result.unwrap());
    }

    #[test]
    fn test_decode_map_without_size() {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::Map(Box::new(Schema::Int)), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    #[test]
    fn test_decode_map_with_size() {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::Map(Box::new(Schema::Int)), &mut input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result.unwrap());
    }

    #[test]
    fn test_negative_decimal_value() {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed {
            size: 2,
            name: Name::new("decimal"),
        });
        let schema = Schema::Decimal {
            inner,
            precision: 4,
            scale: 2,
        };
        let bigint = (-423).to_bigint().unwrap();
        let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer);

        let mut bytes = &buffer[..];
        let result = decode(&schema, &mut bytes).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_decode_decimal_with_bigger_than_necessary_size() {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed {
            size: 13,
            name: Name::new("decimal"),
        });
        let schema = Schema::Decimal {
            inner,
            precision: 4,
            scale: 2,
        };
        let value = Value::Decimal(Decimal::from(
            ((-423).to_bigint().unwrap()).to_signed_bytes_be(),
        ));
        let mut buffer = Vec::<u8>::new();

        encode(&value, &schema, &mut buffer);
        let mut bytes: &[u8] = &buffer[..];
        let result = decode(&schema, &mut bytes).unwrap();
        assert_eq!(result, value);
    }
}
