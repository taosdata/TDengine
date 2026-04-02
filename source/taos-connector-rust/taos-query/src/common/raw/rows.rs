use std::marker::PhantomData;
use std::ptr::NonNull;

use bigdecimal::ToPrimitive;
use chrono_tz::Tz;
use serde::de::{DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;

use crate::common::{BorrowedValue, Value};
use crate::RawBlock;

pub struct IntoRowsIter<'a> {
    pub(crate) raw: RawBlock,
    pub(crate) row: usize,
    pub(super) tz: Option<Tz>,
    pub(crate) _marker: PhantomData<&'a bool>,
}

unsafe impl Send for IntoRowsIter<'_> {}
unsafe impl Sync for IntoRowsIter<'_> {}

impl<'a> Iterator for IntoRowsIter<'a> {
    type Item = RowView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.raw.nrows() {
            None
        } else {
            let row = self.row;
            self.row += 1;
            Some(RowView {
                raw: unsafe { &*(&self.raw as *const RawBlock) },
                row,
                col: 0,
                tz: self.tz,
            })
        }
    }
}

pub struct RowsIter<'a> {
    pub(super) raw: NonNull<RawBlock>,
    pub(super) row: usize,
    pub(super) tz: Option<Tz>,
    pub(crate) _marker: PhantomData<&'a usize>,
}

unsafe impl Send for RowsIter<'_> {}
unsafe impl Sync for RowsIter<'_> {}

impl<'a> Iterator for RowsIter<'a> {
    type Item = RowView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= unsafe { self.raw.as_mut() }.nrows() {
            None
        } else {
            let row = self.row;
            self.row += 1;
            Some(RowView {
                raw: unsafe { self.raw.as_mut() },
                row,
                col: 0,
                tz: self.tz,
            })
        }
    }
}

impl RowsIter<'_> {
    pub fn values(&mut self) -> ValueIter<'_> {
        ValueIter {
            raw: unsafe { self.raw.as_mut() },
            row: self.row,
            col: 0,
        }
    }

    pub fn named_values(&mut self) -> RowView<'_> {
        RowView {
            raw: unsafe { self.raw.as_mut() },
            row: self.row,
            col: 0,
            tz: self.tz,
        }
    }
}

pub struct ValueIter<'a> {
    raw: &'a RawBlock,
    row: usize,
    col: usize,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = BorrowedValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.col >= self.raw.ncols() {
            None
        } else {
            unsafe {
                let col = self.col;
                self.col += 1;
                Some(self.raw.get_ref_unchecked(self.row, col))
            }
        }
    }
}

pub struct RowView<'a> {
    raw: &'a RawBlock,
    row: usize,
    col: usize,
    tz: Option<Tz>,
}

impl<'a> Iterator for RowView<'a> {
    type Item = (&'a str, BorrowedValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.col >= self.raw.ncols() {
            None
        } else {
            unsafe {
                let col = self.col;
                self.col += 1;
                Some((
                    self.raw.fields.get_unchecked(col).as_str(),
                    self.raw.get_ref_unchecked(self.row, col),
                ))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let max = self.raw.ncols();
        if self.col < max {
            let hint = max - self.col;
            (hint, Some(hint))
        } else {
            (0, Some(0))
        }
    }
}

impl ExactSizeIterator for RowView<'_> {}

impl std::fmt::Debug for RowView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowView")
            .field("raw", &self.raw)
            .field("row", &self.row)
            .field("col", &self.col)
            .field("tz", &self.tz)
            .finish()
    }
}

pub struct RowViewOfValue<'a>(RowView<'a>);

impl<'a> Iterator for RowViewOfValue<'a> {
    type Item = BorrowedValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.col >= self.0.raw.ncols() {
            None
        } else {
            unsafe {
                let col = self.0.col;
                self.0.col += 1;
                Some(self.0.raw.get_ref_unchecked(self.0.row, col))
            }
        }
    }
}

impl<'a> RowView<'a> {
    pub fn into_value_iter(self) -> RowViewOfValue<'a> {
        RowViewOfValue(self)
    }

    fn walk_next(&mut self) -> Option<BorrowedValue<'a>> {
        self.next().map(|(_, v)| v)
    }

    fn peek_name(&self) -> Option<&'a str> {
        self.raw.fields.get(self.col).map(String::as_str)
    }

    pub fn into_values(self) -> Vec<Value> {
        self.map(|(_, b)| b.to_value()).collect()
    }

    pub fn set_timezone(&mut self, tz: Option<Tz>) {
        self.tz = tz;
    }
}

pub(super) type DeError = taos_error::Error;

impl<'de, 'a: 'de> SeqAccess<'de> for RowView<'a> {
    type Error = DeError;

    fn next_element_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: DeserializeSeed<'de>,
    {
        match self.next() {
            Some((_, v)) => seed
                .deserialize(v)
                .map_err(<Self::Error as serde::de::Error>::custom)
                .map(Some),
            None => Ok(None),
        }
    }
}

impl<'de, 'a: 'de> MapAccess<'de> for RowView<'a> {
    type Error = DeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.peek_name() {
            Some(name) => seed.deserialize(name.into_deserializer()).map(Some),
            _ => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
            .map_err(<Self::Error as serde::de::Error>::custom)
    }
}

macro_rules! value_forward_to_deserialize_any {
    ($($ty: ty)*) => {
        $(paste::paste! {
            fn [<deserialize_$ty>]<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                match self.walk_next() {
                    Some(BorrowedValue::Decimal(v))  => {
                        visitor.[<visit_$ty>]::<DeError>(v.as_bigdecimal().[<to_$ty>]().ok_or(<Self::Error as serde::de::Error>::custom(format!("decimal cannot cast to target type: {}", stringify!($ty))))?)
                    }
                    Some(BorrowedValue::Decimal64(v)) => {
                       visitor.[<visit_$ty>]::<DeError>(v.as_bigdecimal().[<to_$ty>]().ok_or(<Self::Error as serde::de::Error>::custom(format!("decimal cannot cast to target type: {}", stringify!($ty))))?)
                    }
                    Some(v) => v.deserialize_any(visitor).map_err(|e| serde::de::Error::custom(e)),
                    None => Err(<Self::Error as serde::de::Error>::custom(
                        "expect value, not none",
                    )),
                }
            }
        })*
    };
}

impl<'de, 'a: 'de> Deserializer<'de> for &mut RowView<'a> {
    type Error = DeError;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.walk_next() {
            Some(v) => v
                .deserialize_any(visitor)
                .map_err(<Self::Error as serde::de::Error>::custom),
            None => Err(<Self::Error as serde::de::Error>::custom(
                "expect value, not none",
            )),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        use bigdecimal::Zero;
        match self.walk_next() {
            Some(BorrowedValue::Decimal(v)) => visitor
                .visit_bool::<DeError>(!v.as_bigdecimal().is_zero())
                .map_err(serde::de::Error::custom),
            Some(BorrowedValue::Decimal64(v)) => visitor
                .visit_bool::<DeError>(!v.as_bigdecimal().is_zero())
                .map_err(serde::de::Error::custom),
            Some(v) => v.deserialize_any(visitor).map_err(serde::de::Error::custom),
            None => Err(<Self::Error as serde::de::Error>::custom(
                "expect value, not none",
            )),
        }
    }

    value_forward_to_deserialize_any! {
        i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 i128 u128
    }

    serde::forward_to_deserialize_any! {
        char bytes byte_buf enum identifier ignored_any
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.walk_next() {
            Some(BorrowedValue::Timestamp(v)) => {
                if let Some(tz) = &self.tz {
                    visitor.visit_string(v.to_datetime_with_custom_tz(tz).to_rfc3339())
                } else {
                    visitor.visit_string(v.to_datetime_with_tz().to_rfc3339())
                }
            }
            Some(v) => v
                .deserialize_str(visitor)
                .map_err(<Self::Error as serde::de::Error>::custom),
            None => Err(<Self::Error as serde::de::Error>::custom(
                "expect value, not none",
            )),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.walk_next() {
            Some(v) => {
                if v.is_null() {
                    visitor.visit_none()
                } else {
                    visitor
                        .visit_some(v)
                        .map_err(<Self::Error as serde::de::Error>::custom)
                }
            }
            _ => Err(<Self::Error as serde::de::Error>::custom(
                "expect next value",
            )),
        }
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.walk_next() {
            Some(_v) => visitor.visit_unit(),
            _ => Err(<Self::Error as serde::de::Error>::custom(
                "there's no enough value",
            )),
        }
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    // Deserialization of compound types like sequences and maps happens by
    // passing the visitor an "Access" object that gives it the ability to
    // iterate through the data contained in the sequence.
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    // Tuples look just like sequences.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // self.deserialize_any(visitor)
        visitor.visit_seq(self)
    }

    // Tuple structs look just like sequences.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Much like `deserialize_seq` but calls the visitors `visit_map` method
    // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
    // method with a `SeqAccess` implementation.
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // No field names, just access as sequence.
        if self.raw.fields.is_empty() {
            return visitor.visit_seq(self);
        }
        visitor.visit_map(self)
    }

    // Structs look just like maps in JSON.
    //
    // Notice the `fields` parameter - a "struct" in the Serde data model means
    // that the `Deserialize` implementation is required to know what the fields
    // are before even looking at the input data. Any key-value pairing in which
    // the fields cannot be known ahead of time is probably a map.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::common::Precision;

    #[test]
    fn deserialize_decimal_test() -> anyhow::Result<()> {
        let bytes = bytes::Bytes::from_static(&[
            1, 0, 0, 0, 79, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0,
            9, 8, 0, 0, 0, 21, 2, 10, 0, 8, 4, 4, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, 0,
            138, 123, 47, 178, 149, 1, 0, 0, 0, 68, 214, 18, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
        ]);
        let mut block = RawBlock::parse_from_raw_block(bytes, Precision::Microsecond);
        block.fields = vec!["ts".to_string(), "v".to_string(), "g".to_string()];

        #[allow(dead_code)]
        #[derive(Debug, serde::Deserialize)]
        struct Data {
            ts: i64,
            v: f64,
        }
        let values = block
            .rows()
            .map(|mut v| Data::deserialize(&mut v))
            .collect::<Result<Vec<_>, _>>()?;
        dbg!(values);
        Ok(())
    }
}
