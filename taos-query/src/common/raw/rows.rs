use std::{marker::PhantomData, ptr::NonNull};

use serde::{
    de::{DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor},
    Deserializer,
};

use crate::{
    common::{BorrowedValue, Value},
    RawBlock,
};

pub struct IntoRowsIter<'a> {
    pub(crate) raw: RawBlock,
    pub(crate) row: usize,
    pub(crate) _marker: PhantomData<&'a bool>,
}

pub struct RowsIter<'a> {
    pub(super) raw: NonNull<RawBlock>,
    pub(super) row: usize,
    pub(crate) _marker: PhantomData<&'a usize>,
}

unsafe impl<'a> Send for RowsIter<'a> {}
unsafe impl<'a> Sync for RowsIter<'a> {}

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
                row: row,
                col: 0,
            })
        }
    }
}

impl<'a> RowsIter<'a> {
    pub fn values(&mut self) -> ValueIter {
        ValueIter {
            raw: unsafe { self.raw.as_mut() },
            row: self.row,
            col: 0,
        }
    }
    pub fn named_values(&mut self) -> RowView {
        RowView {
            raw: unsafe { self.raw.as_mut() },
            row: self.row,
            col: 0,
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
}

impl<'a> RowView<'a> {
    fn walk_next(&mut self) -> Option<BorrowedValue<'a>> {
        self.next().map(|(_, v)| v)
    }

    fn walk(&mut self) {
        self.col += 1;
    }

    fn peek_name(&self) -> Option<&'a str> {
        self.raw.fields.get(self.col).map(|s| s.as_str())
    }
    fn peek_value(&self) -> Option<BorrowedValue<'a>> {
        self.raw.get_ref(self.row, self.col)
    }

    pub fn into_values(self) -> Vec<Value> {
        self.map(|(_, b)| b.to_value()).collect()
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
        // let value = self
        //     .walk_next()
        //     .ok_or(<Self::Error as serde::de::Error>::custom(
        //         "expect a value but no value remains",
        //     ))?; // always be here, so it's safe to unwrap

        log::trace!("target value: {:?}", std::any::type_name::<V::Value>());
        seed.deserialize(&mut *self)
            .map_err(<Self::Error as serde::de::Error>::custom)
    }
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
        log::trace!("call deserialize any for <{}>", std::any::type_name::<V>());
        match self.walk_next() {
            Some(v) => v
                .deserialize_any(visitor)
                .map_err(<Self::Error as serde::de::Error>::custom),
            None => Err(<Self::Error as serde::de::Error>::custom(
                "expect value, not none",
            )),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes byte_buf enum
        identifier ignored_any
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        log::trace!("call deserialize_str for <{}>", std::any::type_name::<V>());
        match self.walk_next() {
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
        log::trace!(
            "call deserialize_option for <{}>",
            std::any::type_name::<V>()
        );
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
        log::debug!("deserialize_newtype_struct: {}", _name);
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
        // let value = visitor.visit_map(self);
        // unimplemented!();
        log::trace!("visit map for {}", std::any::type_name::<V::Value>());

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
        log::trace!("name: {_name}, fields: {_fields:?}");
        self.deserialize_map(visitor)
    }
}
