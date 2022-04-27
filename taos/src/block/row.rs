use std::{
    any::type_name,
    cell::Cell,
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    rc::Rc,
};

use bitvec_simd::BitVec;

use crate::{Error, Result};
use serde::de::{self, IntoDeserializer, Visitor};
use taos_query::common::Field;

use super::{BorrowedValue, Block};

#[derive(Debug)]
pub struct Row<'block> {
    // todo: Rc or Arc?
    block: Arc<Block<'block>>,
    index: usize,
}

impl<'block> Deref for Row<'block> {
    type Target = Block<'block>;

    fn deref(&self) -> &Self::Target {
        self.block.deref()
    }
}

impl<'block> Row<'block> {
    pub(crate) fn new(block: Arc<Block<'block>>, index: usize) -> Self {
        Self { block, index }
    }
    pub(crate) fn deserializer(&self) -> Deserializer {
        Deserializer::new(self)
    }

    // fn get(&self, col: usize) -> Option<BorrowedValue> {
    //     self.block.get_value(self.index, col)
    // }

    fn value_iter(&self) -> ValueIter {
        ValueIter::new(self)
    }

    // fn entry_iter(&self) -> EntryIter {
    //     EntryIter::new(self)
    // }

    // fn into_value_iter(self) -> IntoValueIter {}
}

// impl IntoIter

// impl<'block, 'de> IntoDeserializer<'de, Error> for Row<'block> {
//     type Deserializer = Deserializer<'block>;

//     fn into_deserializer(self) -> Self::Deserializer {
//         todo!()
//     }
// }
struct EntryIter<'block>(ValueIter<'block>);

impl<'block> EntryIter<'block> {
    fn new(row: &'block Row<'block>) -> Self {
        Self(ValueIter::new(row))
    }
}

impl<'block> From<ValueIter<'block>> for EntryIter<'block> {
    fn from(rhs: ValueIter<'block>) -> Self {
        Self(rhs)
    }
}

// impl<'block> Iterator for EntryIter<'block> {
//     type Item = (&'block Field, BorrowedValue<'block>);

//     fn next(&mut self) -> Option<Self::Item> {
//         log::trace!("next entry");
//         self.0
//             .next()
//             .map(|value| (unsafe { self.0.block.get_field_unchecked(self.0.col) }, value))
//     }
// }

// impl<'block> IntoIterator for Row<'block> {
//     type Item = (&'block Field, BorrowedValue<'block>);

//     type IntoIter = EntryIter<'block>;

//     fn into_iter(self) -> Self::IntoIter {
//         EntryIter(ValueIter::new(&self))
//     }
// }

pub(crate) struct ValueIter<'block> {
    block: Rc<Block<'block>>,
    row: usize,
    col: usize,
    hint: usize,
}

impl<'block> ValueIter<'block> {
    fn new(row: &Row<'block>) -> Self {
        Self {
            block: row.block.clone(),
            row: row.index,
            col: 0,
            hint: row.num_of_fields(),
        }
    }
}

impl<'a> Deref for ValueIter<'a> {
    type Target = Block<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl<'block> Iterator for ValueIter<'block> {
    type Item = BorrowedValue<'block>;

    fn next(&mut self) -> Option<Self::Item> {
        let (row, col) = (self.row, self.col);
        log::trace!("get: ({row}, {col})");
        if col < self.num_of_fields() {
            self.col += 1;
            self.hint -= 1;
            self.block.get_value(row, col)
        } else {
            None
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.col += n;
        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.hint, Some(self.hint))
    }

    fn count(self) -> usize {
        self.block.num_of_fields() - self.col
    }

    fn last(mut self) -> Option<Self::Item> {
        self.nth(self.hint - 1)
    }
}

impl<'block> ExactSizeIterator for ValueIter<'block> {
    fn len(&self) -> usize {
        self.hint - self.col
    }
}

impl<'block> DoubleEndedIterator for ValueIter<'block> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.col <= self.hint {
            None
        } else {
            self.hint -= 1;
            // todo: next_back should be tested.
            self.nth(self.hint)
        }
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        self.hint -= n;
        self.next_back()
    }
}

pub(crate) struct Deserializer<'a> {
    iter: ValueIter<'a>,
}

impl<'a> Deserializer<'a> {
    fn new(row: &'a Row<'a>) -> Self {
        Self {
            iter: row.value_iter(),
        }
    }
}

impl<'a> Deref for Deserializer<'a> {
    type Target = ValueIter<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}

impl<'a> DerefMut for Deserializer<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iter
    }
}
struct MapReader<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    banned: BitVec,
    fields: BTreeMap<String, usize>,
    acc: std::vec::IntoIter<String>,
    value: Option<BorrowedValue<'de>>,
}

impl<'a, 'de> MapReader<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        let n = de.num_of_fields();
        // let acc = unsafe { de.get_fields_unchecked() }.iter();
        let acc = de.get_field_names_to_string_vec().into_iter();
        let fields = acc
            .clone()
            .enumerate()
            .map(|(index, field)| (field, index))
            .collect();
        Self {
            de,
            banned: BitVec::zeros(n),
            fields,
            acc,
            value: None,
        }
    }

    // fn access_field(&mut self, field: &str) -> Option<BorrowedValue> {
    //     self.fields
    //         .get(field)
    //         .copied()
    //         .and_then(|n| self.access_nth(n))
    // }

    // fn access_nth(&mut self, n: usize) -> Option<BorrowedValue> {
    //     self.banned.get(n).and_then(|banned| {
    //         if banned {
    //             None
    //         } else {
    //             self.banned.set(n, true);
    //             self.de.get(n)
    //         }
    //     })
    // }
    // fn access_next(&mut self) -> Option<BorrowedValue> {
    //     self.banned.get(self.de.col).and_then(|banned| {
    //         if banned {
    //             None
    //         } else {
    //             self.banned.set(self.de.col, true);
    //             self.de.next()
    //         }
    //     })
    // }
    fn next_entry(&mut self) -> Option<(String, BorrowedValue<'de>)> {
        self.acc.next().zip(self.de.next())
    }
}

#[derive(Clone)]
struct StringDeserializer {
    input: String,
}

impl<'de> de::Deserializer<'de> for StringDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.input)
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

impl<'a, 'de> de::MapAccess<'de> for MapReader<'a, 'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: de::DeserializeSeed<'de>,
    {
        match self.next_entry() {
            Some((name, value)) => {
                self.value = Some(value);
                seed.deserialize(name.into_deserializer()).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: de::DeserializeSeed<'de>,
    {
        let value = self.value.take().unwrap(); // always be here, so it's safe to unwrap
        log::debug!("deserialize value: {:?}", value);
        log::trace!("target value: {:?}", type_name::<V::Value>());
        seed.deserialize(value)
            .map_err(<Self::Error as de::Error>::custom)
    }
}

struct SeqReader<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}
impl<'de, 'a> SeqReader<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        Self { de }
    }
}

impl<'de, 'a> de::SeqAccess<'de> for SeqReader<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        match self.de.next() {
            Some(v) => seed
                .deserialize(dbg!(v))
                .map(Some)
                .map_err(<Self::Error as de::Error>::custom),
            None => Ok(None),
        }
    }
}
impl<'a, 'de> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        println!("call deserialize any");
        dbg!(type_name::<V>());
        match self.iter.next() {
            Some(v) => v
                .deserialize_any(visitor)
                .map_err(<Self::Error as de::Error>::custom),
            None => Err(Error::from_string("expect value, not none")),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes byte_buf enum
        identifier ignored_any
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // todo: get_str should be replaced by BorrowedValue::deserialize
        let s = unsafe { self.get_str(self.row, self.col) }?;
        if let Some(s) = s {
            visitor.visit_str(s)
        } else {
            Err(Error::from_string(
                "expect non-null str, but the value is null",
            ))
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        log::debug!("call deserialize_option");
        match self.next() {
            Some(v) => {
                if v.is_null() {
                    visitor.visit_none()
                } else {
                    visitor
                        .visit_some(v)
                        .map_err(<Self::Error as de::Error>::custom)
                }
            }
            _ => Err(<Self::Error as de::Error>::custom("expect next value")),
        }
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.next() {
            Some(_v) => visitor.visit_unit(),
            _ => Err(Error::from_string("there's no enough value")),
        }
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        log::debug!("deserialize_newtype_struct: {_name}");
        visitor.visit_newtype_struct(self)
    }

    // Deserialization of compound types like sequences and maps happens by
    // passing the visitor an "Access" object that gives it the ability to
    // iterate through the data contained in the sequence.
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SeqReader::new(self))
    }

    // Tuples look just like sequences.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // self.deserialize_any(visitor)
        visitor.visit_seq(SeqReader::new(self))
    }

    // Tuple structs look just like sequences.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Much like `deserialize_seq` but calls the visitors `visit_map` method
    // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
    // method with a `SeqAccess` implementation.
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // let value = visitor.visit_map(self);
        // unimplemented!();
        log::info!("visit map");
        dbg!(type_name::<V::Value>());
        visitor.visit_map(MapReader::new(self))
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
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        log::debug!("name: {_name}, fields: {_fields:?}");
        self.deserialize_map(visitor)
    }
}
