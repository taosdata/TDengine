use std::{
    collections::BTreeMap,
    error::Error,
    iter::Rev,
    marker::PhantomData,
    mem::swap,
    ops::{Deref, DerefMut},
    rc::Rc,
    slice,
    sync::{Arc, Mutex},
    task::Poll,
    vec,
};

use bitvec_simd::BitVec;
use futures::Stream;
use serde::de::{self, value::MapDeserializer, Error as DeError, IntoDeserializer, Visitor};
use taos_sys::{taos_is_null, TaosDataType, TAOS_FIELD};

use crate::{timestamp::TimestampValue, Result, TaosError, TaosResult};

use super::{value::BorrowedValue, Block, BlockStream};

pub struct Row<'block> {
    block: Rc<Block<'block>>,
    index: usize,
}

impl<'block> Deref for Row<'block> {
    type Target = Block<'block>;

    fn deref(&self) -> &Self::Target {
        self.block.deref()
    }
}

impl<'block> Row<'block> {
    pub(crate) fn new(block: Rc<Block<'block>>, index: usize) -> Self {
        Self { block, index }
    }
    pub(crate) fn deserializer(&self) -> Deserializer {
        Deserializer::new(self)
    }

    fn get(&self, col: usize) -> Option<BorrowedValue> {
        self.block.get_value(self.index, col)
    }

    fn value_iter(&self) -> ValueIter {
        ValueIter::new(self)
    }

    fn entry_iter(&self) -> EntryIter {
        EntryIter::new(self)
    }

    // fn into_value_iter(self) -> IntoValueIter {}
}
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

impl<'block> Iterator for EntryIter<'block> {
    type Item = (&'block TAOS_FIELD, BorrowedValue<'block>);

    fn next(&mut self) -> Option<Self::Item> {
        log::trace!("next entry");
        self.0
            .next()
            .map(|value| (unsafe { self.0.row.get_field_unchecked(self.0.col) }, value))
    }
}

pub(crate) struct ValueIter<'block> {
    row: &'block Row<'block>,
    col: usize,
    hint: usize,
}

impl<'block> ValueIter<'block> {
    fn new(row: &'block Row<'block>) -> Self {
        Self {
            row,
            col: 0,
            hint: row.num_of_fields(),
        }
    }
}

impl<'a> Deref for ValueIter<'a> {
    type Target = Row<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl<'block> Iterator for ValueIter<'block> {
    type Item = BorrowedValue<'block>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("next value");
        let (row, col) = (self.row.index, self.col);
        self.col += 1;
        if self.hint != 0 {
            self.hint -= 1;
            dbg!(self.row.get_value(row, col))
        } else {
            None
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.col += n;
        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.col, Some(self.hint))
    }

    fn count(self) -> usize {
        self.row.num_of_fields() - self.col
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
    acc: slice::Iter<'a, TAOS_FIELD>,
    value: Option<BorrowedValue<'a>>,
}

impl<'a, 'de> MapReader<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        
        let n = de.num_of_fields();
        let acc = unsafe { de.get_fields_unchecked() }.into_iter();
        let fields = de.get_field_names_to_string_vec().into_iter();
        let fields = fields
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

    fn access_field(&mut self, field: &str) -> Option<BorrowedValue> {
        self.fields
            .get(field)
            .map(|n| *n)
            .and_then(|n| self.access_nth(n))
    }

    fn access_nth(&mut self, n: usize) -> Option<BorrowedValue> {
        self.banned.get(n).and_then(|banned| {
            if banned {
                None
            } else {
                self.banned.set(n, true);
                self.de.get(n)
            }
        })
    }
    fn access_next(&mut self) -> Option<BorrowedValue> {
        self.banned.get(self.de.col).and_then(|banned| {
            if banned {
                None
            } else {
                self.banned.set(self.de.col, true);
                self.de.next()
            }
        })
    }
    fn next_entry(&mut self) -> Option<(&'a TAOS_FIELD, BorrowedValue<'a>)> {
        self.acc.next().zip(self.de.next())
    }
}

#[derive(Clone)]
struct StringDeserializer {
    input: String,
}

impl<'de> de::Deserializer<'de> for StringDeserializer {
    type Error = TaosError;

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

impl<'de, 'a> de::MapAccess<'de> for MapReader<'a, 'de> {
    type Error = TaosError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: de::DeserializeSeed<'de>,
    {
        match self.next_entry() {
            Some((name, value)) => {
                self.value = Some(value);
                seed.deserialize(name).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: de::DeserializeSeed<'de>,
    {
        let value = self.value.take().expect("value must be there");
        seed.deserialize(value)
    }
}

// impl<'de, 'a, E> IntoDeserializer<'de, E> for &'a TAOS_FIELD
// where
//     E: de::Error,
// {
//     type Deserializer = de::value::StringDeserializer<E>;

//     fn into_deserializer(self) -> de::value::StringDeserializer<E> {
//         // StringDeserializer {
//         self.name()
//             .to_string_lossy()
//             .to_string()
//             .into_deserializer()
//         // }
//     }
// }

// impl<

impl<'a, 'de> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = TaosError;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    // Uses the `parse_bool` parsing function defined above to read the JSON
    // identifier `true` or `false` from the input.
    //
    // Parsing refers to looking at the input and deciding that it contains the
    // JSON value `true` or `false`.
    //
    // Deserialization refers to mapping that JSON value into Serde's data
    // model by invoking one of the `Visitor` methods. In the case of JSON and
    // bool that mapping is straightforward so the distinction may seem silly,
    // but in other cases Deserializers sometimes perform non-obvious mappings.
    // For example the TOML format has a Datetime type and Serde's data model
    // does not. In the `toml` crate, a Datetime in the input is deserialized by
    // mapping it to a Serde data model "struct" type with a special name and a
    // single field containing the Datetime represented as a string.
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // The `parse_signed` function is generic over the integer type `T` so here
    // it is invoked with `T=i8`. The next 8 methods are similar.
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_i8(self.parse_signed()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_i16(self.parse_signed()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // visitor.visit_i32(self.parse_signed()?)

        unimplemented!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_i64(self.parse_signed()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_u8(self.parse_unsigned()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_u16(self.parse_unsigned()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_u32(self.parse_unsigned()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
        // visitor.visit_u64(self.parse_unsigned()?)
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // The `Serializer` implementation on the previous page serialized chars as
    // single-character strings so handle that representation here.
    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Parse a string, check that it is one character, call `visit_char`.
        unimplemented!()
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let s = unsafe { self.get_str(self.row.index, self.col) }?;
        if let Some(s) = s {
            visitor.visit_str(s)
        } else {
            Err(TaosError::from_string(
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

    // The `Serializer` implementation on the previous page serialized byte
    // arrays as JSON arrays of bytes. Handle that representation here.
    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // An absent optional is represented as the JSON `null` and a present
    // optional is represented as just the contained value.
    //
    // As commented in `Serializer` implementation, this is a lossy
    // representation. For example the values `Some(())` and `None` both
    // serialize as just `null`. Unfortunately this is typically what people
    // expect when working with JSON. Other formats are encouraged to behave
    // more intelligently if possible.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    // Deserialization of compound types like sequences and maps happens by
    // passing the visitor an "Access" object that gives it the ability to
    // iterate through the data contained in the sequence.
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently.
    //
    // As indicated by the length parameter, the `Deserialize` implementation
    // for a tuple in the Serde data model is required to know the length of the
    // tuple before even looking at the input data.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Tuple structs look just like sequences in JSON.
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
        println!("name: {_name}, fields: {_fields:?}");
        // dbg!(type_of:: V::Value);
        // unimplemented!()
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // An identifier in Serde is the type that identifies a field of a struct or
    // the variant of an enum. In JSON, struct fields and enum variants are
    // represented as strings. In other formats they may be represented as
    // numeric indices.
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Like `deserialize_any` but indicates to the `Deserializer` that it makes
    // no difference which `Visitor` method is called because the data is
    // ignored.
    //
    // Some deserializers are able to implement this more efficiently than
    // `deserialize_any`, for example by rapidly skipping over matched
    // delimiters without paying close attention to the data in between.
    //
    // Some formats are not able to implement this at all. Formats that can
    // implement `deserialize_any` and `deserialize_ignored_any` are known as
    // self-describing.
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}
