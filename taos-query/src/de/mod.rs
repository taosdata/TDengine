use std::any::type_name;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;

use serde::de::value::Error;

use crate::{Field, Valuable};

/// Row-based deserializer helper.
///
/// 'b: field lifetime may go across the whole query.
pub(crate) struct RecordDeserializer<'de, 'b: 'de, T, R>
where
    T: Valuable<'de, 'b>,
    R: IntoIterator<Item = (&'b Field, T)>,
{
    inner: <R as IntoIterator>::IntoIter,
    value: Option<T>,
    _marker: PhantomData<(&'b R, &'de u8)>,
}

impl<'b, 'de, T, R> From<R> for RecordDeserializer<'de, 'b, T, R>
where
    T: Valuable<'de, 'b>,
    R: IntoIterator<Item = (&'b Field, T)>,
{
    fn from(input: R) -> Self {
        Self {
            inner: input.into_iter(),
            value: None,
            _marker: PhantomData,
        }
    }
}

impl<'b, 'de, T, R> RecordDeserializer<'de, 'b, T, R>
where
    T: Valuable<'de, 'b>,
    R: IntoIterator<Item = (&'b Field, T)>,
{
    fn next_value(&mut self) -> Option<T> {
        self.inner.next().map(|(_, v)| v)
    }
}

#[derive(Clone)]
struct StringDeserializer(String);

impl<'de> Deserializer<'de> for StringDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

impl<'b, 'de, T, R> MapAccess<'de> for RecordDeserializer<'de, 'b, T, R>
where
    T: Valuable<'de, 'b>,
    R: IntoIterator<Item = (&'b Field, T)>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.inner.next() {
            Some((name, value)) => {
                self.value = Some(value);
                seed.deserialize(name.name().into_deserializer()).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self.value.take().unwrap(); // always be here, so it's safe to unwrap

        log::debug!("deserialize value: {:?}", value);
        log::trace!("target value: {:?}", type_name::<V::Value>());
        seed.deserialize(value)
            .map_err(<Self::Error as serde::de::Error>::custom)
    }
}

impl<'b, 'de, T, R> SeqAccess<'de> for RecordDeserializer<'de, 'b, T, R>
where
    T: Valuable<'de, 'b>,
    R: IntoIterator<Item = (&'b Field, T)>,
{
    type Error = Error;

    fn next_element_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: DeserializeSeed<'de>,
    {
        match self.inner.next() {
            Some((_, v)) => seed
                .deserialize(v)
                .map_err(<Self::Error as serde::de::Error>::custom)
                .map(Some),
            None => Ok(None),
        }
    }
}

impl<'b, 'de, T, R> Deserializer<'de> for RecordDeserializer<'de, 'b, T, R>
where
    T: Valuable<'de, 'b>,
    R: IntoIterator<Item = (&'b Field, T)>,
{
    type Error = Error;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        println!("call deserialize any");
        dbg!(type_name::<V>());
        match self.next_value() {
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
    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        log::debug!("call deserialize_str");
        match self.next_value() {
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

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        log::debug!("call deserialize_option");
        match self.next_value() {
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
    fn deserialize_unit<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.next_value() {
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
        log::info!("visit map");
        dbg!(type_name::<V::Value>());
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
        log::debug!("name: {_name}, fields: {_fields:?}");
        self.deserialize_map(visitor)
    }
}
