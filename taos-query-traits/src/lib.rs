//! This is the common query traits/types for TDengine connectors.
//!

use std::borrow::Cow;
use std::fmt::Debug;
use std::iter::FlatMap;

pub mod common;
mod de;
pub mod helpers;

use common::*;
use de::RecordDeserializer;
use helpers::*;

pub enum CodecOpts {
    Raw,
    Parquet,
}

/// A value will borrow data from a block, so there's a `'b` lifetime bound here.
/// So the `'de` lifetime will goes along with the `'b` block lifetime.
pub trait Valuable<'de, 'b: 'de>: Debug + serde::de::Deserializer<'de> {
    /// Check if the value is null or not.
    fn is_null(&self) -> bool;

    /// Sql type of the value
    fn ty(&self) -> Ty;

    /// Borrowed value.
    fn as_borrowed_value(&self) -> BorrowedValue<'b>;

    /// Owned value.
    fn into_owned_value(self) -> Value;
}

/// Define what a data block provides.
pub trait BlockExt<'de, 'b: 'de>: Debug + Sized {
    /// A block should container number of rows.
    fn num_of_rows(&self) -> u32;

    /// Fields can be queried from a block.
    fn fields(&'b self) -> Cow<'b, [Field]>;

    /// Number of fields.
    fn filed_count(&'b self) -> usize {
        self.fields().len()
    }

    type Col;
    type ColIter: Iterator<Item = Self::Col>;
    /// Query by columns.
    fn iter_cols(&self) -> Self::ColIter;

    type Value: Valuable<'de, 'b>;

    type Row: 'b + IntoIterator<Item = (&'b Field, Self::Value)>;
    type RowIter: Iterator<Item = Self::Row>;

    /// Query by rows.
    fn iter_rows(&self) -> Self::RowIter;

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize<T>(&self) -> II<'de, 'b, Self, T>
    // std::iter::Map<Self::RowIter, fn(Self::Row) -> Result<T, serde::de::value::Error>>
    where
        T: serde::de::Deserialize<'de>,
    {
        self.iter_rows().map(|row| {
            let de = RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }

    fn write_with(&self, codec: CodecOpts);

    fn write_all_with(&self, codec: CodecOpts);
}

type II<'de, 'b, B, T> = std::iter::Map<
    <B as BlockExt<'de, 'b>>::RowIter,
    fn(<B as BlockExt<'de, 'b>>::Row) -> Result<T, serde::de::value::Error>,
>;

/// A result gained from query lifetime(`'q`), and will produce a block iterator with
/// sub lifetime called `'b`(means block).
pub trait ResultSet<'q, 'de, 'b: 'de>: Sized {
    type B: 'b + BlockExt<'de, 'b>;
    type I: Iterator<Item = Self::B>;

    fn fields(&'q self) -> &'q [Field];

    fn next_block(&'q self) -> Option<Self::B>;

    fn block_iter(&'q self) -> Self::I;

    #[allow(clippy::type_complexity)]
    fn deserialize<T>(
        &'q self,
    ) -> FlatMap<Self::I, II<'de, 'b, Self::B, T>, fn(Self::B) -> II<'de, 'b, Self::B, T>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.block_iter()
            .flat_map(|b| <Self::B as BlockExt<'de, 'b>>::deserialize(&b))
    }
}

pub trait Queryable: Debug {
    type Error: Debug + From<serde::de::value::Error>;
    type ResultSet: for<'q, 'b> ResultSet<'q, 'b, 'b>;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    fn exec<T: AsRef<str>>(&self, sql: T) -> Result<usize, Self::Error> {
        self.query(sql).map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    fn databases(&self) -> Result<Vec<ShowDatabase>, Self::Error> {
        use itertools::Itertools;
        self.query("show databases")?
            .expect("`show databases` must be queryable")
            .deserialize()
            .try_collect()
            .map_err(Into::into)
    }

    fn describe(&self, table: &str) -> Result<Vec<ColumnMeta>, Self::Error> {
        use itertools::Itertools;
        self.query(format!("describe {}", table))?
            .expect("`describe <table>` must be queryable")
            .deserialize()
            .try_collect()
            .map_err(Into::into)
    }

    fn create_database<I: Into<DatabaseProperties>>(
        &self,
        name: &str,
        opts: I,
    ) -> Result<(), Self::Error> {
        let sql = format!("create database {} if not exists {}", name, opts.into());
        self.exec(&sql).map(|_| ())
    }

    fn use_database(&self, database: &str) -> Result<(), Self::Error> {
        let sql = format!("use database {}", database);
        self.exec(&sql).map(|_| ())
    }

    fn create_table(&self, name: &str) -> Result<(), Self::Error> {
        let sql = format!("create table {}", name);
        self.exec(&sql).map(|_| ())
    }
}

/// Queryable trait is the basic starter.
// pub trait Queryable<'de, 'b: 'de>: Debug {
//     type Error: Debug + From<serde::de::value::Error>;
//     type Block: 'b + BlockExt<'de, 'b>;
//     type ResultSet: 'b + ResultSet<'de, 'b>;

//     fn query<T: AsRef<str>>(&'b self, sql: T) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

//     fn exec<T: AsRef<str>>(&'b self, sql: T) -> Result<usize, Self::Error> {
//         self.query(sql).map(|res| match res {
//             Ok(_) => 0, // todo: if we should get the selected rows if not update query?
//             Err(affected) => affected,
//         })
//     }

//     // fn describe(&self, table: &str) -> Result<ColumnMeta, Self::Error>;
// }

// pub trait QueryableExt<'q>: Queryable<'q> {
//     fn describe(&self, table: &str) -> Result<Vec<ColumnMeta>, Self::Error>;

//     fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
//         use itertools::Itertools;
//         self.query(format!("show databases"))?
//             .expect("`show databases` must be queryable")
//             .deserialize()
//             .try_collect()
//             .map_err(Into::into)
//         // <Self as Queryable>::ResultSet::deserialize(rs).try_collect()
//     }

// }

#[cfg(test)]
mod tests {
    use serde::forward_to_deserialize_any;
    use std::marker::PhantomData;

    use super::*;
    #[derive(Debug)]
    struct Conn;

    #[derive(Debug)]
    struct Value<'s>(&'s str);

    struct Deserializer;

    impl<'de> serde::de::Deserializer<'de> for Deserializer {
        type Error = serde::de::value::Error;
        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i32(1)
        }

        forward_to_deserialize_any! {
            bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
            seq bytes byte_buf map unit_struct newtype_struct
            tuple_struct struct tuple enum identifier ignored_any
        }
    }

    impl<'de, 's: 'de> serde::de::Deserializer<'de> for Value<'s> {
        type Error = serde::de::value::Error;
        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i32(1)
        }

        forward_to_deserialize_any! {
            bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char unit option
            seq byte_buf map unit_struct newtype_struct
            tuple_struct struct tuple enum identifier ignored_any
        }

        fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_borrowed_str(self.0)
        }
        fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            self.deserialize_str(visitor)
        }
        fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            const V: u32 = 0x0f0f0f0f;
            let v: &[u8; 4] = unsafe { std::mem::transmute(&V) };
            visitor.visit_borrowed_bytes(v)
        }
    }
    impl<'de, 'b: 'de> Valuable<'de, 'b> for Value<'b> {
        fn as_borrowed_value(&self) -> BorrowedValue<'b> {
            todo!()
        }

        fn into_owned_value(self) -> crate::Value {
            todo!()
        }

        fn is_null(&self) -> bool {
            false
        }

        fn ty(&self) -> Ty {
            Ty::VarChar
        }
    }

    #[derive(Debug)]
    struct ResultSet;

    impl IntoIterator for ResultSet {
        type Item = Block;

        type IntoIter = BlocksIter;

        fn into_iter(self) -> Self::IntoIter {
            BlocksIter
        }
    }

    #[derive(Debug)]
    struct BlocksIter;
    impl Iterator for BlocksIter {
        type Item = Block;
        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };
                Some(Block)
            } else {
                None
            }
        }
    }

    #[derive(Debug)]
    struct Block;

    impl<'de, 'b: 'de> BlockExt<'de, 'b> for Block {
        type Value = Value<'b>;
        fn num_of_rows(&self) -> u32 {
            todo!()
        }

        fn fields(&self) -> Cow<[Field]> {
            todo!()
        }

        type Col = Col<'b>;

        type ColIter = ColsIter<'b>;

        fn iter_cols(&self) -> Self::ColIter {
            ColsIter(PhantomData)
        }

        type Row = Row<'b>;

        type RowIter = RowIter<'b>;

        fn iter_rows(&self) -> Self::RowIter {
            RowIter(PhantomData)
        }

        fn write_with(&self, codec: CodecOpts) {
            todo!()
        }

        fn write_all_with(&self, codec: CodecOpts) {
            todo!()
        }
    }

    #[derive(Debug)]
    struct Row<'a>(PhantomData<&'a u8>);
    impl<'a> Iterator for Row<'a> {
        type Item = (&'a Field, Value<'a>);

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: usize = 0;
            static mut FIELD: Option<Field> = None;
            unsafe {
                if FIELD.is_none() {
                    FIELD = Some(Field::new("name", Ty::Int, 4));
                }
            }
            if unsafe { AVAILABLE } < 3 {
                unsafe { AVAILABLE += 1 };
                Some((unsafe { FIELD.as_ref().unwrap() }, Value("s")))
            } else {
                None
            }
        }
    }

    #[derive(Debug)]
    struct RowIter<'a>(PhantomData<&'a u8>);
    impl<'a> Iterator for RowIter<'a> {
        type Item = Row<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };
                Some(Row(PhantomData))
            } else {
                None
            }
        }
    }

    #[derive(Debug)]
    struct Col<'a>(PhantomData<&'a Block>);

    struct IntoValues<'a>(PhantomData<&'a Block>);
    impl<'a> Iterator for IntoValues<'a> {
        type Item = Value<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };
                Some(Value("s"))
            } else {
                None
            }
        }
    }
    impl<'a> IntoIterator for Col<'a> {
        type Item = Value<'a>;

        type IntoIter = IntoValues<'a>;

        fn into_iter(self) -> Self::IntoIter {
            IntoValues(PhantomData)
        }
    }

    #[derive(Debug)]
    struct ColsIter<'a>(PhantomData<&'a Block>);
    impl<'a> Iterator for ColsIter<'a> {
        type Item = Col<'a>;

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };
                Some(Col(PhantomData))
            } else {
                None
            }
        }
    }

    impl<'q, 'de, 'b: 'de> crate::ResultSet<'q, 'de, 'b> for ResultSet {
        type B = Block;

        type I = BlocksIter;

        fn fields(&'q self) -> &[Field] {
            todo!()
        }

        fn next_block(&'q self) -> Option<Self::B> {
            todo!()
        }

        fn block_iter(&self) -> Self::I {
            BlocksIter
        }
    }

    #[derive(Debug)]
    struct Error;

    impl Queryable for Conn {
        type Error = anyhow::Error;

        type ResultSet = ResultSet;

        fn query<T: AsRef<str>>(
            &self,
            sql: T,
        ) -> Result<Result<Self::ResultSet, usize>, Self::Error> {
            Ok(Ok(ResultSet))
        }

        fn exec<T: AsRef<str>>(&self, sql: T) -> Result<usize, Self::Error> {
            Ok(1)
        }
    }
    #[test]
    fn query_deserialize() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let res = conn.query("abc").unwrap();

        match res {
            Ok(set) => {
                use crate::ResultSet;
                for record in set.deserialize::<(i32, String, u8)>() {
                    dbg!(record.unwrap());
                }
            }
            Err(n) => {
                // A `exec` query is not queryable.
                println!("affected rows: {}", n);
            }
        }
    }
    #[test]
    fn block_deserialize_borrowed() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let res = conn.query("abc").unwrap();

        match res {
            Ok(set) => {
                for block in set {
                    for record in block.deserialize::<(i32, &str, u8)>() {
                        dbg!(record.unwrap());
                    }
                }
            }
            Err(n) => {
                // A `exec` query is not queryable.
                println!("affected rows: {}", n);
            }
        }
    }
    #[test]
    fn block_deserialize_borrowed_bytes() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let res = conn.query("abc").unwrap();

        match res {
            Ok(set) => {
                for block in set {
                    for record in block.deserialize::<(&[u8], &str, u8)>() {
                        dbg!(record.unwrap());
                    }
                }
            }
            Err(n) => {
                // A `exec` query is not queryable.
                println!("affected rows: {}", n);
            }
        }
    }

    #[test]
    fn with_iter() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let res = conn.query("abc").unwrap();

        match res {
            Ok(set) => {
                for block in set {
                    // todo
                    for row in block.iter_rows() {
                        for value in row {
                            println!("{:?}", value);
                        }
                    }
                    for row in block.iter_cols() {
                        for value in row {
                            println!("{:?}", value);
                        }
                    }
                }
            }
            Err(n) => {
                // A `exec` query is not queryable.
                println!("affected rows: {}", n);
            }
        }
    }
}
