//! This is the common query traits/types for TDengine connectors.
//!

use std::fmt::Debug;
use std::iter::FlatMap;
use std::marker::PhantomData;

pub mod common;
mod de;
pub mod helpers;
mod insert;

use common::*;
use de::RecordDeserializer;
use helpers::*;

pub enum CodecOpts {
    Raw,
    Parquet,
}

pub trait Valuable2<'b>: serde::de::Deserializer<'b> {
    /// Check if the value is null or not.
    fn is_null(&self) -> bool;

    /// Sql type of the value
    fn ty(&self) -> Ty;

    /// Borrowed value.
    fn as_borrowed_value(&self) -> BorrowedValue<'b>;

    /// Owned value.
    fn into_owned_value(self) -> Value;
}

pub struct CellIter<'b, T: BlockExt2<'b>> {
    block: &'b T,
    row: usize,
    col: usize,
}

impl<'b, T: BlockExt2<'b>> Iterator for CellIter<'b, T> {
    type Item = (&'b Field, T::Value);

    fn next(&mut self) -> Option<Self::Item> {
        let col = self.col;
        if col < self.block.field_count() {
            self.col += 1;
            Some(unsafe { self.block.cell_unchecked(self.row, col) })
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Row2<'b, T: BlockExt2<'b>> {
    block: &'b T,
    row: usize,
}

impl<'b, T> IntoIterator for Row2<'b, T>
where
    T: BlockExt2<'b>,
{
    type Item = (&'b Field, T::Value);

    type IntoIter = CellIter<'b, T>;

    fn into_iter(self) -> Self::IntoIter {
        CellIter {
            block: self.block,
            row: self.row,
            col: 0,
        }
    }
}

pub struct RowIter2<'b, T: BlockExt2<'b>> {
    block: &'b T,
    row: usize,
}

impl<'b, T> Iterator for RowIter2<'b, T>
where
    T: BlockExt2<'b>,
{
    type Item = Row2<'b, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.row;

        if row < self.block.num_of_rows() {
            self.row += 1;
            Some(Row2 {
                block: self.block,
                row,
            })
        } else {
            None
        }
    }
}

pub trait BlockExt2<'b>: Debug + Sized
where
    Self::Value: Valuable2<'b>,
{
    /// A block should container number of rows.
    fn num_of_rows(&self) -> usize;

    /// Fields can be queried from a block.
    fn fields(&self) -> &[Field];

    fn precision(&self) -> Precision;

    fn is_null(&self, row: usize, col: usize) -> bool;

    /// Get field without column index check.
    unsafe fn get_field_unchecked(&self, col: usize) -> &Field {
        self.fields().get_unchecked(col)
    }

    /// Get field of one column.
    unsafe fn get_field(&self, col: usize) -> Option<&Field> {
        self.fields().get(col)
    }

    /// Number of fields.
    fn field_count(&self) -> usize {
        self.fields().len()
    }

    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, Self::Value);

    type Value;

    /// Query by rows.
    fn iter_rows(&'b self) -> RowIter2<'b, Self> {
        RowIter2 {
            block: self,
            row: 0,
        }
    }

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize<T>(
        &'b self,
    ) -> std::iter::Map<RowIter2<'b, Self>, fn(Row2<'b, Self>) -> Result<T, serde::de::value::Error>>
    where
        T: serde::de::Deserialize<'b>,
    {
        self.iter_rows().map(|row| {
            let de = de::de2::RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }
    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize_owned<T>(
        &'b self,
    ) -> std::iter::Map<RowIter2<'b, Self>, fn(Row2<'b, Self>) -> Result<T, serde::de::value::Error>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.iter_rows().map(|row| {
            let de = de::de2::RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }
}

pub struct BlockIter2<'i, 'r, R: Rs2<'r>>(&'i mut R, PhantomData<&'r u8>);

impl<'i, 'r, R> Iterator for BlockIter2<'i, 'r, R>
where
    R: Rs2<'r>,
{
    type Item = R::Block;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.fetch_block()
    }
}

pub trait Rs2<'r>
where
    for<'b> <Self::Block as BlockExt2<'b>>::Value: Valuable2<'b>,
    Self: Sized,
{
    type Block: for<'b> BlockExt2<'b> + 'r;

    fn fields(&self) -> &[Field];

    fn precision(&self) -> Precision;

    fn num_of_fields(&self) -> usize {
        self.fields().len()
    }

    fn summary(&self) -> (usize, usize);

    fn fetch_block(&mut self) -> Option<Self::Block>;

    fn block_iter(&mut self) -> BlockIter2<'_, 'r, Self> {
        BlockIter2(self, PhantomData)
    }

    fn deserialize2<'i, T>(
        &'i mut self,
    ) -> std::iter::FlatMap<
        BlockIter2<'i, 'r, Self>,
        Vec<Result<T, serde::de::value::Error>>,
        fn(Self::Block) -> Vec<Result<T, serde::de::value::Error>>,
    >
    where
        T: serde::de::DeserializeOwned,
    {
        self.block_iter()
            .flat_map(|block| block.deserialize_owned::<T>().collect())
    }
}
pub trait Queryable2<'r, 'q>: Debug {
    type Error: Debug + From<serde::de::value::Error>;
    // type B: for<'b> BlockExt<'b, 'b>;
    type ResultSet: Rs2<'r>;

    fn query<T: AsRef<str>>(
        &'q self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    fn exec<T: AsRef<str>>(&'q self, sql: T) -> Result<usize, Self::Error> {
        self.query(sql).map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }
    fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
        use itertools::Itertools;
        self.query("show databases")?
            .expect("`show databases` must be queryable")
            .deserialize2()
            .try_collect()
            .map_err(Into::into)
    }
}

/// A result gained from query lifetime(`'q`), and will produce a block iterator with
/// sub lifetime called `'b`(means block).
pub trait ResultSetExt2<'r>: 'r + Sized // where
// Self::B: 'b + BlockExt<'de, 'b>,
{
    fn fields(&'r self) -> &'r [Field];

    fn precision(&'r self) -> Precision;

    fn num_of_fields(&'r self) -> usize {
        self.fields().len()
    }

    fn summary(&self) -> (usize, usize);
}

// pub trait ResultDeserialize<'r, 'b>: IntoIterator
// where
//     &'r Self::Item: BlockDeserialize<'b> + 'r,
//     Self::Item: 'r,
//     <&'r Self::Item as IntoIterator>::Item:
//         IntoIterator<Item = (&'b Field, <&'r Self::Item as BlockDeserialize<'b>>::Value)> + 'r,
//     <&'r Self::Item as BlockDeserialize<'b>>::Value: Valuable2<'b>,

//     Self: Sized,
// {
//     #[allow(clippy::type_complexity)]
//     fn deserialize_owned<T>(
//         self,
//     ) -> FlatMap<
//         Self::IntoIter,
//         Vec<Result<T, serde::de::value::Error>>,
//         fn(Self::Item) -> Vec<Result<T, serde::de::value::Error>>,
//     >
//     where
//         T: serde::de::DeserializeOwned,
//     {
//         self.into_iter()
//             .flat_map(|b| <&'r Self::Item as BlockDeserialize>::deserialize(&b).collect_vec())
//     }
// }

// pub trait RsDeserialize<'b, 'r: 'b>: IntoIterator + Sized
// where
//     Self::Item: BlockExt2<'b>,
// {
//     #[allow(clippy::type_complexity)]
//     fn deserialize<T>(
//         self,
//     ) -> FlatMap<
//         Self::IntoIter,
//         Vec<Result<T, serde::de::value::Error>>,
//         fn(Self::Item) -> Vec<Result<T, serde::de::value::Error>>,
//     >
//     where
//         T: serde::de::DeserializeOwned,
//     {
//         self.into_iter()
//             .flat_map(|b| <Self::Item as BlockExt2>::deserialize(&b).collect_vec())
//     }
// }
// pub trait Queryable2<'b, 'r: 'b, 'q>: Debug
// where
//     &'r mut Self::ResultSet: IntoIterator,
//     &'r mut Self::ResultSet: RsDeserialize<'b, 'r>,
//     // <&'r mut Self::ResultSet as IntoIterator>::IntoIter: RsDeserialize<'b, 'r>,
//     <&'r mut Self::ResultSet as IntoIterator>::Item: BlockExt2<'b>,
// {
//     type Error: Debug + From<serde::de::value::Error>;
//     // type B: for<'b> BlockExt<'b, 'b>;
//     type ResultSet: ResultSetExt2<'r>;

//     fn query<T: AsRef<str>>(
//         &'q self,
//         sql: T,
//     ) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

//     fn exec<T: AsRef<str>>(&'q self, sql: T) -> Result<usize, Self::Error> {
//         self.query(sql).map(|res| match res {
//             Ok(_) => 0, // todo: if we should get the selected rows if not update query?
//             Err(affected) => affected,
//         })
//     }
//     //     fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
//     //         use itertools::Itertools;
//     //         self.query("show databases")?
//     //             .expect("`show databases` must be queryable")
//     //             .deserialize_owned()
//     //             .try_collect()
//     //             .map_err(Into::into)
//     //     }
// }

//     fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
//         use itertools::Itertools;
//         self.query("show databases")?
//             .expect("`show databases` must be queryable")
//             .deserialize_owned()
//             .try_collect()
//             .map_err(Into::into)
//     }

//     fn describe(&'q self, table: &str) -> Result<Vec<ColumnMeta>, Self::Error> {
//         use itertools::Itertools;
//         self.query(format!("describe {}", table))?
//             .expect("`describe <table>` must be queryable")
//             .deserialize_owned()
//             .try_collect()
//             .map_err(Into::into)
//     }

//     fn create_database<I: Into<DatabaseProperties>>(
//         &'q self,
//         name: &str,
//         opts: I,
//     ) -> Result<(), Self::Error> {
//         let sql = format!("create database {} if not exists {}", name, opts.into());
//         self.exec(&sql).map(|_| ())
//     }

//     fn use_database(&'q self, database: &str) -> Result<(), Self::Error> {
//         let sql = format!("use database {}", database);
//         self.exec(&sql).map(|_| ())
//     }

//     fn create_table(&'q self, name: &str) -> Result<(), Self::Error> {
//         let sql = format!("create table {}", name);
//         self.exec(&sql).map(|_| ())
//     }
// }

/// A value will borrow data from a block, so there's a `'b` lifetime bound here.
/// Here &self lifetime is hidden, should named as 'v (value) if once visible.
pub trait Valuable<'de, 'b: 'de, 'r: 'b, 'q: 'r>: serde::de::Deserializer<'de> {
    /// Check if the value is null or not.
    fn is_null(&self) -> bool;

    /// Sql type of the value
    fn ty(&self) -> Ty;

    /// Borrowed value.
    fn as_borrowed_value(&self) -> BorrowedValue<'b>;

    /// Owned value.
    fn into_owned_value(self) -> Value;
}

/// A field bounded to 'b block lifetime.
pub type ValueEntry<'b, T> = (&'b Field, T);

// pub trait ResultBasic {
//     /// Fields can be queried from a block.
//     fn fields(&self) -> &[Field];
//     fn precision(&self) -> Precision;
//     fn fetched_rows(&self) -> u64;

//     /// Get field without column index check.
//     unsafe fn get_field_unchecked(&self, col: usize) -> &Field {
//         self.fields().get_unchecked(col)
//     }

//     /// Get field of one column.
//     unsafe fn get_field(&self, col: usize) -> Option<&Field> {
//         self.fields().get(col)
//     }

//     /// Number of fields.
//     fn filed_count(&self) -> usize {
//         self.fields().len()
//     }
// }

// pub trait Fetchable<'r>
// where
//     &'r mut Self: Iterator,
//     Self: 'r + ResultBasic,
// {
// }

/// Define what a data block provides.
pub trait BlockExt<'de, 'b: 'de, 'r: 'b, 'q: 'r>: Debug + Sized + 'r
// where
//     &'b Self::Row: IntoIterator<Item = (&'b Field, Self::Value)>,
//     Self::Row: 'b,
{
    /// A block should container number of rows.
    fn num_of_rows(&'b self) -> u32;

    /// Fields can be queried from a block.
    fn fields(&'b self) -> &'b [Field];

    /// Get field without column index check.
    unsafe fn get_field_unchecked(&'b self, col: usize) -> &'b Field {
        self.fields().get_unchecked(col)
    }

    /// Get field of one column.
    unsafe fn get_field(&'b self, col: usize) -> Option<&'b Field> {
        self.fields().get(col)
    }

    /// Number of fields.
    fn filed_count(&'b self) -> usize {
        self.fields().len()
    }

    // type Col;
    // type ColIter: Iterator<Item = Self::Col>;
    // /// Query by columns.
    // fn iter_cols(&self) -> Self::ColIter;

    type Value: Valuable<'de, 'b, 'r, 'q>;

    type Row: IntoIterator<Item = (&'b Field, Self::Value)>;
    type RowIter: Iterator<Item = Self::Row>;

    /// Query by rows.
    fn iter_rows(&'b self) -> Self::RowIter;

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize<T>(
        &'b self,
    ) -> std::iter::Map<Self::RowIter, fn(Self::Row) -> Result<T, serde::de::value::Error>>
    where
        T: serde::de::Deserialize<'de>,
    {
        self.iter_rows().map(|row| {
            let de = RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }
    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize_owned<T>(
        &'b self,
    ) -> std::iter::Map<Self::RowIter, fn(Self::Row) -> Result<T, serde::de::value::Error>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.iter_rows().map(|row| {
            let de = RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }
    fn encode(&self, _codec: CodecOpts) -> Vec<u8>;

    fn write_with(&self, _codec: CodecOpts);

    fn write_all_with(&self, _codec: CodecOpts);
}

pub trait BlockCodec {
    fn encode(&self, _codec: CodecOpts) -> Vec<u8>;
    fn decode(from: &[u8], _codec: CodecOpts) -> Self;
}

// pub trait RsBase {
//     fn fields(&self) -> &[Field];

//     fn precision(&self) -> Precision;

//     fn num_of_fields(&self) -> usize {
//         self.fields().len()
//     }

//     fn summary(&self) -> (usize, usize);
// }

/// A result gained from query lifetime(`'q`), and will produce a block iterator with
/// sub lifetime called `'b`(means block).
pub trait ResultSetExt<'de, 'b: 'de, 'r: 'b, 'q: 'r>: 'r + Sized
// where
// Self::B: 'b + BlockExt<'de, 'b>,
{
    fn fields(&'r self) -> &'r [Field];

    fn precision(&'r self) -> Precision;

    fn num_of_fields(&'r self) -> usize {
        self.fields().len()
    }

    fn summary(&self) -> (usize, usize);

    type B: BlockExt<'de, 'b, 'r, 'q>;
    type I: Iterator<Item = Self::B>;

    fn block_iter(&'r mut self) -> Self::I;

    #[allow(clippy::type_complexity)]
    fn deserialize_owned<T>(
        &'r mut self,
    ) -> FlatMap<
        Self::I,
        Vec<Result<T, serde::de::value::Error>>,
        fn(Self::B) -> Vec<Result<T, serde::de::value::Error>>,
    >
    where
        T: serde::de::DeserializeOwned,
    {
        todo!()
        // self.block_iter()
        //     .flat_map(|b| <Self::B as BlockExt>::deserialize(&b).collect_vec())
    }
}

// pub trait RsDeserialize<'r, 'q: 'r>: IntoIterator + Sized + 'r
// where
//     Self::Item: for<'de, 'b> BlockExt<'de, 'b, 'r, 'q>,
// {
//     #[allow(clippy::type_complexity)]
//     fn deserialize<'b, T>(
//         self,
//     ) -> FlatMap<
//         Self::IntoIter,
//         Vec<Result<T, serde::de::value::Error>>,
//         fn(Self::Item) -> Vec<Result<T, serde::de::value::Error>>,
//     >
//     where
//         T: serde::de::DeserializeOwned,
//     {
//         self.into_iter()
//             .flat_map(|b| <Self::Item as BlockExt>::deserialize_owned(&b).collect_vec())
//     }
// }

// pub trait RsDe<'r, B>
// where
//     Self: Sized + 'r,
//     B: for<'b> BlockExt<'b>,
//     &'r mut Self: for<'b> IntoIterator<Item = B> + 'b,
// {
//     #[allow(clippy::type_complexity)]
//     fn deserialize<'b, T>(
//         &'r mut self,
//     ) -> FlatMap<
//         <&'r mut Self as IntoIterator>::IntoIter,
//         II<'b, <&'r mut Self as IntoIterator>::Item, T>,
//         fn(
//             <&'r mut Self as IntoIterator>::Item,
//         ) -> II<'b, <&'r mut Self as IntoIterator>::Item, T>,
//     >
//     where
//         T: serde::de::DeserializeOwned,
//     {
//         self.into_iter().flat_map(|b| {
//             <<&'r mut Self as IntoIterator>::Item as BlockExt<'b>>::deserialize(&b)
//         })
//     }
// }

// pub trait QueryHelperDe<'r>: Queryable
// where
//     &'r mut Self::ResultSet: for<'b> RsDeserialize + 'r,
//     <&'r mut Self::ResultSet as IntoIterator>::Item: for<'b> BlockExt<'b>,
// {
//     fn databases<'b>(&'r self) -> Result<Vec<ShowDatabase>, Self::Error> {
//         use itertools::Itertools;
//         self
//             .query("show databases")?
//             .expect("`show databases` must be queryable")
//             .deserialize()
//             .try_collect()
//             .map_err(Into::into)
//     }

//     fn describe(&self, table: &str) -> Result<Vec<ColumnMeta>, Self::Error> {
//         use itertools::Itertools;
//         self.query(format!("describe {}", table))?
//             .expect("`describe <table>` must be queryable")
//             .deserialize_owned()
//             .try_collect()
//             .map_err(Into::into)
//     }
// }

pub trait Queryable<'de, 'b: 'de, 'r: 'b, 'q: 'r>: Debug
// where
//     for<'q> &'q mut Self::ResultSet: IntoIterator<Item = Self::B>,
{
    type Error: Debug + From<serde::de::value::Error>;
    // type B: for<'b> BlockExt<'b, 'b>;
    type ResultSet: ResultSetExt<'de, 'b, 'r, 'q>;

    fn query<T: AsRef<str>>(
        &'q self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    fn exec<T: AsRef<str>>(&'q self, sql: T) -> Result<usize, Self::Error> {
        self.query(sql).map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    fn create_database<I: Into<DatabaseProperties>>(
        &'q self,
        name: &str,
        opts: I,
    ) -> Result<(), Self::Error> {
        let sql = format!("create database {} if not exists {}", name, opts.into());
        self.exec(&sql).map(|_| ())
    }

    fn use_database(&'q self, database: &str) -> Result<(), Self::Error> {
        let sql = format!("use database {}", database);
        self.exec(&sql).map(|_| ())
    }

    fn create_table(&'q self, name: &str) -> Result<(), Self::Error> {
        let sql = format!("create table {}", name);
        self.exec(&sql).map(|_| ())
    }
}


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

    impl<'b, 's: 'b> Valuable2<'b> for Value<'s> {
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
    struct ResultSet<'q>(PhantomData<&'q u8>);

    impl<'r, 'q> IntoIterator for &'r mut ResultSet<'q> {
        type Item = Block<'r, 'q>;

        type IntoIter = BlocksIter<'r, 'q>;

        fn into_iter(self) -> Self::IntoIter {
            BlocksIter(PhantomData)
        }
    }

    #[derive(Debug)]
    struct BlocksIter<'r, 'q>(PhantomData<&'r ResultSet<'q>>);
    impl<'r, 'q> Iterator for BlocksIter<'r, 'q> {
        type Item = Block<'r, 'q>;
        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };
                Some(Block(PhantomData))
            } else {
                None
            }
        }
    }

    #[derive(Debug)]
    struct Block<'r, 'q>(PhantomData<(&'r u8, &'q u8)>);

    impl<'b, 'r, 'q> BlockExt2<'b> for Block<'r, 'q> {
        type Value = Value<'b>;
        fn num_of_rows(&self) -> usize {
            1
        }

        fn fields(&self) -> &[Field] {
            static mut FIELDS: Vec<Field> = Vec::new();
            unsafe {
                if FIELDS.len() == 0 {
                    FIELDS.push(Field::new("ts", Ty::Timestamp, 8));
                    FIELDS.push(Field::new("int32", Ty::Int, 4));
                    FIELDS.push(Field::new("bin10", Ty::VarChar, 10));
                }
                &FIELDS
            }
        }

        fn field_count(&self) -> usize {
            3
        }

        unsafe fn cell_unchecked(&self, _row: usize, col: usize) -> (&Field, Self::Value) {
            (self.get_field_unchecked(col), Value("abc"))
        }

        fn precision(&self) -> Precision {
            Precision::Microsecond
        }

        fn is_null(&self, _row: usize, _col: usize) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct Row<'b, 'r, 'q>(PhantomData<&'b Block<'r, 'q>>);
    impl<'b, 'r, 'q> Iterator for Row<'b, 'r, 'q> {
        type Item = (&'b Field, Value<'b>);

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
    struct RowIter<'b, 'r, 'q>(PhantomData<&'b Block<'r, 'q>>);
    impl<'b, 'r, 'q> Iterator for RowIter<'b, 'r, 'q> {
        type Item = Row<'b, 'r, 'q>;

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
    struct Col<'b, 'r, 'q>(PhantomData<&'b Block<'r, 'q>>);

    struct IntoValues<'b, 'r, 'q>(PhantomData<&'b Block<'r, 'q>>);
    impl<'b, 'r, 'q> Iterator for IntoValues<'b, 'r, 'q> {
        type Item = Value<'b>;

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
    impl<'b, 'r, 'q> IntoIterator for Col<'b, 'r, 'q> {
        type Item = Value<'b>;

        type IntoIter = IntoValues<'b, 'r, 'q>;

        fn into_iter(self) -> Self::IntoIter {
            IntoValues(PhantomData)
        }
    }

    #[derive(Debug)]
    struct ColsIter<'b, 'r, 'q>(PhantomData<&'b Block<'r, 'q>>);
    impl<'b, 'r, 'q> Iterator for ColsIter<'b, 'r, 'q> {
        type Item = Col<'b, 'r, 'q>;

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

    impl<'r, 'q: 'r> Rs2<'r> for ResultSet<'q> {
        fn fields(&self) -> &[Field] {
            todo!()
        }

        fn precision(&self) -> Precision {
            todo!()
        }

        fn summary(&self) -> (usize, usize) {
            todo!()
        }

        type Block = Block<'r, 'q>;

        fn fetch_block(&mut self) -> Option<Self::Block> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };

                Some(Block(PhantomData))
            } else {
                None
            }
        }
    }

    #[derive(Debug)]
    struct Error;

    impl<'r, 'q: 'r> Queryable2<'r, 'q> for Conn {
        type Error = anyhow::Error;

        type ResultSet = ResultSet<'q>;

        fn query<T: AsRef<str>>(
            &'q self,
            _sql: T,
        ) -> Result<Result<ResultSet, usize>, Self::Error> {
            Ok(Ok(ResultSet(PhantomData)))
        }

        fn exec<T: AsRef<str>>(&self, _sql: T) -> Result<usize, Self::Error> {
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
            Ok(mut set) => {
                // use crate::ResultSetExt2;
                let s = &mut set;
                for record in s.deserialize2::<(i32, String, u8)>() {
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
            Ok(mut set) => {
                for block in &mut set {
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
            Ok(mut set) => {
                for block in &mut set {
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
            Ok(mut set) => {
                for block in &mut set {
                    // todo
                    for row in block.iter_rows() {
                        for value in row {
                            println!("{:?}", value);
                        }
                    }
                    // for row in block.iter_cols() {
                    //     for value in row {
                    //         println!("{:?}", value);
                    //     }
                    // }
                }
            }
            Err(n) => {
                // A `exec` query is not queryable.
                println!("affected rows: {}", n);
            }
        }
    }
}
