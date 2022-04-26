//! This is the common query traits/types for TDengine connectors.
//!

use std::{borrow::Cow, cell::Cell, fmt::Debug, marker::PhantomData, rc::Rc, sync::Arc};

pub mod common;
mod de;
pub mod helpers;
mod insert;

use async_trait::async_trait;
use common::*;
use helpers::*;

pub enum CodecOpts {
    Raw,
    Parquet,
}

pub trait BlockCodec {
    fn encode(&self, _codec: CodecOpts) -> Vec<u8>;
    fn decode(from: &[u8], _codec: CodecOpts) -> Self;
}

pub struct CellIter<'b, T: BlockExt> {
    block: &'b T,
    row: usize,
    col: usize,
}

impl<'b, T: BlockExt> Iterator for CellIter<'b, T> {
    type Item = (&'b Field, BorrowedValue<'b>);

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
pub struct RowInBlock<'b, T: BlockExt> {
    block: &'b T,
    row: usize,
}

impl<'b, T> IntoIterator for RowInBlock<'b, T>
where
    T: BlockExt,
{
    type Item = (&'b Field, BorrowedValue<'b>);

    type IntoIter = CellIter<'b, T>;

    fn into_iter(self) -> Self::IntoIter {
        CellIter {
            block: self.block,
            row: self.row,
            col: 0,
        }
    }
}

pub struct RowsIter<'b, T: BlockExt> {
    block: &'b T,
    row: usize,
}

impl<'b, T> Iterator for RowsIter<'b, T>
where
    T: BlockExt,
{
    type Item = RowInBlock<'b, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.row;

        if row < self.block.num_of_rows() {
            self.row += 1;
            Some(RowInBlock {
                block: self.block,
                row,
            })
        } else {
            None
        }
    }
}

pub struct IntoRowsIter<T: BlockExt> {
    block: Rc<T>,
    row: Cell<usize>,
}

impl<'b, T> Iterator for &'b IntoRowsIter<T>
where
    T: BlockExt,
{
    type Item = RowInBlock<'b, T>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.row.get();

        if row < self.block.num_of_rows() {
            self.row.replace(row + 1);
            Some(RowInBlock {
                block: &self.block,
                row,
            })
        } else {
            None
        }
    }
}

pub struct ColsIter<'b, T: BlockExt> {
    block: &'b T,
    col: usize,
}

impl<'b, T> Iterator for ColsIter<'b, T>
where
    T: BlockExt,
{
    type Item = BorrowedColumn<'b>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.col >= self.block.field_count() {
            return None;
        }

        let v = unsafe { self.block.get_col_unchecked(self.col) };
        self.col += 1;
        Some(v)
    }
}

type DeserializeIter<'b, B, T> =
    std::iter::Map<RowsIter<'b, B>, fn(RowInBlock<'b, B>) -> Result<T, serde::de::value::Error>>;

pub trait BlockExt: Debug + Sized {
    /// A block should container number of rows.
    fn num_of_rows(&self) -> usize;

    /// Fields can be queried from a block.
    fn fields(&self) -> &[Field];

    fn precision(&self) -> Precision;

    fn is_null(&self, row: usize, col: usize) -> bool;

    /// Get field without column index check.
    ///
    /// # Safety
    ///
    /// This should not be called manually, please use [get_field](#method.get_field).
    unsafe fn get_field_unchecked(&self, col: usize) -> &Field {
        self.fields().get_unchecked(col)
    }

    /// Get field of one column.
    fn get_field(&self, col: usize) -> Option<&Field> {
        self.fields().get(col)
    }

    /// Number of fields.
    fn field_count(&self) -> usize {
        self.fields().len()
    }

    /// # Safety
    ///
    /// **DO NOT** call it directly.
    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, BorrowedValue);

    unsafe fn get_col_unchecked(&self, col: usize) -> BorrowedColumn;

    /// Query by rows.
    fn iter_rows(&self) -> RowsIter<'_, Self> {
        RowsIter {
            block: self,
            row: 0,
        }
    }

    fn into_iter_rows(self) -> IntoRowsIter<Self> {
        IntoRowsIter {
            block: Rc::new(self),
            row: Cell::new(0),
        }
    }

    fn columns_iter(&self) -> ColsIter<'_, Self> {
        ColsIter {
            block: self,
            col: 0,
        }
    }

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize<'b, T>(&'b self) -> DeserializeIter<'b, Self, T>
    where
        T: serde::de::Deserialize<'b>,
    {
        self.iter_rows().map(|row| {
            let de = de::RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }
    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize_owned<T>(&self) -> DeserializeIter<'_, Self, T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.iter_rows().map(|row| {
            let de = de::RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }

    fn to_stream(&self) -> futures::stream::Iter<RowsIter<'_, Self>> {
        futures::stream::iter(Self::iter_rows(&self))
    }

    fn deserialize_stream<'b, T>(&'b self) -> futures::stream::Iter<DeserializeIter<'b, Self, T>>
    where
        T: serde::de::Deserialize<'b>,
    {
        futures::stream::iter(Self::deserialize(&self))
    }

    fn deserialize_into_vec<T>(self) -> Vec<Result<T, serde::de::value::Error>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.into_iter_rows()
            .map(|row| {
                let de = de::RecordDeserializer::from(row);
                T::deserialize(de)
            })
            .collect()
    }
}

pub trait ResultSet
where
    // for<'b> <Self::Block as BlockExt>::Value: Valuable<'b>,
    Self: Sized,
    for<'r> &'r mut Self: Iterator,
    for<'b, 'r> <&'r mut Self as Iterator>::Item: BlockExt,
{
    // type Block: for<'b> BlockExt;

    fn fields(&self) -> &[Field];

    fn precision(&self) -> Precision;

    fn num_of_fields(&self) -> usize {
        self.fields().len()
    }

    fn summary(&self) -> (usize, usize);

    // fn fetch_block(&mut self) -> Option<<&mut Self as Iterator>Block>;

    fn blocks_iter(&mut self) -> &mut Self {
        self
    }

    fn deserialize<T>(
        &mut self,
    ) -> std::iter::FlatMap<
        &mut Self,
        Vec<Result<T, serde::de::value::Error>>,
        fn(<&mut Self as Iterator>::Item) -> Vec<Result<T, serde::de::value::Error>>,
    >
    where
        T: serde::de::DeserializeOwned,
    {
        // self.blocks_iter()
        self.flat_map(|block| block.deserialize_owned::<T>().collect())
    }
}

/// The synchronous query trait for TDengine connection.
pub trait Queryable<'q>: Debug
where
    for<'r> &'r mut Self::ResultSet: Iterator,
    for<'b, 'r> <&'r mut Self::ResultSet as Iterator>::Item: BlockExt,
{
    type Error: Debug + From<serde::de::value::Error>;
    // type B: for<'b> BlockExt<'b, 'b>;
    type ResultSet: ResultSet;

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
            .map(|mut r| r.deserialize().try_collect())
            .expect("`show databases` must be queryable")
            .map_err(Into::into)
    }
}

pub trait AsyncResultSet: Send
where
    Self::BlockStream: futures::stream::Stream + Send,
    for<'b> <Self::BlockStream as futures::stream::Stream>::Item: BlockExt + Send,
{
    type BlockStream;
    // type Block: for<'b> BlockExt;

    fn fields(&self) -> &[Field];

    fn precision(&self) -> Precision;

    fn num_of_fields(&self) -> usize {
        self.fields().len()
    }

    fn summary(&self) -> (usize, usize);

    // fn fetch_block(&mut self) -> Option<<&mut Self as Iterator>Block>;

    fn block_stream(&self) -> Self::BlockStream;

    fn deserialize_stream<'a, T>(
        &'a mut self,
    ) -> futures::stream::FlatMap<
        <Self as AsyncResultSet>::BlockStream,
        futures::stream::Iter<std::vec::IntoIter<Result<T, serde::de::value::Error>>>,
        fn(
            <Self::BlockStream as futures::stream::Stream>::Item,
        )
            -> futures::stream::Iter<std::vec::IntoIter<Result<T, serde::de::value::Error>>>,
    >
    where
        T: serde::de::DeserializeOwned,
    {
        // self.blocks_iter()
        use futures::stream::StreamExt;
        self.block_stream()
            .flat_map(|block| futures::stream::iter(block.deserialize_into_vec::<T>()))
    }
}

/// The synchronous query trait for TDengine connection.
#[async_trait]
pub trait AsyncQueryable<'q>: Send + Sync
where
    <Self::AsyncResultSet as AsyncResultSet>::BlockStream: 'q + futures::stream::Stream,
    for<'b> <<Self::AsyncResultSet as AsyncResultSet>::BlockStream as futures::stream::Stream>::Item:
        BlockExt + Send,
{
    type Error: Debug + From<serde::de::value::Error> + From<anyhow::Error> + Send;
    // type B: for<'b> BlockExt<'b, 'b>;
    type AsyncResultSet: AsyncResultSet;

    async fn query<T: AsRef<str> + Send>(
        &'q self,
        sql: T,
    ) -> Result<Result<Self::AsyncResultSet, usize>, Self::Error>;

    async fn exec<T: AsRef<str> + Send>(&'q self, sql: T) -> Result<usize, Self::Error> {
        self.query(sql).await.map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }
    async fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
        use futures::stream::TryStreamExt;
        Ok(self
            .query("show databases")
            .await?
            .expect("`show databases` must be queryable")
            .deserialize_stream()
            .try_collect()
            .await?)
    }
    async fn describe(&'q self, table: &str) -> Result<Vec<ColumnMeta>, Self::Error> {
        use futures::stream::TryStreamExt;
        Ok(self
            .query(format!("describe {table}"))
            .await?
            .expect("`show databases` must be queryable")
            .deserialize_stream()
            .try_collect()
            .await?)
    }

    fn exec_sync<T: AsRef<str> + Send>(&'q self, sql: T) -> Result<usize, Self::Error> {
        futures::executor::block_on(self.exec(sql))
    }

    fn query_sync<T: AsRef<str> + Send>(
        &'q self,
        sql: T,
    ) -> Result<Result<Self::AsyncResultSet, usize>, Self::Error> {
        futures::executor::block_on(self.query(sql))
    }
}

// pub trait AsyncQueryableSync<'q>: AsyncQueryable<'q>
// where
//     <Self::AsyncResultSet as AsyncResultSet>::BlockStream: 'q + futures::stream::Stream,
//     for<'b> <<Self::AsyncResultSet as AsyncResultSet>::BlockStream as futures::stream::Stream>::Item:
//         BlockExt + Send,
// {
//     fn exec_sync<T: AsRef<str> + Send>(&'q self, sql: T) -> Result<usize, Self::Error> {
//         futures::executor::block_on(self.exec(sql))
//     }

//     fn query_sync<T: AsRef<str> + Send>(
//         &'q self,
//         sql: T,
//     ) -> Result<Result<Self::AsyncResultSet, usize>, Self::Error> {
//         futures::executor::block_on(self.query(sql))
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

    #[derive(Debug)]
    struct MyResultSet<'q>(PhantomData<(&'q u8)>);

    #[derive(Debug)]
    struct Block<'r, 'q>(PhantomData<(&'r u8, &'q u8)>);

    impl<'b, 'r, 'q> BlockExt for Block<'r, 'q> {
        fn num_of_rows(&self) -> usize {
            1
        }

        fn fields(&self) -> &[Field] {
            static mut FIELDS: Vec<Field> = Vec::new();
            unsafe {
                if FIELDS.len() == 0 {
                    FIELDS.push(Field::new("ts", Ty::Timestamp, 8));
                    FIELDS.push(Field::new("bin10", Ty::VarChar, 10));
                    FIELDS.push(Field::new("int32", Ty::Int, 4));
                }
                &FIELDS
            }
        }

        fn precision(&self) -> Precision {
            Precision::Microsecond
        }

        fn is_null(&self, _row: usize, _col: usize) -> bool {
            false
        }

        fn field_count(&self) -> usize {
            3
        }

        unsafe fn cell_unchecked(&self, _row: usize, col: usize) -> (&Field, BorrowedValue) {
            match col {
                0 => (
                    self.get_field_unchecked(col) as _,
                    BorrowedValue::Timestamp(crate::Timestamp::Milliseconds(0)),
                ),
                2 => (self.get_field_unchecked(col) as _, BorrowedValue::Int(32)),
                1 => (
                    self.get_field_unchecked(col) as _,
                    BorrowedValue::VarChar("str"),
                ),
                _ => (self.get_field_unchecked(col) as _, BorrowedValue::Int(32)),
            }
        }

        unsafe fn get_col_unchecked(&self, col: usize) -> BorrowedColumn {
            todo!()
        }
    }

    impl<'r, 'q> Iterator for &'r mut MyResultSet<'q> {
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

    impl<'r, 'q> crate::ResultSet for MyResultSet<'q> {
        fn fields(&self) -> &[Field] {
            todo!()
        }

        fn precision(&self) -> Precision {
            todo!()
        }

        fn summary(&self) -> (usize, usize) {
            todo!()
        }
    }

    #[derive(Debug)]
    struct Error;

    impl<'q> Queryable<'q> for Conn {
        type Error = anyhow::Error;

        type ResultSet = MyResultSet<'q>;

        fn query<T: AsRef<str>>(
            &'q self,
            _sql: T,
        ) -> Result<Result<MyResultSet, usize>, Self::Error> {
            Ok(Ok(MyResultSet(PhantomData)))
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
                for record in s.deserialize::<(i32, String, u8)>() {
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
                    for record in block.deserialize::<(String, &str, u8)>() {
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
    #[tokio::test]
    async fn block_deserialize_borrowed_bytes_stream() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let res = conn.query("abc").unwrap();

        use futures::stream::*;

        match res {
            Ok(mut set) => {
                for block in &mut set {
                    for record in block
                        .deserialize_stream::<(String, &str, u8)>()
                        .next()
                        .await
                    {
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
