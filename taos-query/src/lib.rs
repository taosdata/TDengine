//! This is the common query traits/types for TDengine connectors.
//!

use std::fmt::Debug;

pub mod common;
mod de;
pub mod helpers;
mod insert;

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

pub trait Valuable<'b>: serde::de::Deserializer<'b> {
    /// Check if the value is null or not.
    fn is_null(&self) -> bool;

    /// Sql type of the value
    fn ty(&self) -> Ty;

    /// Borrowed value.
    fn as_borrowed_value(&self) -> BorrowedValue<'b>;

    /// Owned value.
    fn into_owned_value(self) -> Value;
}

pub struct CellIter<'b, T: BlockExt<'b>> {
    block: &'b T,
    row: usize,
    col: usize,
}

impl<'b, T: BlockExt<'b>> Iterator for CellIter<'b, T> {
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
pub struct RowInBlock<'b, T: BlockExt<'b>> {
    block: &'b T,
    row: usize,
}

impl<'b, T> IntoIterator for RowInBlock<'b, T>
where
    T: BlockExt<'b>,
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

pub struct RowsIter<'b, T: BlockExt<'b>> {
    block: &'b T,
    row: usize,
}

impl<'b, T> Iterator for RowsIter<'b, T>
where
    T: BlockExt<'b>,
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

type DeserializeIter<'b, B, T> =
    std::iter::Map<RowsIter<'b, B>, fn(RowInBlock<'b, B>) -> Result<T, serde::de::value::Error>>;
pub trait BlockExt<'b>: Debug + Sized
where
    Self::Value: Valuable<'b>,
{
    type Value;
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
    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, Self::Value);

    /// Query by rows.
    fn iter_rows(&'b self) -> RowsIter<Self> {
        RowsIter {
            block: self,
            row: 0,
        }
    }

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize<T>(&'b self) -> DeserializeIter<'b, Self, T>
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
    fn deserialize_owned<T>(&'b self) -> DeserializeIter<'b, Self, T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.iter_rows().map(|row| {
            let de = de::RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }
}

// pub struct BlocksIter<'i, 'r, R: ResultSet<'r>>(&'i mut R, PhantomData<&'r u8>);

// impl<'i, 'r, R> Iterator for BlocksIter<'i, 'r, R>
// where
//     R: ResultSet<'r>,
// {
//     type Item = R::Block;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.0.fetch_block()
//     }
// }

// type FlatDeserializeIter<'i, 'r, R, T> = std::iter::FlatMap<
//     // BlocksIter<'i, 'r, R>,
//     &'i mut R,
//     Vec<Result<T, serde::de::value::Error>>,
//     fn(<R as ResultSet>::Block) -> Vec<Result<T, serde::de::value::Error>>,
// >;
pub trait ResultSet
where
    // for<'b> <Self::Block as BlockExt<'b>>::Value: Valuable<'b>,
    Self: Sized,
    for<'r> &'r mut Self: Iterator,
    for<'b, 'r> <&'r mut Self as Iterator>::Item: BlockExt<'b>,
    for<'b, 'r> <<&'r mut Self as Iterator>::Item as BlockExt<'b>>::Value: Valuable<'b>,
{
    // type Block: for<'b> BlockExt<'b>;

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

    // fn rows_iter<'b>(
    //     &mut self,
    // ) -> std::iter::FlatMap<
    //     &mut Self,
    //     RowsIter<<&mut Self as Iterator>::Item>,
    //     fn(<&mut Self as Iterator>::Item) -> RowsIter<<&mut Self as Iterator>::Item>,
    // > {
    //     self.flat_map(|block| block.iter_rows())
    // }

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
    for<'b, 'r> <&'r mut Self::ResultSet as Iterator>::Item: BlockExt<'b>,
    for<'b, 'r> <<&'r mut Self::ResultSet as Iterator>::Item as BlockExt<'b>>::Value: Valuable<'b>,
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

    impl<'b, 's: 'b> Valuable<'b> for Value<'s> {
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
    struct MyResultSet<'q>(PhantomData<(&'q u8)>);

    // impl<'i, 'r: 'i, 'q: 'r> IntoIterator for &'i mut MyResultSet<'r, 'q>
    // where
    //     Self: 'r,
    // {
    //     type Item = Block<'r, 'q>;

    //     type IntoIter = BlocksIter<'i, 'r, MyResultSet<'r, 'q>>;

    //     fn into_iter(self) -> Self::IntoIter {
    //         BlocksIter(self, PhantomData)
    //     }
    // }

    #[derive(Debug)]
    struct Block<'r, 'q>(PhantomData<(&'r u8, &'q u8)>);

    impl<'b, 'r, 'q> BlockExt<'b> for Block<'r, 'q> {
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
