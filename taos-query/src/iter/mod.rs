use super::*;

// type DeserializeIter<'b, B, T> =
//     std::iter::Map<RowsIter<'b, B>, fn(RowInBlock<'b, B>) -> Result<T, DeError>>;

/// Trait to define a data `Block` to fetch records bulky.
///
/// If query performance is not your main concern, you can just use the deserialize method from result set.
pub trait BlockExt: Debug + Sized {
    /// A block should container number of rows.
    fn num_of_rows(&self) -> usize;

    /// Fields can be queried from a block.
    fn fields(&self) -> &[Field];

    /// Number of fields.
    fn field_count(&self) -> usize {
        self.fields().len()
    }

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

    /// # Safety
    ///
    /// **DO NOT** call it directly.
    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, BorrowedValue);

    unsafe fn get_col_unchecked(&self, col: usize) -> &ColumnView;

    /// Query by rows.
    // fn iter_rows(&self) -> RowsIter<'_, Self> {
    //     RowsIter::new(self)
    // }

    /// Consume self into rows.
    // fn into_iter_rows(self) -> IntoRowsIter<Self> {
    //     IntoRowsIter::new(self)
    // }

    /// Columns iterator with borrowed data from block.
    fn columns_iter(&self) -> ColsIter<'_, Self> {
        ColsIter::new(self)
    }

    // fn to_records(&self) -> Vec<Vec<Value>> {
    //     self.iter_rows()
    //         .map(|row| row.into_iter().map(|(f, v)| v.into_value()).collect_vec())
    //         .collect_vec()
    // }

    // / Deserialize a row to a record type(primitive type or a struct).
    // /
    // / Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    // fn deserialize<'b, T>(&'b self) -> DeserializeIter<'b, Self, T>
    // where
    //     T: serde::de::Deserialize<'b>,
    // {
    //     self.iter_rows().map(|row| {
    //         let de = de::RecordDeserializer::from(row);
    //         T::deserialize(de)
    //     })
    // }

    // / Deserialize a row to a record type(primitive type or a struct).
    // /
    // / Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    // fn deserialize_into<T>(
    //     self,
    // ) -> std::iter::Map<IntoRowsIter<Self>, fn(QueryRowIter<Self>) -> Result<T, DeError>>
    // where
    //     T: serde::de::DeserializeOwned,
    // {
    //     self.into_iter_rows().map(|row| {
    //         let de = de::RecordDeserializer::from(&row);
    //         T::deserialize(de)
    //     })
    // }

    // / Shortcut version to `.deserialize_into.collect::<Vec<T>>()`
    //
    // fn deserialize_into_vec<T>(self) -> Vec<Result<T, DeError>>
    // where
    //     T: serde::de::DeserializeOwned,
    // {
    //     self.deserialize_into().collect()
    // }

    // #[cfg(feature = "async")]
    // /// Rows as [futures::stream::Stream].
    // fn rows_stream(&self) -> futures::stream::Iter<RowsIter<'_, Self>> {
    //     futures::stream::iter(Self::iter_rows(self))
    // }

    // #[cfg(feature = "async")]
    // /// Owned version to rows stream.
    // fn into_rows_stream(self) -> futures::stream::Iter<IntoRowsIter<Self>> {
    //     futures::stream::iter(Self::into_iter_rows(self))
    // }

    // #[cfg(feature = "async")]
    // /// Rows stream to deserialized record.
    // fn deserialize_stream<'b, T>(&'b self) -> futures::stream::Iter<DeserializeIter<'b, Self, T>>
    // where
    //     T: serde::de::Deserialize<'b>,
    // {
    //     futures::stream::iter(Self::deserialize(self))
    // }
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

#[derive(Debug)]
pub struct RowsIter<'b, T: BlockExt> {
    block: &'b T,
    row: usize,
}

impl<'b, T> RowsIter<'b, T>
where
    T: BlockExt,
{
    pub(crate) fn new(block: &'b T) -> Self {
        Self { block, row: 0 }
    }
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

#[derive(Debug)]
pub struct IntoRowsIter<T: BlockExt> {
    block: Rc<T>,
    row: usize,
}

impl<T> IntoRowsIter<T>
where
    T: BlockExt,
{
    pub(crate) fn new(block: T) -> Self {
        Self {
            block: Rc::new(block),
            row: 0,
        }
    }
}

pub struct QueryRowIter<T: BlockExt> {
    block: Rc<T>,
    row: usize,
}

impl<'b, T> IntoIterator for &'b QueryRowIter<T>
where
    T: BlockExt,
{
    type Item = (&'b Field, BorrowedValue<'b>);

    type IntoIter = CellIter<'b, T>;

    fn into_iter(self) -> Self::IntoIter {
        CellIter {
            block: &self.block,
            row: self.row,
            col: 0,
        }
    }
}

impl<T> Iterator for IntoRowsIter<T>
where
    T: BlockExt,
{
    type Item = QueryRowIter<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.row;

        if row < self.block.num_of_rows() {
            self.row += 1;
            Some(QueryRowIter {
                block: self.block.clone(),
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
impl<'b, T> ColsIter<'b, T>
where
    T: BlockExt,
{
    pub(crate) fn new(block: &'b T) -> Self {
        Self { block, col: 0 }
    }
}

impl<'b, T> Iterator for ColsIter<'b, T>
where
    T: BlockExt,
{
    type Item = &'b ColumnView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.col >= self.block.field_count() {
            return None;
        }

        let v = unsafe { self.block.get_col_unchecked(self.col) };
        self.col += 1;
        Some(v)
    }
}
