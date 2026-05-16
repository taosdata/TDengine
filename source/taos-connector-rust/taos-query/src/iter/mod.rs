use super::*;

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
    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&'_ Field, BorrowedValue<'_>);

    /// # Safety
    /// **DO NOT** call it directly.
    unsafe fn get_col_unchecked(&self, col: usize) -> &ColumnView;

    /// Columns iterator with borrowed data from block.
    fn columns_iter(&self) -> ColsIter<'_, Self> {
        ColsIter::new(self)
    }
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
