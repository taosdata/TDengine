use super::*;

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
