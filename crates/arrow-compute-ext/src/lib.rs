use arrow::{
    array::{Array, RecordBatch},
    compute::take,
    error::ArrowError,
};

pub trait RecordBatchExt {
    fn take(&self, indices: &dyn Array) -> Result<RecordBatch, ArrowError>;
}

impl RecordBatchExt for RecordBatch {
    fn take(&self, indices: &dyn Array) -> Result<Self, ArrowError> {
        let columns = self
            .columns()
            .iter()
            .map(|column| {
                let array = take(column, indices, None);
                array.into()
            })
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new(self.schema().clone(), columns)
    }
}
