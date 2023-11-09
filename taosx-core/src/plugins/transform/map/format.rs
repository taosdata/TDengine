use arrow::{array::ArrayRef, datatypes::FieldRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};
use taosx_ipc::prelude::IpcDataType;

use super::{ValueBuilder, ValueBuilderError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatValueBuilder {
    format: String,
}

impl ValueBuilder for FormatValueBuilder {
    fn build_field(
        &self,
        _name: &str,
        _record: &RecordBatch,
        _as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        todo!()
    }
}
