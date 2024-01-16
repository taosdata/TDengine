use taosx_ipc::prelude::IpcDataType;

pub struct TableMeta {
    pub name: String,
    pub columns: Vec<ColumnMeta>,
}

pub struct ColumnMeta {
    pub name: String,
    pub data_type: IpcDataType,
}
