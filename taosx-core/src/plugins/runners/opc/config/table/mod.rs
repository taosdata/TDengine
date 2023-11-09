use serde::{Deserialize, Serialize};
use taos::{Dsn, Ty};
use taosx_ipc::prelude::IpcDataType;

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TableConfig {
    pub stable_prefix: Option<String>,
    pub column_configs: Vec<ColumnConfig>,
    pub tag_configs: Option<Vec<TagConfig>>,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct ColumnConfig {
    pub column_name: String,
    pub column_type: Option<Ty>,
    pub column_alias: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TagConfig {
    pub column_name: String,
    pub column_type: IpcDataType,
}

impl TableConfig{
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self>{
        Ok(Self{
            stable_prefix: None,
            column_configs: vec![],
            tag_configs: None,
        })
    }
}