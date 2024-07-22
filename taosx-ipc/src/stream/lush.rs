use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteMessage {
    pub table: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterMessage {
    pub table: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropMessage {
    table: String,
}

impl AlterMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }
}

impl DropMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }
}

impl DeleteMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LushMessageControl {
    DELETE(DeleteMessage),
    ALTER(AlterMessage),
    DROP(DropMessage),
}
