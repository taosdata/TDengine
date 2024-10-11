use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteMessage {
    // 子表 ID
    pub table: String,
    // delete table 语句 where 后面的条件
    pub condition: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterMessage {
    // 子表 ID
    pub table: String,
    // alter table 的子句，比如： set tag, add column.
    pub alter_table_clause: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropMessage {
    // 子表 ID
    pub table: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertMessage {
    // 子表 ID
    pub table: String,
    // 列名和值部分
    pub column_values: String,
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

impl InsertMessage {
    pub fn table_id(&self) -> &str {
        self.table.as_str()
    }

    pub fn column_values(&self) -> &str {
        self.column_values.as_str()
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Serialize, Deserialize)]
pub enum LushMessageControl {
    DELETE(DeleteMessage),
    ALTER(AlterMessage),
    DROP(DropMessage),
    INSERT(InsertMessage),
}
