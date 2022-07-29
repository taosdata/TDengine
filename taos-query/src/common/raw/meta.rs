use std::{
    ffi::c_void,
    fmt::{Display, Write},
    ops::Deref,
};

use bytes::Bytes;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    common::{Field, Ty},
    // prelude::AsyncInlinable,
    util::{Inlinable, InlinableRead},
};

use super::RawData;

#[derive(Debug)]
pub struct RawMeta(RawData);

impl<T: Into<RawData>> From<T> for RawMeta {
    fn from(bytes: T) -> Self {
        RawMeta(bytes.into())
    }
}

impl Deref for RawMeta {
    type Target = RawData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RawMeta {
    pub fn new(raw: Bytes) -> Self {
        RawMeta(raw.into())
    }
}

impl Inlinable for RawMeta {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        RawData::read_inlined(reader).map(RawMeta)
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        self.deref().write_inlined(wtr)
    }
}

#[async_trait::async_trait]
impl crate::util::AsyncInlinable for RawMeta {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        <RawData as crate::util::AsyncInlinable>::read_inlined(reader)
            .await
            .map(RawMeta)
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        crate::util::AsyncInlinable::write_inlined(self.deref(), wtr).await
    }
}

// #[derive(Debug, Clone)]
// pub struct RawMeta {
//     raw: Bytes,
// }

// impl RawMeta {
//     pub const META_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u16>();

//     pub fn new(raw: Bytes) -> Self {
//         RawMeta { raw }
//     }
//     pub fn meta_len(&self) -> u32 {
//         unsafe { *(self.raw.as_ptr() as *const u32) }
//     }
//     pub fn meta_type(&self) -> u16 {
//         unsafe {
//             *(self
//                 .raw
//                 .as_ptr()
//                 .offset(std::mem::size_of::<u32>() as isize) as *const u16)
//         }
//     }
//     pub fn meta_data_ptr(&self) -> *const c_void {
//         unsafe { self.raw.as_ptr().offset(Self::META_OFFSET as isize) as _ }
//     }
// }

// impl AsRef<[u8]> for RawMeta {
//     fn as_ref(&self) -> &[u8] {
//         self.raw.as_ref()
//     }
// }

// impl Inlinable for RawMeta {
//     fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
//         let mut data = Vec::new();

//         let len = reader.read_u32()?;
//         data.extend(len.to_le_bytes());

//         let meta_type = reader.read_u16()?;
//         data.extend(meta_type.to_le_bytes());

//         data.resize(data.len() + len as usize, 0);

//         let buf = &mut data[RawMeta::META_OFFSET..];

//         reader.read_exact(buf)?;
//         Ok(Self { raw: data.into() })
//     }

//     fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
//         wtr.write_all(self.raw.as_ref())?;
//         Ok(self.raw.len())
//     }
// }

// #[async_trait::async_trait]
// impl crate::util::AsyncInlinable for RawMeta {
//     async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
//         reader: &mut R,
//     ) -> std::io::Result<Self> {
//         use tokio::io::*;
//         let mut data = Vec::new();

//         let len = reader.read_u32_le().await?;
//         data.extend(len.to_le_bytes());

//         let meta_type = reader.read_u16_le().await?;
//         data.extend(meta_type.to_le_bytes());

//         data.resize(data.len() + len as usize, 0);

//         let buf = &mut data[RawMeta::META_OFFSET..];

//         reader.read_exact(buf).await?;
//         Ok(Self { raw: data.into() })
//     }

//     async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
//         &self,
//         wtr: &mut W,
//     ) -> std::io::Result<usize> {
//         use tokio::io::*;
//         wtr.write_all(self.raw.as_ref()).await?;
//         Ok(self.raw.len())
//     }
// }

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TagWithValue {
    #[serde(flatten)]
    field: Field,
    value: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "tableType")]
#[serde(rename_all = "camelCase")]
pub enum MetaCreate {
    #[serde(rename_all = "camelCase")]
    Super {
        table_name: String,
        columns: Vec<Field>,
        tags: Vec<Field>,
    },
    #[serde(rename_all = "camelCase")]
    Child {
        table_name: String,
        using: String,
        tags: Vec<TagWithValue>,
        tag_num: Option<usize>,
    },
    #[serde(rename_all = "camelCase")]
    Normal {
        table_name: String,
        columns: Vec<Field>,
    },
}

impl Display for MetaCreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CREATE TABLE IF NOT EXISTS ")?;
        match self {
            MetaCreate::Super {
                table_name,
                columns,
                tags,
            } => {
                debug_assert!(columns.len() > 0, "{:?}", self);
                debug_assert!(tags.len() > 0);

                f.write_fmt(format_args!("`{}`", table_name))?;
                f.write_char('(')?;
                f.write_str(&columns.iter().map(|f| f.sql_repr()).join(", "))?;
                f.write_char(')')?;

                f.write_str(" TAGS(")?;
                f.write_str(&tags.iter().map(|f| f.sql_repr()).join(", "))?;
                f.write_char(')')?;
            }
            MetaCreate::Child {
                table_name,
                using,
                tags,
                tag_num,
            } => {
                if tags.len() > 0 {
                    f.write_fmt(format_args!(
                        "`{}` USING `{}` ({}) TAGS({})",
                        table_name,
                        using,
                        tags.iter().map(|t| t.field.escaped_name()).join(", "),
                        tags.iter()
                            .map(|t| {
                                match t.field.ty() {
                                    Ty::Json => format!("'{}'", t.value.as_str().unwrap()),
                                    Ty::VarChar | Ty::NChar => {
                                        format!("{}", t.value.as_str().unwrap())
                                    }
                                    _ => format!("{}", t.value),
                                }
                            })
                            .join(", ")
                    ))?;
                } else {
                    f.write_fmt(format_args!(
                        "`{}` USING `{}` TAGS({})",
                        table_name,
                        using,
                        std::iter::repeat("NULL").take(tag_num.unwrap()).join(",")
                    ))?;
                }
            }
            MetaCreate::Normal {
                table_name,
                columns,
            } => {
                debug_assert!(columns.len() > 0);

                f.write_fmt(format_args!("`{}`", table_name))?;
                f.write_char('(')?;
                f.write_str(&columns.iter().map(|f| f.sql_repr()).join(", "))?;
                f.write_char(')')?;
            }
        }
        Ok(())
    }
}

#[test]
fn test_meta_create_to_sql() {
    // let sql = MetaCreate {
    //     table_name: "abc".to_string(),
    //     table_type: TableType::Super,
    //     using: None,
    //     columns: vec![
    //         Field::new("ts", Ty::Timestamp, 0),
    //         Field::new("location", Ty::VarChar, 16),
    //     ],
    //     tags: vec![],
    // }
    // .to_string();

    // assert_eq!(
    //     sql,
    //     "CREATE TABLE IF NOT EXISTS `abc`(`ts` TIMESTAMP, `location` BINARY(16))"
    // );
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum AlterType {
    AddTag = 1,
    DropTag = 2,
    RenameTag = 3,
    SetTagValue = 4,
    AddColumn = 5,
    DropColumn = 6,
    ModifyColumnLength,
    ModifyTagLength,
    ModifyTableOption,
    RenameColumn,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
// #[serde(tag = "tableType")]
#[serde(rename_all = "camelCase")]
pub struct MetaAlter {
    table_name: String,
    alter_type: AlterType,
    #[serde(flatten, with = "ColField")]
    field: Field,
    col_new_name: Option<String>,
    col_value: Option<String>,
    col_value_null: Option<bool>,
}

impl Display for MetaAlter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.alter_type {
            AlterType::AddTag => f.write_fmt(format_args!(
                "ALTER TABLE `{}` ADD TAG {}",
                self.table_name,
                self.field.sql_repr()
            )),
            AlterType::DropTag => f.write_fmt(format_args!(
                "ALTER TABLE `{}` DROP TAG `{}`",
                self.table_name,
                self.field.name()
            )),
            AlterType::RenameTag => f.write_fmt(format_args!(
                "ALTER TABLE `{}` RENAME TAG `{}` `{}`",
                self.table_name,
                self.field.name(),
                self.col_new_name.as_ref().unwrap()
            )),
            AlterType::SetTagValue => {
                f.write_fmt(format_args!(
                    "ALTER TABLE `{}` SET TAG `{}` ",
                    self.table_name,
                    self.field.name()
                ))?;
                if self.col_value_null.unwrap_or(false) {
                    f.write_str("NULL")
                } else if self.field.ty.is_var_type() {
                    f.write_fmt(format_args!("'{}'", self.col_value.as_ref().unwrap()))
                } else {
                    f.write_fmt(format_args!("{}", self.col_value.as_ref().unwrap()))
                }
            }
            AlterType::AddColumn => f.write_fmt(format_args!(
                "ALTER TABLE `{}` ADD COLUMN {}",
                self.table_name,
                self.field.sql_repr()
            )),
            AlterType::DropColumn => f.write_fmt(format_args!(
                "ALTER TABLE `{}` DROP COLUMN `{}`",
                self.table_name,
                self.field.name()
            )),
            AlterType::ModifyColumnLength => f.write_fmt(format_args!(
                "ALTER TABLE `{}` MODIFY COLUMN {}",
                self.table_name,
                self.field.sql_repr(),
            )),
            AlterType::ModifyTagLength => f.write_fmt(format_args!(
                "ALTER TABLE `{}` MODIFY TAG {}",
                self.table_name,
                self.field.sql_repr(),
            )),
            AlterType::ModifyTableOption => todo!(),
            AlterType::RenameColumn => f.write_fmt(format_args!(
                "ALTER TABLE `{}` RENAME COLUMN `{}` `{}`",
                self.table_name,
                self.field.name(),
                self.col_new_name.as_ref().unwrap()
            )),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum MetaDrop {
    #[serde(rename_all = "camelCase")]
    Super {
        /// Use table_name when drop super table
        table_name: String,
    },
    #[serde(rename_all = "camelCase")]
    Other {
        /// Available for child and normal tables.
        table_name_list: Vec<String>,
    },
}

impl Display for MetaDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaDrop::Super { table_name } => {
                f.write_fmt(format_args!("DROP TABLE IF EXISTS `{}`", table_name))
            }
            MetaDrop::Other { table_name_list } => f.write_fmt(format_args!(
                "DROP TABLE IF EXISTS {}",
                table_name_list.iter().map(|n| format!("`{n}`")).join(" ")
            )),
        }
    }
}
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum JsonMeta {
    Create(MetaCreate),
    Alter(MetaAlter),
    Drop(MetaDrop),
}

impl Display for JsonMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonMeta::Create(meta) => meta.fmt(f),
            JsonMeta::Alter(alter) => alter.fmt(f),
            JsonMeta::Drop(drop) => drop.fmt(f),
            // _ => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(remote = "Field")]
pub struct ColField {
    #[serde(rename = "colName")]
    name: String,
    #[serde(default)]
    #[serde(rename = "colType")]
    ty: Ty,
    #[serde(default)]
    #[serde(rename = "colLength")]
    bytes: u32,
}

impl From<ColField> for Field {
    fn from(f: ColField) -> Self {
        Self {
            name: f.name,
            ty: f.ty,
            bytes: f.bytes,
        }
    }
}
