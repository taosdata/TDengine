use std::borrow::Cow;
use std::ffi::CStr;
use std::fmt::{Debug, Display};

use super::ty::Ty;

/// A `Field` represents the name and data type of one column or tag.
///
/// For example, a table as "create table tb1 (ts timestamp, n nchar(100))".
///
/// When query with "select * from tb1", you will get two fields:
///
/// 1. `{ name: "ts", ty: Timestamp, bytes: 8 }`, a `TIMESTAMP` field with name `ts`,
///    bytes length 8 which is the byte-width of `i64`.
/// 2. `{ name: "n", ty: NChar, bytes: 100 }`, a `NCHAR` filed with name `n`,
///    bytes length 100 which is the length of the variable-length data.

#[derive(Debug)]
pub struct Field {
    name: String,
    ty: Ty,
    bytes: u32,
}

impl Field {
    pub fn new(name: impl Into<String>, ty: Ty, bytes: u32) -> Self {
        let name = name.into();
        Self { name, ty, bytes }
    }

    /// Field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Data type of the field.
    pub const fn ty(&self) -> Ty {
        self.ty
    }

    /// Preset length of variable length data type.
    ///
    /// It's the byte-width in other types.
    pub const fn bytes(&self) -> u32 {
        self.bytes
    }

    /// Represent the data type in sql.
    ///
    /// For example: "INT", "VARCHAR(100)".
    pub fn sql_repr(&self) -> String {
        let ty = self.ty();
        if ty.is_var_type() {
            format!("{}({})", ty.name(), self.bytes())
        } else {
            ty.name().to_string()
        }
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ty = self.ty();
        if ty.is_var_type() {
            write!(f, "({}: {}({}))", self.name(), ty.name(), self.bytes())
        } else {
            write!(f, "({}: {})", self.name(), ty.name())
        }
    }
}
