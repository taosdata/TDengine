use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use super::ty::Ty;
use super::views::DataType;
use crate::util::{Inlinable, InlinableRead, InlinableWrite};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_camel_case_types)]
pub struct c_field_t {
    pub name: [u8; 65usize],
    pub type_: u8,
    pub bytes: i32,
}

#[test]
fn test_c_field_t() {
    let bytes = 1960820204i32;
    println!("{:0b}", bytes);
}

/// A `Field` represents the name and data type of one column or tag.
///
/// For example, a table as "create table tb1 (ts timestamp, n nchar(100))".
///
/// When query with "select * from tb1", you will get two fields:
///
/// 1. `{ name: "ts", ty: Timestamp, bytes: 8 }`, a `TIMESTAMP` field with name `ts`,
///    bytes length 8 which is the byte-width of `i64`.
/// 2. `{ name: "n", ty: NChar, bytes: 100 }`, a `NCHAR` field with name `n`,
///    bytes length 100 which is the length of the variable-length data.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct Field {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) ty: Ty,
    #[serde(default)]
    #[serde(rename = "length")]
    pub(crate) bytes: u32,
}

impl From<&Field> for c_field_t {
    fn from(value: &Field) -> Self {
        let name = value.name().as_bytes();
        let mut field = c_field_t {
            name: [0; 65],
            type_: value.ty() as _,
            bytes: value.bytes() as _,
        };

        unsafe {
            std::ptr::copy_nonoverlapping(name.as_ptr(), field.name.as_mut_ptr() as _, name.len());
        }

        field
    }
}

impl Inlinable for Field {
    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        let mut l = wtr.write_u8_le(self.ty as u8)?;
        l += wtr.write_u32_le(self.bytes)?;
        l += wtr.write_inlined_str::<2>(&self.name)?;
        Ok(l)
    }

    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let ty = Ty::from(reader.read_u8()?);
        let bytes = reader.read_u32()?;
        let name = reader.read_inlined_str::<2>()?;
        Ok(Self { name, ty, bytes })
    }
}

impl Field {
    pub const fn empty() -> Self {
        Self {
            name: String::new(),
            ty: Ty::Null,
            bytes: 0,
        }
    }

    pub fn new<T: Into<String>>(name: T, ty: Ty, bytes: u32) -> Self {
        let name = name.into();
        Self { name, ty, bytes }
    }

    /// Field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Escaped file name
    pub fn escaped_name(&self) -> String {
        format!("`{}`", self.name())
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

    pub fn to_c_field(&self) -> c_field_t {
        self.into()
    }

    /// Represent the data type in sql.
    ///
    /// For example: "INT", "VARCHAR(100)".
    pub fn sql_repr(&self) -> String {
        let ty = self.ty();
        if ty.is_var_type() {
            format!("`{}` {}({})", self.name(), ty.name(), self.bytes())
        } else {
            format!("`{}` {}", self.name(), ty.name())
        }
    }

    #[inline]
    pub(crate) fn to_column_schema(&self) -> DataType {
        DataType::new(self.ty(), self.bytes() as _)
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ty = self.ty();
        if ty.is_var_type() {
            write!(f, "`{}` {}({})", self.name(), ty.name(), self.bytes())
        } else {
            write!(f, "`{}` {}", self.name(), ty.name())
        }
    }
}
