use std::fmt::{Debug, Display};

use crate::util::{Inlinable, InlinableRead, InlinableWrite};

use super::{ty::Ty, ColSchema};

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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Field {
    name: String,
    ty: Ty,
    bytes: u32,
}

impl Inlinable for Field {
    fn write_inlined<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let mut l = wtr.write_u8(self.ty as u8)?;
        l += wtr.write_u32(self.bytes)?;
        l += wtr.write_inlined_str::<2>(&self.name)?;
        Ok(l)
    }

    fn read_inlined<R: std::io::Read>(mut reader: R) -> std::io::Result<Self> {
        let ty = Ty::from(reader.read_u8()?);
        let bytes = reader.read_u32()?;
        let name = reader.read_inlined_str::<2>()?;
        Ok(Self { name, ty, bytes })
    }
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

    pub(crate) fn to_column_schema(&self) -> ColSchema {
        ColSchema::new(self.ty, self.bytes as _)
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
