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
#[repr(C)]
#[derive(Clone)]
pub struct Field {
    name: [u8; 65],
    ty: Ty,
    bytes: u32,
}

impl Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Field")
            .field("name", &self.name())
            .field("ty", &self.ty)
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[test]
fn field_size() {
    assert_eq!(std::mem::size_of::<Field>(), 72);
}
impl Field {
    #[cfg(feature = "nightly")]
    pub const fn new(name: &str, ty: Ty, bytes: u32) -> Self {
        let name_raw = name.as_bytes();
        let mut name: [u8; 65] = [0; 65];
        unsafe {
            std::ptr::copy_nonoverlapping(name_raw.as_ptr(), name.as_mut_ptr(), name_raw.len())
        };
        Self { name, ty, bytes }
    }
    #[cfg(not(feature = "nightly"))]
    pub fn new(name: &str, ty: Ty, bytes: u32) -> Self {
        let name_raw = name.as_bytes();
        let mut name: [u8; 65] = [0; 65];
        unsafe {
            std::ptr::copy_nonoverlapping(name_raw.as_ptr(), name.as_mut_ptr(), name_raw.len())
        };
        Self { name, ty, bytes }
    }

    /// Field name.
    pub fn name(&self) -> Cow<str> {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _).to_string_lossy() }
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
