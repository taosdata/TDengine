use std::fmt::{self, Debug, Display};

use derive_more::{Deref, DerefMut};

mod constants;

/// The error code.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Default, Deref, DerefMut)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct Code(i32);

impl Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:#06X}", *self))
    }
}

impl Debug for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("[{:#06X}] {}", self.0, self.as_err_str()))
    }
}

impl Code {
    #[allow(non_upper_case_globals)]
    #[deprecated(since = "0.9.0", note = "Use Code::SUCCESS instead")]
    pub const Success: Code = Code(0);
    #[allow(non_upper_case_globals)]
    #[deprecated(since = "0.9.0", note = "Use Code::FAILED instead")]
    pub const Failed: Code = Code(0xFFFF);

    pub const SUCCESS: Code = Code(0);
    pub const FAILED: Code = Code(0xFFFF);

    pub const INVALID_PARA: Code = Code(0x0118);
    pub const COLUMN_EXISTS: Code = Code(0x036B);
    pub const COLUMN_NOT_EXIST: Code = Code(0x036C);
    pub const TAG_ALREADY_EXIST: Code = Code(0x0369);
    pub const TAG_NOT_EXIST: Code = Code(0x036A);
    pub const MODIFIED_ALREADY: Code = Code(0x264B);
    pub const INVALID_COLUMN_NAME: Code = Code(0x2602);
    pub const TABLE_NOT_EXIST: Code = Code(0x2603);
    pub const STABLE_NOT_EXIST: Code = Code(0x0362);
    pub const INVALID_ROW_BYTES: Code = Code(0x036F);
    pub const DUPLICATED_COLUMN_NAMES: Code = Code(0x263C);
    pub const NO_COLUMN_CAN_BE_DROPPED: Code = Code(0x2651);

    pub const OBJECT_IS_NULL: Code = Code(0xE100);
    pub const TMQ_TOPIC_APPEND_ERR: Code = Code(0xE101);

    /// Code from raw primitive type.
    pub const fn new(code: i32) -> Self {
        Code(code)
    }

    /// Error code == 0. It should not raised in any real errors.
    pub fn success(&self) -> bool {
        self.0 == 0
    }

    const fn as_err_str(&self) -> &'static str {
        if self.0 == 0 {
            return "Success";
        }
        match constants::error_str_of(self.0 as _) {
            Some(s) => s,
            None => "Incomplete",
        }
    }

    pub(crate) const fn _priv_err_str(&self) -> Option<&'static str> {
        constants::error_str_of(self.0 as _)
    }
}

macro_rules! _impl_fmt {
    ($fmt:ident) => {
        impl fmt::$fmt for Code {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::$fmt::fmt(&self.0, f)
            }
        }
    };
}

_impl_fmt!(LowerHex);
_impl_fmt!(UpperHex);

macro_rules! _impl_from {
    ($($from:ty) *) => {
        $(
            impl From<$from> for Code {
                #[inline]
                fn from(c: $from) -> Self {
                    Self(c as i32 & 0xFFFF)
                }
            }
            impl From<Code> for $from {
                #[inline]
                fn from(c: Code) -> Self {
                    c.0 as _
                }
            }
        )*
    };
}

_impl_from!(i8 u8 i16 i32 u16 u32 i64 u64);

impl PartialEq<usize> for Code {
    fn eq(&self, other: &usize) -> bool {
        self.0 == *other as i32
    }
}

impl PartialEq<isize> for Code {
    fn eq(&self, other: &isize) -> bool {
        self.0 == *other as i32
    }
}

impl PartialEq<i32> for Code {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::Code;

    #[test]
    fn test_code() {
        let c: i32 = Code::new(0).into();
        assert_eq!(c, 0);
        let c = Code::from(0).to_string();
        assert_eq!(c, "0x0000");
        dbg!(Code::from(0x200));

        let c: i8 = Code::new(0).into();
        let mut c: Code = c.into();

        let _: &i32 = &c;
        let _: &mut i32 = &mut c;
    }
}
