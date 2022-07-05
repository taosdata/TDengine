use std::{
    borrow::Cow,
    fmt::{self, Display},
    ops::{Deref, DerefMut},
    str::FromStr,
};

macro_rules! _impl_fmt {
    ($fmt:ident) => {
        impl fmt::$fmt for Code {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::$fmt::fmt(&self.0, f)
            }
        }
    };
}

_impl_fmt!(Display);
_impl_fmt!(LowerHex);
_impl_fmt!(UpperHex);

/// TDengine error code.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
#[repr(C)]
pub struct Code(i32);

impl From<i32> for Code {
    #[inline]
    fn from(c: i32) -> Self {
        Self(c)
    }
}

impl From<Code> for i32 {
    fn from(c: Code) -> Self {
        c.0
    }
}

macro_rules! _impl_from {
    ($($from:ty) *) => {
        $(
            impl From<$from> for Code {
                #[inline]
                fn from(c: $from) -> Self {
                    Self(c as i32)
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

_impl_from!(i8 u8 i16 u16 u32 i64 u64);

impl Deref for Code {
    type Target = i32;

    fn deref(&self) -> &i32 {
        &self.0
    }
}

impl DerefMut for Code {
    fn deref_mut(&mut self) -> &mut i32 {
        &mut self.0
    }
}

impl Code {
    /// Code from raw primitive type.
    pub const fn new(code: i32) -> Self {
        Code(code)
    }
}

#[allow(non_upper_case_globals, non_snake_case)]
mod code {
    include!(concat!(env!("OUT_DIR"), "/code.rs"));
}

use serde::de;

#[derive(Debug, Clone)]
pub struct Error {
    code: Code,
    err: Cow<'static, str>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::error::Error for Error {}

impl Error {
    #[inline]
    pub fn new(code: impl Into<Code>, err: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code: code.into(),
            err: err.into(),
        }
    }

    #[inline]
    pub const fn code(&self) -> Code {
        self.code
    }
    #[inline]
    pub fn message(&self) -> &str {
        &self.err
    }

    #[inline]
    pub fn from_code(code: impl Into<Code>) -> Self {
        Self {
            code: code.into(),
            err: "".into(),
        }
    }

    #[inline]
    pub fn from_string(err: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code: Code::Failed,
            err: err.into(),
        }
    }
}

impl FromStr for Error {
    type Err = ();

    #[inline]
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self {
            code: Code::Failed,
            err: s.to_string().into(),
        })
    }
}

impl Display for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:#06X}] {}", self.code, self.err)
    }
}

impl de::Error for Error {
    #[inline]
    fn custom<T: fmt::Display>(msg: T) -> Error {
        Error::from_string(format!("{}", msg))
    }
}

#[test]
fn test_code() {
    let c: i32 = Code::new(0).into();
    assert_eq!(c, 0);
}

#[test]
fn test_display() {
    let err = Error::new(Code::Success, "Success");
    assert_eq!(format!("{err}"), "[0x0000] Success");
}
