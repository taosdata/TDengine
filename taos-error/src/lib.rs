use std::{
    borrow::Cow,
    fmt::{self, Display},
    str::FromStr,
};

mod code {
    include!(concat!(env!("OUT_DIR"), "/code.rs"));
}

pub use code::Code;
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
fn test_display() {
    let err = Error::new(Code::Success, "Success");
    assert_eq!(format!("{err}"), "[0x0000] Success");
}
