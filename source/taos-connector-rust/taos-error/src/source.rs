#[cfg(nightly)]
use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Deref;

use thiserror::Error;

/// Inner error source.
#[derive(Error)]
pub enum Inner {
    /// Raw error message from taos C library.
    #[error("")]
    Empty {
        #[cfg(nightly)]
        backtrace: Backtrace,
    },
    /// Raw error message from taos C library.
    #[error("Internal error: `{}`", .raw)]
    Raw {
        raw: Cow<'static, str>,
        #[cfg(nightly)]
        backtrace: Backtrace,
    },
    /// Source error from other kinds of errors.
    ///
    /// All `std::error::Error`s will be stored as an [anyhow::Error].
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}

impl Clone for Inner {
    fn clone(&self) -> Self {
        match self {
            Self::Empty { .. } => Self::Empty {
                #[cfg(nightly)]
                backtrace: Backtrace::force_capture(),
            },
            Self::Raw { raw, .. } => Self::Raw {
                raw: raw.clone(),
                #[cfg(nightly)]
                backtrace: Backtrace::force_capture(),
            },
            Self::Any(any) => Self::Any(anyhow::format_err!("{:#}", any)),
        }
    }
}

impl Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty { .. } => write!(f, "{:?}", ()),
            Self::Raw { raw, .. } => write!(f, "{:#?}", raw.as_ref()),
            Self::Any(any) => f.debug_tuple("Any").field(any).finish(),
        }
    }
}

impl From<&'static str> for Inner {
    #[inline(always)]
    fn from(value: &'static str) -> Self {
        Self::raw(value.into())
    }
}

impl From<String> for Inner {
    #[inline(always)]
    fn from(value: String) -> Self {
        Self::raw(value.into())
    }
}

impl<'c> From<&'c std::ffi::CStr> for Inner {
    #[inline(always)]
    fn from(value: &'c std::ffi::CStr) -> Self {
        Self::raw(value.to_string_lossy().to_string().into())
    }
}

impl From<std::ffi::CString> for Inner {
    #[inline(always)]
    fn from(value: std::ffi::CString) -> Self {
        Self::raw(value.to_string_lossy().to_string().into())
    }
}

impl Inner {
    /// Empty empty with backtrace only
    pub fn empty() -> Self {
        Inner::Empty {
            #[cfg(nightly)]
            backtrace: Backtrace::force_capture(),
        }
    }

    /// Raw message
    pub fn raw(raw: Cow<'static, str>) -> Self {
        Inner::Raw {
            raw,
            #[cfg(nightly)]
            backtrace: Backtrace::force_capture(),
        }
    }

    pub fn any(error: anyhow::Error) -> Self {
        Inner::Any(error)
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty { .. })
    }

    #[cfg(nightly)]
    #[inline(always)]
    pub fn backtrace(&self) -> &Backtrace {
        match self {
            Inner::Empty { backtrace } | Inner::Raw { backtrace, .. } => backtrace,
            Inner::Any(any) => any.backtrace(),
        }
    }

    pub fn chain(&self) -> Chain<'_> {
        match self {
            Inner::Empty { .. } => Chain::Empty,
            Inner::Raw { raw, .. } => Chain::Raw([raw.deref()].into_iter()),
            Inner::Any(any) => Chain::Any(any.chain()),
        }
    }

    pub fn deep(&self) -> bool {
        match self {
            Inner::Empty { .. } | Inner::Raw { .. } => false,
            Inner::Any(any) => any.source().is_some(),
        }
    }
}

pub enum Chain<'a> {
    Empty,
    Raw(std::array::IntoIter<&'a str, 1>),
    Any(anyhow::Chain<'a>),
}

impl<'a> Iterator for Chain<'a> {
    type Item = Cow<'a, str>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Chain::Empty => None,
            Chain::Raw(raw) => raw.next().map(Into::into),
            Chain::Any(chain) => chain.next().map(ToString::to_string).map(Into::into),
        }
    }
}

#[test]
fn test_inner() {
    let err = Inner::from(anyhow::anyhow!("error"));
    assert_eq!(err.to_string(), "error");
}
