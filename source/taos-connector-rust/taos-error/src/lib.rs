#![cfg_attr(nightly, feature(error_generic_member_access))]

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{self, Debug, Display};
use std::str::FromStr;

pub use code::Code;
use mdsn::DsnError;
use source::Inner;

mod code;
mod source;

/// The `Error` type, a wrapper around raw libtaos.so client errors or
/// dynamic error types that could be integrated into [anyhow::Error].
///
/// # Constructions
///
/// We prefer to use [format_err] to construct errors, but you can always use
/// constructor API in your codes.
///
/// ## Constructor API
///
/// Use error code from native client. You can use it directly with error code
///
/// ```rust
/// # use taos_error::Error;
/// let error = Error::from_code(0x2603);
/// ```
///
/// Or with error message from C API.
///
/// ```rust
/// # use taos_error::Error;
/// let error = Error::new(0x0216, r#"syntax error near "123);""#); // Syntax error in SQL
/// ```
///
/// # Display representations
#[must_use]
pub struct Error {
    /// Error code, will be displayed when code is not 0xFFFF.
    code: Code,
    /// Error context, use this along with `.msg` or `.source`.
    context: Option<String>,
    /// Error source, from raw or other error type.
    source: Inner,
}

unsafe impl Send for Error {}
unsafe impl Sync for Error {}

impl std::error::Error for Error {
    #[cfg(nightly)]
    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        self.source.provide(request);
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.debug_struct("Error")
                .field("code", &self.code)
                .field("context", &self.context)
                .field("source", &self.source)
                .finish()
        } else {
            // Error code prefix
            if self.code != Code::FAILED {
                write!(f, "[{:#06X}] ", self.code)?;
            }
            if let Some(context) = &self.context {
                f.write_fmt(format_args!("{context}"))?;
                writeln!(f)?;
                writeln!(f)?;
                writeln!(f, "Caused by:")?;

                let chain = self.source.chain();
                for (idx, source) in chain.enumerate() {
                    writeln!(f, "{idx:4}: {source}")?;
                }
            } else {
                let mut chain = self.source.chain();
                if let Some(context) = chain.next() {
                    f.write_fmt(format_args!("{context}"))?;
                }

                if self.source.deep() {
                    writeln!(f)?;
                    writeln!(f)?;
                    writeln!(f, "Caused by:")?;
                    for (idx, source) in chain.enumerate() {
                        writeln!(f, "{idx:4}: {source}")?;
                    }
                }
            }

            #[cfg(nightly)]
            {
                writeln!(f)?;
                writeln!(f, "Backtrace:")?;
                writeln!(f, "{}", self.source.backtrace())?;
            }

            Ok(())
        }
    }
}

impl Display for Error {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Error code prefix
        if self.code != Code::FAILED {
            write!(f, "[{:#06X}] ", self.code)?;
        }
        // Error context
        if let Some(context) = self.context.as_deref() {
            write!(f, "{context}")?;

            if self.source.is_empty() {
                return Ok(());
            }
            // Pretty print error source.
            f.write_str(": ")?;
        } else if self.source.is_empty() {
            return f.write_str("Unknown error");
        }
        write!(f, "{:#}", self.source)?;
        Ok(())
    }
}

impl From<DsnError> for Error {
    fn from(dsn: DsnError) -> Self {
        Self::new(Code::FAILED, dsn.to_string())
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self {
            code: Code::FAILED,
            context: None,
            source: Inner::any(error),
        }
    }
}

impl<C: Into<Code>> From<C> for Error {
    fn from(value: C) -> Self {
        Self::from_code(value.into())
    }
}

impl<'a> From<&'a str> for Error {
    fn from(value: &'a str) -> Self {
        Self::from_string(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    #[inline(always)]
    pub fn new_with_context<T: Into<Code>, U: Into<String>, V: Into<String>>(
        code: T,
        err: U,
        context: V,
    ) -> Self {
        Self {
            code: code.into(),
            context: Some(context.into()),
            source: err.into().into(),
        }
    }

    #[inline]
    pub fn new<T: Into<Code>, U: Into<String>>(code: T, err: U) -> Self {
        Self {
            code: code.into(),
            context: None,
            source: err.into().into(),
        }
    }

    #[inline]
    pub fn context<T: Into<String>>(mut self, context: T) -> Self {
        self.context = Some(match self.context {
            Some(pre) => format!("{}: {}", context.into(), pre),
            None => context.into(),
        });
        self
    }

    #[inline]
    #[deprecated = "Use self.code() instead"]
    pub fn errno(&self) -> Code {
        self.code
    }

    #[inline]
    pub const fn code(&self) -> Code {
        self.code
    }

    #[inline]
    pub fn message(&self) -> String {
        self.source.to_string()
    }

    #[inline(always)]
    pub fn from_code<T: Into<Code>>(code: T) -> Self {
        let code = code.into();
        if let Some(str) = code._priv_err_str() {
            Self::new(code, str)
        } else {
            Self {
                code,
                context: None,
                source: Inner::empty(),
            }
        }
    }

    #[inline]
    pub fn from_string<T: Into<Cow<'static, str>>>(err: T) -> Self {
        anyhow::format_err!("{}", err.into()).into()
    }

    #[inline]
    pub fn from_any<T: Into<anyhow::Error>>(err: T) -> Self {
        err.into().into()
    }

    #[inline]
    pub fn any<T: Into<anyhow::Error> + 'static>(err: T) -> Self {
        if err.type_id() == std::any::TypeId::of::<Self>() {
            let err = &err as &dyn Any;
            let err = err.downcast_ref::<Self>().unwrap();
            return Self {
                code: err.code,
                context: err.context.clone(),
                source: err.source.clone(),
            };
        }
        err.into().into()
    }

    #[inline]
    pub fn success(&self) -> bool {
        self.code == 0
    }

    #[inline]
    pub fn with_code<T: Into<Code>>(mut self, code: T) -> Self {
        self.code = code.into();
        self
    }
}

/// Format error with `code`, `raw`, and `context` messages.
///
/// - `code` is come from native C API for from websocket API.
/// - `raw` is the error message which is treated as internal error.
/// - `context` is some context message which is helpful to users.
///
/// We suggest to use all the three fields to construct a more human-readable and
/// meaningful error. Suck as:
///
/// ```rust
/// # use taos_error::*;
/// let err = format_err!(
///     code = 0x0618,
///     raw = "Message error from native API",
///     context = "Query with sql: `select server_version()`"
/// );
/// let err_str = err.to_string();
/// assert_eq!(err_str, "[0x0618] Query with sql: `select server_version()`: Internal error: `Message error from native API`");
/// ```
///
/// It will give the error:
/// ```text
/// [0x0618] Query with sql: `select server_version()`: Internal error: `Message error from native API`
/// ```
///
/// For more complex error expressions, use a `format!` like API as this:
///
/// ```rust
/// # use taos_error::*;
/// # let sql = "select * from test.meters";
/// # let context = "some context";
/// let _ = format_err!(
///     code = 0x0618,
///     raw = ("Message error from native API while calling {}", "some_c_api"),
///     context = ("Query with sql {:?} in {}", sql, context),
/// );
/// ```
///
/// In this kind of usage, `code = ` is optional, so you can use a shorter line:
///
/// ```rust
/// # use taos_error::*;
/// let _ = format_err!(0x0618, raw = "Some error", context = "Query error");
/// ```
///
/// The `raw` or `context` is optional too:
///
/// ```rust
/// # use taos_error::*;
/// let _ = format_err!(0x0618, raw = "Some error");
/// let _ = format_err!(0x0618, context = "Some error");
/// ```
///
/// For non-internal errors, eg. if you prefer construct an [anyhow]-like error manually,
/// you can use the same arguments like [anyhow::format_err] with this pattern:
///
/// ```rust
/// # use taos_error::*;
/// # let message = "message";
/// let err = format_err!(any = "Error here: {}", message);
/// # assert_eq!(err.to_string(), "Error here: message");
/// let err = format_err!("Error here: {}", message);
/// # assert_eq!(err.to_string(), "Error here: message");
/// ```
///
/// It's equivalent to:
///
/// ```rust
/// # use taos_error::*;
/// # use anyhow;
/// # let message = "message";
/// let err = Error::from(anyhow::format_err!("Error here: {}", message));
/// ```
#[macro_export]
macro_rules! format_err {
    (code = $c:expr, raw = $arg:expr, context = $arg2:expr) => {
        $crate::Error::new_with_context($c, $arg, $arg2)
    };
    (code = $c:expr, raw = $arg:expr) => {
        $crate::Error::new($c, $arg)
    };
    (code = $c:expr, raw = ($($arg:tt)*), context = ($($arg2:tt)*) $(,)?) => {
        $crate::Error::new_with_context($c, __priv_format!($($arg)*), __priv_format!($($arg2)*))
    };
    (code = $c:expr, context = $($arg2:tt)*) => {
        $crate::Error::from($c).context(format!($($arg2)*))
    };
    (code = $c:expr, raw = $arg:literal, context = $($arg2:tt)*) => {
        $crate::Error::new_with_context($c, format!($arg), $crate::__priv_format!($($arg2)*))
    };
    (code = $c:expr, raw = $arg:ident, context = $($arg2:tt)*) => {
        $crate::Error::new_with_context($c, $arg, $crate::__priv_format!($($arg2)*))
    };
    (code = $c:expr) => {
        $crate::Error::from_code($c)
    };
    (code = $c:expr, raw = $($arg:tt)*) => {
        $crate::Error::new($c, format!($($arg)*))
    };
    (code = $c:expr, $($arg:tt)*) => {
        $crate::Error::new($c, format!($($arg)*))
    };
    (any = $($arg:tt)*) => {
        $crate::Error::from_string(format!($($arg)*))
    };
    (raw = $($arg:tt)*) => {
        compile_error!("`raw` error message must be used along with an error code!")
    };
    ($c:expr, raw = $arg:expr) => {
        $crate::Error::new($c, $arg)
    };
    ($c:expr, raw = $arg:expr, context = $arg2:expr) => {
        $crate::Error::new_with_context($c, $arg, $arg2)
    };
    ($c:expr, raw = ($($arg:tt)*), context = ($($arg2:tt)*) $(,)?) => {
        $crate::Error::new_with_context($c, format!($($arg)*), format!($($arg2)*))
    };
    ($c:expr, context = $arg:expr) => {
        $crate::Error::from($c).context($arg)
    };
    ($c:expr, context = $($arg2:tt)*) => {
        $crate::Error::from($c).context(format!($($arg2)*))
    };
    ($c:expr, raw = $($arg:tt)*) => {
        $crate::Error::new($c, format!($($arg)*))
    };
    ($c:expr) => {
        $crate::Error::from($c)
    };
    ($($arg:tt)*) => {
        $crate::Error::from_string(format!($($arg)*))
    };
}

#[macro_export]
macro_rules! __priv_format {
    ($msg:literal $(,)?) => {
        literal.to_string()
    };
    ($err:expr $(,)?) => {
        $err
    };
    ($fmt:expr, $($arg:tt)*) => {
        format!($fmt, $($arg)*)
    };
}

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return std::result::Result::Err($crate::format_err!($($arg)*))
    };
}

impl FromStr for Error {
    type Err = ();

    #[inline]
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::from_string(s.to_string()))
    }
}

#[cfg(feature = "serde")]
impl serde::de::Error for Error {
    #[inline]
    fn custom<T: fmt::Display>(msg: T) -> Error {
        Error::from_string(format!("{msg}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_err() {
        let code = 0xF000;
        let raw = "Nothing";
        let context = "Error";
        let err = dbg!(format_err!(code, raw = raw, context = context));
        assert_eq!(err.to_string(), "[0xF000] Error: Internal error: `Nothing`");
        let err = dbg!(format_err!(code, raw = raw));
        assert_eq!(err.to_string(), "[0xF000] Internal error: `Nothing`");
        let err = dbg!(format_err!(code, context = context));
        assert_eq!(err.to_string(), "[0xF000] Error");

        let err = dbg!(format_err!(code = 0xF000, context = "Error here"));
        assert_eq!(err.to_string(), "[0xF000] Error here");
        let err = dbg!(format_err!(0x6789, context = "Error here: {}", 1));
        assert_eq!(err.to_string(), "[0x6789] Error here: 1");
        let err = dbg!(format_err!(code = 0x6789, context = "Error here: {}", 1));
        assert_eq!(err.to_string(), "[0x6789] Error here: 1");

        let err = dbg!(format_err!(code = 0x6789, raw = "Error here: {}", 1));
        assert_eq!(err.to_string(), "[0x6789] Internal error: `Error here: 1`");

        let err = dbg!(format_err!(0x6789, raw = "Error here: {}", 1));
        assert_eq!(err.to_string(), "[0x6789] Internal error: `Error here: 1`");

        let err = dbg!(format_err!(
            code = 0x6789,
            raw = ("Error here: {}", 1),
            context = ("Query error with {:?}", "sql"),
        ));
        assert_eq!(
            err.to_string(),
            "[0x6789] Query error with \"sql\": Internal error: `Error here: 1`"
        );

        let err = dbg!(format_err!("Error here"));
        assert_eq!(err.to_string(), "Error here");

        let err = dbg!(format_err!(0x2603));
        assert_eq!(
            err.to_string(),
            "[0x2603] Internal error: `Table does not exist`"
        );

        let err = dbg!(format_err!(0x6789));
        assert_eq!(err.to_string(), "[0x6789] Unknown error");

        let err = dbg!(format_err!(0x6789, context = "Error here"));
        assert_eq!(err.to_string(), "[0x6789] Error here");
    }

    #[test]
    fn test_bail() {
        fn use_bail() -> Result<()> {
            bail!(code = 0x2603, context = "Failed to insert into table `abc`");
        }
        let err = use_bail();
        dbg!(&err);
        assert!(err.is_err());
        println!("{:?}", err.unwrap_err());

        println!("{:?}", Error::any(use_bail().unwrap_err()));
    }

    #[test]
    fn test_display() {
        let err = Error::new(Code::SUCCESS, "Success").context("nothing");
        assert!(dbg!(format!("{}", err)).contains("[0x0000] nothing"));
        let result = std::panic::catch_unwind(|| {
            let err = Error::new(Code::SUCCESS, "Success").context("nothing");
            panic!("{:?}", err);
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_error() {
        let err = Error::new(Code::SUCCESS, "success");
        assert_eq!(err.code(), Code::SUCCESS);
        assert_eq!(err.message(), "Internal error: `success`");

        let _ = Error::from_code(1);
        assert_eq!(Error::from_string("any").to_string(), "any");
        assert_eq!(Error::from_string("any").to_string(), "any");

        fn raise_error() -> Result<()> {
            Err(Error::from_any(DsnError::InvalidDriver("mq".to_string())))
        }
        assert_eq!(raise_error().unwrap_err().to_string(), "invalid driver mq");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_error() {
        use serde::de::Error as DeError;

        let _ = Error::custom("");
    }

    #[test]
    fn test_duplicate() {
        let err = Error {
            code: Code::SUCCESS,
            context: Some("context".to_string()),
            source: Inner::Raw {
                raw: Cow::from("raw error"),
                #[cfg(nightly)]
                backtrace: std::backtrace::Backtrace::capture(),
            },
        };

        let err = Error {
            code: Code::SUCCESS,
            context: Some("higher context".to_string()),
            source: Inner::any(err.into()),
        };

        let s1 = format!("{}", err);
        let s2 = format!("{:#}", err);
        assert_eq!(s1, s2);
        assert_eq!(
            s1,
            "[0x0000] higher context: [0x0000] context: Internal error: `raw error`"
        );

        let any_err = anyhow::format_err!("{}", err);
        assert_eq!(any_err.to_string(), s1);

        let any_err = anyhow::format_err!("{:#}", err);
        assert_eq!(any_err.to_string(), s1);
    }

    #[cfg(nightly)]
    #[test]
    fn test_error_provide_forwards_backtrace() {
        use std::backtrace::Backtrace;
        use std::error::request_ref;

        let err = Error {
            code: Code::FAILED,
            context: Some("ctx".to_string()),
            source: Inner::Raw {
                raw: Cow::from("raw error"),
                backtrace: Backtrace::capture(),
            },
        };

        let dyn_err: &dyn std::error::Error = &err;
        let bt_from_dyn = request_ref::<Backtrace>(dyn_err).unwrap();

        if let Inner::Raw {
            backtrace: ref bt, ..
        } = err.source
        {
            assert!(std::ptr::eq(bt, bt_from_dyn));
        } else {
            panic!("unexpected inner variant");
        }
    }
}
