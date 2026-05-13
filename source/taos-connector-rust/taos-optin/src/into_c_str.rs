use std::borrow::Cow;
use std::ffi::{CStr, CString};

/// Helper trait to auto convert Rust strings to CStr.
pub trait IntoCStr<'a> {
    fn into_c_str(self) -> Cow<'a, CStr>;
}

macro_rules! _impl_for_raw {
    ($t:ty) => {
        impl<'a> IntoCStr<'a> for $t {
            fn into_c_str(self) -> Cow<'a, CStr> {
                Cow::from(self)
            }
        }
    };
}

_impl_for_raw!(CString);
_impl_for_raw!(&'a CStr);
_impl_for_raw!(&'a CString);

macro_rules! _impl_for_ref {
    ($t:ty) => {
        impl<'a> IntoCStr<'a> for &'a $t {
            fn into_c_str(self) -> Cow<'a, CStr> {
                self.as_c_str().into_c_str()
            }
        }
    };
}

_impl_for_ref!(&CString);
_impl_for_ref!(&&CString);

impl<'a> IntoCStr<'a> for &&'a CStr {
    fn into_c_str(self) -> Cow<'a, CStr> {
        Cow::from(*self)
    }
}

impl<'a> IntoCStr<'a> for String {
    fn into_c_str(self) -> Cow<'a, CStr> {
        let v = self.into_bytes();
        Cow::from(unsafe { CString::from_vec_unchecked(v) })
    }
}

macro_rules! _impl_for_str {
    ($t:ty) => {
        impl<'a> IntoCStr<'a> for &'a $t {
            fn into_c_str(self) -> Cow<'a, CStr> {
                self.to_owned().into_c_str()
            }
        }
    };
}

_impl_for_str!(String);
_impl_for_str!(str);
_impl_for_str!(&str);

#[cfg(test)]
mod test_into_c_str {
    use super::*;

    fn from_c_str<'a>(_: impl IntoCStr<'a>) {}

    #[test]
    fn c_str_to_c_str() {
        let s = CStr::from_bytes_with_nul(b"abc\0").unwrap();
        from_c_str(s);
        from_c_str(s);
    }

    #[test]
    fn c_string_to_c_str() {
        let s = CString::new("abc").unwrap();
        from_c_str(&s);
        from_c_str(s);
    }

    #[test]
    fn str_to_c_str() {
        let s = "abc";
        from_c_str(s);
        from_c_str(s);
    }

    #[test]
    fn string_to_c_str() {
        let s = String::from("abc");
        from_c_str(&s);
        from_c_str(s.as_str());
        from_c_str(s);
    }
}
