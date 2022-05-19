use std::borrow::Cow;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;

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

pub struct NullableCStr<'a>(Option<Cow<'a, CStr>>);

impl<'a> NullableCStr<'a> {
    pub fn as_ptr(&self) -> *const c_char {
        match self.0.as_ref() {
            Some(c) => c.as_ptr(),
            None => std::ptr::null(),
        }
    }
}

pub trait IntoNullableCStr<'a> {
    fn into_nullable_c_str(self) -> NullableCStr<'a>;
}

impl<'a, T> IntoNullableCStr<'a> for T
where
    T: IntoCStr<'a>,
{
    fn into_nullable_c_str(self) -> NullableCStr<'a> {
        NullableCStr(Some(self.into_c_str()))
    }
}

impl<'a, T> IntoNullableCStr<'a> for Option<T>
where
    T: IntoCStr<'a>,
{
    fn into_nullable_c_str(self) -> NullableCStr<'a> {
        NullableCStr(self.map(IntoCStr::into_c_str))
    }
}

impl<'a, T> IntoNullableCStr<'a> for &'a Option<T>
where
    &'a T: IntoCStr<'a>,
{
    fn into_nullable_c_str(self) -> NullableCStr<'a> {
        NullableCStr(self.as_ref().map(|c| c.into_c_str()))
    }
}

impl<'a> IntoNullableCStr<'a> for () {
    fn into_nullable_c_str(self) -> NullableCStr<'a> {
        NullableCStr(None)
    }
}

impl<'a, T> From<T> for NullableCStr<'a>
where
    T: IntoNullableCStr<'a>,
{
    fn from(rhs: T) -> NullableCStr<'a> {
        rhs.into_nullable_c_str()
    }
}

macro_rules! _impl_for_ptr {
    ($t:ty) => {
        impl<'a> IntoNullableCStr<'a> for $t {
            fn into_nullable_c_str(self) -> NullableCStr<'a> {
                NullableCStr(if self.is_null() {
                    None
                } else {
                    Some(Cow::from(unsafe { CStr::from_ptr(self as _) }))
                })
            }
        }
    };
}

_impl_for_ptr!(*const i8);
_impl_for_ptr!(*mut i8);
_impl_for_ptr!(*const u8);
_impl_for_ptr!(*mut u8);
_impl_for_ptr!(*mut c_void);

#[cfg(test)]
mod test_into_c_str {
    use std::{
        borrow::Borrow,
        ffi::{CStr, CString},
    };

    use super::*;

    fn from_c_str<'a>(_: impl IntoCStr<'a>) {}

    #[test]
    fn c_str_to_c_str() {
        let s = CStr::from_bytes_with_nul(b"abc\0").unwrap();
        from_c_str(&s);
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
        from_c_str(&s);
        from_c_str(s);
    }
    #[test]
    fn string_to_c_str() {
        let s = String::from("abc");
        from_c_str(&s);
        from_c_str(s.as_str());
        from_c_str(s);
    }

    #[test]
    fn option_string_to_c_str() {
        let s = Some(String::from("abc"));
        let _ = s.borrow().into_nullable_c_str();
        let _ = s.as_ref().into_nullable_c_str();
        let _ = s.into_nullable_c_str();
    }
    #[test]
    fn option_cstring_to_c_str() {
        let s = Some(CString::new("abc").unwrap());
        let _ = s.borrow().into_nullable_c_str();
        let _ = s.as_ref().into_nullable_c_str();
        let _ = s.into_nullable_c_str();
    }
}
