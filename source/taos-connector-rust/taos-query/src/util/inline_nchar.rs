use std::fmt;

use super::Inlinable;

#[repr(C)]
#[repr(packed(1))]
pub struct InlineNChar<T = u16> {
    len: T,
    data: [u8; 0],
}

pub struct Chars<'a, T = u16> {
    data: &'a InlineNChar<T>,
    i: T,
}

macro_rules! _impl_inline_str {
    ($($ty:ty) *) => {
        $(

            impl fmt::Debug for InlineNChar<$ty> {
                #[inline]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct("InlineNChar")
                        .field("chars_len", &self.chars_len())
                        .field("len", &self.len())
                        .field("data", &self.to_string())
                        .finish()
                }
            }

            impl fmt::Display for InlineNChar<$ty> {
                #[inline]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str(&self.to_string())
                }
            }

            impl AsRef<[u8]> for InlineNChar<$ty> {
                #[inline]
                fn as_ref(&self) -> &[u8] {
                    self.as_bytes()
                }
            }
            impl<'a> Iterator for Chars<'a, $ty> {
                type Item = char;

                #[inline]
                fn next(&mut self) -> Option<Self::Item> {
                    if self.i < self.data.chars_len() as $ty {
                        let c = unsafe { std::ptr::read_unaligned(self.data.data.as_ptr().add(self.i as usize * std::mem::size_of::<char>()) as *const char) };
                        self.i += 1;
                        Some(c)
                    } else {
                        None
                    }
                }
            }

            impl Inlinable for InlineNChar<$ty> {
                #[inline]
                fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
                    let l = wtr.write(&self.len.to_le_bytes())?;
                    Ok(l + wtr.write(self.as_bytes())?)
                }

                #[inline]
                fn read_inlined<R: std::io::Read>(_: &mut R) -> std::io::Result<Self> {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "can't read into a inlined string"))
                }
            }

            impl InlineNChar<$ty> {
                #[inline]
                /// # Safety
                ///
                /// Do not use it directly.
                pub unsafe fn from_ptr<'a>(ptr: *const u8) -> &'a Self {
                    // std::mem::transmute::<*const u8, &InlineNChar<$ty>>(ptr)
                    &*ptr.cast::<InlineNChar<$ty>>()
                }
                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn as_bytes(&self) -> &[u8] {
                    unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len()) }
                }
                #[inline]
                pub fn to_string(&self) -> String {
                    self.chars().collect()
                }

                #[inline]
                pub const fn len(&self) -> usize {
                    self.len as _
                }

                #[inline]
                pub const fn chars_len(&self) -> usize {
                    self.len() / std::mem::size_of::<char>()
                }

                pub fn chars(&self) -> Chars<'_, $ty> {
                    Chars {
                        data: self,
                        i: 0,
                    }
                }

                #[inline]
                #[allow(mutable_transmutes)]
                #[allow(clippy::missing_transmute_annotations)]
                /// # Safety
                ///
                /// Do not use it directly.
                pub unsafe fn into_inline_str(&self) -> &super::InlineStr<$ty> {
                    if self.len() == 0 {
                        return std::mem::transmute(self);
                    }
                    let chars_len = self.chars_len();
                    let v: &mut super::InlineStr<$ty> = std::mem::transmute(self);
                    let ptr = v.as_mut_ptr();
                    let mut len = 0usize;
                    for i in 0..chars_len {
                        let c = std::ptr::read_unaligned(ptr.add(i * std::mem::size_of::<char>()) as *mut char);
                        let mut b = [0; 4];
                        let s = c.encode_utf8(&mut b);
                        debug_assert!(s.len() <= 4);
                        v.replace_utf8(&s, len);
                        len += s.len();
                    }
                    v.set_len(len);
                    v
                }
            }
        )*
    };
}
_impl_inline_str!(u8 u16 u32 u64 usize);

macro_rules! _impl_test_inline_str {
    ($ty:ty, $bytes:literal, $print:literal) => {{
        let bytes = $bytes.to_vec();
        let bytes = bytes.as_slice();
        let inline = unsafe { InlineNChar::<$ty>::from_ptr(bytes.as_ptr()) };
        dbg!(inline);
        assert_eq!(inline.len(), 16);
        assert_eq!(format!("{}", inline), "abcd");
        assert_eq!(inline.inlined(), bytes);
        assert_eq!(inline.printable_inlined(), $print);
        dbg!(unsafe { inline.into_inline_str() });
    }};
}

#[test]
fn test_inline_nchar() {
    _impl_test_inline_str!(
        u8,
        b"\x10a\x00\x00\x00b\x00\x00\x00c\x00\x00\x00d\x00\x00\x00",
        "\\x10a\\x00\\x00\\x00b\\x00\\x00\\x00c\\x00\\x00\\x00d\\x00\\x00\\x00"
    );
    _impl_test_inline_str!(
        u16,
        b"\x10\x00a\x00\x00\x00b\x00\x00\x00c\x00\x00\x00d\x00\x00\x00",
        "\\x10\\x00a\\x00\\x00\\x00b\\x00\\x00\\x00c\\x00\\x00\\x00d\\x00\\x00\\x00"
    );
    _impl_test_inline_str!(
        u32,
        b"\x10\x00\0\0a\x00\x00\x00b\x00\x00\x00c\x00\x00\x00d\x00\x00\x00",
        "\\x10\\x00\\x00\\x00a\\x00\\x00\\x00b\\x00\\x00\\x00c\\x00\\x00\\x00d\\x00\\x00\\x00"
    );
    _impl_test_inline_str!(
        u64,
        b"\x10\0\0\0\0\0\0\0a\x00\x00\x00b\x00\x00\x00c\x00\x00\x00d\x00\x00\x00",
        "\\x10\\x00\\x00\\x00\\x00\\x00\\x00\\x00a\\x00\\x00\\x00b\\x00\\x00\\x00c\\x00\\x00\\x00d\\x00\\x00\\x00"
    );
}

#[test]
fn test_inline_nchar_2() {
    let mut bytes: [u8; 10] = [8, 0, 45, 78, 0, 0, 135, 101, 0, 0];
    let bytes = bytes.as_mut_slice();
    let inline = unsafe { InlineNChar::<u16>::from_ptr(bytes.as_mut_ptr()) };
    dbg!(&inline);
    assert_eq!(inline.inlined(), bytes);
    dbg!(inline.printable_inlined().as_str());
    let p = unsafe { inline.into_inline_str().as_str() };
    println!("{}", p);
}
