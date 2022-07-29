use std::fmt;

use super::Inlinable;

#[repr(C)]
#[repr(packed(1))]
pub struct InlineNChar<T = u16> {
    len: T,
    data: [u8; 0],
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

            impl AsRef<[char]> for InlineNChar<$ty> {
                #[inline]
                fn as_ref(&self) -> &[char] {
                    self.chars()
                }
            }
            impl AsRef<[u8]> for InlineNChar<$ty> {
                #[inline]
                fn as_ref(&self) -> &[u8] {
                    self.as_bytes()
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
                pub fn from_ptr<'a>(ptr: *const u8) -> &'a Self {
                    unsafe { std::mem::transmute::<*const u8, &InlineNChar<$ty>>(ptr) }
                }
                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn as_bytes(&self) -> &[u8] {
                    unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len()) }
                }
                #[inline]
                pub fn to_string(&self) -> String {
                    self.chars().iter().collect()
                }

                #[inline]
                pub const fn len(&self) -> usize {
                    self.len as _
                }
                #[inline]
                pub const fn chars_len(&self) -> usize {
                    self.len() / std::mem::size_of::<char>()
                }

                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn chars(&self) -> &[char] {
                    unsafe { std::slice::from_raw_parts(self.data.as_ptr() as _, self.chars_len()) }
                }

                #[inline]
                #[allow(mutable_transmutes)]
                pub unsafe fn into_inline_str(&self) -> &super::InlineStr<$ty> {
                    if self.len() == 0 {
                        return std::mem::transmute(self);
                    }
                    let v: &mut super::InlineStr<$ty> = std::mem::transmute(self);
                    let ptr = self.data.as_ptr() as *mut u8;
                    let chars = self.chars();
                    let mut len = 0usize;
                    for c in chars {
                        let mut b = [0; 4];
                        let s = c.encode_utf8(&mut b);
                        std::ptr::copy_nonoverlapping(s.as_ptr(), ptr.offset(len as isize), s.len());
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
        let inline = InlineNChar::<$ty>::from_ptr(bytes.as_ptr());
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
