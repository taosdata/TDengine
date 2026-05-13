use std::fmt;

#[repr(C)]
#[repr(packed(1))]
pub struct InlineBytes<T = u16> {
    len: T,
    data: [u8; 0],
}

macro_rules! _impl_inline_lines {
    ($($ty:ty) *) => {
        $(
            impl fmt::Debug for InlineBytes<$ty> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct("InlineBytes")
                        .field("len", &self.len())
                        .field("data", &self.as_bytes())
                        .finish()
                }
            }

            impl fmt::Display for InlineBytes<$ty> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str(&self.printable())
                }
            }

            impl AsRef<[u8]> for InlineBytes<$ty> {
                fn as_ref(&self) -> &[u8] {
                    self.as_bytes()
                }
            }

            impl InlineBytes<$ty> {
                /// # Safety
                ///
                /// Do not use it directly.
                #[inline]
                pub unsafe fn from_ptr<'a>(ptr: *const u8) -> &'a Self {
                    &*ptr.cast::<InlineBytes<$ty>>()
                }

                #[inline]
                pub const fn as_ptr(&self) -> *const u8 {
                    self.data.as_ptr()
                }

                #[inline]
                pub fn as_mut_ptr(&mut self) -> *mut u8 {
                    self.data.as_mut_ptr()
                }

                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn as_bytes(&self) -> &[u8] {
                    unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len()) }
                }

                #[inline]
                pub fn printable(&self) -> String {
                    String::from_utf8(self.as_bytes().iter().flat_map(|b| b.escape_ascii().into_iter()).collect()).expect("")
                }

                #[inline]
                pub const fn len(&self) -> usize {
                    self.len as _
                }

                #[inline]
                pub fn encode(v: &[u8]) -> Vec<u8> {
                    let len = v.len() as $ty;
                    let mut vec = Vec::with_capacity(v.len() + std::mem::size_of::<$ty>());
                    vec.extend(len.to_le_bytes());
                    vec.extend(v);
                    vec
                }
            }
        )*
    };
}

_impl_inline_lines!(u8 u16 u32 u64 usize);

macro_rules! _impl_test_inline_lines {
    ($ty:ty, $bytes:literal) => {{
        let bytes = $bytes;
        let inline = unsafe { InlineBytes::<$ty>::from_ptr(bytes.as_ptr()) };
        dbg!(inline);
        assert_eq!(inline.len(), 4);
        assert_eq!(inline.as_ref(), b"abcd");
        assert_eq!(inline.to_string(), "abcd");
        assert_eq!(InlineBytes::<$ty>::encode(b"abcd"), bytes);
    }};
}

#[test]
fn test_inline_lines() {
    _impl_test_inline_lines!(u8, b"\x04abcd");
    _impl_test_inline_lines!(u16, b"\x04\x00abcd");
    _impl_test_inline_lines!(u32, b"\x04\x00\x00\x00abcd");
    _impl_test_inline_lines!(u64, b"\x04\x00\x00\x00\x00\x00\x00\x00abcd");
}
