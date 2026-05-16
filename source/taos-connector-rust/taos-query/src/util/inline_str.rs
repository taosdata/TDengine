use std::fmt;

use tokio::io::*;

#[repr(C)]
#[repr(packed(1))]
pub struct InlineStr<T = u16> {
    len: T,
    data: [u8; 0],
}

macro_rules! _impl_inline_str {
    ($($ty:ty) *) => {
        $(
            impl fmt::Debug for InlineStr<$ty> {
                #[inline]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct("InlineStr")
                        .field("len", &self.len())
                        .field("data", &self.as_str())
                        .finish()
                }
            }

            impl fmt::Display for InlineStr<$ty> {
                #[inline]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str(self.as_str())
                }
            }

            impl AsRef<str> for InlineStr<$ty> {
                #[inline]
                fn as_ref(&self) -> &str {
                    self.as_str()
                }
            }

            impl AsRef<[u8]> for InlineStr<$ty> {
                #[inline]
                fn as_ref(&self) -> &[u8] {
                    self.as_bytes()
                }
            }

            impl super::Inlinable for InlineStr<$ty> {
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

            #[async_trait::async_trait]
            impl super::AsyncInlinable for InlineStr<$ty> {
                #[inline]
                async fn write_inlined<W: AsyncWrite + Unpin + Send>(&self, wtr: &mut W) -> std::io::Result<usize> {
                    let l = wtr.write(&self.len.to_le_bytes()).await?;
                    Ok(l + wtr.write(self.as_bytes()).await?)
                }

                async fn read_inlined<R: AsyncRead + Send + Unpin>(_: &mut R) -> std::io::Result<Self> {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "can't read into a inlined string"))
                }
            }

            impl InlineStr<$ty> {
                /// # Safety
                ///
                /// Do not use it directly.
                pub unsafe fn from_ptr<'a>(ptr: *const u8) -> &'a Self {
                    &*std::mem::transmute::<*const u8, *const InlineStr<$ty>>(ptr)
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
                    if self.len() == 0 {
                        return &[]
                    }
                    unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len()) }
                }

                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn as_str(&self) -> &str {
                    unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
                }

                #[inline]
                pub const fn len(&self) -> usize {
                    self.len as _
                }

                #[inline]
                pub const fn is_empty(&self) -> bool {
                    self.len == 0
                }

                #[inline(never)]
                pub(crate) unsafe fn set_len(&mut self, len: usize) {
                    self.len = len as _;
                }

                #[inline(never)]
                pub(crate) unsafe fn replace_utf8(&mut self, s: &str, pos: usize) {
                    std::ptr::copy(s.as_ptr(), self.as_mut_ptr().add(pos), s.len());
                }
            }
        )*
    };
}

_impl_inline_str!(u8 u16 u32 u64 usize);

macro_rules! _impl_test_inline_str {
    ($ty:ty, $bytes:literal, $print:literal) => {{
        use super::Inlinable;
        let bytes = $bytes;
        let inline = unsafe { InlineStr::<$ty>::from_ptr(bytes.as_ptr()) };
        dbg!(inline);
        assert_eq!(inline.len(), 4);
        assert_eq!(inline.as_str(), "abcd");
        assert_eq!(format!("{}", inline), "abcd");
        assert_eq!(inline.inlined(), bytes);
        assert_eq!(inline.printable_inlined(), $print);
    }};
}

#[test]
fn test_inline_str() {
    _impl_test_inline_str!(u8, b"\x04abcd", "\\x04abcd");
    _impl_test_inline_str!(u16, b"\x04\x00abcd", "\\x04\\x00abcd");
    _impl_test_inline_str!(u32, b"\x04\x00\x00\x00abcd", "\\x04\\x00\\x00\\x00abcd");
    _impl_test_inline_str!(
        u64,
        b"\x04\x00\x00\x00\x00\x00\x00\x00abcd",
        "\\x04\\x00\\x00\\x00\\x00\\x00\\x00\\x00abcd"
    );
}

#[test]
fn test_empty_inline() {
    let inline = unsafe { InlineStr::<u8>::from_ptr(b"\0".as_ptr()) };
    dbg!(inline);
}
