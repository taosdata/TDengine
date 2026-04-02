use std::fmt;

use super::Inlinable;

#[repr(C)]
#[repr(packed(1))]
pub struct InlineJson<T = u16> {
    len: T,
    data: [u8; 0],
}
macro_rules! _impl_inline_str {
    ($($ty:ty) *) => {
        $(

            impl fmt::Debug for InlineJson<$ty> {
                #[inline]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct("InlineJson")
                        .field("len", &self.len())
                        .field("data", &self.as_str())
                        .finish()
                }
            }

            impl fmt::Display for InlineJson<$ty> {
                #[inline]
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str(self.as_str())
                }
            }

            impl AsRef<str> for InlineJson<$ty> {
                #[inline]
                fn as_ref(&self) -> &str {
                    self.as_str()
                }
            }

            impl Inlinable for InlineJson<$ty> {
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

            impl InlineJson<$ty> {
                #[inline]
                /// # Safety
                ///
                pub unsafe fn from_ptr<'a>(ptr: *const u8) -> &'a Self {
                    &*ptr.cast::<InlineJson<$ty>>()
                }

                #[inline]
                pub fn as_ptr(&self) -> *const u8 {
                    self.data.as_ptr()
                }

                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn as_bytes(&self) -> &[u8] {
                    unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len()) }
                }

                #[inline]
                #[rustversion::attr(nightly, const)]
                pub fn as_str(&self) -> &str {
                    unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
                }

                /// Parse inline bytes to json value.
                ///
                /// # Panic
                ///
                /// It will panic when the inner bytes is not valid to parse as json.
                // pub fn as_json(&self) -> simd_json::BorrowedValue {
                //     let slice = unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr() as *mut u8, self.len()) };
                //     simd_json::to_borrowed_value(slice).unwrap()
                // }

                #[inline]
                pub const fn len(&self) -> usize {
                    self.len as _
                }
            }
        )*
    };
}
_impl_inline_str!(u8 u16 u32 u64 usize);

#[test]
fn test_inline_str() {
    // use simd_json::Writable;
    let bytes =
        b"\x18\0{\"a\":\"\xe6\xb6\x9b\xe6\x80\x9d\xf0\x9d\x84\x9e\xe6\x95\xb0\xe6\x8d\xae\"}";
    let json = "{\"a\":\"涛思𝄞数据\"}";
    let inline = unsafe { InlineJson::<u16>::from_ptr(bytes.as_ptr()) };
    dbg!(inline);
    assert_eq!(inline.len(), 24);
    assert_eq!(inline.as_ref(), json);
    // dbg!(inline.as_json());
    // assert_eq!(format!("{}", inline.as_json().encode()), json);
    assert_eq!(format!("{}", inline), json);
    assert_eq!(inline.inlined(), bytes);
    assert_eq!(inline.printable_inlined(), "\\x18\\x00{\\\"a\\\":\\\"\\xe6\\xb6\\x9b\\xe6\\x80\\x9d\\xf0\\x9d\\x84\\x9e\\xe6\\x95\\xb0\\xe6\\x8d\\xae\\\"}");
}
