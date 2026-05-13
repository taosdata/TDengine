use std::ffi::c_void;

use crate::util::Inlinable;

const RAW_PTR_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u16>();

/// C-struct for raw data, just a data view from native library.
///
/// It can be copy/cloned, but should not use it outbound away a offset lifetime.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct raw_data_t {
    pub raw: *const c_void,
    pub raw_len: u32,
    pub raw_type: u16,
}

unsafe impl Send for raw_data_t {}

/// TMQ message raw data container.
///
/// It's a wrapper for raw data from native library, and will be auto free when drop.
pub struct RawData {
    free: unsafe extern "C" fn(raw: raw_data_t) -> i32,
    raw: raw_data_t,
}

unsafe impl Send for RawData {}
unsafe impl Sync for RawData {}

impl Drop for RawData {
    /// Use native free function to free raw_data_t
    fn drop(&mut self) {
        unsafe {
            (self.free)(self.raw);
        }
    }
}

impl RawData {
    pub fn new(raw: raw_data_t, free: unsafe extern "C" fn(raw: raw_data_t) -> i32) -> Self {
        RawData { free, raw }
    }

    pub fn raw_ptr(&self) -> *const c_void {
        self.raw.raw
    }

    pub fn raw_len(&self) -> u32 {
        self.raw.raw_len
    }

    pub fn raw_type(&self) -> u16 {
        self.raw.raw_type
    }

    pub fn raw_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.raw.raw as *const u8, self.raw.raw_len as _) }
    }

    pub fn as_raw_data_t(&self) -> raw_data_t {
        self.raw
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.raw_len() as usize + RAW_PTR_OFFSET;

        let mut vec = Vec::with_capacity(len);
        vec.extend_from_slice(&self.raw_len().to_le_bytes());
        vec.extend_from_slice(&self.raw_type().to_le_bytes());
        vec.extend_from_slice(self.raw_slice());
        vec
    }
}

extern "C" fn _rust_free_raw(raw: raw_data_t) -> i32 {
    unsafe {
        let ptr = raw.raw as *mut u8;
        let len = raw.raw_len as usize;
        std::alloc::dealloc(
            ptr,
            std::alloc::Layout::from_size_align(len, 1).expect("Invalid layout"),
        );
    }
    0
}

impl Inlinable for RawData {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        use byteorder::{LittleEndian, ReadBytesExt};
        let len = ReadBytesExt::read_u32::<LittleEndian>(reader)?;
        let meta_type = ReadBytesExt::read_u16::<LittleEndian>(reader)?;

        let layout = std::alloc::Layout::from_size_align(len as _, 1).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid raw data length")
        })?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len as _) };

        match reader.read_exact(buf) {
            Ok(_) => {
                let raw = raw_data_t {
                    raw: ptr as _,
                    raw_len: len,
                    raw_type: meta_type,
                };

                let message = RawData::new(raw, _rust_free_raw);

                Ok(message)
            }
            Err(e) => {
                unsafe {
                    // free memory if read failed
                    std::alloc::dealloc(ptr, layout);
                }
                Err(e)
            }
        }
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        wtr.write_all(self.raw_len().to_le_bytes().as_ref())?;
        wtr.write_all(self.raw_type().to_le_bytes().as_ref())?;
        wtr.write_all(self.raw_slice())?;
        Ok(self.raw_len() as usize + RAW_PTR_OFFSET)
    }
}

#[async_trait::async_trait]
impl crate::util::AsyncInlinable for RawData {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        use tokio::io::*;

        let len = reader.read_u32_le().await?;
        let meta_type = reader.read_u16_le().await?;
        let layout = std::alloc::Layout::from_size_align(len as _, 1).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid raw data length")
        })?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len as _) };

        match reader.read_exact(buf).await {
            Ok(_) => {
                let raw = raw_data_t {
                    raw: buf.as_mut_ptr() as _,
                    raw_len: len,
                    raw_type: meta_type,
                };

                let message = RawData::new(raw, _rust_free_raw);

                Ok(message)
            }
            Err(e) => {
                unsafe {
                    // free memory if read failed
                    std::alloc::dealloc(buf.as_mut_ptr(), layout);
                }
                Err(e)
            }
        }
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use tokio::io::*;
        wtr.write_all(self.raw_len().to_le_bytes().as_ref()).await?;
        wtr.write_all(self.raw_type().to_le_bytes().as_ref())
            .await?;
        wtr.write_all(self.raw_slice()).await?;
        Ok(self.raw_len() as usize + 6)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::AsyncInlinable;

    #[tokio::test]
    async fn test_raw_data_async_io() {
        use std::io::Cursor;

        let mut buff = Cursor::new(vec![0; 15]);
        const RAW: &[u8] = b"hello rawdata";

        extern "C" fn _empty_free_raw(_raw: raw_data_t) -> i32 {
            0
        }

        let rawdata = RawData::new(
            raw_data_t {
                raw: RAW.as_ptr() as _,
                raw_len: RAW.len() as _,
                raw_type: 0,
            },
            _empty_free_raw,
        );

        let len = AsyncInlinable::write_inlined(&rawdata, &mut buff)
            .await
            .unwrap();
        assert_eq!(len, 19);

        buff.set_position(0);
        let raw: RawData = AsyncInlinable::read_inlined(&mut buff).await.unwrap();
        assert_eq!(raw.raw_len(), RAW.len() as u32);
        assert_eq!(raw.raw_type(), 0);
        assert_eq!(raw.raw_slice(), RAW);
    }

    #[test]
    fn test_raw_data_io() {
        use std::io::Cursor;

        let mut buff = Cursor::new(vec![0; 15]);
        const RAW: &[u8] = b"hello rawdata";

        extern "C" fn _empty_free_raw(_raw: raw_data_t) -> i32 {
            0
        }

        let rawdata = RawData::new(
            raw_data_t {
                raw: RAW.as_ptr() as _,
                raw_len: RAW.len() as _,
                raw_type: 0,
            },
            _empty_free_raw,
        );

        let len = Inlinable::write_inlined(&rawdata, &mut buff).unwrap();
        assert_eq!(len, 19);

        buff.set_position(0);
        let raw: RawData = Inlinable::read_inlined(&mut buff).unwrap();
        assert_eq!(raw.raw_len(), RAW.len() as u32);
        assert_eq!(raw.raw_type(), 0);
        assert_eq!(raw.raw_slice(), RAW);
    }
}
