use std::{
    borrow::Cow,
    ffi::c_void,
    fmt::{Display, Write},
};

use bytes::Bytes;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    common::{Field, Ty},
    // prelude::AsyncInlinable,
    util::{Inlinable, InlinableRead},
};

const RAW_TYPE_OFFSET: usize = std::mem::size_of::<u32>();
const RAW_PTR_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u16>();

/// C-struct for raw data, just a data view from native library.
///
/// It can be copy/cloned, but should not use it outbound away a offset lifetime.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct raw_data_t {
    pub raw: *const c_void,
    pub raw_len: u32,
    pub raw_type: u16,
}

impl raw_data_t {
    fn to_bytes(&self) -> Bytes {
        let cap = // raw data len
            self.raw_len as usize +
            // self.raw_mem_len
            std::mem::size_of::<u32>() +
            // self.raw_meta
            std::mem::size_of::<u16>();
        let mut data = Vec::with_capacity(cap);

        // first 4 bytes: raw_len
        data.extend(self.raw_len.to_le_bytes());

        // next 2 bytes: raw_type
        data.extend(self.raw_type.to_le_bytes());

        unsafe {
            let ptr = data.as_mut_ptr().offset(RAW_PTR_OFFSET as isize);
            std::ptr::copy_nonoverlapping(self.raw, ptr as _, self.raw_len as _);
            data.set_len(cap);
        }
        Bytes::from(data)
    }
}

#[derive(Debug, Clone)]
enum RawDataInner {
    Raw(raw_data_t),
    Data(Bytes),
}

impl RawDataInner {
    fn raw_len(&self) -> u32 {
        match self {
            RawDataInner::Raw(raw) => raw.raw_len,
            RawDataInner::Data(bytes) => unsafe { *(bytes.as_ptr() as *const u32) },
        }
    }
    fn raw_type(&self) -> u16 {
        match self {
            RawDataInner::Raw(raw) => raw.raw_type,
            RawDataInner::Data(bytes) => unsafe {
                *(bytes.as_ptr().offset(std::mem::size_of::<u32>() as _) as *const u16)
            },
        }
    }
    fn raw(&self) -> *const c_void {
        match self {
            RawDataInner::Raw(raw) => raw.raw,
            RawDataInner::Data(bytes) => unsafe { bytes.as_ptr().offset(RAW_PTR_OFFSET as _) as _ },
        }
    }
    fn into_bytes(self) -> Self {
        Self::Data(self.as_bytes().into_owned())
    }
    fn as_bytes(&self) -> Cow<Bytes> {
        match self {
            Self::Raw(raw) => Cow::Owned(raw.to_bytes()),
            Self::Data(bytes) => Cow::Borrowed(bytes),
        }
    }
}

#[derive(Debug)]
pub struct RawData(RawDataInner);

unsafe impl Send for RawData {}
unsafe impl Sync for RawData {}

impl From<raw_data_t> for RawData {
    fn from(raw: raw_data_t) -> Self {
        RawData(RawDataInner::Raw(raw))
    }
}

impl<T: Into<Bytes>> From<T> for RawData {
    fn from(bytes: T) -> Self {
        RawData(RawDataInner::Data(bytes.into()))
    }
}

impl RawData {
    pub fn new(raw: Bytes) -> Self {
        raw.into()
    }
    pub fn raw(&self) -> *const c_void {
        self.0.raw()
    }
    pub fn raw_len(&self) -> u32 {
        self.0.raw_len()
    }
    pub fn raw_type(&self) -> u16 {
        self.0.raw_type()
    }

    pub fn as_raw_data_t(&self) -> raw_data_t {
        raw_data_t {
            raw: self.raw(),
            raw_len: self.raw_len(),
            raw_type: self.raw_type(),
        }
    }

    pub fn as_bytes(&self) -> Cow<Bytes> {
        self.0.as_bytes()
    }
}

impl Inlinable for RawData {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut data = Vec::new();

        let len = reader.read_u32()?;
        data.extend(len.to_le_bytes());

        let meta_type = reader.read_u16()?;
        data.extend(meta_type.to_le_bytes());

        data.resize(data.len() + len as usize, 0);

        let buf = &mut data[RAW_PTR_OFFSET..];

        reader.read_exact(buf)?;
        Ok(data.into())
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        let bytes = self.0.as_bytes();
        wtr.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

#[async_trait::async_trait]
impl crate::util::AsyncInlinable for RawData {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        use tokio::io::*;
        let mut data = Vec::new();

        let len = reader.read_u32_le().await?;
        data.extend(len.to_le_bytes());

        let meta_type = reader.read_u16_le().await?;
        data.extend(meta_type.to_le_bytes());

        data.resize(data.len() + len as usize, 0);

        let buf = &mut data[RAW_PTR_OFFSET..];

        reader.read_exact(buf).await?;
        Ok(data.into())
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use tokio::io::*;
        let bytes = self.0.as_bytes();
        wtr.write_all(&bytes).await?;
        Ok(bytes.len())
    }
}
