use std::{
    ffi::{c_char, CString},
    ptr, slice,
};

use bytes::Bytes;
use derive_more::Deref;
use taos_query::{
    common::{
        decimal::Decimal,
        itypes::{IValue, IsValue},
        ColumnView, Ty,
    },
    stmt2::Stmt2BindParam,
    RawError, RawResult,
};

use crate::types::{BindFrom, TaosStmt2Bind, TaosStmt2Bindv};

#[derive(Debug, Deref)]
#[repr(transparent)]
struct Stmt2BindTag(TaosStmt2Bind);

impl Stmt2BindTag {
    fn new(ty: Ty) -> Self {
        Self(TaosStmt2Bind {
            buffer_type: ty as _,
            buffer: ptr::null_mut(),
            length: ptr::null_mut(),
            is_null: ptr::null_mut(),
            num: 1,
        })
    }

    fn free(&self) {
        if !self.is_null.is_null() {
            let _ = unsafe { Box::from_raw(self.is_null) };
        }

        if !self.length.is_null() {
            let _ = unsafe { Box::from_raw(self.length) };
        }

        if !self.buffer.is_null() {
            let ty = Ty::from(self.buffer_type as u8);
            match ty {
                Ty::Bool => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut bool) };
                }
                Ty::TinyInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i8) };
                }
                Ty::SmallInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i16) };
                }
                Ty::Int => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i32) };
                }
                Ty::BigInt | Ty::Timestamp => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i64) };
                }
                Ty::UTinyInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u8) };
                }
                Ty::USmallInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u16) };
                }
                Ty::UInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u32) };
                }
                Ty::UBigInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u64) };
                }
                Ty::Float => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut f32) };
                }
                Ty::Double => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut f64) };
                }
                Ty::Json => {
                    let _ = unsafe { CString::from_raw(self.buffer as *mut c_char) };
                }
                _ => (),
            }
        }
    }
}

impl BindFrom for Stmt2BindTag {
    fn null() -> Self {
        let mut bind = Self::new(Ty::Null);
        bind.0.is_null = Box::into_raw(Box::new(1i8)) as _;
        bind
    }

    fn from_primitive<T: IsValue + Clone>(value: &T) -> Self {
        let mut bind = Self::new(T::TY);
        bind.0.buffer = Box::into_raw(Box::new(value.clone())) as _;
        bind.0.length = Box::into_raw(Box::new(value.fixed_length() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_timestamp(value: i64) -> Self {
        let mut bind = Self::new(Ty::Timestamp);
        bind.0.buffer = Box::into_raw(Box::new(value)) as _;
        bind.0.length = Box::into_raw(Box::new(std::mem::size_of::<i64>() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_varchar(value: &str) -> Self {
        let mut bind = Self::new(Ty::VarChar);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_nchar(value: &str) -> Self {
        let mut bind = Self::new(Ty::NChar);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_json(value: &str) -> Self {
        let mut bind = Self::new(Ty::Json);
        let buffer = CString::new(value).expect("failed to build json tag");
        bind.0.buffer = buffer.into_raw() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_varbinary(value: &Bytes) -> Self {
        let mut bind = Self::new(Ty::VarBinary);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_geometry(value: &Bytes) -> Self {
        let mut bind = Self::new(Ty::Geometry);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }
}

impl Drop for Stmt2BindTag {
    fn drop(&mut self) {
        self.free();
    }
}

#[derive(Debug, Deref)]
#[repr(transparent)]
struct Stmt2BindColumn(TaosStmt2Bind);

impl Stmt2BindColumn {
    fn from_primitive<T: IValue>(nulls: Vec<bool>, buffer_ptr: *const T) -> Self {
        Self(TaosStmt2Bind {
            buffer_type: T::TY as _,
            buffer: buffer_ptr as _,
            length: ptr::null_mut(),
            num: nulls.len() as _,
            is_null: into_raw_slice(nulls) as _,
        })
    }

    fn from_timestamp(nulls: Vec<bool>, buffer_ptr: *const i64) -> Self {
        Self(TaosStmt2Bind {
            buffer_type: Ty::Timestamp as _,
            buffer: buffer_ptr as _,
            length: ptr::null_mut(),
            num: nulls.len() as _,
            is_null: into_raw_slice(nulls) as _,
        })
    }

    fn from_varchar(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        Self::from_bytes(Ty::VarChar, values)
    }

    fn from_nchar(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        Self::from_bytes(Ty::NChar, values)
    }

    fn from_json(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        Self::from_bytes(Ty::Json, values)
    }

    fn from_varbinary(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        Self::from_bytes(Ty::VarBinary, values)
    }

    fn from_geometry(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        Self::from_bytes(Ty::Geometry, values)
    }

    fn from_decimal(values: &[Option<Decimal<i128>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|v| v.as_ref().map(|d| d.to_string().into_bytes()))
            .collect();
        Self::from_bytes(Ty::Decimal, &values)
    }

    fn from_decimal64(values: &[Option<Decimal<i64>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|v| v.as_ref().map(|d| d.to_string().into_bytes()))
            .collect();
        Self::from_bytes(Ty::Decimal64, &values)
    }

    fn from_blob(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        Self::from_bytes(Ty::Blob, values)
    }

    fn from_bytes(ty: Ty, values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let num = values.len();
        let mut nulls = vec![true; num];
        let mut lengths = vec![0i32; num];

        let mut buffer_size = 0;
        for (i, value) in values.iter().enumerate() {
            if let Some(value) = value {
                let bytes = value.as_ref();
                nulls[i] = false;
                lengths[i] = bytes.len() as _;
                buffer_size += bytes.len();
            }
        }

        let mut buffer = vec![0u8; buffer_size];
        let mut offset = 0;
        for value in values.iter().flatten() {
            let bytes = value.as_ref();
            let end = offset + bytes.len();
            buffer[offset..end].copy_from_slice(bytes);
            offset = end;
        }
        debug_assert_eq!(offset, buffer_size);

        Self(TaosStmt2Bind {
            buffer_type: ty as _,
            buffer: into_raw_slice(buffer) as _,
            length: into_raw_slice(lengths),
            is_null: into_raw_slice(nulls) as _,
            num: num as _,
        })
    }

    fn free(&self) {
        from_raw_slice(self.is_null as *mut bool, self.num as _);

        if !self.buffer.is_null() {
            let ty = Ty::from(self.buffer_type as u8);
            use Ty::*;
            if matches!(
                ty,
                VarChar | NChar | Json | VarBinary | Geometry | Decimal | Decimal64 | Blob
            ) && !self.length.is_null()
            {
                let lengths =
                    unsafe { slice::from_raw_parts(self.length as *const _, self.num as _) };
                let total: usize = lengths.iter().map(|&l| l as usize).sum();
                from_raw_slice(self.buffer as *mut u8, total);
            }
        }

        from_raw_slice(self.length, self.num as _);
    }
}

impl<'a> From<&'a ColumnView> for Stmt2BindColumn {
    fn from(view: &'a ColumnView) -> Self {
        use ColumnView::*;
        match view {
            Bool(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            TinyInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            SmallInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            Int(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            BigInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            Float(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            Double(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            UTinyInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            USmallInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            UInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            UBigInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_primitive(nulls, view.as_raw_ptr())
            }
            Timestamp(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                Self::from_timestamp(nulls, view.as_raw_ptr())
            }
            VarChar(view) => Self::from_varchar(&view.to_vec()),
            NChar(view) => Self::from_nchar(&view.to_vec()),
            Json(view) => Self::from_json(&view.to_vec()),
            VarBinary(view) => Self::from_varbinary(&view.to_vec()),
            Geometry(view) => Self::from_geometry(&view.to_vec()),
            Decimal(view) => Self::from_decimal(&view.to_vec()),
            Decimal64(view) => Self::from_decimal64(&view.to_vec()),
            Blob(view) => Self::from_blob(&view.to_vec()),
        }
    }
}

impl Drop for Stmt2BindColumn {
    fn drop(&mut self) {
        self.free();
    }
}

pub struct Stmt2Bindv {
    bindv: TaosStmt2Bindv,
    tag_lens: Vec<usize>,
    col_lens: Vec<usize>,
}

impl Stmt2Bindv {
    pub fn as_ptr(&self) -> *const TaosStmt2Bindv {
        &self.bindv as _
    }

    fn free(&self) {
        unsafe {
            let count = self.bindv.count as usize;
            let tbnames = self.bindv.tbnames;
            let tags = self.bindv.tags;
            let cols = self.bindv.bind_cols;

            for i in 0..count as isize {
                let tbname = *tbnames.offset(i);
                if !tbname.is_null() {
                    let _ = CString::from_raw(tbname);
                }

                let tag = *tags.offset(i) as *mut Stmt2BindTag;
                from_raw_slice(tag, self.tag_lens[i as usize]);

                let col = *cols.offset(i) as *mut Stmt2BindColumn;
                from_raw_slice(col, self.col_lens[i as usize]);
            }

            from_raw_slice(tbnames, count);
            from_raw_slice(tags, count);
            from_raw_slice(cols, count);
        }
    }
}

impl Drop for Stmt2Bindv {
    fn drop(&mut self) {
        self.free();
    }
}

pub fn build_bindv(params: &[Stmt2BindParam]) -> RawResult<Stmt2Bindv> {
    let count = params.len();
    let mut tbnames = Vec::with_capacity(count);
    let mut tag_lens = Vec::with_capacity(count);
    let mut tag_ptrs = Vec::with_capacity(count);
    let mut col_lens = Vec::with_capacity(count);
    let mut col_ptrs = Vec::with_capacity(count);

    for param in params {
        if let Some(tbname) = param.table_name() {
            let tbname = CString::new(tbname.as_str()).map_err(RawError::from_any)?;
            tbnames.push(tbname.into_raw());
        } else {
            tbnames.push(ptr::null_mut());
        }

        if let Some(tags) = param.tags() {
            let mut ts = Vec::with_capacity(tags.len());
            for tag in tags {
                ts.push(Stmt2BindTag::from_value(tag));
            }
            tag_lens.push(ts.len());
            tag_ptrs.push(into_raw_slice(ts) as *mut TaosStmt2Bind);
        } else {
            tag_lens.push(0);
            tag_ptrs.push(ptr::null_mut());
        }

        if let Some(cols) = param.columns() {
            let mut cs = Vec::with_capacity(cols.len());
            for col in cols {
                cs.push(Stmt2BindColumn::from(col));
            }
            col_lens.push(cs.len());
            col_ptrs.push(into_raw_slice(cs) as *mut TaosStmt2Bind);
        } else {
            col_lens.push(0);
            col_ptrs.push(ptr::null_mut());
        }
    }

    let bindv = TaosStmt2Bindv {
        count: count as _,
        tbnames: into_raw_slice(tbnames),
        tags: into_raw_slice(tag_ptrs),
        bind_cols: into_raw_slice(col_ptrs),
    };

    Ok(Stmt2Bindv {
        bindv,
        tag_lens,
        col_lens,
    })
}

fn into_raw_slice<T>(v: Vec<T>) -> *mut T {
    if v.is_empty() {
        ptr::null_mut()
    } else {
        Box::leak(v.into_boxed_slice()).as_mut_ptr()
    }
}

fn from_raw_slice<T>(ptr: *mut T, len: usize) {
    if !ptr.is_null() && len > 0 {
        let slice = ptr::slice_from_raw_parts_mut(ptr, len);
        let _ = unsafe { Box::<[T]>::from_raw(slice) };
    }
}
