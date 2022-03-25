use bitvec_simd::BitVec;
use taos_sys::{TaosDataType, TAOS_MULTI_BIND};

#[derive(Debug)]
pub struct MultiBind(TAOS_MULTI_BIND);

unsafe impl Send for MultiBind {}
unsafe impl Sync for MultiBind {}
pub(crate) trait TaosTypeOf {
    fn taos_type_of() -> TaosDataType;
}

macro_rules! impl_taos_type_of {
    ($f:ty, $t:ident) => {
        impl TaosTypeOf for $f {
            #[inline]
            fn taos_type_of() -> TaosDataType {
                TaosDataType::$t
            }
        }
    };
}

impl_taos_type_of!(bool, Bool);
impl_taos_type_of!(i8, TinyInt);
impl_taos_type_of!(i16, SmallInt);
impl_taos_type_of!(i32, Int);
impl_taos_type_of!(i64, BigInt);
impl_taos_type_of!(u8, UTinyInt);
impl_taos_type_of!(u16, USmallInt);
impl_taos_type_of!(u32, UInt);
impl_taos_type_of!(u64, UBigInt);
impl_taos_type_of!(f32, Float);
impl_taos_type_of!(f64, Double);

impl MultiBind {
    pub fn nulls(n: usize) -> Self {
        Self(TAOS_MULTI_BIND {
            buffer_type: TaosDataType::Null as _,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
            length: n as _,
            is_null: std::ptr::null_mut(),
            num: n as _,
        })
    }
    pub(crate) fn from_primitives<T: TaosTypeOf>(nulls: &BitVec, values: &[T]) -> Self {
        Self(TAOS_MULTI_BIND {
            buffer_type: dbg!(T::taos_type_of()) as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<bool>(),
            length: values.len() as _,
            is_null: {
                let bools = nulls.clone().into_bools();
                let ptr = bools.as_ptr();
                std::mem::forget(bools);
                ptr as _
            },
            num: values.len() as _,
        })
    }
    pub fn from_raw_timestamps(nulls: &BitVec, values: &[i64]) -> Self {
        Self(TAOS_MULTI_BIND {
            buffer_type: TaosDataType::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: values.len() as _,
            is_null: {
                let bools = nulls.clone().into_bools();
                let ptr = bools.as_ptr();
                std::mem::forget(bools);
                ptr as _
            },
            num: values.len() as _,
        })
    }

    pub fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut buffer_length = 0;
        let num = values.len();
        let mut nulls: Vec<bool> = Vec::with_capacity(values.len());
        let mut length: Vec<i32> = Vec::with_capacity(num);
        unsafe { nulls.set_len(values.len()) };
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                length[i] = v.len() as _;
                if v.len() > buffer_length {
                    buffer_length = v.len();
                }
            } else {
                nulls[i] = true;
            }
        }
        let buffer_size = buffer_length * values.len();
        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);
        unsafe { buffer.set_len(buffer_size) };
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                unsafe {
                    let dst = buffer.as_mut_ptr().offset((buffer_length * i) as isize);
                    std::intrinsics::copy_nonoverlapping(v.as_ptr(), dst, v.len());
                }
            }
        }
        // let nulls = values.iter().map(Option::is_some).collect_vec();
        let is_null = nulls.as_ptr() as _;
        std::mem::forget(nulls);
        Self(TAOS_MULTI_BIND {
            buffer_type: TaosDataType::Binary as _,
            buffer: buffer.as_ptr() as _,
            buffer_length,
            length: length.as_ptr() as _,
            is_null,
            num: values.len() as _,
        })
    }
    pub fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .into_iter()
            .map(|f| f.as_ref().map(|s| s.as_ref().to_string().into_bytes()))
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.0.buffer_type = TaosDataType::NChar as _;
        s
    }
}

impl Drop for MultiBind {
    fn drop(&mut self) {
        let ty = TaosDataType::from(self.0.buffer_type as u8);
        if ty == TaosDataType::Binary || ty == TaosDataType::NChar {
            let len = self.0.buffer_length * self.0.num as usize;
            unsafe { Vec::from_raw_parts(self.0.buffer as *mut u8, len, len as _) };
            unsafe {
                Vec::from_raw_parts(self.0.length as *mut i32, self.0.num as _, self.0.num as _)
            };
        }
        unsafe { Vec::from_raw_parts(self.0.is_null as *mut i8, self.0.num as _, self.0.num as _) };
    }
}
