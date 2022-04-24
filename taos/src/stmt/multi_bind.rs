use std::{marker::PhantomData, mem::ManuallyDrop};

use bitvec_simd::BitVec;

use taos_sys::{TAOS_MULTI_BIND};
use taos_query::common::Ty;

#[derive(Debug)]
pub struct MultiBind<'a>(TAOS_MULTI_BIND, PhantomData<&'a u8>);

unsafe impl<'a> Send for MultiBind<'a> {}
unsafe impl<'a> Sync for MultiBind<'a> {}

pub(crate) trait TaosTypeOf {
    fn taos_type_of() -> Ty;
}

macro_rules! impl_taos_type_of {
    ($f:ty, $t:ident) => {
        impl TaosTypeOf for $f {
            #[inline]
            fn taos_type_of() -> Ty {
                Ty::$t
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

impl<'a> MultiBind<'a> {
    pub(crate) fn nulls(n: usize) -> Self {
        Self(
            TAOS_MULTI_BIND {
                buffer_type: Ty::Null as _,
                buffer: std::ptr::null_mut(),
                buffer_length: 0,
                length: n as _,
                is_null: std::ptr::null_mut(),
                num: n as _,
            },
            PhantomData,
        )
    }
    pub(crate) fn from_primitives<T: TaosTypeOf>(nulls: &BitVec, values: &[T]) -> Self {
        Self(
            TAOS_MULTI_BIND {
                buffer_type: dbg!(T::taos_type_of()) as _,
                buffer: values.as_ptr() as _,
                buffer_length: std::mem::size_of::<T>(),
                length: values.len() as _,
                is_null: ManuallyDrop::new(nulls.clone().into_bools()).as_ptr() as _,
                num: values.len() as _,
            },
            PhantomData,
        )
    }
    pub(crate) fn from_raw_timestamps(nulls: &BitVec, values: &[i64]) -> Self {
        Self(
            TAOS_MULTI_BIND {
                buffer_type: Ty::Timestamp as _,
                buffer: values.as_ptr() as _,
                buffer_length: std::mem::size_of::<i64>(),
                length: values.len() as _,
                is_null: ManuallyDrop::new(nulls.clone().into_bools()).as_ptr() as _,
                num: values.len() as _,
            },
            PhantomData,
        )
    }

    pub(crate) fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut buffer_length = 0;
        let num = values.len();
        let mut nulls = ManuallyDrop::new(Vec::with_capacity(num));
        unsafe { nulls.set_len(num) };
        nulls.fill(false);
        let mut length: ManuallyDrop<Vec<i32>> = ManuallyDrop::new(Vec::with_capacity(num));
        unsafe { length.set_len(num) };
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
        let mut buffer: ManuallyDrop<Vec<u8>> = ManuallyDrop::new(Vec::with_capacity(buffer_size));
        unsafe { buffer.set_len(buffer_size) };
        buffer.fill(0);
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                unsafe {
                    let dst = buffer.as_mut_ptr().add(buffer_length * i);
                    std::intrinsics::copy_nonoverlapping(v.as_ptr(), dst, v.len());
                }
            }
        }
        Self(
            TAOS_MULTI_BIND {
                buffer_type: Ty::VarChar as _,
                buffer: buffer.as_ptr() as _,
                buffer_length,
                length: length.as_ptr() as _,
                is_null: nulls.as_ptr() as _,
                num: num as _,
            },
            PhantomData,
        )
    }
    pub(crate) fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|f| {
                f.as_ref()
                    .map(|s| dbg!(s.as_ref().to_string()).into_bytes())
            })
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.0.buffer_type = Ty::NChar as _;
        s
    }
}

impl<'a> Drop for MultiBind<'a> {
    fn drop(&mut self) {
        let ty = Ty::from(self.0.buffer_type as u8);
        if ty == Ty::VarChar || ty == Ty::NChar {
            let len = self.0.buffer_length * self.0.num as usize;
            unsafe { Vec::from_raw_parts(self.0.buffer as *mut u8, len, len as _) };
            unsafe {
                Vec::from_raw_parts(self.0.length as *mut i32, self.0.num as _, self.0.num as _)
            };
        }
        unsafe { Vec::from_raw_parts(self.0.is_null as *mut i8, self.0.num as _, self.0.num as _) };
    }
}
