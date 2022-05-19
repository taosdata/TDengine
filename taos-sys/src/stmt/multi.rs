use taos_query::common::{
    itypes::{IsPrimitive, IsValue},
    Ty,
};

use crate::{BindFrom, TaosBind, TaosBindV2, TaosMultiBind};

fn box_into_raw<T>(v: T) -> *mut T {
    Box::into_raw(Box::new(v))
}

// impl<T> From<T> for TaosMultiBind
// where
//     T: IsValue,
// {
//     #[inline(always)]
//     fn from(v: T) -> Self {
//         macro_rules! as_is {
//             () => {{
//                 let mut param = Self::new(T::TY);
//                 param.buffer_length = T::TY.fixed_length();
//                 param.buffer = box_into_raw(v) as _;
//                 param.length = box_into_raw(param.buffer_length) as _;
//                 param
//             }};
//             ($v:expr) => {{
//                 let mut param = Self::new(T::TY);
//                 param.buffer_length = T::TY.fixed_length();
//                 param.buffer = box_into_raw($v) as _;
//                 param.length = box_into_raw(param.buffer_length) as _;
//                 param
//             }};
//         }

//         match T::TY {
//             Ty::Null => Self::null(),
//             Ty::Bool => as_is!(),
//             Ty::TinyInt => as_is!(),
//             Ty::SmallInt => as_is!(),
//             Ty::Int => as_is!(),
//             Ty::BigInt => as_is!(),
//             Ty::UTinyInt => as_is!(),
//             Ty::USmallInt => as_is!(),
//             Ty::UInt => as_is!(),
//             Ty::UBigInt => as_is!(),
//             Ty::Float => as_is!(),
//             Ty::Double => as_is!(),
//             Ty::Timestamp => {
//                 as_is!(v.as_timestamp())
//             }
//             Ty::VarChar => Self::from_varchar(v.as_var_char()),
//             Ty::NChar => Self::from_nchar(v.as_nchar()),
//             Ty::Json => todo!(),
//             _ => Self::null(),
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use crate::{TaosBind, TaosBindV2};

    #[test]
    fn bind_bool() {
        for v in [true, false].iter() {
            let bind = TaosBindV2::from(v);
            dbg!(&bind);
            let v1 = unsafe { (bind.buffer as *const bool).read() };
            assert!(v1.eq(v));
        }
    }

    #[test]
    fn bind_i8() {
        for v in [0i8, 1i8].iter() {
            let bind = TaosBindV2::from(v);
            dbg!(&bind);
            let v1 = unsafe { (bind.buffer as *const i8).read() };
            assert!(v1.eq(v));
        }
    }
}
