fn box_into_raw<T>(v: T) -> *mut T {
    Box::into_raw(Box::new(v))
}


#[cfg(test)]
mod tests {
    use crate::types::TaosBindV2;

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
