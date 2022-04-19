use taos_macros::c_cfg;

#[c_cfg(v3)]
extern "C" {
    fn test_cfg(_a: usize, _b: *mut usize);
}
