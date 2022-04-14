use taos::Taos;
use taos_macros::taos_cfg;

#[taos_cfg(v3)]
fn test_cfg(a: usize, b: *mut usize);


// #[taos_cfg(v3)]
// {
//   fn test_cfg(a: usize, b: *mut usize);
//   fn test_cfg2(a: usize, b: *mut usize);
// }
