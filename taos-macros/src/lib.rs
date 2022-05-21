extern crate proc_macro;

mod cfg;
mod test;

/// A `[cfg]`-like macro to add backport code for extern "C" foreign mod.
///
/// ```rust
/// # use taos_macros::c_cfg;
///
/// #[c_cfg(feature = "ft1")]
/// extern "C" {
///     pub fn raw_c_fn(arg1: *mut std::os::raw::c_void) -> std::os::raw::c_int;
/// }
/// ```
///
/// The code will expand to:
///
/// ```rust
/// #[cfg(feature = "ft1")]
/// extern "C" {
///     pub fn raw_c_fn(arg1: *mut std::os::raw::c_void) -> std::os::raw::c_int;
/// }
/// #[cfg(not(feature = "ft1"))]
/// #[no_mangle]
/// pub extern "C" fn raw_c_fn(arg1: *mut std::os::raw::c_void) -> std::os::raw::c_int {
///     panic!("C function raw_c_fn is not supported in this build");
/// }
/// ```
#[proc_macro_attribute]
pub fn c_cfg(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    cfg::cfg(attr, item).into()
}

/// A powerful test macro for taos, you can replace std test macro in your taos test cases.
///
/// ## Use as [`[test]`](https://doc.rust-lang.org/std/prelude/v1/macro.test.html) does
///
/// ```rust
/// use taos_macros::test;
/// #[test]
/// fn test1() {}
/// ```
///
/// ## Use with taos connection.
///
/// ```rust
/// use taos_macros::test;
///
/// #[test]
/// async fn show_databases(taos: &Taos) -> Result<()> {
///     let _ = taos.databases().await;
///     Ok(())
/// }
/// ```
///
/// ## Use with taos connection and a prebuilt database
///
/// ```rust
/// use taos_macros::test;
/// use taos::{Taos, Result};
///
/// #[test]
/// async fn use_database(taos: &Taos, database: &str) -> Result<()> {
///     let databases = taos.databases().await?;
///     assert!(databases.iter().any(|db| db.name == database));
///     Ok(())
/// }
/// ```
///
/// ## Use with taos connection and a prebuilt database with specific precision
///
/// ```rust
/// use taos_macros::test;
/// use taos::{Taos, Result};
///
/// #[test(precision = "ns")]
/// async fn with_precision(taos: &Taos, database: &str) -> Result<()> {
///     let databases = taos.databases().await?;
///     assert!(databases
///         .iter()
///         .any(|db| db.name == database && db.props.precision.as_ref().unwrap().eq("ns")));
///     Ok(())
/// }
/// ```
///
/// ## Specify a custom database name
///
/// ```rust
/// use taos_macros::test;
/// use taos::{Taos, Result};
///
/// #[test(naming = "abc1")]
/// async fn custom_database(taos: &Taos, database: &str) -> Result<()> {
///     let databases = taos.databases().await?;
///     assert!(database == "abc1");
///     assert!(databases.iter().any(|db| db.name == database));
///     Ok(())
/// }
/// ```
///
/// ## Naming the database with random words
///
/// By default, `[test]` macro use [uuid v1](https://docs.rs/uuid/latest/uuid/struct.Uuid.html#method.new_v1)
/// naming strategy. It will satisfy most cases. But if you want to use another naming strategy, you can use
/// `"random"` to generate database names with [faker_rand](https://docs.rs/faker_rand).
///
/// ```rust
/// use taos_macros::test;
/// use taos::{Taos, Result};
///
/// #[test(naming = "random")]
/// async fn random_naming(taos: &Taos, database: &str) -> Result<()> {
///     let databases = taos.databases().await?;
///     assert!(databases.iter().any(|db| db.name == database));
///     Ok(())
/// }
/// ```
///
/// ## Generate many databases in one case
///
/// ```rust
/// use taos_macros::test;
/// use taos::{Taos, Result};
///
/// #[test(databases = 10)]
/// async fn multi_databases(_taos: &Taos, databases: &[&str]) -> Result<()> {
///     assert!(databases.len() == 10);
///     Ok(())
/// }
/// ```
///
#[proc_macro_attribute]
pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    test::test(attr, item)
}
