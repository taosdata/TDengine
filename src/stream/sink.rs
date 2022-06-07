use std::iter::Product;

use linked_hash_map::LinkedHashMap;
use taos::query::Dsn;

#[repr(C)]
pub enum SourceType {
    Database,
}
pub trait ISSourceType {
    const SOURCE_TYPE: SourceType;
}

pub trait TaosxSource: Sized {
    type Error;
    const NAME: &'static str;
    const KEYS: &'static [&'static str];

    fn new(dsn: Dsn, opts: LinkedHashMap<&'static str, String>) -> Result<Self, Self::Error>;

    type Product;
    fn produce(&mut self) -> Self::Product;
}
