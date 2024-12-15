use std::sync::OnceLock;

pub static mut DRY_RUN: bool = false;
pub static mut SQL_TAG_CACHE_CAPACITY: usize = 0;

pub static TABLE_TAG_CACHE: OnceLock<scc::HashSet<String>> = OnceLock::new();
