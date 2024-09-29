pub mod ack;

pub mod stream;

pub mod types;

pub(crate) mod constants {
    pub const __TYPE__: &str = "__type__";
    pub const __TABLES__: &str = "__tables__";
    pub const __TABLES_INDEX__: usize = 1;
    pub const __ATTRS__: &str = "__attrs__";
    pub const __RECORDS__: &str = "__records__";
    pub const __TABLE_NAME__: &str = "__table_name__";
    pub const __CONTROL__: &str = "__control__";
}

pub mod prelude {
    pub use crate::ack::*;
    pub use crate::stream::reader::*;
    pub use crate::stream::writer::*;
}
