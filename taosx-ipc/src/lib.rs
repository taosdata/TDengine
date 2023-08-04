pub mod ack;

pub mod stream;

pub mod types;

pub(crate) mod constants {
    pub const __TYPE__: &str = "__type__";
    pub const __TABLES__: &str = "__tables__";
    pub const __TABLES__INDEX__: usize = 1;
    pub const __ATTRS__: &'static str = "__attrs__";
    pub const __RECORDS__: &'static str = "__records__";
    pub const __TABLE_NAME__: &'static str = "__table_name__";
}

pub mod prelude {
    pub use crate::ack::*;
    pub use crate::stream::reader::*;
    pub use crate::stream::writer::*;
}
