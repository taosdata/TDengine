pub mod taoz;

mod tmq;


#[derive(clap::ValueEnum, Clone, Debug)]
enum Compression {
    None,
    Brotli,
    Bzip2,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}


mod tmq_to_td;
pub use tmq_to_td::tmq_to_td;

mod tmq_to_local;
pub use tmq_to_local::tmq_to_local;

mod local_to_taos;
pub use local_to_taos::local_to_taos;
