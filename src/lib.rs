mod csv;
mod local_to_taos;
mod parquets;
pub mod taoz;
mod tmq;
mod tmq_to_local;
mod tmq_to_td;

use std::future::Future;
use std::pin::Pin;

use taos::{Dsn, IntoDsn};

pub use csv::*;
pub use local_to_taos::local_to_taos;
pub use parquets::*;
pub use tmq_to_local::tmq_to_local;
pub use tmq_to_td::tmq_to_td;

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

#[derive(Debug, Default)]
pub struct TaskOpts {
    pub from: Dsn,
    pub to: Dsn,
    pub jobs: usize,
    pub compression_level: Option<usize>,
    pub force: bool,
}

impl TaskOpts {
    pub fn synchronize_database_from(database: &str, from: impl IntoDsn) -> anyhow::Result<Self> {
        let to = format!("taos:///{database}").parse()?;
        let from = from.into_dsn()?;
        Ok(Self {
            from,
            to,
            ..Default::default()
        })
    }
    pub fn synchronize_database_to(database: &str, to: impl IntoDsn) -> anyhow::Result<Self> {
        let from = format!("tmq:///{database}").parse()?;
        let to = to.into_dsn()?;
        Ok(Self {
            from,
            to,
            ..Default::default()
        })
    }

    pub fn subscribe_from(database: &str, from: impl IntoDsn) -> anyhow::Result<Self> {
        let to = format!("taos:///{database}").parse()?;
        let from = from.into_dsn()?;
        Ok(Self {
            from,
            to,
            ..Default::default()
        })
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let Self {
            from,
            to,
            jobs,
            compression_level: _,
            force,
        } = self;

        {
            match (from.driver.as_str(), to.driver.as_str()) {
                ("tmq", "taos") => {
                    tmq_to_td(from, to, jobs).await?;
                }
                ("tmq", "local") => {
                    tmq_to_local(from, to, jobs, force).await?;
                }
                ("local", "taos") => {
                    local_to_taos(from, to, jobs, force).await?;
                }
                ("taos", "csv") => {
                    query_to_csv(from, to).await?;
                }
                ("taos", "parquet") => {
                    query_to_parquet(from, to, force).await?;
                }
                (_, _) => anyhow::bail!("unsupported source or target: from {} to {}", from, to),
            }
            Ok(())
        }
    }

}
