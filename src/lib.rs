mod csv;
mod legacy;
mod local_to_taos;
mod parquets;
mod taoz;
mod tmq;
mod tmq_to_local;
mod tmq_to_td;
mod transform;

use taos::{Dsn, IntoDsn};

pub use csv::*;
pub use legacy::legacy_to_taos;
pub use local_to_taos::local_to_taos;
pub use parquets::*;
pub use tmq_to_local::tmq_to_local;
pub use tmq_to_td::tmq_to_td;
use tokio_util::sync::CancellationToken;
pub use transform::Action;

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

#[derive(Debug, Default, Clone)]
pub struct TaskOpts {
    pub from: Dsn,
    pub transform: Vec<Action>,
    pub to: Dsn,
    pub jobs: usize,
    pub compression_level: Option<usize>,
    pub force: bool,
    pub cancel: CancellationToken,
}

impl Drop for TaskOpts {
    fn drop(&mut self) {
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
        }
    }
}

impl TaskOpts {
    // pub fn synchronize_database_from(database: &str, from: impl IntoDsn) -> anyhow::Result<Self> {
    //     let to = format!("taos:///{database}").parse()?;
    //     let from = from.into_dsn()?;
    //     Ok(Self {
    //         from,
    //         to,
    //         ..Default::default()
    //     })
    // }
    // pub fn synchronize_database_to(database: &str, to: impl IntoDsn) -> anyhow::Result<Self> {
    //     let from = format!("tmq:///{database}").parse()?;
    //     let to = to.into_dsn()?;
    //     Ok(Self {
    //         from,
    //         to,
    //         ..Default::default()
    //     })
    // }

    // pub fn subscribe_from(database: &str, from: impl IntoDsn) -> anyhow::Result<Self> {
    //     let to = format!("taos:///{database}").parse()?;
    //     let from = from.into_dsn()?;
    //     Ok(Self {
    //         from,
    //         to,
    //         ..Default::default()
    //     })
    // }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let Self {
            from,
            transform,
            to,
            jobs,
            compression_level: _,
            force,
            cancel,
        } = self;

        {
            match (from.driver.as_str(), to.driver.as_str()) {
                ("tmq", "taos") => {
                    tmq_to_td(
                        from.clone(),
                        transform.clone(),
                        to.clone(),
                        *jobs,
                        cancel.clone(),
                    )
                    .await?;
                }
                ("tmq", "local") => {
                    tmq_to_local(from.clone(), to.clone(), *jobs, *force, cancel.clone()).await?;
                }
                ("local", "taos") => {
                    local_to_taos(from.clone(), to.clone(), *jobs, *force).await?;
                }
                ("taos", "taos") => {
                    legacy_to_taos(from.clone(), transform.clone(), to.clone(), *jobs).await?;
                }
                ("taos", "csv") => {
                    query_to_csv(from.clone(), to.clone()).await?;
                }
                ("taos", "parquet") => {
                    query_to_parquet(from.clone(), to.clone(), *force).await?;
                }
                (_, _) => anyhow::bail!("unsupported source or target: from {} to {}", from, to),
            }
            Ok(())
        }
    }
}
