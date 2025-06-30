pub mod consumer;
pub use consumer::*;
pub mod archive;
pub use archive::*;
pub mod cache;
pub use cache::*;
pub mod utils;

use std::path::PathBuf;
use tokio::sync::oneshot;

pub const CACHE_PREFIX: &str = "cache";
pub const ARCHIVE_PREFIX: &str = "archived";
pub const CACHE_DIR: &str = "cache";
pub const ARCHIVE_DIR: &str = "archive";

pub async fn get_rewrite_files(
    archive_tx: flume::Sender<ArchiveType>,
) -> Result<Vec<PathBuf>, ArchiveError> {
    let (resp_tx, rx) = oneshot::channel::<Result<Vec<PathBuf>, ArchiveError>>();
    archive_tx
        .send(ArchiveType::CacheRewrite(RewriteMsg { resp_tx }))
        .map_err(|e| ArchiveError::OneshotSendError(e.to_string()))?;
    rx.await
        .map_err(|e| ArchiveError::OneshotRecvError(e.to_string()))?
}
