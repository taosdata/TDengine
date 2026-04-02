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
pub const ARCHIVE_PREFIX: &str = "archive";
pub const CACHE_DIR: &str = "cache";
pub const ARCHIVE_DIR: &str = "archived";

pub async fn get_rewrite_files(
    archive_tx: &flume::Sender<ArchiveType>,
) -> Result<Vec<PathBuf>, ArchiveError> {
    let (resp_tx, rx) = oneshot::channel::<Result<Vec<PathBuf>, ArchiveError>>();
    archive_tx
        .send_async(ArchiveType::CacheRewrite(RewriteMsg { resp_tx }))
        .await
        .map_err(|e| ArchiveError::OneshotSendError(e.to_string()))?;
    rx.await
        .map_err(|e| ArchiveError::OneshotRecvError(e.to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_prefix_constant() {
        assert_eq!(CACHE_PREFIX, "cache");
    }

    #[test]
    fn test_archive_prefix_constant() {
        assert_eq!(ARCHIVE_PREFIX, "archive");
    }

    #[test]
    fn test_cache_dir_constant() {
        assert_eq!(CACHE_DIR, "cache");
    }

    #[test]
    fn test_archive_dir_constant() {
        assert_eq!(ARCHIVE_DIR, "archived");
    }

    #[tokio::test]
    async fn test_get_rewrite_files_success() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        // Spawn a task to handle the message
        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let files = vec![
                    PathBuf::from("/tmp/file1.txt"),
                    PathBuf::from("/tmp/file2.txt"),
                ];
                let _ = rewrite_msg.resp_tx.send(Ok(files));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0], PathBuf::from("/tmp/file1.txt"));
        assert_eq!(files[1], PathBuf::from("/tmp/file2.txt"));
    }

    #[tokio::test]
    async fn test_get_rewrite_files_error_response() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        // Spawn a task to handle the message and return an error
        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let _ = rewrite_msg.resp_tx.send(Err(ArchiveError::OneshotSendError(
                    "Test error".to_string(),
                )));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_rewrite_files_empty_list() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        // Spawn a task to return empty file list
        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let _ = rewrite_msg.resp_tx.send(Ok(vec![]));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn test_get_rewrite_files_channel_dropped() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        // Drop the receiver immediately
        drop(rx);

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_rewrite_files_multiple_files() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let files = vec![
                    PathBuf::from("/data/cache/file1.parquet"),
                    PathBuf::from("/data/cache/file2.parquet"),
                    PathBuf::from("/data/cache/file3.parquet"),
                    PathBuf::from("/data/cache/file4.parquet"),
                    PathBuf::from("/data/cache/file5.parquet"),
                ];
                let _ = rewrite_msg.resp_tx.send(Ok(files));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 5);
    }

    #[tokio::test]
    async fn test_get_rewrite_files_with_special_characters() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let files = vec![
                    PathBuf::from("/tmp/file with spaces.txt"),
                    PathBuf::from("/tmp/file-with-dashes.txt"),
                    PathBuf::from("/tmp/file_with_underscores.txt"),
                ];
                let _ = rewrite_msg.resp_tx.send(Ok(files));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 3);
        assert!(files[0].to_string_lossy().contains("spaces"));
    }

    #[tokio::test]
    async fn test_get_rewrite_files_send_error() {
        let (tx, _rx) = flume::unbounded::<ArchiveType>();

        // Drop receiver to cause send error
        drop(_rx);

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_err());

        if let Err(e) = result {
            match e {
                ArchiveError::OneshotSendError(_) => {
                    // Expected error type
                }
                _ => panic!("Expected OneshotSendError"),
            }
        }
    }

    #[tokio::test]
    async fn test_get_rewrite_files_pathbuf_equality() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        let expected_path = PathBuf::from("/tmp/test.txt");
        let expected_clone = expected_path.clone();

        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let _ = rewrite_msg.resp_tx.send(Ok(vec![expected_clone]));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files[0], expected_path);
    }

    #[test]
    fn test_constants_consistency() {
        // Test that prefix and dir constants are related
        assert!(CACHE_DIR.contains("cache"));
        assert!(CACHE_PREFIX.contains("cache"));
        assert!(ARCHIVE_DIR.contains("archive"));
        assert!(ARCHIVE_PREFIX.contains("archive"));
    }

    #[test]
    fn test_constants_lowercase() {
        // Ensure constants are lowercase for consistency
        assert_eq!(CACHE_PREFIX, CACHE_PREFIX.to_lowercase());
        assert_eq!(ARCHIVE_PREFIX, ARCHIVE_PREFIX.to_lowercase());
        assert_eq!(CACHE_DIR, CACHE_DIR.to_lowercase());
    }

    #[test]
    fn test_constants_no_whitespace() {
        assert!(!CACHE_PREFIX.contains(' '));
        assert!(!ARCHIVE_PREFIX.contains(' '));
        assert!(!CACHE_DIR.contains(' '));
        assert!(!ARCHIVE_DIR.contains(' '));
    }

    #[test]
    fn test_constants_no_special_chars() {
        // Ensure no slashes or special characters
        assert!(!CACHE_PREFIX.contains('/'));
        assert!(!ARCHIVE_PREFIX.contains('/'));
        assert!(!CACHE_DIR.contains('\\'));
        assert!(!ARCHIVE_DIR.contains('\\'));
    }

    #[test]
    fn test_pathbuf_creation() {
        let path1 = PathBuf::from("/tmp/test");
        let path2 = PathBuf::from("/tmp/test");
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_pathbuf_join_with_constants() {
        let base = PathBuf::from("/data");
        let cache_path = base.join(CACHE_DIR);
        let archive_path = base.join(ARCHIVE_DIR);

        assert!(cache_path.to_string_lossy().contains("cache"));
        assert!(archive_path.to_string_lossy().contains("archived"));
    }

    #[tokio::test]
    async fn test_get_rewrite_files_receiver_dropped_after_send() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                // Drop the response sender without sending
                drop(rewrite_msg.resp_tx);
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_err());

        if let Err(e) = result {
            match e {
                ArchiveError::OneshotRecvError(_) => {
                    // Expected error type
                }
                _ => panic!("Expected OneshotRecvError"),
            }
        }
    }

    #[test]
    fn test_cache_and_archive_prefix_different() {
        assert_ne!(CACHE_PREFIX, ARCHIVE_PREFIX);
        assert_ne!(CACHE_DIR, ARCHIVE_DIR);
    }

    #[test]
    fn test_constants_are_static_str() {
        // Ensure constants have correct type
        let _: &'static str = CACHE_PREFIX;
        let _: &'static str = ARCHIVE_PREFIX;
        let _: &'static str = CACHE_DIR;
        let _: &'static str = ARCHIVE_DIR;
    }

    #[tokio::test]
    async fn test_get_rewrite_files_large_path_list() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let files: Vec<PathBuf> = (0..100)
                    .map(|i| PathBuf::from(format!("/tmp/file_{}.parquet", i)))
                    .collect();
                let _ = rewrite_msg.resp_tx.send(Ok(files));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 100);
    }

    #[tokio::test]
    async fn test_get_rewrite_files_absolute_and_relative_paths() {
        let (tx, rx) = flume::unbounded::<ArchiveType>();

        tokio::spawn(async move {
            if let Ok(ArchiveType::CacheRewrite(rewrite_msg)) = rx.recv_async().await {
                let files = vec![
                    PathBuf::from("/tmp/file_1.parquet"),
                    PathBuf::from("./file_2.parquet"),
                ];
                let _ = rewrite_msg.resp_tx.send(Ok(files));
            }
        });

        let result = get_rewrite_files(&tx).await;
        assert!(result.is_ok());
        let files = result.unwrap();
        assert_eq!(files.len(), 2);
        assert!(files[0].is_absolute());
        assert!(files[1].is_relative());
    }
}
