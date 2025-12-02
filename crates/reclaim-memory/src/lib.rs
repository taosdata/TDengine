//! Reclaim memory back to the OS, supporting both mimalloc and libc malloc.
//!
//! When the `mimalloc` feature is enabled, it uses mimalloc's `mi_collect` function to
//! reclaim memory. Otherwise, it falls back to using `libc::malloc_trim` for
//! standard libc malloc implementations.
//!
//! This crate also provides a function to spawn a background tokio task that periodically
//! reclaims memory at a specified interval in seconds.
//!
//! # Example
//! ```rust
//! use reclaim_memory::{reclaim_memory, spawn_reclaim_memory_by_interval};
//!
//! // Reclaim memory immediately
//! reclaim_memory(true);
//!
//! // Spawn a background task to reclaim memory every 300 seconds (5 minutes)
//! spawn_reclaim_memory_by_interval(300);
//! ```
use std::time::Duration;

/// Reclaims memory back to the OS.
///
/// This function uses mimalloc's `mi_collect` when the `mimalloc` feature is enabled.
/// Otherwise, it falls back to `libc::malloc_trim` for standard libc malloc implementations
///
/// If `force` is true, it forces a more aggressive memory reclamation. Note that the
/// behavior of `force` may vary depending on the underlying allocator.
///
/// - For mimalloc, it calls `mi_collect(force)`.
/// - For libc malloc, it calls `malloc_trim(0)`, which attempts to release as much memory as possible.
///
/// # Safety
///
/// This function calls unsafe functions from either [libmimalloc-sys] or [libc].
///
/// # Blocking
///
/// This function may perform blocking operations, especially when using mimalloc.
/// It is recommended to call this function within a `tokio::task::spawn_blocking`
/// context to avoid blocking the async runtime.
///
pub fn reclaim_memory(force: bool) {
    tracing::debug!(force, "Reclaiming memory");
    #[cfg(not(feature = "mimalloc"))]
    {
        let _ = force;
        #[cfg(any(all(target_os = "linux", target_env = "gnu"), target_os = "android"))]
        let _ = unsafe { libc::malloc_trim(0) };
    }
    #[cfg(feature = "mimalloc")]
    unsafe {
        libmimalloc_sys::mi_collect(force);
    }
}

static INIT: std::sync::Once = std::sync::Once::new();

/// Starts a background task that reclaims memory at the specified interval (in seconds).
///
/// The first reclamation occurs immediately upon spawning the task. Subsequent reclamations occur
/// at the specified interval.
///
/// If the task is already running, subsequent calls to this function will have no effect.
///
pub fn spawn_reclaim_memory_by_interval(interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    INIT.call_once(|| {
        // Initial reclaim on startup
        tokio::task::spawn(async move {
            tracing::info!(
                "Starting memory reclaim task with interval: {} seconds",
                interval_secs
            );
            loop {
                interval.tick().await;
                if let Err(err) = tokio::task::spawn_blocking(move || reclaim_memory(false)).await {
                    tracing::error!("Memory reclaim task panicked: {:?}", err);
                }
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reclaim_memory() {
        reclaim_memory(true);
        reclaim_memory(false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_reclaim_memory_forever() {
        // Run the reclaim_memory_forever function for a short duration to ensure it works.
        spawn_reclaim_memory_by_interval(5);
        tokio::time::sleep(tokio::time::Duration::from_secs(12)).await;
        INIT.wait();
    }
}
