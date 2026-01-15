use std::{num::NonZeroUsize, sync::OnceLock};

use tonic::transport::Certificate;

pub static mut DRY_RUN: bool = false;
pub static mut SQL_TAG_CACHE_CAPACITY: usize = 0;
pub static mut DRY_RUN_DATASOURCE: bool = false;

pub static TABLE_TAG_CACHE: OnceLock<scc::HashSet<String>> = OnceLock::new();

pub static AGENT_CLIENT_CA: OnceLock<Certificate> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct LogOpts {
    pub instance_id: u8,
    pub compress: Option<bool>,
    pub rotation_count: Option<u16>,
    pub keep_days: Option<u16>,
    pub rotation_size: Option<String>,
    pub reserved_disk_size: Option<String>,
}

pub static GLOBAL_LOG_OPTS: OnceLock<LogOpts> = OnceLock::new();

static AGENT_IN_MEMORY_CACHE_CAPACITY: OnceLock<NonZeroUsize> = OnceLock::new();

pub(crate) fn agent_in_memory_cache_capacity() -> NonZeroUsize {
    const DEFAULT_AGENT_IN_MEMORY_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
    *AGENT_IN_MEMORY_CACHE_CAPACITY.get_or_init(|| {
        std::env::var("AGENT_CACHE_CAPACITY")
            .ok()
            .and_then(|v| {
                v.parse::<usize>()
                    .inspect_err(|e| {
                        tracing::error!("failed to parse AGENT_CACHE_CAPACITY (v), cause: {:?}", e);
                    })
                    .ok()
            })
            .and_then(NonZeroUsize::new)
            .unwrap_or(DEFAULT_AGENT_IN_MEMORY_CACHE_CAPACITY)
    })
}

pub fn set_agent_in_memory_cache_capacity(capacity: usize) {
    if let Some(capacity) = NonZeroUsize::new(capacity)
        && AGENT_IN_MEMORY_CACHE_CAPACITY.set(capacity).is_ok()
    {
        tracing::info!("Set agent cache queue capacity to {}", capacity);
    }
}

/// Set the client CA certificate for the agent.
pub fn set_agent_client_ca(ca: Certificate) {
    if AGENT_CLIENT_CA.set(ca).is_ok() {
        tracing::info!("Set agent client CA");
    }
}

pub fn get_agent_client_ca() -> Option<Certificate> {
    AGENT_CLIENT_CA.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_in_memory_cache_capacity() {
        assert_eq!(agent_in_memory_cache_capacity().get(), 64);
        set_agent_in_memory_cache_capacity(128);
        assert_eq!(agent_in_memory_cache_capacity().get(), 64);
    }

    #[test]
    fn test_set_agent_in_memory_cache_capacity() {
        set_agent_in_memory_cache_capacity(128);
        assert_eq!(agent_in_memory_cache_capacity().get(), 128);
    }

    #[test]
    fn test_set_agent_in_memory_cache_capacity_zero() {
        set_agent_in_memory_cache_capacity(0);
        assert_eq!(agent_in_memory_cache_capacity().get(), 64);
    }

    #[test]
    fn test_agent_ca() {
        let ca = get_agent_client_ca();
        assert!(ca.is_none());
        let ca = Certificate::from_pem(b"test_ca");
        set_agent_client_ca(ca.clone());
        assert!(get_agent_client_ca().is_some());
    }
}
