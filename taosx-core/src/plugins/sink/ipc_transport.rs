use anyhow::Context;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// Abstraction over how to create gRPC channels for the sink.
///
/// This makes connection establishment testable and decoupled from
/// concrete `tonic::transport::Endpoint` construction.
#[async_trait::async_trait]
pub trait ChannelFactory: Send + Sync {
    async fn connect(&self, remote: &str) -> anyhow::Result<Channel>;
}

/// Default `ChannelFactory` implementation that preserves the previous
/// behavior from `try_establish_channel` in `mod.rs`.
pub struct DefaultChannelFactory;

#[async_trait::async_trait]
impl ChannelFactory for DefaultChannelFactory {
    async fn connect(&self, remote: &str) -> anyhow::Result<Channel> {
        let mut endpoint = Endpoint::try_from(remote.to_string())?
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(300))
            .http2_keep_alive_interval(Duration::from_secs(39))
            .tcp_keepalive(Some(Duration::from_secs(7200))); // keep alive for 2 hours

        if let Some(ca) = crate::global::get_agent_client_ca() {
            endpoint = endpoint
                .tls_config(
                    ClientTlsConfig::new()
                        .ca_certificate(ca)
                        .with_enabled_roots(),
                )
                .context("Unable to create TLS config for endpoint")?;
        }

        let channel = endpoint.connect().await?;
        Ok(channel)
    }
}

/// Configuration for retrying connection establishment.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum retry times. When `retry_forever` is true, this is ignored.
    pub max_retries: usize,
    /// Initial backoff duration.
    pub initial_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
}

impl RetryConfig {
    pub const fn new(max_retries: usize, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff,
        }
    }
}

/// Retry establishing a channel with exponential backoff.
///
/// This is a small wrapper around the previous loop inside `ipc_forward` and
/// keeps its semantics, including honoring `retry_forever` and `cancel`.
pub async fn retry_connect<F>(
    factory: &F,
    remote: &str,
    cfg: RetryConfig,
    retry_forever: bool,
    cancel: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Option<Channel>>
where
    F: ChannelFactory,
{
    use std::ops::Mul;

    let mut retries = 0usize;
    let mut retry_interval = cfg.initial_backoff;

    loop {
        match factory.connect(remote).await {
            Ok(ch) => {
                tracing::info!("connect to {remote} successfully!");
                return Ok(Some(ch));
            }
            Err(err) => {
                retries += 1;
                tracing::error!("Failed to establish connection: {}. Retrying...", err);
                if !retry_forever && retries >= cfg.max_retries {
                    tracing::error!("Max retries reached. Exiting...");
                    return Err(err);
                }
                if tokio::time::timeout(retry_interval, cancel.cancelled())
                    .await
                    .is_ok()
                {
                    // Cancelled while waiting for next retry.
                    return Ok(None);
                }
                retry_interval = retry_interval.mul(2).min(cfg.max_backoff);
            }
        }
    }
}
