use std::future::Future;
use std::time::Duration;

use crate::{
    KAFKA_ID,
    blocking::fetch_metadata,
    config::task::{KafkaTaskConfig, build_client_config_inner},
};
use anyhow::Context;
use itertools::Itertools;
use rdkafka::consumer::BaseConsumer;
use taos::Dsn;
use taosx_core::dsv::DataSourceValidation;

const VALIDATION_MAX_RETRIES: usize = 3;
const VALIDATION_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
const VALIDATION_RETRY_SLEEP: Duration = Duration::from_secs(1);

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn).await {
        Ok(()) => DataSourceValidation::valid(KAFKA_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(KAFKA_ID.to_string(), format!("{err:#}")),
    }
}

async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    let config = KafkaTaskConfig::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("invalid dsn: {}, cause: {:#}", dsn, err))?;

    let client_config = build_client_config_inner(config.connect).await?;
    let consumer: BaseConsumer = client_config
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {:#}", err))?;

    retry_validation_metadata(
        consumer,
        VALIDATION_RETRY_SLEEP,
        |_attempt, consumer| async move {
            let (next_consumer, metadata_result) =
                fetch_metadata(consumer, None, VALIDATION_RETRY_TIMEOUT).await?;
            match metadata_result {
                Ok(metadata) => {
                    tracing::info!(
                        brokers = metadata
                            .brokers()
                            .iter()
                            .map(|b| format!("{}={}:{}", b.id(), b.host(), b.port()))
                            .join(","),
                        broker.id = metadata.orig_broker_id(),
                        broker.name = metadata.orig_broker_name(),
                        "kafka metadata"
                    );
                    Ok((next_consumer, Ok(())))
                }
                Err(err) => Ok((next_consumer, Err(err))),
            }
        },
    )
    .await
}

async fn retry_validation_metadata<S, T, Op, Fut>(
    mut state: S,
    retry_sleep: Duration,
    mut op: Op,
) -> anyhow::Result<T>
where
    Op: FnMut(usize, S) -> Fut,
    Fut: Future<Output = anyhow::Result<(S, anyhow::Result<T>)>>,
{
    let mut last_error = None;
    for attempt in 1..=VALIDATION_MAX_RETRIES {
        let (next_state, result) = op(attempt, state).await?;
        state = next_state;
        match result {
            Ok(value) => return Ok(value),
            Err(err) => {
                tracing::warn!(
                    "failed to load kafka metadata during validation, attempt: {attempt}/{VALIDATION_MAX_RETRIES}, error: {err:#}"
                );
                last_error = Some(err);
                if attempt < VALIDATION_MAX_RETRIES {
                    tokio::time::sleep(retry_sleep).await;
                }
            }
        }
    }
    let err = last_error
        .unwrap_or_else(|| anyhow::anyhow!("metadata load failed without an underlying error"));
    Err(err).context(format!(
        "metadata load failed after {VALIDATION_MAX_RETRIES} attempts while checking kafka data source"
    ))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use anyhow::anyhow;
    use taos::Dsn;

    use super::{
        VALIDATION_MAX_RETRIES, VALIDATION_RETRY_SLEEP, VALIDATION_RETRY_TIMEOUT, is_valid_impl,
        retry_validation_metadata,
    };

    #[test]
    fn validation_retry_constants_match_plan() {
        assert_eq!(VALIDATION_MAX_RETRIES, 3);
        assert_eq!(VALIDATION_RETRY_TIMEOUT, Duration::from_secs(10));
        assert_eq!(VALIDATION_RETRY_SLEEP, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn validation_impl_reports_invalid_dsn_async() {
        let dsn = Dsn::from_str("kafka://127.0.0.1:9092").expect("DSN parse should succeed");
        let err = is_valid_impl(&dsn)
            .await
            .expect_err("validation should reject DSN without topics");
        assert_eq!(
            "invalid dsn: kafka://127.0.0.1:9092, cause: topics is required",
            format!("{err:#}")
        );
    }

    #[tokio::test]
    async fn retry_validation_metadata_stops_after_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = retry_validation_metadata((), Duration::ZERO, {
            let attempts = attempts.clone();
            move |_attempt, state| {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if current < 3 {
                        Ok((
                            state,
                            Err::<usize, _>(anyhow!("transient validation failure #{current}")),
                        ))
                    } else {
                        Ok((state, Ok(current)))
                    }
                }
            }
        })
        .await
        .expect("retry helper should return the first successful attempt");

        assert_eq!(result, 3);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_validation_metadata_returns_last_error_with_context() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let err = retry_validation_metadata((), Duration::ZERO, {
            let attempts = attempts.clone();
            move |_attempt, state| {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok((
                        state,
                        Err::<usize, _>(anyhow!("transient validation failure #{current}")),
                    ))
                }
            }
        })
        .await
        .expect_err("retry helper should fail after exhausting validation retries");

        assert_eq!(attempts.load(Ordering::SeqCst), VALIDATION_MAX_RETRIES);
        assert_eq!(
            format!("{err:#}"),
            format!(
                "metadata load failed after {VALIDATION_MAX_RETRIES} attempts while checking kafka data source: transient validation failure #{VALIDATION_MAX_RETRIES}"
            )
        );
    }
}
