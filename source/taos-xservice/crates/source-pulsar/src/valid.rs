use crate::{PULSAR_ID, config::task::PulsarTaskConfig, consumer::build_pulsar};
use pulsar::{TokioExecutor, reader::Reader};
use taos::Dsn;
use taosx_core::dsv::DataSourceValidation;

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn).await {
        Ok(()) => DataSourceValidation::valid(PULSAR_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(PULSAR_ID.to_string(), format!("{err:#}")),
    }
}

async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    let config = PulsarTaskConfig::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("invalid dsn: {}, cause: {:#}", dsn, err))?;

    let Some(topic) = config.topics.first() else {
        anyhow::bail!("pulsar task config must have at least one topic");
    };
    let pulsar = build_pulsar(&config.connect).await?;
    let mut reader: Reader<Vec<u8>, TokioExecutor> = pulsar
        .reader()
        .with_topic(topic)
        .with_consumer_name("taosx-valid-test")
        .into_reader()
        .await?;
    reader.check_connection().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;

    use taos::IntoDsn;

    use super::*;

    /// Example:
    /// ```shell
    /// PULSAR_DSN_VALID="pulsar://192.168.2.131:6650?topics=persistent://public/default/pt-zgc&subscription=dev&consumer_name=c1"  cargo test --package source-pulsar --lib -- valid::tests::test_is_valid --exact --nocapture
    /// ```
    #[tokio::test]
    async fn test_is_valid() {
        if let Ok(pulsar_dsn) = env::var("PULSAR_DSN_VALID") {
            let dsn = pulsar_dsn.into_dsn().expect("always valid");
            dbg!(&dsn);
            let valid = is_valid(&dsn).await;
            dbg!(&valid);
            assert!(valid.valid);
            assert!(valid.support);
            assert_eq!(valid.data_source, PULSAR_ID.to_string());
        }
    }
}
