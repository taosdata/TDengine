use anyhow::Context;
use itertools::Itertools;
use rdkafka::consumer::{BaseConsumer, Consumer};
use taos::Dsn;
use taosx_core::dsv::DataSourceValidation;

use crate::{
    FETCH_METADATA_TIMEOUT, KAFKA_ID,
    config::task::{KafkaTaskConfig, build_client_config},
};

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn) {
        Ok(()) => DataSourceValidation::valid(KAFKA_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(KAFKA_ID.to_string(), format!("{err:#}")),
    }
}

fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    let config = KafkaTaskConfig::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("invalid dsn: {}, cause: {:#}", dsn, err))?;

    let client_config = build_client_config(config.connect)?;
    let consumer: BaseConsumer = client_config
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {:#}", err))?;

    let metadata = consumer
        .fetch_metadata(None, FETCH_METADATA_TIMEOUT)
        .context("failed to load meta data while checking kafka data source")?;

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
    Ok(())
}
