use taos::Dsn;
use taosx_ipc::types::dsv::DataSourceValidation;

use source_mqtt::client::{GenericMessagePoller, MessagePoller};

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match super::config::ConnectConfig::try_from(dsn) {
        Ok(mut config) => {
            let client_id = format!(
                "_taosx_validate_spb_{}_{}",
                config.client_id,
                uuid::Uuid::new_v4().simple()
            );
            config.client_id = client_id;
            let configs = match config.mqtt_config() {
                Ok(configs) => configs,
                Err(e) => {
                    return DataSourceValidation::invalid(
                        "sparkplugb",
                        format!("invalid sparkplugb dsn: {dsn}, cause: {e:#}"),
                    );
                }
            };
            for config in configs {
                if let Err(e) = GenericMessagePoller::try_connect(&config).await {
                    return DataSourceValidation::invalid(
                        "sparkplugb",
                        format!(
                            "failed to connect to dsn: {}, {:#}",
                            dsn,
                            anyhow::Error::new(e)
                        ),
                    );
                }
            }
            DataSourceValidation::valid("sparkplugb", None)
        }
        Err(e) => DataSourceValidation::invalid(
            "sparkplugb".to_string(),
            format!("invalid mqtt dsn: {dsn}, cause: {e:#}"),
        ),
    }
}
