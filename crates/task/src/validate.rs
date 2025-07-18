use taos::IntoDsn;
use taosx_core::dsv::DataSourceValidation;

pub async fn validate_dsn(dsn: impl IntoDsn) -> DataSourceValidation {
    let dsn = dsn.into_dsn();
    match dsn {
        Err(err) => {
            DataSourceValidation::invalid("unknown".to_string(), format!("invalid dsn: {}", err))
        }
        Ok(dsn) => match dsn.driver.as_str() {
            source_historian::AVEVA_HISTORIAN_ID => source_historian::is_valid(&dsn).await,
            "influxdb" => source_influxdb::is_valid(&dsn).await,
            source_kafka::KAFKA_ID => source_kafka::is_valid(&dsn).await,
            source_mqtt::MQTT_ID => source_mqtt::is_valid(&dsn).await,
            source_sparkplugb::SPARKPLUGB_ID => source_sparkplugb::validate::is_valid(&dsn).await,
            "opc" | "opcda" | "opcua" => source_opc::is_valid(&dsn).await,
            "opentsdb" => source_opentsdb::is_valid(&dsn).await,
            "pi" | "pibackfill" => source_pi::is_pi_valid(&dsn).await,
            "taos" => taosx_core::taoz::is_taos_valid(&dsn).await,
            "tmq" | "sync" => {
                let mut dsn = dsn.clone();
                dsn.driver = "tmq".to_string();
                taosx_core::tmq::is_tmq_valid(&dsn).await
            }
            "csv" => taosx_core::csv::is_csv_valid(&dsn).await,
            "local" => local_to_taos::is_local_valid(&dsn).await,
            source_mysql::MYSQL_ID => source_mysql::is_valid(&dsn).await,
            source_postgres::POSTGRES_ID => source_postgres::is_valid(&dsn).await,
            source_oracle::ORACLE_ID => source_oracle::is_valid(&dsn).await,
            source_mssql::MSSQL_ID => source_mssql::is_valid(&dsn).await,
            source_mongodb::MONGODB_ID => source_mongodb::is_valid(&dsn).await,
            &_ => DataSourceValidation::unknown(),
        },
    }
}
