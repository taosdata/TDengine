use std::time::Duration;

use taos::{Dsn, IntoDsn};
use taosx_core::plugins::transform::sample::DsSamples;

pub async fn get_sample(dsn: impl IntoDsn) -> anyhow::Result<DsSamples> {
    let dsn = dsn
        .into_dsn()
        .map_err(|err| anyhow::format_err!("invalid dsn, cause: {err}"))?;
    match dsn.driver.as_str() {
        source_historian::AVEVA_HISTORIAN_ID => source_historian::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        source_kafka::KAFKA_ID => {
            let limit = parse_sample_limit(&dsn);
            let timeout = parse_sample_timeout(&dsn);
            source_kafka::get_sample(&dsn, limit, timeout)
                .await
                .map(DsSamples::Simple)
        }
        source_mqtt::MQTT_ID => {
            let limit = parse_sample_limit(&dsn);
            let timeout = parse_sample_timeout(&dsn);
            source_mqtt::get_sample(&dsn, limit, timeout)
                .await
                .map(DsSamples::Simple)
        }
        source_sparkplugb::SPARKPLUGB_ID => {
            let limit = parse_sample_limit(&dsn);
            let timeout = parse_sample_timeout(&dsn);
            source_sparkplugb::sample::get_sample(&dsn, limit, timeout)
                .await
                .map(DsSamples::MultiSchema)
        }
        source_mysql::MYSQL_ID => source_mysql::get_sample(&dsn).await.map(DsSamples::Simple),
        source_postgres::POSTGRES_ID => source_postgres::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        source_oracle::ORACLE_ID => source_oracle::get_sample(&dsn).await.map(DsSamples::Simple),
        source_mssql::MSSQL_ID => source_mssql::get_sample(&dsn).await.map(DsSamples::Simple),
        source_mongodb::MONGODB_ID => source_mongodb::get_sample(&dsn)
            .await
            .map(DsSamples::Simple),
        s => Err(anyhow::anyhow!(
            "get sample from data source {s} is unsupported"
        )),
    }
}

fn parse_sample_limit(dsn: &Dsn) -> usize {
    dsn.params
        .get("get_sample_limit")
        .or(dsn.params.get("sample_data_limit"))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(5)
}

fn parse_sample_timeout(dsn: &Dsn) -> Duration {
    dsn.params
        .get("get_sample_timeout")
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(30))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_sample_limit() {
        let dsn = Dsn::from_str("taos://?get_sample_limit=123").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 123);

        let dsn = Dsn::from_str("taos://?get_sample_limit=").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 5);

        let dsn = Dsn::from_str("taos://").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 5);

        let dsn = Dsn::from_str("taos://?get_sample_limit=abc").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 5);

        let dsn = Dsn::from_str("taos://?sample_data_limit=123").unwrap();
        assert_eq!(parse_sample_limit(&dsn), 123);
    }

    #[test]
    fn test_parse_sample_timeout() {
        let dsn = Dsn::from_str("taos://?get_sample_timeout=123").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(123));

        let dsn = Dsn::from_str("taos://?get_sample_timeout=").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(30));

        let dsn = Dsn::from_str("taos://").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(30));

        let dsn = Dsn::from_str("taos://?get_sample_timeout=abc").unwrap();
        assert_eq!(parse_sample_timeout(&dsn), Duration::from_secs(30));
    }
}
