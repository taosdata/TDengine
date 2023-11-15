use taos::Dsn;

#[derive(Debug, serde::Serialize)]
pub struct PerformanceConfig {
    #[serde(rename = "readWindow")]
    pub(crate) read_window: u32,
    #[serde(rename = "delay")]
    pub(crate) delay: u32,
    #[serde(rename = "maxThread")]
    pub(crate) max_thread: u32,
    #[serde(rename = "queueSizeT")]
    pub(crate) queue_size_thread: u32,
    #[serde(rename = "queueSizeD")]
    pub(crate) queue_size_data: u32,
    #[serde(rename = "limitSpeed")]
    pub(crate) limit_speed: u32,
}

impl PerformanceConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(PerformanceConfig {
            read_window: dsn
                .params
                .get("readWindow")
                .unwrap_or(&"2".to_string())
                .parse::<u32>()
                .map_err(|err| anyhow::anyhow!("invalid readWindow, cause: {:?}", err))?,
            delay: dsn
                .params
                .get("delay")
                .unwrap_or(&"10".to_string())
                .parse::<u32>()
                .map_err(|err| anyhow::anyhow!("invalid delay, cause: {:?}", err))?,
            max_thread: dsn
                .params
                .get("maxThread")
                .unwrap_or(&"50".to_string())
                .parse::<u32>()
                .map_err(|err| anyhow::anyhow!("invalid maxThread, cause: {:?}", err))?,
            queue_size_thread: dsn
                .params
                .get("queueSizeT")
                .unwrap_or(&"1000".to_string())
                .parse::<u32>()
                .map_err(|err| anyhow::anyhow!("invalid queueSizeT, cause: {:?}", err))?,
            queue_size_data: dsn
                .params
                .get("queueSizeD")
                .unwrap_or(&"200000".to_string())
                .parse::<u32>()
                .map_err(|err| anyhow::anyhow!("invalid queueSizeD, cause: {:?}", err))?,
            limit_speed: dsn
                .params
                .get("limitSpeed")
                .unwrap_or(&"100000".to_string())
                .parse::<u32>()
                .map_err(|err| anyhow::anyhow!("invalid limitSpeed, cause: {:?}", err))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("influxdb://").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn).unwrap();
        assert_eq!(2, config.read_window);
        assert_eq!(10000, config.delay);
        assert_eq!(50, config.max_thread);
        assert_eq!(1000, config.queue_size_thread);
        assert_eq!(200000, config.queue_size_data);
        assert_eq!(100000, config.limit_speed);

        let dsn = Dsn::from_str("influxdb://?readWindow=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid readWindow, cause: ParseIntError { kind: InvalidDigit }",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?delay=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid delay, cause: ParseIntError { kind: InvalidDigit }",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?maxThread=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid maxThread, cause: ParseIntError { kind: InvalidDigit }",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?queueSizeT=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid queueSizeT, cause: ParseIntError { kind: InvalidDigit }",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?queueSizeD=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid queueSizeD, cause: ParseIntError { kind: InvalidDigit }",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?limitSpeed=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid limitSpeed, cause: ParseIntError { kind: InvalidDigit }",
            config.unwrap_err().to_string()
        );
    }
}
