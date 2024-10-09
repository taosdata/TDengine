use std::sync::OnceLock;

static DURATION_PARSER: OnceLock<fundu::DurationParser> = OnceLock::new();

pub fn parse_duration(string: &str) -> Result<std::time::Duration, fundu::ParseError> {
    DURATION_PARSER
        .get_or_init(fundu::DurationParser::with_all_time_units)
        .parse(string)
        // unwrap is safe here because negative durations aren't allowed in the default
        // configuration of the DurationParser
        .map(|s| s.try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn parse_duration_test() {
        // Julian Year = 365.25 days
        assert_eq!(
            parse_duration("1y"),
            Ok(
                Duration::from_secs(1) * 60 * 60 * 24 * 365 // 365 天
             + Duration::from_secs(1) * 60 * 60 * 6 // 6 小时
            )
        );
        // year / 12
        assert_eq!(
            parse_duration("1M"),
            Ok(
                Duration::from_secs(1) * 60 * 60 * 24 * 30 // 30 天
                + Duration::from_secs(1) * 60 * 60 * 10 // 10 小时
                + Duration::from_secs(1) * 60 * 30 // 30 分钟
            )
        );
        assert_eq!(
            parse_duration("1w"),
            Ok(Duration::from_secs(1) * 60 * 60 * 24 * 7)
        );
        assert_eq!(
            parse_duration("1d"),
            Ok(Duration::from_secs(1) * 60 * 60 * 24)
        );
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(1) * 60 * 60));
        assert_eq!(parse_duration("1m"), Ok(Duration::from_secs(1) * 60));
        assert_eq!(parse_duration("1s"), Ok(Duration::from_secs(1)));
        assert_eq!(parse_duration("1"), Ok(Duration::from_secs(1)));
        assert_eq!(parse_duration("1ms"), Ok(Duration::from_millis(1)));
        assert_eq!(parse_duration("1Ms"), Ok(Duration::from_micros(1)));
        assert_eq!(parse_duration("1ns"), Ok(Duration::from_nanos(1)));

        assert!(parse_duration("1ss").is_err());
        assert!(parse_duration("-1s").is_err());
        assert!(parse_duration("s").is_err());
    }
}
