use chrono::{DateTime, Local, MappedLocalTime, NaiveDateTime, TimeZone};

pub const YYMMDD: &str = "%Y%m%d";
pub const YYMMDDHH: &str = "%Y%m%d%H";
pub const YYMMDDHHMM: &str = "%Y%m%d%H%M";
// canonical fmt
const YYMMDDHHMMSS: &str = "%Y%m%d%H%M%S";

pub fn parse_from_str(dt: &str, fmt: &str) -> Result<DateTime<chrono::Local>, anyhow::Error> {
    let dt = match fmt {
        YYMMDD => format!("{}000000", dt),
        YYMMDDHH => format!("{}0000", dt),
        YYMMDDHHMM => format!("{}00", dt),
        _ => anyhow::bail!("invalid datetime format"),
    };
    let norm_fmt = YYMMDDHHMMSS;
    let naive_dt = NaiveDateTime::parse_from_str(&dt, norm_fmt)?;
    let local_dt = Local.from_local_datetime(&naive_dt);
    let local_dt = match local_dt {
        MappedLocalTime::Single(t) => t,
        _ => anyhow::bail!("invalid datetime format"),
    };
    Ok(local_dt)
}

pub fn time_unit_dt_fmt(unit: &str) -> anyhow::Result<&'static str> {
    match unit {
        "d" => Ok(YYMMDD),
        "h" => Ok(YYMMDDHH),
        "m" => Ok(YYMMDDHHMM),
        _ => anyhow::bail!("invalid time unit"),
    }
}

#[cfg(test)]
mod test {
    use chrono::TimeZone;

    use crate::utils::{YYMMDD, YYMMDDHH, YYMMDDHHMM, parse_from_str};

    #[test]
    fn test_parse_from_str() {
        let dt = "20250626";
        let dt = parse_from_str(dt, YYMMDD).unwrap();
        assert_eq!(dt, chrono::Local.timestamp_opt(1750867200, 0).unwrap());

        let dt = "2025062611";
        let dt = parse_from_str(dt, YYMMDDHH).unwrap();
        assert_eq!(dt, chrono::Local.timestamp_opt(1750906800, 0).unwrap());

        let dt = "202506261109";
        let dt = parse_from_str(dt, YYMMDDHHMM).unwrap();
        assert_eq!(dt, chrono::Local.timestamp_opt(1750907340, 0).unwrap());
    }
}
