use std::str::FromStr;

pub enum HistorianTable {
    Live,
    History,
}

impl FromStr for HistorianTable {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Runtime.dbo.History" => Ok(Self::History),
            "Runtime.dbo.Live" => Ok(Self::Live),
            _ => Err(s.to_string()),
        }
    }
}
