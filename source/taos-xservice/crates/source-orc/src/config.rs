use anyhow::{Context, bail};
use taos::Dsn;
use taosx_utils::dsn::{parse_multiple_value, parse_simple_params};

#[derive(Debug, Clone)]
pub enum Projection {
    /// 从 1 开始的索引
    Indices(Vec<usize>),
    Names(Vec<String>),
}

pub struct Config {
    pub paths: Vec<String>,
    pub batch_size: usize,
    pub projection: Option<Projection>,
    pub unprocessed_batches: Option<usize>,
}

impl TryFrom<Dsn> for Config {
    type Error = anyhow::Error;

    // orc:path/to/test1.orc,path/to/test2.orc,path/to/test3.orc?batch_size=1000&named_projection=a,b,c
    // orc:path/to/test1.orc,path/to/test2.orc,path/to/test3.orc?batch_size=1000&index_projection=a,b,c
    fn try_from(dsn: Dsn) -> Result<Self, Self::Error> {
        let path = dsn.path.as_ref().context("orc path is quired")?;
        let paths = path
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>();
        if paths.is_empty() {
            bail!("orc path is quired");
        }
        let batch_size = parse_simple_params::<usize>(&dsn, "batch_size")?;

        let projection = match parse_multiple_value::<usize>(&dsn, "projection") {
            Ok(Some(indices)) => Some(Projection::Indices(indices)),
            Ok(None) => None,
            Err(_) => parse_multiple_value(&dsn, "projection")?.map(Projection::Names),
        };

        let unprocessed_batches = parse_simple_params::<usize>(&dsn, "unprocessed_batches")?;

        Ok(Self {
            paths,
            batch_size: batch_size.unwrap_or(1000),
            projection,
            unprocessed_batches,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn parse_config(input: &str) -> anyhow::Result<Config> {
        Config::try_from(Dsn::from_str(input)?)
    }

    #[test]
    fn config_parses_paths_and_defaults() {
        let config = parse_config("orc:/data/a.orc,/data/b.orc").unwrap();

        assert_eq!(config.paths, vec!["/data/a.orc", "/data/b.orc"]);
        assert_eq!(config.batch_size, 1000);
        assert!(config.projection.is_none());
        assert_eq!(config.unprocessed_batches, None);
    }

    #[test]
    fn config_ignores_empty_path_segments_and_parses_batch_size() {
        let config = parse_config("orc:/data/a.orc, ,/data/b.orc?batch_size=64").unwrap();

        assert_eq!(config.paths, vec!["/data/a.orc", "/data/b.orc"]);
        assert_eq!(config.batch_size, 64);
    }

    #[test]
    fn config_parses_numeric_projection_before_name_projection() {
        let config = parse_config("orc:/data/a.orc?projection=1,3,5").unwrap();

        match config.projection {
            Some(Projection::Indices(indices)) => assert_eq!(indices, vec![1, 3, 5]),
            _ => panic!("expected numeric projection"),
        }
    }

    #[test]
    fn config_falls_back_to_name_projection() {
        let config = parse_config("orc:/data/a.orc?projection=ts,value,status").unwrap();

        match config.projection {
            Some(Projection::Names(names)) => assert_eq!(names, vec!["ts", "value", "status"]),
            _ => panic!("expected name projection"),
        }
    }

    #[test]
    fn config_parses_unprocessed_batches() {
        let config = parse_config("orc:/data/a.orc?unprocessed_batches=3").unwrap();

        assert_eq!(config.unprocessed_batches, Some(3));
    }

    #[test]
    fn config_rejects_missing_or_empty_paths() {
        assert!(parse_config("orc:").is_err());
        assert!(parse_config("orc:, ,").is_err());
    }
}
