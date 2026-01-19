use std::sync::Arc;

use anyhow::{Context, bail};
use taos::Dsn;
use taosx_utils::dsn::{parse_multiple_value, parse_simple_params};

#[derive(Debug, Clone)]
pub enum Projection {
    /// 从 0 开始的索引
    Indices(Vec<usize>),
    Names(Vec<String>),
}

#[derive(Debug)]
pub struct Config {
    pub paths: Vec<String>,
    pub batch_size: usize,
    pub projection: Option<Arc<Projection>>,
    pub unprocessed_batches: Option<usize>,
}

impl TryFrom<Dsn> for Config {
    type Error = anyhow::Error;

    // parquet:path/to/test1.parquet,path/to/test2.parquet,path/to/test3.parquet?batch_size=1000&projection=a,b,c
    // parquet:path/to/test1.parquet,path/to/test2.parquet,path/to/test3.parquet?batch_size=1000&projection=0,1,2
    fn try_from(dsn: Dsn) -> Result<Self, Self::Error> {
        let path = dsn.path.as_ref().context("parquet path is required")?;
        let paths = path
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>();
        if paths.is_empty() {
            bail!("parquet path is required");
        }
        let batch_size = parse_simple_params::<usize>(&dsn, "batch_size")?;

        let projection = match parse_multiple_value::<usize>(&dsn, "projection") {
            Ok(Some(indices)) => Some(Arc::new(Projection::Indices(indices))),
            Ok(None) => None,
            Err(_) => parse_multiple_value(&dsn, "projection")?
                .map(|names| Arc::new(Projection::Names(names))),
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

    #[test]
    fn test_config_single_path() {
        let dsn = Dsn::from_str("parquet:test.parquet").unwrap();
        let config = Config::try_from(dsn).unwrap();
        assert_eq!(config.paths.len(), 1);
        assert_eq!(config.paths[0], "test.parquet");
        assert_eq!(config.batch_size, 1000);
        assert!(config.projection.is_none());
        assert!(config.unprocessed_batches.is_none());
    }

    #[test]
    fn test_config_multiple_paths() {
        let dsn = Dsn::from_str("parquet:test1.parquet,test2.parquet,test3.parquet").unwrap();
        let config = Config::try_from(dsn).unwrap();
        assert_eq!(config.paths.len(), 3);
        assert_eq!(config.paths[0], "test1.parquet");
        assert_eq!(config.paths[1], "test2.parquet");
        assert_eq!(config.paths[2], "test3.parquet");
    }

    #[test]
    fn test_config_multiple_paths_with_spaces() {
        let dsn = Dsn::from_str("parquet: test1.parquet , test2.parquet , test3.parquet ").unwrap();
        let config = Config::try_from(dsn).unwrap();
        assert_eq!(config.paths.len(), 3);
        assert_eq!(config.paths[0], "test1.parquet");
        assert_eq!(config.paths[1], "test2.parquet");
        assert_eq!(config.paths[2], "test3.parquet");
    }

    #[test]
    fn test_config_with_batch_size() {
        let dsn = Dsn::from_str("parquet:test.parquet?batch_size=2000").unwrap();
        let config = Config::try_from(dsn).unwrap();
        assert_eq!(config.batch_size, 2000);
    }

    #[test]
    fn test_config_with_projection_indices() {
        let dsn = Dsn::from_str("parquet:test.parquet?projection=0,1,2").unwrap();
        let config = Config::try_from(dsn).unwrap();
        if let Some(Projection::Indices(indices)) = config.projection.as_deref() {
            assert_eq!(indices, &vec![0, 1, 2]);
        }
    }

    #[test]
    fn test_config_with_projection_names() {
        let dsn = Dsn::from_str("parquet:test.parquet?projection=col_a,col_b,col_c").unwrap();
        let config = Config::try_from(dsn).unwrap();
        if let Some(Projection::Names(names)) = config.projection.as_deref() {
            assert_eq!(names, &vec!["col_a", "col_b", "col_c"]);
        }
    }

    #[test]
    fn test_config_with_unprocessed_batches() {
        let dsn = Dsn::from_str("parquet:test.parquet?unprocessed_batches=128").unwrap();
        let config = Config::try_from(dsn).unwrap();
        assert_eq!(config.unprocessed_batches, Some(128));
    }

    #[test]
    fn test_config_with_all_params() {
        let dsn = Dsn::from_str(
            "parquet:test1.parquet,test2.parquet?batch_size=5000&projection=0,1,2&unprocessed_batches=64",
        )
        .unwrap();
        let config = Config::try_from(dsn).unwrap();
        assert_eq!(config.paths.len(), 2);
        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.unprocessed_batches, Some(64));
        if let Some(Projection::Indices(indices)) = config.projection.as_deref() {
            assert_eq!(indices, &vec![0, 1, 2]);
        }
    }

    #[test]
    fn test_config_missing_path() {
        let dsn = Dsn::from_str("parquet:").unwrap();
        let result = Config::try_from(dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("parquet path is required")
        );
    }

    #[test]
    fn test_config_empty_path_after_filter() {
        let dsn = Dsn::from_str("parquet: , , ").unwrap();
        let result = Config::try_from(dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("parquet path is required")
        );
    }

    #[test]
    fn test_projection_debug() {
        let proj_indices = Projection::Indices(vec![0, 1, 2]);
        let debug_str = format!("{:?}", proj_indices);
        assert!(debug_str.contains("Indices"));

        let proj_names = Projection::Names(vec!["a".to_string(), "b".to_string()]);
        let debug_str = format!("{:?}", proj_names);
        assert!(debug_str.contains("Names"));
    }

    #[test]
    fn test_projection_clone() {
        let proj = Projection::Indices(vec![0, 1, 2]);
        let cloned = proj.clone();
        match cloned {
            Projection::Indices(indices) => assert_eq!(indices, vec![0, 1, 2]),
            _ => panic!("Expected Projection::Indices"),
        }
    }
}
