use anyhow::{Context, bail};
use taos::Dsn;
use taosx_core::utils::dsn::{parse_multiple_value, parse_simple_params};

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
