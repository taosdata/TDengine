use anyhow::{anyhow, Result};
use taos::Dsn;

pub trait DsnParamGetter {
    fn get_bool(&self, key: &str) -> Result<Option<bool>>;
}

impl DsnParamGetter for Dsn {
    fn get_bool(&self, key: &str) -> Result<Option<bool>> {
        self.params
            .get(key)
            .map(|v| {
                v.parse::<bool>()
                    .map_err(|err| anyhow!("invalid param {}, cause: {}", key, err.to_string()))
            })
            .transpose()
    }
}
