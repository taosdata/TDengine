use crate::runners::historian::AVEVA_HISTORIAN_ID;
use crate::runners::mssql::MSSQL_ID;
use crate::runners::mysql::MYSQL_ID;
use crate::runners::oracle::ORACLE_ID;
use crate::runners::postgres::POSTGRES_ID;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::RwLock;
use taos::Dsn;

pub enum TimeoutType {
    Default,
    ValidateDataSource(Dsn),
    GetSample(Dsn),
}

#[derive(Debug)]
pub struct Timeout {
    // timeout for validate data source
    validate_ds: HashMap<String, u64>,
    // timeout for get sample data
    get_sample: HashMap<String, u64>,
}

lazy_static! {
    static ref DEFAULT_TIMEOUT: RwLock<u64> = RwLock::new(30);
}

impl Timeout {
    pub fn set_default_timeout(timeout: Option<u64>) {
        tracing::debug!("set default request timeout: {:?}", timeout);
        if let Some(timeout) = timeout {
            let mut default_timeout = DEFAULT_TIMEOUT.write().unwrap();
            *default_timeout = timeout;
        }
    }

    pub fn get(r#type: TimeoutType) -> u64 {
        lazy_static! {
            static ref TIMEOUT: Timeout = Timeout::default();
        }

        match r#type {
            TimeoutType::Default => {
                let timeout = DEFAULT_TIMEOUT.read().unwrap();
                *timeout
            }
            TimeoutType::ValidateDataSource(dsn) => {
                let ds = dsn.driver;
                TIMEOUT
                    .validate_ds
                    .get(&ds)
                    .map(|t| t.clone())
                    .unwrap_or_else(|| {
                        let timeout = DEFAULT_TIMEOUT.read().unwrap();
                        *timeout
                    })
            }
            TimeoutType::GetSample(dsn) => {
                let ds = dsn.driver;
                TIMEOUT
                    .get_sample
                    .get(&ds)
                    .map(|s| s.clone())
                    .unwrap_or_else(|| {
                        let timeout = DEFAULT_TIMEOUT.read().unwrap();
                        *timeout
                    })
            }
        }
    }

    fn default() -> Timeout {
        let mut get_sample = HashMap::new();
        get_sample.insert(AVEVA_HISTORIAN_ID.to_string(), 120);
        get_sample.insert(MYSQL_ID.to_string(), 120);
        get_sample.insert(POSTGRES_ID.to_string(), 120);
        get_sample.insert(ORACLE_ID.to_string(), 120);
        get_sample.insert(MSSQL_ID.to_string(), 120);

        let timeout = Timeout {
            validate_ds: HashMap::new(), // no specific timeout for validate_ds
            get_sample,
        };
        let default = DEFAULT_TIMEOUT.read().unwrap();
        tracing::debug!("request timeout default: {}, other: {:?}", default, timeout);
        timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::IntoDsn;

    #[test]
    fn test_timeout() {
        let timeout = Timeout::get(TimeoutType::Default);
        assert_eq!(timeout, 30);

        let timeout = Timeout::get(TimeoutType::GetSample(
            "avevaHistorian://".into_dsn().unwrap(),
        ));
        assert_eq!(timeout, 120);

        let timeout = Timeout::get(TimeoutType::GetSample("mysql://".into_dsn().unwrap()));
        assert_eq!(timeout, 120);

        let timeout = Timeout::get(TimeoutType::GetSample("postgres://".into_dsn().unwrap()));
        assert_eq!(timeout, 120);

        let timeout = Timeout::get(TimeoutType::GetSample("oracle://".into_dsn().unwrap()));
        assert_eq!(timeout, 120);

        let timeout = Timeout::get(TimeoutType::GetSample("mssql://".into_dsn().unwrap()));
        assert_eq!(timeout, 120);

        let timeout = Timeout::get(TimeoutType::ValidateDataSource(
            "avevaHistorian://".into_dsn().unwrap(),
        ));
        assert_eq!(timeout, 30);
    }
}
