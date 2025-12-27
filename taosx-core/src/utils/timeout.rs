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
    /// 各个数据源默认是 30s，这里全局超时设置要大于 30s
    static ref DEFAULT_TIMEOUT: RwLock<u64> = RwLock::new(35);
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
                TIMEOUT.validate_ds.get(&ds).copied().unwrap_or_else(|| {
                    let timeout = DEFAULT_TIMEOUT.read().unwrap();
                    *timeout
                })
            }
            TimeoutType::GetSample(dsn) => {
                let ds = dsn.driver;
                TIMEOUT.get_sample.get(&ds).copied().unwrap_or_else(|| {
                    let timeout = DEFAULT_TIMEOUT.read().unwrap();
                    *timeout
                })
            }
        }
    }

    fn default() -> Timeout {
        let mut get_sample = HashMap::new();
        get_sample.insert("avevaHistorian".to_string(), 120);
        get_sample.insert("mysql".to_string(), 120);
        get_sample.insert("postgres".to_string(), 120);
        get_sample.insert("oracle".to_string(), 120);
        get_sample.insert("mssql".to_string(), 120);

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
    use std::sync::Mutex;
    use taos::IntoDsn;

    static TEST_GUARD: Mutex<()> = Mutex::new(());

    #[test]
    fn test_timeout() {
        let _guard = TEST_GUARD.lock().unwrap();
        Timeout::set_default_timeout(Some(35));
        let timeout = Timeout::get(TimeoutType::Default);
        assert_eq!(timeout, 35);

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
        assert_eq!(timeout, 35);

        Timeout::set_default_timeout(Some(35));
    }

    #[test]
    fn test_set_default_timeout_override() {
        let _guard = TEST_GUARD.lock().unwrap();
        Timeout::set_default_timeout(Some(35));

        Timeout::set_default_timeout(Some(50));
        let timeout = Timeout::get(TimeoutType::Default);
        assert_eq!(timeout, 50);

        let timeout = Timeout::get(TimeoutType::ValidateDataSource(
            "mysql://".into_dsn().unwrap(),
        ));
        assert_eq!(timeout, 50);

        Timeout::set_default_timeout(Some(35));
    }

    #[test]
    fn test_set_default_timeout_none_noop() {
        let _guard = TEST_GUARD.lock().unwrap();
        Timeout::set_default_timeout(Some(35));

        Timeout::set_default_timeout(None);
        let timeout = Timeout::get(TimeoutType::Default);
        assert_eq!(timeout, 35);
    }
}
