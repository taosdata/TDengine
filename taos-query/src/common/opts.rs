use std::collections::BTreeMap;

use mdsn::Dsn;
// use url::ParseError;
use mdsn::DsnError as Error;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TaosOpts {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub params: BTreeMap<String, String>,
}

macro_rules! _build_opt {
    ($option:ident, $($doc:literal) *) => {
        $(#[doc = $doc])*
        pub fn $option<T: Into<String>>(mut self, $option: T) -> Self {
            self.$option = Some($option.into());
            self
        }
    };
    ($option:ident, $ty:ty, $($doc:literal) *) => {
        $(#[doc = $doc])*
        pub fn $option<T: Into<$ty>>(mut self, $option: T) -> Self {
            self.$option = Some($option.into());
            self
        }
    };
}

impl TaosOpts {
    /// Default to [Default::default].
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_dsn_string(&self) -> String {
        todo!()
    }

    /// Parse from a [DSN](https://en.wikipedia.org/wiki/Data_source_name) string.
    ///
    /// We use URL-described DSN style like this:
    ///
    /// ```text
    /// <driver>[+<protocol>]://<username>:<password>@<host>:<port>[,<host2>:<port2>]/<database>?<params>
    /// ```
    ///
    /// - **driver**: to distinct from other data source, for TDengine, always use `taos`.
    /// - **protocol**: additional information for connection, for TDengine, we have a plan to support
    ///     taosc(the default protocol), http and websocket(with protocol identifier: `ws`).
    /// - **username**: username for the connection
    /// - **password**: password for the current user.
    /// - **host**: host to TDengine server.
    /// - **port**: port to TDengine server.
    /// - **database**: default database for the connection, so that you don't need to write with full
    ///     database name in sql like `select * from database.tb1`.
    /// - **params**: in-query parameters are key-value pairs, drivers should be aware of the params list
    ///     to support specific configuration for each driver or protocol.
    ///
    pub fn parse(dsn: &str) -> Result<TaosOpts, Error> {
        if dsn.is_empty() {
            return Ok(Self::new());
        }
        let _ = Dsn::parse(dsn)?;

        Ok(Self::default())
    }

    _build_opt!(host, "Set host name or ip address of TDengine server");
    _build_opt!(username, "Set default username for TDengine connection");
    _build_opt!(password, "Set default password for TDengine connection");
    _build_opt!(database, "Set default database name of the connection");
    _build_opt!(port, u16, "Set port to the TDengine server");

    /// Set other params by k-v pair.
    pub fn with<K: Into<String>, V: Into<String>>(
        mut self,
        iter: impl Iterator<Item = (K, V)>,
    ) -> Self {
        for (k, v) in iter {
            self.set(k, v);
        }
        self
    }

    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.params.insert(key.into(), value.into());
        self
    }
}

#[test]
fn test_options() {
    let opts = TaosOpts::parse("postgresql://root:pass@tcp(host1:123,host2:456)/somedb?target_session_attrs=any&application_name=myapp");
    dbg!(opts);
}

#[test]
fn test_options_builder_all() {
    let opts = TaosOpts::new()
        .host("localhost")
        .port(6030u16)
        .username("root")
        .password("taosdata");

    assert_eq!(
        opts,
        TaosOpts {
            host: Some("localhost".to_string()),
            port: Some(6030),
            username: Some("root".to_string()),
            password: Some("taosdata".to_string()),
            database: None,
            params: Default::default(),
        },
        "builder pattern for TaosOpts"
    );
}
