#![recursion_limit = "256"]
use std::fmt::{Debug, Display};

use infra::WsConnReq;
use once_cell::sync::OnceCell;

#[cfg(feature = "async")]
use asyn::WsTaos;
use sync::WsClient;

use taos_query::{common::RawMeta, DsnError, IntoDsn, Queryable, TBuilder};

mod infra;

#[cfg(feature = "async")]
pub mod asyn;
pub mod stmt;
pub mod sync; // todo: if use name `async`, rust-analyzer does not recognize the tests.

// pub mod tmq;
pub mod consumer;

#[derive(Debug, Clone)]
pub enum WsAuth {
    Token(String),
    Plain(String, String),
}

#[derive(Debug, Clone)]
pub struct TaosBuilder {
    scheme: &'static str, // ws or wss
    addr: String,
    auth: WsAuth,
    database: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub struct Error {
    source: anyhow::Error,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source.to_string())
    }
}

impl From<DsnError> for Error {
    fn from(err: DsnError) -> Self {
        Error { source: err.into() }
    }
}

impl TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        &["token"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        Ok(Self::from_dsn(dsn.into_dsn()?)?)
    }

    fn client_version() -> &'static str {
        "0"
    }
    fn ping(&self, _: &mut Self::Target) -> Result<(), Self::Error> {
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        Ok(Taos {
            dsn: self.clone(),
            #[cfg(feature = "async")]
            async_client: OnceCell::new(),
            sync_client: OnceCell::new(),
        })
    }
}

impl TaosBuilder {
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self, DsnError> {
        let mut dsn = dsn.into_dsn()?;
        let scheme = match (
            dsn.driver.as_str(),
            dsn.protocol.as_ref().map(|s| s.as_str()),
        ) {
            ("ws" | "http", _) => "ws",
            ("wss" | "https", _) => "wss",
            ("taos", Some("ws" | "http") | None) => "ws",
            ("taos", Some("wss" | "https")) => "wss",
            _ => Err(DsnError::InvalidDriver(dsn.to_string()))?,
        };
        let token = dsn.params.remove("token");

        let addr = match dsn.addresses.first() {
            Some(addr) => addr.to_string(),
            None => "localhost:6041".to_string(),
        };

        if let Some(token) = token {
            Ok(TaosBuilder {
                scheme,
                addr,
                auth: WsAuth::Token(token),
                database: dsn.database,
            })
        } else {
            let username = dsn.username.unwrap_or("root".to_string());
            let password = dsn.password.unwrap_or("taosdata".to_string());
            Ok(TaosBuilder {
                scheme,
                addr,
                auth: WsAuth::Plain(username, password),
                database: dsn.database,
            })
        }
    }
    pub(crate) fn to_query_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/ws?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/ws", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_stmt_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/stmt?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/stmt", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_tmq_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/tmq?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/tmq", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_conn_request(&self) -> WsConnReq {
        match &self.auth {
            WsAuth::Token(_token) => WsConnReq {
                user: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                db: self.database.as_ref().map(Clone::clone),
            },
            WsAuth::Plain(user, pass) => WsConnReq {
                user: Some(user.to_string()),
                password: Some(pass.to_string()),
                db: self.database.as_ref().map(Clone::clone),
            },
        }
    }
}

#[derive(Debug)]
pub struct Taos {
    dsn: TaosBuilder,
    #[cfg(feature = "async")]
    async_client: OnceCell<WsTaos>,
    sync_client: OnceCell<WsClient>,
}

unsafe impl Send for Taos {}
unsafe impl Sync for Taos {}

impl Queryable for Taos {
    type Error = sync::Error;

    type ResultSet = sync::ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> std::result::Result<Self::ResultSet, Self::Error> {
        if let Some(ws) = self.sync_client.get() {
            ws.s_query(sql.as_ref())
        } else {
            let sync_client = WsClient::from_wsinfo(&self.dsn)?;
            self.sync_client
                .get_or_init(|| sync_client)
                .s_query(sql.as_ref())
        }
    }

    fn exec<T: AsRef<str>>(&self, sql: T) -> std::result::Result<usize, Self::Error> {
        log::info!("execute sql: {}", sql.as_ref());
        if let Some(ws) = self.sync_client.get() {
            ws.s_exec(sql.as_ref())
        } else {
            let sync_client = WsClient::from_wsinfo(&self.dsn)?;
            self.sync_client
                .get_or_init(|| sync_client)
                .s_exec(sql.as_ref())
        }
    }

    fn write_meta(&self, raw: RawMeta) -> Result<(), Self::Error> {
        if let Some(ws) = self.sync_client.get() {
            ws.write_meta(raw)
        } else {
            let sync_client = WsClient::from_wsinfo(&self.dsn)?;
            self.sync_client.get_or_init(|| sync_client).write_meta(raw)
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl taos_query::AsyncQueryable for Taos {
    type Error = asyn::Error;

    type AsyncResultSet = asyn::ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.s_query(sql.as_ref()).await
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .s_query(sql.as_ref())
                .await
        }
    }

    async fn write_raw_meta(&self, raw: RawMeta) -> Result<(), Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.write_meta(raw).await
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .write_meta(raw)
                .await
        }
    }

    async fn write_raw_block(&self, block: &taos_query::RawBlock) -> Result<(), Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use taos_query::TBuilder;

    use crate::TaosBuilder;


    #[test]
    fn ws_sync_json() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();
        use taos_query::{Fetchable, Queryable};
        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?.build()?;
        let db = "ws_sync_json";
        assert_eq!(client.exec(format!("drop database if exists {db}"))?, 0);
        assert_eq!(client.exec(format!("create database {db} keep 36500"))?, 0);
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            )?,
            0
        );
        assert_eq!(
            client.exec(format!(
                r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
            ))?,
            2
        );
        assert_eq!(
            client.exec(format!(
                r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
            ))?,
            2
        );

        // let mut rs = client.s_query("select * from wsabc.tb1").unwrap().unwrap();
        let mut rs = client.query(format!("select * from {db}.tb1 order by ts limit 1"))?;

        #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            b1: bool,
            c8i1: i8,
            c16i1: i16,
            c32i1: i32,
            c64i1: i64,
            c8u1: u8,
            c16u1: u16,
            c32u1: u32,
            c64u1: u64,

            c8i2: i8,
            c16i2: i16,
            c32i2: i32,
            c64i2: i64,
            c8u2: u8,
            c16u2: u16,
            c32u2: u32,
            c64u2: u64,

            cb1: String,
            cb2: String,
            cn1: String,
            cn2: String,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(&values);

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T00:00:00".to_string(),
                b1: true,
                c8i1: -1,
                c16i1: -2,
                c32i1: -3,
                c64i1: -4,
                c8u1: 1,
                c16u1: 2,
                c32u1: 3,
                c64u1: 4,
                c8i2: -5,
                c16i2: -6,
                c32i2: -7,
                c64i2: -8,
                c8u2: 5,
                c16u2: 6,
                c32u2: 7,
                c64u2: 8,
                cb1: "abc".to_string(),
                cb2: "def".to_string(),
                cn1: "涛思".to_string(),
                cn2: "数据".to_string(),
            }
        );

        assert_eq!(client.exec(format!("drop database {db}"))?, 0);
        Ok(())
    }

    #[test]
    fn ws_sync() -> anyhow::Result<()> {
        use taos_query::{Fetchable, Queryable};
        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?.build()?;
        assert_eq!(client.exec("drop database if exists wsabc")?, 0);
        assert_eq!(client.exec("create database wsabc keep 36500")?, 0);
        assert_eq!(
            client.exec(
                "create table wsabc.tb1(ts timestamp,\
                    c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16))"
            )?,
            0
        );
        assert_eq!(
            client.exec(
                "insert into wsabc.tb1 values(65535,\
                -1,-2,-3,-4, 1,2,3,4, 'abc', '涛思',\
                -5,-6,-7,-8, 5,6,7,8, 'def', '数据')"
            )?,
            1
        );

        // let mut rs = client.s_query("select * from wsabc.tb1").unwrap().unwrap();
        let mut rs = client.query("select * from wsabc.tb1")?;

        #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            c8i1: i8,
            c16i1: i16,
            c32i1: i32,
            c64i1: i64,
            c8u1: u8,
            c16u1: u16,
            c32u1: u32,
            c64u1: u64,

            c8i2: i8,
            c16i2: i16,
            c32i2: i32,
            c64i2: i64,
            c8u2: u8,
            c16u2: u16,
            c32u2: u32,
            c64u2: u64,

            cb1: String,
            cb2: String,
            cn1: String,
            cn2: String,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(&values);

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T00:01:05.535".to_string(),
                c8i1: -1,
                c16i1: -2,
                c32i1: -3,
                c64i1: -4,
                c8u1: 1,
                c16u1: 2,
                c32u1: 3,
                c64u1: 4,
                c8i2: -5,
                c16i2: -6,
                c32i2: -7,
                c64i2: -8,
                c8u2: 5,
                c16u2: 6,
                c32u2: 7,
                c64u2: 8,
                cb1: "abc".to_string(),
                cb2: "def".to_string(),
                cn1: "涛思".to_string(),
                cn2: "数据".to_string(),
            }
        );

        assert_eq!(client.exec("drop database wsabc")?, 0);
        Ok(())
    }

    #[test]
    fn ws_show_databases() -> anyhow::Result<()> {
        use taos_query::{Fetchable, Queryable, TBuilder};
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos:///".to_string());

        let client = TaosBuilder::from_dsn(dsn)?.build()?;
        let mut rs = client.query("select groupid from test.d0")?;
        let values = rs.to_rows_vec()?;

        dbg!(values);
        Ok(())
    }
    #[cfg(feature = "async")]
    // !Websocket tests should always use `multi_thread`
    #[tokio::test(flavor = "multi_thread")]
    async fn test_client() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();
        use futures::TryStreamExt;
        use taos_query::{AsyncFetchable, AsyncQueryable};

        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?.build()?;
        assert_eq!(
            client
                .exec("create database if not exists ws_abc_a")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("create table if not exists ws_abc_a.tb1(ts timestamp, v int)")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("insert into ws_abc_a.tb1 values(1655793421375, 1)")
                .await?,
            1
        );

        // let mut rs = client.s_query("select * from ws_abc_a.tb1").unwrap().unwrap();
        let mut rs = client.query("select * from ws_abc_a.tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: i32,
        }

        let values: Vec<A> = rs.deserialize_stream().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database ws_abc_a").await?, 0);
        Ok(())
    }
}
