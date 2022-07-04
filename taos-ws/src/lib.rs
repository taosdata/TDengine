use std::{fmt::Debug, sync::Once};

#[cfg(feature = "async")]
use futures::FutureExt;
use infra::WsConnReq;
use once_cell::sync::{Lazy, OnceCell};

#[cfg(feature = "async")]
use asyn::WsAsyncClient;
use sync::WsClient;
use thiserror::Error;
// use websocket::{client::ParseError, WebSocketError};

use taos_query::{AsyncQueryable, DeError, Dsn, DsnError, FromDsn, IntoDsn, Queryable};

pub mod infra;

#[cfg(feature = "async")]
pub mod asyn;
#[cfg(feature = "stmt")]
pub mod stmt;
pub mod sync; // todo: if use name `async`, rust-analyzer does not recognize the tests.

#[derive(Debug)]
pub enum WsAuth {
    Token(String),
    Plain(String, String),
}

#[derive(Debug)]
pub struct WsInfo {
    scheme: &'static str, // ws or wss
    addr: String,
    auth: WsAuth,
    database: Option<String>,
}

impl WsInfo {
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self, DsnError> {
        let mut dsn = dsn.into_dsn()?;
        let scheme = match (
            dsn.driver.as_str(),
            dsn.protocol.as_ref().map(|s| s.as_str()),
        ) {
            ("ws" | "http", _) => "ws",
            ("wss" | "https", _) => "wss",
            ("taos", Some("ws" | "http")) => "ws",
            ("taos", Some("wss" | "https")) => "wss",
            _ => Err(DsnError::InvalidDriver(dsn.to_string()))?,
        };
        let token = dsn.params.remove("token");

        let addr = match dsn.addresses.first() {
            Some(addr) => addr.to_string(),
            None => "localhost:6041".to_string(),
        };

        if let Some(token) = token {
            Ok(WsInfo {
                scheme,
                addr,
                auth: WsAuth::Token(token),
                database: dsn.database,
            })
        } else {
            let username = dsn.username.unwrap_or("root".to_string());
            let password = dsn.password.unwrap_or("taosdata".to_string());
            Ok(WsInfo {
                scheme,
                addr,
                auth: WsAuth::Plain(username, password),
                database: dsn.database,
            })
        }
    }
    pub fn to_query_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/ws?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/ws", self.scheme, self.addr),
        }
    }

    pub fn to_stmt_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/stmt?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/stmt", self.scheme, self.addr),
        }
    }

    pub fn to_tmq_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/tmq?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/tmq", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_conn_request(&self) -> WsConnReq {
        match &self.auth {
            WsAuth::Token(token) => WsConnReq {
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
pub struct Ws {
    dsn: Dsn,
    #[cfg(feature = "async")]
    async_client: OnceCell<WsAsyncClient>,
    sync_client: OnceCell<WsClient>,
}

unsafe impl Send for Ws {}
unsafe impl Sync for Ws {}

impl FromDsn for Ws {
    type Err = DsnError;

    fn hygienize(
        dsn: taos_query::Dsn,
    ) -> Result<(taos_query::Dsn, Vec<taos_query::Address>), taos_query::DsnError> {
        todo!()
    }

    fn from_dsn<T: taos_query::IntoDsn>(dsn: T) -> Result<Self, Self::Err> {
        let dsn = dsn.into_dsn()?;
        Ok(Self {
            dsn,
            #[cfg(feature = "async")]
            async_client: OnceCell::new(),
            sync_client: OnceCell::new(),
        })
    }

    fn ping(dsn: &taos_query::Dsn) -> Result<(), Self::Err> {
        Ok(())
    }
}

impl<'q> Queryable<'q> for Ws {
    type Error = sync::Error;

    type ResultSet = sync::ResultSet;

    fn query<T: AsRef<str>>(&'q self, sql: T) -> std::result::Result<Self::ResultSet, Self::Error> {
        if let Some(ws) = self.sync_client.get() {
            ws.s_query(sql.as_ref())
        } else {
            let sync_client = WsClient::from_dsn(&self.dsn)?;
            self.sync_client
                .get_or_init(|| sync_client)
                .s_query(sql.as_ref())
        }
    }

    fn exec<T: AsRef<str>>(&'q self, sql: T) -> std::result::Result<usize, Self::Error> {
        if let Some(ws) = self.sync_client.get() {
            ws.s_exec(sql.as_ref())
        } else {
            let sync_client = WsClient::from_dsn(&self.dsn)?;
            self.sync_client
                .get_or_init(|| sync_client)
                .s_exec(sql.as_ref())
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<'q> AsyncQueryable<'q> for Ws {
    type Error = asyn::Error;

    type AsyncResultSet = asyn::ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &'q self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.s_query(sql.as_ref()).await
        } else {
            let async_client = WsAsyncClient::from_dsn(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .s_query(sql.as_ref())
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use taos_query::FromDsn;

    use super::Ws;

    #[test]
    fn ws_sync() -> anyhow::Result<()> {
        use taos_query::{Fetchable, Queryable};
        let client = Ws::from_dsn("ws://localhost:6041/")?;
        assert_eq!(client.exec("create database if not exists wsabc")?, 0);
        assert_eq!(
            client.exec("create table if not exists wsabc.tb1(ts timestamp, v int)")?,
            0
        );
        assert_eq!(client.exec("insert into wsabc.tb1 values(now, 1)")?, 1);

        // let mut rs = client.s_query("select * from wsabc.tb1").unwrap().unwrap();
        let mut rs = client.query("select * from wsabc.tb1")?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: i32,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(values);

        assert_eq!(client.exec("drop database wsabc")?, 0);
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

        let client = Ws::from_dsn("ws://localhost:6041/")?;
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
