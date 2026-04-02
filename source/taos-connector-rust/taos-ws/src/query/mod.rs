use std::sync::Arc;

pub(crate) use asyn::WsTaos;
pub use asyn::{Error, ResultSet};
use faststr::FastStr;
pub use messages::ConnOption;
pub(crate) use messages::WsConnReq;
pub use messages::{BindType, Stmt2Field};
use taos_query::common::{RawMeta, SmlData};
use taos_query::prelude::RawResult;
use taos_query::AsyncQueryable;

use crate::TaosBuilder;

pub mod asyn;
pub use asyn::check_server_status;
mod conn;
pub(super) use conn::send_conn_request;
pub(crate) mod messages;

#[derive(Debug)]
pub struct Taos {
    pub(crate) builder: TaosBuilder,
    pub(crate) client: Arc<WsTaos>,
}

impl Taos {
    pub(super) async fn from_builder(builder: TaosBuilder) -> RawResult<Self> {
        let client = WsTaos::from_builder(&builder).await?;
        Ok(Self { builder, client })
    }

    pub fn version(&self) -> FastStr {
        self.client.version()
    }

    pub fn get_req_id(&self) -> u64 {
        self.client.get_req_id()
    }

    pub fn client(&self) -> &Arc<WsTaos> {
        &self.client
    }

    pub fn client_cloned(&self) -> Arc<WsTaos> {
        self.client.clone()
    }
}

unsafe impl Send for Taos {}
unsafe impl Sync for Taos {}

#[async_trait::async_trait]
impl taos_query::AsyncQueryable for Taos {
    type AsyncResultSet = asyn::ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<Self::AsyncResultSet> {
        self.client().s_query(sql.as_ref()).await
    }

    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> RawResult<Self::AsyncResultSet> {
        self.client()
            .s_query_with_req_id(sql.as_ref(), req_id)
            .await
    }

    async fn write_raw_meta(&self, raw: &RawMeta) -> RawResult<()> {
        self.client().write_meta(raw).await
    }

    async fn write_raw_block(&self, block: &taos_query::RawBlock) -> RawResult<()> {
        self.client().write_raw_block(block).await
    }

    async fn write_raw_block_with_req_id(
        &self,
        block: &taos_query::RawBlock,
        req_id: u64,
    ) -> RawResult<()> {
        self.client()
            .write_raw_block_with_req_id(block, req_id)
            .await
    }

    async fn put(&self, data: &SmlData) -> RawResult<()> {
        let _ = self.client().s_put(data).await?;
        Ok(())
    }
}

impl taos_query::Queryable for Taos {
    type ResultSet = asyn::ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> RawResult<Self::ResultSet> {
        let sql = sql.as_ref();
        taos_query::block_in_place_or_global(<Self as AsyncQueryable>::query(self, sql))
    }

    fn query_with_req_id<T: AsRef<str>>(&self, sql: T, req_id: u64) -> RawResult<Self::ResultSet> {
        let sql = sql.as_ref();
        taos_query::block_in_place_or_global(<Self as AsyncQueryable>::query_with_req_id(
            self, sql, req_id,
        ))
    }

    fn write_raw_meta(&self, meta: &RawMeta) -> RawResult<()> {
        crate::block_in_place_or_global(<Self as AsyncQueryable>::write_raw_meta(self, meta))
    }

    fn write_raw_block(&self, block: &taos_query::RawBlock) -> RawResult<()> {
        crate::block_in_place_or_global(<Self as AsyncQueryable>::write_raw_block(self, block))
    }

    fn write_raw_block_with_req_id(
        &self,
        block: &taos_query::RawBlock,
        req_id: u64,
    ) -> RawResult<()> {
        crate::block_in_place_or_global(<Self as AsyncQueryable>::write_raw_block_with_req_id(
            self, block, req_id,
        ))
    }

    fn put(&self, sml_data: &SmlData) -> RawResult<()> {
        crate::block_in_place_or_global(<Self as AsyncQueryable>::put(self, sml_data))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use serde::Deserialize;
    use taos_query::util::hex::*;

    use crate::{query::asyn::WS_ERROR_NO, TaosBuilder};

    #[test]
    fn ws_sync_json() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        unsafe { std::env::set_var("RUST_LOG", "debug") };

        let client = TaosBuilder::from_dsn("taosws://localhost:6041/")?.build()?;
        let db = "ws_sync_json_1752216123";
        assert_eq!(client.exec(format!("drop database if exists {db}"))?, 0);
        assert_eq!(client.exec(format!("create database {db} keep 36500"))?, 0);
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10), cvb1 varbinary(50), cg1 geometry(50),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16), cvb2 varbinary(50), cg2 geometry(50)) tags (jt json)")
            )?,
            0
        );
        assert_eq!(
            client.exec(format!(
                r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                 values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思', '\x123456', 'POINT(1 2)',
                              false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据', '\x654321', 'POINT(3 4)')
                       (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL, NULL,  NULL,
                              NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL, NULL,  NULL)"#
            ))?,
            2
        );
        assert_eq!(
            client.exec(format!(
                r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                       values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思', '\x123456', 'POINT(1 2)',
                                    false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据', '\x654321', 'POINT(3 4)')
                             (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL, NULL,  NULL,
                                    NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL, NULL,  NULL)"#
            ))?,
            2
        );

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

            cvb1: Bytes,
            cvb2: Bytes,
            cg1: Bytes,
            cg2: Bytes,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(&values);

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T08:00:00+08:00".to_string(),
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
                cvb1: Bytes::from(vec![0x12, 0x34, 0x56]),
                cvb2: Bytes::from(vec![0x65, 0x43, 0x21]),
                cg1: hex_string_to_bytes("0101000000000000000000F03F0000000000000040"),
                cg2: hex_string_to_bytes("010100000000000000000008400000000000001040"),
            }
        );

        assert_eq!(client.exec(format!("drop database {db}"))?, 0);
        Ok(())
    }

    #[test]
    fn ws_sync() -> anyhow::Result<()> {
        use bytes::Bytes;
        use taos_query::prelude::sync::*;
        use taos_query::util::hex::*;

        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?.build()?;
        assert_eq!(client.exec("drop database if exists ws_sync")?, 0);
        assert_eq!(client.exec("create database ws_sync keep 36500")?, 0);
        assert_eq!(
            client.exec(
                "create table ws_sync.tb1(ts timestamp,\
                    c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10), cvb1 varbinary(50), cg1 geometry(50),\
                    c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16), cvb2 varbinary(50), cg2 geometry(50))"
            )?,
            0
        );
        assert_eq!(
            client.exec(
                r#"insert into ws_sync.tb1 values(65535,
                -1,-2,-3,-4, 1,2,3,4, 'abc', '涛思', '\x123456', 'POINT(1 2)',
                -5,-6,-7,-8, 5,6,7,8, 'def', '数据', '\x654321', 'POINT(3 4)')"#
            )?,
            1
        );

        let mut rs = client.query("select * from ws_sync.tb1")?;

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

            cvb1: Bytes,
            cvb2: Bytes,
            cg1: Bytes,
            cg2: Bytes,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(&values);

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T08:01:05.535+08:00".to_string(),
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
                cvb1: Bytes::from(vec![0x12, 0x34, 0x56]),
                cvb2: Bytes::from(vec![0x65, 0x43, 0x21]),
                cg1: hex_string_to_bytes("0101000000000000000000F03F0000000000000040"),
                cg2: hex_string_to_bytes("010100000000000000000008400000000000001040"),
            }
        );

        assert_eq!(client.exec("drop database ws_sync")?, 0);
        Ok(())
    }

    #[test]
    fn ws_show_databases() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos:///".to_string());

        let client = TaosBuilder::from_dsn(dsn)?.build()?;
        let mut rs = client.query("show databases")?;
        let values = rs.to_rows_vec()?;

        dbg!(values);
        Ok(())
    }

    async fn _ws_select_from_meters() -> anyhow::Result<()> {
        unsafe { std::env::set_var("RUST_LOG", "info") };
        use taos_query::prelude::*;
        let dsn = "taos+ws:///test";
        let client = TaosBuilder::from_dsn(dsn)?.build().await?;

        let mut rs = client.query("select * from meters").await?;

        let mut blocks = rs.blocks();
        let mut nb = 0;
        let mut nr = 0;
        while let Some(block) = blocks.try_next().await? {
            nb += 1;
            nr += block.nrows();
        }
        let summary = rs.summary();
        dbg!(summary, (nb, nr));
        Ok(())
    }

    #[tokio::test]
    async fn test_client() -> anyhow::Result<()> {
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        use futures::TryStreamExt;
        use taos_query::{AsyncFetchable, AsyncQueryable, AsyncTBuilder};

        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?;
        let client = client.build().await?;
        assert_eq!(
            client
                .exec("create database if not exists ws_test_client")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("create table if not exists ws_test_client.tb1(ts timestamp, v int)")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("insert into ws_test_client.tb1 values(1655793421375, 1)")
                .await?,
            1
        );

        let mut rs = client.query("select * from ws_test_client.tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: i32,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database ws_test_client").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_disconnect_with_mock() {
        use futures::{SinkExt, StreamExt};
        use serde_json::json;
        use taos_query::{AsyncQueryable, AsyncTBuilder};
        use tokio::sync::{mpsc, oneshot};
        use tracing::debug;
        use warp::Filter;

        let (query_tx, query_rx) = oneshot::channel();

        tokio::spawn(async move {
            let builder = TaosBuilder::from_dsn("ws://127.0.0.1:9981/").unwrap();
            let client = builder.build().await.unwrap();
            let _ = client.query("select * from meters").await;
            debug!("query end");
            let _ = query_tx.send(());
        });

        let (close_tx, mut close_rx) = mpsc::channel(1);
        let routes = warp::path("ws").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let (mut tx, mut rx) = ws.split();
                    while let Some(msg) = rx.next().await {
                        let msg = msg.unwrap();
                        debug!("ws recv msg: {msg:?}");
                        if msg.is_text() {
                            let text = msg.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "version msg",
                                    "action": "version",
                                    "req_id": 100,
                                    "version": "3.0"
                                });
                                let msg = warp::ws::Message::text(data.to_string());
                                let _ = tx.send(msg).await;
                            } else if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "conn msg",
                                    "action": "conn",
                                    "req_id": 100
                                });
                                let msg = warp::ws::Message::text(data.to_string());
                                let _ = tx.send(msg).await;
                            } else if text.contains("query") {
                                let _ = close.send(()).await;
                                break;
                            }
                        }
                    }
                })
            }
        });

        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9981), async move {
                let _ = close_rx.recv().await;
                debug!("Shutting down...");
            });

        server.await;
        let _ = query_rx.await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_ws_ipv6() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://[::1]:6041")?.build().await?;

        taos.exec_many([
            "drop database if exists test_1748584226",
            "create database test_1748584226",
            "use test_1748584226",
            "create table t0(ts timestamp, c1 int)",
            "insert into t0 values(1726803358466, 101)",
            "insert into t0 values(1726803359466, 102)",
        ])
        .await?;

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let mut rs = taos.query("select * from t0").await?;
        let rows: Vec<Row> = rs.deserialize().try_collect().await?;

        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].ts, 1726803358466);
        assert_eq!(rows[1].ts, 1726803359466);

        assert_eq!(rows[0].c1, 101);
        assert_eq!(rows[1].c1, 102);

        taos.exec("drop database test_1748584226").await?;

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_blob() -> anyhow::Result<()> {
        use serde::Deserialize;
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1753079593",
            "create database test_1753079593",
            "use test_1753079593",
            "create table t0(ts timestamp, c1 blob)",
            "insert into t0 values(1752218982761, null)",
            "insert into t0 values(1752218982762, '')",
            "insert into t0 values(1752218982763, 'hello')",
            "insert into t0 values(1752218982764, '\\x12345678')",
        ])
        .await?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: i64,
            c1: Option<Vec<u8>>,
        }

        let records: Vec<Record> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(records.len(), 4);

        assert_eq!(records[0].ts, 1752218982761);
        assert_eq!(records[1].ts, 1752218982762);
        assert_eq!(records[2].ts, 1752218982763);
        assert_eq!(records[3].ts, 1752218982764);

        assert_eq!(records[0].c1, None);
        assert_eq!(records[1].c1, Some(vec![]));
        assert_eq!(records[2].c1, Some(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(records[3].c1, Some(vec![0x12, 0x34, 0x56, 0x78]));

        taos.exec("drop database test_1753079593").await?;

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_blob_all_types() -> anyhow::Result<()> {
        use bytes::Bytes;
        use serde::Deserialize;
        use taos_query::prelude::*;
        use taos_query::util::hex::hex_string_to_bytes;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1753079609",
            "create database test_1753079609",
            "use test_1753079609",
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                c14 varbinary(10), c15 geometry(50), c16 decimal(10, 5), c17 decimal(20, 5), \
                c18 blob)",
            "insert into t0 values (1741780784752, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, \
                'hello', 'hello', 'hello', 'POINT(1 1)', 12345.12345, 123456789012345.12345, \
                '\\x12345678')",
            "insert into t0 values (1741780784753, null, null, null, null, null, null, \
                null, null, null, null, null, null, null, null, null, null, null, null)",
        ])
        .await?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: i64,
            c1: Option<bool>,
            c2: Option<i8>,
            c3: Option<i16>,
            c4: Option<i32>,
            c5: Option<i64>,
            c6: Option<u8>,
            c7: Option<u16>,
            c8: Option<u32>,
            c9: Option<u64>,
            c10: Option<f32>,
            c11: Option<f64>,
            c12: Option<String>,
            c13: Option<String>,
            c14: Option<Bytes>,
            c15: Option<Bytes>,
            c16: Option<String>,
            c17: Option<String>,
            c18: Option<Bytes>,
        }

        let records: Vec<Record> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].ts, 1741780784752);
        assert_eq!(records[1].ts, 1741780784753);

        assert_eq!(records[0].c1, Some(true));
        assert_eq!(records[1].c1, None);

        assert_eq!(records[0].c2, Some(1));
        assert_eq!(records[1].c2, None);

        assert_eq!(records[0].c3, Some(1));
        assert_eq!(records[1].c3, None);

        assert_eq!(records[0].c4, Some(1));
        assert_eq!(records[1].c4, None);

        assert_eq!(records[0].c5, Some(1));
        assert_eq!(records[1].c5, None);

        assert_eq!(records[0].c6, Some(1));
        assert_eq!(records[1].c6, None);

        assert_eq!(records[0].c7, Some(1));
        assert_eq!(records[1].c7, None);

        assert_eq!(records[0].c8, Some(1));
        assert_eq!(records[1].c8, None);

        assert_eq!(records[0].c9, Some(1));
        assert_eq!(records[1].c9, None);

        assert_eq!(records[0].c10, Some(1.1));
        assert_eq!(records[1].c10, None);

        assert_eq!(records[0].c11, Some(1.1));
        assert_eq!(records[1].c11, None);

        assert_eq!(records[0].c12, Some("hello".to_string()));
        assert_eq!(records[1].c12, None);

        assert_eq!(records[0].c13, Some("hello".to_string()));
        assert_eq!(records[1].c13, None);

        assert_eq!(records[0].c14, Some(Bytes::from("hello")));
        assert_eq!(records[1].c14, None);

        assert_eq!(
            records[0].c15,
            Some(hex_string_to_bytes(
                "0101000000000000000000f03f000000000000f03f"
            ))
        );
        assert_eq!(records[1].c15, None);

        assert_eq!(records[0].c16, Some("12345.12345".to_string()));
        assert_eq!(records[1].c16, None);

        assert_eq!(records[0].c17, Some("123456789012345.12345".to_string()));
        assert_eq!(records[1].c17, None);

        assert_eq!(records[0].c18, Some(Bytes::from("\x12\x34\x56\x78")));
        assert_eq!(records[1].c18, None);

        taos.exec("drop database test_1753079609").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_resp_timeout() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        use taos_query::util::ws_proxy::*;

        let intercept: InterceptFn = {
            Arc::new(move |_msg, _ctx| {
                tokio::task::block_in_place(|| {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                });
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8900", "ws://localhost:6041/ws", intercept).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8900?read_timeout=1")?
            .build()
            .await?;

        let err = taos.exec("show databases").await.unwrap_err();
        assert_eq!(err.code(), WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code());

        Ok(())
    }
}
