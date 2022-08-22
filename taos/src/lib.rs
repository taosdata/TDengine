pub use taos_query::prelude::*;
pub use taos_query::Manager;

pub mod sync {
    pub use taos_query::prelude::sync::*;
    pub use super::Stmt;
    pub use super::{Taos, TaosBuilder};
    pub use super::tmq::{Consumer, MessageSet, TmqBuilder};
}

mod stmt;
pub use stmt::Stmt;

mod tmq;
pub use tmq::{Consumer, MessageSet, TmqBuilder};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Dsn(#[from] DsnError),
    #[error(transparent)]
    Raw(#[from] RawError),
    #[error(transparent)]
    Native(#[from] taos_sys::Error),
    #[error(transparent)]
    Ws(#[from] taos_ws::Error),
    #[error(transparent)]
    WsQueryError(#[from] taos_ws::asyn::Error),
    #[error(transparent)]
    WsTmqError(#[from] taos_ws::consumer::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}

enum TaosBuilderInner {
    Native(taos_sys::TaosBuilder),
    Ws(taos_ws::TaosBuilder),
}
enum TaosInner {
    Native(taos_sys::Taos),
    Ws(taos_ws::Taos),
}

enum ResultSetInner {
    Native(taos_sys::ResultSet),
    Ws(taos_ws::ResultSet),
}

pub struct TaosBuilder(TaosBuilderInner);
pub struct Taos(TaosInner);
pub struct ResultSet(ResultSetInner);

impl TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        &[]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let dsn = dsn.into_dsn()?;
        // dbg!(&dsn);
        match (
            dsn.driver.as_str(),
            dsn.protocol.as_ref().map(|s| s.as_str()),
        ) {
            ("ws" | "wss" | "http" | "https" | "taosws" |"taoswss", _) => Ok(Self(TaosBuilderInner::Ws(
                taos_ws::TaosBuilder::from_dsn(dsn)?,
            ))),
            ("taos" | "tmq", None) => Ok(Self(TaosBuilderInner::Native(
                taos_sys::TaosBuilder::from_dsn(dsn)?,
            ))),
            ("taos" | "tmq", Some("ws" | "wss" | "http" | "https")) => Ok(Self(
                TaosBuilderInner::Ws(taos_ws::TaosBuilder::from_dsn(dsn)?),
            )),
            (driver, _) => Err(DsnError::InvalidDriver(driver.to_string()).into()),
        }
    }

    fn client_version() -> &'static str {
        ""
    }

    fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => match &mut conn.0 {
                TaosInner::Native(taos) => Ok(b.ping(taos)?),
                _ => unreachable!(),
            },
            TaosBuilderInner::Ws(b) => match &mut conn.0 {
                TaosInner::Ws(taos) => Ok(b.ping(taos)?),
                _ => unreachable!(),
            },
        }
    }

    fn ready(&self) -> bool {
        match &self.0 {
            TaosBuilderInner::Native(b) => b.ready(),
            TaosBuilderInner::Ws(b) => b.ready(),
        }
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(Taos(TaosInner::Native(b.build()?))),
            TaosBuilderInner::Ws(b) => Ok(Taos(TaosInner::Ws(b.build()?))),
        }
    }
}

impl AsyncFetchable for ResultSet {
    type Error = Error;

    fn affected_rows(&self) -> i32 {
        match &self.0 {
            ResultSetInner::Native(rs) => {
                <taos_sys::ResultSet as AsyncFetchable>::affected_rows(rs)
            }
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::affected_rows(rs),
        }
    }

    fn precision(&self) -> Precision {
        match &self.0 {
            ResultSetInner::Native(rs) => <taos_sys::ResultSet as AsyncFetchable>::precision(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::precision(rs),
        }
    }

    fn fields(&self) -> &[Field] {
        match &self.0 {
            ResultSetInner::Native(rs) => <taos_sys::ResultSet as AsyncFetchable>::fields(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::fields(rs),
        }
    }

    fn summary(&self) -> (usize, usize) {
        match &self.0 {
            ResultSetInner::Native(rs) => <taos_sys::ResultSet as AsyncFetchable>::summary(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::summary(rs),
        }
    }

    fn update_summary(&mut self, nrows: usize) {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <taos_sys::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
        }
    }

    fn fetch_raw_block(
        self: &mut Self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<RawBlock>, Self::Error>> {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <taos_sys::ResultSet as AsyncFetchable>::fetch_raw_block(rs, cx).map_err(Into::into)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as AsyncFetchable>::fetch_raw_block(rs, cx).map_err(Into::into)
            }
        }
    }
}

impl taos_query::Fetchable for ResultSet {
    type Error = Error;

    fn affected_rows(&self) -> i32 {
        match &self.0 {
            ResultSetInner::Native(rs) => {
                <taos_sys::ResultSet as AsyncFetchable>::affected_rows(rs)
            }
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::affected_rows(rs),
        }
    }

    fn precision(&self) -> Precision {
        match &self.0 {
            ResultSetInner::Native(rs) => <taos_sys::ResultSet as AsyncFetchable>::precision(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::precision(rs),
        }
    }

    fn fields(&self) -> &[Field] {
        match &self.0 {
            ResultSetInner::Native(rs) => <taos_sys::ResultSet as AsyncFetchable>::fields(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::fields(rs),
        }
    }

    fn summary(&self) -> (usize, usize) {
        match &self.0 {
            ResultSetInner::Native(rs) => <taos_sys::ResultSet as AsyncFetchable>::summary(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::summary(rs),
        }
    }

    fn update_summary(&mut self, nrows: usize) {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <taos_sys::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
        }
    }

    fn fetch_raw_block(&mut self) -> Result<Option<RawBlock>, Self::Error> {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <taos_sys::ResultSet as taos_query::Fetchable>::fetch_raw_block(rs)
                    .map_err(Into::into)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as taos_query::Fetchable>::fetch_raw_block(rs)
                    .map_err(Into::into)
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncQueryable for Taos {
    type Error = Error;

    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => taos
                .query(sql)
                .await
                .map(ResultSetInner::Native)
                .map(ResultSet)
                .map_err(Into::into),
            TaosInner::Ws(taos) => taos
                .query(sql)
                .await
                .map(ResultSetInner::Ws)
                .map(ResultSet)
                .map_err(Into::into),
        }
    }

    async fn write_raw_meta(&self, meta: RawMeta) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => taos.write_raw_meta(meta).await.map_err(Into::into),
            TaosInner::Ws(taos) => taos.write_raw_meta(meta).await.map_err(Into::into),
        }
    }

    async fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => taos.write_raw_block(block).await.map_err(Into::into),
            TaosInner::Ws(taos) => taos.write_raw_block(block).await.map_err(Into::into),
        }
    }
}

impl taos_query::Queryable for Taos {
    type Error = Error;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Self::ResultSet, Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => <taos_sys::Taos as taos_query::Queryable>::query(taos, sql)
                .map(ResultSetInner::Native)
                .map(ResultSet)
                .map_err(Into::into),
            TaosInner::Ws(taos) => <taos_ws::Taos as taos_query::Queryable>::query(taos, sql)
                .map(ResultSetInner::Ws)
                .map(ResultSet)
                .map_err(Into::into),
        }
    }

    fn write_meta(&self, meta: RawMeta) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <taos_sys::Taos as taos_query::Queryable>::write_meta(taos, meta)
                    .map_err(Into::into)
            }
            TaosInner::Ws(taos) => {
                <taos_ws::Taos as taos_query::Queryable>::write_meta(taos, meta).map_err(Into::into)
            }
        }
    }
}
#[cfg(test)]
mod tests {

    use super::TaosBuilder;

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_json_test_native() -> anyhow::Result<()> {
        sync_json_test("taos:///")
    }
    #[test]
    fn sync_json_test_ws() -> anyhow::Result<()> {
        sync_json_test("ws://localhost:6041/")
    }
    #[test]
    fn sync_json_test_taosws() -> anyhow::Result<()> {
        sync_json_test("taosws://localhost:6041/")
    }

    #[test]
    fn null_test() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let taos = TaosBuilder::from_dsn("taosws://localhost:6041")?.build()?;
        taos.exec_many(["drop database if exists db", "create database db", "use db"])?;

        taos.exec(
            "create table st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)",
        )?;
        taos.exec("create table t1 using st tags(0)")?;
        taos.exec("insert into t1 values(1640000000000, 0)")?;
        taos.exec("create table t2 using st tags(254)")?;
        taos.exec("insert into t2 values(1640000000000, 254)")?;
        taos.exec("create table t3 using st tags(NULL)")?;
        taos.exec("insert into t3 values(1640000000000, NULL)")?;

        let mut rs = taos.query("select * from st where utntag is null")?;
        for row in rs.rows() {
            let row = row?;
            let values = row.into_values();
            assert_eq!(values[1], Value::Null);
            assert_eq!(values[2], Value::Null);
        }
        Ok(())
    }

    fn sync_json_test(dsn: &str) -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        use taos_query::{Fetchable, Queryable};
        let client = TaosBuilder::from_dsn(dsn)?.build()?;
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
}
