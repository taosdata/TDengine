use taos_optin::Stmt2 as NativeStmt2;
use taos_query::stmt2::Stmt2BindParam;
use taos_query::RawResult;
use taos_ws::Stmt2 as WsStmt2;

use crate::{ResultSet, ResultSetInner, TaosInner};

#[derive(Debug)]
enum Stmt2Inner {
    Native(NativeStmt2),
    Ws(WsStmt2),
}

#[derive(Debug)]
pub struct Stmt2(Stmt2Inner);

impl taos_query::stmt2::Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        match &taos.0 {
            TaosInner::Native(taos) => NativeStmt2::init(taos).map(Stmt2Inner::Native).map(Stmt2),
            TaosInner::Ws(taos) => WsStmt2::init(taos).map(Stmt2Inner::Ws).map(Stmt2),
        }
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Native(stmt2) => {
                stmt2.prepare(sql)?;
            }
            Stmt2Inner::Ws(stmt2) => {
                stmt2.prepare(sql)?;
            }
        }
        Ok(self)
    }

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Native(stmt2) => {
                stmt2.bind(params)?;
            }
            Stmt2Inner::Ws(stmt2) => {
                stmt2.bind(params)?;
            }
        }
        Ok(self)
    }

    fn exec(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            Stmt2Inner::Native(stmt2) => Ok(stmt2.exec()?),
            Stmt2Inner::Ws(stmt2) => Ok(stmt2.exec()?),
        }
    }

    fn affected_rows(&self) -> usize {
        match &self.0 {
            Stmt2Inner::Native(stmt2) => stmt2.affected_rows(),
            Stmt2Inner::Ws(stmt2) => {
                <WsStmt2 as taos_query::stmt2::Stmt2Bindable<taos_ws::Taos>>::affected_rows(stmt2)
            }
        }
    }

    fn result_set(&self) -> RawResult<ResultSet> {
        match &self.0 {
            Stmt2Inner::Native(stmt2) => stmt2
                .result_set()
                .map(ResultSetInner::Native)
                .map(ResultSet),
            Stmt2Inner::Ws(stmt2) => stmt2.result_set().map(ResultSetInner::Ws).map(ResultSet),
        }
    }
}

#[async_trait::async_trait]
impl taos_query::stmt2::Stmt2AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        match &taos.0 {
            TaosInner::Native(taos) => NativeStmt2::init(taos)
                .await
                .map(Stmt2Inner::Native)
                .map(Stmt2),
            TaosInner::Ws(taos) => WsStmt2::init(taos).await.map(Stmt2Inner::Ws).map(Stmt2),
        }
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Native(stmt2) => {
                stmt2.prepare(sql).await?;
            }
            Stmt2Inner::Ws(stmt2) => {
                stmt2.prepare(sql).await?;
            }
        }
        Ok(self)
    }

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Native(stmt2) => {
                stmt2.bind(params).await?;
            }
            Stmt2Inner::Ws(stmt2) => {
                stmt2.bind(params).await?;
            }
        }
        Ok(self)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            Stmt2Inner::Native(stmt2) => Ok(stmt2.exec().await?),
            Stmt2Inner::Ws(stmt2) => Ok(stmt2.exec().await?),
        }
    }

    async fn affected_rows(&self) -> usize {
        match &self.0 {
            Stmt2Inner::Native(stmt2) => stmt2.affected_rows().await,
            Stmt2Inner::Ws(stmt2) => {
                <WsStmt2 as taos_query::stmt2::Stmt2AsyncBindable<taos_ws::Taos>>::affected_rows(
                    stmt2,
                )
                .await
            }
        }
    }

    async fn result_set(&self) -> RawResult<ResultSet> {
        match &self.0 {
            Stmt2Inner::Native(stmt2) => stmt2
                .result_set()
                .await
                .map(ResultSetInner::Native)
                .map(ResultSet),
            Stmt2Inner::Ws(stmt2) => stmt2
                .result_set()
                .await
                .map(ResultSetInner::Ws)
                .map(ResultSet),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use taos_query::common::ColumnView;
    use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
    use taos_query::{Queryable, TBuilder};

    use super::Stmt2;
    use crate::sync::*;
    use crate::TaosBuilder;

    #[test]
    fn test_stmt2_insert_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231337";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many([
            &format!("drop database if exists {db}"),
            &format!("create database {db} keep 36500"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![None]),
            ColumnView::from_small_ints(vec![i16::MAX]),
            ColumnView::from_ints(vec![i32::MAX]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
            ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
            ColumnView::from_unsigned_ints(vec![u32::MAX]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["hello"]),
            ColumnView::from_nchar(vec!["中文"]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param])?.exec()?;
        assert_eq!(affected, 1);
        assert_eq!(stmt2.affected_rows(), 1);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: bool,
            c2: Option<i8>,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: Option<f32>,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(rows.len(), 1);

        let row = &rows[0];

        assert_eq!(row.ts, 1726803356466);
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, None);
        assert_eq!(row.c3, i16::MAX);
        assert_eq!(row.c4, i32::MAX);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c6, u8::MAX);
        assert_eq!(row.c7, u16::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10, Some(f32::MAX));
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }

    #[test]
    fn test_stmt2_insert_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411222151";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("insert into t0 values(?, ?)")?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param])?.exec()?;
        assert_eq!(affected, 4);
        assert_eq!(stmt2.affected_rows(), 4);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);
        assert_eq!(rows[2].ts, 1726803358466);
        assert_eq!(rows[3].ts, 1726803359466);

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }

    #[test]
    fn test_stmt2_query_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411281747";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
            &format!(
                "insert into t0 values(1726803356466, 1, NULL, 2, 3, 4, 5, 6, 7, 8, 1.1, 2.2, 'hello', '中文')"
            ),
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("select * from t0 where c8 > ? and c10 > ? and c12 = ?")?;

        let cols = vec![
            ColumnView::from_ints(vec![0]),
            ColumnView::from_floats(vec![0f32]),
            ColumnView::from_varchar(vec!["hello"]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param])?.exec()?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows(), 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: bool,
            c2: Option<u8>,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: Option<f32>,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = stmt2.result_set()?.deserialize().try_collect()?;
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.ts, 1726803356466);
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, None);
        assert_eq!(row.c3, 2);
        assert_eq!(row.c4, 3);
        assert_eq!(row.c5, 4);
        assert_eq!(row.c6, 5);
        assert_eq!(row.c7, 6);
        assert_eq!(row.c8, 7);
        assert_eq!(row.c9, 8);
        assert_eq!(row.c10, Some(1.1));
        assert_eq!(row.c11, 2.2);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }

    #[test]
    fn test_stmt2_query_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411222202";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(1726803356466, 99)",
            "insert into t0 values(1726803357466, 100)",
            "insert into t0 values(1726803358466, 101)",
            "insert into t0 values(1726803359466, 102)",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("select * from t0 where c1 > ?")?;

        let cols = vec![ColumnView::from_ints(vec![100])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param])?.exec()?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows(), 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = stmt2.result_set()?.deserialize().try_collect()?;
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].ts, 1726803358466);
        assert_eq!(rows[1].ts, 1726803359466);

        assert_eq!(rows[0].c1, 101);
        assert_eq!(rows[1].c1, 102);

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }

    #[test]
    fn test_stmt2_native() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop database if exists test_1769308834",
            "create database test_1769308834",
            "use test_1769308834",
            "create table t0 (ts timestamp, c1 int)",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("insert into t0 values(?, ?)")?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param])?.exec()?;
        assert_eq!(affected, 4);
        assert_eq!(stmt2.affected_rows(), 4);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        stmt2.prepare("select * from t0 where c1 >= ?")?;

        let cols = vec![ColumnView::from_ints(vec![99])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param])?.exec()?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows(), 4);

        let rows: Vec<Row> = stmt2.result_set()?.deserialize().try_collect()?;
        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);
        assert_eq!(rows[2].ts, 1726803358466);
        assert_eq!(rows[3].ts, 1726803359466);

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec("drop database test_1769308834")?;

        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use serde::Deserialize;
    use taos_query::common::ColumnView;
    use taos_query::stmt2::{Stmt2AsyncBindable, Stmt2BindParam};
    use taos_query::{AsyncQueryable, AsyncTBuilder};

    use super::Stmt2;
    use crate::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231450";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many([
            &format!("drop database if exists {db}"),
            &format!("create database {db} keep 36500"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare("insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![None]),
            ColumnView::from_small_ints(vec![i16::MAX]),
            ColumnView::from_ints(vec![i32::MAX]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
            ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
            ColumnView::from_unsigned_ints(vec![u32::MAX]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["hello"]),
            ColumnView::from_nchar(vec!["中文"]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param]).await?.exec().await?;
        assert_eq!(affected, 1);
        assert_eq!(stmt2.affected_rows().await, 1);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: u64,
            c1: bool,
            c2: Option<i8>,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: Option<f32>,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 1);

        let row = &rows[0];

        assert_eq!(row.ts, 1726803356466);
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, None);
        assert_eq!(row.c3, i16::MAX);
        assert_eq!(row.c4, i32::MAX);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c6, u8::MAX);
        assert_eq!(row.c7, u16::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10, Some(f32::MAX));
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411222208";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2.prepare("insert into t0 values(?, ?)").await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param]).await?.exec().await?;
        assert_eq!(affected, 4);
        assert_eq!(stmt2.affected_rows().await, 4);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);
        assert_eq!(rows[2].ts, 1726803358466);
        assert_eq!(rows[3].ts, 1726803359466);

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_query_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411281740";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
            &format!(
                "insert into t0 values(1726803356466, 1, NULL, 2, 3, 4, 5, 6, 7, 8, 1.1, 2.2, 'hello', '中文')"
            ),
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare("select * from t0 where c8 > ? and c10 > ? and c12 = ?")
            .await?;

        let cols = vec![
            ColumnView::from_ints(vec![0]),
            ColumnView::from_floats(vec![0f32]),
            ColumnView::from_varchar(vec!["hello"]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param]).await?.exec().await?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows().await, 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: bool,
            c2: Option<u8>,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: Option<f32>,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 1);

        let row = &rows[0];

        assert_eq!(row.ts, 1726803356466);
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, None);
        assert_eq!(row.c3, 2);
        assert_eq!(row.c4, 3);
        assert_eq!(row.c5, 4);
        assert_eq!(row.c6, 5);
        assert_eq!(row.c7, 6);
        assert_eq!(row.c8, 7);
        assert_eq!(row.c9, 8);
        assert_eq!(row.c10, Some(1.1));
        assert_eq!(row.c11, 2.2);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_query_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411222213";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(1726803356466, 99)",
            "insert into t0 values(1726803357466, 100)",
            "insert into t0 values(1726803358466, 101)",
            "insert into t0 values(1726803359466, 102)",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2.prepare("select * from t0 where c1 > ?").await?;

        let cols = vec![ColumnView::from_ints(vec![100])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param]).await?.exec().await?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows().await, 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].ts, 1726803358466);
        assert_eq!(rows[1].ts, 1726803359466);

        assert_eq!(rows[0].c1, 101);
        assert_eq!(rows[1].c1, 102);

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_stmt2_native() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1769309414",
            "create database test_1769309414",
            "use test_1769309414",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2.prepare("insert into t0 values(?, ?)").await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param]).await?.exec().await?;
        assert_eq!(affected, 4);
        assert_eq!(stmt2.affected_rows().await, 4);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        stmt2.prepare("select * from t0 where c1 >= ?").await?;

        let cols = vec![ColumnView::from_ints(vec![99])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        let affected = stmt2.bind(&[param]).await?.exec().await?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows().await, 4);

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);
        assert_eq!(rows[2].ts, 1726803358466);
        assert_eq!(rows[3].ts, 1726803359466);

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec("drop database test_1769309414").await?;

        Ok(())
    }
}
