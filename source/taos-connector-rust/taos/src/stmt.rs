use taos_query::prelude::{ColumnView, Value};
use taos_query::RawResult;
use taos_ws::Stmt as WsStmt;

use crate::sys::Stmt as NativeStmt;

#[derive(Debug)]
enum StmtInner {
    Native(NativeStmt),
    Ws(WsStmt),
}

#[derive(Debug)]
pub struct Stmt(StmtInner);

impl taos_query::stmt::Bindable<super::Taos> for Stmt {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        match &taos.0 {
            crate::TaosInner::Native(taos) => {
                NativeStmt::init(taos).map(StmtInner::Native).map(Stmt)
            }
            crate::TaosInner::Ws(taos) => WsStmt::init(taos).map(StmtInner::Ws).map(Stmt),
        }
    }

    fn init_with_req_id(taos: &super::Taos, req_id: u64) -> RawResult<Self> {
        match &taos.0 {
            crate::TaosInner::Native(taos) => NativeStmt::init_with_req_id(taos, req_id)
                .map(StmtInner::Native)
                .map(Stmt),
            crate::TaosInner::Ws(taos) => WsStmt::init_with_req_id(taos, req_id)
                .map(StmtInner::Ws)
                .map(Stmt),
        }
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.prepare(sql)?;
            }
            StmtInner::Ws(stmt) => {
                stmt.prepare(sql)?;
            }
        }
        Ok(self)
    }

    fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.set_tbname(name)?;
            }
            StmtInner::Ws(stmt) => {
                stmt.set_tbname(name)?;
            }
        }
        Ok(self)
    }

    fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.set_tags(tags)?;
            }
            StmtInner::Ws(stmt) => {
                stmt.set_tags(tags)?;
            }
        }
        Ok(self)
    }

    fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.bind(params)?;
            }
            StmtInner::Ws(stmt) => {
                stmt.bind(params)?;
            }
        }
        Ok(self)
    }

    fn add_batch(&mut self) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.add_batch()?;
            }
            StmtInner::Ws(stmt) => {
                stmt.add_batch()?;
            }
        }
        Ok(self)
    }

    fn execute(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            StmtInner::Native(stmt) => Ok(stmt.execute()?),
            StmtInner::Ws(stmt) => Ok(stmt.execute()?),
        }
    }

    fn affected_rows(&self) -> usize {
        match &self.0 {
            StmtInner::Native(stmt) => stmt.affected_rows(),
            StmtInner::Ws(stmt) => stmt.affected_rows(),
        }
    }
}

#[async_trait::async_trait]
impl taos_query::prelude::AsyncBindable<super::Taos> for Stmt {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        match &taos.0 {
            crate::TaosInner::Native(taos) => NativeStmt::init(taos)
                .await
                .map(StmtInner::Native)
                .map(Stmt),
            crate::TaosInner::Ws(taos) => WsStmt::init(taos).await.map(StmtInner::Ws).map(Stmt),
        }
    }

    async fn init_with_req_id(taos: &super::Taos, req_id: u64) -> RawResult<Self> {
        match &taos.0 {
            crate::TaosInner::Native(taos) => NativeStmt::init_with_req_id(taos, req_id)
                .await
                .map(StmtInner::Native)
                .map(Stmt),
            crate::TaosInner::Ws(taos) => WsStmt::init_with_req_id(taos, req_id)
                .await
                .map(StmtInner::Ws)
                .map(Stmt),
        }
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.prepare(sql).await?;
            }
            StmtInner::Ws(stmt) => {
                stmt.prepare(sql).await?;
            }
        }
        Ok(self)
    }

    async fn set_tbname(&mut self, name: &str) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.set_tbname(name).await?;
            }
            StmtInner::Ws(stmt) => {
                stmt.set_tbname(name).await?;
            }
        }
        Ok(self)
    }

    async fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.set_tags(tags).await?;
            }
            StmtInner::Ws(stmt) => {
                stmt.set_tags(tags).await?;
            }
        }
        Ok(self)
    }

    async fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.bind(params).await?;
            }
            StmtInner::Ws(stmt) => {
                stmt.bind(params).await?;
            }
        }
        Ok(self)
    }

    async fn add_batch(&mut self) -> RawResult<&mut Self> {
        match &mut self.0 {
            StmtInner::Native(stmt) => {
                stmt.add_batch().await?;
            }
            StmtInner::Ws(stmt) => {
                stmt.add_batch().await?;
            }
        }
        Ok(self)
    }

    async fn execute(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            StmtInner::Native(stmt) => Ok(stmt.execute().await?),
            StmtInner::Ws(stmt) => Ok(stmt.execute().await?),
        }
    }

    async fn affected_rows(&self) -> usize {
        match &self.0 {
            StmtInner::Native(stmt) => stmt.affected_rows().await,
            StmtInner::Ws(stmt) => stmt.affected_rows().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bytes::Bytes;
    use serde::Deserialize;
    use taos_query::util::hex::*;
    use taos_query::RawResult;

    #[test]
    fn test_bindable_sync() -> RawResult<()> {
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        // pretty_env_logger::init();
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;

        use crate::sync::*;
        let taos = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "taos_test_bindable";

        taos.exec_many([
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} keep 36500").as_str(),
            format!("use {db}").as_str(),
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;
        let mut stmt = Stmt::init(&taos)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![164000000000]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![i8::MAX]),
            ColumnView::from_small_ints(vec![i16::MAX]),
            ColumnView::from_ints(vec![i32::MAX]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
            ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
            ColumnView::from_unsigned_ints(vec![u32::MAX]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
        ];
        let rows = stmt.bind(&params)?.add_batch()?.execute()?;
        assert_eq!(rows, 1);

        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            ts: String,
            c1: bool,
            c2: i8,
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
            .query("select * from tb1")?
            .deserialize()
            .try_collect()?;
        let row = &rows[0];
        dbg!(&row);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10.unwrap(), f32::MAX);
        // assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "ABC");
        assert_eq!(row.c13, "涛思数据");

        taos.exec(format!("drop database {db}").as_str())?;

        Ok(())
    }

    #[test]
    fn test_ws_stmt_with_req_id_sync() -> RawResult<()> {
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        // pretty_env_logger::init();
        let dsn = std::env::var("TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;

        use crate::sync::*;
        let taos = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_ws_stmt_with_req_id_sync";

        taos.exec_many([
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} keep 36500").as_str(),
            format!("use {db}").as_str(),
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;
        let req_id = 1001;
        let mut stmt = Stmt::init_with_req_id(&taos, req_id)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![164000000000]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![i8::MAX]),
            ColumnView::from_small_ints(vec![i16::MAX]),
            ColumnView::from_ints(vec![i32::MAX]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
            ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
            ColumnView::from_unsigned_ints(vec![u32::MAX]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
        ];
        let rows = stmt.bind(&params)?.add_batch()?.execute()?;
        assert_eq!(rows, 1);

        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            ts: String,
            c1: bool,
            c2: i8,
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
            .query("select * from tb1")?
            .deserialize()
            .try_collect()?;
        let row = &rows[0];
        dbg!(&row);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10.unwrap(), f32::MAX);
        // assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "ABC");
        assert_eq!(row.c13, "涛思数据");

        taos.exec(format!("drop database {db}").as_str())?;

        Ok(())
    }

    #[tokio::test]
    async fn test_bindable() -> RawResult<()> {
        use crate::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_bindable2";

        taos.exec_many([
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} keep 36500").as_str(),
            format!("use {db}").as_str(),
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])
        .await?;
        let mut stmt = Stmt::init(&taos).await.unwrap();
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![0]),
            ColumnView::from_unsigned_small_ints(vec![0]),
            ColumnView::from_unsigned_ints(vec![0]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
        ];
        let rows = stmt
            .bind(&params)
            .await
            .unwrap()
            .add_batch()
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(rows, 1);

        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            ts: String,
            c1: bool,
            c2: i8,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: f32,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = taos
            .query("select * from tb1")
            .await?
            .deserialize()
            .try_collect()
            .await
            .unwrap();
        let row = &rows[0];
        dbg!(&row);
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "ABC");
        assert_eq!(row.c13, "涛思数据");

        taos.exec(format!("drop database {db}").as_str())
            .await
            .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_ws_stmt_with_req_id() -> RawResult<()> {
        use crate::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_ws_stmt_with_req_id_async";

        taos.exec_many([
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} keep 36500").as_str(),
            format!("use {db}").as_str(),
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100), c14 varbinary(50), c15 geometry(50))",
        ])
        .await?;
        let req_id = 1001;
        let mut stmt = Stmt::init_with_req_id(&taos, req_id).await.unwrap();
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![0]),
            ColumnView::from_unsigned_small_ints(vec![0]),
            ColumnView::from_unsigned_ints(vec![0]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
            ColumnView::from_bytes(vec![hex_string_to_bytes("123456").to_vec()]),
            ColumnView::from_geobytes(vec![hex_string_to_bytes(
                "0101000000000000000000F03F0000000000000040",
            )
            .to_vec()]),
        ];
        let rows = stmt
            .bind(&params)
            .await
            .unwrap()
            .add_batch()
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(rows, 1);

        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            ts: String,
            c1: bool,
            c2: i8,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: f32,
            c11: f64,
            c12: String,
            c13: String,
            c14: Bytes,
            c15: Bytes,
        }

        let rows: Vec<Row> = taos
            .query("select * from tb1")
            .await?
            .deserialize()
            .try_collect()
            .await
            .unwrap();
        let row = &rows[0];
        dbg!(&row);
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "ABC");
        assert_eq!(row.c13, "涛思数据");
        assert_eq!(row.c14, hex_string_to_bytes("123456"));
        assert_eq!(
            row.c15,
            hex_string_to_bytes("0101000000000000000000F03F0000000000000040")
        );

        taos.exec(format!("drop database {db}").as_str())
            .await
            .unwrap();

        Ok(())
    }
}
