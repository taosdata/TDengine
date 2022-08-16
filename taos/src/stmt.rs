use taos_query::stmt::Bindable;
use taos_sys::{Stmt as NativeStmt, Value};
use taos_ws::Stmt as WsStmt;
enum StmtInner {
    Native(NativeStmt),
    Ws(WsStmt),
}

pub struct Stmt(StmtInner);

impl Bindable<super::Taos> for Stmt {
    type Error = super::Error;

    fn init(taos: &super::Taos) -> Result<Self, Self::Error> {
        match &taos.0 {
            crate::TaosInner::Native(taos) => NativeStmt::init(taos)
                .map(StmtInner::Native)
                .map(Stmt)
                .map_err(Into::into),
            crate::TaosInner::Ws(taos) => WsStmt::init(taos)
                .map(StmtInner::Ws)
                .map(Stmt)
                .map_err(Into::into),
        }
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> Result<&mut Self, Self::Error> {
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

    fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> Result<&mut Self, Self::Error> {
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

    fn set_tags(&mut self, tags: &[Value]) -> Result<&mut Self, Self::Error> {
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

    fn bind(&mut self, params: &[taos_sys::ColumnView]) -> Result<&mut Self, Self::Error> {
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

    fn add_batch(&mut self) -> Result<&mut Self, Self::Error> {
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

    fn execute(&mut self) -> Result<usize, Self::Error> {
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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[test]
    fn test_bindable_sync() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();
        use crate::sync::*;
        let taos = TaosBuilder::from_dsn("taos+ws://localhost:6041")?.build()?;
        taos.exec_many([
            "drop database if exists test_bindable",
            "create database test_bindable keep 36500",
            "use test_bindable",
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;
        let mut stmt = Stmt::init(&taos)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![164000000000]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
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
            c10: f32,
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
        assert_eq!(row.c10, f32::MAX);
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "ABC");
        assert_eq!(row.c13, "涛思数据");

        Ok(())
    }
    #[tokio::test]
    async fn test_bindable() -> anyhow::Result<()> {
        use crate::*;
        let taos = TaosBuilder::from_dsn("taos://")?.build()?;
        taos.exec_many([
            "drop database if exists test_bindable",
            "create database test_bindable keep 36500",
            "use test_bindable",
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])
        .await?;
        let mut stmt = Stmt::init(&taos)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
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
            .await?;
        let row = &rows[0];
        dbg!(&row);
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "ABC");
        assert_eq!(row.c13, "涛思数据");

        Ok(())
    }
}
