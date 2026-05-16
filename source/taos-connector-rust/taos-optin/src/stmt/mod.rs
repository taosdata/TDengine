use std::ffi::{c_void, CStr, CString};
use std::sync::Arc;

use itertools::Itertools;
use taos_query::prelude::sync::{Bindable, Queryable, RawError as Error};
use taos_query::prelude::{Code, RawResult};
use taos_query::stmt::AsyncBindable;

use crate::into_c_str::IntoCStr;
use crate::raw::{ApiEntry, StmtApi};
use crate::types::*;
use crate::{err_or, RawRes, RawTaos, ResultSet};

mod bind;

#[derive(Debug)]
pub struct Stmt {
    raw: RawStmt,
}

impl Bindable<super::Taos> for Stmt {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt::from_raw_taos(&taos.raw),
        })
    }

    fn init_with_req_id(taos: &super::Taos, req_id: u64) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt::from_raw_taos_with_req_id(&taos.raw, req_id),
        })
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        self.raw.prepare(sql.as_ref())?;
        Ok(self)
    }

    fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        self.raw.set_tbname(sql.as_ref())?;
        Ok(self)
    }

    fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> RawResult<&mut Self> {
        if self.raw.is_v3() {
            let tags = tags.iter().map(TaosBindV3::from_value).collect_vec();
            self.raw.set_tags(tags.as_ptr() as _)?;
        } else {
            let tags = tags.iter().map(TaosBindV2::from_value).collect_vec();
            self.raw.set_tags(tags.as_ptr() as _)?;
        }
        Ok(self)
    }

    fn bind(&mut self, params: &[taos_query::common::ColumnView]) -> RawResult<&mut Self> {
        let params: Vec<DropMultiBind> = params.iter().map(|c| c.into()).collect_vec();
        self.raw
            .bind_param_batch(unsafe { std::mem::transmute(params.as_slice()) })?;
        Ok(self)
    }

    fn add_batch(&mut self) -> RawResult<&mut Self> {
        self.raw.add_batch()?;
        Ok(self)
    }

    fn execute(&mut self) -> RawResult<usize> {
        self.raw.execute()
    }

    fn result_set(&mut self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        self.raw.use_result()
    }

    fn affected_rows(&self) -> usize {
        self.raw.affected_rows() as _
    }
}

#[async_trait::async_trait]
impl AsyncBindable<super::Taos> for Stmt {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt::from_raw_taos(&taos.raw),
        })
    }

    async fn init_with_req_id(taos: &super::Taos, req_id: u64) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt::from_raw_taos_with_req_id(&taos.raw, req_id),
        })
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.prepare(sql)?;
        Ok(self)
    }

    async fn set_tbname(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.set_tbname(sql)?;
        Ok(self)
    }

    async fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> RawResult<&mut Self> {
        if self.raw.is_v3() {
            let tags = tags.iter().map(TaosBindV3::from_value).collect_vec();
            self.raw.set_tags(tags.as_ptr() as _)?;
        } else {
            let tags = tags.iter().map(TaosBindV2::from_value).collect_vec();
            self.raw.set_tags(tags.as_ptr() as _)?;
        }
        Ok(self)
    }

    async fn bind(&mut self, params: &[taos_query::common::ColumnView]) -> RawResult<&mut Self> {
        let params: Vec<DropMultiBind> = params.iter().map(|c| c.into()).collect_vec();
        self.raw
            .bind_param_batch(unsafe { std::mem::transmute(params.as_slice()) })?;
        Ok(self)
    }

    async fn add_batch(&mut self) -> RawResult<&mut Self> {
        self.raw.add_batch()?;
        Ok(self)
    }

    async fn execute(&mut self) -> RawResult<usize> {
        self.raw.execute()
    }

    async fn result_set(&mut self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        self.raw.use_result()
    }

    async fn affected_rows(&self) -> usize {
        self.raw.affected_rows() as _
    }
}

#[derive(Debug)]
pub struct RawStmt {
    c: Arc<ApiEntry>,
    api: StmtApi,
    ptr: *mut TAOS_STMT,
    tbname: Option<CString>,
}

unsafe impl Sync for RawStmt {}
unsafe impl Send for RawStmt {}

impl Drop for RawStmt {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl RawStmt {
    fn is_v3(&self) -> bool {
        self.c.version().starts_with('3')
    }

    #[inline(always)]
    fn ok(&self, code: impl Into<Code>) -> RawResult<()> {
        let code = code.into();
        if code.success() {
            Ok(())
        } else {
            Err(Error::from_string(self.err_as_str()))
        }
    }

    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut TAOS_STMT {
        self.ptr
    }

    #[inline]
    pub fn err_as_str(&self) -> String {
        unsafe {
            match self.api.taos_stmt_errstr {
                Some(f) => CStr::from_ptr(f(self.as_ptr()))
                    .to_string_lossy()
                    .to_string(),
                None => todo!(),
            }
        }
    }

    #[inline]
    pub fn from_raw_taos(taos: &RawTaos) -> RawStmt {
        RawStmt {
            c: taos.c.clone(),
            api: taos.c.stmt,
            ptr: unsafe { (taos.c.stmt.taos_stmt_init)(taos.as_ptr()) },
            tbname: None,
        }
    }

    #[inline]
    pub fn from_raw_taos_with_req_id(taos: &RawTaos, req_id: u64) -> RawStmt {
        RawStmt {
            c: taos.c.clone(),
            api: taos.c.stmt,
            ptr: unsafe { (taos.c.stmt.taos_stmt_init_with_reqid.unwrap())(taos.as_ptr(), req_id) },
            tbname: None,
        }
    }

    #[inline]
    pub fn close(&mut self) -> RawResult<()> {
        err_or!(self, (self.api.taos_stmt_close)(self.as_ptr()))
    }

    #[inline]
    pub fn prepare<'c, T: IntoCStr<'c>>(&mut self, sql: T) -> RawResult<()> {
        let sql = sql.into_c_str();
        tracing::trace!("prepare stmt with sql: {sql:?}");
        self.ok(unsafe {
            (self.api.taos_stmt_prepare)(self.as_ptr(), sql.as_ptr(), sql.to_bytes().len() as _)
        })
    }

    #[inline]
    pub fn set_tbname<'c, T: IntoCStr<'c>>(&mut self, name: T) -> RawResult<()> {
        let name = name.into_c_str();
        let res = self.ok(unsafe {
            match self.api.taos_stmt_set_tbname {
                Some(f) => f(self.as_ptr(), name.into_c_str().as_ptr()),
                None => todo!(),
            }
        });
        if !self.is_v3() {
            self.tbname = Some(name.into_owned());
        }
        res
    }

    #[inline]
    pub fn set_tags(&mut self, tags: *const c_void) -> RawResult<()> {
        if self.is_v3() {
            self.ok(unsafe { (self.api.taos_stmt_set_tags.unwrap())(self.as_ptr(), tags as _) })
        } else {
            self.ok(unsafe {
                match self.api.taos_stmt_set_tbname_tags {
                    Some(f) => f(
                        self.as_ptr(),
                        self.tbname.as_deref().unwrap().as_ptr(),
                        tags as _,
                    ),
                    None => todo!(),
                }
            })
        }
    }

    #[inline]
    pub fn use_result(&mut self) -> RawResult<ResultSet> {
        unsafe {
            RawRes::from_ptr(
                self.c.clone(),
                (self.api.taos_stmt_use_result)(self.as_ptr()),
            )
            .map(ResultSet::new)
        }
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe {
            match self.api.taos_stmt_affected_rows {
                Some(f) => f(self.as_ptr()),
                None => todo!(),
            }
        }
    }

    #[inline]
    pub fn execute(&self) -> RawResult<usize> {
        let cur = self.affected_rows();
        err_or!(self, (self.api.taos_stmt_execute)(self.as_ptr()))?;
        let new = self.affected_rows();
        Ok((new - cur) as usize)
    }

    #[inline]
    pub fn add_batch(&self) -> RawResult<()> {
        err_or!(self, (self.api.taos_stmt_add_batch)(self.as_ptr()))
    }

    #[inline]
    pub fn bind_param_batch(&mut self, bind: &[TaosMultiBind]) -> RawResult<()> {
        match self.api.taos_stmt_bind_param_batch {
            Some(f) => err_or!(self, f(self.as_ptr(), bind.as_ptr())),
            None => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use taos_query::util::hex::*;

    use crate::{Stmt, TaosBuilder};

    #[test]
    fn test_tbname_tags() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build()?;
        taos.query("drop database if exists stt1")?;
        taos.query("create database if not exists stt1 keep 36500")?;
        taos.query("use stt1")?;
        taos.query(
            "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 varchar(32))",
        )?;

        let mut stmt = Stmt::init(&taos)?;
        let sql = "insert into ? using st1 tags(?, ?) values(?, ?)";
        stmt.prepare(sql)?;

        let tbname = "tb1";
        stmt.set_tbname(tbname)?;

        let tags = vec![Value::Int(0), Value::VarChar(String::from("taos"))];
        stmt.set_tags(&tags)?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("bind");

        let params = vec![
            ColumnView::from_millis_timestamp(vec![1]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        println!("execute");
        stmt.execute()?;

        assert_eq!(stmt.affected_rows(), 2);
        println!("done");

        taos.query("drop database stt1")?;
        Ok(())
    }

    #[test]
    fn test_stmt_tags() -> anyhow::Result<()> {
        use taos_query::common::Timestamp;
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop database if exists test_1769583738",
            "create database test_1769583738",
            "use test_1769583738",
            "create stable s0 (ts timestamp, c1 int) tags (t1 timestamp, t2 bool, t3 tinyint, \
                t4 smallint, t5 int, t6 bigint, t7 tinyint unsigned, t8 smallint unsigned, \
                t9 int unsigned, t10 bigint unsigned, t11 float, t12 double, t13 varchar(20), \
                t14 nchar(20))",
        ])?;

        let mut stmt = Stmt::init(&taos)?;
        stmt.prepare(
            "insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values(?, ?)",
        )?;
        stmt.set_tbname("t0")?;

        let tags = vec![
            Value::Timestamp(Timestamp::new(1726803356466, Precision::Millisecond)),
            Value::Bool(true),
            Value::TinyInt(1),
            Value::SmallInt(1),
            Value::Int(1),
            Value::BigInt(1),
            Value::UTinyInt(1),
            Value::USmallInt(1),
            Value::UInt(1),
            Value::UBigInt(1),
            Value::Float(1.1),
            Value::Double(1.1),
            Value::VarChar("hello".to_string()),
            Value::NChar("hello".to_string()),
        ];
        stmt.set_tags(&tags)?;

        let params = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_ints(vec![1]),
        ];
        let affected_rows = stmt.bind(&params)?.add_batch()?.execute()?;
        assert_eq!(affected_rows, 1);

        #[derive(Debug, serde::Deserialize)]
        struct Tag {
            tag_value: String,
        }

        let tags: Vec<Tag> = taos
            .query("show tags from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(tags.len(), 14);
        assert_eq!(tags[0].tag_value, "1726803356466".to_string());
        assert_eq!(tags[1].tag_value, "true".to_string());
        assert_eq!(tags[2].tag_value, "1".to_string());
        assert_eq!(tags[3].tag_value, "1".to_string());
        assert_eq!(tags[4].tag_value, "1".to_string());
        assert_eq!(tags[5].tag_value, "1".to_string());
        assert_eq!(tags[6].tag_value, "1".to_string());
        assert_eq!(tags[7].tag_value, "1".to_string());
        assert_eq!(tags[8].tag_value, "1".to_string());
        assert_eq!(tags[9].tag_value, "1".to_string());
        assert_eq!(tags[10].tag_value, "1.10000".to_string());
        assert_eq!(tags[11].tag_value, "1.100000000".to_string());
        assert_eq!(tags[12].tag_value, "hello".to_string());
        assert_eq!(tags[13].tag_value, "hello".to_string());

        taos.exec("drop database if exists test_1769583738")?;

        Ok(())
    }

    #[test]
    fn test_tbname_tags_json() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build()?;
        taos.query("drop database if exists stt2")?;
        taos.query("create database if not exists stt2 keep 36500")?;
        taos.query("use stt2")?;
        taos.query(
            "create stable if not exists st1(ts timestamp, v int) tags(jt json)", // "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;

        let mut stmt = Stmt::init(&taos)?;
        let sql = "insert into ? using st1 tags(?) values(?, ?)";
        stmt.prepare(sql)?;

        let tags = vec![Value::Json(serde_json::from_str(r#"{"name":"value"}"#)?)];
        println!("tags: {tags:#?}");
        let tbname = "tb1";
        stmt.set_tbname(tbname)?;

        stmt.set_tags(&tags)?;
        println!("bind");

        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        assert!(stmt.affected_rows() == 1);

        stmt.prepare("insert into tb1 values(?,?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![1]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        let mut res = taos.query("select * from st1")?;

        if let Some(raw) = res.fetch_raw_block()? {
            assert_eq!(raw.nrows(), 2);
        } else {
            panic!("no data retrieved");
        }

        taos.query("drop database stt2")?;
        Ok(())
    }

    #[test]
    fn test_bindable() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop database if exists test_bindable",
            "create database test_bindable keep 36500",
            "use test_bindable",
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100), c14 varbinary(50), c15 geometry(50))",
        ])?;
        let mut stmt = Stmt::init(&taos)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
            ColumnView::from_big_ints(vec![0]),
            ColumnView::from_unsigned_tiny_ints(vec![0]),
            ColumnView::from_unsigned_small_ints(vec![0]),
            ColumnView::from_unsigned_ints(vec![0]),
            ColumnView::from_unsigned_big_ints(vec![0]),
            ColumnView::from_floats(vec![0.0]),
            ColumnView::from_doubles(vec![0.]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
            ColumnView::from_bytes(vec![hex_string_to_bytes("123456").to_vec()]),
            ColumnView::from_geobytes(vec![hex_string_to_bytes(
                "0101000000000000000000F03F0000000000000040",
            )
            .to_vec()]),
        ];
        let rows = stmt.bind(&params)?.add_batch()?.execute()?;
        assert_eq!(rows, 1);

        let rows: Vec<(
            String,
            bool,
            i8,
            i16,
            i32,
            i64,
            u8,
            u16,
            u32,
            u64,
            f32,
            f64,
            String,
            String,
            Bytes,
            Bytes,
        )> = taos
            .query("select * from tb1")?
            .deserialize()
            .try_collect()?;
        let row = &rows[0];
        assert_eq!(row.12, "ABC");
        assert_eq!(row.13, "涛思数据");
        assert_eq!(row.14, hex_string_to_bytes("123456"));
        assert_eq!(
            row.15,
            hex_string_to_bytes("0101000000000000000000F03F0000000000000040")
        );
        taos.query("drop database test_bindable")?;

        Ok(())
    }

    #[test]
    fn test_stmt_with_req_id() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop database if exists test_db_stmt",
            "create database test_db_stmt keep 36500",
            "use test_db_stmt",
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;
        let req_id = 1000;
        let mut stmt = Stmt::init_with_req_id(&taos, req_id)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
            ColumnView::from_big_ints(vec![0]),
            ColumnView::from_unsigned_tiny_ints(vec![0]),
            ColumnView::from_unsigned_small_ints(vec![0]),
            ColumnView::from_unsigned_ints(vec![0]),
            ColumnView::from_unsigned_big_ints(vec![0]),
            ColumnView::from_floats(vec![0.0]),
            ColumnView::from_doubles(vec![0.]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
        ];
        let rows = stmt.bind(&params)?.add_batch()?.execute()?;
        assert_eq!(rows, 1);

        let rows: Vec<(
            String,
            bool,
            i8,
            i16,
            i32,
            i64,
            u8,
            u16,
            u32,
            u64,
            f32,
            f64,
            String,
            String,
        )> = taos
            .query("select * from tb1")?
            .deserialize()
            .try_collect()?;
        let row = &rows[0];
        assert_eq!(row.12, "ABC");
        assert_eq!(row.13, "涛思数据");
        taos.query("drop database test_db_stmt")?;

        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use crate::prelude::*;

    #[tokio::test]
    async fn test_stmt_with_req_id() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop database if exists test_db_stmt_async",
            "create database test_db_stmt_async keep 36500",
            "use test_db_stmt_async",
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])
        .await?;
        let req_id = 1000;
        let mut stmt = Stmt::init_with_req_id(&taos, req_id).await?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
            ColumnView::from_big_ints(vec![0]),
            ColumnView::from_unsigned_tiny_ints(vec![0]),
            ColumnView::from_unsigned_small_ints(vec![0]),
            ColumnView::from_unsigned_ints(vec![0]),
            ColumnView::from_unsigned_big_ints(vec![0]),
            ColumnView::from_floats(vec![0.0]),
            ColumnView::from_doubles(vec![0.]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
        ];
        let rows = stmt
            .bind(&params)
            .await?
            .add_batch()
            .await?
            .execute()
            .await?;
        assert_eq!(rows, 1);

        let rows: Vec<(
            String,
            bool,
            i8,
            i16,
            i32,
            i64,
            u8,
            u16,
            u32,
            u64,
            f32,
            f64,
            String,
            String,
        )> = taos
            .query("select * from tb1")
            .await?
            .deserialize()
            .try_collect()
            .await?;
        let row = &rows[0];
        assert_eq!(row.12, "ABC");
        assert_eq!(row.13, "涛思数据");
        taos.query("drop database test_db_stmt_async").await?;

        Ok(())
    }
}
