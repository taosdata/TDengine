use std::{
    cell::UnsafeCell,
    ffi::{c_int, c_void},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use taos_query::{stmt2::*, util::generate_req_id, AsyncQueryable, Queryable, RawError, RawResult};

use crate::{
    into_c_str::IntoCStr,
    raw::{ApiEntry, RawRes, RawTaos, Stmt2Api},
    types::{TaosStmt2Option, TAOS_RES, TAOS_STMT2},
    ResultSet,
};

mod bind;

#[derive(Debug)]
pub struct Stmt2 {
    raw: RawStmt2,
}

impl Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt2::from_raw_taos(&taos.raw)?,
        })
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.prepare(sql)?;
        Ok(self)
    }

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        self.raw.bind(params)?;
        Ok(self)
    }

    fn exec(&mut self) -> RawResult<usize> {
        taos_query::block_in_place_or_global(self.raw.exec())
    }

    fn affected_rows(&self) -> usize {
        self.raw.affected_rows()
    }

    fn result_set(&self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        self.raw.result_set()
    }
}

#[async_trait::async_trait]
impl Stmt2AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt2::from_raw_taos(&taos.raw)?,
        })
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.prepare(sql)?;
        Ok(self)
    }

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        self.raw.bind(params)?;
        Ok(self)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        self.raw.exec().await
    }

    async fn affected_rows(&self) -> usize {
        self.raw.affected_rows()
    }

    async fn result_set(&self) -> RawResult<<super::Taos as AsyncQueryable>::AsyncResultSet> {
        self.raw.result_set()
    }
}

#[derive(Debug)]
pub struct RawStmt2 {
    api: Stmt2Api,
    ptr: *mut TAOS_STMT2,
    res: UnsafeCell<Option<RawRes>>,
    state: Rc<UnsafeCell<Stmt2ExecState>>,
    affected_rows: usize,
}

unsafe impl Send for RawStmt2 {}
unsafe impl Sync for RawStmt2 {}

impl Drop for RawStmt2 {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            tracing::error!("Failed to close Stmt2: {err}");
        }
    }
}

impl RawStmt2 {
    fn from_raw_taos(taos: &RawTaos) -> RawResult<Self> {
        let state = Rc::new(UnsafeCell::new(Stmt2ExecState::new(taos.c.clone())));
        let userdata = Rc::as_ptr(&state) as *mut c_void;
        let mut option = TaosStmt2Option {
            reqid: generate_req_id() as _,
            single_stb_insert: true,
            single_table_bind_once: false,
            async_exec_fn: stmt2_exec_cb,
            userdata,
        };

        let stmt2 = taos.c.stmt2.init(taos.as_ptr(), &mut option)?;
        let s = unsafe { &mut *state.get() };
        s.stmt2 = Some(stmt2);
        Ok(RawStmt2 {
            api: taos.c.stmt2,
            ptr: stmt2,
            res: UnsafeCell::new(None),
            state,
            affected_rows: 0,
        })
    }

    fn prepare<'c, T: IntoCStr<'c>>(&self, sql: T) -> RawResult<()> {
        let sql = sql.into_c_str();
        tracing::trace!(?sql, "Stmt2 prepare");
        self.api
            .prepare(self.as_ptr(), sql.as_ptr(), sql.to_bytes().len() as _)
    }

    fn bind(&self, params: &[Stmt2BindParam]) -> RawResult<()> {
        tracing::trace!(?params, "Stmt2 bind");
        let bindv = bind::build_bindv(params)?;
        self.api.bind(self.as_ptr(), bindv.as_ptr() as _, -1)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        let state = unsafe { &mut *self.state.get() };
        state.clear();

        let fut = Stmt2ExecFuture {
            state: self.state.clone(),
        };
        let (affected_rows, res) = fut.await?;
        unsafe { *self.res.get() = Some(res) };
        self.affected_rows += affected_rows;
        Ok(affected_rows)
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    fn close(&self) -> RawResult<()> {
        self.api.close(self.as_ptr())
    }

    fn result_set(&self) -> RawResult<ResultSet> {
        let slot = unsafe { &mut *self.res.get() };
        match slot.take() {
            Some(res) => Ok(ResultSet::new(res)),
            None => Err(RawError::from_string("No result available from statement")),
        }
    }

    const fn as_ptr(&self) -> *mut TAOS_STMT2 {
        self.ptr
    }
}

struct Stmt2ExecState {
    api: Arc<ApiEntry>,
    stmt2: Option<*mut TAOS_STMT2>,
    result: Option<Result<(usize, RawRes), RawError>>,
    waiting: bool,
    start_time: Option<Instant>,
    callback_cost: Option<Duration>,
    waker: Option<Waker>,
}

impl Stmt2ExecState {
    const fn new(api: Arc<ApiEntry>) -> Self {
        Self {
            api,
            stmt2: None,
            result: None,
            waiting: false,
            start_time: None,
            callback_cost: None,
            waker: None,
        }
    }

    fn clear(&mut self) {
        self.result = None;
        self.waiting = false;
        self.start_time = None;
        self.callback_cost = None;
        self.waker = None;
    }
}

struct Stmt2ExecFuture {
    state: Rc<UnsafeCell<Stmt2ExecState>>,
}

unsafe impl Send for Stmt2ExecFuture {}

impl Future for Stmt2ExecFuture {
    type Output = Result<(usize, RawRes), RawError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *self.state.get() };
        if let Some(result) = state.result.take() {
            tracing::trace!(
                elapsed = ?state.start_time.map(|t| t.elapsed()).unwrap_or_default()
                    .saturating_sub(state.callback_cost.unwrap_or_default()),
                "Stmt2 exec future woken after callback",
            );
            return Poll::Ready(result);
        }

        if state.waiting {
            tracing::trace!("It's waked but still waiting for stmt2 exec callback");
            return Poll::Pending;
        }

        state.waiting = true;
        state.start_time = Some(Instant::now());
        state.waker = Some(cx.waker().clone());

        match state.stmt2 {
            Some(stmt) => {
                if let Err(err) = state.api.stmt2.exec(stmt) {
                    tracing::error!(%err, stmt2 = ?state.stmt2, "Failed to start stmt2 exec async");
                    return Poll::Ready(Err(err));
                }
            }
            None => {
                let err = RawError::from_string("Stmt2 exec async failed: stmt2 pointer missing");
                tracing::error!(%err, stmt2 = ?state.stmt2, "Failed to start stmt2 exec async");
                return Poll::Ready(Err(err));
            }
        }

        Poll::Pending
    }
}

unsafe extern "C" fn stmt2_exec_cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
    if res.is_null() && code == 0 {
        unreachable!("Stmt2 exec callback should be ok or error");
    }

    if param.is_null() {
        tracing::error!("Stmt2 exec callback param should not be null");
        return;
    }

    let cell = &*(param as *const UnsafeCell<Stmt2ExecState>);
    let state = unsafe { &mut *cell.get() };
    let elapsed = state.start_time.map(|t| t.elapsed()).unwrap_or_default();
    tracing::trace!(elapsed = ?elapsed, "Received stmt2 exec callback");
    state.callback_cost.replace(elapsed);

    let result = if code < 0 {
        let err = if let Some(stmt) = state.stmt2 {
            match state.api.stmt2.err_as_str(stmt) {
                Ok(msg) => RawError::new(code, msg),
                Err(err) => err,
            }
        } else {
            RawError::new(code, "Stmt2 exec callback failed: stmt2 pointer missing")
        };
        Err(err)
    } else {
        debug_assert!(!res.is_null());
        assert_ne!(res as usize, 1, "res should not be 1");
        let raw_res = RawRes::from_ptr_unowned(state.api.clone(), res);
        let affected_rows = raw_res.affected_rows() as usize;
        Ok((affected_rows, raw_res))
    };

    state.result.replace(result);
    state.waiting = false;
    if let Some(waker) = state.waker.take() {
        waker.wake();
    }
}

#[cfg(feature = "test-new-feat")]
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde::Deserialize;
    use taos_query::common::{decimal::Decimal, Timestamp};

    use crate::prelude::*;

    #[tokio::test]
    async fn test_stmt2_table() -> RawResult<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1768466795",
            "create database test_1768466795",
            "use test_1768466795",
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                c9 bigint unsigned, c10 float, c11 double, c12 varchar(20), c13 nchar(20), \
                c14 varbinary(20), c15 geometry(50), c16 blob, c17 decimal(10, 2), \
                c18 decimal(20, 5))",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare(
                "insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;

        let tss = vec![1726803356466, 1726803357466, 1726803358466];
        let c1s = vec![Some(true), None, Some(false)];
        let c2s = vec![Some(1), None, Some(-1)];
        let c3s = vec![Some(1), None, Some(-1)];
        let c4s = vec![Some(1), None, Some(-1)];
        let c5s = vec![Some(1), None, Some(-1)];
        let c6s = vec![Some(1), None, Some(1)];
        let c7s = vec![Some(1), None, Some(1)];
        let c8s = vec![Some(1), None, Some(1)];
        let c9s = vec![Some(1), None, Some(1)];
        let c10s = vec![Some(1.1), None, Some(-1.1)];
        let c11s = vec![Some(1.11), None, Some(-1.11)];
        let c12s = vec![Some("hello"), None, Some("world")];
        let c13s = vec![Some("hello"), None, Some("中文")];
        let c14s = vec![Some(&b"hello"[..]), None, Some(&b"\x00\x01\x02"[..])];
        let c15s = vec![
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
            None,
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
        ];
        let c16s = vec![Some(&b"hello blob"[..]), None, Some(&b"world blob"[..])];
        let c17s = vec![Some(1234567), None, Some(-1234567)];
        let c18s = vec![Some(123456789012345), None, Some(-123456789012345)];

        let cols = vec![
            ColumnView::from_millis_timestamp(tss.clone()),
            ColumnView::from_bools(c1s.clone()),
            ColumnView::from_tiny_ints(c2s.clone()),
            ColumnView::from_small_ints(c3s.clone()),
            ColumnView::from_ints(c4s.clone()),
            ColumnView::from_big_ints(c5s.clone()),
            ColumnView::from_unsigned_tiny_ints(c6s.clone()),
            ColumnView::from_unsigned_small_ints(c7s.clone()),
            ColumnView::from_unsigned_ints(c8s.clone()),
            ColumnView::from_unsigned_big_ints(c9s.clone()),
            ColumnView::from_floats(c10s.clone()),
            ColumnView::from_doubles(c11s.clone()),
            ColumnView::from_varchar::<&str, _, _, _>(c12s.clone()),
            ColumnView::from_nchar::<&str, _, _, _>(c13s.clone()),
            ColumnView::from_bytes::<&[u8], _, _, _>(c14s.clone()),
            ColumnView::from_geobytes::<&[u8], _, _, _>(c15s.clone()),
            ColumnView::from_blob_bytes::<&[u8], _, _, _>(c16s.clone()),
            ColumnView::from_decimal64(c17s.clone(), 10, 2),
            ColumnView::from_decimal(c18s.clone(), 20, 5),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 3);
        assert_eq!(stmt2.affected_rows().await, 3);

        stmt2.prepare("select * from t0 where ts >= ?").await?;

        let cols = vec![ColumnView::from_millis_timestamp(vec![1726803356466])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows().await, 3);

        #[derive(Debug, Deserialize)]
        struct Row {
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
            c14: Option<Vec<u8>>,
            c15: Option<Vec<u8>>,
            c16: Option<Vec<u8>>,
            c17: Option<String>,
            c18: Option<String>,
        }

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 3);

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.ts, tss[i]);
            assert_eq!(row.c1, c1s[i]);
            assert_eq!(row.c2, c2s[i]);
            assert_eq!(row.c3, c3s[i]);
            assert_eq!(row.c4, c4s[i]);
            assert_eq!(row.c5, c5s[i]);
            assert_eq!(row.c6, c6s[i]);
            assert_eq!(row.c7, c7s[i]);
            assert_eq!(row.c8, c8s[i]);
            assert_eq!(row.c9, c9s[i]);
            assert_eq!(row.c10, c10s[i]);
            assert_eq!(row.c11, c11s[i]);
            assert_eq!(row.c12.as_deref(), c12s[i]);
            assert_eq!(row.c13.as_deref(), c13s[i]);
            assert_eq!(row.c14.as_deref(), c14s[i]);
            assert_eq!(row.c15.as_deref(), c15s[i]);
            assert_eq!(row.c16.as_deref(), c16s[i]);
            assert_eq!(row.c17, c17s[i].map(|v| Decimal::new(v, 10, 2).to_string()));
            assert_eq!(row.c18, c18s[i].map(|v| Decimal::new(v, 20, 5).to_string()));
        }

        taos.exec("drop database if exists test_1768466795").await?;

        Ok(())
    }

    fn push_tag<T, F>(tag: &mut Vec<Value>, v: Option<T>, ty: Ty, f: F)
    where
        F: FnOnce(T) -> Value,
    {
        match v {
            Some(x) => tag.push(f(x)),
            None => tag.push(Value::Null(ty)),
        }
    }

    #[tokio::test]
    async fn test_stmt2_stable() -> RawResult<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1769256745",
            "create database test_1769256745",
            "use test_1769256745",
            "create table s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                c9 bigint unsigned, c10 float, c11 double, c12 varchar(20), c13 nchar(20), \
                c14 varbinary(20), c15 geometry(50), c16 blob, c17 decimal(10, 2), \
                c18 decimal(20, 5)) \
                tags (t1 timestamp, t2 bool, t3 tinyint, t4 smallint, t5 int, \
                t6 bigint, t7 tinyint unsigned, t8 smallint unsigned, t9 int unsigned, \
                t10 bigint unsigned, t11 float, t12 double, t13 varchar(20), t14 nchar(20), \
                t15 varbinary(20), t16 geometry(50))",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare(
                "insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;

        let tbnames = vec!["t0", "t1", "t2"];

        let t1s = vec![Some(1726803356466), None, Some(1726803358466)];
        let t2s = vec![Some(true), None, Some(false)];
        let t3s = vec![Some(1), None, Some(-1)];
        let t4s = vec![Some(1), None, Some(-1)];
        let t5s = vec![Some(1), None, Some(-1)];
        let t6s = vec![Some(1), None, Some(-1)];
        let t7s = vec![Some(1), None, Some(1)];
        let t8s = vec![Some(1), None, Some(1)];
        let t9s = vec![Some(1), None, Some(1)];
        let t10s = vec![Some(1), None, Some(1)];
        let t11s = vec![Some(1.1), None, Some(-1.1)];
        let t12s = vec![Some(1.11), None, Some(-1.11)];
        let t13s = vec![Some("hello"), None, Some("world")];
        let t14s = vec![Some("hello"), None, Some("中文")];
        let t15s = vec![Some(&b"hello"[..]), None, Some(&b"\x00\x01\x02"[..])];
        let t16s = vec![
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
            None,
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
        ];

        let mut tags = Vec::with_capacity(t1s.len());
        for i in 0..t1s.len() {
            let mut tag = Vec::with_capacity(17);
            push_tag(&mut tag, t1s[i], Ty::Timestamp, |v| {
                Value::Timestamp(Timestamp::new(v, Precision::Millisecond))
            });
            push_tag(&mut tag, t2s[i], Ty::Bool, Value::Bool);
            push_tag(&mut tag, t3s[i], Ty::TinyInt, Value::TinyInt);
            push_tag(&mut tag, t4s[i], Ty::SmallInt, Value::SmallInt);
            push_tag(&mut tag, t5s[i], Ty::Int, Value::Int);
            push_tag(&mut tag, t6s[i], Ty::BigInt, Value::BigInt);
            push_tag(&mut tag, t7s[i], Ty::UTinyInt, Value::UTinyInt);
            push_tag(&mut tag, t8s[i], Ty::USmallInt, Value::USmallInt);
            push_tag(&mut tag, t9s[i], Ty::UInt, Value::UInt);
            push_tag(&mut tag, t10s[i], Ty::UBigInt, Value::UBigInt);
            push_tag(&mut tag, t11s[i], Ty::Float, Value::Float);
            push_tag(&mut tag, t12s[i], Ty::Double, Value::Double);
            push_tag(&mut tag, t13s[i], Ty::VarChar, |v| {
                Value::VarChar(v.to_string())
            });
            push_tag(&mut tag, t14s[i], Ty::NChar, |v| {
                Value::NChar(v.to_string())
            });
            push_tag(&mut tag, t15s[i], Ty::VarBinary, |v| {
                Value::VarBinary(Bytes::copy_from_slice(v))
            });
            push_tag(&mut tag, t16s[i], Ty::Geometry, |v| {
                Value::Geometry(Bytes::copy_from_slice(v))
            });
            tags.push(tag);
        }

        let tss = vec![1726803356466, 1726803357466, 1726803358466];
        let c1s = vec![Some(true), None, Some(false)];
        let c2s = vec![Some(1), None, Some(-1)];
        let c3s = vec![Some(1), None, Some(-1)];
        let c4s = vec![Some(1), None, Some(-1)];
        let c5s = vec![Some(1), None, Some(-1)];
        let c6s = vec![Some(1), None, Some(1)];
        let c7s = vec![Some(1), None, Some(1)];
        let c8s = vec![Some(1), None, Some(1)];
        let c9s = vec![Some(1), None, Some(1)];
        let c10s = vec![Some(1.1), None, Some(-1.1)];
        let c11s = vec![Some(1.11), None, Some(-1.11)];
        let c12s = vec![Some("hello"), None, Some("world")];
        let c13s = vec![Some("hello"), None, Some("中文")];
        let c14s = vec![Some(&b"hello"[..]), None, Some(&b"\x00\x01\x02"[..])];
        let c15s = vec![
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
            None,
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
        ];
        let c16s = vec![Some(&b"hello blob"[..]), None, Some(&b"world blob"[..])];
        let c17s = vec![Some(1234567), None, Some(-1234567)];
        let c18s = vec![Some(123456789012345), None, Some(-123456789012345)];

        let cols = vec![
            ColumnView::from_millis_timestamp(tss.clone()),
            ColumnView::from_bools(c1s.clone()),
            ColumnView::from_tiny_ints(c2s.clone()),
            ColumnView::from_small_ints(c3s.clone()),
            ColumnView::from_ints(c4s.clone()),
            ColumnView::from_big_ints(c5s.clone()),
            ColumnView::from_unsigned_tiny_ints(c6s.clone()),
            ColumnView::from_unsigned_small_ints(c7s.clone()),
            ColumnView::from_unsigned_ints(c8s.clone()),
            ColumnView::from_unsigned_big_ints(c9s.clone()),
            ColumnView::from_floats(c10s.clone()),
            ColumnView::from_doubles(c11s.clone()),
            ColumnView::from_varchar::<&str, _, _, _>(c12s.clone()),
            ColumnView::from_nchar::<&str, _, _, _>(c13s.clone()),
            ColumnView::from_bytes::<&[u8], _, _, _>(c14s.clone()),
            ColumnView::from_geobytes::<&[u8], _, _, _>(c15s.clone()),
            ColumnView::from_blob_bytes::<&[u8], _, _, _>(c16s.clone()),
            ColumnView::from_decimal64(c17s.clone(), 10, 2),
            ColumnView::from_decimal(c18s.clone(), 20, 5),
        ];

        let mut params = Vec::with_capacity(tbnames.len());
        for i in 0..tbnames.len() {
            params.push(Stmt2BindParam::new(
                Some(tbnames[i].to_string()),
                Some(tags.remove(0)),
                Some(cols.clone()),
            ));
        }
        stmt2.bind(&params).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 9);

        #[derive(Debug, Deserialize)]
        struct Tag {
            tag_value: Option<String>,
        }

        #[derive(Debug, Deserialize)]
        struct Row {
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
            c14: Option<Vec<u8>>,
            c15: Option<Vec<u8>>,
            c16: Option<Vec<u8>>,
            c17: Option<String>,
            c18: Option<String>,
        }

        let t11s = vec![Some("1.10000"), None, Some("-1.10000")];
        let t12s = vec![Some("1.110000000"), None, Some("-1.110000000")];
        let t15s = vec![Some("\\x68656C6C6F"), None, Some("\\x000102")];
        let t16s = vec![
            Some("POINT (1.000000 1.000000)"),
            None,
            Some("POINT (1.000000 1.000000)"),
        ];

        for (i, &tbname) in tbnames.iter().enumerate() {
            let tags: Vec<Tag> = taos
                .query(format!("show tags from {tbname}"))
                .await?
                .deserialize()
                .try_collect()
                .await?;

            let expected: Vec<Option<String>> = vec![
                t1s[i].map(|v| v.to_string()),
                t2s[i].map(|v| v.to_string()),
                t3s[i].map(|v| v.to_string()),
                t4s[i].map(|v| v.to_string()),
                t5s[i].map(|v| v.to_string()),
                t6s[i].map(|v| v.to_string()),
                t7s[i].map(|v| v.to_string()),
                t8s[i].map(|v| v.to_string()),
                t9s[i].map(|v| v.to_string()),
                t10s[i].map(|v| v.to_string()),
                t11s[i].map(|v| v.to_string()),
                t12s[i].map(|v| v.to_string()),
                t13s[i].map(|v| v.to_string()),
                t14s[i].map(|v| v.to_string()),
                t15s[i].map(|v| v.to_string()),
                t16s[i].map(|v| v.to_string()),
            ];

            for (tag, expect) in tags.iter().zip(expected) {
                assert_eq!(tag.tag_value.as_deref(), expect.as_deref());
            }
        }

        for (_, &tbname) in tbnames.iter().enumerate() {
            stmt2
                .prepare(&format!("select * from {tbname} where ts >= ?"))
                .await?;

            let cols = vec![ColumnView::from_millis_timestamp(vec![1726803356466])];
            let param = Stmt2BindParam::new(None, None, Some(cols));
            stmt2.bind(&[param]).await?;

            let affected = stmt2.exec().await?;
            assert_eq!(affected, 0);

            let rows: Vec<Row> = stmt2
                .result_set()
                .await?
                .deserialize()
                .try_collect()
                .await?;

            assert_eq!(rows.len(), 3);

            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.ts, tss[i]);
                assert_eq!(row.c1, c1s[i]);
                assert_eq!(row.c2, c2s[i]);
                assert_eq!(row.c3, c3s[i]);
                assert_eq!(row.c4, c4s[i]);
                assert_eq!(row.c5, c5s[i]);
                assert_eq!(row.c6, c6s[i]);
                assert_eq!(row.c7, c7s[i]);
                assert_eq!(row.c8, c8s[i]);
                assert_eq!(row.c9, c9s[i]);
                assert_eq!(row.c10, c10s[i]);
                assert_eq!(row.c11, c11s[i]);
                assert_eq!(row.c12.as_deref(), c12s[i]);
                assert_eq!(row.c13.as_deref(), c13s[i]);
                assert_eq!(row.c14.as_deref(), c14s[i]);
                assert_eq!(row.c15.as_deref(), c15s[i]);
                assert_eq!(row.c16.as_deref(), c16s[i]);
                assert_eq!(row.c17, c17s[i].map(|v| Decimal::new(v, 10, 2).to_string()));
                assert_eq!(row.c18, c18s[i].map(|v| Decimal::new(v, 20, 5).to_string()));
            }
        }

        taos.exec("drop database if exists test_1769256745").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_stmt2_tbname() -> RawResult<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1769256746",
            "create database test_1769256746",
            "use test_1769256746",
            "create table s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                c9 bigint unsigned, c10 float, c11 double, c12 varchar(20), c13 nchar(20), \
                c14 varbinary(20), c15 geometry(50), c16 blob, c17 decimal(10, 2), \
                c18 decimal(20, 5)) \
                tags (t1 timestamp, t2 bool, t3 tinyint, t4 smallint, t5 int, \
                t6 bigint, t7 tinyint unsigned, t8 smallint unsigned, t9 int unsigned, \
                t10 bigint unsigned, t11 float, t12 double, t13 varchar(20), t14 nchar(20), \
                t15 varbinary(20), t16 geometry(50))",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare(
                "insert into s0 (tbname, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, \
                t14, t15, t16, ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, \
                c15, c16, c17, c18) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;

        let tbnames = vec!["t0", "t1", "t2"];

        let t1s = vec![Some(1726803356466), None, Some(1726803358466)];
        let t2s = vec![Some(true), None, Some(false)];
        let t3s = vec![Some(1), None, Some(-1)];
        let t4s = vec![Some(1), None, Some(-1)];
        let t5s = vec![Some(1), None, Some(-1)];
        let t6s = vec![Some(1), None, Some(-1)];
        let t7s = vec![Some(1), None, Some(1)];
        let t8s = vec![Some(1), None, Some(1)];
        let t9s = vec![Some(1), None, Some(1)];
        let t10s = vec![Some(1), None, Some(1)];
        let t11s = vec![Some(1.1), None, Some(-1.1)];
        let t12s = vec![Some(1.11), None, Some(-1.11)];
        let t13s = vec![Some("hello"), None, Some("world")];
        let t14s = vec![Some("hello"), None, Some("中文")];
        let t15s = vec![Some(&b"hello"[..]), None, Some(&b"\x00\x01\x02"[..])];
        let t16s = vec![
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
            None,
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
        ];

        let mut tags = Vec::with_capacity(t1s.len());
        for i in 0..t1s.len() {
            let mut tag = Vec::with_capacity(17);
            push_tag(&mut tag, t1s[i], Ty::Timestamp, |v| {
                Value::Timestamp(Timestamp::new(v, Precision::Millisecond))
            });
            push_tag(&mut tag, t2s[i], Ty::Bool, Value::Bool);
            push_tag(&mut tag, t3s[i], Ty::TinyInt, Value::TinyInt);
            push_tag(&mut tag, t4s[i], Ty::SmallInt, Value::SmallInt);
            push_tag(&mut tag, t5s[i], Ty::Int, Value::Int);
            push_tag(&mut tag, t6s[i], Ty::BigInt, Value::BigInt);
            push_tag(&mut tag, t7s[i], Ty::UTinyInt, Value::UTinyInt);
            push_tag(&mut tag, t8s[i], Ty::USmallInt, Value::USmallInt);
            push_tag(&mut tag, t9s[i], Ty::UInt, Value::UInt);
            push_tag(&mut tag, t10s[i], Ty::UBigInt, Value::UBigInt);
            push_tag(&mut tag, t11s[i], Ty::Float, Value::Float);
            push_tag(&mut tag, t12s[i], Ty::Double, Value::Double);
            push_tag(&mut tag, t13s[i], Ty::VarChar, |v| {
                Value::VarChar(v.to_string())
            });
            push_tag(&mut tag, t14s[i], Ty::NChar, |v| {
                Value::NChar(v.to_string())
            });
            push_tag(&mut tag, t15s[i], Ty::VarBinary, |v| {
                Value::VarBinary(Bytes::copy_from_slice(v))
            });
            push_tag(&mut tag, t16s[i], Ty::Geometry, |v| {
                Value::Geometry(Bytes::copy_from_slice(v))
            });
            tags.push(tag);
        }

        let tss = vec![1726803356466, 1726803357466, 1726803358466];
        let c1s = vec![Some(true), None, Some(false)];
        let c2s = vec![Some(1), None, Some(-1)];
        let c3s = vec![Some(1), None, Some(-1)];
        let c4s = vec![Some(1), None, Some(-1)];
        let c5s = vec![Some(1), None, Some(-1)];
        let c6s = vec![Some(1), None, Some(1)];
        let c7s = vec![Some(1), None, Some(1)];
        let c8s = vec![Some(1), None, Some(1)];
        let c9s = vec![Some(1), None, Some(1)];
        let c10s = vec![Some(1.1), None, Some(-1.1)];
        let c11s = vec![Some(1.11), None, Some(-1.11)];
        let c12s = vec![Some("hello"), None, Some("world")];
        let c13s = vec![Some("hello"), None, Some("中文")];
        let c14s = vec![Some(&b"hello"[..]), None, Some(&b"\x00\x01\x02"[..])];
        let c15s = vec![
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
            None,
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
        ];
        let c16s = vec![Some(&b"hello blob"[..]), None, Some(&b"world blob"[..])];
        let c17s = vec![Some(1234567), None, Some(-1234567)];
        let c18s = vec![Some(123456789012345), None, Some(-123456789012345)];

        let cols = vec![
            ColumnView::from_millis_timestamp(tss.clone()),
            ColumnView::from_bools(c1s.clone()),
            ColumnView::from_tiny_ints(c2s.clone()),
            ColumnView::from_small_ints(c3s.clone()),
            ColumnView::from_ints(c4s.clone()),
            ColumnView::from_big_ints(c5s.clone()),
            ColumnView::from_unsigned_tiny_ints(c6s.clone()),
            ColumnView::from_unsigned_small_ints(c7s.clone()),
            ColumnView::from_unsigned_ints(c8s.clone()),
            ColumnView::from_unsigned_big_ints(c9s.clone()),
            ColumnView::from_floats(c10s.clone()),
            ColumnView::from_doubles(c11s.clone()),
            ColumnView::from_varchar::<&str, _, _, _>(c12s.clone()),
            ColumnView::from_nchar::<&str, _, _, _>(c13s.clone()),
            ColumnView::from_bytes::<&[u8], _, _, _>(c14s.clone()),
            ColumnView::from_geobytes::<&[u8], _, _, _>(c15s.clone()),
            ColumnView::from_blob_bytes::<&[u8], _, _, _>(c16s.clone()),
            ColumnView::from_decimal64(c17s.clone(), 10, 2),
            ColumnView::from_decimal(c18s.clone(), 20, 5),
        ];

        let mut params = Vec::with_capacity(tbnames.len());
        for i in 0..tbnames.len() {
            params.push(Stmt2BindParam::new(
                Some(tbnames[i].to_string()),
                Some(tags.remove(0)),
                Some(cols.clone()),
            ));
        }
        stmt2.bind(&params).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 9);

        #[derive(Debug, Deserialize)]
        struct Tag {
            tag_value: Option<String>,
        }

        #[derive(Debug, Deserialize)]
        struct Row {
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
            c14: Option<Vec<u8>>,
            c15: Option<Vec<u8>>,
            c16: Option<Vec<u8>>,
            c17: Option<String>,
            c18: Option<String>,
        }

        let t11s = vec![Some("1.10000"), None, Some("-1.10000")];
        let t12s = vec![Some("1.110000000"), None, Some("-1.110000000")];
        let t15s = vec![Some("\\x68656C6C6F"), None, Some("\\x000102")];
        let t16s = vec![
            Some("POINT (1.000000 1.000000)"),
            None,
            Some("POINT (1.000000 1.000000)"),
        ];

        for (i, &tbname) in tbnames.iter().enumerate() {
            let tags: Vec<Tag> = taos
                .query(format!("show tags from {tbname}"))
                .await?
                .deserialize()
                .try_collect()
                .await?;

            let expected: Vec<Option<String>> = vec![
                t1s[i].map(|v| v.to_string()),
                t2s[i].map(|v| v.to_string()),
                t3s[i].map(|v| v.to_string()),
                t4s[i].map(|v| v.to_string()),
                t5s[i].map(|v| v.to_string()),
                t6s[i].map(|v| v.to_string()),
                t7s[i].map(|v| v.to_string()),
                t8s[i].map(|v| v.to_string()),
                t9s[i].map(|v| v.to_string()),
                t10s[i].map(|v| v.to_string()),
                t11s[i].map(|v| v.to_string()),
                t12s[i].map(|v| v.to_string()),
                t13s[i].map(|v| v.to_string()),
                t14s[i].map(|v| v.to_string()),
                t15s[i].map(|v| v.to_string()),
                t16s[i].map(|v| v.to_string()),
            ];

            for (tag, expect) in tags.iter().zip(expected) {
                assert_eq!(tag.tag_value.as_deref(), expect.as_deref());
            }
        }

        for (_, &tbname) in tbnames.iter().enumerate() {
            stmt2
                .prepare(&format!("select * from {tbname} where ts >= ?"))
                .await?;

            let cols = vec![ColumnView::from_millis_timestamp(vec![1726803356466])];
            let param = Stmt2BindParam::new(None, None, Some(cols));
            stmt2.bind(&[param]).await?;

            let affected = stmt2.exec().await?;
            assert_eq!(affected, 0);

            let rows: Vec<Row> = stmt2
                .result_set()
                .await?
                .deserialize()
                .try_collect()
                .await?;

            assert_eq!(rows.len(), 3);

            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.ts, tss[i]);
                assert_eq!(row.c1, c1s[i]);
                assert_eq!(row.c2, c2s[i]);
                assert_eq!(row.c3, c3s[i]);
                assert_eq!(row.c4, c4s[i]);
                assert_eq!(row.c5, c5s[i]);
                assert_eq!(row.c6, c6s[i]);
                assert_eq!(row.c7, c7s[i]);
                assert_eq!(row.c8, c8s[i]);
                assert_eq!(row.c9, c9s[i]);
                assert_eq!(row.c10, c10s[i]);
                assert_eq!(row.c11, c11s[i]);
                assert_eq!(row.c12.as_deref(), c12s[i]);
                assert_eq!(row.c13.as_deref(), c13s[i]);
                assert_eq!(row.c14.as_deref(), c14s[i]);
                assert_eq!(row.c15.as_deref(), c15s[i]);
                assert_eq!(row.c16.as_deref(), c16s[i]);
                assert_eq!(row.c17, c17s[i].map(|v| Decimal::new(v, 10, 2).to_string()));
                assert_eq!(row.c18, c18s[i].map(|v| Decimal::new(v, 20, 5).to_string()));
            }
        }

        taos.exec("drop database if exists test_1769256746").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_stmt2_json() -> RawResult<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec_many([
            "drop database if exists test_1769406117",
            "create database test_1769406117",
            "use test_1769406117",
            "create table s0 (ts timestamp, c1 int) tags (t1 json)",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare("insert into t0 using s0 tags (?) values(?, ?)")
            .await?;

        let tags = vec![Value::Json(
            serde_json::from_str(r#"{"key":"value"}"#).unwrap(),
        )];
        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466, 1726803357466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(None, Some(tags), Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 2);

        #[derive(Debug, Deserialize)]
        struct Tag {
            tag_value: Option<String>,
        }

        let tags: Vec<Tag> = taos
            .query("show tags from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].tag_value.as_deref(), Some(r#"{"key":"value"}"#));

        taos.exec("drop database if exists test_1769406117").await?;

        Ok(())
    }
}

#[cfg(feature = "test-new-feat")]
#[cfg(test)]
mod sync_tests {
    use serde::Deserialize;
    use taos_query::common::decimal::Decimal;

    use crate::prelude::sync::*;

    #[test]
    fn test_stmt2_table() -> RawResult<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;

        taos.exec_many([
            "drop database if exists test_1769259199",
            "create database test_1769259199",
            "use test_1769259199",
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                c9 bigint unsigned, c10 float, c11 double, c12 varchar(20), c13 nchar(20), \
                c14 varbinary(20), c15 geometry(50), c16 blob, c17 decimal(10, 2), \
                c18 decimal(20, 5))",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare(
            "insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )?;

        let tss = vec![1726803356466, 1726803357466, 1726803358466];
        let c1s = vec![Some(true), None, Some(false)];
        let c2s = vec![Some(1), None, Some(-1)];
        let c3s = vec![Some(1), None, Some(-1)];
        let c4s = vec![Some(1), None, Some(-1)];
        let c5s = vec![Some(1), None, Some(-1)];
        let c6s = vec![Some(1), None, Some(1)];
        let c7s = vec![Some(1), None, Some(1)];
        let c8s = vec![Some(1), None, Some(1)];
        let c9s = vec![Some(1), None, Some(1)];
        let c10s = vec![Some(1.1), None, Some(-1.1)];
        let c11s = vec![Some(1.11), None, Some(-1.11)];
        let c12s = vec![Some("hello"), None, Some("world")];
        let c13s = vec![Some("hello"), None, Some("中文")];
        let c14s = vec![Some(&b"hello"[..]), None, Some(&b"\x00\x01\x02"[..])];
        let c15s = vec![
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
            None,
            Some(
                &[
                    1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
                ][..],
            ),
        ];
        let c16s = vec![Some(&b"hello blob"[..]), None, Some(&b"world blob"[..])];
        let c17s = vec![Some(1234567), None, Some(-1234567)];
        let c18s = vec![Some(123456789012345), None, Some(-123456789012345)];

        let cols = vec![
            ColumnView::from_millis_timestamp(tss.clone()),
            ColumnView::from_bools(c1s.clone()),
            ColumnView::from_tiny_ints(c2s.clone()),
            ColumnView::from_small_ints(c3s.clone()),
            ColumnView::from_ints(c4s.clone()),
            ColumnView::from_big_ints(c5s.clone()),
            ColumnView::from_unsigned_tiny_ints(c6s.clone()),
            ColumnView::from_unsigned_small_ints(c7s.clone()),
            ColumnView::from_unsigned_ints(c8s.clone()),
            ColumnView::from_unsigned_big_ints(c9s.clone()),
            ColumnView::from_floats(c10s.clone()),
            ColumnView::from_doubles(c11s.clone()),
            ColumnView::from_varchar::<&str, _, _, _>(c12s.clone()),
            ColumnView::from_nchar::<&str, _, _, _>(c13s.clone()),
            ColumnView::from_bytes::<&[u8], _, _, _>(c14s.clone()),
            ColumnView::from_geobytes::<&[u8], _, _, _>(c15s.clone()),
            ColumnView::from_blob_bytes::<&[u8], _, _, _>(c16s.clone()),
            ColumnView::from_decimal64(c17s.clone(), 10, 2),
            ColumnView::from_decimal(c18s.clone(), 20, 5),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param])?;

        let affected = stmt2.exec()?;
        assert_eq!(affected, 3);
        assert_eq!(stmt2.affected_rows(), 3);

        stmt2.prepare("select * from t0 where ts >= ?")?;

        let cols = vec![ColumnView::from_millis_timestamp(vec![1726803356466])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param])?;

        let affected = stmt2.exec()?;
        assert_eq!(affected, 0);
        assert_eq!(stmt2.affected_rows(), 3);

        #[derive(Debug, Deserialize)]
        struct Row {
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
            c14: Option<Vec<u8>>,
            c15: Option<Vec<u8>>,
            c16: Option<Vec<u8>>,
            c17: Option<String>,
            c18: Option<String>,
        }

        let rows: Vec<Row> = stmt2.result_set()?.deserialize().try_collect()?;
        assert_eq!(rows.len(), 3);

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.ts, tss[i]);
            assert_eq!(row.c1, c1s[i]);
            assert_eq!(row.c2, c2s[i]);
            assert_eq!(row.c3, c3s[i]);
            assert_eq!(row.c4, c4s[i]);
            assert_eq!(row.c5, c5s[i]);
            assert_eq!(row.c6, c6s[i]);
            assert_eq!(row.c7, c7s[i]);
            assert_eq!(row.c8, c8s[i]);
            assert_eq!(row.c9, c9s[i]);
            assert_eq!(row.c10, c10s[i]);
            assert_eq!(row.c11, c11s[i]);
            assert_eq!(row.c12.as_deref(), c12s[i]);
            assert_eq!(row.c13.as_deref(), c13s[i]);
            assert_eq!(row.c14.as_deref(), c14s[i]);
            assert_eq!(row.c15.as_deref(), c15s[i]);
            assert_eq!(row.c16.as_deref(), c16s[i]);
            assert_eq!(row.c17, c17s[i].map(|v| Decimal::new(v, 10, 2).to_string()));
            assert_eq!(row.c18, c18s[i].map(|v| Decimal::new(v, 20, 5).to_string()));
        }

        taos.exec("drop database if exists test_1769259199")?;

        Ok(())
    }
}
