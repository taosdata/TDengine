use crate::{err_or, ffi::*, into_c_str::IntoCStr, RawRes, RawTaos, ResultSet};

use std::{ffi::CStr, os::raw::*};

use taos_error::{Code, Error};
use taos_query::common::{itypes::ITimestamp, Ty};

use crate::types::*;

mod bind;
mod multi;

#[derive(Debug)]
pub struct RawStmt(*mut TAOS_STMT);

impl Drop for RawStmt {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl RawStmt {
    #[inline(always)]
    fn ok(&self, code: impl Into<Code>) -> Result<(), Error> {
        let code = code.into();

        if code.success() {
            Ok(())
        } else {
            Err(Error::from_string(self.err_as_str()))
        }
    }

    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut TAOS_STMT {
        self.0
    }

    #[inline]
    pub fn errstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(taos_stmt_errstr(self.as_ptr())) }
    }

    #[inline]
    pub fn err_as_str(&self) -> String {
        unsafe {
            CStr::from_ptr(taos_stmt_errstr(self.as_ptr()))
                .to_string_lossy()
                .to_string()
        }
    }

    #[inline]
    pub fn from_raw_taos(taos: &RawTaos) -> RawStmt {
        RawStmt(unsafe { taos_stmt_init(taos.as_ptr()) })
    }
    #[inline]
    pub fn close(&mut self) -> Result<(), Error> {
        err_or!(self, taos_stmt_close(self.as_ptr()))
    }

    #[inline]
    pub fn prepare<'c>(&mut self, sql: impl IntoCStr<'c>) -> Result<(), Error> {
        let sql = sql.into_c_str();
        self.ok(unsafe {
            taos_stmt_prepare(self.as_ptr(), sql.as_ptr(), sql.to_bytes().len() as _)
        })
    }

    pub fn set_tbname_tags_v3<'a>(
        &mut self,
        name: impl IntoCStr<'a>,
        tags: &[TaosBind],
    ) -> Result<(), Error> {
        self.ok(unsafe {
            taos_stmt_set_tbname_tags(
                self.as_ptr(),
                name.into_c_str().as_ptr(),
                tags.as_ptr() as _,
            )
        })
    }

    #[inline]
    pub fn set_tbname<'c>(&mut self, name: impl IntoCStr<'c>) -> Result<(), Error> {
        self.ok(unsafe { taos_stmt_set_tbname(self.as_ptr(), name.into_c_str().as_ptr()) })
    }

    #[inline]
    pub fn set_sub_tbname<'c>(&mut self, name: impl IntoCStr<'c>) -> Result<(), Error> {
        self.ok(unsafe { taos_stmt_set_sub_tbname(self.as_ptr(), name.into_c_str().as_ptr()) })
    }

    #[inline]
    pub fn set_tags(&mut self, tags: &[TaosBind]) -> Result<(), Error> {
        self.ok(unsafe { taos_stmt_set_tags(self.as_ptr(), tags.as_ptr() as _) })
    }

    #[inline]
    pub fn use_result(&mut self) -> Result<ResultSet, Error> {
        unsafe { RawRes::from_ptr(taos_stmt_use_result(self.as_ptr())).map(ResultSet::new) }
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { taos_stmt_affected_rows(self.as_ptr()) }
    }

    #[inline]
    pub fn execute(&self) -> Result<(), Error> {
        err_or!(self, taos_stmt_execute(self.as_ptr()))
    }

    #[inline]
    pub fn add_batch(&self) -> Result<(), Error> {
        err_or!(self, taos_stmt_add_batch(self.as_ptr()))
    }

    #[inline]
    pub fn is_insert(&self) -> Result<bool, Error> {
        let mut is_insert = 0;
        err_or!(
            self,
            taos_stmt_is_insert(self.as_ptr(), &mut is_insert as _),
            is_insert != 0
        )
    }

    #[inline]
    pub fn num_params(&self) -> Result<usize, Error> {
        let mut num = 0i32;
        err_or!(
            self,
            taos_stmt_num_params(self.as_ptr(), &mut num as _),
            num as usize
        )
    }

    #[inline]
    pub fn get_param(&mut self, idx: i32) -> Result<(Ty, i32), Error> {
        let (mut type_, mut bytes) = (0, 0);
        err_or!(
            self,
            taos_stmt_get_param(self.as_ptr(), idx, &mut type_ as _, &mut bytes as _),
            ((type_ as u8).into(), bytes)
        )
    }
    #[inline]
    pub fn bind_param(&mut self, bind: &[TaosBind]) -> Result<(), Error> {
        err_or!(self, taos_stmt_bind_param(self.as_ptr(), bind.as_ptr()))
    }

    #[inline]
    pub fn bind_param_batch(&mut self, bind: &[TaosMultiBind]) -> Result<(), Error> {
        err_or!(
            self,
            taos_stmt_bind_param_batch(self.as_ptr(), bind.as_ptr())
        )
    }

    #[inline]
    pub fn bind_single_param_batch(&self, bind: &TaosMultiBind, col: i32) -> Result<(), Error> {
        self.ok(unsafe {
            taos_stmt_bind_single_param_batch(self.as_ptr(), bind as *const _ as _, col)
        })
    }
}

#[test]
fn test_tbname_tags() -> Result<(), Error> {
    use std::ptr::null;
    let host = null();
    let user = null();
    let pass = null();
    let db = null();
    let port = 0;
    let taos = RawTaos::connect(host, user, pass, db, port)?;
    taos.query("drop database if exists stt1")?;
    taos.query("create database if not exists stt1 keep 36500")?;
    taos.query("use stt1")?;
    taos.query(
        // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
        "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 float)",
    )?;

    let mut stmt = RawStmt::from_raw_taos(&taos);
    let sql = "insert into ? using st1 tags(?, ?) values(?, ?)";
    stmt.prepare(sql)?;

    // let tags = vec![TaosBind::from_json(r#"{"name":"value"}"#)];
    let tags = vec![TaosBind::from(&0i32), TaosBind::from(&0.0f32)];
    println!("tags: {tags:#?}");
    let tbname = "tb1";
    stmt.set_tbname(tbname)?;
    stmt.set_tags(&tags)?;
    // stmt.set_tbname_tags_v3(&tbname, &tags)?;
    println!("bind");

    // todo: get_param not implemented in taosc 3.0
    // let p = stmt.get_param(0)?;
    // dbg!(p);

    let params = vec![TaosBind::from(&ITimestamp(0)), TaosBind::from(&0i32)];
    stmt.bind_param(&params)?;
    println!("add batch");

    stmt.add_batch()?;
    stmt.execute()?;

    assert!(stmt.affected_rows() == 1);
    println!("done");

    // let mut stmt = RawStmt::from_raw_taos(&taos);
    // stmt.prepare("select * from ?")?;
    // stmt.set_tbname("st1")?;
    // stmt.execute()?;

    // let mut res = stmt.use_result()?;
    // use taos_query::prelude::*;
    // let blocks = res.blocks().try_for_each(|block| {
    //     dbg!(block);
    //     futures::future::ready(Ok(()))
    // });

    taos.query("drop database stt1")?;
    Ok(())
}

#[test]
fn test_tbname_tags_json() -> Result<(), Error> {
    use std::ptr::null;
    let host = null();
    let user = null();
    let pass = null();
    let db = null();
    let port = 0;
    let taos = RawTaos::connect(host, user, pass, db, port)?;
    taos.query("drop database if exists stt2")?;
    taos.query("create database if not exists stt2 keep 36500")?;
    taos.query("use stt2")?;
    taos.query(
        "create stable if not exists st1(ts timestamp, v int) tags(jt json)", // "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 float)",
    )?;

    let mut stmt = RawStmt::from_raw_taos(&taos);
    let sql = "insert into ? using st1 tags(?) values(?, ?)";
    stmt.prepare(sql)?;

    let tags = vec![TaosBind::from_json(r#"{"name":"value"}"#)];
    // let tags = vec![TaosBind::from(&0i32), TaosBind::from(&0.0f32)];
    println!("tags: {tags:#?}");
    let tbname = "tb1";
    stmt.set_tbname(tbname)?;

    stmt.set_tags(&tags)?;
    // stmt.set_tbname_tags_v3(&tbname, &tags)?;
    println!("bind");

    // todo: get_param not implemented in taosc 3.0
    // let p = stmt.get_param(0)?;
    // dbg!(p);

    let params = vec![TaosBind::from(&ITimestamp(0)), TaosBind::from(&0i32)];
    stmt.bind_param(&params)?;
    println!("add batch");

    stmt.add_batch()?;
    stmt.execute()?;

    assert!(stmt.affected_rows() == 1);

    stmt.prepare("insert into tb1 values(?,?)")?;
    let params = vec![TaosBind::from(&ITimestamp(1)), TaosBind::from(&0i32)];
    stmt.bind_param(&params)?;
    println!("add batch");

    stmt.add_batch()?;
    stmt.execute()?;

    let res = taos.query("select * from st1")?;

    if let Some(raw) = res.fetch_raw_block()? {
        assert_eq!(raw.nrows(), 2);
    } else {
        panic!("no data retrieved");
    }

    taos.query("drop database stt2")?;
    Ok(())
}
