use crate::{ffi::*, into_c_str::IntoCStr, RawRes, RawTaos};

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
    pub fn use_result(&mut self) -> RawRes {
        unsafe { RawRes::from_ptr_unchecked(taos_stmt_use_result(self.as_ptr())) }
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
    taos.query(b"drop database if exists stt1\0".as_ptr() as _)?;
    taos.query(b"create database if not exists stt1 keep 36500\0".as_ptr() as _)?;
    taos.query(b"use stt1\0".as_ptr() as _)?;
    taos.query(
        b"create stable if not exists st1(ts timestamp, v int) tags(t1 int, t2 bool)\0".as_ptr()
            as _,
    )?;

    let mut stmt = RawStmt::from_raw_taos(&taos);
    let sql = "insert into ? using st1 tags(?, ?) values(?, ?)";
    stmt.prepare(sql)?;

    let tags = vec![TaosBind::from(&1i32), TaosBind::from(&true)];
    println!("tags: {tags:#?}");
    let tbname = "tb1";
    stmt.set_tbname_tags_v3(&tbname, &tags)?;
    println!("bind");

    let params = vec![TaosBind::from(&ITimestamp(0)), TaosBind::from(&0i32)];
    stmt.bind_param(&params)?;
    println!("add batch");

    stmt.add_batch()?;
    stmt.execute()?;

    assert!(stmt.affected_rows() == 1);

    let res = taos.query(b"select count(*) from st1\0".as_ptr() as _)?;
    

    // taos.query(b"drop database stt1\0".as_ptr() as _)?;
    Ok(())
}
