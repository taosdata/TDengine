// todo: some const functions are not available for stable Rust.
use taos_query::common::{RawBlock, Value};
#[test]
fn raw_block() -> Result<(), taos_error::Error> {
    use taos_sys::*;
    let taos = RawTaos::connect(
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        0,
    )?;
    let rs = taos.query("show databases")?;
    let fields = rs.fields();
    let precision = rs.precision();
    let field_count = rs.field_count();
    let (ptr, rows) = rs.fetch_raw_block()?;

    let inner = unsafe { RawBlock::from_ptr(ptr, rows as _, field_count as _, precision) };
    let gid = inner.group_id();
    println!("group id: {gid}");

    for i in 0..field_count {
        let col = inner.get_schema_of(i as _);
        let field = &fields[i as usize];
        println!("{field:?}, {col:#x?}");
    }

    dbg!(unsafe { inner.get_unchecked(0, field_count as usize - 1) });
    for row in 0..dbg!(rows) as usize {
        for col in 0..field_count as usize {
            println!("({row}, {col}): ");
            let v = unsafe { inner.get_unchecked(row, col) };
            dbg!(v);
        }
    }
    Ok(())
}

#[test]
fn raw_block_full_test() -> Result<(), taos_error::Error> {
    use taos_sys::*;
    let taos = RawTaos::connect(
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        0,
    )?;

    let _ = taos.query("drop database if exists _rs_ts_raw_block_full_")?;
    let _ = taos.query("create database if not exists _rs_ts_raw_block_full_")?;
    let _ = taos.query("use _rs_ts_raw_block_full_")?;
    let _ = taos.query("create stable stb1 (ts timestamp,vb bool,vi8 tinyint,vi16 smallint,\
        vi32 int,vi64 bigint, vu8 tinyint unsigned,vu16 smallint unsigned,vu32 int unsigned,vu64 bigint unsigned,\
        vf float,vd double,vv varchar(100), vn nchar(100)) tags(tj json)")?;

    let _ = taos.query(
        "insert into tb1 using stb1 tags(NULL) values\
        (1655793421375,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
    )?;
    let _ = taos.query(
        "insert into tb2 using stb1 tags('{\"a\":\"涛思𝄞数据\"}') values\
        (1655793421374,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)\
        (1655793421375,true, -1, -1, -1, -1, 1, 1, 1, 1, 0.0, 0.0, 'abc', '涛思𝄞数据')",
    )?;
    let rs = taos.query("select * from stb1 order by tbname,ts")?;
    let fields = rs.fields();
    let precision = rs.precision();
    let field_count = rs.field_count();
    let (ptr, rows) = rs.fetch_raw_block()?;

    let inner = unsafe { RawBlock::from_ptr(ptr, rows as _, field_count as _, precision) };
    let gid = inner.group_id();
    println!("group id: {gid}");

    use std::ascii::escape_default;

    pub fn show_buf<B: AsRef<[u8]>>(buf: B) -> String {
        String::from_utf8(
            buf.as_ref()
                .iter()
                .map(|b| escape_default(*b))
                .flatten()
                .collect(),
        )
        .unwrap()
    }

    dbg!(inner.len());
    dbg!(inner.as_bytes());
    let bytes = inner.to_vec();
    println!("{}", show_buf(bytes));

    for i in 0..field_count {
        let col = inner.get_schema_of(i as _);
        let field = &fields[i as usize];
        println!("{field:?}, {col:#x?}");
    }
    dbg!(unsafe { inner.get_unchecked(0, field_count as usize - 1) });
    for row in 0..dbg!(rows) as usize {
        for col in 0..field_count as usize {
            println!("({row}, {col}): ");
            let v = unsafe { inner.get_unchecked(row, col) };

            dbg!(v);
        }
    }
    Ok(())
}
