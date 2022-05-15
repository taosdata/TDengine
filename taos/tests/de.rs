#![allow(dead_code)]
use chrono::NaiveDateTime;
use futures::StreamExt;
use futures::TryStreamExt;
use std::ffi::CString;

use taos::helpers::ShowDatabase;

use taos::prelude::*;

pub use taos::helpers;
#[derive(serde::Deserialize, Debug)]
struct JsonTag {
    name: String,
}

#[derive(serde::Deserialize, Debug)]
struct Record {
    ts: NaiveDateTime,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    raw_ts: i64,
    c_str: CString,
    str: String,
    json_tag: serde_json::Value,
}
#[derive(serde::Deserialize, Debug)]
struct RecordOption {
    ts: Option<NaiveDateTime>,
    i8: Option<i8>,
    i16: Option<i16>,
    i32: Option<i32>,
    i64: Option<i64>,
    u8: Option<u8>,
    u16: Option<u16>,
    u32: Option<u32>,
    u64: Option<u64>,
    raw_ts: Option<i64>,
    c_str: Option<CString>,
    str: Option<String>,
    json_tag: Option<serde_json::Value>,
}
#[derive(serde::Deserialize, Debug)]
struct RecordOptionWithJsonTag {
    ts: Option<NaiveDateTime>,
    i8: Option<i8>,
    i16: Option<i16>,
    i32: Option<i32>,
    i64: Option<i64>,
    u8: Option<u8>,
    u16: Option<u16>,
    u32: Option<u32>,
    u64: Option<u64>,
    raw_ts: Option<i64>,
    c_str: Option<CString>,
    str: Option<String>,
    json_tag: Option<JsonTag>,
}

#[cfg(any(feature = "test", test))]
#[taos::test(log_level = "trace")]
async fn de_seq_value(taos: &Taos, _database: &str) -> anyhow::Result<()> {
    log::info!("create table");
    taos.exec_sync(
        "create table if not exists stb1(ts timestamp,
            i8 tinyint, i16 smallint, i32 int, i64 bigint,
            u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, u64 bigint unsigned,
            raw_ts timestamp, c_str binary(100), str nchar(100)) tags (gid int, location nchar(16))",
    )?;
    log::info!("insert data");
    taos.exec_sync(concat!(
        r#"insert into tb1 using stb1 tags(1, 'beijing') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;
    taos.exec_sync(concat!(
        r#"insert into tb2 using stb1 tags(2, 'shanghai') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;

    log::info!("select");
    let mut res = taos
        .query("select tbname,gid,location from stb1")
        .await?;
    use futures::StreamExt;

    use futures::future;
    res.deserialize_stream::<Vec<Value>>()
        .enumerate()
        .for_each(|(_, v)| {
            let value = v.unwrap();
            log::debug!("{:?}", value);
            future::ready(())
        })
        .await;

    Ok(())
}

#[taos::test]
async fn de_seq_value2(taos: &Taos, _database: &str) -> anyhow::Result<()> {
    log::info!("create table");
    taos.exec_sync(
        "create table if not exists stb1(ts timestamp,
            i8 tinyint, i16 smallint, i32 int, i64 bigint,
            u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, u64 bigint unsigned,
            raw_ts timestamp, c_str binary(100), str nchar(100)) tags (groupid int, location nchar(16))",
    )?;
    log::info!("insert data");
    taos.exec_sync(concat!(
        r#"insert into tb1 using stb1 tags(1, 'beijing') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;
    taos.exec_sync(concat!(
        r#"insert into tb2 using stb1 tags(2, 'shanghai') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;

    log::info!("select");
    let mut res = taos
        .query("select tbname,groupid,location from stb1")
        .await?;
    use futures::StreamExt;

    // block.rows_iter()
    // let mut stream = res.deserialize_stream();
    use futures::future;
    res.deserialize_stream::<Vec<Value>>()
        .enumerate()
        .for_each(|(_, v)| {
            let value = v.unwrap();
            log::debug!("{:?}", value);
            future::ready(())
        })
        .await;
    Ok(())
}

#[taos::test]
async fn de_all(taos: &Taos, _database: &str) -> anyhow::Result<()> {
    log::info!("create table");
    taos.exec_sync(
        "create table if not exists stb1(ts timestamp,
            i8 tinyint, i16 smallint, i32 int, i64 bigint,
            u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, u64 bigint unsigned,
            raw_ts timestamp, c_str binary(100), str nchar(100)) tags (json_tag json)",
    )?;
    log::info!("insert data");
    taos.exec_sync(concat!(
        r#"insert into tb1 using stb1 tags('{"name":"abc"}') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;

    log::info!("select");
    let mut res = taos.query("select * from stb1").await?;
    use futures::StreamExt;

    // block.rows_iter()
    let record: Record = res.deserialize_stream().next().await.unwrap()?;
    log::debug!("fetched record {:?}", record);
    Ok(())
}

#[taos::test(log_level = "trace", dropping = "none")]
async fn de_all_option(taos: &Taos, _database: &str) -> anyhow::Result<()> {
    log::info!("create table");
    taos.exec_sync(
        "create table if not exists stb1(ts timestamp,
            i8 tinyint, i16 smallint, i32 int, i64 bigint,
            u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, u64 bigint unsigned,
            raw_ts timestamp, c_str binary(100), str nchar(100)) tags (json_tag json)",
    )?;
    log::info!("insert data");
    taos.exec_sync(concat!(
        r#"insert into tb1 using stb1 tags('{"name":"abc"}') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;
    taos.exec_sync(concat!(
        r#"insert into tb2 using stb1 tags('{"name":"涛思数据"}') "#,
        r#"values (now,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"#
    ))?;
    log::info!("select");
    let mut res = taos.query("select * from stb1").await?;
    // block.rows_iter()
    let record: Vec<RecordOption> = res.deserialize_stream().try_collect().await?;
    log::debug!("fetched record {:?}", record);
    Ok(())
}

#[taos::test(log_level = "trace")]
async fn de_all_option_with_json_tag_struct(taos: &Taos, _database: &str) -> anyhow::Result<()> {
    log::info!("create table");
    taos.exec_sync(
        "create table if not exists stb1(ts timestamp,
            i8 tinyint, i16 smallint, i32 int, i64 bigint,
            u8 tinyint unsigned, u16 smallint unsigned, u32 int unsigned, u64 bigint unsigned,
            raw_ts timestamp, c_str binary(100), str nchar(100)) tags (json_tag json)",
    )?;
    log::info!("insert data");
    taos.exec_sync(concat!(
        r#"insert into tb1 using stb1 tags('{"name":"abc"}') "#,
        r#"values (now,1,2,3,4,5,6,7,8, now, "abc", "世界")"#
    ))?;
    taos.exec_sync(concat!(
        r#"insert into tb2 using stb1 tags('{"name":"涛思数据"}') "#,
        r#"values (now,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"#
    ))?;
    log::info!("select");
    let mut res = taos.query("select * from stb1").await?;
    // block.rows_iter()
    let record: Vec<RecordOptionWithJsonTag> = res.deserialize_stream().try_collect().await?;
    log::debug!("fetched records {:#?}", record);
    Ok(())
}

#[taos::test]
async fn de_string(taos: &Taos) -> anyhow::Result<()> {
    let mut res = taos.query("select server_version() as version").await?;
    use futures::StreamExt;

    let version: String = res
        .deserialize_stream()
        .next()
        .await
        .expect("select version")?;
    println!("version: {version}");
    Ok(())
}

#[taos::test]
async fn de_wrapper_struct(taos: &Taos) -> anyhow::Result<()> {
    let mut res = taos.query("select server_version() as version").await?;
    use futures::StreamExt;

    #[derive(::serde::Deserialize, Debug)]
    struct Version(String);
    let version: Version = res
        .deserialize_stream()
        .next()
        .await
        .expect("select version")?;
    println!("version: {:?}", version);
    Ok(())
}

#[taos::test]
async fn de_named_struct(taos: &Taos) -> anyhow::Result<()> {
    macro_rules! de {
        ($taos:expr, $sql:expr) => {
            $taos
                .query($sql)
                .await?
                .deserialize_stream()
                .next()
                .await
                .unwrap()?
        };
    }
    #[derive(::serde::Deserialize, Debug)]
    struct Version {
        version: String,
    }
    #[derive(::serde::Deserialize, Debug)]
    struct WrapperVersion(String);
    #[derive(::serde::Deserialize, Debug)]
    struct WrapperOptionVersion(Option<String>);

    // value
    let _version: String = de!(taos, "select server_version()");
    // tuple
    let _version: (String,) = de!(taos, "select server_version()");
    // struct
    let _version: Version = de!(taos, "select server_version() as version");
    // option
    let _version: Option<String> = de!(taos, "select server_version() as version");
    // wrapper struct
    let _version: WrapperVersion = de!(taos, "select server_version() as version");
    // wrapper struct with option
    let _version: WrapperOptionVersion = dbg!(de!(taos, "select server_version() as version"));
    Ok(())
}

#[taos::test]
async fn de_vec(taos: &Taos) -> anyhow::Result<()> {
    // let taos = TaosOptions::new().build()?;
    // std::env::set_var("RUST_LOG", "trace");
    // pretty_env_logger::init();

    let db: Vec<ShowDatabase> = taos.databases().await?;
    println!("db: {:?}", db);
    Ok(())
}
