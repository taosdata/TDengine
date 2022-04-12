#![allow(dead_code)]
use std::{ffi::CString, io::Write, ops::Deref, sync::Once};

use chrono::NaiveDateTime;
use futures::StreamExt;
use futures::TryStreamExt;
use log::Level;
use pretty_env_logger::env_logger::fmt::{Color, StyledValue};

use taos::{block::Value, helpers::Precision, *};

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
struct TaosWrapper {
    taos: Taos,
    db: String,
}
impl Deref for TaosWrapper {
    type Target = Taos;

    fn deref(&self) -> &Self::Target {
        &self.taos
    }
}

impl Drop for TaosWrapper {
    fn drop(&mut self) {
        self.clean().unwrap();
    }
}

impl TaosWrapper {
    fn new() -> Result<Self> {
        static LOGGER_INIT: Once = Once::new();
        LOGGER_INIT.call_once(|| {
            pretty_env_logger::formatted_timed_builder()
                .format_module_path(true)
                .filter_level(log::LevelFilter::Trace)
                .format(|buf, record| -> std::result::Result<(), std::io::Error> {
                    fn colored_level<'a>(
                        style: &'a mut pretty_env_logger::env_logger::fmt::Style,
                        level: Level,
                    ) -> StyledValue<'a, &'static str> {
                        match level {
                            Level::Trace => style.set_color(Color::Magenta).value("TRACE"),
                            Level::Debug => style.set_color(Color::Blue).value("DEBUG"),
                            Level::Info => style.set_color(Color::Green).value("INFO "),
                            Level::Warn => style.set_color(Color::Yellow).value("WARN "),
                            Level::Error => style.set_color(Color::Red).value("ERROR"),
                        }
                    }
                    let mut style = buf.style();
                    writeln!(
                        buf,
                        "[{}:{}] {} {} - {}",
                        record.file().unwrap_or("unknown"),
                        record.line().unwrap_or(0),
                        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                        colored_level(&mut style, record.level()),
                        record.args()
                    )
                })
                .is_test(true)
                .init();
        });
        let taos = TaosOptions::new().build()?;
        use rand::Rng;
        let mut rng = rand::thread_rng();

        use faker_rand::lorem::Word;
        let db = String::from_iter([rng.gen::<Word>().to_string(), rng.gen::<Word>().to_string()]);
        taos.exec_sync(format!("drop database if exists {db}",))?;
        taos.exec_sync(format!("create database if not exists {db} precision 'ns'",))?;
        taos.exec_sync(format!("use {db}"))?;

        log::debug!("test in database: {db}");
        Ok(Self { taos, db })
    }

    fn clean(&self) -> Result<()> {
        let db = &self.db;
        log::debug!("drop database: {db}");
        self.taos
            .exec_sync(format!("drop database if exists {}", db))?;
        log::debug!("dropped database: {db}");
        Ok(())
    }
}
#[tokio::test]
async fn de_seq_value() -> Result<()> {
    let taos = &TaosWrapper::new()?;
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
    let res = taos
        .query("select tbname,groupid,location from stb1")
        .await?;
    use futures::StreamExt;

    // block.rows_iter()
    // let mut stream = res.rows_de_stream();
    use futures::future;
    res.rows_de_stream::<Vec<Value>>()
        .enumerate()
        .for_each(|(_, v)| {
            let value = v.unwrap();
            log::debug!("{:?}", value);
            future::ready(())
        })
        .await;

    // let record: Vec<Value> = stream.next().await.unwrap().unwrap();
    // log::debug!("fetched record {:?}", record);
    // let record: Vec<Value> = stream.next().await.unwrap().unwrap();
    // log::debug!("fetched record {:?}", record);
    // let record: Vec<Value> = stream.next().await.unwrap().unwrap();
    // log::debug!("fetched record {:?}", record);
    taos.clean()?;
    Ok(())
}
#[tokio::test]
async fn de_all() -> Result<()> {
    let taos = &TaosWrapper::new()?;
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
    let res = taos.query("select * from stb1").await?;
    use futures::StreamExt;

    // block.rows_iter()
    let record: Record = res.rows_de_stream().next().await.unwrap()?;
    log::debug!("fetched record {:?}", record);
    taos.clean()?;
    Ok(())
}
#[tokio::test]
async fn de_all_option() -> Result<()> {
    let taos = &TaosWrapper::new()?;
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
    let res = taos.query("select * from stb1").await?;
    // block.rows_iter()
    let record: Vec<RecordOption> = res.rows_de_stream().try_collect().await?;
    log::debug!("fetched record {:?}", record);
    taos.clean()?;
    Ok(())
}
#[tokio::test]
async fn de_all_option_with_json_tag_struct() -> Result<()> {
    let taos = &TaosWrapper::new()?;
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
    let res = taos.query("select * from stb1").await?;
    // block.rows_iter()
    let record: Vec<RecordOptionWithJsonTag> = res.rows_de_stream().try_collect().await?;
    log::debug!("fetched record {:?}", record);
    taos.clean()?;
    Ok(())
}

#[tokio::test]
async fn de_string() -> Result<()> {
    let taos = &TaosWrapper::new()?;
    let res = taos.query("select server_version() as version").await?;
    use futures::StreamExt;

    let version: String = res.rows_de_stream().next().await.expect("select version")?;
    println!("version: {version}");
    Ok(())
}

#[tokio::test]
async fn de_wrapper_struct() -> Result<()> {
    let taos = TaosOptions::new().build()?;
    let res = taos.query("select server_version() as version").await?;
    use futures::StreamExt;

    #[derive(::serde::Deserialize, Debug)]
    struct Version(String);
    let version: Version = res.rows_de_stream().next().await.expect("select version")?;
    println!("version: {:?}", version);
    Ok(())
}

#[tokio::test]
async fn de_named_struct() -> Result<()> {
    let taos = TaosOptions::new().build()?;

    macro_rules! de {
        ($taos:expr, $sql:expr) => {
            $taos
                .query($sql)
                .await?
                .rows_de_stream()
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
#[tokio::test]
async fn de_vec() -> Result<()> {
    let taos = TaosOptions::new().build()?;

    #[derive(::serde::Deserialize, Debug)]
    struct Database {
        name: String,
        created_time: NaiveDateTime,
        ntables: u64,
        precision: Precision,
    }
    let db: Vec<Database> = taos
        .query(format!("show databases"))
        .await?
        .rows_de_stream()
        .try_collect()
        .await?;
    println!("db: {:?}", db);
    Ok(())
}
