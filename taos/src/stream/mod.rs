#[cfg(test)]
mod test {

    use std::{ffi::CString, ops::Deref};

    use chrono::NaiveDateTime;

    use crate::*;

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
            simple_logger::init().expect("start logger");
            let taos = TaosOptions::new().build()?;
            use rand::Rng;
            let mut rng = rand::thread_rng();

            use faker_rand::lorem::Word;
            let db = rng.gen::<Word>().to_string();
            taos.exec_sync(format!("create database if not exists {db}",))?;
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
        let record: Record = res.rows_de_stream().next().await.unwrap()?;
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
        let res = taos.query("select server_version() as version").await?;
        use futures::StreamExt;

        #[derive(::serde::Deserialize, Debug)]
        struct Version {
            version: String,
        };
        let version: Version = res.rows_de_stream().next().await.expect("select version")?;
        println!("version: {:?}", version);
        Ok(())
    }
    #[tokio::test]
    async fn de_show_databases() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let res = taos.query("show databases").await?;
        use futures::StreamExt;

        #[derive(::serde::Deserialize, Debug)]
        struct Database {
            name: String,
            created_time: i64,
        };
        let db: Database = res
            .rows_de_stream()
            .next()
            .await
            .expect("there's no database")?;
        println!("db: {:?}", db);
        Ok(())
    }
}
