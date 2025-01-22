#[cfg(test)]
mod simulator {
    use anyhow::bail;
    use chrono::{DateTime, Local, Utc};
    use itertools::Itertools;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use std::env;
    use std::env::VarError;
    use taos::{AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};
    use taosx_core::utils::parse_duration;
    use taosx_core::utils::table_meta::TableMetaQueryBuilder;

    // TDengine address
    const TAOS_ADDR: &str = "TAOS_ADDR";
    // 预先执行的 SQL
    const WRITE_TAOS_PRE_SQLS: &str = "WRITE_TAOS_PRE_SQLS";
    // 写入的表名
    const WRITE_TAOS_STABLE: &str = "WRITE_TAOS_STABLE";
    // 子表的数量
    const WRITE_TAOS_SUB_TABLES: &str = "WRITE_TAOS_SUB_TABLES";
    // 写入的行数
    const WRITE_TAOS_TOTAL_ROWS: &str = "WRITE_TAOS_TOTAL_ROWS";
    // 写入的时间戳开始值
    const WRITE_TAOS_TS_START: &str = "WRITE_TAOS_TS_START";
    // 写入的时间间隔
    const WRITE_TAOS_INTERVAL: &str = "WRITE_TAOS_INTERVAL";

    pub fn addr() -> String {
        env::var(TAOS_ADDR).unwrap_or("taos://".to_string())
    }

    /// # Example：空跑
    /// ```
    /// cargo nextest run test_write_with_taos --nocapture
    /// ```
    /// # Example：只建库建超级表，不写数据
    /// ```
    /// TAOS_ADDR='taos+ws://192.168.0.201:6041' WRITE_TAOS_PRE_SQLS='drop database if exists `ABC`;create database if not exists `ABC`;use `ABC`;create stable if not exists `Stb`(ts timestamp, f1 int) tags(t1 int);' cargo nextest run test_write_with_taos --nocapture
    /// ```
    /// # Example：建库建表，建子表
    /// ```
    /// TAOS_ADDR='taos+ws://192.168.0.201:6041' WRITE_TAOS_PRE_SQLS='drop database if exists `ABC`;create database if not exists `ABC`;use `ABC`;create stable if not exists `Stb`(ts timestamp, f1 int) tags(t1 int);' WRITE_TAOS_STABLE='`ABC`.`Stb`' WRITE_TAOS_SUB_TABLES=10 cargo nextest run test_write_with_taos --nocapture
    /// ```
    /// # Example：建库建表，建子表，写数据
    /// ```
    /// TAOS_ADDR='taos+ws://192.168.0.201:6041' WRITE_TAOS_PRE_SQLS='drop database if exists `ABC`;create database if not exists `ABC`;use `ABC`;create stable if not exists `Stb`(ts timestamp, f1 int) tags(t1 int);' WRITE_TAOS_STABLE='`ABC`.`Stb`' WRITE_TAOS_SUB_TABLES=10 WRITE_TAOS_TOTAL_ROWS=100 WRITE_TAOS_TS_START='2025-01-21T00:00:00Z' cargo nextest run test_write_with_taos --nocapture
    /// ```
    /// # Example：建库建表，建子表，写数据，间隔1s
    /// ```
    /// TAOS_ADDR='taos+ws://192.168.0.201:6041' WRITE_TAOS_PRE_SQLS='drop database if exists `ABC`;create database if not exists `ABC`;use `ABC`;create stable if not exists `Stb`(ts timestamp, f1 int) tags(t1 int);' WRITE_TAOS_STABLE='`ABC`.`Stb`' WRITE_TAOS_SUB_TABLES=10 WRITE_TAOS_TOTAL_ROWS=100 WRITE_TAOS_TS_START='2025-01-21T00:00:00Z' WRITE_TAOS_INTERVAL=1s cargo nextest run test_write_with_taos --nocapture
    /// ```
    /// # Example：建子表，写数据，间隔1s
    /// ```
    /// TAOS_ADDR='taos+ws://192.168.0.201:6041' WRITE_TAOS_STABLE='`ABC`.`Stb`' WRITE_TAOS_SUB_TABLES=10 WRITE_TAOS_TOTAL_ROWS=100 WRITE_TAOS_TS_START='2025-01-21T00:00:00Z' cargo nextest run  test_write_with_taos --nocapture
    /// ```
    #[tokio::test]
    async fn test_write_with_taos() -> anyhow::Result<()> {
        // 执行 PRE_SQL
        let sqls = env::var(WRITE_TAOS_PRE_SQLS).ok();
        if let Some(sqls) = sqls {
            let sqls = sqls
                .split(";")
                .filter(|s| !s.is_empty())
                .map(|s| format!("{};", s.trim()))
                .collect::<Vec<String>>();
            dbg!(addr());
            let dsn = addr().into_dsn()?;
            let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
            for sql in sqls.iter() {
                dbg!(sql);
                taos.exec(sql).await?;
            }
        }

        if let Ok(stable) = env::var(WRITE_TAOS_STABLE)
            .map_err(|_| anyhow::anyhow!("missing env {}", WRITE_TAOS_STABLE))
        {
            let (db_name, tb_name) = parse_stable_name(&stable)?;
            let dsn = format!("{}/{}", addr(), db_name).into_dsn()?;
            dbg!(dsn.to_string());

            // connect
            let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;
            let mut rand = rand::thread_rng();

            // 建子表
            let meta_querier = TableMetaQueryBuilder::new(&dsn)?.build().await?;
            let stable_meta = meta_querier
                .super_table_meta(tb_name.as_str())?
                .ok_or(anyhow::anyhow!("stable: {} not found", tb_name))?;
            let db = stable_meta.db_name.clone();
            let stable = stable_meta.tbname.clone();
            let tables = env::var(WRITE_TAOS_SUB_TABLES)
                .map(|s| s.parse::<u64>())?
                .unwrap_or(0);
            for i in 0..tables {
                let tags = stable_meta
                    .tags
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|t| rand_value(&mut rand, t.r#type.as_str()))
                    .join(",");
                let sql = format!(
                    "create table if not exists `{db}`.`tb{i}` using `{db}`.`{stable}` tags({tags})",
                    i = i + 1,
                    tags = tags
                );
                dbg!(&sql);
                taos.exec(sql).await?;
            }

            // 写数据
            if let Err(VarError::NotPresent) = env::var(WRITE_TAOS_TOTAL_ROWS) {
                return Ok(());
            }
            if let Ok(rows) = env::var(WRITE_TAOS_TOTAL_ROWS).map(|s| s.parse::<u64>())? {
                let mut ts = env::var(WRITE_TAOS_TS_START)
                    .map(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .map(|dt| dt.with_timezone(&Utc))
                            .map(|dt| dt.timestamp())
                    })
                    .unwrap_or_else(|_| Ok(Utc::now().timestamp()))
                    .map_err(|err| {
                        anyhow::anyhow!(format!("invalid {}, err: {:?}", WRITE_TAOS_TS_START, err))
                    })?;

                for _ in 0..rows {
                    let table_idx = rand.gen_range(0..tables) + 1;
                    let values = stable_meta
                        .columns
                        .as_ref()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .filter(|(idx, c)| !(idx == &0usize && c.r#type == "TIMESTAMP"))
                        .map(|(_idx, c)| rand_value(&mut rand, c.r#type.as_str()))
                        .join(",");
                    let sql = format!(
                        "insert into `{db}`.`tb{table_idx}` values(\"{}\",{values})",
                        DateTime::from_timestamp(ts, 0)
                            .unwrap()
                            .with_timezone(&Local)
                            .to_rfc3339()
                    );
                    dbg!(&sql);
                    taos.exec(sql).await?;
                    ts += 1;

                    if let Err(VarError::NotPresent) = env::var(WRITE_TAOS_INTERVAL) {
                        continue;
                    }
                    if let Ok(interval) =
                        env::var(WRITE_TAOS_INTERVAL).map(|s| parse_duration(&s))?
                    {
                        tokio::time::sleep(interval).await;
                    }
                }
            }
        }
        Ok(())
    }

    fn parse_stable_name(tb_name: &str) -> anyhow::Result<(String, String)> {
        let parts = tb_name.split(".").collect_vec();
        if parts.len() != 2 {
            bail!(
                "invalid table name: {}, e.g. `DB_NAME`.`TABLE_NAME`",
                tb_name
            );
        }
        Ok((
            parts[0].to_string().replace("`", ""),
            parts[1].to_string().replace("`", ""),
        ))
    }

    fn rand_value(rand: &mut ThreadRng, t: &str) -> String {
        let val_type = t.to_uppercase();
        match val_type.as_str() {
            "TINYINT" => {
                let i: i8 = rand.gen_range(i8::MIN..i8::MAX);
                i.to_string()
            }
            "TINYINT UNSIGNED" => {
                let i: u8 = rand.gen_range(u8::MIN..u8::MAX);
                i.to_string()
            }
            "SMALLINT" => {
                let i: i16 = rand.gen_range(i16::MIN..i16::MAX);
                i.to_string()
            }
            "SMALLINT UNSIGNED" => {
                let i: u16 = rand.gen_range(u16::MIN..u16::MAX);
                i.to_string()
            }
            "INT" => {
                let i: i32 = rand.gen_range(i32::MIN..i32::MAX);
                i.to_string()
            }
            "INT UNSIGNED" => {
                let i: u32 = rand.gen_range(u32::MIN..u32::MAX);
                i.to_string()
            }
            "BIGINT" => {
                let i: i64 = rand.gen_range(i64::MIN..i64::MAX);
                i.to_string()
            }
            "BIGINT UNSIGNED" => {
                let i: u64 = rand.gen_range(u64::MIN..u64::MAX);
                i.to_string()
            }
            "FLOAT" => {
                let f: f32 = rand.gen_range(f32::MIN..f32::MAX);
                f.to_string()
            }
            "DOUBLE" => {
                let f: f64 = rand.gen_range(f64::MIN..f64::MAX);
                f.to_string()
            }
            "BOOL" => {
                let b: bool = rand.gen();
                b.to_string()
            }
            "TIMESTAMP" => "now".to_string(),
            _ => {
                if val_type.starts_with("VARCHAR") || val_type.starts_with("NCHAR") {
                    let len = val_type
                        .chars()
                        .filter(|c| c.is_ascii_digit())
                        .collect::<String>()
                        .parse::<usize>()
                        .unwrap_or(0);
                    let s = rand
                        .sample_iter(&rand::distributions::Alphanumeric)
                        .take(len)
                        .collect_vec();
                    String::from_utf8(s).unwrap_or("NULL".to_string())
                } else {
                    "NULL".to_string()
                }
            }
        }
    }
}
