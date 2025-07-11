#[cfg(test)]
mod test_tmq_to_local {
    use anyhow::Context;
    use assert_cmd::Command;
    use local_to_taos::local_to_taos;
    use opendal::Entry;
    use std::env;
    use std::path::Path;
    use taos::{
        AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, IntoDsn, Taos, TaosBuilder,
        TryStreamExt,
    };
    use taosx_core::s3::{S3Config, S3Loader};
    use taosx_core::utils::sql::connect_taos;
    use tmq_to_local::conf::BackupConfigBuilder;
    use tmq_to_local::tmq_to_local;
    use tokio_util::sync::CancellationToken;

    /// # description_cn
    /// 测试备份数据到本地
    /// # example
    /// 1. 备份到临时目录，默认先创建一个 test_backup 数据库，用 taosBenchmark 写入 1 亿行（ 1 万子表，1 万行/表），备份到本地
    /// ```shell
    /// BACKUP_DSN=tmq+ws://192.168.2.139:6041 cargo nextest run test_backup_with_taos --nocapture --retries 0
    /// ```
    /// 2. 备份指定的数据库，到临时目录
    /// ```shell
    /// BACKUP_DSN=tmq://192.168.2.139/log cargo nextest run test_backup_with_taos --nocapture --retries 0
    /// ```
    /// 3. 备份指定的数据库，到指定的目录
    /// ```shell
    /// BACKUP_DSN=tmq://192.168.2.139/log LOCAL_DIR=/opt cargo nextest run test_backup_with_taos --nocapture --retries 0
    /// ```
    /// 4. 用指定路径下的 taosx 命令备份
    /// ```shell
    /// TAOSX_CMD=./target/debug/taosx cargo nextest run test_backup_with_taos --nocapture --retries 0
    /// ```
    #[tokio::test]
    pub async fn test_backup_with_taos() -> anyhow::Result<()> {
        let mut backup_dsn = env::var("BACKUP_DSN")
            .unwrap_or("tmq://".to_string())
            .into_dsn()?;
        let taosx_cmd = env::var("TAOSX_CMD").unwrap_or("taosx".to_string());
        let dir = tempfile::tempdir()?;
        let backup_dir = match env::var("LOCAL_DIR").ok() {
            Some(p) => {
                let p = Path::new(&p);
                if !p.exists() {
                    std::fs::create_dir_all(p)?;
                }
                p.to_path_buf()
            }
            None => dir.path().to_path_buf(),
        };

        let taos = TaosBuilder::from_dsn(&backup_dsn)?
            .build()
            .await
            .context(format!("failed to create taos connect, dsn: {backup_dsn}"))?;

        const DEFAULT_DATABASE: &str = "test_backup";
        // 如果 tmq 中没有指定 database，则使用 test_backup
        let mut remove_db = false;
        let db_name = match backup_dsn.subject.as_deref() {
            None => {
                drop_database_and_related_topics(&taos, DEFAULT_DATABASE).await?;
                backup_dsn.subject = Some(DEFAULT_DATABASE.to_string());
                // 创建一个数据库：test_backup，并写入 1W 个表，每个表 100 条数据
                write_by_benchmark(&backup_dsn, 10000, 10000, false)
                    .await
                    .context(format!(
                        "failed to write by taosBenchmark, dsn: {backup_dsn}"
                    ))?;
                remove_db = true;

                DEFAULT_DATABASE
            }
            Some(db_name) => db_name,
        };

        // 执行备份：$TAOSX_CMD -f "$BACKUP_DSN" -t "local:$LOCAL_DIR"
        let mut taosx = Command::cargo_bin(&taosx_cmd)?;
        taosx
            .arg("run")
            .arg("-f")
            .arg(backup_dsn.to_string())
            .arg("-t")
            .arg(format!(
                "local:{}",
                backup_dir.to_string_lossy().into_owned()
            ))
            .env("TAOSX_DATA_DIR", backup_dir.as_path())
            .assert()
            .success();
        dbg!(taosx.get_args().collect::<Vec<_>>());

        // 检查备份目录
        assert!(backup_dir.exists());

        // 检查备份目录下的文件
        let files = list_local_files(backup_dir.as_path())?;
        let table_num: u32 = taos
            .query_one(format!(
                "select count(*) from information_schema.ins_tables where db_name = '{db_name}'"
            ))
            .await?
            .unwrap_or(0);
        if table_num == 0 {
            assert!(files.is_empty());
        } else {
            assert!(!files.is_empty());
        }

        if remove_db {
            drop_database_and_related_topics(&taos, db_name).await?;
        }

        Ok(())
    }

    /// # Case
    /// 创建一个数据库，写入数据，用 taosx 备份到本地，然后恢复到新的数据库。
    /// 测试流程：
    /// 1. 创建数据库 backup_few_rows_src 和 backup_few_rows_dst，vgroups = 10，在 backup_few_rows_src 中写入10行
    /// 2. 备份 backup_few_rows_src 到本地
    /// 3. 恢复 backup_few_rows_src 到 backup_few_rows_dst
    /// 4. 检查 backup_few_rows_dst 中的数据是否和 backup_few_rows_src 一致
    /// 5. 检查备份目录下的文件应该为 10 个，且文件名以 .z 结尾
    /// # Example
    /// 指定 TDengine 的地址，执行备份 + 恢复
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' cargo nextest run test_backup_and_restore_with_taos --nocapture --retries 0
    /// ```
    #[tokio::test]
    pub async fn test_backup_and_restore_with_taos() -> anyhow::Result<()> {
        // 初始化环境变量
        let taos_addr = env::var("TAOS_ADDR").unwrap_or("tmq://".to_string());
        let taosx_cmd = env::var("TAOSX_CMD").unwrap_or("taosx".to_string());
        let backup_dir = env::var("BACKUP_DIR")
            .ok()
            .map(|p| Path::new(&p).to_path_buf())
            .unwrap_or_else(|| tempfile::TempDir::new().unwrap().keep());
        const SRC_DB: &str = "backup_few_rows_src";
        const DST_DB: &str = "backup_few_rows_dst";
        const VGROUPS: usize = 10;
        const ROWS: usize = 10;
        dbg!(&taos_addr);

        // 在 TDengine 准备数据
        let taos = TaosBuilder::from_dsn(taos_addr.clone().into_dsn()?)?
            .build()
            .await?;
        drop_database_and_related_topics(&taos, SRC_DB).await?;
        drop_database_and_related_topics(&taos, DST_DB).await?;
        write_few_rows(&taos, SRC_DB, VGROUPS, ROWS).await?;

        // 备份数据
        let from = format!("{taos_addr}/{SRC_DB}");
        let to = format!("local:{}", backup_dir.to_string_lossy().into_owned());
        let mut cmd = Command::cargo_bin(&taosx_cmd)?;
        cmd.arg("run")
            .arg("-f")
            .arg(&from)
            .arg("-t")
            .arg(&to)
            .env("TAOSX_DATA_DIR", backup_dir.as_path())
            .assert()
            .success();
        dbg!(cmd.get_args().collect::<Vec<_>>());

        // 恢复数据
        let from = format!("local:{}?to=now", backup_dir.to_string_lossy().into_owned(),);
        let to = format!("{taos_addr}/{DST_DB}");
        taos.exec(format!("CREATE DATABASE `{DST_DB}`")).await?;
        let mut cmd = Command::cargo_bin(&taosx_cmd)?;
        cmd.arg("run")
            .arg("-f")
            .arg(&from)
            .arg("-t")
            .arg(&to)
            .env("TAOSX_DATA_DIR", backup_dir.as_path())
            .assert()
            .success();
        dbg!(cmd.get_args().collect::<Vec<_>>());

        // 检查 TDengine 的数据
        assert_database_rows(&taos, SRC_DB, DST_DB, ROWS).await?;

        // 检查本地的备份文件
        assert!(backup_dir.exists());
        let files = list_local_files(backup_dir.as_path())?;
        assert_eq!(files.len(), VGROUPS);
        for f in files.iter() {
            assert!(f.ends_with(".z"));
        }

        // 清理 TDengine
        drop_database_and_related_topics(&taos, SRC_DB).await?;
        drop_database_and_related_topics(&taos, DST_DB).await?;

        Ok(())
    }

    /// # Case
    /// 测试备份数据使用 S3 转储。用例使用以下环境变量：
    /// * TAOSX_CMD: taosx 的可执行文件路径。默认为 taosx
    /// * TAOS_ADDR: TDengine 的连接地址。默认为 tmq://
    /// * BACKUP_RETENTION_PERIOD: 本地备份文件的保留时长，所有早于 now - backup_retention_period 的备份文件都会上传 S3。
    /// * BACKUP_RETENTION_SIZE: 本地备份文件的保留大小，只保留最新的 backup_retention_size 个备份文件。
    /// * S3_ENDPOINT: S3 的 endpoint。如果不填，用例空跑
    /// * S3_ACCESS_KEY_ID: S3 的密钥key。默认为 minioadmin
    /// * S3_SECRET_ACCESS_KEY: S3 的密钥。默认为 minioadmin
    /// * S3_REGION: 区域。默认为 us-west-1
    /// * S3_BUCKET: 存储桶。默认为 taosx
    /// * S3_OBJECT_PREFIX: 对象前缀，类似于文件夹。默认为 backup/
    /// # Example
    /// 备份文件全部在本地
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' S3_ENDPOINT='https://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture --retries 0
    /// ```
    /// 本地备份文件不超过 5 个文件
    /// ```
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_SIZE=5 S3_ENDPOINT='https://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture --retries 0
    /// ```
    /// 本地备份文件最多存 1 分钟
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_PERIOD=1m S3_ENDPOINT='https://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture --retries 0
    /// ```
    /// 本地备份文件最多存 1 分钟，且不超过 5 个文件
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_PERIOD=1m BACKUP_RETENTION_SIZE=5 S3_ENDPOINT='https://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture --retries 0
    /// ```
    /// 备份文件全部上传到 S3
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_PERIOD=0 S3_ENDPOINT='https://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture --retries 0
    /// 或者
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_SIZE=0 S3_ENDPOINT='https://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture --retries 0
    /// ```
    #[tokio::test]
    pub async fn test_backup_and_restore_with_s3() -> anyhow::Result<()> {
        if let Ok(endpoint) = env::var("S3_ENDPOINT") {
            // 初始化环境变量
            let taosx_cmd = env::var("TAOSX_CMD").unwrap_or("taosx".to_string());
            let taos_addr = env::var("TAOS_ADDR").unwrap_or("tmq://".to_string());
            let backup_retention_period = env::var("BACKUP_RETENTION_PERIOD").ok();
            let backup_retention_size = env::var("BACKUP_RETENTION_SIZE").ok();
            let s3_config = S3Config {
                endpoint,
                access_key_id: env::var("S3_ACCESS_KEY_ID").unwrap_or("minioadmin".to_string()),
                secret_access_key: env::var("S3_SECRET_ACCESS_KEY")
                    .unwrap_or("minioadmin".to_string()),
                region: Some(
                    env::var("S3_REGION")
                        .ok()
                        .unwrap_or("us-west-1".to_string()),
                ),
                bucket: env::var("S3_BUCKET").unwrap_or("taosx".to_string()),
                prefix: Some(
                    env::var("S3_OBJECT_PREFIX")
                        .ok()
                        .unwrap_or("backup/".to_string()),
                ),
            };
            dbg!(&taos_addr);
            dbg!(&backup_retention_period);
            dbg!(&backup_retention_size);
            dbg!(&s3_config);
            const SRC_DB: &str = "backup_s3_src";
            const DST_DB: &str = "backup_s3_dst";
            const VGROUPS: usize = 10;
            const ROWS: usize = 10;

            // 在 TDengine 准备数据
            let taos = TaosBuilder::from_dsn(taos_addr.clone().into_dsn()?)?
                .build()
                .await?;
            drop_database_and_related_topics(&taos, SRC_DB).await?;
            drop_database_and_related_topics(&taos, DST_DB).await?;
            write_few_rows(&taos, SRC_DB, VGROUPS, ROWS).await?;

            // 在 S3 上初始化 bucket 和 object_prefix
            let op = s3_config.connect().await?;
            if let Some(prefix) = &s3_config.prefix {
                op.remove_all(prefix).await?;
                op.create_dir(prefix).await?;
            }

            // 创建备份任务
            let backup_path = tempfile::TempDir::new()?;
            // --from "tmq+ws://TAOS_HOST:6041/backup_s3_src"
            let from = format!("{taos_addr}/{SRC_DB}");
            // --to "local:/tmp?s3_enable=true&s3_endpoint=...&s3_access_key_id=...&s3_secret_access_key=...&s3_bucket=..."
            let mut to = format!("local:{}?", backup_path.path().to_str().unwrap());
            to = append_s3_config_on_url(to, &s3_config);
            if let Some(retention_period) = &backup_retention_period {
                to.push_str(&format!("&backup_retention_period={}", retention_period));
            }
            if let Some(retention_size) = &backup_retention_size {
                to.push_str(&format!("&backup_retention_size={}", retention_size));
            }
            let data_dir = tempfile::tempdir()?;
            dbg!(&from, &to, data_dir.path());
            let mut cmd = Command::cargo_bin(&taosx_cmd)?;
            cmd.arg("run")
                .arg("-f")
                .arg(&from)
                .arg("-t")
                .arg(&to)
                .env("TAOSX_DATA_DIR", data_dir.path())
                .assert()
                .success();

            // 创建恢复任务
            let from_dsn = from.clone().into_dsn()?;
            let to_dsn = to.clone().into_dsn()?;
            let backup_config = BackupConfigBuilder::new(None, &from_dsn, &to_dsn)
                .build()
                .await?;
            // --from "local:/tmp?to=now&topic=...&db_name=...&db_sql=...&s3_enable=true&s3_endpoint=..."
            let mut from = format!("local:{}?to=now", backup_path.path().to_str().unwrap());
            from.push_str(&format!("&topic={}", backup_config.topic));
            from.push_str(&format!("&db_name={}", SRC_DB));
            from.push_str(&format!(
                "&db_sql=CREATE DATABASE `{SRC_DB}` VGROUPS {VGROUPS}",
            ));
            from = append_s3_config_on_url(from, &s3_config);
            // --to "tmq+ws://TAOS_HOST:6041/DST_DB"
            let to = format!("{taos_addr}/{DST_DB}");
            taos.exec(format!("CREATE DATABASE `{DST_DB}`")).await?;
            dbg!(&from, &to, data_dir.path());
            let mut cmd = Command::cargo_bin(&taosx_cmd)?;
            cmd.arg("run")
                .arg("-f")
                .arg(from)
                .arg("-t")
                .arg(to)
                .env("TAOSX_DATA_DIR", data_dir.path())
                .assert()
                .success();

            // 检查 TDengine 的数据
            assert_database_rows(&taos, SRC_DB, DST_DB, 5).await?;

            // 检查本地备份目录和 S3 上的文件
            match (backup_retention_period, backup_retention_size) {
                // 备份文件全在本地
                (None, None) => {
                    // 本地文件数量为
                    let local_files = list_local_files(backup_path.path())?;
                    assert_eq!(local_files.len(), 10);
                    // S3 上的文件数量为 0
                    let s3_files = list_s3_files(&s3_config).await?;
                    assert_eq!(s3_files.len(), 0);
                }
                // 使用备份文件的保留数量
                (None, Some(retention_size)) => {
                    let size = retention_size.parse::<usize>()?;
                    // S3 上的文件数量为 0
                    let s3_files = list_s3_files(&s3_config).await?;
                    assert_eq!(s3_files.len(), VGROUPS - size);
                }
                // 使用备份文件的保留时长
                (Some(_retention_period), None) => {
                    // TODO: 检查 S3 上备份文件的数量
                }
                // 同时使用备份文件的保留时长和数量
                (Some(_retention_period), Some(_retention_size)) => {
                    // TODO: 检查 S3 上备份文件的数量
                }
            }

            // 清理 TDengine
            drop_database_and_related_topics(&taos, SRC_DB).await?;
            drop_database_and_related_topics(&taos, DST_DB).await?;

            // 清理 S3
        }

        Ok(())
    }

    /// # description_cn
    /// 测试通过备份和恢复来实现数据的复制
    /// 1. 创建数据库 tmq_replica_src 和 tmq_replica_dst
    /// 2. 创建一个线程，执行备份任务，备份 tmq_replica_src 到本地文件
    /// 3. 创建另一个线程，执行恢复任务，将本地文件恢复到 tmq_replica_dst
    /// 4. 写入数据到 tmq_replica_src
    /// 5. 检查 tmq_replica_src 中的数据是否和 tmq_replica_dst 一致
    /// 6. 输出性能指标：同步
    /// # example
    /// ```shell
    /// cargo nextest run test_replica_by_backup_and_restore_with_taos --nocapture --retries 0
    /// ```
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    pub async fn test_replica_by_backup_and_restore_with_taos() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();

        let host = env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = env::var("WS_ENABLE")
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        // backup file directory
        let temp_dir = tempfile::TempDir::new()?;
        let work_dir = match env::var("WORK_DIR").ok() {
            Some(p) => {
                let p = Path::new(&p);
                if !p.exists() {
                    std::fs::create_dir_all(p)?;
                }
                p.to_path_buf()
            }
            None => temp_dir.path().to_path_buf(),
        };
        let backup_dir = work_dir.join("backup");
        tokio::fs::create_dir_all(&backup_dir).await?;
        let restore_dir = work_dir.join("restore");
        tokio::fs::create_dir_all(&restore_dir).await?;
        const DB_SRC: &str = "tmq_replica_src";
        const DB_DST: &str = "tmq_replica_dst";
        const ROWS: u64 = 1000;

        let taos = connect_taos(&host, ws_enable).await?;

        // clean database and related topics
        drop_database_and_related_topics(&taos, DB_SRC).await?;
        drop_database_and_related_topics(&taos, DB_DST).await?;

        // create database and stable
        taos.exec_many(vec![
            format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
            format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
            format!("CREATE TABLE `{DB_SRC}`.meters (ts timestamp, val float) TAGS (id int)"),
        ])
        .await?;

        // create backup task
        let (from, to) = if ws_enable {
            let from =
                format!("tmq+ws://{host}:6041/{DB_SRC}?interval=1s&self.repeat=true").into_dsn()?;
            let to = format!(
                "local:{}?move.to={}",
                backup_dir.as_path().display(),
                restore_dir.as_path().display()
            )
            .into_dsn()?;
            (from, to)
        } else {
            let from = format!("tmq://{host}/{DB_SRC}?interval=1s&self.repeat=true").into_dsn()?;
            let to = format!(
                "local:{}?move.to={}",
                backup_dir.as_path().display(),
                restore_dir.as_path().display()
            )
            .into_dsn()?;
            (from, to)
        };
        let b_cancel = CancellationToken::new();
        let cancel = b_cancel.clone();
        let backup_handler =
            tokio::spawn(async move { tmq_to_local(None, from, to, cancel).await });

        // create restore task
        let (from, to) = if ws_enable {
            let from = format!("local:{}?to=now", restore_dir.as_path().display()).into_dsn()?;
            let to = format!("tmq+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!("local:{}?to=now", restore_dir.as_path().display()).into_dsn()?;
            let to = format!("tmq://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        let r_cancel = CancellationToken::new();
        let cancel = r_cancel.clone();
        let restore_handler =
            tokio::spawn(async move { local_to_taos(None, from, to, cancel).await });

        // write data to source database
        println!("write data to source database `{DB_SRC}` start");
        for i in 1..=ROWS {
            taos.exec(format!(
                "INSERT INTO `{DB_SRC}`.t{i} USING `{DB_SRC}`.meters TAGS({i}) VALUES(now, {i}.{i})"
            ))
            .await?;
        }
        println!("write data to source database `{DB_SRC}` stop");
        // wait for replica
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // stop tasks
        r_cancel.cancel();
        b_cancel.cancel();
        backup_handler.await??;
        restore_handler.await??;

        // 检查数据
        let count_src: u64 = taos
            .query_one(format!("select count(*) from `{DB_SRC}`.meters"))
            .await?
            .unwrap_or(0);
        let count_dst: u64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.meters"))
            .await?
            .unwrap_or(0);
        assert_eq!(count_src, ROWS);
        assert_eq!(count_dst, ROWS);

        // 清理
        temp_dir.close()?;
        // drop_database_and_related_topics(&taos, DB_SRC).await?;
        // drop_database_and_related_topics(&taos, DB_DST).await?;

        Ok(())
    }

    async fn drop_database_related_topics(taos: &Taos, db_name: &str) -> anyhow::Result<()> {
        let topics: Vec<String> = taos
            .query(format!(
                "select topic_name from information_schema.ins_topics where db_name = '{db_name}'"
            ))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        // drop topics
        for t in topics {
            taos.exec(format!("DROP TOPIC IF EXISTS force `{t}`"))
                .await?;
        }
        Ok(())
    }

    async fn drop_database_and_related_topics(taos: &Taos, db_name: &str) -> anyhow::Result<()> {
        // drop database related topics
        drop_database_related_topics(taos, db_name).await?;

        // drop database
        taos.exec(format!("DROP DATABASE IF EXISTS `{db_name}`"))
            .await?;

        Ok(())
    }

    /// 使用 taosBenchmark 工具写数据
    async fn write_by_benchmark(
        dsn: &Dsn,
        tables: usize,
        records: usize,
        no_drop: bool,
    ) -> anyhow::Result<()> {
        let mut taos_benchmark = Command::new("taosBenchmark");
        let mut args = vec!["-y".to_string()];

        match dsn.protocol.as_deref() {
            Some("ws") | Some("wss") | Some("http") | Some("https") => {
                args.push("-Z".to_string());
                args.push("WebSocket".to_string());
            }
            _ => {}
        }

        if let Some(addr) = dsn.addresses.first() {
            if let Some(host) = addr.host.as_deref() {
                args.push("-h".to_string());
                args.push(host.to_string());
            }
            if let Some(port) = addr.port {
                args.push("-P".to_string());
                args.push(port.to_string());
            }
        }
        if let Some(user) = dsn.username.as_deref() {
            args.push("-u".to_string());
            args.push(user.to_string());
        }
        if let Some(password) = dsn.password.as_deref() {
            args.push("-p".to_string());
            args.push(password.to_string());
        }
        if let Some(db_name) = dsn.subject.as_deref() {
            args.push("-d".to_string());
            args.push(db_name.to_string());
        }
        args.push("-t".to_string());
        args.push(tables.to_string());
        args.push("-n".to_string());
        args.push(records.to_string());

        // args.push("-s".to_string());
        // args.push(Utc::now().timestamp_millis().to_string());

        if no_drop {
            args.push("--nodrop".to_string());
        }
        taos_benchmark.args(args.as_slice()).assert().success();
        dbg!(taos_benchmark.get_args().collect::<Vec<_>>());

        Ok(())
    }

    async fn write_few_rows(
        taos: &Taos,
        db: &str,
        vgroups: usize,
        rows: usize,
    ) -> anyhow::Result<()> {
        taos.exec_many(vec![
            format!("CREATE DATABASE `{db}` VGROUPS {vgroups}"),
            format!("CREATE TABLE `{db}`.stb (ts TIMESTAMP, f1 INT) TAGS(t1 INT)"),
        ])
        .await?;

        for i in 1..=rows {
            taos.exec(format!(
                "INSERT INTO `{db}`.t{i} USING `{db}`.stb TAGS({i}) VALUES (now, {i})"
            ))
            .await?;
        }

        Ok(())
    }

    async fn assert_database_rows(
        taos: &Taos,
        src_db: &str,
        dst_db: &str,
        rows: usize,
    ) -> anyhow::Result<()> {
        let src: Vec<(String, i32)> = taos
            .query(format!(
                "select tbname,f1 from `{src_db}`.stb ORDER BY tbname"
            ))
            .await?
            .deserialize()
            .try_collect()
            .await?;
        let dst: Vec<(String, i32)> = taos
            .query(format!(
                "select tbname,f1 from `{dst_db}`.stb ORDER BY tbname"
            ))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        // 源和目的的数据应该一致
        assert_eq!(src.len(), dst.len());
        for i in 0..rows {
            assert_eq!(src[i].0, dst[i].0);
            assert_eq!(src[i].1, dst[i].1);
        }

        Ok(())
    }

    // 列出当前目录下的文件
    fn list_local_files(path: &Path) -> anyhow::Result<Vec<String>> {
        let mut files = vec![];
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_file() {
                if let Some(file_name) = p.file_name() {
                    files.push(file_name.to_string_lossy().to_string());
                }
            }
        }
        dbg!(&files);

        Ok(files)
    }

    async fn list_s3_files(s3_config: &S3Config) -> anyhow::Result<Vec<Entry>> {
        let loader = S3Loader::try_from(s3_config).await?;
        let prefix = s3_config.prefix.as_deref().unwrap_or("/");
        let uploaded = loader.list_dir(prefix).await?;

        Ok(uploaded)
    }

    fn append_s3_config_on_url(mut url: String, s3_config: &S3Config) -> String {
        url.push_str(&format!(
            "&s3_enable=true&s3_endpoint={}",
            urlencoding::encode(&s3_config.endpoint)
        ));
        url.push_str(&format!(
            "&s3_access_key_id={}",
            urlencoding::encode(&s3_config.access_key_id)
        ));
        url.push_str(&format!(
            "&s3_secret_access_key={}",
            urlencoding::encode(&s3_config.secret_access_key)
        ));
        url.push_str(&format!(
            "&s3_bucket={}",
            urlencoding::encode(&s3_config.bucket)
        ));
        if let Some(region) = &s3_config.region {
            url.push_str(&format!("&s3_region={}", urlencoding::encode(region)));
        }
        if let Some(prefix) = &s3_config.prefix {
            url.push_str(&format!(
                "&s3_object_prefix={}",
                urlencoding::encode(prefix)
            ));
        }

        url
    }
}
