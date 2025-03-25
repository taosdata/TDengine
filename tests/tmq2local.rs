#[cfg(test)]
mod test_tmq_to_local {
    use anyhow::bail;
    use assert_cmd::Command;
    use opendal::Entry;
    use std::env;
    use std::path::Path;
    use taos::{
        AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, Taos, TaosBuilder, TryStreamExt,
    };
    use taosx_core::s3::{S3Config, S3Loader};
    use taosx_core::tmq_to_local::conf::BackupConfigBuilder;

    const VGROUPS: usize = 10;
    const ROWS: usize = 10;

    /// # Case
    /// 创建一个数据库，写入数据，用 taosx 备份到本地，然后恢复到新的数据库
    /// # Example
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' cargo nextest run test_backup_and_restore_few_rows_with_taos --nocapture --retries 0
    /// ```
    #[tokio::test]
    async fn test_backup_and_restore_few_rows_with_taos() -> anyhow::Result<()> {
        // 初始化环境变量
        let taos_addr = env::var("TAOS_ADDR").unwrap_or("tmq://".to_string());
        let taosx_cmd = env::var("TAOSX_CMD").unwrap_or("taosx".to_string());
        const SRC_DB: &str = "backup_few_rows_src";
        const DST_DB: &str = "backup_few_rows_dst";
        dbg!(&taos_addr);

        // 在 TDengine 准备数据
        let taos = TaosBuilder::from_dsn(taos_addr.clone().into_dsn()?)?
            .build()
            .await?;
        drop_topic_and_database(&taos, SRC_DB).await?;
        drop_topic_and_database(&taos, DST_DB).await?;
        write_few_rows(&taos, SRC_DB, ROWS).await?;

        let backup_dir = env::var("BACKUP_DIR")
            .ok()
            .map(|p| Path::new(&p).to_path_buf())
            .unwrap_or_else(|| tempfile::TempDir::new().unwrap().into_path());

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
        let from = from.into_dsn()?;
        let to = to.into_dsn()?;
        let backup_config = BackupConfigBuilder::new(None, &from, &to).build().await?;
        let from = format!(
            "local:{}?topic={}&db_name={SRC_DB}&db_sql=CREATE DATABASE `{SRC_DB}` VGROUPS {VGROUPS}&to=now",
            backup_dir.to_string_lossy().into_owned(),
            &backup_config.topic,
        );
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
        drop_topic_and_database(&taos, SRC_DB).await?;
        drop_topic_and_database(&taos, DST_DB).await?;

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
    async fn test_backup_and_restore_with_s3() -> anyhow::Result<()> {
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

            // 在 TDengine 准备数据
            let taos = TaosBuilder::from_dsn(taos_addr.clone().into_dsn()?)?
                .build()
                .await?;
            drop_topic_and_database(&taos, SRC_DB).await?;
            drop_topic_and_database(&taos, DST_DB).await?;
            write_few_rows(&taos, SRC_DB, ROWS).await?;

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
            drop_topic_and_database(&taos, SRC_DB).await?;
            drop_topic_and_database(&taos, DST_DB).await?;

            // 清理 S3
        }

        Ok(())
    }

    async fn drop_topic_and_database(taos: &Taos, db_name: &str) -> anyhow::Result<()> {
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
            let mut count = 0;
            loop {
                count += 1;
                if let Err(err) = taos.exec(format!("DROP TOPIC IF EXISTS `{t}`")).await {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    if count < 3 {
                        continue;
                    }
                    bail!("failed to drop topic: {:#}", err);
                }
                break;
            }
        }
        // drop database
        taos.exec(format!("DROP DATABASE IF EXISTS `{db_name}`"))
            .await?;

        Ok(())
    }

    async fn write_few_rows(taos: &Taos, db_name: &str, rows: usize) -> anyhow::Result<()> {
        taos.exec_many(vec![
            format!("CREATE DATABASE `{db_name}` VGROUPS {VGROUPS}"),
            format!("CREATE TABLE `{db_name}`.stb (ts TIMESTAMP, f1 INT) TAGS(t1 INT)"),
        ])
        .await?;

        for i in 1..=rows {
            taos.exec(format!(
                "INSERT INTO `{db_name}`.t{i} USING `{db_name}`.stb TAGS({i}) VALUES (now, {i})"
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
