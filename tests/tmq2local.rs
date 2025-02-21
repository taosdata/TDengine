#[cfg(test)]
mod test_tmq_to_local {
    use anyhow::bail;
    use assert_cmd::Command;
    use std::env;
    use taos::{
        AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, Taos, TaosBuilder, TryStreamExt,
    };
    use taosx_core::taoz::ZFile;
    use taosx_core::utils::parse_duration;

    /// # Case
    /// 创建一个数据库，写入 5 行数据，用 taosx 备份到本地，然后恢复到新的数据库
    /// # Example
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' cargo nextest run test_backup_and_restore_5rows_with_taos --nocapture
    /// ```
    #[tokio::test]
    async fn test_backup_and_restore_5rows_with_taos() -> anyhow::Result<()> {
        // 初始化环境变量
        let taos_addr = env::var("TAOS_ADDR").unwrap_or("tmq://".to_string());
        const SRC_DB: &str = "backup_5rows_src";
        const DST_DB: &str = "backup_5rows_dst";
        const ROWS: usize = 5;
        dbg!(&taos_addr);

        // 在 TDengine 准备数据
        let taos = TaosBuilder::from_dsn(taos_addr.clone().into_dsn()?)?
            .build()
            .await?;
        drop_topic_and_database(&taos, SRC_DB).await?;
        drop_topic_and_database(&taos, DST_DB).await?;
        write_few_rows(&taos, SRC_DB, ROWS).await?;

        let backup_path = tempfile::TempDir::new()?;
        // 备份 5 行数据
        let data_dir = tempfile::tempdir()?;
        let mut cmd = Command::cargo_bin("taosx")?;
        cmd.arg("run")
            .arg("-f")
            .arg(format!("{taos_addr}/{SRC_DB}"))
            .arg("-t")
            .arg(format!("local:{}", backup_path.path().to_str().unwrap()))
            .env("TAOSX_DATA_DIR", data_dir.path())
            .assert()
            .success();

        // 检查本地的备份文件
        assert!(backup_path.path().exists());
        let assert = Command::new("ls").arg(backup_path.path()).assert();
        let files = String::from_utf8(assert.get_output().stdout.clone())?;
        let files = files.lines().collect::<Vec<_>>();
        assert_eq!(files.len(), 3);
        for f in files.iter() {
            assert!(f.ends_with(".z"));
        }

        // 恢复备份的数据
        let file_name = files[0];
        let (topic, to, _, _) = ZFile::parse_file_name(file_name)?;
        taos.exec(format!("CREATE DATABASE `{DST_DB}`")).await?;
        let data_dir = tempfile::tempdir()?;
        let mut cmd = Command::cargo_bin("taosx")?;
        cmd.arg("run")
            .arg("-f")
            .arg(format!(
                "local:{}?topic={}&db_name={}&db_sql=CREATE DATABASE `{}` VGROUPS 3&to={}",
                backup_path.path().to_str().unwrap(),
                topic,
                SRC_DB,
                SRC_DB,
                to.to_rfc3339().as_str()
            ))
            .arg("-t")
            .arg(format!("{taos_addr}/{DST_DB}"))
            .env("TAOSX_DATA_DIR", data_dir.path())
            .assert()
            .success();

        // 检查 TDengine 的数据
        assert_database_rows(&taos, SRC_DB, DST_DB, ROWS).await?;

        // 清理 TDengine
        drop_topic_and_database(&taos, SRC_DB).await?;
        drop_topic_and_database(&taos, DST_DB).await?;

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
            format!("CREATE DATABASE `{db_name}` VGROUPS 3"),
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
        assert_eq!(src.len(), dst.len());
        for i in 0..rows {
            assert_eq!(src[i].0, dst[i].0);
            assert_eq!(src[i].1, dst[i].1);
        }

        Ok(())
    }

    /// # Case
    /// 测试备份数据使用 S3 转储。用例使用以下环境变量：
    /// * TAOS_ADDR: TDengine 的连接地址。默认为 tmq://
    /// * BACKUP_RETENTION_PERIOD: 本地备份文件的保留时长，所有早于 now - backup_retention_period 的备份文件都会上传 S3。默认值为 0
    /// * BACKUP_RETENTION_SIZE: 本地备份文件的保留大小，只保留最新的 backup_retention_size 个备份文件。默认为 0
    /// * S3_ENDPOINT: S3 的 endpoint。如果不填，用例空跑
    /// * S3_ACCESS_KEY_ID: S3 的密钥key。默认为 minioadmin
    /// * S3_SECRET_ACCESS_KEY: S3 的密钥。默认为 minioadmin
    /// * S3_REGION: 区域。默认为 None
    /// * S3_BUCKET: 存储桶。默认为 test
    /// * S3_OBJECT_PREFIX: 对象前缀，类似于文件夹。默认为 None
    /// # Example
    /// 全部备份文件上传到 S3
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' S3_ENDPOINT='http://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture
    /// ```
    /// 本地备份文件不超过 5 个文件
    /// ```
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_SIZE=5 S3_ENDPOINT='http://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture
    /// ```
    /// 本地备份文件最多存 10 分钟
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_PERIOD=10m S3_ENDPOINT='http://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture
    /// ```
    /// 本地备份文件最多存 10 分钟，且不超过 5 个文件
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' BACKUP_RETENTION_PERIOD=10m BACKUP_RETENTION_SIZE=5 S3_ENDPOINT='http://192.168.2.139:9000' cargo nextest run test_backup_and_restore_with_s3 --nocapture
    /// ```
    #[tokio::test]
    async fn test_backup_and_restore_with_s3() -> anyhow::Result<()> {
        if let Ok(s3_endpoint) = env::var("S3_ENDPOINT") {
            // 初始化环境变量
            let taos_addr = env::var("TAOS_ADDR").unwrap_or("tmq://".to_string());
            let backup_retention_period = env::var("BACKUP_RETENTION_PERIOD")
                .ok()
                .and_then(|v| parse_duration(&v).ok());
            let backup_retention_size = env::var("BACKUP_RETENTION_SIZE")
                .ok()
                .and_then(|v| v.parse::<u64>().ok());
            let s3_access_key_id = env::var("S3_ACCESS_KEY_ID").unwrap_or("minioadmin".to_string());
            let s3_secret_access_key =
                env::var("S3_SECRET_ACCESS_KEY").unwrap_or("minioadmin".to_string());
            let s3_region = env::var("S3_REGION").ok();
            let s3_bucket = env::var("S3_BUCKET").unwrap_or("test".to_string());
            let s3_object_prefix = env::var("S3_OBJECT_PREFIX").ok();
            dbg!(&taos_addr);
            dbg!(&backup_retention_period);
            dbg!(&backup_retention_size);
            dbg!(&s3_endpoint);
            dbg!(&s3_access_key_id);
            dbg!(&s3_secret_access_key);
            dbg!(&s3_region);
            dbg!(&s3_bucket);
            dbg!(&s3_object_prefix);
            const SRC_DB: &str = "backup_s3_src";
            const DST_DB: &str = "backup_s3_dst";

            // 在 TDengine 准备数据
            let taos = TaosBuilder::from_dsn(taos_addr.clone().into_dsn()?)?
                .build()
                .await?;
            drop_topic_and_database(&taos, SRC_DB).await?;
            drop_topic_and_database(&taos, DST_DB).await?;
            write_few_rows(&taos, SRC_DB, 5).await?;

            // 在 S3 上初始化 bucket 和 object_prefix

            // 创建备份任务

            // 创建恢复任务

            // 检查 TDengine 的数据
            // assert_database_rows(&taos, SRC_DB, DST_DB, 5).await?;

            // 清理 TDengine
            drop_topic_and_database(&taos, SRC_DB).await?;
            drop_topic_and_database(&taos, DST_DB).await?;

            // 清理 S3
        }

        Ok(())
    }
}
