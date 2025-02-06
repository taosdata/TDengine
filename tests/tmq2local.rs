#[cfg(test)]
mod test_tmq_to_local {
    use anyhow::bail;
    use assert_cmd::assert::OutputAssertExt;
    use assert_cmd::Command;
    use taos::{
        AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, Taos, TaosBuilder, TryStreamExt,
    };
    use taosx_core::taoz::ZFile;

    /// 备份数据库到本地，备份 5 行数据，然后恢复备份的数据
    #[tokio::test]
    async fn test_td31475_5rows_with_taos() -> anyhow::Result<()> {
        // given
        const ADDR: &str = "tmq://";
        const SRC_DB: &str = "td31475_backup_5rows";
        const DST_DB: &str = "td31475_backup_5rows_target";

        // prepare
        let taos = TaosBuilder::from_dsn(ADDR)?.build().await?;
        drop_topic_and_database(&taos, SRC_DB).await?;
        drop_topic_and_database(&taos, DST_DB).await?;
        taos.exec_many(vec![
            format!("CREATE DATABASE `{SRC_DB}` VGROUPS 3"),
            format!("CREATE TABLE `{SRC_DB}`.stb (ts TIMESTAMP, f1 INT) TAGS(t1 INT)"),
            format!("INSERT INTO `{SRC_DB}`.t1 USING `{SRC_DB}`.stb TAGS(1) VALUES (now, 1)"),
            format!("INSERT INTO `{SRC_DB}`.t2 USING `{SRC_DB}`.stb TAGS(2) VALUES (now, 2)"),
            format!("INSERT INTO `{SRC_DB}`.t3 USING `{SRC_DB}`.stb TAGS(3) VALUES (now, 3)"),
            format!("INSERT INTO `{SRC_DB}`.t4 USING `{SRC_DB}`.stb TAGS(4) VALUES (now, 4)"),
            format!("INSERT INTO `{SRC_DB}`.t5 USING `{SRC_DB}`.stb TAGS(5) VALUES (now, 5)"),
        ])
        .await?;
        let backup_path = tempfile::TempDir::new()?;

        let temp_data_dir = std::env::temp_dir();

        // when：备份 5 行数据
        let mut cmd = Command::cargo_bin("taosx")?;
        cmd.arg("run")
            .arg("-f")
            .arg(format!("{ADDR}/{SRC_DB}"))
            .arg("-t")
            .arg(format!("local:{}", backup_path.path().to_str().unwrap()))
            .env("TAOSX_DATA_DIR", &temp_data_dir)
            .assert()
            .success();
        // then
        assert!(backup_path.path().exists());
        let assert = Command::new("ls").arg(backup_path.path()).assert();
        let files = String::from_utf8(assert.get_output().stdout.clone())?;
        let files = files.lines().collect::<Vec<_>>();
        assert_eq!(files.len(), 3);
        for f in files.iter() {
            assert!(f.ends_with(".z"));
        }

        // when：恢复备份的数据
        let file_name = files[0];
        let (topic, to, _, _) = ZFile::parse_file_name(file_name)?;
        taos.exec(format!("CREATE DATABASE `{DST_DB}`")).await?;
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
            .arg(format!("{ADDR}/{DST_DB}"))
            .env("TAOSX_DATA_DIR", &temp_data_dir)
            .assert()
            .success();
        // then
        let tables: Vec<(String, i32)> = taos
            .query(format!(
                "select tbname,f1 from `{DST_DB}`.stb ORDER BY tbname"
            ))
            .await?
            .deserialize()
            .try_collect()
            .await?;
        assert_eq!(tables.len(), 5);
        assert_eq!(tables[0].0, "t1");
        assert_eq!(tables[0].1, 1);
        assert_eq!(tables[1].0, "t2");
        assert_eq!(tables[1].1, 2);
        assert_eq!(tables[2].0, "t3");
        assert_eq!(tables[2].1, 3);
        assert_eq!(tables[3].0, "t4");
        assert_eq!(tables[3].1, 4);
        assert_eq!(tables[4].0, "t5");
        assert_eq!(tables[4].1, 5);

        // clean-up resources
        drop_topic_and_database(&taos, SRC_DB).await?;
        drop_topic_and_database(&taos, DST_DB).await?;

        Ok(())
    }

    /// 备份数据库到本地，备份 1 亿行数据，然后恢复备份的数据
    #[tokio::test]
    #[ignore]
    async fn test_td31475_1e8rows_with_taos() -> anyhow::Result<()> {
        // given
        let addr = "tmq+ws://192.168.0.201:6041";
        let db = "td31475_backup_1e8rows";
        let backup_path = tempfile::TempDir::new()?;

        // prepare
        let taos = TaosBuilder::from_dsn(format!("{addr}/").into_dsn()?)?
            .build()
            .await?;
        drop_topic_and_database(&taos, db).await?;
        insert_1e8rows(db).await?;

        // when
        let mut cmd = Command::cargo_bin("taosx")?;
        cmd.arg("run")
            .arg("-f")
            .arg(format!("{addr}/{db}"))
            .arg("-t")
            .arg(format!("local:{}", backup_path.path().to_str().unwrap()))
            .assert()
            .success();

        // then
        assert!(backup_path.path().exists());
        let assert = Command::new("ls").arg(backup_path.path()).assert();
        let files = String::from_utf8(assert.get_output().stdout.clone())?;
        let files = files.lines().collect::<Vec<_>>();
        dbg!(&files);

        // clean-up resources
        drop_topic_and_database(&taos, db).await?;

        Ok(())
    }

    pub async fn drop_topic_and_database(taos: &Taos, db_name: &str) -> anyhow::Result<()> {
        let topics: Vec<String> = taos
            .query(format!(
                "select topic_name from information_schema.ins_topics where db_name = '{db_name}'"
            ))
            .await?
            .deserialize()
            .try_collect()
            .await?;
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
        taos.exec(format!("DROP DATABASE IF EXISTS `{db_name}`"))
            .await?;

        Ok(())
    }

    pub async fn insert_1e8rows(db_name: &str) -> anyhow::Result<()> {
        Command::new("taosBenchmark")
            .args(["-y", "-d", db_name, "-n", "10000", "-t", "10000"])
            .output()
            .expect("failed to execute process")
            .assert()
            .append_context("taosBenchmark", "insert with benchmark tool")
            .success();

        Ok(())
    }
}
