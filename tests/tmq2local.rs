#[cfg(test)]
mod test_tmq_to_local {
    use anyhow::bail;
    use assert_cmd::Command;
    use std::env;
    use taos::{
        AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, Taos, TaosBuilder, TryStreamExt,
    };
    use taosx_core::taoz::ZFile;

    /// # 用例
    /// 创建一个名称为 backup_5rows 的数据库，写入 5 行数据，用 taosx 备份到本地，然后恢复到新的数据库 backup_5rows_target
    /// # Example
    /// ```shell
    /// TAOS_ADDR='tmq+ws://192.168.0.201:6041' cargo nextest run test_backup_and_restore_5rows_with_taos --nocapture
    /// ```
    #[tokio::test]
    async fn test_backup_and_restore_5rows_with_taos() -> anyhow::Result<()> {
        // given
        let addr = env::var("TAOS_ADDR").unwrap_or("tmq://".to_string());
        const SRC_DB: &str = "backup_5rows";
        const DST_DB: &str = "backup_5rows_target";

        // prepare
        let taos = TaosBuilder::from_dsn(addr.clone().into_dsn()?)?
            .build()
            .await?;
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

        // when：备份 5 行数据
        let mut cmd = Command::cargo_bin("taosx")?;
        cmd.arg("run")
            .arg("-f")
            .arg(format!("{addr}/{SRC_DB}"))
            .arg("-t")
            .arg(format!("local:{}", backup_path.path().to_str().unwrap()))
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
            .arg(format!("{addr}/{DST_DB}"))
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

    pub async fn drop_topic_and_database(taos: &Taos, db_name: &str) -> anyhow::Result<()> {
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
}
