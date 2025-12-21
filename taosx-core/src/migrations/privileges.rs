use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, NoneAsEmptyString};
use taos::{AsyncFetchable, AsyncQueryable, TryStreamExt};

#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Privilege {
    pub user_name: String,
    pub privilege: String,
    pub db_name: String,
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
    table_name: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
    condition: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
    notes: Option<String>,
}

impl Privilege {
    pub fn target(&self) -> String {
        if let Some(table_name) = &self.table_name {
            format!("{} on `{}`.`{}`", self.privilege, self.db_name, table_name)
        } else {
            format!("{} on `{}`", self.privilege, self.db_name)
        }
    }
    pub fn to_sql(&self) -> String {
        let mut sql = format!("GRANT {} ON `{}`", self.privilege, self.db_name);

        if let Some(table_name) = &self.table_name {
            sql.push_str(&format!(".`{}`", table_name));
        }

        if let Some(condition) = &self.condition {
            let target = format!(
                "`{}`.`{}`.",
                self.db_name,
                self.table_name
                    .as_deref()
                    .expect("table_name should not be empty with condition")
            );
            let condition = condition.replace(&target, "");
            sql.push_str(&format!(" WITH {}", condition));
        }

        sql.push_str(&format!(" TO `{}`", self.user_name));
        sql
    }

    #[cfg(test)]
    pub fn to_sql_revoke(&self) -> String {
        let mut sql = format!("REVOKE {} ON `{}`", self.privilege, self.db_name);

        if let Some(table_name) = &self.table_name {
            sql.push_str(&format!(".`{}`", table_name));
        }

        if let Some(condition) = &self.condition {
            let target = format!(
                "`{}`.`{}`.",
                self.db_name,
                self.table_name
                    .as_deref()
                    .expect("table_name should not be empty with condition")
            );
            let condition = condition.replace(&target, "");
            sql.push_str(&format!(" WITH {}", condition));
        }

        sql.push_str(&format!(" FROM `{}`", self.user_name));
        sql
    }
}

pub async fn get_user_privileges(conn: &taos::Taos) -> Result<Vec<Privilege>, taos::Error> {
    let sql = "select * from information_schema.ins_user_privileges";

    let mut set = conn.query(sql).await?;
    set.deserialize()
        .try_filter(|privilege: &Privilege| std::future::ready(privilege.user_name != "root"))
        .try_collect::<Vec<_>>()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::{AsyncTBuilder, TaosBuilder};

    #[test]
    fn test_to_sql_database_privilege() {
        let privilege = Privilege {
            user_name: "testuser".to_string(),
            privilege: "read".to_string(),
            db_name: "testdb".to_string(),
            table_name: None,
            condition: None,
            notes: None,
        };

        let sql = privilege.to_sql();
        assert_eq!(sql, "GRANT read ON `testdb` TO `testuser`");
    }

    #[test]
    fn test_to_sql_table_privilege() {
        let privilege = Privilege {
            user_name: "testuser".to_string(),
            privilege: "write".to_string(),
            db_name: "testdb".to_string(),
            table_name: Some("testtable".to_string()),
            condition: None,
            notes: None,
        };

        let sql = privilege.to_sql();
        assert_eq!(sql, "GRANT write ON `testdb`.`testtable` TO `testuser`");
    }

    #[test]
    fn test_to_sql_with_condition() {
        let privilege = Privilege {
            user_name: "testuser".to_string(),
            privilege: "read".to_string(),
            db_name: "mydb".to_string(),
            table_name: Some("mytable".to_string()),
            condition: Some("`mydb`.`mytable`.col1 = 'value'".to_string()),
            notes: None,
        };

        let sql = privilege.to_sql();
        assert_eq!(
            sql,
            "GRANT read ON `mydb`.`mytable` WITH col1 = 'value' TO `testuser`"
        );
    }

    #[test]
    fn test_to_sql_revoke_database() {
        let privilege = Privilege {
            user_name: "user1".to_string(),
            privilege: "all".to_string(),
            db_name: "db1".to_string(),
            table_name: None,
            condition: None,
            notes: None,
        };

        let sql = privilege.to_sql_revoke();
        assert_eq!(sql, "REVOKE all ON `db1` FROM `user1`");
    }

    #[test]
    fn test_to_sql_revoke_table() {
        let privilege = Privilege {
            user_name: "user2".to_string(),
            privilege: "read".to_string(),
            db_name: "db2".to_string(),
            table_name: Some("table2".to_string()),
            condition: None,
            notes: None,
        };

        let sql = privilege.to_sql_revoke();
        assert_eq!(sql, "REVOKE read ON `db2`.`table2` FROM `user2`");
    }

    #[test]
    fn test_to_sql_revoke_with_condition() {
        let privilege = Privilege {
            user_name: "user3".to_string(),
            privilege: "read".to_string(),
            db_name: "db3".to_string(),
            table_name: Some("table3".to_string()),
            condition: Some("`db3`.`table3`.tag1 = 1".to_string()),
            notes: None,
        };

        let sql = privilege.to_sql_revoke();
        assert_eq!(
            sql,
            "REVOKE read ON `db3`.`table3` WITH tag1 = 1 FROM `user3`"
        );
    }

    #[test]
    fn test_target_database() {
        let privilege = Privilege {
            user_name: "testuser".to_string(),
            privilege: "read".to_string(),
            db_name: "testdb".to_string(),
            table_name: None,
            condition: None,
            notes: None,
        };

        assert_eq!(privilege.target(), "read on `testdb`");
    }

    #[test]
    fn test_target_table() {
        let privilege = Privilege {
            user_name: "testuser".to_string(),
            privilege: "write".to_string(),
            db_name: "mydb".to_string(),
            table_name: Some("mytable".to_string()),
            condition: None,
            notes: None,
        };

        assert_eq!(privilege.target(), "write on `mydb`.`mytable`");
    }

    #[ignore]
    #[tokio::test]
    async fn test_privileges_roundtrip_with_taos() -> anyhow::Result<()> {
        use file_guard::Lock;
        use std::fs::OpenOptions;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("./tests/migrations.lock")?;

        let _lock = file_guard::lock(&mut file, Lock::Exclusive, 0, 1)?;

        let pool = TaosBuilder::from_dsn("taos://")?.pool()?;
        let conn = pool.get().await?;

        let privileges = super::get_user_privileges(&conn).await?;
        for p in privileges.iter().take(1) {
            let grant = p.to_sql();
            let revoke = p.to_sql_revoke();
            let _ = conn.exec(&revoke).await;
            let _ = conn.exec(&grant).await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_user_privileges_with_taos() -> anyhow::Result<()> {
        use file_guard::Lock;
        use std::fs::OpenOptions;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("./tests/migrations.lock")?;

        let _lock = file_guard::lock(&mut file, Lock::Exclusive, 0, 1)?;

        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("taosx_core=trace".parse()?),
            )
            .try_init();
        let pool = TaosBuilder::from_dsn("taos://")?.pool()?;
        let conn = pool.get().await?;

        let _ = conn.exec("DROP USER `_xTest`").await;
        let _ = conn
            .exec_many([
                "DROP TOPIC IF EXISTS `_xTopicT1`",
                "DROP TOPIC IF EXISTS `_xTopicT2`",
                "DROP DATABASE IF EXISTS `_xTest`",
                "DROP DATABASE IF EXISTS `_xTest2`",
            ])
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let mut retries = 0;
        while let Err(err) = conn
            .exec_many([
                "CREATE DATABASE IF NOT EXISTS `_xTest`",
                "CREATE TABLE IF NOT EXISTS `_xTest`.test (ts timestamp, v1 int)",
                "CREATE DATABASE IF NOT EXISTS `_xTest2`",
                "CREATE TABLE IF NOT EXISTS `_xTest2`.meters (ts timestamp, v1 int) tags(t1 int)",
                "CREATE TABLE IF NOT EXISTS `_xTest2`.`cT1` using `_xTest2`.meters tags(1)",
                "CREATE TABLE IF NOT EXISTS `_xTest2`.`nT1` (ts timestamp, v1 int)",
                "CREATE TOPIC IF NOT EXISTS `_xTopicT1` as SELECT * FROM `_xTest`.`test`",
                "CREATE TOPIC IF NOT EXISTS `_xTopicT2` as database `_xTest2`",
                "CREATE USER `_xTest` PASS 'taosdata'",
                "GRANT all ON `_xTest` TO `_xTest`",
                "GRANT read ON `_xTest2`.* TO `_xTest`",
                "GRANT read ON `_xTest2`.meters WITH (t1 = 1) TO `_xTest`",
                "GRANT subscribe ON `_xTopicT1` TO `_xTest`",
                "GRANT subscribe ON `_xTopicT2` TO `_xTest`",
            ])
            .await
        {
            dbg!(&err);
            if retries > 5 {
                return Err(err.into());
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            retries += 1;
        }
        let privileges = super::get_user_privileges(&conn)
            .await?
            .into_iter()
            .filter(|p| p.user_name == "_xTest")
            .collect::<Vec<_>>();
        dbg!(&privileges);
        for p in &privileges {
            let grant = p.to_sql();
            let revoke = p.to_sql_revoke();
            dbg!(&p, &p.target(), &grant, &revoke);
            conn.exec(&revoke).await?;
            if let Err(err) = conn.exec(&grant).await {
                dbg!(&err);
                if err.to_string().contains("User already have this") {
                    continue;
                } else {
                    return Err(err.into());
                }
            }
        }
        let p2 = super::get_user_privileges(&conn)
            .await?
            .into_iter()
            .filter(|p| p.user_name == "_xTest")
            .collect::<Vec<_>>();
        dbg!(&p2);

        assert_eq!(privileges, p2);
        for p in &p2 {
            let revoke = p.to_sql_revoke();
            conn.exec(&revoke).await?;
        }
        conn.exec_many([
            "DROP USER `_xTest`",
            "DROP TOPIC `_xTopicT1`",
            "DROP TOPIC `_xTopicT2`",
            "DROP DATABASE `_xTest`",
            "DROP DATABASE `_xTest2`",
        ])
        .await?;

        Ok(())
    }
}
