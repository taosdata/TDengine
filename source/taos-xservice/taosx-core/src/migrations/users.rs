use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_with::{NoneAsEmptyString, serde_as, skip_serializing_none};
use taos::{AsyncFetchable, AsyncQueryable, Itertools, TryStreamExt};

#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct User {
    pub(super) name: String,
    #[serde(rename = "super")]
    is_super: u8,
    enable: u8,
    sysinfo: u8,
    createdb: u8,
    encrypted_pass: String,
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
    pub(super) allowed_host: Option<String>,
}

impl User {
    pub fn to_sqls(&self, with_whitelist: bool) -> Vec<String> {
        let mut create = format!(
            "CREATE USER `{}` PASS '{}' SYSINFO {} CREATEDB {} IS_IMPORT 1",
            self.name, self.encrypted_pass, self.sysinfo, self.createdb
        );

        if with_whitelist && let Some(allowed_host) = &self.allowed_host {
            create.push_str(" HOST ");
            create.push_str(
                &allowed_host
                    .split(',')
                    .map(|host| format!("'{}'", host))
                    .join(","),
            );
        }
        let cap = match (self.is_super, self.enable) {
            (1, 0) => 3,
            (0, 0) | (1, 1) => 2,
            _ => 1,
        };
        let mut sqls = Vec::with_capacity(cap);
        sqls.push(create);

        if self.is_super == 1 {
            sqls.push(format!("ALTER USER `{}` SUPER 1", self.name));
        }
        if self.enable == 0 {
            sqls.push(format!("ALTER USER `{}` ENABLE 0", self.name));
        }
        sqls
    }

    #[cfg(test)]
    pub fn to_sql_drop(&self) -> String {
        format!("DROP USER `{}`", self.name)
    }
}

pub async fn get_user_passwords(conn: &taos::Taos) -> Result<Vec<User>, taos::Error> {
    let sql = "select * from information_schema.ins_users_full";

    let error = |err: taos::Error| {
        tracing::error!(error = format!("{err:#}"), "Failed to get user passwords");
        let code = *err.code().deref();
        if matches!(code, 0x2662 | 0x2603 | 0x039A) {
            // 0x2662: the table does not exist
            // 0x2603: the table does not exist
            // 0x039A: invalid system table name
            err.context("Current version is not supported, please upgrade to a later one.")
        } else {
            err.context("Failed to get user passwords")
        }
    };
    let mut set = conn.query(sql).await.map_err(error)?;
    set.deserialize()
        .try_filter(|obj: &User| std::future::ready(obj.name != "root"))
        .try_collect::<Vec<_>>()
        .await
        .map_err(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::{AsyncTBuilder, TaosBuilder};

    #[test]
    fn test_to_sqls_basic_user() {
        let user = User {
            name: "testuser".to_string(),
            is_super: 0,
            enable: 1,
            sysinfo: 1,
            createdb: 0,
            encrypted_pass: "encrypted123".to_string(),
            allowed_host: None,
        };

        let sqls = user.to_sqls(false);
        assert_eq!(sqls.len(), 1);
        assert_eq!(
            sqls[0],
            "CREATE USER `testuser` PASS 'encrypted123' SYSINFO 1 CREATEDB 0 IS_IMPORT 1"
        );
    }

    #[test]
    fn test_to_sqls_super_user() {
        let user = User {
            name: "adminuser".to_string(),
            is_super: 1,
            enable: 1,
            sysinfo: 1,
            createdb: 1,
            encrypted_pass: "pass456".to_string(),
            allowed_host: None,
        };

        let sqls = user.to_sqls(false);
        assert_eq!(sqls.len(), 2);
        assert!(sqls[0].contains("CREATE USER `adminuser`"));
        assert_eq!(sqls[1], "ALTER USER `adminuser` SUPER 1");
    }

    #[test]
    fn test_to_sqls_disabled_user() {
        let user = User {
            name: "disabled".to_string(),
            is_super: 0,
            enable: 0,
            sysinfo: 0,
            createdb: 0,
            encrypted_pass: "pass789".to_string(),
            allowed_host: None,
        };

        let sqls = user.to_sqls(false);
        assert_eq!(sqls.len(), 2);
        assert!(sqls[0].contains("CREATE USER `disabled`"));
        assert_eq!(sqls[1], "ALTER USER `disabled` ENABLE 0");
    }

    #[test]
    fn test_to_sqls_super_disabled_user() {
        let user = User {
            name: "superdisabled".to_string(),
            is_super: 1,
            enable: 0,
            sysinfo: 1,
            createdb: 1,
            encrypted_pass: "passabc".to_string(),
            allowed_host: None,
        };

        let sqls = user.to_sqls(false);
        assert_eq!(sqls.len(), 3);
        assert!(sqls[0].contains("CREATE USER `superdisabled`"));
        assert_eq!(sqls[1], "ALTER USER `superdisabled` SUPER 1");
        assert_eq!(sqls[2], "ALTER USER `superdisabled` ENABLE 0");
    }

    #[test]
    fn test_to_sqls_with_whitelist_single_host() {
        let user = User {
            name: "whitelistuser".to_string(),
            is_super: 0,
            enable: 1,
            sysinfo: 1,
            createdb: 0,
            encrypted_pass: "pass000".to_string(),
            allowed_host: Some("192.168.1.1".to_string()),
        };

        let sqls = user.to_sqls(true);
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].contains("HOST '192.168.1.1'"));
    }

    #[test]
    fn test_to_sqls_with_whitelist_multiple_hosts() {
        let user = User {
            name: "multihost".to_string(),
            is_super: 0,
            enable: 1,
            sysinfo: 1,
            createdb: 0,
            encrypted_pass: "passmulti".to_string(),
            allowed_host: Some("192.168.1.1,192.168.1.2,10.0.0.1".to_string()),
        };

        let sqls = user.to_sqls(true);
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].contains("HOST '192.168.1.1','192.168.1.2','10.0.0.1'"));
    }

    #[test]
    fn test_to_sqls_without_whitelist_ignores_host() {
        let user = User {
            name: "nohostuser".to_string(),
            is_super: 0,
            enable: 1,
            sysinfo: 1,
            createdb: 0,
            encrypted_pass: "passnohost".to_string(),
            allowed_host: Some("192.168.1.1".to_string()),
        };

        let sqls = user.to_sqls(false);
        assert_eq!(sqls.len(), 1);
        assert!(!sqls[0].contains("HOST"));
    }

    #[test]
    fn test_to_sql_drop() {
        let user = User {
            name: "dropme".to_string(),
            is_super: 0,
            enable: 1,
            sysinfo: 1,
            createdb: 0,
            encrypted_pass: "passdrop".to_string(),
            allowed_host: None,
        };

        let drop_sql = user.to_sql_drop();
        assert_eq!(drop_sql, "DROP USER `dropme`");
    }

    // todo: open after TD-38169 fixed
    #[ignore]
    #[tokio::test]
    async fn test_user_full_with_taos() -> anyhow::Result<()> {
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("./tests/migrations.lock")?;

        file.lock()?;

        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("taosx_core=trace".parse()?)
                    .add_directive("taos_query=trace".parse()?)
                    .add_directive("taos_optin=trace".parse()?),
            )
            .try_init();
        let pool = TaosBuilder::from_dsn("taos://")?.pool()?;
        let conn = pool.get().await?;

        let _ = conn.exec_many(["DROP USER `_xTest`"]).await;
        conn.exec_many(["CREATE USER `_xTest` PASS 'taosdata'"])
            .await?;
        let users = super::get_user_passwords(&conn)
            .await?
            .into_iter()
            .filter(|p| p.name == "_xTest")
            .collect::<Vec<_>>();
        dbg!(&users);
        for p in &users {
            let with_whitelist = p.to_sqls(true);
            let without_whitelist = p.to_sqls(false);
            let drop = p.to_sql_drop();
            dbg!(&p, &with_whitelist, &drop);
            conn.exec(&drop).await?;
            conn.exec_many(&with_whitelist).await?;
            conn.exec(&drop).await?;
            conn.exec_many(&without_whitelist).await?;
        }
        let p2 = super::get_user_passwords(&conn)
            .await?
            .into_iter()
            .filter(|p| p.name == "_xTest")
            .collect::<Vec<_>>();
        dbg!(&p2);

        // FIXME: broken by https://jira.taosdata.com:18080/browse/TD-34250
        // assert_eq!(users, p2);
        if users != p2 {
            dbg!("TD-34250 has not been fixed yet", &users);
        }

        for p in &p2 {
            let revoke = p.to_sql_drop();
            conn.exec(&revoke).await?;
        }
        Ok(())
    }
}
