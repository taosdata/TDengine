use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, NoneAsEmptyString};
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

        if with_whitelist {
            if let Some(allowed_host) = &self.allowed_host {
                create.push_str(" HOST ");
                create.push_str(
                    &allowed_host
                        .split(',')
                        .map(|host| format!("'{}'", host))
                        .join(","),
                );
            }
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

    #[tokio::test]
    async fn test_user_full() -> anyhow::Result<()> {
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

        assert_eq!(users, p2);
        for p in &p2 {
            let revoke = p.to_sql_drop();
            conn.exec(&revoke).await?;
        }
        Ok(())
    }
}
