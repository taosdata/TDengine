use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, NoneAsEmptyString};
use taos::{AsyncFetchable, AsyncQueryable, TryStreamExt};

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
    pub fn to_sql(&self, with_whitelist: bool) -> String {
        let mut sql = format!(
            "CREATE USER `{}` PASS '{}' SYSINFO {} CREATEDB {} IS_IMPORT 1",
            self.name, self.encrypted_pass, self.sysinfo, self.createdb
        );

        if with_whitelist {
            if let Some(allowed_host) = &self.allowed_host {
                sql.push_str(" HOST ");
                allowed_host.split(',').for_each(|host| {
                    sql.push_str(&format!("'{}'", host));
                });
            }
        }
        sql
    }

    #[cfg(test)]
    pub fn to_sql_drop(&self) -> String {
        format!("DROP USER `{}`", self.name)
    }
}

pub async fn get_user_passwords(conn: &taos::Taos) -> Result<Vec<User>, taos::Error> {
    let sql = "select * from information_schema.ins_users_full";

    let mut set = conn.query(sql).await?;
    set.deserialize()
        .try_filter(|obj: &User| std::future::ready(obj.name != "root"))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| {
            tracing::error!(error = format!("{err:#}"), "Failed to get user passwords");
            let code = *err.code().deref();
            if matches!(code, 0x2662 | 0x2603 | 0x039A) {
                err.context("Current version is not supported, please upgrade to a later one.")
            } else {
                err.context("Failed to get user passwords")
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::{AsyncTBuilder, TaosBuilder};

    #[tokio::test]
    async fn test_user_full() -> anyhow::Result<()> {
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
            let with_whitelist = p.to_sql(true);
            let without_whitelist = p.to_sql(false);
            let drop = p.to_sql_drop();
            dbg!(&p, &with_whitelist, &drop);
            conn.exec(&drop).await?;
            conn.exec(&with_whitelist).await?;
            conn.exec(&drop).await?;
            conn.exec(&without_whitelist).await?;
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
