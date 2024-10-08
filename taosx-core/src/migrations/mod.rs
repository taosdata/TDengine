use std::{fmt::Display, path::Path};

use anyhow::Context;
use privileges::Privilege;
use serde::{Deserialize, Serialize};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use users::User;

mod privileges;
mod users;

#[derive(Debug, Deserialize, Serialize)]
pub struct Options {
    pub passwords: bool,
    pub privileges: bool,
    pub whitelist: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            passwords: true,
            privileges: true,
            whitelist: true,
        }
    }
}

#[derive(Debug)]
enum Op {
    /// Tilde requirements allow the patch part of the semver version (the third number) to increase.
    ///
    /// For example, ~3.3.3.0 allows 3.3.3.1, 3.3.3.2, and so on, but not 3.3.4.0.
    ///
    /// * `~I.J.K.L` — equivalent to `>=I.J.K.L, <I.J.(K+1).0`
    /// * `~I.J.K` — equivalent to `>=I.J.K, <I.(J+1).0`
    /// * `~I.J` — equivalent to `=I.J`
    /// * `~I` — equivalent to `=I`
    Tilde = 0,
}

#[derive(Debug)]
struct Comparator {
    #[allow(dead_code)]
    pub op: Op,
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub build: u64,
    #[allow(dead_code)]
    pub pre: Option<String>,
}

impl Comparator {
    pub fn tilde_from(version: &str) -> Self {
        let item = version.splitn(5, ['.', '-']).collect::<Vec<_>>();
        Self {
            op: Op::Tilde,
            major: item.get(1).and_then(|s| s.parse().ok()).unwrap_or_default(),
            minor: item.get(1).and_then(|s| s.parse().ok()).unwrap_or_default(),
            patch: item.get(2).and_then(|s| s.parse().ok()).unwrap_or_default(),
            build: item.get(3).and_then(|s| s.parse().ok()).unwrap_or_default(),
            pre: item.get(4).map(|pre| pre.to_string()),
        }
    }

    pub fn matches(&self, rhs: &str) -> bool {
        let rhs = Self::tilde_from(rhs);
        if self.major != rhs.major {
            return false;
        }
        if self.minor != rhs.minor {
            return false;
        }
        if self.patch != rhs.patch {
            return false;
        }
        if self.build > rhs.build {
            return false;
        }
        true
    }
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Export {
    version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    users: Vec<User>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    privileges: Vec<Privilege>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ApplyFail {
    user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    privilege: Option<String>,
    reason: String,
}

impl ApplyFail {
    fn new(user: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            privilege: None,
            reason: reason.into(),
        }
    }
    fn privilege(
        user: impl Into<String>,
        privilege: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            privilege: Some(privilege.into()),
            reason: reason.into(),
        }
    }
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ApplySuccess {
    passwords: u32,
    privileges: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ApplyFails {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    passwords: Vec<ApplyFail>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    privileges: Vec<ApplyFail>,
}
impl ApplyFails {
    pub fn is_empty(&self) -> bool {
        self.passwords.is_empty() && self.privileges.is_empty()
    }

    pub fn len(&self) -> usize {
        self.passwords.len() + self.privileges.len()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ApplyResults {
    success: ApplySuccess,
    fails: Option<ApplyFails>,
}

impl Display for ApplyResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.fails.is_none() {
            if self.success.passwords == 0 && self.success.privileges == 0 {
                write!(f, "No users and privileges imported")?;
                return Ok(());
            }
            f.write_str("Success:")?;
            if self.success.passwords != 0 {
                write!(f, " {} users", self.success.passwords)?;
            }
            if self.success.privileges == 0 {
                write!(f, " {} privileges", self.success.privileges)?;
            }
            write!(f, " imported successfully")?;
            return Ok(());
        }

        write!(f, "Partially failed:")?;
        if self.success.passwords != 0 {
            write!(f, " {} users", self.success.passwords)?;
        }
        if self.success.privileges != 0 {
            write!(f, " {} privileges", self.success.privileges)?;
        }
        if self.success.passwords != 0 || self.success.privileges != 0 {
            write!(f, " imported successfully, ")?;
        }
        if let Some(fails) = &self.fails {
            if fails.len() == 1 {
                writeln!(f, "1 item failed: ")?;
            } else {
                writeln!(f, "{} items failed: ", fails.len())?;
            }

            if !fails.passwords.is_empty() {
                for fail in &fails.passwords {
                    writeln!(f, "- User `{}` import fails: {}, ", fail.user, fail.reason)?;
                }
            }
            if !fails.privileges.is_empty() {
                for fail in &fails.privileges {
                    writeln!(
                        f,
                        "- User `{}` privilege `{}` import fails: {}, ",
                        fail.user,
                        fail.privilege.as_deref().unwrap_or(""),
                        fail.reason
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl Export {
    #[cfg(test)]
    pub async fn revoke(&self, conn: &taos::Taos) -> anyhow::Result<()> {
        for privilege in &self.privileges {
            let sql = privilege.to_sql_revoke();
            conn.exec(&sql).await?;
        }

        for user in &self.users {
            let sql = user.to_sql_drop();
            conn.exec(&sql).await?;
        }
        Ok(())
    }

    fn patch_with(&mut self, options: &Options) {
        if !options.passwords {
            self.users.clear();
        } else if !options.whitelist {
            self.users.iter_mut().for_each(|user| {
                user.allowed_host.take();
            });
        }
        if !options.privileges {
            self.privileges.clear();
        }
    }

    pub async fn apply_to(&mut self, to: &Dsn, options: &Options) -> anyhow::Result<ApplyResults> {
        self.patch_with(options);
        let builder = TaosBuilder::from_dsn(to)?;
        let conn = builder.pool()?.get().await?;
        self.apply(&conn).await
    }
    pub async fn apply(&self, conn: &taos::Taos) -> anyhow::Result<ApplyResults> {
        let version = conn.server_version().await?;

        let comparator = Comparator::tilde_from(&self.version);
        if !comparator.matches(&version) {
            return Err(anyhow::anyhow!(
                "Version mismatch, expected {} compatible version, got {}",
                self.version,
                version
            ));
        }

        let mut success = ApplySuccess {
            passwords: 0,
            privileges: 0,
        };

        let mut fails = ApplyFails {
            passwords: vec![],
            privileges: vec![],
        };

        for user in &self.users {
            if let Err(err) = conn.exec_many(user.to_sqls(true)).await {
                fails
                    .passwords
                    .push(ApplyFail::new(&user.name, format!("{err:#}")));
            } else {
                success.passwords += 1;
            }
        }
        for privilege in &self.privileges {
            if let Err(err) = conn.exec(privilege.to_sql()).await {
                fails.privileges.push(ApplyFail::privilege(
                    &privilege.user_name,
                    privilege.target(),
                    err.message().to_string(),
                ));
            } else {
                success.privileges += 1;
            }
        }

        Ok(ApplyResults {
            success,
            fails: if fails.is_empty() { None } else { Some(fails) },
        })
    }
}

impl Options {
    pub fn new(passwords: bool, privileges: bool, whitelist: bool) -> Self {
        Self {
            passwords,
            privileges,
            whitelist,
        }
    }

    fn load_from_file(&self, path: impl AsRef<Path>) -> anyhow::Result<Export> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Open file {} error", path.display()))?;
        let mut export: Export = serde_json::from_str(&content)
            .with_context(|| format!("Loading from file {} error", path.display()))?;

        if !self.passwords {
            export.users.clear();
        } else if !self.whitelist {
            export.users.iter_mut().for_each(|user| {
                user.allowed_host.take();
            });
        }
        if !self.privileges {
            export.privileges.clear();
        }
        Ok(export)
    }

    async fn get_users_and_privileges(&self, from: &Dsn) -> anyhow::Result<Export> {
        let builder = TaosBuilder::from_dsn(from)?;
        let version = builder.server_version().await?.to_string();
        let pool = builder.pool()?;
        let conn = pool.get().await?;

        let users = if self.passwords {
            let users = users::get_user_passwords(&conn).await?;
            if !self.whitelist {
                users
                    .into_iter()
                    .map(|mut user| {
                        user.allowed_host.take();
                        user
                    })
                    .collect()
            } else {
                users
            }
        } else {
            vec![]
        };
        let privileges = if self.privileges {
            privileges::get_user_privileges(&conn).await?
        } else {
            vec![]
        };

        let export = Export {
            version,
            users,
            privileges,
        };
        Ok(export)
    }
}

/// Export users and privileges from a cluster.
///
/// The `from` parameter is the DSN of the cluster to export from.
/// The `to` parameter is the path to export to(a JSON file or a directory).
pub async fn export(from: &Dsn, to: impl AsRef<Path>, options: &Options) -> anyhow::Result<()> {
    let to = to.as_ref();
    let to = if to.is_dir() {
        to.join("exported-privileges.json")
    } else {
        to.to_path_buf()
    };

    let export = options.get_users_and_privileges(from).await?;
    std::fs::write(
        to,
        serde_json::to_string_pretty(&export).expect("deserialize should always success"),
    )?;
    Ok(())
}

/// Import users and privileges to a cluster.
///
/// The `from` parameter is the path to import from(a JSON file or a directory).
/// The `to` parameter is the DSN of the cluster to import to.
pub async fn import(
    from: impl AsRef<Path>,
    to: &Dsn,
    options: &Options,
) -> anyhow::Result<ApplyResults> {
    let from = from.as_ref();
    let fi = if from.is_dir() {
        from.join("exported-privileges.json")
    } else {
        from.to_path_buf()
    };
    let export = options.load_from_file(&fi)?;

    let builder = TaosBuilder::from_dsn(to)?;
    let pool = builder.pool()?;
    let conn = pool.get().await?;

    export.apply(&conn).await
}

/// Migrate users and privileges from a cluster to another.
///
/// The `from` parameter is the DSN of the cluster to migrate from.
/// The `to` parameter is the DSN of the cluster to migrate to.
/// The `options` parameter is the options to control the migration scope.
pub async fn migrate(from: &Dsn, to: &Dsn, options: &Options) -> anyhow::Result<ApplyResults> {
    let export = options.get_users_and_privileges(from).await?;

    let builder = TaosBuilder::from_dsn(to).context("Migration target connection error")?;
    let pool = builder
        .pool()
        .context("Migration target connection error")?;
    let conn = pool
        .get()
        .await
        .context("Migration target connection error")?;
    export.apply(&conn).await
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use taos::{AsyncQueryable, AsyncTBuilder, TaosBuilder};

    #[test]
    fn test_comparator() {
        let version = "3.1.1.0";
        let comparator = super::Comparator::tilde_from(version);
        dbg!(&comparator);
        assert!(comparator.matches("3.1.1.0"));
        assert!(comparator.matches("3.1.1.1"));
        assert!(comparator.matches("3.1.1.2"));
        assert!(comparator.matches("3.1.1"));
        assert!(comparator.matches("3.1.1.0.20240608"));
        assert!(comparator.matches("3.1.1.0.alpha"));
        assert!(!comparator.matches("3.1.2.0"));
        assert!(!comparator.matches("3.1.0.0"));
    }

    #[tokio::test]
    async fn test_export_import() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("taosx_core=trace".parse()?),
            )
            .try_init();
        let pool = TaosBuilder::from_dsn("taos://")?.pool()?;
        let conn = pool.get().await?;

        let _ = conn
            .exec_many([
                "DROP USER `_xTest`",
                "DROP TOPIC `_xTopicT1`",
                "DROP TOPIC `_xTopicT2`",
                "DROP DATABASE `_xTest`",
                "DROP DATABASE `_xTest2`",
            ])
            .await;
        conn.exec_many([
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
            "ALTER USER `_xTest` ENABLE 0",
        ])
        .await?;

        let from = taos::Dsn::from_str("taos://").unwrap();
        let to = taos::Dsn::from_str("taos://").unwrap();

        let all = super::Options::new(true, true, true);
        {
            // Partial export tests
            let users_only = super::Options::new(true, false, false);
            let d1 = users_only.get_users_and_privileges(&from).await?;
            assert!(d1.privileges.is_empty(), "Users only without whitelist");
            assert!(
                d1.users.iter().all(|u| u.allowed_host.is_none()),
                "Users only without whitelist"
            );
            let users_with_whitelist = super::Options::new(true, false, true);
            let d2 = users_with_whitelist.get_users_and_privileges(&from).await?;
            assert!(d2.privileges.is_empty(), "Users only with whitelist");
            assert!(
                d2.users.iter().all(|u| u.allowed_host.is_some()),
                "Users only with whitelist"
            );
            let privileges_only = super::Options::new(false, true, false);
            let d3 = privileges_only.get_users_and_privileges(&from).await?;
            assert!(d3.users.is_empty(), "Privileges only");
        }

        let tmp = tempfile::TempDir::new()?;
        let path = tmp.path().join("exported-privileges.json");
        super::export(&from, &path, &all).await?;

        // First import should always fail.
        let result = super::import(&path, &to, &all).await?;
        dbg!(&result);
        assert_eq!(result.success.passwords, 0, "First import should fail");

        // We revoke the privileges and users to make the second import successful.
        let export = all.load_from_file(&path)?;
        export.revoke(&conn).await?;

        export
            .users
            .iter()
            .filter(|user| user.name == "_xTest")
            .for_each(|user| {
                let sqls = user.to_sqls(true);
                println!("sqls: {:?}", sqls);
                debug_assert!(sqls[1].contains("ENABLE 0"));
            });

        let result = super::import(&path, &to, &all).await?;
        dbg!(&result);
        assert!(result.fails.is_none(), "Second import should success");
        export.revoke(&conn).await?;

        conn.exec_many([
            "DROP TOPIC `_xTopicT1`",
            "DROP TOPIC `_xTopicT2`",
            "DROP DATABASE `_xTest`",
            "DROP DATABASE `_xTest2`",
        ])
        .await?;
        Ok(())
    }
}
