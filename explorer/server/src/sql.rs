use std::sync::LazyLock;

use anyhow::Context;
use futures::TryStreamExt;
use regex::Regex;
use taos::{AsyncFetchable, AsyncQueryable, Dsn};
use tracing::instrument;

use crate::get_connection;

pub(crate) fn need_limit(sql: &str) -> bool {
    let sql = sql.trim().to_uppercase();
    if !sql.starts_with("SELECT") {
        return false;
    }
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"LIMIT\s+(\d+)(?:\s*(?:,\s*(\d+)|OFFSET\s+(\d+)))?\b").unwrap()
    });

    !RE.is_match(&sql)
}

#[instrument(skip_all)]
pub async fn query<T: serde::de::DeserializeOwned>(dsn: &Dsn, sql: &str) -> anyhow::Result<Vec<T>> {
    let conn = get_connection(dsn).await?;

    conn.query(sql)
        .await
        .with_context(|| format!("query sql `{sql}`"))?
        .deserialize::<T>()
        .try_collect::<Vec<_>>()
        .await
        .with_context(|| format!("deserialize fetch `{sql}` data error"))
}

#[instrument(skip_all)]
pub async fn query_one<T>(dsn: &Dsn, sql: &str) -> anyhow::Result<Option<T>>
where
    T: serde::de::DeserializeOwned + Send,
{
    let conn = get_connection(dsn).await?;
    conn.query_one(sql)
        .await
        .with_context(|| format!("query one item by sql {sql} error"))
}

#[instrument(skip_all)]
pub async fn exec(dsn: &Dsn, sql: &str) -> anyhow::Result<()> {
    let conn = get_connection(dsn).await?;
    conn.exec(sql)
        .await
        .with_context(|| format!("exec sql {sql}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_has_limit_test() {
        assert!(need_limit("select * from test;"));
        assert!(!need_limit("select * from test limit 5, 2;"));
        assert!(!need_limit("select * from test limit 2 offset 5;"));
        assert!(!need_limit("select * from test limit 5;"));

        assert!(!need_limit(
            "INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');"
        ));
        assert!(!need_limit("delete from test"));
        assert!(need_limit("select * from test where a like '% LIMIT %'"));
    }
}
