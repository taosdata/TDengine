use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use ha_core::types::{SplitJobResult, SplitJobTask};
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, Taos, TaosBuilder};

pub async fn split_job(task: SplitJobTask) -> anyhow::Result<SplitJobResult> {
    let mut from = task.from;
    let db = from.subject.clone().context("db name is required")?;
    let conn = TaosBuilder::from_dsn(&from)
        .context("invalid `from` dsn")?
        .build()
        .await
        .context("build db connection error")?;

    let mut tables = Vec::new();
    if let Some(stbs) = from.get("stables").map(|v| v.split(",")) {
        tables.extend(stbs.map(|v| v.to_string()));
    }
    if let Some(tbs) = from.get("tables").map(|v| v.split(",")) {
        tables.extend(tbs.map(|v| v.to_string()));
    }

    if tables.is_empty() {
        let stables = query(&conn, "show stables").await?;
        tables.extend(stables);
        let sql = format!(
            "select table_name from information_schema.ins_tables where `type`='NORMAL_TABLE' and `db_name`='{db}';"
        );
        let normals = query(&conn, &sql).await?;
        tables.extend(normals);
    }

    let mut start: Option<DateTime<Utc>> = from
        .get("start")
        .map(|v| DateTime::parse_from_rfc3339(v))
        .transpose()
        .context("invalid `start` param")?
        .map(Into::into);
    if start.is_none() {
        for table in &tables {
            let sql = format!("select first(_c0) from `{table}`");
            let Some(ts) = conn.query_one::<_, DateTime<Utc>>(&sql).await? else {
                continue;
            };
            if start.is_none_or(|v| v > ts) {
                start = Some(ts);
            }
        }
    }
    if let Some(ts) = start {
        from.set("start", ts.to_rfc3339());
    }
    let mut end: Option<DateTime<Utc>> = from
        .get("end")
        .map(|v| DateTime::parse_from_rfc3339(v))
        .transpose()
        .context("invalid `end` param")?
        .map(Into::into);
    for table in &tables {
        let sql = format!("select last(_c0) from `{table}`");
        let Some(ts) = conn.query_one::<_, DateTime<Utc>>(&sql).await? else {
            continue;
        };
        if end.is_none_or(|v| v < ts) {
            end = Some(ts);
        }
    }
    if let Some(ts) = end {
        from.set("end", ts.to_rfc3339());
    }

    Ok(SplitJobResult {
        from: serde_json::Value::String(from.to_string()),
        to: task.to.to_string(),
        parser: task.parser,
    })
}

async fn query<T>(conn: &Taos, sql: &str) -> anyhow::Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    conn.query(sql)
        .await
        .with_context(|| format!("query {sql} error"))?
        .deserialize::<T>()
        .try_collect()
        .await
        .context("deserialize stable name error")
}
