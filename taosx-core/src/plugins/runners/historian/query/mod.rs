use crate::runners::historian::config::connect::ConnectConfig;
use crate::runners::historian::config::HistorianTable;
use chrono::{DateTime, Local, Utc};
use futures_util::TryStreamExt;
use itertools::Itertools;
use tiberius::{AuthMethod, Client, Config, QueryStream};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub struct HistorianQuery {
    client: Client<Compat<TcpStream>>,
}

impl HistorianQuery {
    pub async fn try_new(config: ConnectConfig) -> anyhow::Result<Self> {
        let client = Self::connect(
            &config.host,
            config.port,
            &config.username,
            &config.password,
        )
        .await
        .map_err(|err| {
            anyhow::anyhow!("failed to connect to historian, cause: {}", err.to_string())
        })?;
        Ok(Self { client })
    }

    async fn connect(
        host: &String,
        port: u16,
        username: &String,
        password: &String,
    ) -> anyhow::Result<Client<Compat<TcpStream>>> {
        let mut config = Config::new();
        config.host(host);
        config.port(port);
        config.database("Runtime");
        config.authentication(AuthMethod::sql_server(username, password));
        config.trust_cert();

        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let client: Client<Compat<TcpStream>> = Client::connect(config, tcp.compat_write()).await?;

        Ok(client)
    }

    pub async fn select_from_live(&mut self, tags: Vec<String>) -> anyhow::Result<QueryStream> {
        let sql;

        if !tags.is_empty() && tags.len() == 1 && tags.first().unwrap() == "*" {
            sql = "select * from Runtime.dbo.Live where TagName not like 'Sys%'".to_string();
        } else {
            sql = format!(
                "select * from Runtime.dbo.Live where TagName in ({})",
                tags.iter().map(|t| { format!("'{}'", t) }).join(",")
            );
        }

        tracing::debug!("query sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn select_from_history(
        &mut self,
        tags: Vec<String>,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<QueryStream> {
        let begin: DateTime<Local> = DateTime::from(begin);
        let end: DateTime<Local> = DateTime::from(end);

        let sql = format!(
            "select * from Runtime.dbo.History where TagName in ({}) and DateTime >= '{}' and DateTime < '{}' and wwRetrievalMode = 'full'",
            tags.iter().map(|t| {
                format!("'{}'", t)
            }).join(","),
            begin.to_rfc3339(),
            end.to_rfc3339()
        );

        tracing::debug!("query sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn describe_table(&mut self, table: HistorianTable) -> anyhow::Result<QueryStream> {
        let sql = match table {
            HistorianTable::History => "exec sp_columns History".to_string(),
            HistorianTable::Live => "exec sp_columns Live".to_string(),
        };

        tracing::debug!("query sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn get_tags_with_condition(
        &mut self,
        top_n: Option<usize>,
        condition: Vec<String>,
    ) -> anyhow::Result<QueryStream> {
        let sql = get_tags_with_condition_sql(top_n, condition);

        tracing::debug!("query sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn top_n(
        &mut self,
        top_n: usize,
        table: HistorianTable,
        tags_condition: Vec<String>,
        begin_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> anyhow::Result<QueryStream> {
        let mut rows = self
            .get_tags_with_condition(Some(top_n), tags_condition)
            .await?
            .into_row_stream();

        let mut tags = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let tag_name = row
                .try_get::<&str, _>("TagName")?
                .ok_or(anyhow::anyhow!("TagName cannot be None"))?
                .to_string();
            tags.push(tag_name);
        }
        drop(rows);

        let sql = top_n_sql(top_n, table, tags, begin_time, end_time);

        tracing::debug!("query sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
    }
}

/// parameter: tag_conditions like: ["tag1", "tag2", "HD*", "ABC*"]
/// return: select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName in ('tag1', 'tag2') or TagName like 'HD%' or TagName like 'ABC%')
fn get_tags_with_condition_sql(top: Option<usize>, tag_conditions: Vec<String>) -> String {
    let mut tags_query = match top {
        Some(n) => format!(
            "select top {} * from Runtime.dbo.Tag where TagName NOT like 'Sys%'",
            n
        ),
        None => String::from("select * from Runtime.dbo.Tag where TagName NOT like 'Sys%'"),
    };

    let conditions = tag_conditions
        .iter()
        .chunk_by(|t| t.contains('*'))
        .into_iter()
        .flat_map(|(contain_wildcard, group)| {
            if contain_wildcard {
                group
                    .map(|t| {
                        let tag = t.clone();
                        let condition = tag.replace("*", "%");
                        format!("TagName like '{}'", condition)
                    })
                    .collect::<Vec<String>>()
            } else {
                let tags = group.cloned().join("','");
                vec![format!("TagName in ('{}')", tags)]
            }
        })
        .collect::<Vec<String>>()
        .join(" or ");

    if !conditions.is_empty() {
        tags_query.push_str(" and (");
        tags_query.push_str(conditions.as_str());
        tags_query.push(')');
    }

    tags_query
}

fn top_n_sql(
    top_n: usize,
    table: HistorianTable,
    tags: Vec<String>,
    begin_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
) -> String {
    let mut sql = format!(
        "select top {} * from {} where wwRetrievalMode = 'full'",
        top_n, table,
    );

    if tags.is_empty() {
        sql.push_str(" and TagName not like 'Sys%'");
    } else {
        sql.push_str(
            format!(
                " and TagName in ({})",
                tags.iter().map(|t| { format!("'{}'", t) }).join(",")
            )
            .as_str(),
        );
    }

    if let Some(begin_time) = begin_time {
        let begin_time: DateTime<Local> = DateTime::from(begin_time);
        sql.push_str(format!(" and DateTime >= '{}'", begin_time.to_rfc3339()).as_str());
    }

    if let Some(end_time) = end_time {
        let end_time: DateTime<Local> = DateTime::from(end_time);
        sql.push_str(format!(" and DateTime < '{}'", end_time.to_rfc3339()).as_str());
    }

    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_from_tag_sql() {
        let tag_conditions = vec![
            "tag1".to_string(),
            "tag2".to_string(),
            "HD*".to_string(),
            "ABC*".to_string(),
        ];
        let sql = get_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName in ('tag1','tag2') or TagName like 'HD%' or TagName like 'ABC%')"
        );

        let tag_conditions = vec!["*".to_string()];
        let sql = get_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like '%')"
        );

        let tag_conditions = vec!["HD*".to_string()];
        let sql = get_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like 'HD%')"
        );

        let tag_conditions = vec![
            "HD*".to_string(),
            "020401021*".to_string(),
            "02040111320002_018".to_string(),
            "02040111320005_015".to_string(),
        ];
        let sql = get_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like 'HD%' or TagName like '020401021%' or TagName in ('02040111320002_018','02040111320005_015'))"
        );
    }

    #[test]
    #[ignore]
    fn test_top_n_sql() {
        let sql = top_n_sql(1, HistorianTable::Live, Vec::new(), None, None);
        assert_eq!(
            sql,
            "select top 1 * from Runtime.dbo.Live where TagName not like 'Sys%' and wwRetrievalMode = 'full'"
        );

        let begin: DateTime<Utc> = DateTime::parse_from_rfc3339("2024-01-01T11:11:11.111+08:00")
            .unwrap()
            .into();
        let end: DateTime<Utc> = DateTime::parse_from_rfc3339("2024-01-01T22:22:22.222+08:00")
            .unwrap()
            .into();
        let sql = top_n_sql(
            10,
            HistorianTable::History,
            Vec::new(),
            Some(begin),
            Some(end),
        );
        assert_eq!(sql, "select top 10 * from Runtime.dbo.History where TagName not like 'Sys%' and wwRetrievalMode = 'full' and DateTime >= '2024-01-01T11:11:11.111+08:00' and DateTime < '2024-01-01T22:22:22.222+08:00'");
    }

    #[test]
    fn test_datetime_convert() {
        let utc = Utc::now();
        dbg!(utc.to_rfc3339());

        let local: DateTime<Local> = DateTime::from(utc);
        dbg!(local.to_rfc3339());
    }
}
