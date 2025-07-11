use chrono::{DateTime, Local, Utc};
use futures_util::TryStreamExt;
use itertools::Itertools;
use tiberius::{AuthMethod, Client, Config, QueryStream};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::HistorianTable;
use super::config::ConnectConfig;

pub struct HistorianQuery {
    client: Client<Compat<TcpStream>>,
}

impl HistorianQuery {
    pub async fn try_connect(config: ConnectConfig) -> anyhow::Result<Self> {
        let client = Self::connect(
            &config.host,
            config.port,
            &config.username,
            &config.password,
        )
        .await?;
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
        let sql = select_from_live_sql(tags);
        tracing::debug!("sql: {}", sql);

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn select_from_history(
        &mut self,
        tags: Vec<String>,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<QueryStream> {
        let sql = select_from_history_sql(tags, begin, end);
        tracing::debug!("sql: {}", sql);

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn describe_table(&mut self, table: HistorianTable) -> anyhow::Result<QueryStream> {
        let sql = describe_table_sql(table);
        tracing::debug!("sql: {}", sql);

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn select_tags_with_condition(
        &mut self,
        top_n: Option<usize>,
        condition: Vec<String>,
    ) -> anyhow::Result<QueryStream> {
        let sql = select_tags_with_condition_sql(top_n, condition);
        tracing::debug!("sql: {}", sql);

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn select_top_n(
        &mut self,
        top_n: usize,
        table: HistorianTable,
        tags_condition: Vec<String>,
        begin_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> anyhow::Result<QueryStream> {
        let mut rows = self
            .select_tags_with_condition(Some(top_n), tags_condition)
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

        let sql = select_top_n_sql(top_n, table, tags, begin_time, end_time);
        tracing::debug!("sql: {}", sql);

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }
}

/// SELECT * FROM Runtime.dbo.History
/// WHERE wwRetrievalMode = 'full'
/// AND TagName in ($tags)
/// AND DateTime >= '$begin'
/// AND DateTime < '$end'
fn select_from_history_sql(tags: Vec<String>, begin: DateTime<Utc>, end: DateTime<Utc>) -> String {
    let begin: DateTime<Local> = DateTime::from(begin);
    let end: DateTime<Local> = DateTime::from(end);

    format!(
        "select * from Runtime.dbo.History where wwRetrievalMode = 'full' and TagName in ({}) and DateTime >= '{}' and DateTime < '{}'",
        tags.iter().map(|t| { format!("'{}'", t) }).join(","),
        begin.to_rfc3339(),
        end.to_rfc3339()
    )
}

/// 如果 tags == *，查所有表
fn select_from_live_sql(tags: Vec<String>) -> String {
    if !tags.is_empty() && tags.len() == 1 && tags.first().unwrap() == "*" {
        "select * from Runtime.dbo.Live where TagName not like 'Sys%'".to_string()
    } else {
        format!(
            "select * from Runtime.dbo.Live where TagName in ({})",
            tags.iter().map(|t| { format!("'{}'", t) }).join(",")
        )
    }
}

/// 查表的 schema
fn describe_table_sql(t: HistorianTable) -> String {
    match t {
        HistorianTable::History => "exec sp_columns History".to_string(),
        HistorianTable::Live => "exec sp_columns Live".to_string(),
    }
}

/// parameter: tag_conditions like: ["tag1", "tag2", "HD*", "ABC*"]
/// return: select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName in ('tag1', 'tag2') or TagName like 'HD%' or TagName like 'ABC%')
fn select_tags_with_condition_sql(top: Option<usize>, tag_conditions: Vec<String>) -> String {
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

fn select_top_n_sql(
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
    use taos::IntoDsn;

    #[tokio::test]
    async fn test_try_connect() {
        // given
        let dsn = "historian://aaAdmin:aaAdmin@localhost:1433"
            .into_dsn()
            .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // when
        let query = HistorianQuery::try_connect(config).await;
        // then
        assert!(query.is_err());
    }

    #[test]
    fn test_select_from_history_sql() {
        // given
        let begin = "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let end = "2025-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        // when
        let sql =
            select_from_history_sql(vec![String::from("tag1"), String::from("tag2")], begin, end);
        // then
        let exp_begin: DateTime<Local> = DateTime::from(begin);
        let exp_end: DateTime<Local> = DateTime::from(end);
        let expect = format!(
            "select * from Runtime.dbo.History where wwRetrievalMode = 'full' and TagName in ('tag1','tag2') and DateTime >= '{}' and DateTime < '{}'",
            exp_begin.to_rfc3339(),
            exp_end.to_rfc3339()
        );
        assert_eq!(expect, sql);
    }

    #[test]
    fn test_select_from_live_sql() {
        let sql = select_from_live_sql(vec!["*".to_string()]);
        assert_eq!(
            "select * from Runtime.dbo.Live where TagName not like 'Sys%'",
            sql
        );

        let sql = select_from_live_sql(vec![String::from("tag1")]);
        assert_eq!(
            "select * from Runtime.dbo.Live where TagName in ('tag1')",
            sql
        );

        let sql = select_from_live_sql(vec![String::from("tag1"), String::from("tag2")]);
        assert_eq!(
            "select * from Runtime.dbo.Live where TagName in ('tag1','tag2')",
            sql
        );
    }

    #[test]
    fn test_describe_table_sql() {
        let sql = describe_table_sql(HistorianTable::History);
        assert_eq!("exec sp_columns History", sql);

        let sql = describe_table_sql(HistorianTable::Live);
        assert_eq!("exec sp_columns Live", sql);
    }

    #[test]
    fn test_select_from_tag_sql() {
        let tag_conditions = vec![
            "tag1".to_string(),
            "tag2".to_string(),
            "HD*".to_string(),
            "ABC*".to_string(),
        ];
        let sql = select_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName in ('tag1','tag2') or TagName like 'HD%' or TagName like 'ABC%')"
        );

        let tag_conditions = vec!["*".to_string()];
        let sql = select_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like '%')"
        );

        let tag_conditions = vec!["HD*".to_string()];
        let sql = select_tags_with_condition_sql(None, tag_conditions);
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
        let sql = select_tags_with_condition_sql(None, tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like 'HD%' or TagName like '020401021%' or TagName in ('02040111320002_018','02040111320005_015'))"
        );
    }

    #[test]
    fn test_select_top_n_sql() {
        let sql = select_top_n_sql(1, HistorianTable::Live, Vec::new(), None, None);
        assert_eq!(
            sql,
            "select top 1 * from Runtime.dbo.Live where wwRetrievalMode = 'full' and TagName not like 'Sys%'"
        );

        let begin: DateTime<Utc> = DateTime::parse_from_rfc3339("2024-01-01T11:11:11.111+08:00")
            .unwrap()
            .into();
        let end: DateTime<Utc> = DateTime::parse_from_rfc3339("2024-01-01T22:22:22.222+08:00")
            .unwrap()
            .into();
        let sql = select_top_n_sql(
            10,
            HistorianTable::History,
            Vec::new(),
            Some(begin),
            Some(end),
        );
        assert_eq!(
            sql,
            "select top 10 * from Runtime.dbo.History where wwRetrievalMode = 'full' and TagName not like 'Sys%' and DateTime >= '2024-01-01T11:11:11.111+08:00' and DateTime < '2024-01-01T22:22:22.222+08:00'"
        );
    }
}
