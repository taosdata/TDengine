use std::str::FromStr;

use anyhow::bail;
use chrono::{DateTime, Local, Utc};
use itertools::Itertools;
use tiberius::{AuthMethod, Client, Config, QueryItem, QueryStream};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use taosx_ipc::prelude::IpcDataType;

use crate::runners::historian::config::connect::ConnectConfig;
use crate::runners::historian::config::HistorianTable;

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
        config.authentication(AuthMethod::sql_server(username, password));
        config.trust_cert();

        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let client: Client<Compat<TcpStream>> = Client::connect(config, tcp.compat_write()).await?;

        Ok(client)
    }

    pub async fn select_from_tag(
        &mut self,
        tag_conditions: Vec<String>,
    ) -> anyhow::Result<QueryStream> {
        let sql = select_tags_where(tag_conditions);

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn describe_table(&mut self, table: HistorianTable) -> anyhow::Result<QueryStream> {
        let sql = match table {
            HistorianTable::History => "exec sp_columns History".to_string(),
            HistorianTable::Live => "exec sp_columns Live".to_string(),
        };

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn select_from_live(&mut self, tags: Vec<String>) -> anyhow::Result<QueryStream> {
        let sql;

        if !tags.is_empty() && tags.len() == 1 && tags.get(0).unwrap() == "*" {
            sql = "select * from Runtime.dbo.Live where TagName not like 'Sys%'".to_string();
        } else {
            sql = format!(
                "select * from Runtime.dbo.Live where TagName in ({})",
                tags.iter().map(|t| { format!("'{}'", t) }).join(",")
            );
        }

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn select_from_history(
        &mut self,
        tags: Vec<String>,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<QueryStream> {
        let sql;

        let begin: DateTime<Local> = DateTime::from(begin);
        let end: DateTime<Local> = DateTime::from(end);

        sql = format!(
                "select * from Runtime.dbo.History where TagName in ({}) and DateTime >= '{}' and DateTime < '{}' and wwRetrievalMode = 'full'",
                tags.iter().map(|t| {
                    format!("'{}'", t)
                }).join(","),
                begin.to_rfc3339(),
                end.to_rfc3339()
            );

        tracing::debug!("sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
    }
}

pub fn to_ipc_data_type(data_type: &str, precision: i32) -> anyhow::Result<IpcDataType> {
    let db_type = match data_type {
        "datetime2" => "timestamp(ms)".to_string(),
        "nvarchar" => format!("varchar({})", precision).to_string(),
        "tinyint" => "tinyint".to_string(),
        "int" => "int".to_string(),
        "float" => "double".to_string(),
        _ => bail!(
            "unsupported data type: {}, precision: {}",
            data_type,
            precision
        ),
    };

    IpcDataType::from_str(db_type.as_str()).map_err(|err| {
        anyhow::anyhow!(
            "failed to convert data type: {}, precision: {}, cause: {}",
            data_type,
            precision,
            err.to_string()
        )
    })
}

/// parameter: tag_conditions like: ["tag1", "tag2", "HD*", "ABC*"]
/// return: select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName in ('tag1', 'tag2') or TagName like 'HD%' or TagName like 'ABC%')
fn select_tags_where(tag_conditions: Vec<String>) -> String {
    let mut tags_query =
        String::from("select * from Runtime.dbo.Tag where TagName NOT like 'Sys%'");

    let conditions = tag_conditions
        .iter()
        .group_by(|t| t.contains('*'))
        .into_iter()
        .map(|(contain_wildcard, group)| {
            if contain_wildcard {
                group
                    .map(|t| {
                        let tag = t.clone();
                        let condition = tag.replace("*", "%");
                        format!("TagName like '{}'", condition)
                    })
                    .collect::<Vec<String>>()
            } else {
                let tags = group.map(|t| t.clone()).join("','");
                vec![format!("TagName in ('{}')", tags)]
            }
        })
        .flatten()
        .collect::<Vec<String>>()
        .join(" or ");
    // .map(|conditions| conditions.join(" or "))
    // .join(" or ");

    if !conditions.is_empty() {
        tags_query.push_str(" and (");
        tags_query.push_str(conditions.as_str());
        tags_query.push_str(")");
    }

    tags_query
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{Local, NaiveDateTime, TimeZone};
    use taos::Dsn;

    use crate::runners::historian::config::connect::ConnectConfig;

    use super::*;

    #[test]
    fn test_select_tag_query() {
        let tag_conditions = vec![
            "tag1".to_string(),
            "tag2".to_string(),
            "HD*".to_string(),
            "ABC*".to_string(),
        ];
        let sql = select_tags_where(tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName in ('tag1','tag2') or TagName like 'HD%' or TagName like 'ABC%')"
        );

        let tag_conditions = vec!["*".to_string()];
        let sql = select_tags_where(tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like '%')"
        );

        let tag_conditions = vec!["HD*".to_string()];
        let sql = select_tags_where(tag_conditions);
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
        let sql = select_tags_where(tag_conditions);
        assert_eq!(
            sql,
            "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%' and (TagName like 'HD%' or TagName like '020401021%' or TagName in ('02040111320002_018','02040111320005_015'))"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_query_tag_meta() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let mut client = HistorianQuery::try_new(config).await.unwrap();
        let tag_meta = client.select_from_tag(vec!["*".to_string()]).await.unwrap();
        dbg!(tag_meta);
    }

    #[tokio::test]
    #[ignore]
    async fn test_query_history() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut client = HistorianQuery::try_new(config).await.unwrap();

        let tags = vec!["tag0".to_string(), "tag1".to_string()];
        let end = Utc::now();
        let begin = end - chrono::Duration::days(7);

        let mut rows = client.select_from_history(tags, begin, end).await.unwrap();
        while let Some(row) = rows.try_next().await.unwrap() {
            match row {
                QueryItem::Row(row) => {
                    dbg!(row);
                }
                QueryItem::Metadata(_) => {
                    continue;
                }
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_query_live() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut client = HistorianQuery::try_new(config).await.unwrap();

        let tags = vec!["tag0".to_string(), "tag1".to_string()];
        let mut rows = client.select_from_live(tags).await.unwrap();
        while let Some(row) = rows.try_next().await.unwrap() {
            match row {
                QueryItem::Row(row) => {
                    let date_time = row
                        .try_get::<NaiveDateTime, &str>("DateTime")
                        .unwrap()
                        .unwrap();

                    dbg!(date_time);

                    let ts = Local::now()
                        .fixed_offset()
                        .timezone()
                        .from_local_datetime(&date_time)
                        .unwrap()
                        .timestamp_nanos_opt()
                        .unwrap();

                    dbg!(ts);
                }
                QueryItem::Metadata(_) => {
                    continue;
                }
            }
        }
    }

    #[test]
    fn test_datetime_convert() {
        let utc = Utc::now();
        dbg!(utc.to_rfc3339());

        let local: DateTime<Local> = DateTime::from(utc);
        dbg!(local.to_rfc3339());
    }
}
