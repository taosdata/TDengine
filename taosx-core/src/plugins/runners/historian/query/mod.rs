use chrono::{DateTime, Utc};
use itertools::Itertools;
use tiberius::{AuthMethod, Client, Config, QueryItem, QueryStream};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::runners::historian::config::connect::ConnectConfig;
use crate::runners::historian::query::tag::TagMeta;

mod tag;

pub struct HistorianQuery {
    client: Client<Compat<TcpStream>>,
}

const HISTORY_COLUMNS: &str = "DateTime,TagName,Value,vValue,Quality,QualityDetail,wwTagKey,wwResolution,StartDateTime,SourceTag,SourceServer";
const LIVE_COLUMNS: &str = "DateTime,TagName,Value,vValue,Quality,QualityDetail,OPCQuality,wwTagKey,SourceTag,SourceServer";

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

    #[allow(dead_code)]
    pub async fn get_tags(&mut self) -> anyhow::Result<Vec<TagMeta>> {
        let tags_query = "select * from Runtime.dbo.Tag where TagName NOT like 'Sys%'".to_string();
        let mut response = self.client.query(tags_query, &[]).await?;

        let mut tag_meta: Vec<TagMeta> = Vec::new();
        while let Some(row) = response.try_next().await? {
            match row {
                QueryItem::Row(row) => {
                    tag_meta.push(TagMeta::from_row(&row)?);
                }
                _ => {}
            }
        }

        Ok(tag_meta)
    }

    pub async fn query_live(&mut self, tags: Vec<String>) -> anyhow::Result<QueryStream> {
        let sql;
        sql = format!(
            "select {} from Runtime.dbo.Live where TagName in ({})",
            LIVE_COLUMNS,
            tags.iter().map(|t| { format!("'{}'", t) }).join(",")
        );

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn query_history(
        &mut self,
        tags: Vec<String>,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<QueryStream> {
        let sql;

        sql = format!(
                "select {} from Runtime.dbo.History where TagName in ({}) and DateTime >= '{}' and DateTime < '{}' and wwRetrievalMode = 'full'",
                HISTORY_COLUMNS,
                tags.iter().map(|t| {
                    format!("'{}'", t)
                }).join(","),
                begin.to_rfc3339(),
                end.to_rfc3339()
            );

        tracing::debug!("sql: {}", sql);
        Ok(self.client.query(sql.as_str(), &[]).await?)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runners::historian::config::connect::ConnectConfig;
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test]
    #[ignore]
    async fn test_query_tag_meta() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let mut client = HistorianQuery::try_new(config).await.unwrap();
        let tag_meta = client.get_tags().await.unwrap();
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

        let mut rows = client.query_history(tags, begin, end).await.unwrap();
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
        let mut rows = client.query_live(tags).await.unwrap();
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
}
