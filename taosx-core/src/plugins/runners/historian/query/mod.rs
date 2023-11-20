use chrono::{DateTime, Utc};
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
    pub async fn new(config: ConnectConfig) -> anyhow::Result<Self> {
        let client = Self::connect(&config.host, config.port, &config.username, &config.password)
            .await
            .map_err(|err| anyhow::anyhow!("failed to connect to historian, cause: {}", err.to_string()))?;
        Ok(Self {
            client
        })
    }

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
        if tags.len() == 1 && tags.first().unwrap().as_str() == "*" {
            sql = format!("select {} from Runtime.dbo.Live where TagName not like 'Sys%'", LIVE_COLUMNS);
        } else {
            sql = format!("select {} from Runtime.dbo.Live where TagName in ({})", LIVE_COLUMNS, tags.join(","));
        }

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    pub async fn query_history(
        &mut self,
        tags: Vec<String>,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<QueryStream> {
        let sql;
        if tags.len() == 1 && tags.first().unwrap().as_str() == "*" {
            sql = format!(
                "select {} from Runtime.dbo.History where TagName not like 'Sys%' and DateTime >= '{}' and DateTime < '{}'",
                HISTORY_COLUMNS,
                begin.to_rfc3339(),
                end.to_rfc3339()
            );
        } else {
            sql = format!(
                "select {} from Runtime.dbo.History where TagName in ({}) and DateTime >= '{}' and DateTime < '{}'",
                HISTORY_COLUMNS,
                tags.join(","),
                begin.to_rfc3339(),
                end.to_rfc3339()
            );
        }

        Ok(self.client.query(sql.as_str(), &[]).await?)
    }

    async fn connect(host: &String, port: u16, username: &String, password: &String) -> anyhow::Result<Client<Compat<TcpStream>>> {
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
    use std::str::FromStr;
    use taos::Dsn;
    use crate::runners::historian::config::connect::ConnectConfig;
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_query_tag_meta() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let mut client = HistorianQuery::new(config).await.unwrap();
        let tag_meta = client.get_tags().await.unwrap();

        println!("tag_meta: {:?}", tag_meta);
    }
}