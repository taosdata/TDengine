use tiberius::{Client, QueryItem, Row};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::compat::Compat;

#[derive(Debug)]
pub struct TagMeta {
    pub name: String,
    pub description: String,
}

impl TagMeta {
    pub fn from_row(row: &Row) -> anyhow::Result<Self> {
        let tag_name = (row.try_get("TagName")? as Option<&str>)
            .ok_or_else(|| anyhow::anyhow!("TagName is required"))?
            .to_string();
        let description = (row.try_get("Description")? as Option<&str>)
            .unwrap_or("")
            .to_string();

        Ok(TagMeta {
            name: tag_name,
            description,
        })
    }
}

pub async fn query_tags(client: &mut Client<Compat<TcpStream>>) -> anyhow::Result<Vec<TagMeta>> {
    let tags_query: &str = "select * from dbo.Tag";
    let mut response = client.query(tags_query, &[]).await?;

    let mut tag_meta: Vec<TagMeta> = Vec::new();

    while let Some(row) = response.try_next().await? {
        match row {
            QueryItem::Row(row) => {
                let tag_name: &str = row.get("TagName").unwrap();
                if tag_name.starts_with("Sys") {
                    continue;
                }
                tag_meta.push(TagMeta::from_row(&row)?);
            }
            _ => {}
        }
    }

    Ok(tag_meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::runners::historian::config::SourceConfig;
    use crate::plugins::runners::historian::connect;
    use std::str::FromStr;
    use taos::Dsn;

    #[test]
    fn test_from_row() {}

    #[tokio::test]
    #[ignore]
    async fn test_query_tag_meta() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@192.168.3.40:1433").unwrap();
        let config = SourceConfig::from_dsn(&dsn).unwrap();
        let mut client = connect(&config).await.unwrap();

        let tag_meta = query_tags(&mut client).await.unwrap();

        println!("tag_meta: {:?}", tag_meta);
    }
}
