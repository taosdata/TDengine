#[cfg(test)]
mod test {

    use crate::*;

    #[tokio::test]
    async fn de_string() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let res = taos.query("select server_version() as version").await?;
        use futures::StreamExt;


        let version: String = res.rows_de_stream().next().await.expect("select version")?;
        println!("version: {version}");
        Ok(())
    }


    #[tokio::test]
    async fn de_wrapper_struct() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let res = taos.query("select server_version() as version").await?;
        use futures::StreamExt;

        #[derive(::serde::Deserialize, Debug)]
        struct Version(String);
        let version: Version = res.rows_de_stream().next().await.expect("select version")?;
        println!("version: {:?}", version);
        Ok(())
    }

    #[tokio::test]
    async fn de_named_struct() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let res = taos.query("select server_version() as version").await?;
        use futures::StreamExt;

        #[derive(::serde::Deserialize, Debug)]
        struct Version {
          version: String,
        };
        let version: Version = res.rows_de_stream().next().await.expect("select version")?;
        println!("version: {:?}", version);
        Ok(())
    }
    #[tokio::test]
    async fn de_show_databases() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let res = taos.query("show databases").await?;
        use futures::StreamExt;

        #[derive(::serde::Deserialize, Debug)]
        struct Database {
          name: String,
          created_time: String,
        };
        let db: Database = res.rows_de_stream().next().await.expect("select version")?;
        println!("db: {:?}", db);
        Ok(())
    }
}
