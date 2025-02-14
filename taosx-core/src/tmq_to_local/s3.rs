use crate::utils;
use opendal::{EntryMode, Operator};
use taos::Dsn;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct S3Config {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    region: Option<String>,
    bucket: String,
    /// S3 对象存储的前缀，类似于 directory，默认为：'/'，即根目录
    prefix: String,
}

impl S3Config {
    /// 从 DSN 中解析 S3 配置
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let endpoint = utils::parse_key_in_dsn::<String>(dsn, "s3_endpoint")?
            .ok_or(anyhow::anyhow!("s3_endpoint not found"))?;
        let access_key_id = utils::parse_key_in_dsn::<String>(dsn, "s3_access_key_id")?
            .ok_or(anyhow::anyhow!("s3_access_key_id not found"))?;
        let secret_access_key = utils::parse_key_in_dsn::<String>(dsn, "s3_secret_access_key")?
            .ok_or(anyhow::anyhow!("s3_secret_access_key not found"))?;
        let region = utils::parse_key_in_dsn::<String>(dsn, "s3_region")?;
        let bucket = utils::parse_key_in_dsn(dsn, "s3_bucket")?
            .ok_or(anyhow::anyhow!("s3_bucket not found"))?;
        let prefix = utils::parse_key_in_dsn::<String>(dsn, "s3_prefix")?
            .map(|s| {
                if s.trim().ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            })
            .unwrap_or("/".to_string());

        Ok(Self {
            endpoint,
            access_key_id,
            secret_access_key,
            region,
            bucket,
            prefix,
        })
    }

    pub async fn connect(&self) -> anyhow::Result<Operator> {
        // Pick a builder and configure it.
        let mut builder = opendal::services::S3::default()
            .bucket(&self.bucket)
            .endpoint(&self.endpoint)
            .access_key_id(&self.access_key_id)
            .secret_access_key(&self.secret_access_key);

        if let Some(region) = &self.region {
            builder = builder.region(region.as_str());
        }

        // Init an operator
        let op = Operator::new(builder)?.finish();
        // read the meta of the prefix
        let meta = op.stat(&self.prefix).await?;
        match meta.mode() {
            EntryMode::DIR => {
                tracing::info!("connected with s3 directory: {}", self.prefix);
            }
            EntryMode::FILE | EntryMode::Unknown => {
                anyhow::bail!("prefix: {} is not a directory", self.prefix);
            }
        }

        Ok(op)
    }
}

#[derive(Debug)]
pub struct S3Dumper {
    config: S3Config,
    cancel_token: CancellationToken,

    op: Operator,
}

impl S3Dumper {
    pub async fn new(config: S3Config, cancel_token: CancellationToken) -> anyhow::Result<Self> {
        // connect to s3
        let op = config.connect().await?;
        Ok(Self {
            config,
            cancel_token,
            op,
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("s3 dumper started");
        tracing::info!(
            "s3 dumper config: {:?}, operator: {:?}",
            self.config,
            self.op
        );
        let _cancel_guard = self.cancel_token.clone().drop_guard();

        loop {
            tokio::select! {
                _ =  self.cancel_token.cancelled() => {
                    tracing::info!("s3 dumper cancelled");
                    break;
                }
                res = self.process() => match res{
                    Ok(_) => {},
                    Err(e) => {
                        tracing::error!("s3 dumper error: {:?}", e);
                        break;
                    }
                }
            }
        }

        tracing::info!("s3 dumper stopped");
        Ok(())
    }

    async fn process(&self) -> anyhow::Result<()> {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        println!("s3 dumper processing");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use taos::IntoDsn;

    /// # Case
    /// 测试 S3 配置
    /// # Example
    /// ```shell
    /// S3_ENDPOINT=http://192.168.2.139:9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=us-west-1 cargo nextest run -p taosx-core test_s3_config --no-capture
    ///
    /// S3_ENDPOINT=http%3A%2F%2F192.168.2.139%3A9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=us-west-1 cargo nextest run -p taosx-core test_s3_config --no-capture
    /// ```
    #[tokio::test]
    async fn test_s3_config() {
        let endpoint = env::var("S3_ENDPOINT").ok();
        if let Some(endpoint) = endpoint {
            // given
            let key = env::var("S3_ACCESS_KEY_ID").unwrap();
            let secret = env::var("S3_SECRET_ACCESS_KEY").unwrap();
            let bucket = env::var("S3_BUCKET").unwrap();
            let mut dsn = format!(
                "local:/tmp?s3_endpoint={}&s3_access_key_id={}&s3_secret_access_key={}&s3_bucket={}",
                endpoint, key, secret, bucket
            );
            let region = env::var("S3_REGION").ok();
            if let Some(region) = &region {
                dsn.push_str(&format!("&s3_region={}", region));
            }
            let dsn = dsn.into_dsn().unwrap();
            // when
            let config = super::S3Config::from_dsn(&dsn).unwrap();
            // then
            let endpoint = urlencoding::decode(&endpoint).unwrap().to_string();
            assert_eq!(config.endpoint, endpoint);
            assert_eq!(config.access_key_id, key);
            assert_eq!(config.secret_access_key, secret);
            assert_eq!(config.region, region);
            assert_eq!(config.bucket, bucket);
            assert_eq!(config.prefix, "/");

            let op = config.connect().await;
            assert!(op.is_ok());
        }
    }

    /// # Case
    /// 测试 S3 Dumper
    /// # Example
    /// ```shell
    /// S3_ENDPOINT=http://192.168.2.13:9000 S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_BUCKET=test S3_REGION=auto cargo nextest run -p taosx-core test_s3_dumper --no-capture
    /// ```
    #[tokio::test]
    async fn test_s3_dumper() {
        let endpoint = env::var("S3_ENDPOINT").ok();
        if let Some(endpoint) = endpoint {
            // given
            let key = env::var("S3_ACCESS_KEY_ID").unwrap();
            let secret = env::var("S3_SECRET_ACCESS_KEY").unwrap();
            let bucket = env::var("S3_BUCKET").unwrap();
            let mut dsn = format!(
                "local:/tmp?s3_endpoint={}&s3_access_key_id={}&s3_secret_access_key={}&s3_bucket={}",
                endpoint, key, secret, bucket
            );
            let region = env::var("S3_REGION").ok();
            if let Some(region) = &region {
                dsn.push_str(&format!("&s3_region={}", region));
            }
            let dsn = dsn.into_dsn().unwrap();
            let config = super::S3Config::from_dsn(&dsn).unwrap();
            let cancel_token = tokio_util::sync::CancellationToken::new();
            let dumper = super::S3Dumper::new(config, cancel_token.clone())
                .await
                .unwrap();
            // when
            let h = tokio::spawn(async move {
                dumper.run().await.unwrap();
            });
            // then
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            cancel_token.cancel();
            h.await.unwrap();
        }
    }
}
